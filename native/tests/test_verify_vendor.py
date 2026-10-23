# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the provenance CLI with real copies under every optimization mode."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

NATIVE = Path(__file__).resolve().parents[1]


@pytest.fixture(params=["normal", "flag", "environment"])
def mode(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def native_copy(tmp_path: Path) -> Path:
    native = tmp_path / "native"
    (native / "scripts").mkdir(parents=True)
    shutil.copyfile(
        NATIVE / "scripts/verify_vendor.py", native / "scripts/verify_vendor.py"
    )
    shutil.copytree(NATIVE / "vendor", native / "vendor")
    return native


def run_verifier(
    native: Path,
    mode: str,
    *args: str,
    metadata: dict[str, Any] | None = None,  # JSON-shaped external Cargo response
) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment.pop("PYTHONOPTIMIZE", None)
    command = [sys.executable]
    if mode == "flag":
        command.append("-O")
    elif mode == "environment":
        environment["PYTHONOPTIMIZE"] = "1"
    script = native / "scripts/verify_vendor.py"
    if metadata is None:
        command.extend([str(script), *args])
    else:
        # Stub only Cargo's external JSON response; run the actual CLI in a child.
        code = (
            "import runpy, subprocess, sys; from types import SimpleNamespace; "
            f"subprocess.run = lambda *a, **k: SimpleNamespace(stdout={json.dumps(metadata)!r}); "
            f"sys.argv = {[str(script), '--cargo', *args]!r}; "
            f"runpy.run_path({str(script)!r}, run_name='__main__')"
        )
        command.extend(["-c", code])
    return subprocess.run(
        command,
        cwd=native,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_clean_source_verifies(native_copy: Path, mode: str) -> None:
    result = run_verifier(native_copy, mode)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["verified_files"] == 581


@pytest.mark.parametrize("member", ["LICENSE", "src/runtime/builder.rs"])
def test_changed_vendor_bytes_fail(native_copy: Path, mode: str, member: str) -> None:
    path = native_copy / "vendor/tokio" / member
    path.write_bytes(path.read_bytes() + b"\nchanged\n")
    result = run_verifier(native_copy, mode)
    assert result.returncode != 0
    assert "vendored source differs" in result.stderr
    assert not result.stdout


def test_extra_vendor_file_fails(native_copy: Path, mode: str) -> None:
    (native_copy / "vendor/tokio/unexpected.txt").write_text("unexpected")
    result = run_verifier(native_copy, mode)
    assert result.returncode != 0
    assert "vendored source differs" in result.stderr


def test_changed_patch_fails(native_copy: Path, mode: str) -> None:
    patch = native_copy / "vendor/tokio-1.54.1-startup-cleanup.patch"
    patch.write_bytes(patch.read_bytes() + b"\nchanged\n")
    result = run_verifier(native_copy, mode)
    assert result.returncode != 0
    assert "patch file checksum differs" in result.stderr


@pytest.mark.parametrize("field", ["archive_sha256", "patch_sha256"])
def test_changed_trusted_checksum_fails(
    native_copy: Path, mode: str, field: str
) -> None:
    path = native_copy / "vendor/tokio-provenance.json"
    provenance = json.loads(path.read_text())
    provenance[field] = "0" * 64
    path.write_text(json.dumps(provenance))
    result = run_verifier(native_copy, mode)
    assert result.returncode != 0
    assert "checksum changed" in result.stderr


def test_changed_archive_fails(native_copy: Path, mode: str) -> None:
    archive = native_copy / "corrupt.crate"
    archive.write_bytes(b"not the official archive")
    result = run_verifier(native_copy, mode, "--archive", str(archive))
    assert result.returncode != 0
    assert "official archive checksum differs" in result.stderr


@pytest.mark.parametrize(
    "identity", ["clean", "missing", "duplicate", "path", "version"]
)
def test_cargo_identity_is_enforced(
    native_copy: Path, mode: str, identity: str
) -> None:
    tokio = {
        "name": "tokio",
        "version": "1.54.1",
        "manifest_path": str(native_copy / "vendor/tokio/Cargo.toml"),
    }
    packages = [tokio]
    expected = ""
    if identity == "missing":
        packages = []
        expected = "expected exactly one Tokio crate identity"
    elif identity == "duplicate":
        packages.append(dict(tokio))
        expected = "expected exactly one Tokio crate identity"
    elif identity == "path":
        tokio["manifest_path"] = str(native_copy / "wrong/Cargo.toml")
        expected = "Tokio does not resolve to the vendored manifest"
    elif identity == "version":
        tokio["version"] = "1.54.0"
        expected = "Tokio version differs from 1.54.1"
    result = run_verifier(native_copy, mode, metadata={"packages": packages})
    if identity == "clean":
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["one_tokio"] is True
    else:
        assert result.returncode != 0
        assert expected in result.stderr
        assert not result.stdout


@pytest.mark.parametrize(
    "member", ["src/runtime/blocking/pool.rs", "src/runtime/builder.rs"]
)
def test_source_and_regenerated_manifest_drift_fail(
    native_copy: Path, mode: str, member: str
) -> None:
    path = native_copy / "vendor/tokio" / member
    path.write_bytes(path.read_bytes() + b"\n// unintended regenerated source drift\n")
    manifest = native_copy / "vendor/tokio-provenance.json"
    provenance = json.loads(manifest.read_text())
    changed = hashlib.sha256(path.read_bytes()).hexdigest()
    provenance["patched_files"][member] = changed
    if member == "src/runtime/builder.rs":
        # Regenerating both maps used to hide changes to a pristine file too.
        provenance["upstream_files"][member] = changed
    manifest.write_text(json.dumps(provenance))
    result = run_verifier(native_copy, mode)
    assert result.returncode != 0
    assert "approved patched-file manifest differs" in result.stderr
    assert not result.stdout
