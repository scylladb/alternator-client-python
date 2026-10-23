#!/usr/bin/env python3
# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Build evidence and isolated interface-wheel qualification (Python 3.10+).

This stage intentionally cannot satisfy coordinated release installation evidence:
native requests, both extras and per-client cleanup belong to P10 qualification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import venv
import zipfile
from email.parser import BytesParser
from pathlib import Path
from typing import Any

PYTHONS = tuple(f"3.{minor}" for minor in range(10, 15))
PLATFORMS = (
    "manylinux_2_28_x86_64",
    "macos_x86_64",
    "macos_arm64",
    "win_amd64",
)
SLOTS = frozenset(
    f"cp{python.replace('.', '')}/{platform}"
    for python in PYTHONS
    for platform in PLATFORMS
)
BACKEND = "alternator-client-rust-backend"
SHA = re.compile(r"[0-9a-f]{40}\Z")
CHECKS = (
    "fresh_environments",
    "outside_checkout",
    "without_rust",
    "module_locations",
    "wheel_tags",
    "interface",
    "shared_runtime",
    "task_cleanup",
    "base_import",
)


def canonical(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def check_source(root: Path, commit: str) -> None:
    if not SHA.fullmatch(commit) or git(root, "rev-parse", "HEAD") != commit:
        raise ValueError("Source must be the exact requested full commit SHA")
    if git(root, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Tracked source changes are not allowed")


def restore_bundle(root: Path, bundle: Path, commit: str) -> None:
    """Restore the same-run prepared source; the caller authenticates it first."""
    if not SHA.fullmatch(commit):
        raise ValueError("Expected full prepared source SHA")
    bundle = bundle.resolve()
    git(root, "bundle", "verify", str(bundle))
    advertised = git(root, "bundle", "list-heads", str(bundle), "HEAD")
    if advertised != f"{commit} HEAD":
        raise ValueError("Prepared bundle HEAD differs from requested source")
    if git(root, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Cannot restore source over tracked changes")
    git(root, "fetch", str(bundle), "HEAD")
    git(root, "-c", "advice.detachedHead=false", "checkout", "--detach", commit)
    check_source(root, commit)


def platform_matches(tag: str, platform: str) -> bool:
    if platform == "macos_x86_64":
        return tag == "macosx_11_0_x86_64"
    if platform == "macos_arm64":
        return tag == "macosx_11_0_arm64"
    return tag == platform


def inspect_wheel(path: Path, python: str, platform: str, version: str) -> str:
    """Require one interpreter-specific, GIL-enabled native extension wheel."""
    interpreter = f"cp{python.replace('.', '')}"
    if python not in PYTHONS or platform not in PLATFORMS:
        raise ValueError("Unsupported interpreter/platform")
    parts = path.name.removesuffix(".whl").split("-")
    if (
        not path.name.endswith(".whl")
        or len(parts) != 5
        or parts[:2] != ["alternator_client_rust_backend", version]
        or parts[2:4] != [interpreter, interpreter]
        or not all(platform_matches(tag, platform) for tag in parts[4].split("."))
    ):
        raise ValueError(f"Unexpected native wheel tags/name: {path.name}")
    expected_tags = {
        f"{interpreter}-{interpreter}-{tag}" for tag in parts[4].split(".")
    }
    with zipfile.ZipFile(path) as wheel:
        names = wheel.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate wheel members")
        metadata_names = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise ValueError("Expected exactly one distribution metadata")
        prefix = metadata_names[0].removesuffix("METADATA")
        metadata = BytesParser().parsebytes(wheel.read(prefix + "METADATA"))
        info = BytesParser().parsebytes(wheel.read(prefix + "WHEEL"))
        if metadata["Name"] != BACKEND or metadata["Version"] != version:
            raise ValueError("Native metadata name/version mismatch")
        if set(info.get_all("Tag", [])) != expected_tags:
            raise ValueError("Wheel filename/metadata tags differ")
        if info["Root-Is-Purelib"] != "false":
            raise ValueError("Native wheel must not be pure Python")
        extensions = [
            name
            for name in names
            if Path(name).name.startswith("_alternator_rust.")
            and name.endswith((".so", ".pyd"))
        ]
        if len(extensions) != 1:
            raise ValueError("Native wheel needs exactly one extension")
    return f"{interpreter}/{platform}"


def isolated_environment(python: Path) -> dict[str, str]:
    """Remove inherited import/build settings and every toolchain PATH entry."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PYTHON", "PIP_", "UV_", "CARGO", "RUST"))
        and key not in {"VIRTUAL_ENV", "CONDA_PREFIX", "PATH"}
    }
    # Windows needs its DLL/system tools, but no inherited user tool directories.
    paths = [str(python.parent)]
    if os.name == "nt":
        paths.append(str(Path(os.environ["SYSTEMROOT"]) / "System32"))
    env["PATH"] = os.pathsep.join(paths)
    env["PIP_CONFIG_FILE"] = os.devnull
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    return env


def smoke(
    root: Path, wheel: Path, base: Path, version: str, python: str
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="alternator-wheel-") as directory:
        workspace = Path(directory).resolve()
        if workspace.is_relative_to(root.resolve()):
            raise ValueError("Installation must run outside checkout")
        environment = workspace / "env"
        venv.EnvBuilder(with_pip=True).create(environment)
        executable = environment / (
            "Scripts/python.exe" if os.name == "nt" else "bin/python"
        )
        env = isolated_environment(executable)
        subprocess.run(
            [
                str(executable),
                "-I",
                "-m",
                "pip",
                "install",
                "--only-binary=:all:",
                "--no-cache-dir",
                str(base.resolve()),
                str(wheel.resolve()),
            ],
            cwd=workspace,
            env=env,
            check=True,
            timeout=300,
        )
        result = subprocess.run(
            [
                str(executable),
                "-I",
                str(root / "scripts/native/smoke.py"),
                "--checkout",
                str(root.resolve()),
                "--version",
                version,
                "--python",
                python,
            ],
            cwd=workspace,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
        if result.returncode:
            sys.stderr.write(result.stdout + result.stderr)
            result.check_returncode()
        evidence = json.loads(result.stdout)
        checks = evidence["checks"]
        if checks != dict.fromkeys(CHECKS, True):
            raise ValueError(f"Incomplete interface smoke: {checks}")
        return evidence


def record(args: argparse.Namespace) -> None:
    root = args.root.resolve()
    check_source(root, args.commit)
    wheels = list(args.wheels.glob("*.whl"))
    bases = list(args.base.glob("*.whl"))
    if len(wheels) != 1 or len(bases) != 1:
        raise ValueError("Expected exactly one native wheel and one base wheel")
    wheel, base = wheels[0], bases[0]
    # Read static metadata without executing the package or requiring tomllib on 3.10.
    version_text = (root / "native/pyproject.toml").read_text()
    version = re.search(r'^version = "([^"]+)"$', version_text, re.MULTILINE)
    if version is None:
        raise ValueError("Missing static native version")
    slot = inspect_wheel(wheel, args.python, args.platform, version[1])
    evidence = smoke(root, wheel, base, version[1], args.python)
    check_source(root, args.commit)
    args.output.mkdir(parents=True, exist_ok=True)
    receipt = {
        "filename": wheel.name,
        "sha256": digest(wheel),
        "commit": args.commit,
        "run_id": args.run_id,
    }
    install = {
        "schema": 1,
        "stage": "interface",
        "slot": slot,
        **receipt,
        "version": version[1],
        "base_sha256": digest(base),
        **evidence["checks"],
    }
    info = args.build_info
    provenance = {
        "schema": 1,
        "stage": "interface",
        "build": receipt,
        "slot": slot,
        "target": args.target,
        "version": version[1],
        "rustc": (info / "rustc.txt").read_text(),
        "maturin": (info / "maturin.txt").read_text().strip(),
        "cargo_metadata": json.loads((info / "cargo-metadata.json").read_text()),
        "cargo_lock_sha256": digest(root / "native/Cargo.lock"),
        "native_pyproject_sha256": digest(root / "native/pyproject.toml"),
        "base_sha256": digest(base),
        "installed_environment": evidence["environment"],
        "repository": os.environ.get("GITHUB_REPOSITORY"),
        "workflow_ref": os.environ.get("GITHUB_WORKFLOW_REF"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
    }
    for name, value in (
        ("build.json", receipt),
        ("install.json", install),
        ("provenance.json", provenance),
    ):
        (args.output / name).write_bytes(canonical(value))
    (args.output / "SHA256SUMS").write_text(f"{digest(wheel)}  {wheel.name}\n")


def aggregate(
    directory: Path, commit: str, run_id: str, needs: dict[str, Any]
) -> dict[str, Any]:
    """Fail closed for missing, duplicate, skipped, failed or mismatched cells."""
    if set(needs) != {"core", "wheel"} or any(
        job.get("result") != "success" for job in needs.values()
    ):
        raise ValueError("Every native job must conclude successfully")
    if not SHA.fullmatch(commit):
        raise ValueError("Expected full source SHA")
    receipts = []
    builds = []
    seen = set()
    for path in sorted(directory.iterdir()):
        if not path.is_dir():
            raise ValueError("Expected only cell artifact directories")
        install = json.loads((path / "install.json").read_bytes())
        build = json.loads((path / "build.json").read_bytes())
        provenance = json.loads((path / "provenance.json").read_bytes())
        slot = install["slot"]
        if slot in seen or slot not in SLOTS or install.get("stage") != "interface":
            raise ValueError("Unknown, duplicate or wrong-stage cell")
        if any(install.get(check) is not True for check in CHECKS):
            raise ValueError("Incomplete isolated interface qualification")
        expected = {
            key: install[key] for key in ("filename", "sha256", "commit", "run_id")
        }
        if build != expected or (build["commit"], build["run_id"]) != (commit, run_id):
            raise ValueError("Receipt source/run/hash mismatch")
        wheel = path / build["filename"]
        python, platform = slot.split("/")
        inspect_wheel(wheel, f"3.{python[3:]}", platform, install["version"])
        if digest(wheel) != build["sha256"] or provenance["build"] != build:
            raise ValueError("Wheel/provenance digest mismatch")
        if provenance["slot"] != slot or provenance["stage"] != "interface":
            raise ValueError("Provenance cell mismatch")
        if (path / "SHA256SUMS").read_text() != f"{digest(wheel)}  {wheel.name}\n":
            raise ValueError("Wheel checksum mismatch")
        seen.add(slot)
        builds.append(build)
        receipts.append(install)
    if seen != SLOTS:
        raise ValueError(f"Missing cells: {sorted(SLOTS - seen)}")
    if (
        len({item["version"] for item in receipts}) != 1
        or len({item["base_sha256"] for item in receipts}) != 1
    ):
        raise ValueError("Cells used different base wheels or versions")
    return {
        "schema": 1,
        "stage": "interface",
        "commit": commit,
        "run_id": run_id,
        "complete": True,
        "builds": builds,
        "installs": receipts,
        "release_qualified": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    source = sub.add_parser("source")
    source.add_argument("--root", type=Path, default=Path.cwd())
    source.add_argument("--commit", required=True)
    bundle = sub.add_parser("restore-bundle")
    bundle.add_argument("--root", type=Path, default=Path.cwd())
    bundle.add_argument("--bundle", type=Path, required=True)
    bundle.add_argument("--commit", required=True)
    build = sub.add_parser("record")
    build.add_argument("--root", type=Path, default=Path.cwd())
    for name in ("wheels", "base", "build-info", "output"):
        build.add_argument(f"--{name}", type=Path, required=True)
    for name in ("commit", "run-id", "python", "platform", "target"):
        build.add_argument(f"--{name}", required=True)
    summary = sub.add_parser("aggregate")
    summary.add_argument("--directory", type=Path, required=True)
    summary.add_argument("--output", type=Path, required=True)
    summary.add_argument("--commit", required=True)
    summary.add_argument("--run-id", required=True)
    summary.add_argument("--needs", required=True)
    args = parser.parse_args()
    if args.command == "source":
        check_source(args.root, args.commit)
    elif args.command == "restore-bundle":
        restore_bundle(args.root, args.bundle, args.commit)
    elif args.command == "record":
        record(args)
    else:
        result = aggregate(
            args.directory, args.commit, args.run_id, json.loads(args.needs)
        )
        args.output.write_bytes(canonical(result))


if __name__ == "__main__":
    main()
