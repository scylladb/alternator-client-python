# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fail-closed matrix/evidence checks; synthetic wheels never masquerade as builds."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
import venv
import zipfile
from pathlib import Path
from unittest.mock import patch

import ci

COMMIT = "a" * 40
RUN_ID = "12345"
VERSION = "2.0.1"
NEEDS = {"core": {"result": "success"}, "wheel": {"result": "success"}}


def wheel(
    directory: Path, python: str, platform: str, *, abi: str | None = None
) -> Path:
    interpreter = f"cp{python.replace('.', '')}"
    tags = {
        "macos_x86_64": "macosx_11_0_x86_64",
        "macos_arm64": "macosx_11_0_arm64",
    }
    tag = f"{interpreter}-{abi or interpreter}-{tags.get(platform, platform)}"
    path = directory / f"alternator_client_rust_backend-{VERSION}-{tag}.whl"
    prefix = f"alternator_client_rust_backend-{VERSION}.dist-info/"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            prefix + "METADATA", f"Name: {ci.BACKEND}\nVersion: {VERSION}\n"
        )
        archive.writestr(prefix + "WHEEL", f"Root-Is-Purelib: false\nTag: {tag}\n")
        archive.writestr(
            "_alternator_rust.pyd"
            if platform == "win_amd64"
            else "_alternator_rust.so",
            b"fixture",
        )
    return path


def cell(directory: Path, python: str, platform: str) -> Path:
    path = directory / f"custom-cell-{python}-{platform}"
    path.mkdir()
    artifact = wheel(path, python, platform)
    build = {
        "filename": artifact.name,
        "sha256": ci.digest(artifact),
        "commit": COMMIT,
        "run_id": RUN_ID,
    }
    install = {
        "schema": 1,
        "stage": "interface",
        "slot": f"cp{python.replace('.', '')}/{platform}",
        **build,
        "version": VERSION,
        "base_sha256": "b" * 64,
        **dict.fromkeys(ci.CHECKS, True),
    }
    (path / "build.json").write_bytes(ci.canonical(build))
    (path / "install.json").write_bytes(ci.canonical(install))
    (path / "provenance.json").write_bytes(
        ci.canonical(
            {
                "build": build,
                "slot": install["slot"],
                "stage": "interface",
            }
        )
    )
    (path / "SHA256SUMS").write_text(f"{ci.digest(artifact)}  {artifact.name}\n")
    return path


class EvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.cells = [
            cell(self.directory, python, platform)
            for python in ci.PYTHONS
            for platform in ci.PLATFORMS
        ]

    def aggregate(self) -> dict:
        return ci.aggregate(self.directory, COMMIT, RUN_ID, NEEDS)

    def edit(self, file: str, **changes: object) -> None:
        path = self.cells[0] / file
        value = json.loads(path.read_bytes())
        value.update(changes)
        path.write_bytes(ci.canonical(value))

    def test_exact_twenty_cell_cartesian_product(self) -> None:
        self.assertEqual(len(ci.SLOTS), 20)
        result = self.aggregate()
        self.assertEqual(len(result["builds"]), 20)
        self.assertEqual({item["slot"] for item in result["installs"]}, ci.SLOTS)
        self.assertTrue(result["complete"])
        self.assertFalse(result["release_qualified"])
        self.assertTrue(
            all(
                "native_request" not in item
                and "extras" not in item
                and "cleanup" not in item
                for item in result["installs"]
            )
        )

    def test_each_missing_cell_fails(self) -> None:
        for path in self.cells:
            hidden = path.rename(path.with_name("hidden"))
            try:
                # Simulate a successful download containing the other 19 only.
                with tempfile.TemporaryDirectory() as holding:
                    moved = hidden.rename(Path(holding) / "cell")
                    try:
                        with self.assertRaisesRegex(ValueError, "Missing cells"):
                            self.aggregate()
                    finally:
                        moved.rename(path)
            finally:
                if hidden.exists():
                    hidden.rename(path)

    def test_non_success_job_conclusions_fail_even_with_complete_files(self) -> None:
        for conclusion in ("failure", "cancelled", "skipped", "timed_out", ""):
            for job in NEEDS:
                with self.subTest(job=job, conclusion=conclusion):
                    needs = {**NEEDS, job: {"result": conclusion}}
                    with self.assertRaisesRegex(ValueError, "conclude successfully"):
                        ci.aggregate(self.directory, COMMIT, RUN_ID, needs)

    def test_missing_job_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "conclude successfully"):
            ci.aggregate(self.directory, COMMIT, RUN_ID, {"core": NEEDS["core"]})

    def test_wrong_head_or_run_fails(self) -> None:
        for commit, run in (("c" * 40, RUN_ID), (COMMIT, "different-run")):
            with self.assertRaisesRegex(ValueError, "source/run/hash"):
                ci.aggregate(self.directory, commit, run, NEEDS)

    def test_incomplete_smoke_fails(self) -> None:
        for check in ci.CHECKS:
            self.edit("install.json", **{check: False})
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                self.aggregate()
            self.edit("install.json", **{check: True})

    def test_duplicate_cell_fails(self) -> None:
        import shutil

        shutil.copytree(self.cells[0], self.directory / "duplicate")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.aggregate()

    def test_changed_wheel_fails(self) -> None:
        artifact = next(self.cells[0].glob("*.whl"))
        with artifact.open("ab") as stream:
            stream.write(b"changed original artifact")
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            self.aggregate()

    def test_inconsistent_base_wheel_fails(self) -> None:
        self.edit("install.json", base_sha256="c" * 64)
        with self.assertRaisesRegex(ValueError, "different base"):
            self.aggregate()

    def test_mismatched_provenance_fails(self) -> None:
        self.edit("provenance.json", build={})
        with self.assertRaisesRegex(ValueError, "provenance digest"):
            self.aggregate()


class WheelAndIsolationTests(unittest.TestCase):
    def test_abi3_free_threaded_and_wrong_architecture_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for abi in ("abi3", "cp314t", "cp310"):
                artifact = wheel(root, "3.14", "win_amd64", abi=abi)
                with self.assertRaisesRegex(ValueError, "tags/name"):
                    ci.inspect_wheel(artifact, "3.14", "win_amd64", VERSION)
            artifact = wheel(root, "3.14", "macos_arm64")
            with self.assertRaisesRegex(ValueError, "tags/name"):
                ci.inspect_wheel(artifact, "3.14", "macos_x86_64", VERSION)

    def test_metadata_tags_must_match_filename(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact = wheel(Path(directory), "3.10", "win_amd64")
            with zipfile.ZipFile(artifact) as original:
                entries = {name: original.read(name) for name in original.namelist()}
            key = next(name for name in entries if name.endswith("/WHEEL"))
            entries[key] = entries[key].replace(b"cp310-cp310", b"cp310-abi3")
            with zipfile.ZipFile(artifact, "w") as modified:
                for name, data in entries.items():
                    modified.writestr(name, data)
            with self.assertRaisesRegex(ValueError, "filename/metadata"):
                ci.inspect_wheel(artifact, "3.10", "win_amd64", VERSION)

    def test_isolated_child_cannot_find_inherited_rust_or_import_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            venv.EnvBuilder().create(root / "env")
            executable = (
                root
                / "env"
                / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            )
            with patch.dict(
                os.environ,
                {
                    "PATH": "/fake/rust/bin",
                    "PYTHONPATH": "/injected",
                    "RUSTUP_HOME": "/rust",
                    "CARGO_HOME": "/cargo",
                },
            ):
                env = ci.isolated_environment(executable)
            result = subprocess.run(
                [
                    str(executable),
                    "-I",
                    "-c",
                    "import shutil,sys; assert shutil.which('rustc') is None; assert shutil.which('cargo') is None; assert '/injected' not in sys.path; assert sys.prefix != sys.base_prefix",
                ],
                env=env,
                cwd=root,
                check=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0)
            self.assertNotIn("PYTHONPATH", env)
            self.assertNotIn("RUSTUP_HOME", env)
            self.assertNotIn("CARGO_HOME", env)

    def test_source_requires_full_sha_and_exact_commit(self) -> None:
        with patch.object(ci, "git", return_value=COMMIT):
            for invalid in ("main", COMMIT[:12], "b" * 40):
                with self.assertRaisesRegex(ValueError, "exact requested"):
                    ci.check_source(Path.cwd(), invalid)
        with (
            patch.object(ci, "git", side_effect=[COMMIT, " M native/Cargo.toml"]),
            self.assertRaisesRegex(ValueError, "Tracked source changes"),
        ):
            ci.check_source(Path.cwd(), COMMIT)

    def test_prepared_bundle_requires_exact_advertised_head(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ci.git(root, "init", "--initial-branch=main")
            ci.git(root, "config", "user.name", "Test")
            ci.git(root, "config", "user.email", "test@example.invalid")
            (root / "source").write_text("baseline")
            ci.git(root, "add", "source")
            ci.git(root, "commit", "-m", "Baseline")
            baseline = ci.git(root, "rev-parse", "HEAD")
            (root / "source").write_text("prepared")
            ci.git(root, "commit", "-am", "Prepared")
            prepared = ci.git(root, "rev-parse", "HEAD")
            bundle = root / "release-source.bundle"
            ci.git(root, "bundle", "create", str(bundle), f"{baseline}..HEAD", "HEAD")
            ci.git(root, "checkout", "--detach", baseline)
            with self.assertRaisesRegex(ValueError, "bundle HEAD differs"):
                ci.restore_bundle(root, bundle, baseline)
            self.assertEqual(ci.git(root, "rev-parse", "HEAD"), baseline)
            ci.restore_bundle(root, bundle, prepared)
            self.assertEqual(ci.git(root, "rev-parse", "HEAD"), prepared)
            self.assertEqual((root / "source").read_text(), "prepared")


if __name__ == "__main__":
    unittest.main()
