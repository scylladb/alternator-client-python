# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Complete portable provenance fixtures and focused gate rejection cases."""

from __future__ import annotations

import copy
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ci


def source_fixture(root: Path) -> None:
    native = root / "native"
    native.mkdir(exist_ok=True)
    (root / "pyproject.toml").write_text(
        '[project]\ndependencies = ["boto3>=1.0", "botocore>=1.0"]\n'
    )
    (native / "pyproject.toml").write_text("""[project]
version = "2.0.1"
[build-system]
requires = ["maturin==1.15.0"]
[tool.maturin]
features = ["extension"]
""")
    (native / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.94.1"\n')
    (native / "Cargo.toml").write_text("""[package]
name = "alternator-python-native"
version = "0.1.0"
[features]
default = []
extension = ["dep:pyo3", "dep:pyo3-async-runtimes", "pyo3/extension-module"]
[dependencies]
tokio = { version = "=1.54.1", features = ["rt-multi-thread", "sync", "time", "net"] }
pyo3 = { version = "=0.27.2", optional = true }
pyo3-async-runtimes = { version = "=0.27.0", features = ["tokio-runtime"], optional = true }
""")
    (native / "Cargo.lock").write_text("""version = 4
[[package]]
name = "alternator-python-native"
version = "0.1.0"
dependencies = ["tokio", "pyo3", "pyo3-async-runtimes"]
[[package]]
name = "tokio"
version = "1.54.1"
[[package]]
name = "pyo3"
version = "0.27.2"
source = "registry+https://github.com/rust-lang/crates.io-index"
[[package]]
name = "pyo3-async-runtimes"
version = "0.27.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = ["tokio", "pyo3"]
""")


def metadata_fixture(source: dict) -> dict:
    packages = []
    nodes = []
    ids = {
        key: f"opaque-builder-path-{index}"
        for index, key in enumerate(sorted(source["identities"], key=str))
    }
    for key, identifier in ids.items():
        features = (
            source["features"]
            if key == source["root"]
            else source["dependency_features"][key[0]]
        )
        definitions = (
            source["definitions"]
            if key == source["root"]
            else dict.fromkeys(features, [])
        )
        packages.append(
            {
                "name": key[0],
                "version": key[1],
                "source": key[2],
                "id": identifier,
                "features": definitions,
            }
        )
        if key == source["root"]:
            packages[-1]["dependencies"] = [
                {
                    "name": name,
                    "req": req,
                    "features": list(features),
                    "optional": optional,
                    "uses_default_features": defaults,
                    "rename": rename,
                    "kind": None,
                    "target": None,
                }
                for name, req, features, optional, defaults, rename in source[
                    "declarations"
                ]
            ]
        dependencies = [ids[child] for child in source["edges"][key]]
        nodes.append(
            {
                "id": identifier,
                "dependencies": dependencies,
                "deps": [{"pkg": child} for child in dependencies],
                "features": sorted(features),
            }
        )
    root = ids[source["root"]]
    return {
        "version": 1,
        "packages": packages,
        "resolve": {"root": root, "nodes": nodes},
        "workspace_members": [root],
    }


def provenance_fixture(root: Path, install: dict, build: dict) -> dict:
    source = ci.source_provenance(root)
    python, platform = install["slot"].split("/")
    target, runner_os, runner_arch = ci.TARGETS[platform]
    repository = os.environ.get("GITHUB_REPOSITORY", "example/client")
    return {
        "schema": 1,
        "stage": "interface",
        "build": build,
        "slot": install["slot"],
        "target": target,
        "version": source["version"],
        "maturin": source["maturin"],
        "rustc": f"rustc 1.94.1 (aaaaaaaaa 2026-03-25)\ncommit-hash: {'a' * 40}\nhost: {target}\nrelease: 1.94.1\n",
        "cargo_metadata": metadata_fixture(source),
        **{
            name: hashlib.sha256(
                payload.replace(b"\n", b"\r\n") if runner_os == "Windows" else payload
            ).hexdigest()
            for name, payload in source["hashes"].items()
        },
        "base_sha256": install["base_sha256"],
        "installed_environment": {
            "python": f"3.{python[3:]}.0 (fixture)",
            "distributions": [
                {"name": name, "version": source["version"]}
                for name in ("alternator-client", ci.BACKEND, "boto3", "botocore")
            ],
        },
        "repository": repository,
        "workflow_ref": os.environ.get(
            "GITHUB_WORKFLOW_REF",
            repository + "/.github/workflows/ci.yml@refs/pull/1/merge",
        ),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        "runner_os": runner_os,
        "runner_arch": runner_arch,
    }


class ProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        source_fixture(self.root)
        self.source = ci.source_provenance(self.root)
        self.build = {
            "filename": "wheel.whl",
            "sha256": "a" * 64,
            "commit": "b" * 40,
            "run_id": "123",
        }
        self.install = {
            "slot": "cp310/manylinux_2_28_x86_64",
            "version": "2.0.1",
            "base_sha256": "c" * 64,
        }
        self.provenance = provenance_fixture(self.root, self.install, self.build)

    def validate(self, value: dict) -> object:
        return ci.validate_provenance(value, self.install, self.build, self.source)

    def test_all_cells_accept_complete_source_bound_provenance(self) -> None:
        for slot in ci.SLOTS:
            install = {**self.install, "slot": slot}
            value = provenance_fixture(self.root, install, self.build)
            ci.validate_provenance(value, install, self.build, self.source)

    def test_every_required_field_is_required(self) -> None:
        for field in ci.PROVENANCE_FIELDS:
            value = copy.deepcopy(self.provenance)
            del value[field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate(value)

    def test_contradictory_scalar_fields_fail(self) -> None:
        for field, value in {
            "schema": 999,
            "stage": "qualified",
            "target": "aarch64-apple-darwin",
            "version": "0.0.0",
            "base_sha256": "e" * 64,
            "cargo_lock_sha256": "d" * 64,
            "native_pyproject_sha256": "d" * 64,
            "maturin": "maturin 0.0.0",
            "rustc": "",
            "runner_arch": "ARM64",
            "runner_os": "Windows",
            "installed_environment": {},
            "cargo_metadata": {},
            "repository": None,
            "workflow_ref": "other/repo/.github/workflows/ci.yml@main",
            "run_attempt": "",
        }.items():
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.validate({**self.provenance, field: value})

    def test_unknown_fields_and_boolean_schema_fail(self) -> None:
        for change in ({"unknown": "value"}, {"schema": True}):
            with self.assertRaises(ValueError):
                self.validate({**self.provenance, **change})

    def test_source_and_compiler_versions_are_pinned(self) -> None:
        with self.assertRaisesRegex(ValueError, "toolchain"):
            self.validate(
                {
                    **self.provenance,
                    "rustc": self.provenance["rustc"].replace("1.94.1", "1.94.0"),
                }
            )
        self.install["version"] = "0.0.0"
        with self.assertRaisesRegex(ValueError, "version"):
            self.validate({**self.provenance, "version": "0.0.0"})

    def test_windows_accepts_only_pinned_lf_or_crlf_hashes(self) -> None:
        install = {**self.install, "slot": "cp310/win_amd64"}
        value = provenance_fixture(self.root, install, self.build)
        ci.validate_provenance(value, install, self.build, self.source)
        value.update(
            {
                name: hashlib.sha256(payload).hexdigest()
                for name, payload in self.source["hashes"].items()
            }
        )
        ci.validate_provenance(value, install, self.build, self.source)
        with self.assertRaisesRegex(ValueError, "pinned source"):
            ci.validate_provenance(
                {**value, "cargo_lock_sha256": "d" * 64},
                install,
                self.build,
                self.source,
            )

    def test_cargo_missing_identities_edges_and_features_fail(self) -> None:
        for mutation in (
            "package",
            "pin",
            "source",
            "node",
            "edge",
            "features",
            "extension",
        ):
            value = copy.deepcopy(self.provenance)
            metadata = value["cargo_metadata"]
            node = next(
                item
                for item in metadata["resolve"]["nodes"]
                if item["id"] == metadata["resolve"]["root"]
            )
            if mutation == "package":
                metadata["packages"].pop()
            elif mutation == "pin":
                metadata["packages"][0]["version"] = "0.0.0"
            elif mutation == "source":
                metadata["packages"][0]["source"] = "git+wrong"
            elif mutation == "node":
                metadata["resolve"]["nodes"].pop()
            elif mutation == "edge":
                node["dependencies"] = []
                node["deps"] = []
            elif mutation == "features":
                for item in metadata["resolve"]["nodes"]:
                    item["features"] = []
            else:
                node["features"].remove("extension")
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.validate(value)

    def test_opaque_path_ids_are_not_compared_with_checkout_paths(self) -> None:
        value = copy.deepcopy(self.provenance)
        metadata = value["cargo_metadata"]
        mapping = {
            item["id"]: f"path+file:///D:/foreign/source/{index}"
            for index, item in enumerate(metadata["packages"])
        }
        for package in metadata["packages"]:
            package["id"] = mapping[package["id"]]
        for node in metadata["resolve"]["nodes"]:
            node["id"] = mapping[node["id"]]
            node["dependencies"] = [mapping[item] for item in node["dependencies"]]
            for edge in node["deps"]:
                edge["pkg"] = mapping[edge["pkg"]]
        metadata["resolve"]["root"] = mapping[metadata["resolve"]["root"]]
        metadata["workspace_members"] = [metadata["resolve"]["root"]]
        self.validate(value)

    def test_unrequested_direct_dependency_features_fail(self) -> None:
        value = copy.deepcopy(self.provenance)
        metadata = value["cargo_metadata"]
        package = next(item for item in metadata["packages"] if item["name"] == "pyo3")
        package["features"]["abi3"] = []
        node = next(
            item for item in metadata["resolve"]["nodes"] if item["id"] == package["id"]
        )
        node["features"].append("abi3")
        with self.assertRaisesRegex(ValueError, "features differ"):
            self.validate(value)

    def test_native_declared_pins_cannot_contradict_source(self) -> None:
        value = copy.deepcopy(self.provenance)
        metadata = value["cargo_metadata"]
        root = next(
            item
            for item in metadata["packages"]
            if item["id"] == metadata["resolve"]["root"]
        )
        root["dependencies"][0]["req"] = "*"
        with self.assertRaisesRegex(ValueError, "declarations differ"):
            self.validate(value)

    def test_installed_interpreter_and_distribution_evidence_fail_closed(self) -> None:
        for mutation in (
            "python",
            "free-threaded",
            "base",
            "native",
            "dependency",
            "duplicate",
            "empty",
        ):
            value = copy.deepcopy(self.provenance)
            environment = value["installed_environment"]
            if mutation == "python":
                environment["python"] = "3.11.0"
            elif mutation == "free-threaded":
                environment["python"] = "3.10.0 experimental free-threading build"
            elif mutation == "empty":
                environment["distributions"] = []
            elif mutation == "duplicate":
                environment["distributions"].append(
                    {"name": "alternator_client", "version": "2.0.1"}
                )
            else:
                environment["distributions"].pop(
                    {"base": 0, "native": 1, "dependency": 2}[mutation]
                )
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.validate(value)

    def test_execution_identity_must_match_the_current_workflow_when_present(
        self,
    ) -> None:
        with (
            patch.dict(os.environ, {"GITHUB_REPOSITORY": "different/repository"}),
            self.assertRaisesRegex(ValueError, "current build"),
        ):
            self.validate(self.provenance)

    def test_dependency_versions_must_satisfy_source_requirements(self) -> None:
        for version in ("", "not-a-version", "0.1.0"):
            value = copy.deepcopy(self.provenance)
            value["installed_environment"]["distributions"][2]["version"] = version
            with self.subTest(version=version), self.assertRaises(ValueError):
                self.validate(value)

    def test_successful_prior_attempt_receipt_remains_valid_on_failed_job_rerun(
        self,
    ) -> None:
        with patch.dict(os.environ, {"GITHUB_RUN_ATTEMPT": "2"}):
            self.validate({**self.provenance, "run_attempt": "1"})


if __name__ == "__main__":
    unittest.main()
