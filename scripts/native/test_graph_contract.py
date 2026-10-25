# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Full SDK graph, optional-branch and source-binding rejection controls."""

from __future__ import annotations

import copy
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import ci
import graph_contract

ROOT = Path(__file__).resolve().parents[2]
DECLARATION_FIELDS = (
    "name",
    "req",
    "source",
    "optional",
    "uses_default_features",
    "features",
    "rename",
    "kind",
    "target",
    "registry",
)


def metadata_fixture(graph: dict) -> dict:
    ids = {
        tuple(package["identity"]): f"opaque-other-runner-{index}"
        for index, package in enumerate(graph["packages"])
    }
    packages, nodes = [], []
    for package in graph["packages"]:
        key = tuple(package["identity"])
        packages.append(
            {
                "id": ids[key],
                "name": key[0],
                "version": key[1],
                "source": key[2],
                "features": copy.deepcopy(package["definitions"]),
                "dependencies": [
                    dict(zip(DECLARATION_FIELDS, declaration, strict=True))
                    for declaration in copy.deepcopy(package["declarations"])
                ],
            }
        )
        nodes.append(
            {
                "id": ids[key],
                "features": package["features"].copy(),
                "dependencies": [ids[tuple(edge)] for edge in package["edges"]],
                "deps": [
                    {
                        "name": name,
                        "pkg": ids[tuple(edge)],
                        "dep_kinds": [
                            {"kind": kind, "target": target} for kind, target in kinds
                        ],
                    }
                    for name, edge, kinds in package["links"]
                ],
            }
        )
    root = ids[tuple(graph["root"])]
    return {
        "version": 1,
        "packages": packages,
        "resolve": {"root": root, "nodes": nodes},
        "workspace_members": [root],
    }


class FullGraphTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = ci.source_provenance(ROOT)
        cls.contract = json.loads((ROOT / "native/cargo-provenance.json").read_text())
        cls.metadata = metadata_fixture(cls.source["graph_contract"])

    def source_copy(self) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / "native").mkdir()
        for name in (
            "Cargo.toml",
            "Cargo.lock",
            "pyproject.toml",
            "rust-toolchain.toml",
            "cargo-provenance.json",
        ):
            shutil.copyfile(ROOT / "native" / name, root / "native" / name)
        shutil.copyfile(ROOT / "pyproject.toml", root / "pyproject.toml")
        return root

    def test_complete_portable_sdk_union_matches_source(self) -> None:
        portable = ci.cargo_provenance(self.metadata, self.source)
        self.assertEqual(len(portable), len(self.contract["graph"]["packages"]))
        self.assertIn(
            "full",
            next(features for key, features in portable.items() if key[0] == "tokio"),
        )
        self.assertIn(
            "http-auth",
            next(
                features
                for key, features in portable.items()
                if key[0] == "aws-smithy-runtime-api"
            ),
        )
        self.assertIn(
            "default-https-client",
            next(
                features
                for key, features in portable.items()
                if key[0] == "aws-sdk-dynamodb"
            ),
        )

    def test_coherent_identity_edge_feature_and_manifest_forgery_fails(self) -> None:
        for mutation in (
            "missing",
            "extra",
            "identity",
            "feature",
            "coherent-feature",
            "edge",
            "kind",
            "alias",
            "target",
        ):
            metadata = copy.deepcopy(self.metadata)
            root = metadata["resolve"]["root"]
            node = next(
                item for item in metadata["resolve"]["nodes"] if item["id"] == root
            )
            package = next(item for item in metadata["packages"] if item["id"] == root)
            if mutation == "missing":
                removed = metadata["packages"].pop()["id"]
                metadata["resolve"]["nodes"] = [
                    item
                    for item in metadata["resolve"]["nodes"]
                    if item["id"] != removed
                ]
                for item in metadata["resolve"]["nodes"]:
                    item["dependencies"] = [
                        value for value in item["dependencies"] if value != removed
                    ]
                    item["deps"] = [
                        value for value in item["deps"] if value["pkg"] != removed
                    ]
            elif mutation == "extra":
                metadata["packages"].append(
                    {
                        "id": "extra",
                        "name": "extra",
                        "version": "1.0.0",
                        "source": None,
                        "features": {},
                        "dependencies": [],
                    }
                )
                metadata["resolve"]["nodes"].append(
                    {"id": "extra", "features": [], "dependencies": [], "deps": []}
                )
            elif mutation == "identity":
                metadata["packages"][-1]["source"] = "git+forged"
            elif mutation in ("feature", "coherent-feature"):
                package = next(
                    item for item in metadata["packages"] if item["name"] == "tokio"
                )
                node = next(
                    item
                    for item in metadata["resolve"]["nodes"]
                    if item["id"] == package["id"]
                )
                if mutation == "feature":
                    node["features"].remove("full")
                else:
                    package["features"]["forged"] = []
                    node["features"].append("forged")
            elif mutation == "edge":
                removed = node["dependencies"].pop()
                node["deps"] = [
                    value for value in node["deps"] if value["pkg"] != removed
                ]
            elif mutation == "kind":
                next(
                    value for value in package["dependencies"] if value["kind"] == "dev"
                )["kind"] = "build"
            elif mutation == "alias":
                node["deps"][0]["name"] = "different_alias"
            else:
                node["deps"][0]["dep_kinds"][0]["target"] = "cfg(fake_target)"
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                ci.cargo_provenance(metadata, self.source)

    def test_driver_override_cannot_claim_a_different_git_identity(self) -> None:
        root = self.source_copy()
        path = root / "native/cargo-provenance.json"
        contract = json.loads(path.read_text())
        contract["driver_source"] = "git+forged#" + "f" * 40
        path.write_text(json.dumps(contract))
        with self.assertRaisesRegex(ValueError, "driver source"):
            ci.source_provenance(root)
        metadata = copy.deepcopy(self.metadata)
        next(
            package
            for package in metadata["packages"]
            if package["name"] == "alternator-client"
        )["source"] = "git+forged#" + "f" * 40
        with self.assertRaises(ValueError):
            ci.cargo_provenance(metadata, self.source)

    def test_empty_opaque_ids_are_rejected_like_the_minimal_model(self) -> None:
        metadata = copy.deepcopy(self.metadata)
        root = metadata["resolve"]["root"]
        next(package for package in metadata["packages"] if package["id"] == root)[
            "id"
        ] = ""
        next(node for node in metadata["resolve"]["nodes"] if node["id"] == root)[
            "id"
        ] = ""
        metadata["resolve"]["root"] = ""
        metadata["workspace_members"] = [""]
        with self.assertRaisesRegex(ValueError, "opaque package ID"):
            ci.cargo_provenance(metadata, self.source)

    def test_sdk_source_cannot_fall_back_when_contract_missing(self) -> None:
        root = self.source_copy()
        (root / "native/cargo-provenance.json").unlink()
        with self.assertRaisesRegex(ValueError, "source-owned Cargo contract"):
            ci.source_provenance(root)

    def test_source_or_selection_changes_require_regeneration(self) -> None:
        for name in ("Cargo.toml", "Cargo.lock"):
            root = self.source_copy()
            with (root / "native" / name).open("a") as output:
                output.write("\n# source drift\n")
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ValueError, "binding differs"),
            ):
                ci.source_provenance(root)
        root = self.source_copy()
        path = root / "native/pyproject.toml"
        path.write_text(
            path.read_text().replace('features = ["extension"]', "features = []")
        )
        with self.assertRaises(ValueError):
            ci.source_provenance(root)

    def test_python_version_stamp_preserves_graph_but_updates_raw_provenance(
        self,
    ) -> None:
        root = self.source_copy()
        path = root / "native/pyproject.toml"
        original_version = self.source["version"]
        probe_version = "2.0.2" if original_version != "2.0.2" else "2.0.3"
        path.write_text(
            path.read_text().replace(
                f'version = "{original_version}"', f'version = "{probe_version}"', 1
            )
        )
        stamped = ci.source_provenance(root)
        self.assertEqual(stamped["graph_contract"], self.source["graph_contract"])
        self.assertEqual(stamped["version"], probe_version)
        self.assertNotEqual(
            stamped["hashes"]["native_pyproject_sha256"],
            self.source["hashes"]["native_pyproject_sha256"],
        )

    def test_explicit_inactive_optional_branch_is_supported_and_required(self) -> None:
        root = self.source_copy()
        manifest = root / "native/Cargo.toml"
        manifest.write_text(
            manifest.read_text()
            + '\n[dependencies.inactive-fixture]\nversion = "=1.0.0"\noptional = true\ndefault-features = false\n'
        )
        lock = root / "native/Cargo.lock"
        text = lock.read_text().replace(
            'name = "alternator-python-native"\nversion = "0.1.0"\ndependencies = [',
            'name = "alternator-python-native"\nversion = "0.1.0"\ndependencies = [\n "inactive-fixture",',
        )
        lock.write_text(
            text
            + '\n[[package]]\nname = "inactive-fixture"\nversion = "1.0.0"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n'
        )
        source = ci.source_inputs(root)
        contract = copy.deepcopy(self.contract)
        identity = ["inactive-fixture", "1.0.0", graph_contract.REGISTRY]
        contract["locked"].append(identity)
        contract["inactive"].append(identity)
        contract["bindings"] = graph_contract.bindings(root / "native")
        import tomllib

        native_root = next(
            package
            for package in contract["graph"]["packages"]
            if tuple(package["identity"]) == source["root"]
        )
        native_root["definitions"] = source["definitions"]
        native_root["features"] = sorted(source["features"])
        native_root["declarations"] = graph_contract.root_declarations(
            tomllib.loads(manifest.read_text())
        )
        path = root / "native/cargo-provenance.json"
        path.write_text(json.dumps(contract))
        enabled = ci.source_provenance(root)
        ci.cargo_provenance(metadata_fixture(enabled["graph_contract"]), enabled)
        contract["inactive"] = []
        path.write_text(json.dumps(contract))
        with self.assertRaisesRegex(ValueError, "inactive optional"):
            ci.source_provenance(root)


if __name__ == "__main__":
    unittest.main()
