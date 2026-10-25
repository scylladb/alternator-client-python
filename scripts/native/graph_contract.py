#!/usr/bin/env python3
# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Capture/recheck the reviewed portable Cargo graph for a full SDK source.

Capture runs locked Cargo itself; received provenance is never an expectation
source. Aggregation loads the committed contract without Cargo or network.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(f"Invalid Cargo source graph: {message}")


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def bindings(native: Path) -> dict[str, Any]:
    import tomllib

    project = tomllib.loads((native / "pyproject.toml").read_text())
    options = project["tool"]["maturin"]
    require(
        not options.get("all-features", False)
        and not options.get("no-default-features", False),
        "unsupported maturin feature selection",
    )
    return {
        **{
            name: hashlib.sha256(
                (native / name).read_bytes().replace(b"\r\n", b"\n")
            ).hexdigest()
            for name in ("Cargo.toml", "Cargo.lock")
        },
        "maturin_selection": {
            "features": sorted(options["features"]),
            "no_default_features": False,
            "all_features": False,
        },
    }


def identity(package: dict[str, Any]) -> tuple[str, str, str | None]:
    require(
        isinstance(package.get("name"), str)
        and isinstance(package.get("version"), str)
        and (package.get("source") is None or isinstance(package["source"], str)),
        "malformed package identity",
    )
    return package["name"], package["version"], package.get("source")


def declaration(value: dict[str, Any]) -> list[Any]:
    names = (
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
    require(all(name in value for name in names), "incomplete dependency declaration")
    require(
        isinstance(value["name"], str)
        and isinstance(value["req"], str)
        and type(value["optional"]) is bool
        and type(value["uses_default_features"]) is bool
        and isinstance(value["features"], list)
        and all(isinstance(name, str) for name in value["features"]),
        "malformed dependency declaration",
    )
    require(
        value["kind"] in (None, "dev", "build")
        and all(
            value[name] is None or isinstance(value[name], str)
            for name in ("source", "rename", "target", "registry")
        ),
        "malformed dependency source/kind/target",
    )
    return [
        sorted(value[name]) if name == "features" else value[name] for name in names
    ]


def root_declarations(cargo: dict[str, Any]) -> list[list[Any]]:
    result = []
    sections = [(None, cargo), *cargo.get("target", {}).items()]
    for target, section in sections:
        for kind, table in (
            (None, "dependencies"),
            ("dev", "dev-dependencies"),
            ("build", "build-dependencies"),
        ):
            for alias, raw in section.get(table, {}).items():
                value = raw if isinstance(raw, dict) else {"version": raw}
                require(
                    not value.get("workspace") and not value.get("registry"),
                    "unsupported inherited/named registry declaration",
                )
                source = REGISTRY
                if "path" in value:
                    source = None
                elif "git" in value:
                    query = {
                        name: value[name]
                        for name in ("branch", "tag", "rev")
                        if name in value
                    }
                    source = (
                        "git+"
                        + value["git"]
                        + ("?" + urlencode(query) if query else "")
                    )
                req = value.get("version", "*")
                if req[0].isdigit():
                    req = "^" + req
                result.append(
                    declaration(
                        {
                            "name": value.get("package", alias),
                            "req": req,
                            "source": source,
                            "optional": value.get("optional", False),
                            "uses_default_features": value.get(
                                "default-features", True
                            ),
                            "features": value.get("features", []),
                            "rename": alias if "package" in value else None,
                            "kind": kind,
                            "target": target,
                            "registry": None,
                        }
                    )
                )
    return sorted(result, key=canonical)


def normalize(metadata: object) -> dict[str, Any]:
    require(
        isinstance(metadata, dict)
        and type(metadata.get("version")) is int
        and metadata["version"] == 1,
        "Cargo format-version1 required",
    )
    packages = metadata.get("packages")
    resolve = metadata.get("resolve")
    require(
        isinstance(packages, list) and bool(packages) and isinstance(resolve, dict),
        "complete packages/resolve required",
    )
    by_id = {}
    for package in packages:
        require(
            isinstance(package, dict)
            and isinstance(package.get("id"), str)
            and bool(package["id"])
            and package["id"] not in by_id,
            "duplicate/missing opaque package ID",
        )
        identity(package)
        by_id[package["id"]] = package
    root = resolve.get("root")
    require(
        root in by_id and metadata.get("workspace_members") == [root],
        "one native root/workspace required",
    )
    nodes = resolve.get("nodes")
    require(
        isinstance(nodes, list) and len(nodes) == len(by_id),
        "complete active graph nodes required",
    )
    normalized = {}
    for node in nodes:
        require(
            isinstance(node, dict) and node.get("id") in by_id, "unknown graph node"
        )
        package = by_id[node["id"]]
        key = identity(package)
        require(key not in normalized, "duplicate active package identity")
        features, definitions = node.get("features"), package.get("features")
        require(
            isinstance(definitions, dict)
            and all(
                isinstance(name, str)
                and isinstance(values, list)
                and all(isinstance(value, str) for value in values)
                for name, values in definitions.items()
            ),
            "malformed feature definitions",
        )
        require(
            isinstance(features, list)
            and all(isinstance(name, str) and name in definitions for name in features)
            and len(set(features)) == len(features),
            "malformed resolved feature union",
        )
        dependencies, links = node.get("dependencies"), node.get("deps")
        require(
            isinstance(dependencies, list)
            and all(isinstance(item, str) and item in by_id for item in dependencies)
            and len(set(dependencies)) == len(dependencies),
            "missing/dangling graph edge",
        )
        require(
            isinstance(links, list)
            and all(
                isinstance(link, dict)
                and link.get("pkg") in by_id
                and isinstance(link.get("name"), str)
                and isinstance(link.get("dep_kinds"), list)
                for link in links
            ),
            "missing dependency aliases/kinds",
        )
        require(
            {link["pkg"] for link in links} == set(dependencies),
            "contradictory dependency edges",
        )
        portable_links = []
        for link in links:
            kinds = link["dep_kinds"]
            require(
                all(
                    isinstance(item, dict)
                    and item.get("kind") in (None, "dev", "build")
                    and (item.get("target") is None or isinstance(item["target"], str))
                    and set(item) == {"kind", "target"}
                    for item in kinds
                ),
                "malformed edge kind/target",
            )
            portable_links.append(
                [
                    link["name"],
                    list(identity(by_id[link["pkg"]])),
                    sorted(
                        [[item["kind"], item["target"]] for item in kinds],
                        key=canonical,
                    ),
                ]
            )
        declared = package.get("dependencies")
        require(isinstance(declared, list), "package dependency declarations required")
        normalized[key] = {
            "identity": list(key),
            "definitions": definitions,
            "declarations": sorted(
                [declaration(item) for item in declared], key=canonical
            ),
            "features": sorted(features),
            "edges": sorted(
                [list(identity(by_id[item])) for item in dependencies], key=canonical
            ),
            "links": sorted(portable_links, key=canonical),
        }
    return {
        "root": list(identity(by_id[root])),
        "packages": sorted(
            normalized.values(), key=lambda item: canonical(item["identity"])
        ),
    }


def check(
    contract: dict[str, Any], source: dict[str, Any], native: Path
) -> dict[str, Any]:
    require(
        set(contract)
        == {
            "schema",
            "command",
            "bindings",
            "selected_features",
            "locked",
            "inactive",
            "graph",
            "driver_source",
        }
        and type(contract["schema"]) is int
        and contract["schema"] == 1,
        "unrecognized contract schema",
    )
    require(
        contract["command"]
        == [
            "cargo",
            "metadata",
            "--locked",
            "--features",
            "extension",
            "--format-version",
            "1",
        ],
        "unfiltered locked extension capture command required",
    )
    require(
        contract["bindings"] == bindings(native),
        "contract source manifest/lock/project binding differs",
    )
    require(
        contract["selected_features"] == sorted(source["features"]),
        "contract root selection differs from source",
    )
    locked = {tuple(item) for item in contract["locked"]}
    require(
        locked == source["identities"] and len(locked) == len(contract["locked"]),
        "contract locked identities differ",
    )
    graph = contract["graph"]
    require(
        set(graph) == {"root", "packages"} and tuple(graph["root"]) == source["root"],
        "contract root differs",
    )
    packages = {tuple(item["identity"]): item for item in graph["packages"]}
    require(
        len(packages) == len(graph["packages"])
        and set(packages) <= locked
        and source["root"] in packages,
        "contract active identities differ",
    )
    inactive = {tuple(item) for item in contract["inactive"]}
    require(
        inactive == locked - packages.keys()
        and len(inactive) == len(contract["inactive"]),
        "contract inactive optional branches differ",
    )
    for key, package in packages.items():
        require(
            {tuple(edge) for edge in package["edges"]} <= source["edges"][key]
            and {tuple(edge) for edge in package["edges"]} <= packages.keys(),
            "contract edges differ from lock/active graph",
        )
    import tomllib

    cargo = tomllib.loads((native / "Cargo.toml").read_text())
    root = packages[source["root"]]
    require(
        root["definitions"] == source["definitions"]
        and root["features"] == sorted(source["features"])
        and root["declarations"] == root_declarations(cargo),
        "contract native declarations/features differ from source",
    )
    driver = [key for key in packages if key[0] == "alternator-client"]
    expected_driver = (
        "absent"
        if not driver
        else "provisional-local-override"
        if driver[0][2] is None
        else driver[0][2]
    )
    require(
        len(driver) <= 1 and contract["driver_source"] == expected_driver,
        "driver source identity/status differs",
    )
    return graph


def capture(root: Path, output: Path) -> None:
    import ci

    native = root / "native"
    before = bindings(native)
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--features",
        "extension",
        "--format-version",
        "1",
    ]
    metadata = json.loads(subprocess.check_output(command, cwd=native, text=True))
    require(bindings(native) == before, "source changed during capture")
    source = ci.source_inputs(root)
    graph = normalize(metadata)
    active = {tuple(item["identity"]) for item in graph["packages"]}
    driver = [key for key in active if key[0] == "alternator-client"]
    contract = {
        "schema": 1,
        "command": command,
        "bindings": before,
        "selected_features": sorted(source["features"]),
        "locked": sorted([list(item) for item in source["identities"]], key=canonical),
        "inactive": sorted(
            [list(item) for item in source["identities"] - active], key=canonical
        ),
        "graph": graph,
        "driver_source": "absent"
        if not driver
        else "provisional-local-override"
        if driver[0][2] is None
        else driver[0][2],
    }
    check(contract, source, native)
    output.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "path": str(output),
                "bytes": output.stat().st_size,
                "active": len(active),
                "inactive": len(contract["inactive"]),
                "driver_source": contract["driver_source"],
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture(args.root.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
