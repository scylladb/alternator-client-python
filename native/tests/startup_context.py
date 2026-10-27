# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Build and bind Linux startup-probe expectations to a fresh normal wheel.

This is test tooling, never imported by the extension. Run ``build`` before
probes; ``verify`` needs the exact wheel and installed extension, but no Rust.
Dependency features come from the one freshly compiled Tokio fingerprint,
never from a dev/all-features graph or from observed descriptor counts.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

# Independently audited ownership boundary. Any change requires renewed source
# audit: the global owns one original pair; each driver owns its receiver dup.
SIGNAL_SOURCE = {
    "src/signal/unix.rs": "8dcfb6950a39c1fd27c500670cc24e7df4c95278b6e7e52e7629b25e91fe49ea",
    "src/signal/registry.rs": "43d9f43411aeaf729d112e9f5878c8d996f316a24e296ea9858bb09905938d07",
    "src/runtime/signal/mod.rs": "20ad912680df1e8c1223ff8903f1a993a9528fe8144e94223389ee492477ea75",
    "src/runtime/driver.rs": "c97ca0ff2dd795cd77e1ce8d2d1a5603ef9a15ec60c63900cb76102bb7218ce3",
    "src/macros/cfg.rs": "e5f53e84e0538e4b7562732cbea25d79942181915114aa86bf756985d4a36448",
}

# These source contracts independently determine whether signal-internal is
# compiled. Counts cannot be changed by editing only context features/count.
# The accepted upstream manifest requests Tokio full. The accepted P2 manifest
# has neither signal nor Unix process; unknown source contracts fail closed.
UPSTREAM_FULL_MANIFEST = (
    "9b0ddba41cf7851cf16696874b4aa8dfbaa8acfeb76bafd96e3b4fff507f5228"
)
P2_MINIMAL_MANIFEST = "ffa54b07c93a6560ddf820313aad951a31fcced74675ac558efd628a87ca2c6a"


# Audited against Rust PR #158 at d2044849bd3b0c100d18541b01df4a1a1eac3139.
P4_FULL_MANIFEST = "d7c10e71f2197612bddce04236e032ee284aa78382cb452db1859ca59cb91d7b"


def source_expected_pairs(
    source: dict[str, str], upstream: dict[str, str] | None
) -> int:
    if source["Cargo.toml"] == P2_MINIMAL_MANIFEST:
        require(upstream is None, "minimal source must not claim an upstream driver")
        return 0
    require(
        source["Cargo.toml"] == P4_FULL_MANIFEST,
        "unrecognized native feature source contract; renew source audit",
    )
    require(
        upstream is not None and upstream["sha256"] == UPSTREAM_FULL_MANIFEST,
        "upstream Tokio feature source contract changed; renew source audit",
    )
    return 1


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(f"startup context: {message}")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_files(native: Path) -> dict[str, str]:
    files = [
        native / name
        for name in ("Cargo.toml", "Cargo.lock", "build.rs", "pyproject.toml")
    ]
    files.extend(sorted((native / "src").rglob("*.rs")))
    for name, checksum in SIGNAL_SOURCE.items():
        path = native / "vendor/tokio" / name
        require(
            digest(path) == checksum, f"audited signal ownership source changed: {name}"
        )
        files.append(path)
    return {path.relative_to(native).as_posix(): digest(path) for path in files}


def wheel_extension(wheel: Path) -> tuple[str, str]:
    with zipfile.ZipFile(wheel) as archive:
        members = [name for name in archive.namelist() if name.endswith(".so")]
        require(len(members) == 1, "expected exactly one Linux wheel extension")
        member = members[0]
        return member, hashlib.sha256(archive.read(member)).hexdigest()


def build(args: argparse.Namespace) -> None:
    require(sys.platform == "linux", "Linux build required")
    native = args.native.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    before = source_files(native)
    target = Path(tempfile.mkdtemp(prefix="normal-startup-build-", dir=output))
    wheels = target / "wheels"
    command = [
        shutil.which("maturin") or "maturin",
        "build",
        "--locked",
        "--release",
        "--manifest-path",
        str(native / "Cargo.toml"),
        "--target-dir",
        str(target),
        "--interpreter",
        str(args.interpreter.resolve()),
        "--out",
        str(wheels),
    ]
    # The fresh target is used only by this normal wheel command. No core/dev or
    # all-feature compilation may precede fingerprint capture in this directory.
    with (target / "build.log").open("w") as log:
        subprocess.run(
            command, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=900
        )
    require(source_files(native) == before, "source changed during normal wheel build")
    built = list(wheels.glob("*.whl"))
    require(len(built) == 1, "normal build must produce exactly one wheel")
    wheel = built[0]
    fingerprints = list(
        (target / "release/.fingerprint").glob("tokio-*/lib-tokio.json")
    )
    require(
        len(fingerprints) == 1,
        "fresh normal build must compile exactly one Tokio identity",
    )
    fingerprint = json.loads(fingerprints[0].read_text())
    features = json.loads(fingerprint["features"])
    require(
        set(features) <= set(json.loads(fingerprint["declared_features"])),
        "invalid Tokio feature fingerprint",
    )
    expected = int(bool({"signal", "process"} & set(features)))
    member, extension_hash = wheel_extension(wheel)
    require(
        digest(target / "release/lib_alternator_rust.so") == extension_hash,
        "wheel differs from freshly compiled extension",
    )
    metadata = json.loads(
        subprocess.check_output(
            [
                "cargo",
                "metadata",
                "--manifest-path",
                str(native / "Cargo.toml"),
                "--locked",
                "--format-version",
                "1",
                "--features",
                "extension",
            ],
            text=True,
        )
    )
    tokio = [package for package in metadata["packages"] if package["name"] == "tokio"]
    require(
        len(tokio) == 1
        and Path(tokio[0]["manifest_path"]).resolve()
        == native / "vendor/tokio/Cargo.toml",
        "compiled graph must use the audited Tokio source",
    )
    upstream = [
        package
        for package in metadata["packages"]
        if package["name"] == "alternator-client"
    ]
    require(len(upstream) <= 1, "ambiguous upstream driver source")
    upstream_source = None
    if upstream:
        upstream_source = {
            "manifest": upstream[0]["manifest_path"],
            "sha256": digest(Path(upstream[0]["manifest_path"])),
            "cargo_source": upstream[0]["source"],
        }
    require(
        expected == source_expected_pairs(before, upstream_source),
        "compiled signal features disagree with audited source contract",
    )
    context = {
        "schema": 1,
        "kind": "normal-linux-wheel-startup",
        "wheel": wheel.name,
        "wheel_sha256": digest(wheel),
        "extension_member": member,
        "extension_sha256": extension_hash,
        "source_files": before,
        "tokio_features": features,
        "tokio_fingerprint": fingerprint,
        "expected_signal_pairs": expected,
        "upstream_source": upstream_source,
        "build_command": command,
        "build_target": str(target),
        "rustc": subprocess.check_output(["rustc", "-vV"], text=True),
    }
    context_path = output / "startup-context.json"
    context_path.write_text(json.dumps(context, indent=2) + "\n")
    # Keep wheel and context together for transfer to compiler-free probe hosts.
    destination = output / wheel.name
    destination.write_bytes(wheel.read_bytes())
    print(
        json.dumps(
            {
                "wheel": str(destination),
                "context": str(context_path),
                "expected_signal_pairs": expected,
            }
        )
    )


def verify(args: argparse.Namespace) -> None:
    context = json.loads(args.context.read_text())
    require(
        context.get("schema") == 1
        and context.get("kind") == "normal-linux-wheel-startup",
        "unrecognized build context",
    )
    require(
        context["source_files"] == source_files(args.native.resolve()),
        "context source differs from current source",
    )
    require(digest(args.wheel) == context["wheel_sha256"], "exact wheel hash differs")
    member, extension_hash = wheel_extension(args.wheel)
    require(
        (member, extension_hash)
        == (context["extension_member"], context["extension_sha256"]),
        "wheel extension binding differs",
    )
    fingerprint = context["tokio_fingerprint"]
    features = json.loads(fingerprint["features"])
    require(features == context["tokio_features"], "compiled feature proof differs")
    expected = int(bool({"signal", "process"} & set(features)))
    require(
        expected
        == source_expected_pairs(context["source_files"], context["upstream_source"]),
        "compiled signal features disagree with audited source contract",
    )
    require(
        context["expected_signal_pairs"] == expected,
        "expected pair count differs from compiled features",
    )
    require(
        "--all-features" not in context["build_command"]
        and "--target-dir" in context["build_command"],
        "normal isolated build command required",
    )
    native = importlib.import_module("_alternator_rust")
    extension = (
        importlib.import_module("_alternator_rust._alternator_rust")
        if hasattr(native, "__path__")
        else native
    )
    require(
        extension.__file__ is not None
        and digest(Path(extension.__file__)) == extension_hash,
        "installed extension differs from exact context wheel",
    )
    print(
        json.dumps(
            {
                "expected_signal_pairs": expected,
                "extension_sha256": extension_hash,
                "wheel_sha256": context["wheel_sha256"],
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    normal = commands.add_parser("build")
    normal.add_argument(
        "--native", type=Path, default=Path(__file__).resolve().parents[1]
    )
    normal.add_argument("--output", type=Path, required=True)
    normal.add_argument("--interpreter", type=Path, default=Path(sys.executable))
    check = commands.add_parser("verify")
    check.add_argument(
        "--native", type=Path, default=Path(__file__).resolve().parents[1]
    )
    check.add_argument("--context", type=Path, required=True)
    check.add_argument("--wheel", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "build":
        build(args)
    else:
        verify(args)


if __name__ == "__main__":
    main()
