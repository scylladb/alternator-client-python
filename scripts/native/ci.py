#!/usr/bin/env python3
# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Build evidence and isolated interface-wheel qualification.

This stage intentionally cannot satisfy coordinated release installation evidence:
native requests, both extras and per-client cleanup belong to P10 qualification.
Build/install commands support Python 3.10+; source-TOML aggregation uses 3.11+.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import venv
import zipfile
from email.parser import BytesParser
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

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
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
TARGETS = {
    "manylinux_2_28_x86_64": ("x86_64-unknown-linux-gnu", "Linux", "X64"),
    "macos_x86_64": ("x86_64-apple-darwin", "macOS", "X64"),
    "macos_arm64": ("aarch64-apple-darwin", "macOS", "ARM64"),
    "win_amd64": ("x86_64-pc-windows-msvc", "Windows", "X64"),
}
PROVENANCE_FIELDS = frozenset(
    {
        "schema",
        "stage",
        "build",
        "slot",
        "target",
        "version",
        "rustc",
        "maturin",
        "cargo_lock_sha256",
        "native_pyproject_sha256",
        "base_sha256",
        "installed_environment",
        "repository",
        "workflow_ref",
        "run_attempt",
        "runner_os",
        "runner_arch",
    }
)
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
CORE_SUITES = {
    "vendor": ("test_verify_vendor.py", 45),
    "interface": ("test_extension.py", 4),
    "startup": ("test_startup_cleanup.py", 4),
}
STARTUP_CASES = frozenset(
    {
        "test_installed_wheel_failed_startup_cleanup[first]",
        "test_installed_wheel_failed_startup_cleanup[middle]",
        "test_installed_wheel_failed_startup_cleanup[last]",
        "test_core_cleanup_and_subsequent_runtime",
    }
)
INTERFACE_CASES = frozenset(
    {
        "test_wheel_import_and_lazy_runtime",
        "test_sync_callers_share_runtime",
        "test_async_and_sync_share_runtime_across_loops",
        "test_async_bridge_requires_a_running_loop",
    }
)
VENDOR_CASES = frozenset(
    f"{name}[{mode}{'-' + parameter if parameter else ''}]"
    for mode in ("normal", "flag", "environment")
    for name, parameters in {
        "test_clean_source_verifies": ("",),
        "test_changed_vendor_bytes_fail": ("LICENSE", "src/runtime/builder.rs"),
        "test_extra_vendor_file_fails": ("",),
        "test_changed_patch_fails": ("",),
        "test_changed_trusted_checksum_fails": ("archive_sha256", "patch_sha256"),
        "test_changed_archive_fails": ("",),
        "test_cargo_identity_is_enforced": (
            "clean",
            "missing",
            "duplicate",
            "path",
            "version",
        ),
        "test_source_and_regenerated_manifest_drift_fail": (
            "src/runtime/blocking/pool.rs",
            "src/runtime/builder.rs",
        ),
    }.items()
    for parameter in parameters
)


def pytest_report(path: Path, suite: str | None = None) -> dict[str, Any]:
    """Reject skipped or missing native probes even when pytest exits zero."""
    root = ElementTree.parse(path).getroot()
    cases = list(root.iter("testcase"))
    if suite is not None:
        module = Path(CORE_SUITES[suite][0]).stem
        cases = [
            case
            for case in cases
            if case.attrib.get("classname", "").rsplit(".", 1)[-1] == module
        ]
    names = [
        case.attrib["name"]
        if suite
        else f"{case.attrib['classname']}.{case.attrib['name']}"
        for case in cases
    ]
    if (
        (
            len(cases) < CORE_SUITES[suite][1]
            if suite
            else len(cases) < sum(count for _, count in CORE_SUITES.values())
        )
        or len(set(names)) != len(names)
        or any(list(root.iter(tag)) for tag in ("skipped", "failure", "error"))
    ):
        raise ValueError(f"Missing, skipped or unsuccessful {suite} probes")
    if suite == "startup" and not STARTUP_CASES.issubset(names):
        raise ValueError("Required first/middle/last/core startup probes did not run")
    if suite == "interface" and not INTERFACE_CASES.issubset(names):
        raise ValueError("Required installed-wheel interface probes did not run")
    if suite == "vendor" and not VENDOR_CASES.issubset(names):
        raise ValueError(
            "Required vendor probes must cover all three optimization modes"
        )
    return {"passed": len(cases), "names": sorted(names), "sha256": digest(path)}


def require_core_tools() -> dict[str, str]:
    if sys.platform != "linux":
        raise ValueError("Startup fault qualification requires Linux")
    tools = {tool: shutil.which(tool) for tool in ("cc", "cargo", "rustc")}
    if any(value is None for value in tools.values()):
        raise ValueError("Startup fault qualification requires cc, cargo and rustc")
    if not Path("/proc/self/task").is_dir() or not Path("/proc/self/fd").is_dir():
        raise ValueError("Startup fault qualification requires Linux /proc probes")
    return {tool: str(value) for tool, value in tools.items()}


CORE_IMPORT_CHECK = (
    "import importlib.metadata, pathlib, sys, alternator, _alternator_rust, tests; "
    "prefix = pathlib.Path(sys.prefix).resolve(); "
    "assert all(pathlib.Path(module.__file__).resolve().is_relative_to(prefix) "
    "for module in (alternator, _alternator_rust, tests)); "
    "assert importlib.metadata.version('alternator-client') == sys.argv[1]; "
    "assert importlib.metadata.version('alternator-client-rust-backend') == sys.argv[1]"
)


def stage_core_fixtures(root: Path, site: Path) -> None:
    """Copy tracked fixtures and Python test helpers, never checkout packages."""
    tracked = git(root, "ls-files", "-z", "--", "tests", "native/tests").split("\0")
    fixture_roots = set()
    for filename in filter(None, tracked):
        source = Path(filename)
        if source.is_relative_to("tests"):
            relative = source
        else:
            relative = source.relative_to("native/tests")
            if (
                relative.suffix != ".py"
                or relative.name.startswith("test_")
                or relative.name == "conftest.py"
            ):
                continue
        top = relative.parts[0].removesuffix(".py")
        if top not in fixture_roots:
            if top in {"alternator", "_alternator_rust"} or any(
                (site / name).exists() for name in (top, top + ".py")
            ):
                raise ValueError("Core fixtures must not shadow installed packages")
            fixture_roots.add(top)
        target = site / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if (root / source).is_symlink():
            raise ValueError("Core fixtures must be tracked regular files")
        shutil.copyfile(root / source, target)


def prepare_core_environment(
    root: Path, executable: Path, wheel: Path, base: Path, version: str, workspace: Path
) -> None:
    """Install production wheels and fixture-only support for the complete suite."""
    subprocess.run(
        [
            str(executable),
            "-I",
            "-m",
            "pip",
            "install",
            "--only-binary=:all:",
            "--no-cache-dir",
            "pytest==8.4.2",
            "pytest-asyncio==1.2.0",
            str(base.resolve()) + "[async]",
            str(wheel),
        ],
        cwd=workspace,
        check=True,
        timeout=300,
    )
    result = subprocess.run(
        [
            str(executable),
            "-I",
            "-c",
            "import sysconfig; print(sysconfig.get_path('purelib'))",
        ],
        cwd=workspace,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    site = Path(result.stdout.strip())
    if not site.is_relative_to(executable.parent.parent):
        raise ValueError(
            "Core fixture destination must belong to the fresh environment"
        )
    stage_core_fixtures(root, site)
    subprocess.run(
        [str(executable), "-I", "-c", CORE_IMPORT_CHECK, version],
        cwd=workspace,
        check=True,
        timeout=20,
    )


def core_test_command(
    executable: Path, version: str, tests: Path, report: Path
) -> list[str]:
    """Check installed origins in the same isolated process that imports tests."""
    return [
        str(executable),
        "-I",
        "-c",
        CORE_IMPORT_CHECK + "; import pytest; sys.exit(pytest.main(sys.argv[2:]))",
        version,
        "-q",
        "-o",
        "addopts=",
        "--import-mode=importlib",
        "--confcutdir",
        str(tests),
        "--junitxml",
        str(report),
        str(tests),
    ]


def core_checks(args: argparse.Namespace) -> None:
    """Qualify current source/wheel with required developer tools, without skips."""
    root = args.root.resolve()
    check_source(root, args.commit)
    tools = require_core_tools()
    verifier = subprocess.run(
        [sys.executable, str(root / "native/scripts/verify_vendor.py"), "--cargo"],
        cwd=root / "native",
        check=True,
        capture_output=True,
        text=True,
        timeout=90,
    )
    verified = json.loads(verifier.stdout)
    if verified.get("verified_files") != 581 or verified.get("one_tokio") is not True:
        raise ValueError("Audited vendor and one-Tokio verification did not pass")
    wheels = list(args.wheels.glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("Expected exactly one current native core wheel")
    wheel = wheels[0].resolve()
    bases = list(args.base.glob("*.whl"))
    if len(bases) != 1:
        raise ValueError("Expected exactly one current base wheel for core probes")
    base = bases[0].resolve()
    version = re.search(
        r'^version = "([^"]+)"$',
        (root / "native/pyproject.toml").read_text(),
        re.MULTILINE,
    )
    if version is None:
        raise ValueError("Missing static native version")
    args.output.mkdir(parents=True, exist_ok=True)
    output = args.output.resolve()
    with tempfile.TemporaryDirectory(prefix="alternator-core-") as directory:
        workspace = Path(directory).resolve()
        if workspace.is_relative_to(root):
            raise ValueError("Core wheel probes must run outside checkout")
        environment = workspace / "env"
        venv.EnvBuilder(with_pip=True).create(environment)
        executable = environment / "bin/python"
        # These developer-only probes compile a child-local interposer and Rust
        # executable. The separate 20 install cells still remove Rust from PATH.
        prepare_core_environment(root, executable, wheel, base, version[1], workspace)
        report = output / "native.xml"
        subprocess.run(
            core_test_command(executable, version[1], root / "native/tests", report),
            env={
                **os.environ,
                "ALTERNATOR_STARTUP_CONTEXT": str(args.startup_context.resolve()),
                "ALTERNATOR_STARTUP_WHEEL": str(wheel),
            },
            cwd=workspace,
            check=True,
            timeout=300,
        )
        native_tests = pytest_report(report)
        reports = {suite: pytest_report(report, suite) for suite in CORE_SUITES}
    check_source(root, args.commit)
    receipt = {
        "schema": 1,
        "stage": "interface",
        "commit": args.commit,
        "run_id": args.run_id,
        "wheel": {"filename": wheel.name, "sha256": digest(wheel)},
        "vendor": verified,
        "vendor_provenance_sha256": digest(
            root / "native/vendor/tokio-provenance.json"
        ),
        "tools": tools,
        "tests": reports,
        "native_tests": native_tests,
        "startup_context_sha256": digest(args.startup_context),
    }
    shutil.copyfile(wheel, output / wheel.name)
    shutil.copyfile(args.startup_context, output / "startup-context.json")
    (output / "core.json").write_bytes(canonical(receipt))


def verify_core_evidence(
    directory: Path, commit: str, run_id: str, root: Path
) -> dict[str, Any]:
    receipt = json.loads((directory / "core.json").read_bytes())
    if (
        receipt.get("schema") != 1
        or receipt.get("stage") != "interface"
        or (receipt.get("commit"), receipt.get("run_id")) != (commit, run_id)
        or receipt.get("vendor", {}).get("verified_files") != 581
        or receipt["vendor"].get("one_tokio") is not True
        or set(receipt.get("tests", {})) != set(CORE_SUITES)
    ):
        raise ValueError("Core evidence does not qualify this source/run")
    if receipt.get("vendor_provenance_sha256") != digest(
        root / "native/vendor/tokio-provenance.json"
    ):
        raise ValueError("Core vendor provenance differs from this source")
    wheel = receipt["wheel"]
    if (
        Path(wheel["filename"]).name != wheel["filename"]
        or digest(directory / wheel["filename"]) != wheel["sha256"]
    ):
        raise ValueError("Core wheel hash mismatch")
    if digest(directory / "startup-context.json") != receipt.get(
        "startup_context_sha256"
    ):
        raise ValueError("Core startup context proof mismatch")
    context = json.loads((directory / "startup-context.json").read_bytes())
    if context.get("wheel_sha256") != wheel["sha256"]:
        raise ValueError("Core startup context wheel binding mismatch")
    source_files = context.get("source_files")
    if (
        context.get("schema") != 1
        or context.get("kind") != "normal-linux-wheel-startup"
        or not isinstance(source_files, dict)
        or not {"Cargo.toml", "Cargo.lock", "build.rs", "pyproject.toml"}.issubset(
            source_files
        )
    ):
        raise ValueError("Core startup context source proof missing")
    for name, checksum in source_files.items():
        path = Path(name)
        if (
            path.is_absolute()
            or ".." in path.parts
            or digest(root / "native" / path) != checksum
        ):
            raise ValueError("Core startup context source binding mismatch")
    for suite in CORE_SUITES:
        if pytest_report(directory / "native.xml", suite) != receipt["tests"][suite]:
            raise ValueError("Core report proof mismatch")
    if pytest_report(directory / "native.xml") != receipt.get("native_tests"):
        raise ValueError("Complete native report proof mismatch")
    return receipt


def canonical(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(f"Invalid provenance: {message}")


def source_provenance(root: Path) -> dict[str, Any]:
    """Read build pins; Cargo owns dependency and feature resolution."""
    import tomllib

    native = root / "native"
    project = tomllib.loads((native / "pyproject.toml").read_text())
    toolchain = tomllib.loads((native / "rust-toolchain.toml").read_text())
    maturin = [
        value.removeprefix("maturin==")
        for value in project["build-system"]["requires"]
        if value.startswith("maturin==")
    ]
    require(len(maturin) == 1, "maturin must be pinned in source")
    base_dependencies = {}
    for value in tomllib.loads((root / "pyproject.toml").read_text())["project"][
        "dependencies"
    ]:
        requirement = re.fullmatch(r"([\w.-]+)>=(\d+(?:\.\d+)*)", value)
        require(requirement is not None, "unsupported base dependency requirement")
        base_dependencies[requirement[1].lower().replace("_", "-")] = tuple(
            int(part) for part in requirement[2].split(".")
        )
    return {
        "version": project["project"]["version"],
        "rust": toolchain["toolchain"]["channel"],
        "maturin": f"maturin {maturin[0]}",
        "base_dependencies": base_dependencies,
        "hashes": {
            name: (native / filename).read_bytes().replace(b"\r\n", b"\n")
            for name, filename in (
                ("cargo_lock_sha256", "Cargo.lock"),
                ("native_pyproject_sha256", "pyproject.toml"),
            )
        },
    }


def validate_provenance(
    provenance: object,
    install: dict[str, Any],
    build: dict[str, str],
    source: dict[str, Any],
) -> tuple[str, str]:
    require(
        isinstance(provenance, dict) and set(provenance) == PROVENANCE_FIELDS,
        "missing or unknown provenance fields",
    )
    require(
        type(provenance["schema"]) is int
        and provenance["schema"] == 2
        and provenance["stage"] == "interface"
        and provenance["build"] == build
        and provenance["slot"] == install["slot"],
        "schema/build/cell mismatch",
    )
    python, platform = install["slot"].split("/")
    target, runner_os, runner_arch = TARGETS[platform]
    require(
        (provenance["target"], provenance["runner_os"], provenance["runner_arch"])
        == (target, runner_os, runner_arch),
        "target/runner differs from cell",
    )
    require(
        provenance["version"] == install["version"] == source["version"],
        "release version differs from source",
    )
    require(
        isinstance(install.get("base_sha256"), str)
        and DIGEST.fullmatch(install["base_sha256"]) is not None
        and provenance["base_sha256"] == install["base_sha256"],
        "base wheel hash differs",
    )
    for name, payload in source["hashes"].items():
        allowed = {hashlib.sha256(payload).hexdigest()}
        if platform == "win_amd64":
            allowed.add(hashlib.sha256(payload.replace(b"\n", b"\r\n")).hexdigest())
        require(
            isinstance(provenance[name], str) and provenance[name] in allowed,
            f"{name} differs from pinned source",
        )
    rustc = provenance["rustc"]
    require(
        isinstance(rustc, str)
        and rustc.startswith(f"rustc {source['rust']} ")
        and f"\nrelease: {source['rust']}\n" in rustc
        and f"\nhost: {target}\n" in rustc
        and re.search(r"^commit-hash: [0-9a-f]{40}$", rustc, re.MULTILINE) is not None,
        "missing or contradictory Rust toolchain",
    )
    require(
        provenance["maturin"] == source["maturin"],
        "maturin version differs from source",
    )
    environment = provenance["installed_environment"]
    require(
        isinstance(environment, dict)
        and set(environment) == {"python", "distributions"},
        "missing installed environment",
    )
    require(
        isinstance(environment["python"], str)
        and "free-thread" not in environment["python"].lower()
        and re.match(rf"3\.{python[3:]}\.\d+(?:\s|$)", environment["python"])
        is not None,
        "installed interpreter differs from cell",
    )
    distributions = environment["distributions"]
    require(
        isinstance(distributions, list) and bool(distributions),
        "missing installed distributions",
    )
    installed = {}
    for item in distributions:
        require(
            isinstance(item, dict)
            and set(item) == {"name", "version"}
            and all(isinstance(item[key], str) and item[key].strip() for key in item),
            "incomplete installed distribution",
        )
        require(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", item["name"]) is not None
            and item["version"] == item["version"].strip(),
            "malformed installed distribution",
        )
        name = re.sub(r"[-_.]+", "-", item["name"]).lower()
        require(name not in installed, "duplicate installed distribution")
        installed[name] = item["version"]
    require(
        installed.get("alternator-client") == source["version"]
        and installed.get(BACKEND) == source["version"]
        and set(source["base_dependencies"]).issubset(installed),
        "installed project/dependency versions missing or contradictory",
    )
    for name, minimum in source["base_dependencies"].items():
        version = installed[name]
        require(
            re.fullmatch(r"\d+(?:\.\d+)*", version) is not None
            and tuple(int(part) for part in version.split(".")) >= minimum,
            "installed dependency version does not satisfy source",
        )
    identity = tuple(
        provenance[key] for key in ("repository", "workflow_ref", "run_attempt")
    )
    require(
        all(isinstance(value, str) and value.strip() for value in identity)
        and re.fullmatch(r"[^/\s]+/[^/\s]+", identity[0]) is not None
        and identity[1].startswith(identity[0] + "/.github/workflows/")
        and "@" in identity[1]
        and re.fullmatch(r"[1-9][0-9]*", identity[2]) is not None,
        "missing execution identity",
    )
    # Earlier successful cells remain valid on unchanged-head failed-job reruns.
    # Their original run is bound through build.json; attempts can legitimately vary.
    for key, value in zip(
        ("GITHUB_REPOSITORY", "GITHUB_WORKFLOW_REF"), identity[:2], strict=True
    ):
        require(
            key not in os.environ or os.environ[key] == value,
            f"{key} differs from current build",
        )
    return identity[:2]


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
    wheel_hash, base_hash = digest(wheel), digest(base)
    # Read static metadata without executing the package or requiring tomllib on 3.10.
    version_text = (root / "native/pyproject.toml").read_text()
    version = re.search(r'^version = "([^"]+)"$', version_text, re.MULTILINE)
    if version is None:
        raise ValueError("Missing static native version")
    slot = inspect_wheel(wheel, args.python, args.platform, version[1])
    evidence = smoke(root, wheel, base, version[1], args.python)
    if (digest(wheel), digest(base)) != (wheel_hash, base_hash):
        raise ValueError("Wheel bytes changed during installation tests")
    check_source(root, args.commit)
    args.output.mkdir(parents=True, exist_ok=True)
    receipt = {
        "filename": wheel.name,
        "sha256": wheel_hash,
        "commit": args.commit,
        "run_id": args.run_id,
    }
    install = {
        "schema": 1,
        "stage": "interface",
        "slot": slot,
        **receipt,
        "version": version[1],
        "base_sha256": base_hash,
        **evidence["checks"],
    }
    info = args.build_info
    provenance = {
        "schema": 2,
        "stage": "interface",
        "build": receipt,
        "slot": slot,
        "target": args.target,
        "version": version[1],
        "rustc": (info / "rustc.txt").read_text(),
        "maturin": (info / "maturin.txt").read_text().strip(),
        "cargo_lock_sha256": digest(root / "native/Cargo.lock"),
        "native_pyproject_sha256": digest(root / "native/pyproject.toml"),
        "base_sha256": base_hash,
        "installed_environment": evidence["environment"],
        "repository": os.environ.get("GITHUB_REPOSITORY"),
        "workflow_ref": os.environ.get("GITHUB_WORKFLOW_REF"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
    }
    shutil.copyfile(info / "cargo-metadata.json", args.output / "cargo-metadata.json")
    for name, value in (
        ("build.json", receipt),
        ("install.json", install),
        ("provenance.json", provenance),
    ):
        (args.output / name).write_bytes(canonical(value))
    (args.output / "SHA256SUMS").write_text(f"{wheel_hash}  {wheel.name}\n")


def aggregate(
    directory: Path,
    commit: str,
    run_id: str,
    needs: dict[str, Any],
    core_evidence: Path,
    root: Path,
    base: Path,
) -> dict[str, Any]:
    """Fail closed for missing, duplicate, skipped, failed or mismatched cells."""
    if set(needs) != {"core", "wheel"} or any(
        job.get("result") != "success" for job in needs.values()
    ):
        raise ValueError("Every native job must conclude successfully")
    if not SHA.fullmatch(commit):
        raise ValueError("Expected full source SHA")
    core = verify_core_evidence(core_evidence, commit, run_id, root)
    source = source_provenance(root)
    bases = list(base.glob("*.whl"))
    if len(bases) != 1:
        raise ValueError("Expected exactly one retained base wheel")
    base_hash = digest(bases[0])
    receipts = []
    builds = []
    seen = set()
    common_identity = None
    for path in sorted(directory.iterdir()):
        if not path.is_dir():
            raise ValueError("Expected only cell artifact directories")
        install = json.loads((path / "install.json").read_bytes())
        build = json.loads((path / "build.json").read_bytes())
        provenance = json.loads((path / "provenance.json").read_bytes())
        slot = install["slot"]
        if (
            slot in seen
            or slot not in SLOTS
            or install.get("stage") != "interface"
            or type(install.get("schema")) is not int
            or install["schema"] != 1
        ):
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
        identity = validate_provenance(provenance, install, build, source)
        if common_identity is None:
            common_identity = identity
        require(identity == common_identity, "cells disagree on execution identity")
        if (path / "SHA256SUMS").read_text() != f"{digest(wheel)}  {wheel.name}\n":
            raise ValueError("Wheel checksum mismatch")
        seen.add(slot)
        builds.append(build)
        receipts.append(install)
    if seen != SLOTS:
        raise ValueError(f"Missing cells: {sorted(SLOTS - seen)}")
    if len({item["version"] for item in receipts}) != 1 or {
        item["base_sha256"] for item in receipts
    } != {base_hash}:
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
        "core": core,
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
    core = sub.add_parser("core-checks")
    core.add_argument("--root", type=Path, default=Path.cwd())
    core.add_argument("--wheels", type=Path, required=True)
    core.add_argument("--base", type=Path, required=True)
    core.add_argument("--startup-context", type=Path, required=True)
    core.add_argument("--output", type=Path, required=True)
    core.add_argument("--commit", required=True)
    core.add_argument("--run-id", required=True)
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
    summary.add_argument("--root", type=Path, default=Path.cwd())
    summary.add_argument("--core-evidence", type=Path, required=True)
    summary.add_argument("--base", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "source":
        check_source(args.root, args.commit)
    elif args.command == "restore-bundle":
        restore_bundle(args.root, args.bundle, args.commit)
    elif args.command == "record":
        record(args)
    elif args.command == "core-checks":
        core_checks(args)
    else:
        result = aggregate(
            args.directory,
            args.commit,
            args.run_id,
            json.loads(args.needs),
            args.core_evidence,
            args.root,
            args.base,
        )
        args.output.write_bytes(canonical(result))


if __name__ == "__main__":
    main()
