#!/usr/bin/env python3
# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run only in an isolated installed-wheel environment, never skip failures."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import importlib.util
import json
import shutil
import sys
import sysconfig
from concurrent.futures import ThreadPoolExecutor
from email.parser import Parser
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--python", required=True)
    args = parser.parse_args()
    checkout = args.checkout.resolve()
    assert sys.prefix != sys.base_prefix
    assert sys.implementation.name == "cpython"
    assert f"{sys.version_info.major}.{sys.version_info.minor}" == args.python
    assert not sysconfig.get_config_var("Py_GIL_DISABLED")
    assert not Path.cwd().resolve().is_relative_to(checkout)
    assert all(not Path(path).resolve().is_relative_to(checkout) for path in sys.path)
    assert shutil.which("cargo") is None
    assert shutil.which("rustc") is None
    import alternator
    import alternator._experimental.rust

    assert "_alternator_rust" not in sys.modules
    assert importlib.metadata.version("alternator-client") == args.version
    distribution = importlib.metadata.distribution("alternator-client-rust-backend")
    assert distribution.version == args.version
    wheel = Parser().parsestr(distribution.read_text("WHEEL"))
    interpreter = "cp" + args.python.replace(".", "")
    assert wheel["Root-Is-Purelib"] == "false"
    assert wheel.get_all("Tag")
    assert all(
        tag.startswith(f"{interpreter}-{interpreter}-") for tag in wheel.get_all("Tag")
    )
    spec = importlib.util.find_spec("_alternator_rust")
    assert spec is not None and spec.origin is not None
    assert Path(spec.origin).resolve().is_relative_to(Path(sys.prefix).resolve())
    assert not Path(spec.origin).resolve().is_relative_to(checkout)
    assert (
        Path(alternator.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    )
    from alternator._experimental.rust._native import load_extension

    native = load_extension()
    # Maturin may provide a Python re-export package around the binary module.
    binaries = [
        Path(module.__file__).resolve()
        for name, module in sys.modules.items()
        if name.startswith("_alternator_rust")
        and getattr(module, "__file__", "").endswith((".so", ".pyd"))
    ]
    assert len(binaries) == 1
    assert binaries[0].is_relative_to(Path(sys.prefix).resolve())
    assert not binaries[0].is_relative_to(checkout)
    assert native.INTERFACE_VERSION == 1
    assert native.__version__ == args.version
    assert not native._runtime_initialized()
    with ThreadPoolExecutor(max_workers=8) as workers:
        identities = list(workers.map(lambda _: native._runtime_id(), range(32)))
    assert len(set(identities)) == 1
    identity = identities[0]

    async def run() -> None:
        values = await asyncio.gather(*(native._runtime_id_async() for _ in range(16)))
        assert all(value == identity for value in values)
        tasks = [asyncio.ensure_future(native._runtime_id_async()) for _ in range(16)]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert await native._runtime_id_async() == identity

    asyncio.run(run())
    asyncio.run(run())
    assert native._runtime_id() == identity
    try:
        native._runtime_id_async()
    except RuntimeError as error:
        assert "no running event loop" in str(error)
    else:
        raise AssertionError("Async bridge accepted a missing loop")
    print(
        json.dumps(
            {
                "checks": dict.fromkeys(
                    (
                        "fresh_environments",
                        "outside_checkout",
                        "without_rust",
                        "module_locations",
                        "wheel_tags",
                        "interface",
                        "shared_runtime",
                        "task_cleanup",
                        "base_import",
                    ),
                    True,
                ),
                "environment": {
                    "python": sys.version,
                    "distributions": sorted(
                        (
                            {"name": item.metadata["Name"], "version": item.version}
                            for item in importlib.metadata.distributions()
                        ),
                        key=lambda item: item["name"],
                    ),
                },
            }
        )
    )


if __name__ == "__main__":
    main()
