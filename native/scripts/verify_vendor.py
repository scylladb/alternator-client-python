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

"""Verify the audited Tokio source, optionally against its official archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path

ARCHIVE_SHA256 = "b52fd3255258d26fac7264ab5a76b53c13367136dc011d3d6ad4f8ed070396a3"
PATCH_SHA256 = "d0856157d59a7e9bf4df0578089fb7d58dbacb8907df8cb2c5c61df00657a324"
# Independent anchor for the audited result, not a value read from provenance.
PATCHED_FILES_SHA256 = (
    "281e96a7b8647ba097d17296e77feb1d7538c4ec13810f4371e074a141d358f1"
)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(f"vendor verification failed: {message}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--cargo", action="store_true")
    args = parser.parse_args()
    native = Path(__file__).resolve().parents[1]
    vendor = native / "vendor"
    provenance = json.loads((vendor / "tokio-provenance.json").read_text())
    require(
        provenance["archive_sha256"] == ARCHIVE_SHA256,
        "upstream archive checksum changed",
    )
    require(
        provenance["patch_sha256"] == PATCH_SHA256, "approved patch checksum changed"
    )
    require(
        digest((vendor / provenance["patch_file"]).read_bytes()) == PATCH_SHA256,
        "patch file checksum differs",
    )
    patched_manifest = "".join(
        f"{checksum}  {name}\n"
        for name, checksum in sorted(provenance["patched_files"].items())
    )
    require(
        digest(patched_manifest.encode("utf-8")) == PATCHED_FILES_SHA256,
        "approved patched-file manifest differs",
    )
    actual = {
        path.relative_to(vendor / "tokio").as_posix(): digest(path.read_bytes())
        for path in (vendor / "tokio").rglob("*")
        if path.is_file()
    }
    require(actual == provenance["patched_files"], "vendored source differs")
    upstream = provenance["upstream_files"]
    require(actual.keys() == upstream.keys(), "upstream and patched file lists differ")
    require(
        sorted(name for name in actual if actual[name] != upstream[name])
        == [
            "src/runtime/blocking/pool.rs",
            "src/runtime/scheduler/multi_thread/worker.rs",
        ],
        "local changes differ from the two approved Tokio files",
    )
    require(actual["LICENSE"] == upstream["LICENSE"], "upstream license changed")
    if args.archive:
        require(
            digest(args.archive.read_bytes()) == ARCHIVE_SHA256,
            "official archive checksum differs",
        )
        with tarfile.open(args.archive) as archive:
            originals = {}
            for item in archive.getmembers():
                if item.isfile():
                    contents = archive.extractfile(item)
                    if contents is None:
                        raise ValueError(
                            f"vendor verification failed: unreadable archive member {item.name}"
                        )
                    originals[item.name.split("/", 1)[1]] = digest(contents.read())
        require(originals == upstream, "official archive differs")
    if args.cargo:
        result = subprocess.run(
            [
                "cargo",
                "metadata",
                "--locked",
                "--format-version",
                "1",
                "--features",
                "extension",
            ],
            cwd=native,
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )
        metadata = json.loads(result.stdout)
        tokio = [
            package for package in metadata["packages"] if package["name"] == "tokio"
        ]
        require(len(tokio) == 1, "expected exactly one Tokio crate identity")
        require(
            Path(tokio[0]["manifest_path"]).resolve()
            == vendor / "tokio" / "Cargo.toml",
            "Tokio does not resolve to the vendored manifest",
        )
        require(tokio[0]["version"] == "1.54.1", "Tokio version differs from 1.54.1")
    print(
        json.dumps(
            {
                "verified_files": len(actual),
                "patch_sha256": PATCH_SHA256,
                "one_tokio": bool(args.cargo),
            }
        )
    )


if __name__ == "__main__":
    main()
