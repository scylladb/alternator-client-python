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

"""Exercise version input handling used by the release workflow."""

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def prepare_version(
    version_file: Path, requested: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "--no-print-directory", "release-version"],
        cwd=ROOT,
        env={
            **os.environ,
            "RELEASE_VERSION": requested,
            "VERSION_FILE": str(version_file),
            "GITHUB_ACTIONS": "true",
            "GITHUB_TRIGGERING_ACTOR": "scylladb-publisher",
        },
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("requested", "normalized", "prerelease"),
    [
        ("2.0.2", "2.0.2", "false"),
        ("0.0.0", "0.0.0", "false"),
        ("2.1.0-alpha.0", "2.1.0a0", "true"),
        ("2.1.0-beta.2", "2.1.0b2", "true"),
        ("2.1.0-rc.1", "2.1.0rc1", "true"),
        ("2.1.0a1", "2.1.0a1", "true"),
        ("2.1.0b2", "2.1.0b2", "true"),
        ("2.1.0rc3", "2.1.0rc3", "true"),
    ],
)
@pytest.mark.parametrize("newline", ["\n", "\r\n", ""])
def test_release_version_is_set_from_input(
    tmp_path: Path, requested: str, normalized: str, prerelease: str, newline: str
) -> None:
    version_file = tmp_path / "_version.py"
    separator = newline or "\n"
    version_file.write_bytes(
        f'# Copyright ScyllaDB, Inc.{separator}__version__ = "1.0.0"{newline}'.encode()
    )

    result = prepare_version(version_file, requested)
    result.check_returncode()

    trailing = "\n" if newline else ""
    assert version_file.read_bytes() == (
        f'# Copyright ScyllaDB, Inc.\n__version__ = "{normalized}"{trailing}'.encode()
    )
    assert result.stdout.splitlines() == [
        f"package-version={normalized}",
        f"prerelease={prerelease}",
    ]


@pytest.mark.parametrize(
    "requested",
    [
        "",
        "v2.0.2",
        "2.0",
        "02.0.2",
        "2.0.2-alpha",
        "2.0.2-preview.1",
        "2.0.2-rc.01",
        "2.0.2+local",
        "2.0.2\nprerelease=false",
        "2.0.2; echo unsafe",
    ],
)
def test_invalid_version_does_not_modify_source(tmp_path: Path, requested: str) -> None:
    version_file = tmp_path / "_version.py"
    original = '__version__ = "1.0.0"\n'
    version_file.write_text(original)

    result = prepare_version(version_file, requested)

    assert result.returncode != 0
    assert result.stdout == ""
    assert version_file.read_text() == original


@pytest.mark.parametrize("original", ["", '__version__ = "1"\n__version__ = "2"\n'])
def test_ambiguous_version_file_is_rejected(tmp_path: Path, original: str) -> None:
    version_file = tmp_path / "_version.py"
    version_file.write_text(original)

    result = prepare_version(version_file, "2.0.2")

    assert result.returncode != 0
    assert result.stdout == ""
    assert version_file.read_text() == original
