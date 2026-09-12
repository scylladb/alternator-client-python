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

"""Repository-level constraints for CCM test infrastructure."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _tracked_files() -> tuple[Path, ...]:
    result = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    return tuple(
        PROJECT_ROOT / os.fsdecode(value)
        for value in result.stdout.split(b"\0")
        if value
    )


def test_tracked_files_are_resolved_from_repository_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repository enforcement is independent of the pytest working directory."""
    monkeypatch.chdir(tmp_path)

    tracked = _tracked_files()

    assert PROJECT_ROOT / "Makefile" in tracked
    assert all(path.is_absolute() for path in tracked)


def test_repository_contains_no_shell_scripts() -> None:
    """Keep all CCM ownership and recovery logic in Python."""
    shell_suffixes = {".sh", ".bash", ".zsh", ".fish"}
    shell_shebang = re.compile(
        rb"^#![ \t]*(?:(?:/usr/bin/env)[ \t]+(?:-S[ \t]+)?|(?:\S*/))?"
        rb"(?:sh|bash|dash|zsh|ksh|ash|fish)(?:[ \t]|$)"
    )
    violations: list[str] = []
    for path in _tracked_files():
        display_path = path.relative_to(PROJECT_ROOT)
        if path.suffix in shell_suffixes:
            violations.append(str(display_path))
            continue
        try:
            first_line = path.read_bytes().splitlines()[0]
        except (IndexError, OSError):
            continue
        if shell_shebang.search(first_line):
            violations.append(str(display_path))

    assert violations == [], f"shell scripts are forbidden: {violations}"
