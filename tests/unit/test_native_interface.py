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

"""The base distribution can discover the optional backend without loading it."""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from alternator._experimental.rust._native import INTERFACE_VERSION, load_extension
from alternator._version import __version__


def test_ordinary_imports_do_not_attempt_native_loading() -> None:
    code = """
import importlib.abc
import sys
class RejectNative(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == '_alternator_rust':
            raise AssertionError('ordinary imports attempted native loading')
sys.meta_path.insert(0, RejectNative())
import alternator
import alternator.async_client
import alternator._experimental.rust
import alternator._experimental.rust._native
assert '_alternator_rust' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=20)


def test_missing_wheel_has_an_actionable_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "_alternator_rust", None)
    with pytest.raises(ImportError, match="requires alternator-client-rust-backend=="):
        load_extension()


@pytest.mark.parametrize("version", [None, 0, 2, "1", True])
def test_interface_mismatch_is_clear(
    monkeypatch: pytest.MonkeyPatch, version: object
) -> None:
    extension = ModuleType("_alternator_rust")
    extension.__dict__.update(INTERFACE_VERSION=version, __version__=__version__)
    monkeypatch.setitem(sys.modules, "_alternator_rust", extension)
    with pytest.raises(ImportError, match="Incompatible _alternator_rust interface"):
        load_extension()


@pytest.mark.parametrize("version", [None, "0.0.0"])
def test_release_mismatch_is_clear(
    monkeypatch: pytest.MonkeyPatch, version: str | None
) -> None:
    extension = ModuleType("_alternator_rust")
    extension.__dict__.update(INTERFACE_VERSION=INTERFACE_VERSION, __version__=version)
    monkeypatch.setitem(sys.modules, "_alternator_rust", extension)
    with pytest.raises(ImportError, match="Incompatible _alternator_rust release"):
        load_extension()


def test_matching_extension_is_returned(monkeypatch: pytest.MonkeyPatch) -> None:
    extension = ModuleType("_alternator_rust")
    extension.__dict__.update(
        INTERFACE_VERSION=INTERFACE_VERSION, __version__=__version__
    )
    monkeypatch.setitem(sys.modules, "_alternator_rust", extension)
    assert load_extension() is extension


def test_dependency_import_errors_are_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = ModuleNotFoundError("missing extension dependency", name="dependency")

    def fail(name: str) -> ModuleType:
        raise error

    monkeypatch.setattr(importlib, "import_module", fail)
    with pytest.raises(ModuleNotFoundError) as caught:
        load_extension()
    assert caught.value is error


def test_native_python_release_metadata_matches_base() -> None:
    # Keep this runnable on Python 3.10 without a TOML parser dependency.
    metadata = (Path(__file__).parents[2] / "native" / "pyproject.toml").read_text()
    project = metadata.split("[project]\n", 1)[1].split("\n[", 1)[0]
    assert f'version = "{__version__}"' in project.splitlines()
    assert "dynamic" not in project
