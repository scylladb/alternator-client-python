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

"""Load the optional wheel only when a native client is explicitly requested."""

from __future__ import annotations

import importlib
from types import ModuleType

from alternator._version import __version__

INTERFACE_VERSION = 1


def load_extension() -> ModuleType:
    """Require matching wheel release and interface, preserving dependency errors."""
    try:
        native = importlib.import_module("_alternator_rust")
    except ModuleNotFoundError as error:
        if error.name != "_alternator_rust":
            raise
        raise ImportError(
            "The experimental Rust backend requires "
            f"alternator-client-rust-backend=={__version__}; "
            "install a supported native wheel explicitly."
        ) from error
    actual = getattr(native, "INTERFACE_VERSION", None)
    if type(actual) is not int or actual != INTERFACE_VERSION:
        raise ImportError(
            "Incompatible _alternator_rust interface: "
            f"expected {INTERFACE_VERSION}, got {actual!r}. "
            f"Install alternator-client-rust-backend=={__version__}."
        )
    actual_version = getattr(native, "__version__", None)
    if actual_version != __version__:
        raise ImportError(
            "Incompatible _alternator_rust release: "
            f"expected {__version__!r}, got {actual_version!r}. "
            f"Install alternator-client-rust-backend=={__version__}."
        )
    return native
