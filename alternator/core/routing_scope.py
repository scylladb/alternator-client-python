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

"""Routing scope classes for topology-aware request routing."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from urllib.parse import quote


class RoutingScope(ABC):
    """Base class for topology-aware routing scopes."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Short name for logging."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description."""
        ...

    @property
    @abstractmethod
    def fallback(self) -> RoutingScope | None:
        """Next scope to try if this one returns no nodes."""
        ...

    @abstractmethod
    def get_localnodes_query(self) -> str:
        """Query string for /localnodes endpoint."""
        ...


@dataclass(frozen=True)
class ClusterScope(RoutingScope):
    """Route to any node in the cluster."""

    @property
    def name(self) -> str:
        return "Cluster"

    @property
    def description(self) -> str:
        return "All nodes in cluster"

    @property
    def fallback(self) -> RoutingScope | None:
        return None

    def get_localnodes_query(self) -> str:
        return ""


@dataclass(frozen=True, init=False)
class DatacenterScope(RoutingScope):
    """Route to nodes in a specific datacenter."""

    datacenter: str
    _fallback: RoutingScope | None = None

    def __init__(
        self,
        datacenter: str,
        *,
        fallback: RoutingScope | None = None,
    ) -> None:
        """Create a datacenter scope.

        Omitting fallback keeps routing constrained to the datacenter. Use
        ``fallback=...`` to broaden routing when this scope returns no nodes.
        """
        object.__setattr__(self, "datacenter", datacenter)
        object.__setattr__(
            self,
            "_fallback",
            _validate_fallback(fallback),
        )

    @classmethod
    def without_fallback(cls, datacenter: str) -> DatacenterScope:
        """Create a datacenter-only scope."""
        return cls(datacenter, fallback=None)

    @property
    def name(self) -> str:
        return "Datacenter"

    @property
    def description(self) -> str:
        return f"Nodes in datacenter '{self.datacenter}'"

    @property
    def fallback(self) -> RoutingScope | None:
        return self._fallback

    def get_localnodes_query(self) -> str:
        return f"dc={quote(self.datacenter, safe='')}"


@dataclass(frozen=True, init=False)
class RackScope(RoutingScope):
    """Route to nodes in a specific rack within a datacenter."""

    datacenter: str
    rack: str
    _fallback: RoutingScope | None = None

    def __init__(
        self,
        datacenter: str,
        rack: str,
        *,
        fallback: RoutingScope | None = None,
    ) -> None:
        """Create a rack scope.

        Omitting fallback keeps routing constrained to the rack. Use
        ``fallback=...`` to broaden routing when this scope returns no nodes.
        """
        object.__setattr__(self, "datacenter", datacenter)
        object.__setattr__(self, "rack", rack)
        object.__setattr__(
            self,
            "_fallback",
            _validate_fallback(fallback),
        )

    @classmethod
    def without_fallback(cls, datacenter: str, rack: str) -> RackScope:
        """Create a rack-only scope."""
        return cls(datacenter, rack, fallback=None)

    @property
    def name(self) -> str:
        return "Rack"

    @property
    def description(self) -> str:
        return f"Nodes in rack '{self.rack}' of datacenter '{self.datacenter}'"

    @property
    def fallback(self) -> RoutingScope | None:
        return self._fallback

    def get_localnodes_query(self) -> str:
        return f"dc={quote(self.datacenter, safe='')}&rack={quote(self.rack, safe='')}"


def _validate_fallback(fallback: RoutingScope | None | object) -> RoutingScope | None:
    if fallback is None or isinstance(fallback, RoutingScope):
        return fallback
    raise TypeError(f"fallback must be a RoutingScope or None, got {type(fallback)!r}")


def scope_chain_includes_cluster(scope: RoutingScope) -> bool:
    """Return whether a scope chain can fall back to cluster scope."""
    seen: set[int] = set()
    current: RoutingScope | None = scope
    while current is not None:
        current_id = id(current)
        if current_id in seen:
            return False
        seen.add(current_id)
        if isinstance(current, ClusterScope):
            return True
        current = current.fallback
    return False
