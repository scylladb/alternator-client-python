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


@dataclass(frozen=True)
class DatacenterScope(RoutingScope):
    """Route to nodes in a specific datacenter."""

    datacenter: str
    _fallback: RoutingScope | None = None

    @property
    def name(self) -> str:
        return "Datacenter"

    @property
    def description(self) -> str:
        return f"Nodes in datacenter '{self.datacenter}'"

    @property
    def fallback(self) -> RoutingScope | None:
        return self._fallback if self._fallback is not None else ClusterScope()

    def get_localnodes_query(self) -> str:
        return f"dc={quote(self.datacenter, safe='')}"


@dataclass(frozen=True)
class RackScope(RoutingScope):
    """Route to nodes in a specific rack within a datacenter."""

    datacenter: str
    rack: str
    _fallback: RoutingScope | None = None

    @property
    def name(self) -> str:
        return "Rack"

    @property
    def description(self) -> str:
        return f"Nodes in rack '{self.rack}' of datacenter '{self.datacenter}'"

    @property
    def fallback(self) -> RoutingScope | None:
        if self._fallback is None:
            return DatacenterScope(self.datacenter)
        return self._fallback

    def get_localnodes_query(self) -> str:
        return f"dc={quote(self.datacenter, safe='')}&rack={quote(self.rack, safe='')}"
