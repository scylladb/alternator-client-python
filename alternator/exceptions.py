"""Custom exceptions for Alternator load balancing client."""

from __future__ import annotations


class AlternatorError(Exception):
    """Base exception for all Alternator client errors."""

    pass


class NoNodesAvailableError(AlternatorError):
    """
    Raised when no nodes are available for routing.

    This can happen when:
    - Initial node discovery fails and no seed hosts are configured
    - All discovered nodes have become unavailable
    - The routing scope doesn't match any available nodes

    Attributes:
        scope_name: Name of the routing scope that was attempted
        attempted_hosts: List of hosts that were attempted for discovery
        last_known_nodes: List of nodes that were previously known (if any)

    Troubleshooting:
    - Verify seed hosts are reachable
    - Check that the Alternator port is correct
    - Verify network connectivity to the cluster
    - Check if the routing scope (datacenter/rack) is valid
    """

    def __init__(
        self,
        message: str,
        *,
        scope_name: str | None = None,
        attempted_hosts: list[str] | None = None,
        last_known_nodes: list[str] | None = None,
    ) -> None:
        """
        Initialize the exception with diagnostic context.

        Args:
            message: Error message
            scope_name: Name of the routing scope that was attempted
            attempted_hosts: List of hosts that were attempted for discovery
            last_known_nodes: List of nodes that were previously known
        """
        super().__init__(message)
        self.scope_name = scope_name
        self.attempted_hosts = attempted_hosts or []
        self.last_known_nodes = last_known_nodes or []

    def __str__(self) -> str:
        """Return detailed error message with diagnostic context."""
        parts = [super().__str__()]
        if self.scope_name:
            parts.append(f"scope={self.scope_name}")
        if self.attempted_hosts:
            parts.append(f"attempted_hosts={self.attempted_hosts}")
        if self.last_known_nodes:
            parts.append(f"last_known_nodes={self.last_known_nodes}")
        return " | ".join(parts) if len(parts) > 1 else parts[0]


class ConfigurationError(AlternatorError, ValueError):
    """
    Raised when there is a configuration error.

    This can happen when:
    - Required configuration values are missing
    - Configuration values are invalid
    - Conflicting configuration options are specified

    Inherits from both ``AlternatorError`` and ``ValueError`` for backward
    compatibility with code that catches ``ValueError`` from config validation.

    Troubleshooting:
    - Verify all required fields are provided
    - Check that port numbers are in valid range (1-65535)
    - Verify scheme is 'http' or 'https'
    """

    pass
