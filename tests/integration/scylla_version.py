"""ScyllaDB version detection for integration tests.

This module provides utilities to detect the running ScyllaDB version
and skip tests that require specific versions.

Version can be detected automatically via system.local table or
overridden via SCYLLA_VERSION environment variable.

Examples:
    SCYLLA_VERSION=2025.4.0 pytest tests/integration/
    SCYLLA_VERSION=2026.1.0 pytest tests/integration/
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class ScyllaVersion:
    """Parsed ScyllaDB version for comparison.

    Supports both old-style (6.x) and new-style (2025.x) versioning.

    Examples:
        ScyllaVersion(2025, 4, 0)  -> "2025.4.0"
        ScyllaVersion(2026, 1, 0)  -> "2026.1.0"
        ScyllaVersion(6, 2, 0)    -> "6.2.0"
    """

    major: int
    minor: int
    patch: int = 0

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    @classmethod
    def parse(cls, version_str: str) -> ScyllaVersion | None:
        """Parse version string like '2025.4.0' or '6.2.3'.

        Also handles version strings with suffixes like:
        - '2026.1.0-rc0'
        - '5.4.0-0.20240101'
        - '2025.4.0~rc1'

        Returns None if parsing fails.
        """
        if not version_str:
            return None

        # Clean the version string - extract just the version numbers
        # Handle formats: "2025.4.0", "2025.4.0-rc0", "2025.4.0~rc1", "5.4.0-0.20240101"
        match = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", version_str.strip())
        if not match:
            return None

        major = int(match.group(1))
        minor = int(match.group(2))
        patch = int(match.group(3)) if match.group(3) else 0

        return cls(major, minor, patch)


# Minimum version that supports gzip request compression
GZIP_COMPRESSION_MIN_VERSION = ScyllaVersion(2026, 1, 0)


def get_version_from_env() -> ScyllaVersion | None:
    """Get ScyllaDB version from SCYLLA_VERSION environment variable."""
    version_str = os.environ.get("SCYLLA_VERSION", "").strip()
    if not version_str:
        return None

    version = ScyllaVersion.parse(version_str)
    if version:
        logger.info("Using ScyllaDB version from environment: %s", version)
    else:
        logger.warning("Failed to parse SCYLLA_VERSION='%s'", version_str)
    return version


def detect_version_from_cluster(client: DynamoDBClient) -> ScyllaVersion | None:
    """Detect ScyllaDB version by querying system.local table.

    Uses Alternator's system table access feature:
    TableName='.scylla.alternator.system.local'

    Args:
        client: A boto3 DynamoDB client connected to Alternator

    Returns:
        Parsed ScyllaVersion or None if detection fails
    """
    try:
        # Query system.local table via Alternator's special table name
        response = client.scan(
            TableName=".scylla.alternator.system.local",
            Limit=1,
        )

        items = response.get("Items", [])
        if not items:
            logger.warning("No items returned from system.local")
            return None

        item = items[0]

        # Try different version fields that might exist
        version_str = None
        for field in ("release_version", "version", "scylla_version"):
            if field in item:
                # Handle DynamoDB attribute value format
                attr_value = item[field]
                if "S" in attr_value:
                    version_str = attr_value["S"]
                    break

        if not version_str:
            logger.warning(
                "No version field found in system.local: %s", list(item.keys())
            )
            return None

        version = ScyllaVersion.parse(version_str)
        if version:
            logger.info("Detected ScyllaDB version from cluster: %s", version)
        else:
            logger.warning("Failed to parse version string: %s", version_str)

        return version

    except Exception as e:
        logger.debug("Failed to detect version from cluster: %s", e)
        return None


_cached_version: ScyllaVersion | None = None
_version_resolved: bool = False


def get_scylla_version(client: DynamoDBClient | None = None) -> ScyllaVersion | None:
    """Get ScyllaDB version, using environment variable or auto-detection.

    Results are cached after the first successful resolution.

    Priority:
    1. SCYLLA_VERSION environment variable (if set)
    2. Auto-detection from cluster (if client provided)
    3. None (version unknown)

    Args:
        client: Optional DynamoDB client for auto-detection

    Returns:
        Parsed ScyllaVersion or None if unknown
    """
    global _cached_version, _version_resolved

    if _version_resolved:
        return _cached_version

    # First check environment variable
    env_version = get_version_from_env()
    if env_version:
        _cached_version = env_version
        _version_resolved = True
        return _cached_version

    # Try auto-detection if client provided
    if client is not None:
        _cached_version = detect_version_from_cluster(client)
        _version_resolved = True
        return _cached_version

    return None


def clear_version_cache() -> None:
    """Clear the cached version (useful for testing)."""
    global _cached_version, _version_resolved
    _cached_version = None
    _version_resolved = False


def supports_gzip_compression(version: ScyllaVersion | None) -> bool:
    """Check if the given version supports gzip request compression.

    Gzip request compression was added in ScyllaDB 2026.1.0.
    See: https://github.com/scylladb/scylladb/pull/27080

    Args:
        version: ScyllaDB version to check, or None if unknown

    Returns:
        True if compression is supported, False otherwise.
        Returns False if version is unknown (conservative approach).
    """
    if version is None:
        return False
    return version >= GZIP_COMPRESSION_MIN_VERSION


def requires_scylla_version(min_version: ScyllaVersion, feature_name: str = "") -> str:
    """Generate a skip reason for version requirements.

    Args:
        min_version: Minimum required ScyllaDB version
        feature_name: Optional feature name for the message

    Returns:
        Skip reason string
    """
    if feature_name:
        return f"{feature_name} requires ScyllaDB {min_version}+"
    return f"Requires ScyllaDB {min_version}+"
