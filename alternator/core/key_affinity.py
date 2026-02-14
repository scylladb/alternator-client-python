"""Key route affinity for LWT-optimized routing."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

from alternator._constants import PK_DISCOVERY_TIMEOUT_SECONDS

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

    from alternator.core.live_nodes import NodeList

logger = logging.getLogger("alternator")


class AffinitySelector:
    """
    Hash-based node selector for key affinity routing.

    Pure computation - no I/O.
    """

    def select(self, nodes: NodeList, hash_value: int) -> str | None:
        """Select node based on hash value (deterministic)."""
        if not nodes:
            return None

        # Use hash to deterministically select node
        index = abs(hash_value) % len(nodes)
        selected = nodes.nodes[index]
        logger.debug(
            "Affinity selection: hash=%d -> node_index=%d -> %s (of %d nodes)",
            hash_value,
            index,
            selected,
            len(nodes),
            extra={
                "event": "affinity_selection",
                "hash_value": hash_value,
                "node_index": index,
                "selected_node": selected,
                "node_count": len(nodes),
            },
        )
        return selected


def is_rmw_operation(operation_name: str, params: dict[str, Any]) -> bool:
    """
    Check if operation is a read-modify-write operation.

    RMW operations have ConditionExpression or non-NONE ReturnValues.
    """
    if operation_name not in ("UpdateItem", "PutItem", "DeleteItem"):
        return False

    if "ConditionExpression" in params:
        return True

    return_values = params.get("ReturnValues", "NONE")
    return bool(return_values != "NONE")


def is_write_operation(operation_name: str) -> bool:
    """Check if operation is a write operation."""
    return operation_name in ("PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem")


def should_use_affinity(mode: str, operation_name: str, params: dict[str, Any]) -> bool:
    """Determine if key affinity should be used for this request."""
    if mode == "NONE":
        return False
    if mode == "RMW":
        return is_rmw_operation(operation_name, params)
    if mode == "ANY_WRITE":
        return is_write_operation(operation_name)
    return False


def extract_partition_key(
    params: dict[str, Any], pk_name: str
) -> tuple[str, Any] | None:
    """
    Extract partition key value from request params.

    Returns (attr_type, value) tuple or None if not found.
    """
    # Try extracting from Key (GetItem, DeleteItem, UpdateItem)
    if "Key" in params and pk_name in params["Key"]:
        pk_value = params["Key"][pk_name]
        return _extract_typed_value(pk_value)

    # Try extracting from Item (PutItem)
    if "Item" in params and pk_name in params["Item"]:
        pk_value = params["Item"][pk_name]
        return _extract_typed_value(pk_value)

    return None


def _extract_typed_value(attr_value: dict[str, Any]) -> tuple[str, Any] | None:
    """Extract type and value from DynamoDB AttributeValue."""
    for attr_type in ("S", "N", "B"):
        if attr_type in attr_value:
            return (attr_type, attr_value[attr_type])
    return None


def get_table_name(params: dict[str, Any]) -> str | None:
    """Extract table name from request params."""
    return params.get("TableName")


class PartitionKeyCache:
    """
    Thread-safe cache for partition key names discovered via DescribeTable.

    This cache stores the partition key attribute name for each table,
    avoiding repeated DescribeTable calls.
    """

    def __init__(self, client: DynamoDBClient) -> None:
        """
        Initialize the cache.

        Args:
            client: boto3 DynamoDB client for DescribeTable calls
        """
        self._client = client
        self._cache: dict[str, str] = {}
        self._pending: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def get_pk_name(self, table_name: str) -> str | None:
        """
        Get partition key name for a table, using cache when available.

        Uses a pending-state pattern to avoid duplicate DescribeTable calls
        for concurrent requests to the same table.

        Args:
            table_name: Name of the DynamoDB table

        Returns:
            Partition key attribute name, or None if not found
        """
        should_fetch = False
        event: threading.Event | None = None

        with self._lock:
            if table_name in self._cache:
                logger.debug(
                    "Partition key cache hit: table=%s pk=%s",
                    table_name,
                    self._cache[table_name],
                    extra={
                        "event": "pk_cache_hit",
                        "table": table_name,
                        "pk_name": self._cache[table_name],
                    },
                )
                return self._cache[table_name]

            if table_name in self._pending:
                event = self._pending[table_name]
            else:
                event = threading.Event()
                self._pending[table_name] = event
                should_fetch = True

        if should_fetch:
            try:
                pk_name = self._fetch_pk_name(table_name)

                if pk_name:
                    logger.info(
                        "Discovered partition key for table %s: %s",
                        table_name,
                        pk_name,
                        extra={
                            "event": "pk_discovery",
                            "table": table_name,
                            "pk_name": pk_name,
                        },
                    )
                    with self._lock:
                        self._cache[table_name] = pk_name
                else:
                    logger.debug(
                        "Failed to discover partition key for table %s",
                        table_name,
                        extra={
                            "event": "pk_discovery_failed",
                            "table": table_name,
                        },
                    )
            finally:
                with self._lock:
                    self._pending.pop(table_name, None)
                if event:
                    event.set()
            return self._cache.get(table_name)

        # Wait for in-progress fetch to complete (with timeout to avoid deadlock)
        if event and not event.wait(timeout=PK_DISCOVERY_TIMEOUT_SECONDS):
            logger.warning(
                "Timed out waiting for partition key discovery for table %s",
                table_name,
                extra={
                    "event": "pk_discovery_timeout",
                    "table": table_name,
                },
            )
            return None
        return self._cache.get(table_name)

    def _fetch_pk_name(self, table_name: str) -> str | None:
        """
        Fetch partition key name from DescribeTable.

        Args:
            table_name: Name of the DynamoDB table

        Returns:
            Partition key attribute name, or None if not found
        """
        try:
            response = self._client.describe_table(TableName=table_name)
            key_schema = response.get("Table", {}).get("KeySchema", [])
            for key in key_schema:
                if key.get("KeyType") == "HASH":
                    attr_name = key.get("AttributeName")
                    return str(attr_name) if attr_name else None
        except Exception as e:
            logger.warning(
                "Failed to describe table %s for partition key discovery: %s",
                table_name,
                e,
                extra={
                    "event": "describe_table_failed",
                    "table": table_name,
                    "error_type": type(e).__name__,
                },
            )
        return None

    def clear(self) -> None:
        """Clear the cache."""
        with self._lock:
            self._cache.clear()

    def preload(self, table_pk_map: dict[str, str]) -> None:
        """
        Preload cache with known table -> pk mappings.

        Args:
            table_pk_map: Mapping of table name to partition key name
        """
        with self._lock:
            self._cache.update(table_pk_map)
