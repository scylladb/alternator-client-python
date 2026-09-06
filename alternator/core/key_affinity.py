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

"""Key route affinity for LWT-optimized routing."""

from __future__ import annotations

import base64
import binascii
import json
import logging
import threading
from collections import Counter
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, NamedTuple

from alternator._constants import PK_DISCOVERY_TIMEOUT_SECONDS
from alternator.core.hashing import hash_attribute_value
from alternator.core.query_plan import LazyQueryPlan

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

    from alternator.core.live_nodes import NodeList

logger = logging.getLogger("alternator")
AffinityTarget = str | tuple[str, ...]


class _BatchWriteRoutingTarget(NamedTuple):
    table_name: str
    attributes: dict[str, Any]
    sort_key: tuple[str, str, str]


class _BatchWriteCandidate(NamedTuple):
    table_name: str
    attributes: dict[str, Any]
    operation: str


class AffinitySelector:
    """
    Hash-based node selector for key affinity routing.

    Pure computation - no I/O.
    """

    def select(self, nodes: NodeList, hash_value: int) -> str | None:
        """Select node based on hash value (deterministic)."""
        if not nodes:
            return None

        sorted_nodes = tuple(sorted(nodes.nodes))

        # Use hash to deterministically select node
        index = abs(hash_value) % len(sorted_nodes)
        selected = sorted_nodes[index]
        logger.debug(
            "Affinity selection: hash=%d -> node_index=%d -> %s (of %d nodes)",
            hash_value,
            index,
            selected,
            len(sorted_nodes),
            extra={
                "event": "affinity_selection",
                "hash_value": hash_value,
                "node_index": index,
                "selected_node": selected,
                "node_count": len(sorted_nodes),
            },
        )
        return selected


def is_rmw_operation(operation_name: str, params: dict[str, Any]) -> bool:
    """
    Check if operation is a read-modify-write operation.

    RMW operations require a read-before-write path.
    """
    if operation_name not in {"UpdateItem", "PutItem", "DeleteItem"}:
        return False

    if "Expected" in params:
        return True

    if _non_empty_string(params.get("ConditionExpression")):
        return True

    return_values = params.get("ReturnValues")

    if operation_name in {"PutItem", "DeleteItem"}:
        return return_values == "ALL_OLD"

    if operation_name == "UpdateItem":
        if _non_empty_string(params.get("UpdateExpression")):
            return True

        if return_values not in (None, "", "NONE", "UPDATED_NEW"):
            return True

        return _attribute_updates_need_read(params.get("AttributeUpdates"))

    return False


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


def select_affinity_node(
    *,
    mode: str,
    operation_name: str,
    params: dict[str, Any],
    nodes: NodeList,
    get_pk_name: Callable[[str], str | None],
) -> AffinityTarget | None:
    """Select the preferred affinity node for a request, or None for fallback."""
    if not should_use_affinity(mode, operation_name, params):
        return None

    if not nodes:
        return None

    if operation_name == "BatchWriteItem":
        if mode != "ANY_WRITE":
            return None
        return _select_batch_write_affinity_node(params, nodes, get_pk_name)

    table_name = get_table_name(params)
    if not table_name:
        return None

    pk_name = get_pk_name(table_name)
    if not pk_name:
        logger.debug("Could not determine partition key for table %s", table_name)
        return None

    pk_info = extract_partition_key(params, pk_name)
    if not pk_info:
        logger.debug("Could not extract partition key %s from request", pk_name)
        return None

    attr_type, value = pk_info
    try:
        hash_value = hash_attribute_value(attr_type, value)
    except (ValueError, TypeError, UnicodeEncodeError) as e:
        logger.debug("Error hashing partition key: %s", e)
        return None

    return AffinitySelector().select(nodes, hash_value)


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

    batch_target = _find_batch_write_routing_target(params)
    if batch_target and pk_name in batch_target.attributes:
        pk_value = batch_target.attributes[pk_name]
        return _extract_typed_value(pk_value)

    return None


def _extract_typed_value(attr_value: dict[str, Any]) -> tuple[str, Any] | None:
    """Extract type and value from DynamoDB AttributeValue."""
    for attr_type in ("S", "N", "B"):
        if attr_type in attr_value:
            value = attr_value[attr_type]
            if attr_type == "B" and isinstance(value, str):
                try:
                    value = base64.b64decode(value, validate=True)
                except binascii.Error:
                    return None
            return (attr_type, value)
    return None


def _select_batch_write_affinity_node(
    params: dict[str, Any],
    nodes: NodeList,
    get_pk_name: Callable[[str], str | None],
) -> tuple[str, ...] | None:
    votes: Counter[str] = Counter()

    for candidate in _iter_batch_write_candidates(params):
        pk_name = get_pk_name(candidate.table_name)
        if not pk_name:
            continue

        pk_value = candidate.attributes.get(pk_name)
        if not isinstance(pk_value, dict):
            continue

        pk_info = _extract_typed_value(pk_value)
        if pk_info is None:
            continue

        attr_type, value = pk_info
        try:
            hash_value = hash_attribute_value(attr_type, value)
        except (ValueError, TypeError, UnicodeEncodeError):
            continue

        node = _select_query_plan_first_node(nodes, hash_value)
        if node is not None:
            votes[node] += 1

    if not votes:
        return None

    return tuple(
        sorted(
            votes,
            key=lambda node: (-votes[node], node),
        )
    )


def _select_query_plan_first_node(nodes: NodeList, hash_value: int) -> str | None:
    """Return first node from canonical seeded affinity query plan."""
    if not nodes:
        return None
    return next(LazyQueryPlan(nodes=tuple(sorted(nodes.nodes)), seed=hash_value))


def _iter_batch_write_candidates(
    params: dict[str, Any],
) -> Iterable[_BatchWriteCandidate]:
    request_items = params.get("RequestItems")
    if not isinstance(request_items, dict):
        return ()

    candidates: list[_BatchWriteCandidate] = []
    for table_name, writes in sorted(
        request_items.items(), key=lambda item: str(item[0])
    ):
        if not isinstance(table_name, str) or not isinstance(writes, list):
            continue
        for write in writes:
            candidate = _batch_write_candidate(table_name, write)
            if candidate is not None:
                candidates.append(candidate)

    return tuple(candidates)


def _batch_write_candidate(
    table_name: str,
    write: object,
) -> _BatchWriteCandidate | None:
    if not isinstance(write, dict):
        return None

    operations = [
        operation for operation in ("PutRequest", "DeleteRequest") if operation in write
    ]
    if len(operations) != 1:
        return None

    operation = operations[0]
    request = write[operation]
    if not isinstance(request, dict):
        return None

    attribute_field = "Item" if operation == "PutRequest" else "Key"
    attributes = request.get(attribute_field)
    if not isinstance(attributes, dict):
        return None

    return _BatchWriteCandidate(table_name, attributes, operation)


def _non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value != ""


def _attribute_updates_need_read(attribute_updates: object) -> bool:
    if not isinstance(attribute_updates, dict):
        return False

    for update in attribute_updates.values():
        if not isinstance(update, dict):
            continue
        action = update.get("Action")
        if action == "ADD":
            return True
        if action == "DELETE" and _attribute_update_value_is_non_empty(
            update.get("Value")
        ):
            return True
    return False


def _attribute_update_value_is_non_empty(value: object) -> bool:
    if not isinstance(value, dict):
        return bool(value)
    return any(bool(item) for item in value.values())


def get_table_name(params: dict[str, Any]) -> str | None:
    """Extract table name from request params."""
    table_name = params.get("TableName")
    if isinstance(table_name, str):
        return table_name

    batch_target = _find_batch_write_routing_target(params)
    if batch_target:
        return batch_target.table_name

    return None


def _find_batch_write_routing_target(
    params: dict[str, Any],
) -> _BatchWriteRoutingTarget | None:
    request_items = params.get("RequestItems")
    if not isinstance(request_items, dict):
        return None

    target: _BatchWriteRoutingTarget | None = None
    for table_name, writes in request_items.items():
        if not isinstance(table_name, str) or not isinstance(writes, list):
            continue
        for write in writes:
            candidate = _batch_write_candidate(table_name, write)
            if candidate is None:
                continue

            target = _min_batch_write_target(
                target,
                _BatchWriteRoutingTarget(
                    table_name,
                    candidate.attributes,
                    _batch_write_sort_key(
                        table_name,
                        candidate.operation,
                        candidate.attributes,
                    ),
                ),
            )

    return target


def _min_batch_write_target(
    current: _BatchWriteRoutingTarget | None,
    candidate: _BatchWriteRoutingTarget,
) -> _BatchWriteRoutingTarget:
    if current is None or candidate.sort_key < current.sort_key:
        return candidate
    return current


def _batch_write_sort_key(
    table_name: str,
    operation: str,
    attributes: dict[str, Any],
) -> tuple[str, str, str]:
    return (table_name, _canonical_json(attributes), operation)


def _canonical_json(value: object) -> str:
    return json.dumps(
        _to_jsonable(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _to_jsonable(value: object) -> object:
    if isinstance(value, dict):
        return {
            str(key): _to_jsonable(item_value)
            for key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, bytes | bytearray):
        return {"__bytes__": bytes(value).hex()}
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return repr(value)


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
