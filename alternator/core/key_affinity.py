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
import contextlib
import json
import logging
import queue
import threading
import weakref
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from alternator._constants import PK_DISCOVERY_TIMEOUT_SECONDS
from alternator.core.hashing import hash_attribute_value
from alternator.core.query_plan import LazyQueryPlan

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

    from alternator.core.live_nodes import NodeList

logger = logging.getLogger("alternator")

_PK_DISCOVERY_QUEUE_CAPACITY = 64
_PK_DISCOVERY_SHUTDOWN_TIMEOUT_SECONDS = 1.0
_PK_DISCOVERY_STOP = object()


@dataclass(frozen=True)
class SeededAffinityPlan:
    """Descriptor for a canonical partition-key-seeded query plan."""

    seed: int


AffinityTarget = SeededAffinityPlan | tuple[str, ...]


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
        """Select first node from the canonical seeded query plan."""
        if not nodes:
            return None

        sorted_nodes = tuple(sorted(set(nodes.nodes)))
        selected = next(LazyQueryPlan(nodes=sorted_nodes, seed=hash_value))
        logger.debug(
            "Affinity selection: hash=%d -> %s (of %d nodes)",
            hash_value,
            selected,
            len(sorted_nodes),
            extra={
                "event": "affinity_selection",
                "hash_value": hash_value,
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

    expected = params.get("Expected")
    if isinstance(expected, dict) and expected:
        return True

    if _non_empty_string(params.get("ConditionExpression")):
        return True

    return_values = params.get("ReturnValues")

    if operation_name in {"PutItem", "DeleteItem"}:
        return isinstance(return_values, str) and return_values not in ("", "NONE")

    if operation_name == "UpdateItem":
        if _non_empty_string(params.get("UpdateExpression")):
            return True

        if return_values in ("ALL_OLD", "UPDATED_OLD", "ALL_NEW"):
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
        if operation_name == "BatchWriteItem":
            return any(_iter_batch_write_candidates(params))
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

    return SeededAffinityPlan(seed=hash_value)


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
    recognized_types = [
        attr_type for attr_type in ("S", "N", "B") if attr_type in attr_value
    ]
    if len(recognized_types) != 1 or len(attr_value) != 1:
        return None

    attr_type = recognized_types[0]
    value = attr_value[attr_type]
    if attr_type == "B" and isinstance(value, str):
        try:
            value = base64.b64decode(value, validate=True)
        except binascii.Error:
            return None
    return (attr_type, value)


def _select_batch_write_affinity_node(
    params: dict[str, Any],
    nodes: NodeList,
    get_pk_name: Callable[[str], str | None],
) -> tuple[str, ...] | None:
    votes: Counter[str] = Counter()
    pk_names: dict[str, str | None] = {}

    for candidate in _iter_batch_write_candidates(params):
        if candidate.table_name not in pk_names:
            # Keep metadata stable for the whole request.  In particular, a
            # background lookup started by the first candidate must not make a
            # timing-dependent suffix of a cold batch affinity-eligible.
            pk_names[candidate.table_name] = get_pk_name(candidate.table_name)
        pk_name = pk_names[candidate.table_name]
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
    return next(LazyQueryPlan(nodes=tuple(sorted(set(nodes.nodes))), seed=hash_value))


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
        if action == "DELETE" and update.get("Value") is not None:
            return True
    return False


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


class _PartitionKeyDiscoveryWorker:
    """Bounded daemon worker which never owns its cache or boto client."""

    def __init__(
        self,
        cache_ref: weakref.ReferenceType[PartitionKeyCache],
        *,
        queue_capacity: int,
        shutdown_timeout_seconds: float,
        thread_name: str,
    ) -> None:
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be at least 1")
        if shutdown_timeout_seconds < 0:
            raise ValueError("shutdown_timeout_seconds must not be negative")

        self._cache_ref = cache_ref
        self._queue: queue.Queue[str | object] = queue.Queue(maxsize=queue_capacity)
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._thread_name = thread_name
        self._stopped = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def submit(self, table_name: str) -> bool:
        """Queue one table without blocking, starting the worker lazily."""
        with self._lifecycle_lock:
            if self._stopped.is_set():
                return False

            if self._thread is None or not self._thread.is_alive():
                thread = threading.Thread(
                    target=self._run,
                    name=self._thread_name,
                    daemon=True,
                )
                try:
                    thread.start()
                except RuntimeError as e:
                    logger.debug(
                        "Failed to start partition key discovery worker: %s",
                        e,
                        extra={
                            "event": "pk_discovery_worker_start_failed",
                            "error_type": type(e).__name__,
                        },
                    )
                    return False
                self._thread = thread

            try:
                self._queue.put_nowait(table_name)
            except queue.Full:
                return False
            return True

    def shutdown(self) -> None:
        """Cancel queued work and wait a bounded time for active work."""
        with self._lifecycle_lock:
            first_shutdown = not self._stopped.is_set()
            self._stopped.set()
            thread = self._thread

            if first_shutdown and thread is not None:
                while True:
                    try:
                        self._queue.get_nowait()
                    except queue.Empty:
                        break
                self._queue.put_nowait(_PK_DISCOVERY_STOP)

        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=self._shutdown_timeout_seconds)

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _PK_DISCOVERY_STOP:
                return
            if self._stopped.is_set():
                continue

            cache = self._cache_ref()
            if cache is None:
                return
            try:
                cache._discover_pk_name(cast(str, item))
            finally:
                # Do not retain cache -> client while waiting for more work.
                del cache


def _shutdown_partition_key_worker(worker: _PartitionKeyDiscoveryWorker) -> None:
    """Weakref callback safe for normal and interpreter-shutdown cleanup."""
    with contextlib.suppress(Exception):
        worker.shutdown()


class PartitionKeyCache:
    """Thread-safe cache with non-blocking probes for request routing."""

    def __init__(
        self,
        client: DynamoDBClient,
        *,
        queue_capacity: int = _PK_DISCOVERY_QUEUE_CAPACITY,
        shutdown_timeout_seconds: float = _PK_DISCOVERY_SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize background discovery using the supplied boto3 client."""
        self._client = client
        self._cache: dict[str, str] = {}
        self._pending: dict[str, threading.Event] = {}
        self._closed = False
        self._lock = threading.Lock()
        self._worker = _PartitionKeyDiscoveryWorker(
            weakref.ref(self),
            queue_capacity=queue_capacity,
            shutdown_timeout_seconds=shutdown_timeout_seconds,
            thread_name=f"alternator-pk-discovery-{id(self):x}",
        )
        # Callback owns only worker state.  Worker holds a weak cache reference,
        # so this finalizer cannot keep the cache or client alive.
        self._client_finalizer = weakref.finalize(
            client,
            _shutdown_partition_key_worker,
            self._worker,
        )

    def _lookup_or_schedule(
        self,
        table_name: str,
        *,
        schedule_in_background: bool,
    ) -> tuple[str | None, threading.Event | None, bool]:
        """Return cached metadata, pending event, and direct-fetch ownership."""
        not_scheduled = False
        with self._lock:
            pk_name = self._cache.get(table_name)
            if pk_name is not None:
                logger.debug(
                    "Partition key cache hit: table=%s pk=%s",
                    table_name,
                    pk_name,
                    extra={
                        "event": "pk_cache_hit",
                        "table": table_name,
                        "pk_name": pk_name,
                    },
                )
                return (pk_name, None, False)

            if self._closed:
                return (None, None, False)

            event = self._pending.get(table_name)
            if event is not None:
                return (None, event, False)

            event = threading.Event()
            self._pending[table_name] = event
            if schedule_in_background:
                scheduled = self._worker.submit(table_name)
                if not scheduled:
                    self._pending.pop(table_name, None)
                    event = None
                    not_scheduled = True

        if not_scheduled:
            logger.debug(
                "Partition key discovery unavailable for table %s",
                table_name,
                extra={
                    "event": "pk_discovery_not_scheduled",
                    "table": table_name,
                },
            )

        owns_discovery = event is not None and not schedule_in_background
        return (None, event, owns_discovery)

    def get_cached_pk_name(self, table_name: str) -> str | None:
        """Return cached metadata, scheduling a background lookup on a miss."""
        pk_name, _, _ = self._lookup_or_schedule(
            table_name,
            schedule_in_background=True,
        )

        # This request must use random fallback even if discovery finishes now.
        return pk_name

    def get_pk_name(self, table_name: str) -> str | None:
        """Return metadata, waiting for coalesced discovery on a cache miss."""
        pk_name, event, owns_discovery = self._lookup_or_schedule(
            table_name,
            schedule_in_background=False,
        )
        if pk_name is not None or event is None:
            return pk_name

        if owns_discovery:
            # Match the original synchronous API: the initiating diagnostic
            # lookup is bounded by the SDK operation's own timeout/retry policy.
            self._discover_pk_name(table_name)
        elif not event.wait(timeout=PK_DISCOVERY_TIMEOUT_SECONDS):
            logger.warning(
                "Timed out waiting for partition key discovery for table %s",
                table_name,
                extra={
                    "event": "pk_discovery_timeout",
                    "table": table_name,
                },
            )
            return None

        with self._lock:
            return self._cache.get(table_name)

    def _discover_pk_name(self, table_name: str) -> None:
        """Discover and publish one table's partition key in the worker."""
        pk_name: str | None = None
        try:
            pk_name = self._fetch_pk_name(table_name)
        finally:
            with self._lock:
                event = self._pending.pop(table_name, None)
                if not self._closed and pk_name:
                    # Explicitly preloaded metadata wins a race with discovery.
                    self._cache.setdefault(table_name, pk_name)
            if event is not None:
                event.set()

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
        else:
            logger.debug(
                "Failed to discover partition key for table %s",
                table_name,
                extra={
                    "event": "pk_discovery_failed",
                    "table": table_name,
                },
            )

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
            if not self._closed:
                self._cache.update(table_pk_map)

    def close(self) -> None:
        """Stop background discovery without waiting indefinitely."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending_events = tuple(self._pending.values())
            self._pending.clear()

        for event in pending_events:
            event.set()

        # Invoking the finalizer is idempotent and disables its later GC run.
        self._client_finalizer()
