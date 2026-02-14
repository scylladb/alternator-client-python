"""Shared event handlers for Alternator clients."""

from __future__ import annotations

import random
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.parse import urlparse

from botocore.awsrequest import AWSPreparedRequest
from botocore.hooks import BaseEventHooks

from alternator.config import CompressionAlgorithm
from alternator.core.compression import create_compression_handler
from alternator.core.headers import (
    compute_header_whitelist,
    create_header_filter_handler,
)
from alternator.core.query_plan import LazyQueryPlan
from alternator.core.request import extract_operation_name, extract_request_params
from alternator.exceptions import NoNodesAvailableError

if TYPE_CHECKING:
    from alternator.config import AlternatorConfig
    from alternator.core.live_nodes import NodeList

# Type alias for DynamoDB request parameters (inherently flexible key-value structure)
DynamoDBParams = dict[str, Any]

# Attribute name for storing query plan on request object
_QUERY_PLAN_ATTR = "_alternator_query_plan"


@runtime_checkable
class _HasNodes(Protocol):
    """Protocol for objects that provide a nodes property."""

    @property
    def nodes(self) -> NodeList: ...


def _register_alternator_handlers(
    events: BaseEventHooks,
    manager: _HasNodes,
    config: AlternatorConfig,
    compute_affinity_hash: Callable[[str, DynamoDBParams], int | None] | None = None,
    *,
    auth_enabled: bool = False,
) -> None:
    """
    Register all Alternator event handlers on a boto3/aioboto3 client.

    This is shared between sync and async clients to avoid code duplication.

    Args:
        events: The boto3/aioboto3 events object to register handlers on
        manager: Object with a ``nodes`` property returning a ``NodeList``
        config: Alternator configuration
        compute_affinity_hash: Optional function to compute partition key hash
                              for deterministic node ordering
    """
    scheme = config.scheme
    port = config.port
    scope_name = config.routing_scope.name

    # Operations that may use key affinity routing
    _affinity_operations = frozenset(
        {"PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem", "GetItem"}
    )

    def create_query_plan(affinity_hash: int | None) -> Iterator[str]:
        """Create a URI iterator for a single request."""
        nodes = manager.nodes
        if not nodes:
            raise NoNodesAvailableError(
                "No nodes available",
                scope_name=scope_name,
            )
        seed = affinity_hash if affinity_hash is not None else random.getrandbits(64)
        plan = LazyQueryPlan(nodes=nodes.nodes, seed=seed)
        for node in plan:
            yield f"{scheme}://{node}:{port}"

    # Register event handler to update endpoint per-request
    def update_endpoint(request: AWSPreparedRequest, **kwargs: Any) -> None:  # noqa: ANN401 -- botocore event handler signature
        """Update request URL based on routing strategy."""
        # Get or create query plan
        plan: Iterator[str] | None = getattr(request, _QUERY_PLAN_ATTR, None)
        if plan is None:
            # First attempt: create plan with optional affinity hash
            affinity_hash = None
            if compute_affinity_hash is not None:
                # Check operation name first (cheap header read) before
                # parsing the JSON body (expensive)
                operation_name = extract_operation_name(request)
                if operation_name in _affinity_operations:
                    params = extract_request_params(request)
                    affinity_hash = compute_affinity_hash(operation_name, params)
            plan = create_query_plan(affinity_hash)
            setattr(request, _QUERY_PLAN_ATTR, plan)

        # Get next node from plan
        new_uri = next(plan)

        parsed = urlparse(request.url)
        request.url = f"{new_uri}{parsed.path}"
        if parsed.query:
            request.url += f"?{parsed.query}"

    events.register("before-send.dynamodb.*", update_endpoint)

    # Register compression handler if enabled
    if config.request_compression.algorithm == CompressionAlgorithm.GZIP:
        compress_handler = create_compression_handler(
            config.request_compression.min_size_bytes
        )
        events.register("before-send.dynamodb.*", compress_handler)

    # Register header filter if optimization enabled
    if config.header_optimization.enabled:
        whitelist = compute_header_whitelist(
            auth_enabled=auth_enabled,
            compression_enabled=config.request_compression.enabled,
            custom_whitelist=config.header_optimization.whitelist,
        )
        header_filter = create_header_filter_handler(whitelist)
        events.register("before-send.dynamodb.*", header_filter)
