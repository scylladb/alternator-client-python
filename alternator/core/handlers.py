"""Shared event handlers for Alternator clients."""

from __future__ import annotations

import hashlib
import random
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable
from urllib.parse import urlparse

from botocore.awsrequest import AWSPreparedRequest, AWSRequest
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
    from alternator.config import Config
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
    config: Config,
    compute_affinity_node: Callable[[str, DynamoDBParams, NodeList], str | None]
    | None = None,
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
        compute_affinity_node: Optional function to select a preferred affinity node
    """
    scheme = config.scheme
    port = config.port
    scope_name = config.routing_scope.name

    # Operations that may use key affinity routing
    _affinity_operations = frozenset(
        {"PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem", "GetItem"}
    )

    def create_query_plan(
        nodes: NodeList,
        preferred_node: str | None,
    ) -> Iterator[str]:
        """Create a URI iterator for a single request."""
        node_addresses = nodes.nodes
        if preferred_node is not None and preferred_node in node_addresses:
            yield f"{scheme}://{preferred_node}:{port}"
            remaining_nodes = tuple(
                node for node in node_addresses if node != preferred_node
            )
            seed = _stable_seed(preferred_node)
            plan = LazyQueryPlan(nodes=remaining_nodes, seed=seed)
            for node in plan:
                yield f"{scheme}://{node}:{port}"
            return

        seed = random.getrandbits(64)
        plan = LazyQueryPlan(nodes=node_addresses, seed=seed)
        for node in plan:
            yield f"{scheme}://{node}:{port}"

    # Register event handler to update endpoint per-request
    def update_endpoint(request: AWSRequest | AWSPreparedRequest, **kwargs: Any) -> None:  # noqa: ANN401 -- botocore event handler signature
        """Update request URL based on routing strategy."""
        # Get or create query plan
        context = getattr(request, "context", None)
        plan: Iterator[str] | None
        if isinstance(context, dict):
            plan = context.get(_QUERY_PLAN_ATTR)
        else:
            plan = getattr(request, _QUERY_PLAN_ATTR, None)

        if plan is None:
            plan = _create_request_query_plan(request)
            _store_query_plan(request, plan)

        # Get next node from plan
        try:
            new_uri = next(plan)
        except StopIteration:
            plan = _create_request_query_plan(request, preferred_node=None)
            _store_query_plan(request, plan)
            new_uri = next(plan)

        request_url = (
            request.url.decode("utf-8")
            if isinstance(request.url, bytes)
            else request.url
        )
        parsed = urlparse(request_url)
        path = (
            parsed.path.decode("utf-8")
            if isinstance(parsed.path, bytes)
            else parsed.path
        )
        query = (
            parsed.query.decode("utf-8")
            if isinstance(parsed.query, bytes)
            else parsed.query
        )
        request.url = f"{new_uri}{path}"
        if query:
            request.url += f"?{query}"

    def _create_request_query_plan(
        request: AWSRequest | AWSPreparedRequest,
        preferred_node: str | None | object = _PREFERRED_NODE_UNSET,
    ) -> Iterator[str]:
        nodes = manager.nodes
        if not nodes:
            raise NoNodesAvailableError(
                "No nodes available",
                scope_name=scope_name,
            )

        selected_preferred_node: str | None
        if preferred_node is _PREFERRED_NODE_UNSET:
            selected_preferred_node = None
            if compute_affinity_node is not None:
                # Check operation name first (cheap header read) before
                # parsing the JSON body (expensive)
                operation_name = extract_operation_name(request)
                if operation_name in _affinity_operations:
                    params = extract_request_params(request)
                    selected_preferred_node = compute_affinity_node(
                        operation_name,
                        params,
                        nodes,
                    )
        else:
            selected_preferred_node = cast("str | None", preferred_node)
        return create_query_plan(nodes, selected_preferred_node)

    def _store_query_plan(
        request: AWSRequest | AWSPreparedRequest,
        plan: Iterator[str],
    ) -> None:
        context = getattr(request, "context", None)
        if isinstance(context, dict):
            context[_QUERY_PLAN_ATTR] = plan
            return
        setattr(request, _QUERY_PLAN_ATTR, plan)

    events.register("request-created.dynamodb.*", update_endpoint)

    # Register compression handler if enabled
    if config.request_compression.algorithm == CompressionAlgorithm.GZIP:
        compress_handler = create_compression_handler(
            config.request_compression.min_size_bytes,
            gzip_level=config.request_compression.gzip_level,
        )
        events.register("request-created.dynamodb.*", compress_handler)

    # Register header filter if optimization enabled
    if config.header_optimization.enabled:
        whitelist = compute_header_whitelist(
            config=config,
            auth_enabled=auth_enabled,
            compression_enabled=config.request_compression.enabled,
            custom_whitelist=config.header_optimization.whitelist,
            whitelist_callback=config.header_optimization.whitelist_callback,
        )
        header_filter = create_header_filter_handler(whitelist)
        events.register("before-send.dynamodb.*", header_filter)


def _stable_seed(value: str) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


_PREFERRED_NODE_UNSET = object()
