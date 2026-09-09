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

"""Shared event handlers for Alternator clients."""

from __future__ import annotations

import random
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.parse import urlparse

from botocore.awsrequest import AWSPreparedRequest, AWSRequest
from botocore.hooks import BaseEventHooks

from alternator.config import DEFAULT_USER_AGENT, CompressionAlgorithm
from alternator.core.compression import (
    create_compression_handler,
    create_response_compression_decode_handler,
    create_response_compression_request_handler,
)
from alternator.core.headers import (
    compute_header_whitelist,
    create_header_filter_handler,
    create_user_agent_header_handler,
)
from alternator.core.key_affinity import AffinityTarget, SeededAffinityPlan
from alternator.core.live_nodes import _format_host_port
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
    compute_affinity_node: Callable[
        [str, DynamoDBParams, NodeList], AffinityTarget | None
    ]
    | None = None,
    *,
    auth_enabled: bool = False,
    user_agent: str | None = DEFAULT_USER_AGENT,
) -> None:
    """
    Register all Alternator event handlers on a boto3/aioboto3 client.

    This is shared between sync and async clients to avoid code duplication.

    Args:
        events: The boto3/aioboto3 events object to register handlers on
        manager: Object with a ``nodes`` property returning a ``NodeList``
        config: Alternator configuration
        compute_affinity_node: Optional function to select a preferred affinity node
        user_agent: Final Alternator User-Agent header value, or None to remove it
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
        affinity_target: AffinityTarget | None,
    ) -> Iterator[str]:
        """Create a URI iterator for a single request."""
        node_addresses = tuple(sorted(set(nodes.nodes)))
        if isinstance(affinity_target, tuple):
            emitted: set[str] = set()
            ordered_nodes: list[str] = []
            for node in affinity_target:
                if node in node_addresses and node not in emitted:
                    emitted.add(node)
                    ordered_nodes.append(node)
            for node in node_addresses:
                if node not in emitted:
                    ordered_nodes.append(node)

            while True:
                for node in ordered_nodes:
                    yield f"{scheme}://{_format_host_port(node, port)}"

        if isinstance(affinity_target, SeededAffinityPlan):
            while True:
                plan = LazyQueryPlan(nodes=node_addresses, seed=affinity_target.seed)
                for node in plan:
                    yield f"{scheme}://{_format_host_port(node, port)}"

        while True:
            plan = LazyQueryPlan(nodes=node_addresses, seed=random.getrandbits(64))
            for node in plan:
                yield f"{scheme}://{_format_host_port(node, port)}"

    # Register event handler to update endpoint per-request
    def update_endpoint(
        request: AWSPreparedRequest | AWSRequest,
        **kwargs: Any,  # noqa: ANN401 -- botocore event handler signature
    ) -> None:
        """Update request URL based on routing strategy."""
        mark_activity = getattr(manager, "mark_activity", None)
        if callable(mark_activity):
            mark_activity()

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
    ) -> Iterator[str]:
        nodes = manager.nodes
        if not nodes:
            raise NoNodesAvailableError(
                "No nodes available",
                scope_name=scope_name,
            )

        affinity_target: AffinityTarget | None = None
        if compute_affinity_node is not None:
            # Check operation name first (cheap header read) before
            # parsing the JSON body (expensive)
            operation_name = extract_operation_name(request)
            if operation_name in _affinity_operations:
                params = extract_request_params(request)
                affinity_target = compute_affinity_node(
                    operation_name,
                    params,
                    nodes,
                )
        return create_query_plan(nodes, affinity_target)

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

    # Register response compression handlers if enabled
    if config.response_compression:
        accept_encoding_handler = create_response_compression_request_handler(
            config.response_compression,
        )
        decode_response_handler = create_response_compression_decode_handler()
        events.register("before-send.dynamodb.*", accept_encoding_handler)
        events.register("before-parse.dynamodb.*", decode_response_handler)

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

    user_agent_handler = create_user_agent_header_handler(user_agent)
    events.register_last("before-send.dynamodb.*", user_agent_handler)
