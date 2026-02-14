"""Shared event handlers for Alternator clients."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from alternator.config import CompressionAlgorithm
from alternator.core.compression import create_compression_handler
from alternator.core.headers import (
    compute_header_whitelist,
    create_header_filter_handler,
)
from alternator.core.request import extract_operation_name, extract_request_params

if TYPE_CHECKING:
    from alternator.config import AlternatorConfig

# Type alias for DynamoDB request parameters (inherently flexible key-value structure)
DynamoDBParams = dict[str, Any]

# Attribute name for storing execution plan on request object
_EXECUTION_PLAN_ATTR = "_alternator_execution_plan"


def register_alternator_handlers(
    events: Any,
    create_execution_plan: Callable[[int | None], Iterator[str]],
    config: AlternatorConfig,
    compute_affinity_hash: Callable[[str, DynamoDBParams], int | None] | None = None,
) -> None:
    """
    Register all Alternator event handlers on a boto3/aioboto3 client.

    This is shared between sync and async clients to avoid code duplication.

    Args:
        events: The boto3/aioboto3 events object to register handlers on
        create_execution_plan: Factory that creates ExecutionPlan instances
        config: Alternator configuration
        compute_affinity_hash: Optional function to compute partition key hash
                              for deterministic node ordering
    """

    # Operations that may use key affinity routing
    _affinity_operations = frozenset(
        {"PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem", "GetItem"}
    )

    # Register event handler to update endpoint per-request
    def update_endpoint(request: Any, **kwargs: Any) -> None:
        """Update request URL based on routing strategy."""
        # Get or create execution plan
        plan: Iterator[str] | None = getattr(request, _EXECUTION_PLAN_ATTR, None)
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
            plan = create_execution_plan(affinity_hash)
            setattr(request, _EXECUTION_PLAN_ATTR, plan)

        # Get next node from plan
        new_uri = next(plan)

        parsed = urlparse(request.url)
        request.url = f"{new_uri}{parsed.path}"
        if parsed.query:
            request.url += f"?{parsed.query}"

    events.register("before-send.dynamodb.*", update_endpoint)

    # Register compression handler if enabled
    if config.compression == CompressionAlgorithm.GZIP:
        compress_handler = create_compression_handler(config.min_compression_size_bytes)
        events.register("before-send.dynamodb.*", compress_handler)

    # Register header filter if optimization enabled
    if config.optimize_headers:
        whitelist = compute_header_whitelist(
            auth_enabled=config.authentication_enabled,
            compression_enabled=config.compression != CompressionAlgorithm.NONE,
            custom_whitelist=config.headers_whitelist,
        )
        header_filter = create_header_filter_handler(whitelist)
        events.register("before-send.dynamodb.*", header_filter)
