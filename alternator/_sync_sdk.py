# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Per-client adjustments to the synchronous SDK transport."""

from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING, Any, cast

from botocore.httpsession import URLLib3Session
from urllib3 import PoolManager
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import LocationParseError
from urllib3.poolmanager import PoolKey
from urllib3.util import parse_url

from alternator.core.live_nodes import _format_host_port

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

    from alternator.config import Config
    from alternator.core.live_nodes import SyncLiveNodesManager


def configure_sync_connection_pools(
    client: DynamoDBClient,
    manager: SyncLiveNodesManager,
    config: Config,
) -> None:
    """Keep a host pool for every routable node, including after discovery.

    Botocore configures urllib3's per-host connection count but leaves its
    cache of host pools at ten. A larger cluster would continually evict pools.
    Resize that cache in place so existing connections and SDK TLS/proxy
    settings survive. The cache only grows, avoiding churn during refreshes.
    """
    session = cast(Any, client)._endpoint.http_session
    if not isinstance(session, URLLib3Session):
        return
    transport = cast(Any, session)
    original_proxy_manager = transport._get_proxy_manager
    lock = Lock()
    configured: set[PoolManager] = set()

    def configure_manager(pool_manager: PoolManager) -> None:
        if pool_manager in configured:
            return
        original_lookup = pool_manager.connection_from_pool_key
        pools = pool_manager.pools

        def lookup(
            pool_key: PoolKey, request_context: dict[str, Any]
        ) -> HTTPConnectionPool:
            with pools.lock:
                if pools.get(pool_key) is None:
                    # Read snapshots after acquiring the cache lock so a
                    # waiting lookup cannot promote an obsolete topology over
                    # current pools inserted while it waited.
                    ordinary = manager.nodes.nodes
                    affinity = manager.affinity_nodes.nodes
                    hosts = set(config.seed_hosts)
                    hosts.update(ordinary)
                    hosts.update(affinity)
                    normalized = set()
                    for host in hosts:
                        try:
                            normalized.add(
                                parse_url(
                                    f"{config.scheme}://{_format_host_port(host, config.port)}"
                                ).host
                            )
                        except LocationParseError:
                            # An unrelated malformed discovery host must not
                            # make a valid request fail. The SDK validates the
                            # selected destination through its normal path.
                            continue
                    pools._maxsize = max(
                        pools._maxsize,
                        len(hosts) + (pool_key.key_host not in normalized),
                    )
                    # Keep current pools newer than retired pools, so the
                    # SDK's insertion evicts a retired entry first. Promotion
                    # and insertion share its reentrant cache lock.
                    for key in pools.keys():  # noqa: SIM118 -- urllib3's cache disallows iteration
                        if key.key_host in normalized:
                            pools.get(key)
                return original_lookup(pool_key, request_context)

        cast(Any, pool_manager).connection_from_pool_key = lookup
        configured.add(pool_manager)

    def get_proxy_manager(proxy_url: str) -> PoolManager:
        # Serialize lazy proxy creation inside the SDK send path so its
        # exception mapping and retry handling still apply. Network I/O
        # runs after this getter, outside the lock.
        with lock:
            pool_manager: PoolManager = original_proxy_manager(proxy_url)
            configure_manager(pool_manager)
            return pool_manager

    configure_manager(transport._manager)
    transport._get_proxy_manager = get_proxy_manager
