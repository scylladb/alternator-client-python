# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Per-client adjustments to the synchronous SDK transport."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from botocore.httpsession import URLLib3Session

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

    def grow_pools(**kwargs: object) -> None:
        hosts = set(config.seed_hosts)
        hosts.update(manager.nodes.nodes)
        hosts.update(manager.affinity_nodes.nodes)
        # Proxy managers are created lazily by botocore. The first request
        # creates only one pool; subsequent sends resize the new manager too.
        pool_managers = [transport._manager, *transport._proxy_managers.values()]
        for pool_manager in pool_managers:
            pools = pool_manager.pools
            # urllib3 has no public resize API. Use its own cache lock so
            # concurrent requests cannot race with eviction or another resize.
            with pools.lock:
                pools._maxsize = max(pools._maxsize, len(hosts))

    grow_pools()
    client.meta.events.register_first("before-send.dynamodb.*", grow_pools)
