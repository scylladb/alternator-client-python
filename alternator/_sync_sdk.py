# Copyright ScyllaDB, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Per-client adjustments to the synchronous SDK transport."""

from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

from botocore.awsrequest import AWSPreparedRequest
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
    lock = Lock()

    def grow_pools(request: AWSPreparedRequest | None = None, **kwargs: object) -> None:
        # Serialize lazy proxy creation before send() can use the manager.
        # Network I/O runs after this callback, outside the lock.
        with lock:
            hosts = set(config.seed_hosts)
            hosts.update(manager.nodes.nodes)
            hosts.update(manager.affinity_nodes.nodes)
            pool_manager = transport._manager
            if request is not None:
                # A captured query plan can outlive its topology snapshot.
                host = urlsplit(request.url).hostname
                if host is not None:
                    hosts.add(host)
                proxy_url = transport._proxy_config.proxy_url_for(request.url)
                pool_manager = transport._get_connection_manager(request.url, proxy_url)
            pools = pool_manager.pools
            # urllib3 has no public resize API. Use its own cache lock so
            # concurrent requests cannot race with eviction or another resize.
            with pools.lock:
                pools._maxsize = max(pools._maxsize, len(hosts))

    grow_pools()
    client.meta.events.register_first("before-send.dynamodb.*", grow_pools)
