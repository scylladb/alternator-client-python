# Alternator Load Balancing Client for Python

A Python library that provides client-side load balancing for [ScyllaDB Alternator](https://docs.scylladb.com/stable/alternator/), wrapping boto3/aioboto3 to transparently distribute requests across cluster nodes.

## Features

- **Automatic Load Balancing**: Distributes requests across all available Alternator nodes using round-robin selection
- **Node Discovery**: Automatically discovers cluster topology via the `/localnodes` endpoint
- **Topology Awareness**: Route requests to specific datacenters or racks
- **Key Affinity Routing**: Optimizes LWT (Lightweight Transaction) operations by routing requests for the same partition key to the same node
- **Request Compression**: Optional gzip request compression to reduce bandwidth
- **Response Compression**: Optional gzip/deflate response decompression
- **Header Optimization**: Filters unnecessary headers to reduce request overhead
- **TLS Support**: Full TLS/SSL support with custom CA certificates
- **Async Support**: Full async/await support via aioboto3

See [docs/CAPABILITY_MATRIX.md](docs/CAPABILITY_MATRIX.md) for the current
capability matrix and planned follow-up work.

## Installation

```bash
# Basic installation (sync client only)
pip install alternator-client

# With async support
pip install alternator-client[async]
```

> **Note:** The PyPI package name is `alternator-client`, but the Python import remains `alternator`.

## Quick Start

### Which API Should I Use?

Use `alternator.client("dynamodb", ...)` for the common synchronous case where
a context manager can own the SDK client and background node refresh.

Use `alternator.resource("dynamodb", ...)` for the boto3 table-oriented
resource interface.

Use `Session` or `AsyncSession` when one object should own client/resource
lifecycle and expose topology diagnostics such as node refresh, node inspection,
routing validation, and partition-key cache inspection.

### Synchronous Client

For the common case, use the top-level `alternator.client` context manager.
Seeds are host names or IP addresses only; use `port` for the single Alternator
port.

```python
import alternator

with alternator.client(
    "dynamodb",
    seeds=["192.168.1.1", "192.168.1.2"],
    port=8000,
) as client:
    response = client.list_tables()
    print(response["TableNames"])
```

```python
import alternator
from alternator import Config

# Configure the client
config = Config(
    seed_hosts=["192.168.1.1", "192.168.1.2"],
    port=8000,
)

with alternator.client("dynamodb", cluster_config=config) as client:
    # Use like a normal boto3 DynamoDB client
    response = client.list_tables()
    print(response["TableNames"])

    # Put an item
    client.put_item(
        TableName="my_table",
        Item={
            "pk": {"S": "user123"},
            "data": {"S": "Hello, World!"},
        }
    )
```

### Asynchronous Client

```python
import asyncio
from alternator import Config
from alternator.async_client import AsyncSession

async def main():
    config = Config(
        seed_hosts=["192.168.1.1"],
        port=8000,
    )

    async with AsyncSession(config) as session:
        client = await session.client("dynamodb")

        # Use like a normal aioboto3 DynamoDB client
        response = await client.list_tables()
        print(response["TableNames"])

asyncio.run(main())
```

### Session Facade

Use `Session` when you need explicit lifecycle control or diagnostics in addition
to standard boto3 clients and resources.

```python
from alternator import Config, Session

config = Config(seed_hosts=["192.168.1.1", "192.168.1.2"], port=8000)

with Session(config) as session:
    client = session.client("dynamodb")
    resource = session.resource("dynamodb")

    session.refresh_nodes()
    print(session.nodes)

    client.list_tables()
    resource.Table("my_table").get_item(Key={"pk": "user123"})
```

Async code can use `AsyncSession`:

```python
from alternator import Config
from alternator.async_client import AsyncSession

config = Config(seed_hosts=["192.168.1.1"], port=8000)

async with AsyncSession(config) as session:
    client = await session.client("dynamodb")
    await session.refresh_nodes()
    print(session.nodes)
    await client.list_tables()
```

## Configuration

### Basic Configuration

```python
from alternator import Config

config = Config(
    seed_hosts=["node1.example.com", "node2.example.com"],
    port=8000,
    scheme="http",  # or "https" for TLS
)
```

### Composed Configuration

```python
from alternator import (
    CompressionAlgorithm,
    Config,
    DatacenterScope,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
    NodeListPollingConfig,
    RequestCompressionConfig,
    ResponseCompression,
    TLS,
)

config = Config(
    seed_hosts=["node1.example.com", "node2.example.com"],
    port=8000,
    scheme="https",
    tls=TLS.system_default(),
    routing_scope=DatacenterScope("us-east-1"),
    request_compression=RequestCompressionConfig(
        algorithm=CompressionAlgorithm.GZIP,
        min_size_bytes=1024,
    ),
    response_compression=(ResponseCompression.GZIP,),
    key_affinity=KeyRouteAffinityConfig(mode=KeyRouteAffinityMode.RMW),
    node_list_polling=NodeListPollingConfig(
        active_interval_ms=1000,
        idle_interval_ms=60000,
    ),
)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `seed_hosts` | `Sequence[str]` | (required) | Initial nodes for cluster discovery |
| `port` | `int` | (required) | Alternator port |
| `scheme` | `str` | `"http"` | Protocol scheme (`"http"` or `"https"`) |
| `routing_scope` | `RoutingScope` | `ClusterScope()` | Topology-aware routing |
| `request_compression` | `RequestCompressionConfig` | disabled; 1 KiB threshold, level 9 when enabled | Request gzip settings |
| `response_compression` | `Sequence[ResponseCompression]` | empty | Accepted response compression encodings |
| `header_optimization` | `HeaderOptimizationConfig` | disabled | Header filtering settings |
| `tls` | `TLS` | system default | TLS trust, client certificates, and key logging |
| `key_affinity` | `KeyRouteAffinityConfig` | `NONE` | Key-based routing |
| `retries` | `RetryConfig` | standard, 3 attempts | SDK retry behavior |
| `max_pool_connections` | `int` | `200` | Max connections per host |
| `timeouts` | `TimeoutConfig` | discovery 5s, connect 5s, read 30s | Discovery and SDK per-attempt timeouts |
| `aws_region` | `str` | `"us-east-1"` | Region placeholder required by the SDK |
| `user_agent` | str, callable, or `None` | `alternator-client-python/<version>` | Final User-Agent; `None` omits the wire header |
| `node_list_polling` | `NodeListPollingConfig` | active 1s, idle 60s | Node refresh intervals |

## Authentication

Authentication is disabled by default. Alternator authentication in this client
supports static credentials only; AWS SDK environment, profile, and provider-chain
credentials are not used for Alternator auth.

```python
import alternator
from alternator import Auth, Config

config = Config(seed_hosts=["node1"], port=8000)

# Default: unsigned requests
with alternator.client(
    "dynamodb",
    cluster_config=config,
    auth=Auth.disabled(),
) as client:
    client.list_tables()

# Signed requests with static Alternator credentials
with alternator.client(
    "dynamodb",
    cluster_config=config,
    auth=Auth.static_credentials("alternator", "secret"),
) as client:
    client.list_tables()
```

You can also pass boto-style credential kwargs such as `aws_access_key_id`.
Prefer `auth=Auth.static_credentials(...)` when you want the authentication
choice to be explicit in Alternator code.

## Comparing with a Regular AWS SDK Client

An Alternator client is a boto3 DynamoDB client configured with ScyllaDB
Alternator node discovery and load balancing. A regular AWS SDK client uses the
normal AWS DynamoDB regional endpoint and AWS SDK credential chain.

```python
import boto3
import alternator

with alternator.client(
    "dynamodb",
    seeds=["node1.example.com", "node2.example.com"],
    port=8000,
) as alternator_client:
    aws_client = boto3.client("dynamodb", region_name="us-east-1")

    print("Alternator endpoint:", alternator_client.meta.endpoint_url)
    print("AWS endpoint:", aws_client.meta.endpoint_url)

    print("Alternator tables:", alternator_client.list_tables()["TableNames"])
    # Requires normal AWS credentials:
    # print("AWS tables:", aws_client.list_tables()["TableNames"])
```

See `examples/compare_aws_sdk.py` for a runnable version. The example keeps
Alternator seeds host-only and uses one `port` setting for all seeds.

## Routing Scopes

Control which nodes receive your requests based on topology:

```python
from alternator import Config, ClusterScope, DatacenterScope, RackScope

# Route to any node in the cluster (default)
config = Config(
    seed_hosts=["node1"],
    port=8000,
    routing_scope=ClusterScope(),
)

# Route only to nodes in a specific datacenter
config = Config(
    seed_hosts=["node1"],
    port=8000,
    routing_scope=DatacenterScope(datacenter="us-east-1"),
)

# Route only to nodes in a specific rack
config = Config(
    seed_hosts=["node1"],
    port=8000,
    routing_scope=RackScope(datacenter="us-east-1", rack="rack1"),
)
```

`ClusterScope` queries every configured seed host and combines the returned
`/localnodes` results. Because ScyllaDB's optionless `/localnodes` endpoint
returns nodes from the contacted seed's local datacenter, cluster-wide routing
spans multiple datacenters only when the configuration includes at least one
reachable seed from each datacenter.

Default constructors stay constrained to their requested scope:

- `DatacenterScope("dc1")` tries only datacenter `dc1`
- `RackScope("dc1", "rack1")` tries only rack `rack1` in datacenter `dc1`

Use the named `fallback` argument when a broader fallback chain is desired:

```python
from alternator import ClusterScope, DatacenterScope, RackScope

cluster_only = ClusterScope()
datacenter_only = DatacenterScope("dc1")
datacenter_then_cluster = DatacenterScope("dc1", fallback=ClusterScope())
rack_only = RackScope("dc1", "rack1")
rack_then_datacenter = RackScope(
    "dc1",
    "rack1",
    fallback=DatacenterScope("dc1", fallback=None),
)
rack_then_datacenter_then_cluster = RackScope(
    "dc1",
    "rack1",
    fallback=DatacenterScope("dc1", fallback=ClusterScope()),
)
```

`Session.validate_scope()` validates the configured
scope by querying `/localnodes` with the configured datacenter and rack filters
without replacing the session's current live-node list.
`AsyncSession` exposes the same validation methods as awaitable methods.

## Key Affinity (LWT Optimization)

For Lightweight Transactions (conditional writes), routing requests for the same partition key to the same node can improve performance:

```python
from alternator import (
    Config,
    KeyRouteAffinityConfig,
    KeyRouteAffinityMode,
)

config = Config(
    seed_hosts=["node1"],
    port=8000,
    key_affinity=KeyRouteAffinityConfig(
        mode=KeyRouteAffinityMode.RMW,  # Only for read-modify-write ops
        table_pk_attributes={"my_table": "pk"},  # Optional: preload PK names
    ),
)
```

### Affinity Modes

| Mode | Description |
|------|-------------|
| `NONE` | Disabled (default round-robin) |
| `RMW` | Only for write operations that require a read-before-write path |
| `ANY_WRITE` | For all write operations (`PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`) |

`RMW` mode applies affinity to conditional `PutItem`/`DeleteItem`, `ALL_OLD`
returns, and `UpdateItem` requests that need prior item state, including
non-empty update or condition expressions, `Expected`, selected `ReturnValues`,
`ADD`, and value-bearing `DELETE` attribute updates. `BatchWriteItem` does not
use affinity in `RMW` mode.

`ANY_WRITE` mode applies affinity to single-item writes using the request
partition key. For `BatchWriteItem`, each valid put/delete votes for its
preferred node. The request tries the unique winning node first and keeps the
remaining nodes in the retry plan. Missing partition-key metadata, unsupported
key values, no active nodes, no votes, or tied votes fall back to normal routing.

## TLS Configuration

```python
from alternator import TLS
from pathlib import Path

# Use system CA certificates (default)
tls = TLS.system_default()

# Use custom CA certificate
tls = TLS.with_custom_ca(Path("/path/to/ca.pem"))

# Trust all certificates (INSECURE - dev only)
tls = TLS.trust_all()

# Mutual TLS with separate certificate and key files
tls = TLS(
    custom_ca_cert_paths=[Path("/path/to/ca.pem")],
    client_cert_path=Path("/path/to/client.crt"),
    client_key_path=Path("/path/to/client.key"),
)

# Mutual TLS with a combined certificate/key PEM file
tls = TLS(
    custom_ca_cert_paths=[Path("/path/to/ca.pem")],
    client_cert_path=Path("/path/to/client-combined.pem"),
)

# Debug TLS traffic with a key log file
tls = TLS(
    custom_ca_cert_paths=[Path("/path/to/ca.pem")],
    key_log_file_path=Path("/secure/tmp/alternator-tls.keys"),
)

# Full configuration
tls = TLS(
    custom_ca_cert_paths=[Path("/path/to/ca.pem")],
    trust_system_ca_certs=True,
    verify_hostname=True,
    session_tickets_enabled=True,
    client_cert_path=Path("/path/to/client.crt"),
    client_key_path=Path("/path/to/client.key"),
    key_log_file_path=Path("/secure/tmp/alternator-tls.keys"),
)
```

Client certificate settings are loaded into the SSL context used for
`/localnodes` discovery and passed to the SDK as `client_cert` for HTTPS
DynamoDB API calls. If you configure custom server CA certificates, continue to
pass the matching SDK `verify` argument or an equivalent SDK setting for API
calls.

TLS key logs contain traffic decryption material. Store them only in protected
temporary locations, delete them after debugging, and never commit them. Key log
support depends on Python/OpenSSL exposing `SSLContext.keylog_filename`; runtimes
without that attribute ignore `key_log_file_path`.

## Request And Response Compression

Enable gzip compression for large request bodies:

> **Note:** Gzip request compression requires **ScyllaDB 2026.1.0 or later**. HTTP response compression requires an Alternator build that includes ScyllaDB core response-compression support from `scylladb/scylladb#27454`.

```python
from alternator import (
    CompressionAlgorithm,
    Config,
    RequestCompressionConfig,
    ResponseCompression,
)

config = Config(
    seed_hosts=["node1"],
    port=8000,
    request_compression=RequestCompressionConfig(
        algorithm=CompressionAlgorithm.GZIP,
        min_size_bytes=1024,  # Only compress bodies >= 1KB
        gzip_level=6,   # Python gzip level 0-9; default is 9
    ),
    response_compression=(
        ResponseCompression.GZIP,
        ResponseCompression.DEFLATE,
    ),
)
```

Compression uses Python's `gzip.compress` implementation. Levels `0` through
`9` are accepted: lower levels spend less CPU and usually produce larger bodies;
higher levels spend more CPU and usually produce smaller bodies. The client only
sends compressed bodies when the compressed payload is smaller than the original
payload.

Response compression is disabled by default. When enabled, the client sends
`Accept-Encoding` with the configured encodings and decodes `Content-Encoding:
gzip` or `Content-Encoding: deflate` responses before boto3/aioboto3 parses the
DynamoDB JSON body.

## Header Optimization

Header optimization remains opt-in. Required protocol, compression, and auth
headers are preserved automatically. Use `whitelist` for static additions and
`whitelist_callback` when the allowed headers depend on configuration or auth
state:

```python
from alternator import Config, HeaderOptimizationConfig, HeaderWhitelistContext

def extra_headers(context: HeaderWhitelistContext) -> set[str]:
    if context.auth_enabled:
        return {"X-Service-Trace"}
    return {"X-Anonymous-Trace"}

config = Config(
    seed_hosts=["node1"],
    port=8000,
    header_optimization=HeaderOptimizationConfig(
        enabled=True,
        whitelist=frozenset({"X-Static-Header"}),
        whitelist_callback=extra_headers,
    ),
)
```

The callback returns additional headers to keep. It cannot remove the required
headers exposed in `context.required_headers`.

## Error Handling

```python
import alternator
from alternator import (
    Config,
    AlternatorError,
    NoNodesAvailableError,
    ConfigurationError,
)

try:
    config = Config(seed_hosts=[], port=8000)
except ConfigurationError as e:
    print(f"Invalid configuration: {e}")

try:
    with alternator.client("dynamodb", cluster_config=config) as client:
        client.list_tables()
except NoNodesAvailableError as e:
    print(f"No nodes available: {e}")
except AlternatorError as e:
    print(f"Alternator error: {e}")
```

## Logging

The library uses Python's standard logging module with the logger name `alternator`:

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("alternator").setLevel(logging.DEBUG)
```

Log levels:
- `INFO`: Node discovery events
- `WARNING`: Fallback events, connection issues
- `DEBUG`: Detailed routing decisions, node lists
- `ERROR`: Failed operations

## DynamoDB Resource Interface

For table-oriented operations, use `alternator.resource("dynamodb", ...)` which wraps boto3's DynamoDB resource:

```python
import alternator
from alternator import Config

config = Config(seed_hosts=["192.168.1.1"], port=8000)

with alternator.resource("dynamodb", cluster_config=config) as resource:
    table = resource.Table("my_table")
    table.put_item(Item={"pk": "user123", "data": "hello"})
    response = table.get_item(Key={"pk": "user123"})
```

You can also use `Session` when one object should own both lifecycle and
diagnostics:

```python
from alternator import Config, Session

config = Config(seed_hosts=["node1"], port=8000)

with Session(config) as session:
    resource = session.resource("dynamodb")
    table = resource.Table("my_table")
    table.scan()
```

## Vector Search (ScyllaDB Extension)

ScyllaDB Alternator supports vector similarity search, which is not part of the standard AWS DynamoDB API. All clients and resources created by this library have vector search support enabled automatically — no extra setup is needed.

> **Note:** Vector search requires ScyllaDB with Alternator vector search support enabled. These operations are not available on AWS DynamoDB. The feature is fully supported from ScyllaDB 2026.3, and only partially supported in ScyllaDB 2026.2: Version 2026.2 did not yet support the optimized "Vector" type, configurable SimilarityFunction, returning scores (ReturnScores), pre-filtering (KeyConditionExpression) or projected attributes (ProjectionType=INCLUDE).

### Creating a Table with a Vector Index

```python
client.create_table(
    TableName="embeddings",
    KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
    AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
    BillingMode="PAY_PER_REQUEST",
    VectorIndexes=[{
        "IndexName": "embedding_index",
        "VectorAttribute": {"AttributeName": "embedding", "Dimensions": 128},
        "SimilarityFunction": "COSINE",  # or "DOT_PRODUCT" / "EUCLIDEAN"
    }],
)
```

You can also add a vector index to an existing table via `UpdateTable`:

```python
client.update_table(
    TableName="embeddings",
    VectorIndexUpdates=[{
        "Create": {
            "IndexName": "embedding_index",
            "VectorAttribute": {"AttributeName": "embedding", "Dimensions": 128},
            "SimilarityFunction": "COSINE",
        }
    }],
)
```

### Storing and Querying Vectors

#### Low-level client (FLOAT32VECTOR wire format)

```python
# Store an item with a vector attribute
client.put_item(
    TableName="embeddings",
    Item={
        "id": {"S": "item1"},
        "embedding": {"FLOAT32VECTOR": [0.1, 0.2, 0.3, 0.4]},
    },
)

# Query by vector similarity (returns the k nearest neighbors)
result = client.query(
    TableName="embeddings",
    VectorSearch={
        "QueryVector": {"FLOAT32VECTOR": [0.1, 0.2, 0.3, 0.4]},
        "ReturnScores": "SIMILARITY",  # optional: include similarity scores
    },
    Limit=10,
)
for item in result["Items"]:
    print(item)
# If ReturnScores was set, similarity scores are in result["Scores"]
```

#### High-level resource interface (Vector type)

The `Vector` class is a `list` subclass that signals to Alternator that the
value should be stored as an array of 32-bit floats using the `FLOAT32VECTOR`
wire type. Without `Vector`, a list of numbers is serialized as a DynamoDB `L`
(list) of high-precision `N` (decimal) values — correct for arbitrary numbers,
but wasteful for embedding vectors where 32-bit precision is sufficient.
Using `Vector` for stored attributes reduces storage significantly (4 bytes per
element instead of a variable-length decimal string). For query vectors it
makes less difference, but using `Vector` consistently keeps the code uniform.

```python
from alternator.vector import Vector

table = resource.Table("embeddings")

# Store a vector (sent as FLOAT32VECTOR, stored as compact 32-bit floats)
table.put_item(Item={"id": "item1", "embedding": Vector([0.1, 0.2, 0.3, 0.4])})

# Query by vector similarity
result = table.query(
    VectorSearch={
        "QueryVector": Vector([0.1, 0.2, 0.3, 0.4]),
        "ReturnScores": "SIMILARITY",
    },
    Limit=10,
)
for item in result["Items"]:
    # embedding is automatically deserialized back as a Vector instance
    print(item["embedding"])
```

## Manual Resource Management

If you prefer not to use context managers:

```python
from alternator import Config, Session

config = Config(seed_hosts=["node1"], port=8000)

session = Session(config)
try:
    client = session.client("dynamodb")
    client.list_tables()
finally:
    session.stop()  # Stop background refresh thread and close created clients
```

Async equivalent:

```python
from alternator import Config
from alternator.async_client import AsyncSession

config = Config(seed_hosts=["node1"], port=8000)

session = AsyncSession(config)
try:
    client = await session.client("dynamodb")
    await client.list_tables()
finally:
    await session.stop()
```

## Transport Configuration

`TimeoutConfig.discovery_seconds` applies only to `/localnodes` discovery
requests. `TimeoutConfig.connect_seconds` and `TimeoutConfig.read_seconds` are
passed to botocore/aiobotocore as per-attempt SDK connect and read timeouts;
they are not whole-operation deadlines. Use application-level cancellation or
your own deadline wrapper for end-to-end call deadlines.

`RetryConfig`, `max_pool_connections`, `aws_region`, and SDK timeouts are passed
to the generated SDK config. `aws_region` is a placeholder required by the SDK;
Alternator request routing still uses discovered Alternator endpoints.

```python
from alternator import Config, RetryConfig, RetryMode, TimeoutConfig

config = Config(
    seed_hosts=["node1", "node2"],
    port=8000,
    retries=RetryConfig(max_attempts=4, mode=RetryMode.STANDARD),
    max_pool_connections=300,
    timeouts=TimeoutConfig(
        discovery_seconds=3.0,
        connect_seconds=2.0,
        read_seconds=10.0,
    ),
)
```

By default, Alternator sends `alternator-client-python/<version>` as the final
wire `User-Agent` header. Pass `None` to omit the header:

```python
import alternator
from alternator import Config

config = Config(
    seed_hosts=["node1", "node2"],
    port=8000,
    user_agent=None,
)
with alternator.client("dynamodb", cluster_config=config) as client:
    client.list_tables()
```

Pass a string to `user_agent` when you need to set a final value:

```python
config = Config(
    seed_hosts=["node1", "node2"],
    port=8000,
    user_agent="orders-service/1.0",
)
with alternator.client("dynamodb", cluster_config=config) as client:
    client.list_tables()
```

Pass a callback when you need to wrap or add to the default
`alternator-client-python/<version>` identity:

```python
config = Config(
    seed_hosts=["node1", "node2"],
    port=8000,
    user_agent=lambda default: f"orders-service {default}",
)
with alternator.client("dynamodb", cluster_config=config) as client:
    client.list_tables()
```

The client still owns the SDK config object, endpoint routing, and the final
wire `User-Agent` header. Use typed Alternator config fields for SDK transport
settings: `RetryConfig` for retry behavior, `TimeoutConfig` for connect/read
timeouts, `max_pool_connections` for pool sizing, `aws_region` for the SDK
region placeholder, and `TLS` for client certificates. Python botocore does not
expose direct knobs for max idle connections, max idle connections per host, or
idle connection timeout; tune `max_pool_connections`, retries, and timeouts
instead.

## Production Recommendations

- **Connection pool sizing**: The default `max_pool_connections=200` works for most workloads. Increase if you see connection pool exhaustion warnings under high concurrency.
- **Refresh intervals**: Default active refresh (1s) is appropriate for dynamic clusters. For stable clusters, set `node_list_polling=NodeListPollingConfig(active_interval_ms=...)` to reduce discovery overhead.
- **Timeouts**: Default `TimeoutConfig.discovery_seconds=5.0`, `connect_seconds=5.0`, and `read_seconds=30.0` are conservative. Tune based on your network latency and query complexity.
- **Monitoring**: Enable `INFO`-level logging for the `alternator` logger to track node discovery events. Use `DEBUG` for detailed routing decisions during troubleshooting.
- **Seed hosts**: Configure at least 2-3 seed hosts for redundancy in case one seed is temporarily unavailable during startup.

## Thread Safety

Sync clients created by `alternator.client("dynamodb", ...)` or `Session.client("dynamodb")` are thread-safe: the underlying node selection, round-robin counter, and node list updates are all protected by locks. You can safely share a single client across multiple threads.

Async clients created by `AsyncSession.client("dynamodb")` are safe to use from multiple concurrent coroutines within the same event loop. Do not share an async client across different event loops.

## Known Limitations

- **Request Compression**: Gzip request compression requires ScyllaDB 2026.1.0+.
- **Response Compression**: Response gzip/deflate decoding requires an Alternator build that includes `scylladb/scylladb#27454` and must be enabled explicitly with `Config.response_compression`.
- **Gzip Compression Levels**: Python's gzip module supports levels `0` through `9`; this client does not expose alternative compression algorithms or custom compressor objects.
- **TLS Session Tickets**: `TLS.session_tickets_enabled` controls session ticket behavior. Python's `ssl` module does not expose direct cache size or timeout controls.
- **TLS Key Logs**: Key log file support depends on Python/OpenSSL runtime support for `SSLContext.keylog_filename` and should only be used in protected debugging environments.
- **mTLS Integration Fixtures**: The local Scylla fixture in this repository does not require client certificate authentication, so automated tests cover configuration propagation and SSL context setup rather than a full mutual-TLS handshake.
- **Async Key Affinity**: For async clients, partition key auto-discovery happens asynchronously. The first request for an unknown table will use round-robin routing while discovery runs in the background. Subsequent requests will use affinity. Preloading `KeyRouteAffinityConfig.table_pk_attributes` avoids this initial miss.
- **Batch Operations**: `BatchWriteItem` key affinity in `ANY_WRITE` mode uses preferred-node voting across eligible put/delete entries. Ties, missing partition-key metadata, unsupported key values, no active nodes, or no eligible votes fall back to normal routing. Batches are not split by affinity target.
- **Node Health**: Node health, quarantine behavior, decommission handling, and dead-node handling are planning-only.

## Examples

- `examples/sync_demo.py`: synchronous client lifecycle and basic operations
- `examples/async_demo.py`: async client lifecycle and concurrent operations
- `examples/compare_aws_sdk.py`: Alternator client setup compared with a regular AWS SDK DynamoDB client
- `examples/capability_configuration.py`: session lifecycle, explicit routing fallback, static auth, timeouts/retries, mTLS, compression/header optimization, and key affinity configuration recipes

## Release Notes

See [docs/RELEASE_NOTES.md](docs/RELEASE_NOTES.md) for capability release-note
guidance covering additive APIs, API notes, behavior notes, review steps, and
versioning expectations.

## Development

```bash
# Clone the repository
git clone https://github.com/scylladb/alternator-client-python.git
cd alternator-client-python

# Install in development mode
make install

# Run tests
make test-unit

# Run linting
make lint

# Run mypy type checks
make typecheck

# Start local Scylla cluster for integration tests
make scylla-start
make test-integration
make scylla-stop
```

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting a pull request.
