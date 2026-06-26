# Alternator Load Balancing Client for Python

A Python library that provides client-side load balancing for [ScyllaDB Alternator](https://docs.scylladb.com/stable/alternator/), wrapping boto3/aioboto3 to transparently distribute requests across cluster nodes.

## Features

- **Automatic Load Balancing**: Distributes requests across all available Alternator nodes using round-robin selection
- **Node Discovery**: Automatically discovers cluster topology via the `/localnodes` endpoint
- **Topology Awareness**: Route requests to specific datacenters or racks
- **Key Affinity Routing**: Optimizes LWT (Lightweight Transaction) operations by routing requests for the same partition key to the same node
- **Request Compression**: Optional gzip compression to reduce bandwidth
- **Header Optimization**: Filters unnecessary headers to reduce request overhead
- **TLS Support**: Full TLS/SSL support with custom CA certificates
- **Async Support**: Full async/await support via aioboto3

## Installation

```bash
# Basic installation (sync client only)
pip install alternator-client

# With async support
pip install alternator-client[async]
```

> **Note:** The PyPI package name is `alternator-client`, but the Python import remains `alternator`.

## Quick Start

### Synchronous Client

For the common case, use the top-level `alternator.client` context manager.
Seeds are host names or IP addresses only; use `port` for the single Alternator
port.

```python
import alternator

with alternator.client(
    seeds=["192.168.1.1", "192.168.1.2"],
    port=8000,
) as client:
    response = client.list_tables()
    print(response["TableNames"])
```

```python
from alternator import Config, AlternatorClient

# Configure the client
config = Config(
    seed_hosts=["192.168.1.1", "192.168.1.2"],
    port=8000,
)

# Use as a context manager (recommended)
with AlternatorClient(config) as client:
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
from alternator.async_client import AsyncAlternatorClient

async def main():
    config = Config(
        seed_hosts=["192.168.1.1"],
        port=8000,
    )

    async with AsyncAlternatorClient(config) as client:
        # Use like a normal aioboto3 DynamoDB client
        response = await client.list_tables()
        print(response["TableNames"])

asyncio.run(main())
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

> **Compatibility:** `AlternatorConfig` and `TlsConfig` remain available for
> existing callers, but are deprecated. Prefer `Config` and `TLS` for new code.

### Using the Builder Pattern

```python
from alternator import (
    AlternatorConfigBuilder,
    CompressionAlgorithm,
    KeyRouteAffinityMode,
    TLS,
)

config = (
    AlternatorConfigBuilder()
    .with_seeds("node1.example.com", "node2.example.com")
    .with_port(8000)
    .with_https(TLS.system_default())
    .with_datacenter("us-east-1")
    .with_compression(CompressionAlgorithm.GZIP, min_size=1024)
    .with_key_affinity(KeyRouteAffinityMode.RMW)
    .with_refresh_intervals(active_ms=1000, idle_ms=60000)
    .build()
)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `seed_hosts` | `Sequence[str]` | (required) | Initial nodes for cluster discovery |
| `port` | `int` | (required) | Alternator port |
| `scheme` | `str` | `"http"` | Protocol scheme (`"http"` or `"https"`) |
| `routing_scope` | `RoutingScope` | `ClusterScope()` | Topology-aware routing |
| `compression` | `CompressionAlgorithm` | `NONE` | Request compression |
| `min_compression_size_bytes` | `int` | `1024` | Minimum body size to compress |
| `optimize_headers` | `bool` | `False` | Enable header filtering |
| `headers_whitelist` | `frozenset[str]` | `None` | Additional headers to keep |
| `tls` | `TLS` | system default | TLS configuration |
| `key_affinity` | `KeyRouteAffinityConfig` | `NONE` | Key-based routing |
| `max_pool_connections` | `int` | `200` | Max connections per host |
| `active_refresh_interval_ms` | `int` | `1000` | Node refresh interval when active |
| `idle_refresh_interval_ms` | `int` | `60000` | Node refresh interval when idle |

## Authentication

Authentication is disabled by default. Alternator authentication in this client
supports static credentials only; AWS SDK environment, profile, and provider-chain
credentials are not used for Alternator auth.

```python
from alternator import Auth, AlternatorClient, Config

config = Config(seed_hosts=["node1"], port=8000)

# Default: unsigned requests
with AlternatorClient(config, auth=Auth.disabled()) as client:
    client.list_tables()

# Signed requests with static Alternator credentials
with AlternatorClient(
    config,
    auth=Auth.static_credentials("alternator", "secret"),
) as client:
    client.list_tables()
```

Passing raw boto credential kwargs such as `aws_access_key_id` still works for
compatibility, but is deprecated. Prefer `auth=Auth.static_credentials(...)`.

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

# Route to nodes in a specific datacenter
config = Config(
    seed_hosts=["node1"],
    port=8000,
    routing_scope=DatacenterScope(datacenter="us-east-1"),
)

# Route to nodes in a specific rack (with fallback)
config = Config(
    seed_hosts=["node1"],
    port=8000,
    routing_scope=RackScope(datacenter="us-east-1", rack="rack1"),
)
```

Scopes automatically fall back to broader scopes if no nodes are available:
- `RackScope` → `DatacenterScope` → `ClusterScope`

## Key Affinity (LWT Optimization)

For Lightweight Transactions (conditional writes), routing requests for the same partition key to the same node can improve performance:

```python
from alternator import (
    AlternatorConfigBuilder,
    KeyRouteAffinityMode,
)

config = (
    AlternatorConfigBuilder()
    .with_seeds("node1")
    .with_port(8000)
    .with_key_affinity(
        mode=KeyRouteAffinityMode.RMW,  # Only for read-modify-write ops
        table_pk_map={"my_table": "pk"},  # Optional: preload PK names
    )
    .build()
)
```

### Affinity Modes

| Mode | Description |
|------|-------------|
| `NONE` | Disabled (default round-robin) |
| `RMW` | Only for operations with `ConditionExpression` or `ReturnValues` |
| `ANY_WRITE` | For all write operations (`PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`) |

## TLS Configuration

```python
from alternator import TLS, TlsSessionCacheConfig
from pathlib import Path

# Use system CA certificates (default)
tls = TLS.system_default()

# Use custom CA certificate
tls = TLS.with_custom_ca(Path("/path/to/ca.pem"))

# Trust all certificates (INSECURE - dev only)
tls = TLS.trust_all()

# Full configuration
tls = TLS(
    custom_ca_cert_paths=[Path("/path/to/ca.pem")],
    trust_system_ca_certs=True,
    verify_hostname=True,
    session_cache=TlsSessionCacheConfig(
        enabled=True,
        cache_size=1024,
        timeout_seconds=86400,
    ),
)
```

## Request Compression

Enable gzip compression for large request bodies:

> **Note:** Gzip request compression requires **ScyllaDB 2026.1.0 or later**. Earlier versions do not support the `Content-Encoding: gzip` header. Response compression (`Accept-Encoding: gzip`) is not yet supported by Alternator.

```python
from alternator import AlternatorConfigBuilder, CompressionAlgorithm

config = (
    AlternatorConfigBuilder()
    .with_seeds("node1")
    .with_port(8000)
    .with_compression(
        CompressionAlgorithm.GZIP,
        min_size=1024,  # Only compress bodies >= 1KB
    )
    .build()
)
```

## Error Handling

```python
from alternator import (
    AlternatorClient,
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
    with AlternatorClient(config) as client:
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

For table-oriented operations, use `AlternatorResource` which wraps boto3's DynamoDB resource:

```python
from alternator import Config, AlternatorResource

config = Config(seed_hosts=["192.168.1.1"], port=8000)

with AlternatorResource(config) as resource:
    table = resource.Table("my_table")
    table.put_item(Item={"pk": "user123", "data": "hello"})
    response = table.get_item(Key={"pk": "user123"})
```

You can also use the factory function:

```python
from alternator import create_resource, close_resource, Config

config = Config(seed_hosts=["node1"], port=8000)
resource = create_resource(config)

try:
    table = resource.Table("my_table")
    table.scan()
finally:
    close_resource(resource)
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
from alternator import create_client, close_client, Config

config = Config(seed_hosts=["node1"], port=8000)
client = create_client(config)

try:
    client.list_tables()
finally:
    close_client(client)  # Stop background refresh thread
```

Async equivalent:

```python
from alternator import Config
from alternator.async_client import create_async_client, close_async_client

config = Config(seed_hosts=["node1"], port=8000)
client = await create_async_client(config)

try:
    await client.list_tables()
finally:
    await close_async_client(client)
```

## Production Recommendations

- **Connection pool sizing**: The default `max_pool_connections=200` works for most workloads. Increase if you see connection pool exhaustion warnings under high concurrency.
- **Refresh intervals**: Default active refresh (1s) is appropriate for dynamic clusters. For stable clusters, increase `active_refresh_interval_ms` to reduce discovery overhead.
- **Timeouts**: Default `discovery_timeout_seconds=5.0` and `read_timeout_seconds=30.0` are conservative. Tune based on your network latency and query complexity.
- **Monitoring**: Enable `INFO`-level logging for the `alternator` logger to track node discovery events. Use `DEBUG` for detailed routing decisions during troubleshooting.
- **Seed hosts**: Configure at least 2-3 seed hosts for redundancy in case one seed is temporarily unavailable during startup.

## Thread Safety

Sync clients created by `create_client` / `AlternatorClient` are thread-safe: the underlying node selection, round-robin counter, and node list updates are all protected by locks. You can safely share a single client across multiple threads.

Async clients created by `create_async_client` / `AsyncAlternatorClient` are safe to use from multiple concurrent coroutines within the same event loop. Do not share an async client across different event loops.

## Known Limitations

- **Request Compression**: Gzip compression requires ScyllaDB 2026.1.0+. Response compression is not yet supported by Alternator.
- **TLS Session Cache Settings**: The `cache_size` and `timeout_seconds` parameters in `TlsSessionCacheConfig` are not currently used by Python's `ssl` module. Only the `enabled` flag controls session ticket behavior.
- **Async Key Affinity**: For async clients, partition key auto-discovery happens asynchronously. The first request for an unknown table will use round-robin routing while discovery runs in the background. Subsequent requests will use affinity. Preloading via `table_pk_map` avoids this initial miss.
- **Batch Operations**: `BatchWriteItem` key affinity is based on a deterministic `PutRequest` or `DeleteRequest` selected from the batch. Batches with items targeting different partition keys are not split by affinity target.

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

# Start local Scylla cluster for integration tests
make scylla-start
make test-integration
make scylla-stop
```

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting a pull request.
