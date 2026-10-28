# DynamoDB response parser measurements

Alternator automatically compiles successful DynamoDB response shapes once per
sync client, resource client, or async client. There is no configuration switch
or additional dependency. The native backend and other AWS services are outside
this change.

## Compatibility

Only SDK shape traversal is replaced. SDK JSON decoding, error parsing (including
modeled fields), checksums, headers, status, request IDs, retry metadata, events,
and resource conversions retain their existing paths. Unsupported shapes,
malformed containers, custom factories, custom JSON parser registrations, and
streaming responses retain stock/custom handling. Blob and timestamp defaults
present at installation are copied into the compiled plans. Later defaults
updates through the original or installed factory use stock parsing with the
updated converters. Replacing the SDK's global timestamp converter also uses
stock parsing when the factory has no explicit timestamp converter. Restoring
the original global converter allows the existing plans to be used again.
Later JSON parser registrations and original-factory method
changes also delegate to the SDK. Existing class-level factory overrides remain
untouched, including wrappers and descriptors. Installation checks existing
registrations without constructing custom parsers; later opaque factory
callables delegate without inspecting method metadata. Each response gets a
fresh parser; each client owns its compiled plans and endpoint factory.

Differential tests exercise every output and modeled error shape in the
installed DynamoDB model, recursive AttributeValue values, sparse lists, nulls,
empty collections, serialized names, field order, reference traits, and unknown
fields. Real loopback HTTP tests exercise gzip/deflate, signing, retries,
concurrency, response hooks, isolation, and boto3 Decimal/set/Binary conversions.
Before the rebase and recursion-depth fix, the focused suite contained 499 tests.
The recorded full unit suites (including the existing Python transport contracts)
passed on CPython 3.10–3.14: 1,447 passed with three skips on 3.10/3.12/3.13,
and 1,450 passed on 3.11/3.14. Repository lint and strict type checks passed;
recorded coverage was approximately 90% overall and 100% for the new parser
module. These historical runs used boto3/botocore 1.43.117 and aiobotocore 3.10.0.
They do not validate the rebased tree or the recursion-depth fix, or establish
compatibility with every future SDK release.

## CPU check after the timestamp-default compatibility fix

After the rebase and the timestamp-default compatibility fix, a focused check
used CPython 3.10.18, boto3/botocore 1.43.75 and aiobotocore 3.9.1 on CPU 12 of an
Intel Core i7-13700H host.
Five alternating paired trials per workload retained parameter validation,
unsigned requests and transport stubbing. Each variant used the current code;
only the endpoint parser factory differed. The calibration target was 0.5
seconds. Results are medians of paired changes:

| Mode | Workload | Stock µs/op | Compiled µs/op | Paired change | Pair range |
| --- | --- | ---: | ---: | ---: | ---: |
| Sync | Nested GetItem | 942.1 | 528.7 | -43.7% | -44.9% to -43.4% |
| Sync | Query (25 items) | 19083.4 | 7519.3 | -60.8% | -62.3% to -51.4% |
| Async | Nested GetItem | 990.7 | 573.7 | -42.1% | -42.7% to -42.0% |
| Async | Query (25 items) | 17964.9 | 8310.4 | -54.0% | -64.2% to -47.6% |

[Raw pairs, environment, source hashes and summary](response-parser-results/timestamp-default-cpu/)
bind this check to the measured parser source. To reproduce a row, start the
benchmark's `server` mode, use its printed port, then run `cpu --port PORT
--cpu 12 --workload get_nested --trials 5 --duration 0.5`. Use `query25` for
Query and add `--asynchronous` for async calls.

The current paired medians exceed
the 30% GetItem and 50% Query thresholds; individual trials vary, as shown above.

The [earlier compatibility CPU check](response-parser-results/factory-cpu/)
is retained with its original sources and predates the timestamp-default fix.
The current CPU results cover that fix and the rebased client code. They
do not repeat the HTTP quota or allocation measurements below. Different
interpreter and SDK versions also prevent direct comparison of absolute timings
with the historical runs.

## Method

The measurements below were recorded before the rebase and recursion-depth fix.
The rebase changed synchronous connection-pool setup, and the depth fix changed
structure traversal. The retained results describe their recorded source and
workload; they do not establish exact performance of the current tree.

Run from the repository root on a Linux host with user systemd CPU accounting:

```sh
uv run python scripts/bench_response_parser.py run \
  --output /tmp/response-parser-results --cpu 12 --trials 5 --duration 1.5
uv run python scripts/bench_response_parser.py summarize \
  --output /tmp/response-parser-results > /tmp/response-parser-summary.json
```

Choose a permitted client CPU for `--cpu`; CPU 12 is specific to this machine.
The HTTP peer runs in a separate process on the highest allowed CPU, outside the
client quota. The measured machine has an Intel Core i7-13700H; the quiet-core
client runs use CPU 12 (no simultaneous multithreading sibling) and server CPU 19.
CPU/network workers are pinned; startup measurements use the normal scheduler.
The retained environment includes Python/SDK/kernel versions, CPU affinity,
source hashes, benchmark hash, timestamp, and the source base/diff.

Both variants ran identical Alternator code at measurement time with parameter
validation enabled, unsigned requests, and all other settings equal. The stock
variant restores only the endpoint's original parser factory. Clients are warmed
and reused. CPU/op measures complete SDK calls with only HTTP transport stubbed;
JSON decoding, shape parsing, serialization, validation, routing, and SDK events
still execute. Five paired trials alternate execution order, with CPU trials
also interleaving eight smaller blocks per pair. Values below are medians of
paired changes, so they need not equal ratios of the separately reported medians.

HTTP trials use a real persistent loopback HTTP connection and an asyncio peer
serving precomputed DynamoDB JSON. Each side targets 1.5 seconds in
each of five alternating pairs, at concurrency 1 and 8, under both 0.25-CPU and
1-CPU cgroup quotas. Raw rows retain counts, elapsed time, process CPU/op,
throughput, mean/p99 latency, and cgroup throttling counters.

Nested items contain 16 maps with strings, numeric strings, blobs and lists,
plus a binary set. Multi-item responses contain 25 such items. BatchGetItem
requests one key and returns 25 items to isolate response parsing; it models
parser cost rather than valid service cardinality. PutItem writes a small item
and returns `{}`; empty GetItem also returns `{}`.

## Complete SDK CPU/op

Negative changes mean lower CPU cost. The acceptance thresholds are at least
30% lower CPU/op for nested GetItem and 50% lower for multi-item Query.

| Mode | Workload | Stock µs/op | Compiled µs/op | Paired change | Pair range |
| --- | --- | ---: | ---: | ---: | ---: |
| Sync | Nested GetItem | 643.6 | 400.8 | -38.1% | -38.5% to -37.7% |
| Sync | Query (25 items) | 11214.1 | 3704.8 | -67.1% | -67.3% to -61.2% |
| Sync | Scan (25 items) | 11389.5 | 3677.8 | -67.7% | -68.2% to -62.2% |
| Sync | BatchGetItem (25 returned items) | 11095.5 | 3739.0 | -66.3% | -67.0% to -60.5% |
| Sync | Small PutItem | 248.8 | 246.3 | -0.2% | -3.3% to +0.7% |
| Sync | Empty GetItem | 237.2 | 237.7 | -0.8% | -1.0% to +1.1% |
| Async | Nested GetItem | 706.3 | 448.6 | -36.7% | -36.7% to -36.1% |
| Async | Query (25 items) | 10262.1 | 6183.9 | -39.8% | -63.4% to -38.4% |
| Async | Scan (25 items) | 10209.7 | 6191.1 | -39.4% | -63.9% to -38.1% |
| Async | BatchGetItem (25 returned items) | 10163.7 | 6243.2 | -38.6% | -62.8% to -38.0% |
| Async | Small PutItem | 299.7 | 298.2 | -0.4% | -0.6% to +0.8% |
| Async | Empty GetItem | 285.4 | 284.6 | -0.4% | -0.8% to +0.2% |
| Async (longer trials) | Query (25 items) | 11260.0 | 4530.3 | -60.2% | -64.0% to -56.5% |

The short async multi-item CPU trials contain only 16 operations per side
(two per interleaved block), and the Query result varies substantially between
pairs. The longer Query row uses nine alternating pairs of 128 operations per
side on CPU 14, another core without a simultaneous multithreading sibling.
It retains normal garbage collection and the same SDK/parser settings. Use
this larger sample for the async Query acceptance decision; both short and
extended measurements are retained. The extended run reduces CPU/op by 60.2%
(median), with individual reductions of 56.5%–64.0%.

## Real HTTP under CPU quotas

Positive throughput changes and negative mean latency changes are improvements.
The full summary retains every workload and p99 latency.

| Mode | CPU quota | Concurrency | GetItem throughput | Query throughput | GetItem mean latency | Query mean latency |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Sync | 0.25 | 1 | +30.1% | +134.3% | -23.1% | -57.3% |
| Sync | 0.25 | 8 | +48.1% | +129.0% | -28.8% | -51.9% |
| Sync | 1 | 1 | +34.0% | +122.7% | -25.4% | -55.1% |
| Sync | 1 | 8 | +33.7% | +136.5% | -25.2% | -57.6% |
| Async | 0.25 | 1 | +34.1% | +98.0% | -25.4% | -49.5% |
| Async | 0.25 | 8 | +27.6% | +136.3% | -24.5% | -55.3% |
| Async | 1 | 1 | +28.8% | +118.5% | -22.4% | -54.2% |
| Async | 1 | 8 | +35.4% | +129.5% | -26.1% | -55.8% |

Small-response ranges below span the four quota/concurrency combinations,
using the paired median within each combination.

| Mode | Workload | CPU/op change range | Throughput change range |
| --- | --- | ---: | ---: |
| Sync | Small PutItem | -9.4% to -0.1% | +0.8% to +12.3% |
| Sync | Empty GetItem | -1.6% to +0.2% | -2.6% to +0.9% |
| Async | Small PutItem | -7.7% to +1.2% | -0.8% to +8.3% |
| Async | Empty GetItem | -1.6% to +0.6% | -0.8% to +2.2% |

## Startup and memory

These are incremental factory/plan installation costs after SDK client/model
construction. Five samples measure untraced CPU/wall time and separately measure
Python allocations with tracemalloc. Retained memory excludes preexisting SDK
models and the base client; peak memory includes temporary compilation storage.

| Mode | CPU time | Wall time | Retained allocations | Peak allocations |
| --- | ---: | ---: | ---: | ---: |
| Sync | 4.95 ms | 4.93 ms | 707 KiB | 726 KiB |
| Async | 5.26 ms | 5.26 ms | 710 KiB | 728 KiB |

## Evidence and limits

[Recorded quiet-core samples, environment, and summary](response-parser-results/quiet-core/)
and the [longer async Query CPU trials](response-parser-results/extended-async-query/)
are retained alongside the [initial shared-core run](response-parser-results/).
The initial CPU-0 HTTP run showed large positive and negative small-response
swings (including a median regression above 5% in some combinations). The
subsequent CPU-12 run repeats all workloads and settings rather than removing
unfavorable samples. Source hashes establish that both runs use the same parser
implementation. The initial environment also retains its benchmark source;
the quiet-core harness adds an explicit client CPU selection.

The `factory-cpu`, `quiet-core`, and `extended-async-query` environments record
benchmark SHA-256 `b6478b4a…` for the measured script revision.
The initial shared-core environment instead records `c5f7647b…` and embeds that
benchmark source. Later lifecycle changes affect only untimed client
acquisition, setup, and cleanup. The subsequent summary-only repair retains
groups with no complete stock/compiled pair as `status: no_complete_pairs`,
including their incomplete trial numbers and no metrics. Neither change alters
the timed regions. The checked-in script's hash therefore differs from those
historical benchmark hashes. The timestamp-default CPU check records the current
script and parser hashes; it does not repeat the historical HTTP quota or
allocation measurements. The commands above run the current harness, while
retained samples remain bound to their recorded benchmark and parser sources.

These results describe Python client CPU cost against a deterministic loopback
peer. They do not predict ScyllaDB throughput, remote-network performance,
server execution time, signing overhead, other item layouts, unlimited
concurrency, or short-lived client economics. CPU frequency, other machine
activity, garbage collection and cgroup period boundaries affect individual
pairs, especially p99 latency. Construction cost is paid once per client, so
long-lived clients benefit most. SDK upgrades should rerun the differential
suite and benchmark before relying on these specific measurements.
