# Header optimization

This specification defines how an Alternator client may reduce request size by removing HTTP
headers that Alternator does not require. Header names and observable wire behavior are normative.

The keywords **must**, **must not**, **should**, and **may** are normative.

## Purpose and scope

Header optimization is an opt-in transport feature. It filters the final outgoing header set while
preserving every header required by active authentication, compression, connection, operation, and
client-identification configuration.

Filtering must not change the request URI, method, body, response handling, retry routing, metrics,
or transport lifecycle.

## Vocabulary

| Term | Meaning |
| --- | --- |
| Header optimization | Removal of non-whitelisted outgoing ordinary HTTP headers immediately before transport execution. |
| Ordinary header | Named HTTP header field subject to whitelist filtering, including `Content-Length` when it is represented as a field; protocol pseudo-headers and protocol-native framing metadata are not ordinary headers. |
| Whitelist | Case-insensitive set of ordinary header names allowed on the wire. |
| Required headers | Minimum ordinary-header whitelist derived from enabled client features and the transport's protocol representation. |
| Base headers | Ordinary headers required for every Alternator request in that protocol representation. |
| Conditional headers | Headers required only when authentication, compression, user-agent reporting, or explicit HTTP/1.x connection behavior is enabled. |
| Main transport | Transport used for DynamoDB data-plane operations. |
| Polling transport | Transport used for topology discovery and health probes. |

## Configuration and defaults

Header optimization is disabled by default. An optional custom whitelist replaces the computed
default and may add application-specific headers. When supplied, a custom whitelist must be
non-empty, contain only present and non-empty header names, and contain the complete applicable
ordinary-header set for every protocol representation that the configured transports can use or
negotiate. The machine-readable default is recorded in [`defaults.tsv`](vectors/defaults.tsv).

Validation must be case-insensitive and independent of process locale. It must occur after all
feature settings are considered, even when optimization is disabled. Missing required headers are
configuration errors.

The stored whitelist and every exposed required-header set must be immutable snapshots. Mutating a
caller-owned input collection after configuration must not change client behavior.

## Required behavior

### Filtering

When disabled, the client must not remove transport-generated headers through this feature.

When enabled, the client must:

1. inspect the final request after normal request construction;
2. compare header names case-insensitively using locale-independent HTTP rules;
3. remove every ordinary header whose name is not in the effective whitelist;
4. preserve the original spelling and all values of every allowed ordinary header; and
5. pass the request body and all non-header transport metadata through unchanged.

Filtering applies independently to every attempt because attempt headers may be regenerated.
Whitelist membership is by header name, not by value.

### Required protocol fields

Every request requires the protocol semantics below. The effective default whitelist is the union
of applicable ordinary header names across every protocol representation that the configured
transports can use or negotiate. Pseudo-headers and protocol-native framing signals are outside the
scope of the ordinary-header whitelist, regardless of how a transport stores them, and must pass
through filtering unchanged.

#### Always required

| Protocol requirement | Ordinary-header representation | Transport-specific alternative |
| --- | --- | --- |
| HTTP authority and request signing | `Host` for HTTP/1.x | `:authority` or equivalent transport authority metadata for HTTP/2 |
| DynamoDB operation selection | `X-Amz-Target` | — |
| DynamoDB protocol media type | `Content-Type` | — |
| Request-body framing | `Content-Length` when the protocol uses it | HTTP/2 stream framing or equivalent transport metadata |

The authority value in the negotiated protocol representation must remain consistent with request
signing. HTTP/2 transports need not whitelist or emit ordinary `Host` or `Content-Length` fields
when their protocol-specific representations supply the same semantics.

#### Conditionally required

| Condition | Required headers |
| --- | --- |
| Authentication enabled | `Authorization`, `X-Amz-Date`, every ordinary header named by the signature, and authentication-token headers such as `X-Amz-Security-Token` when temporary credentials are used. A signed `host` requirement may instead use the protocol-specific authority representation above. |
| Request compression enabled | `Content-Encoding` |
| Response compression enabled | `Accept-Encoding` |
| User-agent reporting enabled | `User-Agent` |
| Explicit HTTP/1.x connection behavior configured | `Connection` |

`Connection` is required only for explicitly configured HTTP/1.x wire behavior. It must not be
preserved when the negotiated protocol forbids connection-specific fields, including HTTP/2.

Disabling a feature must remove only that feature's conditional requirement. Disabling
authentication, for example, does not remove base, compression, or user-agent requirements.

## Interactions with other features

### Authentication

When credentials are configured, signing headers and any session-token header are mandatory. When
anonymous access is used, authentication headers are not required and should be removed unless
explicitly retained for a separate application purpose.

Filtering must happen late enough to see signed requests, but it must not remove an ordinary header
or protocol-specific authority field required by the signature represented in `Authorization`.

### Request and response compression

Request compression requires `Content-Encoding`; response compression requires `Accept-Encoding`.
Changing either setting changes the computed minimum whitelist. See [Compression](compression.md).

### User-agent reporting

If user-agent reporting is enabled, `User-Agent` must reach the wire even when filtering is enabled.
If reporting is disabled, it is not required and must not be reintroduced by wrapper ordering.

### Query plans and retries

Per-attempt routing may depend on internal execution identifiers present before filtering. Routing
must consume required internal metadata before filtering removes it from the wire. Removing retry
metadata must not disable retry rerouting. See [Query plans](query-plan.md).

### Node health

Filtering must not change attempt outcome attribution. A filtered request is reported against the
endpoint selected before filtering, using [Node health](node-health.md) classification.

### Polling and control-plane requests

Header optimization is a main-transport feature. A polling transport may use a smaller independent
configuration, but it must retain headers required for its authentication and user-agent behavior.
Main-transport filtering must not accidentally wrap or close a caller-owned polling transport.

### Blocking and non-blocking transports

For the same protocol representation, both forms must produce the same filtered ordinary-header
map. When the forms use different protocol representations, their authority and framing fields may
differ. Filtering must preserve request-body delivery, response handling, streaming and duplex
behavior, instrumentation, transport execution metadata, completion, cancellation, and error
behavior exposed by the underlying transport.

## Edge cases

### Feature interactions

- Enabling request compression with a custom whitelist lacking `Content-Encoding` must fail.
- Enabling response compression with a custom whitelist lacking `Accept-Encoding` must fail.
- Disabling response compression makes a whitelist without `Accept-Encoding` valid.
- Disabling user-agent reporting makes a whitelist without `User-Agent` valid.
- Disabling authentication makes a whitelist without authentication headers valid.
- Retry routing must still work after invocation and retry metadata are removed from the wire.
- Route changes must update the effective destination before filtering without losing required
  authority behavior.
- Compression and signing may replace headers; filtering must inspect final values on every attempt.
- Closing the filtering wrapper must close its wrapped main transport according to normal ownership
  rules.

### Standalone behavior

- Header matching is case-insensitive and locale-independent.
- Multi-value headers preserve value count and order.
- A request with no ordinary headers remains valid input and produces no ordinary headers.
- Ordinary headers not in the whitelist are removed even if generated internally.
- Filtering changes only ordinary headers; URI, method, request content, and response callbacks are
  unchanged.
- Filtering preserves transport identity and propagates the underlying transport's completion or
  executable-operation behavior.

## Conformance requirements

### HEAD-REQ-001: Opt-in filtering

Header optimization must be disabled by default and, when enabled, must filter every attempt by
case-insensitive ordinary-header name while preserving allowed names and values.

### HEAD-REQ-002: Required headers

The computed whitelist must preserve every applicable base and conditionally required ordinary
header, including all ordinary headers required by authentication and signing. Filtering must also
preserve required protocol fields outside the scope of the ordinary-header whitelist, including
protocol-specific authority metadata and protocol-native framing signals.

### HEAD-REQ-003: Custom whitelist safety

An omitted custom whitelist must select the computed default. A supplied custom whitelist must be
validated after final configuration, copied immutably, and rejected when it is empty, contains any
header name that is absent or empty, is malformed, or is incomplete.

### HEAD-REQ-004: Feature ordering

Filtering must run after features that create final wire headers and after routing has consumed
internal metadata, without reintroducing disabled headers.

### HEAD-REQ-005: Transport transparency

Filtering must preserve all non-header request and transport metadata, completion, cancellation,
error, and lifecycle behavior.

### HEAD-REQ-006: Transport-form parity

Blocking and non-blocking transports using the same protocol representation must produce equivalent
filtered ordinary-header maps and ownership behavior. When their protocols differ, their
transport-specific fields must preserve equivalent required semantics.
