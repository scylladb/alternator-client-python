// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use _alternator_rust::{client::Client, config::NativeConfig, runtime::shared_runtime};
use aws_sdk_dynamodb::types::AttributeValue;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("barrier timed out")
}

#[derive(Clone)]
struct Reply {
    status: u16,
    body: Vec<u8>,
    encoding: Option<&'static str>,
    release: Option<Arc<Semaphore>>,
    extra_headers: Vec<u8>,
}

impl Reply {
    fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
            encoding: None,
            release: None,
            extra_headers: Vec::new(),
        }
    }
}

#[derive(Debug)]
enum Event {
    Discovery(usize, String),
    Request {
        connection: usize,
        headers: String,
        body: Vec<u8>,
    },
    Closed(usize),
}

struct Server {
    port: u16,
    scripts: Arc<Mutex<VecDeque<Reply>>>,
    discovery: Arc<Mutex<Reply>>,
    events: mpsc::UnboundedReceiver<Event>,
    cancellation: CancellationToken,
    tasks: TaskTracker,
}

impl Server {
    async fn start() -> Self {
        Self::start_with_tls(None).await
    }

    async fn start_with_tls(tls: Option<Arc<rustls::ServerConfig>>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        Self::from_listener(listener, tls)
    }

    fn from_listener(listener: TcpListener, tls: Option<Arc<rustls::ServerConfig>>) -> Self {
        let port = listener.local_addr().unwrap().port();
        let scripts = Arc::new(Mutex::new(VecDeque::new()));
        let discovery = Arc::new(Mutex::new(Reply::json(200, r#"["127.0.0.1"]"#)));
        let (events_tx, events) = mpsc::unbounded_channel();
        let cancellation = CancellationToken::new();
        let tasks = TaskTracker::new();
        let sockets = tasks.clone();
        let token = cancellation.clone();
        let queue = scripts.clone();
        let topology = discovery.clone();
        tasks.spawn(async move {
            let mut id = 0;
            loop {
                let accepted = tokio::select! { _ = token.cancelled() => return, result = listener.accept() => result };
                let (socket, _) = accepted.unwrap();
                id += 1;
                let events = events_tx.clone();
                let queue = queue.clone();
                let topology = topology.clone();
                let token = token.clone();
                let connection = id;
                let tls = tls.clone();
                sockets.spawn(async move {
                    tokio::select! { _ = token.cancelled() => {}, _ = async {
                        if let Some(tls) = tls {
                            if let Ok(socket) = tokio_rustls::TlsAcceptor::from(tls).accept(socket).await {
                                serve(socket, connection, events.clone(), queue, topology).await;
                            }
                        } else {
                            serve(socket, connection, events.clone(), queue, topology).await;
                        }
                    } => {} }
                    let _ = events.send(Event::Closed(connection));
                });
            }
        });
        Self {
            port,
            scripts,
            discovery,
            events,
            cancellation,
            tasks,
        }
    }

    fn push(&self, reply: Reply) {
        self.scripts.lock().unwrap().push_back(reply);
    }

    async fn discovery(&mut self) -> String {
        bounded(async {
            loop {
                if let Event::Discovery(_, headers) = self.events.recv().await.unwrap() {
                    return headers;
                }
            }
        })
        .await
    }

    async fn request(&mut self) -> (usize, String, Vec<u8>) {
        bounded(async {
            loop {
                if let Event::Request {
                    connection,
                    headers,
                    body,
                } = self.events.recv().await.unwrap()
                {
                    return (connection, headers, body);
                }
            }
        })
        .await
    }

    async fn disconnected(&mut self, expected: usize) {
        bounded(async {
            loop {
                if let Some(Event::Closed(connection)) = self.events.recv().await {
                    if connection == expected {
                        return;
                    }
                }
            }
        })
        .await
    }

    async fn stop(self) {
        self.cancellation.cancel();
        self.tasks.close();
        bounded(self.tasks.wait()).await;
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.tasks.close();
    }
}

async fn serve<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    socket: S,
    connection: usize,
    events: mpsc::UnboundedSender<Event>,
    scripts: Arc<Mutex<VecDeque<Reply>>>,
    discovery: Arc<Mutex<Reply>>,
) {
    let (read, mut write) = tokio::io::split(socket);
    let mut read = BufReader::new(read);
    loop {
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            headers.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        let size = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut body = vec![0; size];
        if read.read_exact(&mut body).await.is_err() {
            return;
        }
        if headers
            .to_ascii_lowercase()
            .contains("content-encoding: gzip")
        {
            let mut decoded = Vec::new();
            flate2::read::GzDecoder::new(body.as_slice())
                .read_to_end(&mut decoded)
                .unwrap();
            body = decoded;
        }
        let reply = if headers.starts_with("GET /localnodes") {
            let reply = discovery.lock().unwrap().clone();
            let _ = events.send(Event::Discovery(connection, headers));
            reply
        } else {
            let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let table = document["TableName"].as_str().unwrap_or("");
            assert!(
                !table.starts_with(".scylla."),
                "discovery must use GET /localnodes"
            );
            let _ = events.send(Event::Request {
                connection,
                headers,
                body,
            });
            scripts
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected SDK data request")
        };
        if reply.status == 0 {
            return;
        }
        if let Some(release) = &reply.release {
            let mut byte = [0];
            tokio::select! {
                permit = release.acquire() => { permit.unwrap().forget(); },
                _ = read.read(&mut byte) => return,
            }
        }
        let encoding = reply.encoding.map_or(String::new(), |value| {
            format!("Content-Encoding: {value}\r\n")
        });
        let mut headers = format!("HTTP/1.1 {} OK\r\nContent-Length: {}\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amzn-RequestId: request-{connection}\r\nX-Original-Header: preserved\r\n{encoding}", reply.status, reply.body.len()).into_bytes();
        headers.extend_from_slice(&reply.extra_headers);
        headers.extend_from_slice(b"\r\n");
        if write.write_all(&headers).await.is_err() || write.write_all(&reply.body).await.is_err() {
            return;
        }
    }
}

fn config(port: u16) -> NativeConfig {
    NativeConfig {
        schema_version: 1,
        seed_hosts: vec!["127.0.0.1".into()],
        port,
        scheme: "http".into(),
        scopes: vec![("cluster".into(), None, None)],
        active_interval_ms: 1_000,
        idle_interval_ms: 60_000,
        key_affinity: "NONE".into(),
        table_pk_attributes: HashMap::new(),
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
        region: "us-east-1".into(),
        request_gzip: false,
        compression_min_bytes: 1024,
        gzip_level: 9,
        response_compression: Vec::new(),
        optimize_headers: false,
        user_agent: Some("contract-agent".into()),
        max_attempts: 3,
        connect_seconds: 2.,
        read_seconds: 3.,
        discovery_seconds: 2.,
        system_roots: true,
        ca_pem: Vec::new(),
        client_cert_pem: None,
        client_key_pem: None,
    }
}

async fn get(
    client: &Client,
) -> Result<
    (
        Result<
            aws_sdk_dynamodb::operation::get_item::GetItemOutput,
            aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::get_item::GetItemError>,
        >,
        _alternator_rust::metadata::OperationMetadata,
    ),
    _alternator_rust::registry::Closed,
> {
    client
        .run(|sdk| async move {
            sdk.get_item()
                .table_name("contract")
                .key("pk", AttributeValue::S("x".into()))
                .send()
                .await
        })
        .await
}

#[test]
fn sdk_signing_compression_retries_metadata_and_persistent_pool() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply::json(
            500,
            r#"{"__type":"InternalServerError","message":"retry"}"#,
        ));
        let mut compressed =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        compressed
            .write_all(br#"{"Item":{"pk":{"S":"x"}}}"#)
            .unwrap();
        server.push(Reply {
            status: 200,
            body: compressed.finish().unwrap(),
            encoding: Some("deflate"),
            release: None,
            extra_headers: b"X-Bytes: \x80\xff\r\nX-Repeated: first\r\nX-Repeated: second\r\n"
                .to_vec(),
        });
        server.push(Reply::json(200, r#"{"Item":{}}"#));
        let mut cfg = config(server.port);
        cfg.access_key_id = Some("test-key".into());
        cfg.secret_access_key = Some("test-secret".into());
        cfg.session_token = Some("session-token".into());
        cfg.request_gzip = true;
        cfg.compression_min_bytes = 0;
        cfg.response_compression = vec!["gzip".into(), "deflate".into()];
        let client = Client::create(cfg).await.unwrap();
        let (output, attempts) = bounded(get(&client)).await.unwrap();
        assert_eq!(output.unwrap().item.unwrap()["pk"].as_s().unwrap(), "x");
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].status, 500);
        assert_eq!(
            attempts[0].body,
            br#"{"__type":"InternalServerError","message":"retry"}"#
        );
        assert_eq!(attempts[1].attempt, 2);
        assert!(attempts[1]
            .headers
            .iter()
            .any(|(name, value)| name == "content-encoding" && value == b"deflate"));
        assert!(attempts[1].request_id().is_some());
        let header_values = |name: &str| {
            attempts[1]
                .headers
                .iter()
                .filter(|(key, _)| key == name)
                .map(|(_, value)| value.as_slice())
                .collect::<Vec<_>>()
        };
        assert_eq!(header_values("x-bytes"), [b"\x80\xff".as_slice()]);
        assert_eq!(
            header_values("x-repeated"),
            [b"first".as_slice(), b"second".as_slice()]
        );
        let (first, headers, body) = server.request().await;
        let (second, _, _) = server.request().await;
        // Discovery may use a pooled connection between retries.
        assert!(first > 0 && second > 0);
        let headers = headers.to_ascii_lowercase();
        assert!(headers.contains("authorization: aws4-hmac-sha256 credential=test-key/"));
        assert!(headers.contains("/us-east-1/dynamodb/aws4_request"));
        assert!(headers.contains("x-amz-security-token: session-token"));
        assert!(headers.contains("content-encoding: gzip"));
        assert!(headers.contains("accept-encoding: gzip, deflate"));
        assert!(headers.contains("user-agent: contract-agent"));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["TableName"],
            "contract"
        );
        assert!(bounded(get(&client)).await.unwrap().0.is_ok());
        let final_connection = server.request().await.0;
        bounded(client.close()).await;
        assert_eq!(client.live_drivers(), 0);
        server.disconnected(final_connection).await;
        server.stop().await;
    });
}

#[test]
fn close_cancels_held_request_drains_sockets_and_preserves_another_client() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply {
            release: Some(Arc::new(Semaphore::new(0))),
            ..Reply::json(200, "{}")
        });
        server.push(Reply::json(200, "{}"));
        let client = Client::create(config(server.port)).await.unwrap();
        let other = Client::create(config(server.port)).await.unwrap();
        let request = tokio::spawn({
            let client = client.clone();
            async move { get(&client).await }
        });
        let (connection, headers, _) = server.request().await;
        assert!(!headers.to_ascii_lowercase().contains("authorization:"));
        let first_close = tokio::spawn({
            let client = client.clone();
            async move { client.close().await }
        });
        client.start_close();
        first_close.abort();
        let closes = (client.close(), client.close(), client.close());
        bounded(async {
            tokio::join!(closes.0, closes.1, closes.2);
        })
        .await;
        assert!(bounded(request).await.unwrap().is_err());
        assert_eq!(client.live_drivers(), 0);
        assert!(get(&client).await.is_err());
        server.disconnected(connection).await;
        assert!(bounded(get(&other)).await.unwrap().0.is_ok());
        bounded(other.close()).await;
        server.stop().await;
    });
}

#[test]
fn cancelled_read_closes_only_its_connection_and_metadata_stays_isolated() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply {
            release: Some(Arc::new(Semaphore::new(0))),
            ..Reply::json(200, "{}")
        });
        server.push(Reply::json(
            403,
            r#"{"__type":"UnknownError","message":"full error","extra":"retained"}"#,
        ));
        let client = Client::create(config(server.port)).await.unwrap();
        let pending = tokio::spawn({
            let client = client.clone();
            async move { get(&client).await }
        });
        let (first, _, _) = server.request().await;
        let (output, attempts) = bounded(get(&client)).await.unwrap();
        let (second, _, _) = server.request().await;
        assert_ne!(first, second);
        assert!(output.is_err());
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].attempt, 1);
        assert_eq!(attempts[0].status, 403);
        assert_eq!(
            attempts[0].request_id().unwrap(),
            format!("request-{second}")
        );
        assert_eq!(
            attempts[0].body,
            br#"{"__type":"UnknownError","message":"full error","extra":"retained"}"#
        );
        pending.abort();
        let _ = bounded(pending).await;
        server.disconnected(first).await;
        assert!(!client.is_closed());
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[test]
fn configuration_accepts_native_widths_and_rejects_invalid_values() {
    let mut cfg = config(1);
    cfg.scopes = (0..65)
        .map(|index| ("datacenter".into(), Some(format!("dc-{index}")), None))
        .collect();
    cfg.active_interval_ms = u64::MAX;
    cfg.idle_interval_ms = 1 << 31;
    cfg.compression_min_bytes = usize::MAX;
    cfg.max_attempts = u32::MAX;
    cfg.connect_seconds = (1_u64 << 32) as f64;
    cfg.read_seconds = cfg.connect_seconds;
    cfg.discovery_seconds = cfg.connect_seconds;
    cfg.validate().unwrap();

    for value in [f64::NAN, f64::INFINITY, 0., 1e-20, u64::MAX as f64] {
        cfg.read_seconds = value;
        assert!(cfg.validate().is_err(), "accepted timeout {value}");
    }
    // Duration can hold this value, but some platform Instants cannot. Such
    // values must fail rather than reaching Tokio's timer-less timeout path.
    cfg.read_seconds = (1_u64 << 63) as f64;
    let duration = Duration::try_from_secs_f64(cfg.read_seconds).unwrap();
    assert_eq!(
        cfg.validate().is_ok(),
        std::time::Instant::now().checked_add(duration).is_some()
    );
    cfg.read_seconds = 3.;
    cfg.max_attempts = 0;
    assert!(cfg.validate().is_err());
    cfg.max_attempts = 3;
    cfg.active_interval_ms = 0;
    assert!(cfg.validate().is_err());
    cfg.active_interval_ms = 1_000;
    cfg.scopes.clear();
    assert!(cfg.validate().is_err());
}

#[test]
fn invalid_configuration_fails_before_any_network_and_repeated_clients_close() {
    shared_runtime().unwrap().block_on(async {
        let mut cfg = config(1);
        cfg.discovery_seconds = f64::NAN;
        assert!(Client::create(cfg).await.is_err());
        let mut cfg = config(1);
        cfg.ca_pem = vec![b"invalid PEM".to_vec()];
        assert!(Client::create(cfg).await.is_err());
        let server = Server::start().await;
        for _ in 0..4 {
            let client = Client::create(config(server.port)).await.unwrap();
            bounded(client.close()).await;
            bounded(client.close()).await;
            assert_eq!(client.live_drivers(), 0);
        }
        server.stop().await;
    });
}

#[test]
fn server_unwind_cancels_held_sockets_and_releases_listener() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply {
            release: Some(Arc::new(Semaphore::new(0))),
            ..Reply::json(200, "{}")
        });
        let port = server.port;
        let tasks = server.tasks.clone();
        let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        socket
            .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}")
            .await
            .unwrap();
        server.request().await;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _server = server;
                panic!("simulate a failed test assertion");
            }))
            .is_err()
        );
        bounded(tasks.wait()).await;
        assert!(tasks.is_closed());
        assert_eq!(bounded(socket.read(&mut [0])).await.unwrap(), 0);
        let _listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    });
}

#[test]
fn discovery_deadline_cancels_held_socket_and_recovers_through_seed() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        *server.discovery.lock().unwrap() = Reply {
            release: Some(Arc::new(Semaphore::new(0))),
            ..Reply::json(200, r#"["127.0.0.1"]"#)
        };
        server.push(Reply::json(200, "{}"));
        let mut cfg = config(server.port);
        // A single scoped candidate makes the initial request and the retry
        // through the original seed deterministic, without shuffled candidates.
        cfg.scopes = vec![("datacenter".into(), Some("dc 1".into()), None)];
        cfg.active_interval_ms = 10;
        cfg.discovery_seconds = 0.1;
        cfg.read_seconds = 30.;
        let client = Client::create(cfg).await.unwrap();
        let (held_connection, headers) = match bounded(server.events.recv()).await.unwrap() {
            Event::Discovery(connection, headers) => (connection, headers),
            event => panic!("expected initial held discovery, got {event:?}"),
        };
        assert!(headers.starts_with("GET /localnodes?dc=dc+1 "));
        // The event follows reply capture, so this changes only later requests.
        // Never release the original reply: only deadline expiry can unblock it.
        *server.discovery.lock().unwrap() = Reply::json(200, r#"["localhost"]"#);
        // Allow ample scheduling slack over 100ms while distinguishing the
        // driver's 5s default (when forwarding is absent) and 30s read timeout.
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut held_closed = false;
            let mut seed_retried = false;
            let mut topology_updated = false;
            while !held_closed || !seed_retried || !topology_updated {
                match server.events.recv().await.unwrap() {
                    Event::Closed(connection) if connection == held_connection => {
                        held_closed = true;
                    }
                    Event::Discovery(connection, headers) => {
                        assert_ne!(connection, held_connection);
                        let headers = headers.to_ascii_lowercase();
                        if headers.contains(&format!("host: 127.0.0.1:{}", server.port)) {
                            seed_retried = true;
                        } else {
                            assert!(seed_retried);
                            assert!(headers.contains(&format!("host: localhost:{}", server.port)));
                            topology_updated = true;
                        }
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("discovery deadline must close the held socket and recover before read timeout");
        assert!(bounded(get(&client)).await.unwrap().0.is_ok());
        let (_, headers, _) = server.request().await;
        assert!(headers
            .to_ascii_lowercase()
            .contains(&format!("host: localhost:{}", server.port)));
        bounded(client.close()).await;
        assert_eq!(client.live_drivers(), 0);
        server.stop().await;
    });
}

#[test]
fn unsuccessful_unscoped_discovery_cannot_route_to_response_nodes() {
    shared_runtime().unwrap().block_on(async {
        for status in [403, 500] {
            let mut server = Server::start().await;
            *server.discovery.lock().unwrap() = Reply::json(status, r#"["rejected.invalid"]"#);
            server.push(Reply::json(200, "{}"));
            let mut cfg = config(server.port);
            cfg.active_interval_ms = 10;
            let client = Client::create(cfg).await.unwrap();
            server.discovery().await;
            // A subsequent refresh proves the first failed response completed.
            let next = server.discovery().await;
            assert!(next.starts_with("GET /localnodes HTTP/1.1"));
            assert!(bounded(get(&client)).await.unwrap().0.is_ok());
            let (_, headers, _) = server.request().await;
            assert!(headers
                .to_ascii_lowercase()
                .contains(&format!("host: 127.0.0.1:{}", server.port)));
            bounded(client.close()).await;
            server.stop().await;
        }
    });
}

#[test]
fn unsuccessful_discovery_preserves_topology_and_falls_back_to_seeds() {
    shared_runtime().unwrap().block_on(async {
        for status in [403, 500] {
            let mut server = Server::start().await;
            *server.discovery.lock().unwrap() = Reply::json(200, r#"["localhost"]"#);
            server.push(Reply::json(200, "{}"));
            let mut cfg = config(server.port);
            cfg.scopes = vec![
                ("rack".into(), Some("dc 1".into()), Some("rack 1".into())),
                ("cluster".into(), None, None),
            ];
            cfg.active_interval_ms = 10;
            let client = Client::create(cfg).await.unwrap();
            // The next refresh uses the node published by the first response.
            server.discovery().await;
            let next = server.discovery().await.to_ascii_lowercase();
            assert!(next.contains(&format!("host: localhost:{}", server.port)));
            assert!(next.starts_with("get /localnodes?dc=dc+1&rack=rack+1 "));
            *server.discovery.lock().unwrap() = Reply::json(status, r#"["rejected.invalid"]"#);
            // An unsuccessful candidate must trigger the original seed, rather
            // than publishing its valid-looking array or treating it as empty.
            bounded(async {
                loop {
                    let headers = server.discovery().await.to_ascii_lowercase();
                    assert!(headers.starts_with("get /localnodes?dc=dc+1&rack=rack+1 "));
                    if headers.contains(&format!("host: 127.0.0.1:{}", server.port)) {
                        break;
                    }
                }
            })
            .await;
            // Starting the next refresh proves the failed seed result finished.
            let next = server.discovery().await.to_ascii_lowercase();
            assert!(next.contains(&format!("host: localhost:{}", server.port)));
            assert!(bounded(get(&client)).await.unwrap().0.is_ok());
            let (_, headers, _) = server.request().await;
            assert!(headers
                .to_ascii_lowercase()
                .contains(&format!("host: localhost:{}", server.port)));
            bounded(client.close()).await;
            server.stop().await;
        }
    });
}

struct Certificates {
    ca: Vec<u8>,
    client_cert: Vec<u8>,
    client_key: Vec<u8>,
    server: Arc<rustls::ServerConfig>,
}

fn certificates(mtls: bool) -> Certificates {
    let mut params = rcgen::CertificateParams::default();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    // IP SAN intentionally excludes localhost to exercise hostname rejection.
    let server_params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    let server_key = rcgen::KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();
    let mut client_params = rcgen::CertificateParams::new(vec!["test-client".into()]).unwrap();
    client_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = rcgen::KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &ca, &ca_key).unwrap();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap();
    let builder = if mtls {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .unwrap();
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let server = builder
        .with_single_cert(
            vec![server_cert.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
        )
        .unwrap();
    Certificates {
        ca: ca.pem().into_bytes(),
        client_cert: client_cert.pem().into_bytes(),
        client_key: client_key.serialize_pem().into_bytes(),
        server: Arc::new(server),
    }
}

#[test]
fn custom_ca_hostname_verification_and_mtls_are_enforced() {
    shared_runtime().unwrap().block_on(async {
        let certs = certificates(true);
        let mut server = Server::start_with_tls(Some(certs.server)).await;
        server.push(Reply::json(200, "{}"));
        let mut cfg = config(server.port);
        cfg.scheme = "https".into();
        cfg.system_roots = false;
        cfg.ca_pem = vec![certs.ca.clone()];
        cfg.client_cert_pem = Some(certs.client_cert.clone());
        cfg.client_key_pem = Some(certs.client_key.clone());
        let client = Client::create(cfg.clone()).await.unwrap();
        assert!(bounded(get(&client)).await.unwrap().0.is_ok());
        let (connection, _, _) = server.request().await;
        bounded(client.close()).await;
        server.disconnected(connection).await;

        let mut no_client_cert = cfg.clone();
        no_client_cert.client_cert_pem = None;
        no_client_cert.client_key_pem = None;
        let client = Client::create(no_client_cert).await.unwrap();
        assert!(bounded(get(&client)).await.unwrap().0.is_err());
        bounded(client.close()).await;

        let mut wrong_hostname = cfg.clone();
        wrong_hostname.seed_hosts = vec!["localhost".into()];
        let client = Client::create(wrong_hostname).await.unwrap();
        let (output, metadata) = bounded(get(&client)).await.unwrap();
        assert!(output.is_err());
        assert_eq!(metadata.attempts, 1, "hostname failures must not retry");
        bounded(client.close()).await;

        let wrong_ca = certificates(false);
        cfg.ca_pem = vec![wrong_ca.ca];
        let client = Client::create(cfg).await.unwrap();
        let (output, metadata) = bounded(get(&client)).await.unwrap();
        assert!(output.is_err());
        assert_eq!(metadata.attempts, 1, "certificate failures must not retry");
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[test]
fn native_request_deadline_cancels_held_read_and_keeps_client_open() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply {
            release: Some(Arc::new(Semaphore::new(0))),
            ..Reply::json(200, "{}")
        });
        let mut cfg = config(server.port);
        cfg.read_seconds = 0.05;
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        let request = tokio::spawn({
            let client = client.clone();
            async move { get(&client).await }
        });
        let (connection, _, _) = server.request().await;
        assert!(bounded(request).await.unwrap().unwrap().0.is_err());
        server.disconnected(connection).await;
        assert!(!client.is_closed());
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[test]
fn connector_reuses_idle_connection_without_untracked_background_drivers() {
    use _alternator_rust::transport::Transport;
    use alternator_driver::AlternatorClient;
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply::json(200, "{}"));
        server.push(Reply::json(200, "{}"));
        let cfg = config(server.port);
        let transport = Transport::new(cfg.tls_config().unwrap());
        // Disable discovery only for this focused pool assertion: concurrent
        // topology I/O legitimately borrows the same per-client idle pool.
        let sdk = AlternatorClient::try_from_conf(
            cfg.upstream(transport.clone())
                .unwrap()
                .to_builder()
                .without_discovery()
                .build(),
        )
        .unwrap();
        bounded(
            sdk.get_item()
                .table_name("contract")
                .key("pk", AttributeValue::S("x".into()))
                .send(),
        )
        .await
        .unwrap();
        let first = server.request().await.0;
        bounded(
            sdk.get_item()
                .table_name("contract")
                .key("pk", AttributeValue::S("x".into()))
                .send(),
        )
        .await
        .unwrap();
        assert_eq!(server.request().await.0, first);
        sdk.shutdown().await;
        bounded(transport.finish_close()).await;
        assert_eq!(transport.live_drivers(), 0);
        server.disconnected(first).await;
        server.stop().await;
    });
}

#[test]
fn retries_keep_actual_attempt_count_with_optimized_headers() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply::json(500, r#"{"__type":"InternalServerError"}"#));
        server.push(Reply::json(200, "{}"));
        let mut cfg = config(server.port);
        cfg.optimize_headers = true;
        let client = Client::create(cfg).await.unwrap();
        let (output, attempts) = bounded(get(&client)).await.unwrap();
        assert!(output.is_ok());
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[1].attempt, 2);
        for _ in 0..2 {
            let (_, headers, _) = server.request().await;
            assert!(!headers.to_ascii_lowercase().contains("amz-sdk-request:"));
        }
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[test]
fn cancelling_during_tls_connect_drops_the_socket() {
    use _alternator_rust::{registry::RequestRegistry, transport::Transport};
    use alternator_driver::AlternatorClient;
    shared_runtime().unwrap().block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let mut cfg = config(listener.local_addr().unwrap().port());
        cfg.scheme = "https".into();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Hold the TLS handshake after receiving the real ClientHello.
            let mut bytes = [0; 4096];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
            entered_tx.send(()).unwrap();
            while socket.read(&mut bytes).await.unwrap_or(0) != 0 {}
        });
        let transport = Transport::new(cfg.tls_config().unwrap());
        let sdk = AlternatorClient::try_from_conf(
            cfg.upstream(transport.clone())
                .unwrap()
                .to_builder()
                .without_discovery()
                .build(),
        )
        .unwrap();
        let registry = Arc::new(RequestRegistry::default());
        let request = tokio::spawn({
            let registry = registry.clone();
            let sdk = sdk.clone();
            async move {
                registry
                    .run(
                        sdk.get_item()
                            .table_name("contract")
                            .key("pk", AttributeValue::S("x".into()))
                            .send(),
                    )
                    .await
            }
        });
        bounded(entered_rx).await.unwrap();
        request.abort();
        let _ = bounded(request).await;
        bounded(registry.drained()).await;
        bounded(peer).await.unwrap();
        assert!(!registry.is_closed());
        sdk.shutdown().await;
        bounded(transport.finish_close()).await;
    });
}

#[derive(Debug)]
struct RetryBarrier {
    entered: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    armed: Arc<std::sync::atomic::AtomicBool>,
}

impl aws_smithy_async::rt::sleep::AsyncSleep for RetryBarrier {
    fn sleep(&self, duration: Duration) -> aws_smithy_async::rt::sleep::Sleep {
        // Keep the unrelated SDK identity/stalled-stream monitors operational.
        if duration >= Duration::from_secs(3)
            || !self.armed.load(std::sync::atomic::Ordering::Acquire)
        {
            return aws_smithy_async::rt::sleep::Sleep::new(tokio::time::sleep(duration));
        }
        let entered = self.entered.clone();
        aws_smithy_async::rt::sleep::Sleep::new(async move {
            if let Some(entered) = entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            std::future::pending::<()>().await;
        })
    }
}

#[test]
fn close_interrupts_an_sdk_retry_sleep() {
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply::json(500, r#"{"__type":"InternalServerError"}"#));
        let client = Client::create(config(server.port)).await.unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let request = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .run(|sdk| async move {
                        sdk.get_item()
                            .table_name("contract")
                            .key("pk", AttributeValue::S("x".into()))
                            .customize()
                            .interceptor(ArmOnServerError(armed.clone()))
                            // An explicit barrier sleep makes cancellation deterministic;
                            // public native construction still uses the SDK Tokio sleep.
                            .config_override(
                                aws_sdk_dynamodb::config::Builder::new()
                                    .timeout_config(
                                        aws_smithy_types::timeout::TimeoutConfig::disabled(),
                                    )
                                    .sleep_impl(
                                        aws_smithy_async::rt::sleep::SharedAsyncSleep::new(
                                            RetryBarrier {
                                                entered: Arc::new(Mutex::new(Some(entered_tx))),
                                                armed,
                                            },
                                        ),
                                    ),
                            )
                            .send()
                            .await
                    })
                    .await
            }
        });
        if bounded(entered_rx).await.is_err() {
            panic!("retry barrier not entered: {:?}", bounded(request).await);
        }
        bounded(client.close()).await;
        assert!(bounded(request).await.unwrap().is_err());
        assert_eq!(client.live_drivers(), 0);
        // Only the initial attempt reached the peer before the blocked sleep.
        server.request().await;
        server.stop().await;
    });
}

#[derive(Debug)]
struct RetryGate {
    entered: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    release: Arc<Semaphore>,
}

impl aws_smithy_async::rt::sleep::AsyncSleep for RetryGate {
    fn sleep(&self, duration: Duration) -> aws_smithy_async::rt::sleep::Sleep {
        if duration >= Duration::from_secs(3) {
            return aws_smithy_async::rt::sleep::Sleep::new(tokio::time::sleep(duration));
        }
        let entered = self.entered.clone();
        let release = self.release.clone();
        aws_smithy_async::rt::sleep::Sleep::new(async move {
            if let Some(entered) = entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            release.acquire().await.unwrap().forget();
        })
    }
}

#[test]
fn metadata_counts_connection_refusal_then_success_and_final_dispatch_failure() {
    shared_runtime().unwrap().block_on(async {
        // Keep the port bound without listening so the first real TCP attempt
        // is refused. After the retry barrier, turn the same socket into the
        // peer's listener without releasing the port to concurrent fixtures.
        let reservation = TcpSocket::new_v4().unwrap();
        reservation.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = reservation.local_addr().unwrap().port();
        let client = Client::create(config(port)).await.unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(Semaphore::new(0));
        let request = tokio::spawn({
            let client = client.clone();
            let release = release.clone();
            async move {
                client
                    .run(|sdk| async move {
                        sdk.get_item()
                            .table_name("contract")
                            .key("pk", AttributeValue::S("x".into()))
                            .customize()
                            .config_override(
                                aws_sdk_dynamodb::config::Builder::new()
                                    .timeout_config(
                                        aws_smithy_types::timeout::TimeoutConfig::disabled(),
                                    )
                                    .sleep_impl(
                                        aws_smithy_async::rt::sleep::SharedAsyncSleep::new(
                                            RetryGate {
                                                entered: Arc::new(Mutex::new(Some(entered_tx))),
                                                release,
                                            },
                                        ),
                                    ),
                            )
                            .send()
                            .await
                    })
                    .await
            }
        });
        bounded(entered_rx).await.unwrap();
        let mut server = Server::from_listener(reservation.listen(128).unwrap(), None);
        server.push(Reply::json(200, "{}"));
        release.add_permits(1);
        let (output, metadata) = bounded(request).await.unwrap().unwrap();
        assert!(output.is_ok());
        assert_eq!(metadata.attempts, 2);
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].attempt, 2);
        server.request().await;
        bounded(client.close()).await;

        server.push(Reply::json(0, ""));
        let mut cfg = config(port);
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        let (output, metadata) = bounded(get(&client)).await.unwrap();
        assert!(output.is_err());
        assert_eq!(metadata.attempts, 1);
        assert!(metadata.is_empty());
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[derive(Debug)]
struct ArmOnServerError(Arc<std::sync::atomic::AtomicBool>);

impl aws_smithy_runtime_api::client::interceptors::Intercept for ArmOnServerError {
    fn name(&self) -> &'static str {
        "ArmRetryBarrierAfterServiceError"
    }
    fn read_after_attempt(
        &self,
        context: &aws_smithy_runtime_api::client::interceptors::context::FinalizerInterceptorContextRef<'_>,
        _: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
        _: &mut aws_smithy_types::config_bag::ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        if context
            .response()
            .is_some_and(|response| response.status().as_u16() == 500)
        {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }
}
