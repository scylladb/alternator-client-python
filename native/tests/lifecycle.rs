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
use tokio_util::task::{AbortOnDropHandle, TaskTracker};

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
    truncate_body: bool,
}

impl Reply {
    fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
            encoding: None,
            release: None,
            extra_headers: Vec::new(),
            truncate_body: false,
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
        let body = if reply.truncate_body {
            &reply.body[..reply.body.len() / 2]
        } else {
            &reply.body
        };
        if write.write_all(&headers).await.is_err() || write.write_all(body).await.is_err() {
            return;
        }
        if reply.truncate_body {
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
        operation_timeout_seconds: 3.,
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
            truncate_body: false,
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
        #[cfg(feature = "conversion-tests")]
        pyo3::Python::attach(|py| {
            use pyo3::prelude::*;
            let error = _alternator_rust::errors::sdk_error(
                py,
                "GetItem",
                "unused",
                output.as_ref().unwrap_err(),
                &attempts,
            )
            .unwrap();
            let response = error.value(py).getattr("response").unwrap();
            assert_eq!(
                response
                    .get_item("extra")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "retained"
            );
            let metadata = response.get_item("ResponseMetadata").unwrap();
            assert_eq!(
                metadata
                    .get_item("RequestId")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                format!("request-{second}")
            );
            assert_eq!(
                metadata
                    .get_item("RetryAttempts")
                    .unwrap()
                    .extract::<u32>()
                    .unwrap(),
                0
            );
        });
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
    cfg.operation_timeout_seconds = cfg.connect_seconds;
    cfg.discovery_seconds = cfg.connect_seconds;
    cfg.validate().unwrap();

    for value in [f64::NAN, f64::INFINITY, 0., 1e-20, u64::MAX as f64] {
        cfg.operation_timeout_seconds = value;
        assert!(cfg.validate().is_err(), "accepted timeout {value}");
    }
    // Duration can hold this value, but some platform Instants cannot. Such
    // values must fail rather than reaching Tokio's timer-less timeout path.
    cfg.operation_timeout_seconds = (1_u64 << 63) as f64;
    let duration = Duration::try_from_secs_f64(cfg.operation_timeout_seconds).unwrap();
    assert_eq!(
        cfg.validate().is_ok(),
        std::time::Instant::now().checked_add(duration).is_some()
    );
    cfg.operation_timeout_seconds = 3.;
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
        cfg.operation_timeout_seconds = 30.;
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
        assert_eq!(
            _alternator_rust::transport_failure::TransportFailure::find(
                output.as_ref().unwrap_err()
            )
            .unwrap()
            .category,
            _alternator_rust::transport_failure::Category::Tls
        );
        bounded(client.close()).await;

        let wrong_ca = certificates(false);
        cfg.ca_pem = vec![wrong_ca.ca];
        let client = Client::create(cfg).await.unwrap();
        let (output, metadata) = bounded(get(&client)).await.unwrap();
        assert!(output.is_err());
        assert_eq!(metadata.attempts, 1, "certificate failures must not retry");
        assert_eq!(
            _alternator_rust::transport_failure::TransportFailure::find(
                output.as_ref().unwrap_err()
            )
            .unwrap()
            .category,
            _alternator_rust::transport_failure::Category::Tls
        );
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
        cfg.operation_timeout_seconds = 0.05;
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        let request = tokio::spawn({
            let client = client.clone();
            async move { get(&client).await }
        });
        let (connection, _, _) = server.request().await;
        let (output, metadata) = bounded(request).await.unwrap().unwrap();
        assert!(output.is_err());
        #[cfg(feature = "conversion-tests")]
        pyo3::Python::attach(|py| {
            use pyo3::prelude::*;
            let error = _alternator_rust::errors::sdk_error(
                py,
                "GetItem",
                "http://node",
                output.as_ref().unwrap_err(),
                &metadata,
            )
            .unwrap();
            assert_eq!(
                error
                    .value(py)
                    .getattr("category")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "read_timeout"
            );
        });
        let _ = metadata;
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
            cfg.to_driver_config(transport.clone())
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
    use _alternator_rust::{registry::OperationRegistry, transport::Transport};
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
            cfg.to_driver_config(transport.clone())
                .unwrap()
                .to_builder()
                .without_discovery()
                .build(),
        )
        .unwrap();
        let registry = Arc::new(OperationRegistry::default());
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
        bounded(registry.wait_until_drained()).await;
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
                                    // Keep the retry barrier separate from the
                                    // upload observer's periodic sleep timers.
                                    .stalled_stream_protection(
                                        aws_sdk_dynamodb::config::StalledStreamProtectionConfig::disabled(),
                                    )
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
        #[cfg(feature = "conversion-tests")]
        pyo3::Python::attach(|py| {
            use pyo3::prelude::*;
            let output = _alternator_rust::conversion::Output::GetItem(Box::new(
                output.as_ref().unwrap().clone(),
            ));
            let result = _alternator_rust::response::success(py, &output, &metadata).unwrap();
            assert_eq!(
                result
                    .get_item("ResponseMetadata")
                    .unwrap()
                    .unwrap()
                    .get_item("RetryAttempts")
                    .unwrap()
                    .extract::<u32>()
                    .unwrap(),
                1
            );
        });
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
        assert_eq!(
            _alternator_rust::transport_failure::TransportFailure::find(
                output.as_ref().unwrap_err()
            )
            .unwrap()
            .category,
            _alternator_rust::transport_failure::Category::ConnectionClosed
        );
        bounded(client.close()).await;
        server.push(Reply {
            truncate_body: true,
            ..Reply::json(200, r#"{"Item":{"pk":{"S":"truncated"}}}"#)
        });
        let mut cfg = config(port);
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        let (output, metadata) = bounded(get(&client)).await.unwrap();
        let error = output.unwrap_err();
        assert_eq!(metadata.attempts, 1);
        assert!(metadata.is_empty());
        assert_eq!(
            _alternator_rust::transport_failure::TransportFailure::find(&error)
                .unwrap()
                .category,
            _alternator_rust::transport_failure::Category::ConnectionClosed
        );
        #[cfg(feature = "conversion-tests")]
        pyo3::Python::attach(|py| {
            use pyo3::prelude::*;
            let mapped = _alternator_rust::errors::sdk_error(
                py, "GetItem", "unused", &error, &metadata,
            ).unwrap();
            assert!(mapped.is_instance_of::<_alternator_rust::errors::TransportError>(py));
            let value = mapped.value(py);
            assert_eq!(value.getattr("category").unwrap().extract::<String>().unwrap(), "connection_closed");
            assert_eq!(value.getattr("endpoint_url").unwrap().extract::<String>().unwrap(), format!("http://127.0.0.1:{port}/"));
            let diagnostic = value.str().unwrap().to_string();
            assert_eq!(diagnostic.matches("connection_closed:").count(), 1);
            assert!(diagnostic.contains("end of file before message length reached"), "{diagnostic}");
        });
        bounded(client.close()).await;
        assert_eq!(client.live_drivers(), 0);
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

// These checks execute only in the explicitly Python-linked converter test
// configuration. The ordinary core suite remains entirely independent of Python.
#[cfg(feature = "conversion-tests")]
#[test]
fn parsed_sdk_scalars_preserve_final_response_presence_and_retry_metadata() {
    use _alternator_rust::{conversion::Output, response};
    use pyo3::prelude::*;
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        let mut cfg = config(server.port);
        cfg.response_compression = vec!["gzip".into()];
        let client = Client::create(cfg).await.unwrap();
        for operation in ["Query", "Scan"] {
            for (body, encoding) in [
                ("{}", "x-gzip"),
                (r#"{"Count":0}"#, "GzIp ; q=1.0"),
                (
                    r#"{"ScannedCount":0,"Items":[],"LastEvaluatedKey":{}}"#,
                    "gzip",
                ),
            ] {
                server.push(Reply::json(500, r#"{"__type":"InternalServerError"}"#));
                let mut compressed =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                compressed.write_all(body.as_bytes()).unwrap();
                server.push(Reply {
                    // The SDK ignores a second gzip member and arbitrary trailer.
                    // Presence projection must consume the same first member.
                    body: {
                        let mut bytes = compressed.finish().unwrap();
                        let mut trailer = flate2::write::GzEncoder::new(
                            Vec::new(),
                            flate2::Compression::default(),
                        );
                        trailer.write_all(b"ignored member").unwrap();
                        bytes.extend(trailer.finish().unwrap());
                        bytes.extend(b"ignored trailer");
                        bytes
                    },
                    encoding: Some(encoding),
                    ..Reply::json(200, "")
                });
                let (output, metadata) = client
                    .run(|sdk| async move {
                        if operation == "Query" {
                            Output::Query(Box::new(
                                sdk.query().table_name("contract").send().await.unwrap(),
                            ))
                        } else {
                            Output::Scan(Box::new(
                                sdk.scan().table_name("contract").send().await.unwrap(),
                            ))
                        }
                    })
                    .await
                    .unwrap();
                Python::attach(|py| {
                    let result = response::success(py, &output, &metadata).unwrap();
                    assert_eq!(
                        result.contains("Count").unwrap(),
                        body.contains("\"Count\"")
                    );
                    assert_eq!(
                        result.contains("ScannedCount").unwrap(),
                        body.contains("\"ScannedCount\"")
                    );
                    assert_eq!(
                        result.contains("Items").unwrap(),
                        body.contains("\"Items\"")
                    );
                    assert_eq!(
                        result
                            .get_item("ResponseMetadata")
                            .unwrap()
                            .unwrap()
                            .get_item("RetryAttempts")
                            .unwrap()
                            .extract::<u32>()
                            .unwrap(),
                        1
                    );
                });
                server.request().await;
                server.request().await;
            }
        }
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[test]
fn connection_and_connect_timeout_have_structured_categories() {
    use _alternator_rust::{
        transport::Transport,
        transport_failure::{Category, TransportFailure},
    };
    use alternator_driver::AlternatorClient;
    shared_runtime().unwrap().block_on(async {
        let reserved = TcpSocket::new_v4().unwrap();
        reserved.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = reserved.local_addr().unwrap().port();
        let mut cfg = config(port);
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        let (output, metadata) = bounded(get(&client)).await.unwrap();
        let error = output.unwrap_err();
        let failure = TransportFailure::find(&error).unwrap();
        assert_eq!(failure.category, Category::EndpointConnection);
        assert_eq!(failure.endpoint_url, format!("http://127.0.0.1:{port}/"));
        assert!(metadata.responses.is_empty());
        bounded(client.close()).await;

        // Complete TCP but hold the TLS handshake until the connect deadline.
        let listener = reserved.listen(128).unwrap();
        let held = AbortOnDropHandle::new(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 1);
            let mut rest = Vec::new();
            socket.read_to_end(&mut rest).await.unwrap();
        }));
        let mut cfg = config(port);
        cfg.scheme = "https".into();
        cfg.connect_seconds = 0.03;
        cfg.max_attempts = 1;
        let transport = Transport::new(cfg.tls_config().unwrap());
        let sdk = AlternatorClient::try_from_conf(
            cfg.to_driver_config(transport.clone())
                .unwrap()
                .to_builder()
                .without_discovery()
                .build(),
        )
        .unwrap();
        let error = bounded(
            sdk.get_item()
                .table_name("contract")
                .key("pk", AttributeValue::S("x".into()))
                .customize()
                .mutate_request(move |request| {
                    request
                        .set_uri(format!("https://127.0.0.1:{port}/connect/path?key=value"))
                        .unwrap();
                })
                .send(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            TransportFailure::find(&error).unwrap().category,
            Category::ConnectTimeout
        );
        assert_eq!(
            TransportFailure::find(&error).unwrap().endpoint_url,
            format!("https://127.0.0.1:{port}/connect/path?key=value")
        );
        bounded(held).await.unwrap();
        sdk.shutdown().await;
        bounded(transport.finish_close()).await;
    });
}

#[test]
fn tls_handshake_io_and_protocol_failures_preserve_categories_and_retries() {
    use _alternator_rust::{
        transport::Transport,
        transport_failure::{Category, TransportFailure},
    };
    use alternator_driver::AlternatorClient;
    shared_runtime().unwrap().block_on(async {
        for alert in [false, true] {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let peer = AbortOnDropHandle::new(tokio::spawn(async move {
                for _ in 0..2 {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut hello = [0; 4096];
                    assert!(socket.read(&mut hello).await.unwrap() > 0);
                    if alert {
                        // TLS fatal handshake_failure alert, distinct from plain EOF.
                        socket
                            .write_all(b"\x15\x03\x03\x00\x02\x02\x28")
                            .await
                            .unwrap();
                    }
                    socket.shutdown().await.unwrap();
                }
            }));
            let mut cfg = config(port);
            cfg.scheme = "https".into();
            cfg.max_attempts = 2;
            let transport = Transport::new(cfg.tls_config().unwrap());
            let sdk = AlternatorClient::try_from_conf(
                cfg.to_driver_config(transport.clone())
                    .unwrap()
                    .to_builder()
                    .without_discovery()
                    .build(),
            )
            .unwrap();
            let endpoint_url = if alert {
                format!("https://127.0.0.1:{port}/retry/path?key=value")
            } else {
                format!("https://127.0.0.1:{port}")
            };
            let wire_endpoint = endpoint_url.clone();
            let error = bounded(
                sdk.get_item()
                    .table_name("contract")
                    .key("pk", AttributeValue::S("x".into()))
                    .customize()
                    .mutate_request(move |request| {
                        request.set_uri(wire_endpoint.clone()).unwrap();
                    })
                    .send(),
            )
            .await
            .unwrap_err();
            let expected_category = if alert {
                Category::Tls
            } else {
                Category::EndpointConnection
            };
            let failure = TransportFailure::find(&error).unwrap();
            assert_eq!(failure.category, expected_category);
            assert_eq!(failure.endpoint_url, endpoint_url);
            if alert {
                assert!(failure.to_string().contains("HandshakeFailure"));
            }
            // Both accepts are required: the actual SDK must retry the EOF.
            bounded(peer).await.unwrap();
            #[cfg(feature = "conversion-tests")]
            pyo3::Python::attach(|py| {
                use pyo3::prelude::*;
                let mapped = _alternator_rust::errors::sdk_error(
                    py,
                    "GetItem",
                    "unused",
                    &error,
                    &_alternator_rust::metadata::OperationMetadata::default(),
                )
                .unwrap();
                assert_eq!(
                    mapped
                        .value(py)
                        .getattr("category")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    expected_category.as_str()
                );
                assert_eq!(
                    mapped
                        .value(py)
                        .getattr("endpoint_url")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    endpoint_url
                );
            });
            sdk.shutdown().await;
            bounded(transport.finish_close()).await;
        }
    });
}

#[cfg(feature = "conversion-tests")]
#[test]
fn final_http_errors_and_protocol_failures_keep_distinct_python_categories() {
    use _alternator_rust::{conversion::Output, errors, response};
    use aws_smithy_runtime_api::client::result::SdkError;
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        let mut cfg = config(server.port);
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        for reply in [
            Reply::json(503, ""),
            Reply::json(502, "<html>proxy failure</html>"),
            Reply::json(500, " \t\r\n\x0b\x0c"),
            Reply {
                body: b" \r\n<html>caf\xe9</html>".to_vec(),
                extra_headers: b"x-amzn-errortype: ProxyError:http\r\n".to_vec(),
                ..Reply::json(503, "")
            },
            Reply::json(
                400,
                r#"{"__type":"ResourceNotFoundException","message":null,"future":[1e400,-1e400]}"#,
            ),
            Reply::json(200, r#"{"Count":"oops"}"#),
            Reply::json(200, ""),
            Reply {
                extra_headers: b"invalid header\r\n".to_vec(),
                ..Reply::json(200, "{}")
            },
            Reply::json(0, ""),
        ] {
            let status = reply.status;
            let expected_message = if status >= 500 {
                http::StatusCode::from_u16(status)
                    .unwrap()
                    .canonical_reason()
                    .unwrap()
                    .to_owned()
            } else {
                String::from_utf8(reply.body.clone()).unwrap()
            };
            let malformed_header = reply.extra_headers == b"invalid header\r\n";
            let expected_success = status == 200 && reply.body.is_empty() && !malformed_header;
            server.push(reply);
            let (result, metadata) = client
                .run(|sdk| async move { sdk.query().table_name("contract").send().await })
                .await
                .unwrap();
            assert_eq!(result.is_ok(), expected_success);
            Python::attach(|py| match result {
                Ok(output) => {
                    assert_eq!(status, 200);
                    let result =
                        response::success(py, &Output::Query(Box::new(output)), &metadata).unwrap();
                    assert_eq!(result.len(), 1);
                    assert!(result.contains("ResponseMetadata").unwrap());
                }
                Err(error) => {
                    let mapped =
                        errors::sdk_error(py, "Query", "http://node", &error, &metadata).unwrap();
                    if status >= 400 {
                        assert!(matches!(error, SdkError::ServiceError(_)));
                        assert!(mapped.is_instance_of::<errors::ServiceError>(py));
                        assert_eq!(
                            mapped
                                .value(py)
                                .getattr("response")
                                .unwrap()
                                .get_item("Error")
                                .unwrap()
                                .get_item("Code")
                                .unwrap()
                                .extract::<String>()
                                .unwrap(),
                            if status == 400 {
                                "ResourceNotFoundException".into()
                            } else {
                                status.to_string()
                            }
                        );
                        let response = mapped.value(py).getattr("response").unwrap();
                        let message = response
                            .get_item("Error")
                            .unwrap()
                            .get_item("Message")
                            .unwrap();
                        if status == 400 {
                            assert!(message.is_none());
                            assert_eq!(
                                response
                                    .get_item("future")
                                    .unwrap()
                                    .extract::<Vec<f64>>()
                                    .unwrap(),
                                vec![f64::INFINITY, f64::NEG_INFINITY]
                            );
                        } else {
                            assert_eq!(response.len().unwrap(), 2);
                            assert_eq!(message.extract::<String>().unwrap(), expected_message);
                        }
                        assert_eq!(
                            mapped
                                .value(py)
                                .getattr("operation_name")
                                .unwrap()
                                .extract::<String>()
                                .unwrap(),
                            "Query"
                        );
                    } else if status == 0 {
                        assert!(mapped.is_instance_of::<errors::TransportError>(py));
                        assert_eq!(
                            mapped
                                .value(py)
                                .getattr("category")
                                .unwrap()
                                .extract::<String>()
                                .unwrap(),
                            "connection_closed"
                        );
                        assert_eq!(
                            mapped
                                .value(py)
                                .getattr("endpoint_url")
                                .unwrap()
                                .extract::<String>()
                                .unwrap(),
                            format!("http://127.0.0.1:{}/", server.port)
                        );
                    } else {
                        if !malformed_header {
                            assert!(matches!(error, SdkError::ServiceError(_)));
                        }
                        assert!(mapped.is_instance_of::<PyRuntimeError>(py));
                        if !malformed_header {
                            let diagnostic = mapped.to_string();
                            assert!(diagnostic.contains("failed to parse JSON"), "{diagnostic}");
                            assert!(diagnostic.contains("found `oops`"), "{diagnostic}");
                        }
                    }
                }
            });
            server.request().await;
        }
        for (status, body, encoding, local_diagnostic) in [
            (400, b"\xff".as_slice(), None, "utf-8"),
            (503, b"caf\xe9".as_slice(), None, "utf-8"),
            (
                400,
                b"bad gzip".as_slice(),
                Some("gzip"),
                "invalid gzip header",
            ),
        ] {
            server.push(Reply {
                body: body.to_vec(),
                encoding,
                ..Reply::json(status, "")
            });
            let (result, metadata) = bounded(
                client.run(|sdk| async move { sdk.query().table_name("contract").send().await }),
            )
            .await
            .unwrap();
            let error = result.unwrap_err();
            if encoding.is_some() {
                assert!(matches!(&error, SdkError::ResponseError(_)), "{error:?}");
            } else {
                assert!(matches!(&error, SdkError::ServiceError(_)), "{error:?}");
            }
            Python::attach(|py| {
                let mapped = errors::sdk_error(py, "Query", "http://node", &error, &metadata)
                    .unwrap_or_else(|error| error);
                assert!(mapped.is_instance_of::<PyRuntimeError>(py));
                let diagnostic = mapped.to_string();
                assert!(
                    diagnostic.to_ascii_lowercase().contains(local_diagnostic),
                    "{diagnostic}"
                );
                assert!(
                    diagnostic.contains(if encoding.is_some() {
                        "response error"
                    } else {
                        "service error"
                    }),
                    "{diagnostic}"
                );
                assert!(
                    diagnostic.contains(&format!("StatusCode({status})")),
                    "{diagnostic}"
                );
            });
            server.request().await;
        }
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[cfg(feature = "conversion-tests")]
#[test]
fn modeled_error_projection_retains_sdk_parsing_categories() {
    use _alternator_rust::errors;
    use aws_smithy_types::error::metadata::ProvideErrorMetadata;
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        let mut cfg = config(server.port);
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        // Expected service code, SDK code, and typed ErrorMetadata source.
        for (body, expected, sdk_code, metadata_source) in [
            (
                r#"{"__type":"ConditionalCheckFailedException","Item":{"a":{"S":7}}}"#.to_owned(),
                None,
                None,
                false,
            ),
            (
                r#"{"__type":"ConditionalCheckFailedException","Item":{"a":{}}}"#.to_owned(),
                None,
                None,
                false,
            ),
            (
                format!(
                    r#"{{"__type":"ResourceNotFoundException","future":{}0{}}}"#,
                    "[".repeat(512),
                    "]".repeat(512)
                ),
                Some("ResourceNotFoundException"),
                Some("ResourceNotFoundException"),
                false,
            ),
            (
                format!(
                    r#"{{"__type":"ResourceNotFoundException","future":{}0{}}}"#,
                    "[".repeat(513),
                    "]".repeat(513)
                ),
                None,
                None,
                false,
            ),
            (
                r#"{"message":"no code"}"#.to_owned(),
                Some("400"),
                None,
                true,
            ),
            (
                r#"{"__type":"FutureError","message":"unknown code"}"#.to_owned(),
                Some("FutureError"),
                Some("FutureError"),
                true,
            ),
            (
                r#"{"__type":"ResourceNotFoundException","future":"\ud800"}"#.to_owned(),
                Some("ResourceNotFoundException"),
                Some("ResourceNotFoundException"),
                false,
            ),
            (
                r#"{"__type":"ResourceNotFoundException","future":"\udc00"}"#.to_owned(),
                Some("ResourceNotFoundException"),
                Some("ResourceNotFoundException"),
                false,
            ),
            (
                r#"{"__type":"ResourceNotFoundException","future":{"\ud800":"\udc00"}}"#.to_owned(),
                Some("ResourceNotFoundException"),
                Some("ResourceNotFoundException"),
                false,
            ),
            (r#"{"broken":"#.to_owned(), Some("400"), None, false),
            ("proxy failure".to_owned(), Some("400"), None, false),
        ] {
            server.push(Reply::json(400, &body));
            let (result, metadata) = bounded(client.run(|sdk| async move {
                sdk.put_item()
                    .table_name("contract")
                    .item("a", AttributeValue::S("x".into()))
                    .send()
                    .await
            }))
            .await
            .unwrap();
            let error = result.unwrap_err();
            let service = error.as_service_error().unwrap();
            assert_eq!(service.code(), sdk_code);
            assert_eq!(
                std::error::Error::source(service)
                    .is_some_and(|source| source.is::<aws_smithy_types::error::ErrorMetadata>()),
                metadata_source
            );
            Python::attach(|py| {
                let mapped =
                    errors::sdk_error(py, "PutItem", "http://node", &error, &metadata).unwrap();
                if let Some(expected) = expected {
                    assert!(mapped.is_instance_of::<errors::ServiceError>(py));
                    let value = mapped.value(py);
                    let response = value.getattr("response").unwrap();
                    assert_eq!(
                        response
                            .get_item("Error")
                            .unwrap()
                            .get_item("Code")
                            .unwrap()
                            .extract::<String>()
                            .unwrap(),
                        expected
                    );
                    assert_eq!(
                        value.getattr("body").unwrap().extract::<Vec<u8>>().unwrap(),
                        body.as_bytes()
                    );
                    if expected == "ResourceNotFoundException" && body.contains("[[[") {
                        let mut value = response.get_item("future").unwrap();
                        for _ in 0..512 {
                            value = value.get_item(0).unwrap();
                        }
                        assert_eq!(value.extract::<i32>().unwrap(), 0);
                    } else if expected == "ResourceNotFoundException" {
                        let expected = py
                            .import("json")
                            .unwrap()
                            .call_method1("loads", (&body,))
                            .unwrap()
                            .get_item("future")
                            .unwrap();
                        assert!(response.get_item("future").unwrap().eq(expected).unwrap());
                    }
                    if body == "proxy failure" || body == r#"{"broken":"# {
                        assert_eq!(
                            response
                                .get_item("Error")
                                .unwrap()
                                .get_item("Message")
                                .unwrap()
                                .extract::<String>()
                                .unwrap(),
                            body
                        );
                    }
                } else {
                    assert!(mapped.is_instance_of::<PyRuntimeError>(py));
                    assert!(!mapped.is_instance_of::<errors::ServiceError>(py));
                    // Retain the original structured SDK cause in the report.
                    let report = mapped.to_string();
                    assert!(
                        report.contains("expected ValueString")
                            || report.contains("valid variant")
                            || report.contains("max recursion depth"),
                        "{report}"
                    );
                }
            });
            server.request().await;
        }
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[cfg(feature = "conversion-tests")]
#[test]
fn error_binary_fields_follow_the_successfully_parsed_sdk_shape() {
    use _alternator_rust::errors;
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        let mut cfg = config(server.port);
        cfg.max_attempts = 1;
        let client = Client::create(cfg).await.unwrap();
        let deep_item = format!(
            r#"{{"__type":"ConditionalCheckFailedException","Item":{{"a":{}{{"B":"AP8="}}{}}}}}"#,
            r#"{"M":{"a":"#.repeat(64),
            "}}".repeat(64)
        );
        // Operation/header select the SDK shape; the body retains Python identity.
        // Shape: 0 = generic, 1 = conditional, 2 = cancellation.
        for (operation, body, header, shape, binary, canonical) in [
            ("Query", deep_item.as_str(), None, 0, false, "ConditionalCheckFailedException"),
            ("Query", r#"{"__type":"ConditionalCheckFailedException","Item":[]}"#, None, 0, false, "ConditionalCheckFailedException"),
            ("Query", r#"{"__type":"ConditionalCheckFailedException","Item":{"a":{"B":"AP8="}}}"#, None, 0, false, "ConditionalCheckFailedException"),
            ("Query", r#"{"__type":"TransactionCanceledException","CancellationReasons":{}}"#, None, 0, false, "TransactionCanceledException"),
            ("Query", r#"{"__type":"TransactionCanceledException","CancellationReasons":[{"Item":{"a":{"B":"AP8="}}}]}"#, None, 0, false, "TransactionCanceledException"),
            ("PutItem", r#"{"__type":"ConditionalCheckFailedException","Item":{"a":{"B":"AP8="}}}"#, None, 1, true, "ConditionalCheckFailedException"),
            ("PutItem", r#"{"__type":"ConditionalCheckFailedException","Item":{"a":{"B":"AP8="}}}"#, Some("FutureError"), 0, false, "ConditionalCheckFailedException"),
            ("PutItem", r#"{"__type":"FutureError","Item":{"a":{"B":"AP8="}}}"#, Some("ConditionalCheckFailedException"), 1, true, "FutureError"),
            ("TransactWriteItems", r#"{"__type":"TransactionCanceledException","CancellationReasons":[{"Item":{"a":{"B":"AP8="}}}]}"#, None, 2, true, "TransactionCanceledException"),
            ("PutItem", r#"{"__type":"ConditionalCheckFailedException","Item":{"a":{"B":"AP8=","B":null}}}"#, None, 1, false, "ConditionalCheckFailedException"),
            ("PutItem", r#"{"__type":"ConditionalCheckFailedException","Item":{"a":{"future":7,"future":null}}}"#, None, 1, false, "ConditionalCheckFailedException"),
            ("TransactWriteItems", r#"{"__type":"TransactionCanceledException","CancellationReasons":[{"Item":{"a":{"B":"AP8=","B":null}}}]}"#, None, 2, false, "TransactionCanceledException"),
        ] {
            let mut reply = Reply::json(400, body);
            if let Some(header) = header {
                reply.extra_headers = format!("x-amzn-errortype: {header}\r\n").into_bytes();
            }
            server.push(reply);
            // Keep each operation's actual SDK error type through sdk_error.
            macro_rules! project {
                ($request:expr) => {{
                    let (result, metadata) = bounded(client.run(|sdk| async move { $request(sdk).await })).await.unwrap();
                    assert_eq!(metadata.attempts, 1);
                    let error = result.unwrap_err();
                    let source = std::error::Error::source(error.as_service_error().unwrap()).unwrap();
                    assert_eq!(source.is::<aws_sdk_dynamodb::types::error::ConditionalCheckFailedException>(), shape == 1);
                    assert_eq!(source.is::<aws_sdk_dynamodb::types::error::TransactionCanceledException>(), shape == 2);
                    assert_eq!(source.is::<aws_smithy_types::error::ErrorMetadata>(), shape == 0);
                    let mapped = Python::attach(|py| errors::sdk_error(py, operation, "http://node", &error, &metadata))
                        .unwrap_or_else(|error| error);
                    (mapped, metadata)
                }};
            }
            let (mapped, metadata) = match operation {
                "Query" => project!(|sdk: &alternator_driver::AlternatorClient| sdk.query().table_name("contract").send()),
                "PutItem" => project!(|sdk: &alternator_driver::AlternatorClient| sdk.put_item().table_name("contract").item("a", AttributeValue::S("x".into())).send()),
                "TransactWriteItems" => project!(|sdk: &alternator_driver::AlternatorClient| sdk.transact_write_items().send()),
                _ => unreachable!(),
            };
            Python::attach(|py| {
                assert!(mapped.is_instance_of::<errors::ServiceError>(py), "{operation}: {mapped}");
                let value = mapped.value(py);
                let response = value.getattr("response").unwrap();
                let expected = py.import("json").unwrap().call_method1("loads", (body,)).unwrap();
                expected.del_item("__type").unwrap();
                if binary {
                    let item = if shape == 1 {
                        expected.get_item("Item").unwrap()
                    } else {
                        expected.get_item("CancellationReasons").unwrap().get_item(0).unwrap().get_item("Item").unwrap()
                    };
                    item.get_item("a").unwrap().set_item("B", PyBytes::new(py, &[0, 255])).unwrap();
                }
                for (key, expected) in expected.cast::<pyo3::types::PyDict>().unwrap().iter() {
                    assert!(response.get_item(&key).unwrap().eq(expected).unwrap(), "{operation}: {body}");
                }
                assert_eq!(response.get_item("Error").unwrap().get_item("Code").unwrap().extract::<String>().unwrap(), canonical);
                assert_eq!(value.getattr("operation_name").unwrap().extract::<String>().unwrap(), operation);
                assert_eq!(value.getattr("body").unwrap().extract::<Vec<u8>>().unwrap(), body.as_bytes());
                assert_eq!(value.getattr("headers").unwrap().extract::<Vec<(String, Vec<u8>)>>().unwrap(), metadata.responses[0].headers);
                let context = response.get_item("ResponseMetadata").unwrap();
                assert_eq!(context.get_item("HTTPStatusCode").unwrap().extract::<u16>().unwrap(), 400);
                assert_eq!(context.get_item("RetryAttempts").unwrap().extract::<u32>().unwrap(), 0);
                assert!(!context.get_item("RequestId").unwrap().extract::<String>().unwrap().is_empty());
            });
            server.request().await;
        }
        bounded(client.close()).await;
        assert_eq!(client.live_drivers(), 0);
        server.stop().await;
    });
}

#[cfg(feature = "conversion-tests")]
#[test]
fn unknown_success_attributes_match_python_empty_structures() {
    use _alternator_rust::{conversion::Output, response};
    use pyo3::prelude::*;
    shared_runtime().unwrap().block_on(async {
        let mut server = Server::start().await;
        server.push(Reply::json(200, r#"{"Item":{"known":{"S":"kept"},"future":{"FUTURE":{"x":7}},"nested":{"M":{"list":{"L":[{"FUTURE":7},{"N":"1.00"}]}}}}}"#));
        let client = Client::create(config(server.port)).await.unwrap();
        let (output, metadata) = bounded(get(&client)).await.unwrap();
        let output = output.unwrap();
        assert!(output.item.as_ref().unwrap()["future"].is_unknown());
        Python::attach(|py| {
            let first = response::success(py, &Output::GetItem(Box::new(output.clone())), &metadata).unwrap();
            let second = response::success(py, &Output::GetItem(Box::new(output)), &metadata).unwrap();
            let item = first.get_item("Item").unwrap().unwrap();
            let unknown = item.get_item("future").unwrap();
            assert!(unknown.is_empty().unwrap());
            assert_eq!(item.get_item("known").unwrap().get_item("S").unwrap().extract::<String>().unwrap(), "kept");
            let nested = item.get_item("nested").unwrap().get_item("M").unwrap().get_item("list").unwrap().get_item("L").unwrap();
            assert!(nested.get_item(0).unwrap().is_empty().unwrap());
            assert_eq!(nested.get_item(1).unwrap().get_item("N").unwrap().extract::<String>().unwrap(), "1.00");
            unknown.set_item("changed", true).unwrap();
            assert!(second.get_item("Item").unwrap().unwrap().get_item("future").unwrap().is_empty().unwrap());
        });
        server.request().await;
        bounded(client.close()).await;
        server.stop().await;
    });
}

#[cfg(feature = "conversion-tests")]
#[test]
fn malformed_http_retries_keep_final_protocol_error_category() {
    use _alternator_rust::{
        conversion::Output, errors, response, transport_failure::TransportFailure,
    };
    use aws_smithy_runtime_api::client::result::SdkError;
    use pyo3::prelude::*;
    shared_runtime().unwrap().block_on(async {
        for malformed_status in [false, true] {
            for final_success in [true, false] {
                let server = Server::start().await;
                let malformed = if malformed_status {
                    // Four digits are not a valid HTTP status line.
                    Reply::json(1000, "{}")
                } else {
                    Reply {
                        extra_headers: b"invalid header\r\n".to_vec(),
                        ..Reply::json(200, "{}")
                    }
                };
                server.push(malformed.clone());
                server.push(if final_success {
                    Reply::json(200, r#"{"Item":{"pk":{"S":"ok"}}}"#)
                } else {
                    malformed
                });
                let mut cfg = config(server.port);
                cfg.max_attempts = 2;
                cfg.operation_timeout_seconds = 10.;
                let client = Client::create(cfg).await.unwrap();
                let (output, metadata) = bounded(get(&client)).await.unwrap();
                assert_eq!(metadata.attempts, 2);
                assert!(server.scripts.lock().unwrap().is_empty());
                Python::attach(|py| match output {
                    Ok(output) => {
                        assert!(final_success);
                        assert_eq!(output.item.as_ref().unwrap()["pk"].as_s().unwrap(), "ok");
                        assert_eq!(metadata.responses.len(), 1);
                        assert_eq!(metadata.responses[0].attempt, 2);
                        let result =
                            response::success(py, &Output::GetItem(Box::new(output)), &metadata)
                                .unwrap();
                        let response_metadata =
                            result.get_item("ResponseMetadata").unwrap().unwrap();
                        assert_eq!(
                            response_metadata
                                .get_item("HTTPStatusCode")
                                .unwrap()
                                .extract::<u16>()
                                .unwrap(),
                            200
                        );
                        assert_eq!(
                            response_metadata
                                .get_item("RetryAttempts")
                                .unwrap()
                                .extract::<u32>()
                                .unwrap(),
                            1
                        );
                    }
                    Err(error) => {
                        assert!(!final_success);
                        assert!(
                            matches!(&error, SdkError::DispatchFailure(failure) if failure.is_io())
                        );
                        assert!(metadata.responses.is_empty());
                        assert!(TransportFailure::find(&error).is_none());
                        let mapped =
                            errors::sdk_error(py, "GetItem", "http://node", &error, &metadata)
                                .unwrap();
                        assert!(mapped.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
                        assert!(!mapped.is_instance_of::<errors::TransportError>(py));
                        let diagnostic = mapped.to_string();
                        assert!(
                            diagnostic.contains(if malformed_status {
                                "invalid HTTP status-code"
                            } else {
                                "invalid HTTP header"
                            }),
                            "{diagnostic}"
                        );
                    }
                });
                bounded(client.close()).await;
                assert_eq!(client.live_drivers(), 0);
                server.stop().await;
            }
        }
    });
}
