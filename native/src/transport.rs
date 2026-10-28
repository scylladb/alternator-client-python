// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-client HTTP/1 pools with explicitly owned socket driver tasks.
//! The SDK continues to serialize, sign, route, retry, decompress and parse.
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use http_body_util::BodyExt;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::metadata::{self, AttemptResponse};
use crate::transport_failure::{Category, TransportFailure};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}

#[derive(Debug)]
struct Connection {
    sender: SendRequest<SdkBody>,
    cancellation: CancellationToken,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[derive(Debug)]
struct Inner {
    idle: Mutex<HashMap<String, Vec<Connection>>>,
    cancellation: CancellationToken,
    drivers: TaskTracker,
    tls: Arc<rustls::ClientConfig>,
}

#[derive(Clone, Debug)]
pub struct Transport(Arc<Inner>);

impl Transport {
    pub fn new(tls: rustls::ClientConfig) -> Self {
        Self(Arc::new(Inner {
            idle: Mutex::default(),
            cancellation: CancellationToken::new(),
            drivers: TaskTracker::new(),
            tls: Arc::new(tls),
        }))
    }

    /// Admission closes before clearing pools, so in-flight connects cannot
    /// install a new driver after the tracker has drained.
    pub fn begin_close(&self) {
        self.0.cancellation.cancel();
        self.0.idle.lock().unwrap().clear();
    }

    /// Caller first joins upstream discovery and drains operation registrations.
    pub async fn finish_close(&self) {
        self.begin_close();
        self.0.drivers.close();
        self.0.drivers.wait().await;
    }

    pub fn live_drivers(&self) -> usize {
        self.0.drivers.len()
    }

    async fn connect(
        &self,
        uri: &http::Uri,
        timeout: Option<Duration>,
    ) -> Result<Connection, BoxError> {
        let host = uri.host().ok_or("HTTP endpoint has no host")?;
        // http::Uri preserves brackets around IPv6 host literals.
        let hostname = host
            .strip_prefix('[')
            .and_then(|v| v.strip_suffix(']'))
            .unwrap_or(host);
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err("unsupported endpoint scheme".into()),
        };
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let connect = async {
            let stream = TcpStream::connect((hostname, port)).await?;
            stream.set_nodelay(true)?;
            let socket: Box<dyn Socket> = if https {
                let name = ServerName::try_from(hostname.to_owned())?;
                Box::new(
                    TlsConnector::from(self.0.tls.clone())
                        .connect(name, stream)
                        .await?,
                )
            } else {
                Box::new(stream)
            };
            let (sender, driver) =
                hyper::client::conn::http1::handshake(TokioIo::new(socket)).await?;
            let cancellation = self.0.cancellation.child_token();
            // spawn and cancellation are performed on the still-running shared
            // runtime; every created driver stays owned until it is joined.
            let token = cancellation.clone();
            self.0.drivers.spawn(async move {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {},
                    _ = driver => {},
                }
            });
            Ok::<_, BoxError>(Connection {
                sender,
                cancellation,
            })
        };
        match timeout {
            Some(duration) => tokio::time::timeout(duration, connect).await.map_err(|_| {
                Box::<dyn std::error::Error + Send + Sync>::from(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "native connect timeout",
                ))
            })?,
            None => connect.await,
        }
    }

    async fn request(
        &self,
        request: HttpRequest,
        settings: HttpConnectorSettings,
    ) -> Result<HttpResponse, BoxError> {
        let attempt = request
            .extension::<metadata::AttemptNumber>()
            .map_or(1, |attempt| attempt.0);
        // Retain Smithy's original spelling; http::Uri formatting can add a
        // root slash before we rewrite the request to origin-form.
        let endpoint_url = request.uri().to_string();
        let mut request = request.try_into_http1x()?;
        let uri = request.uri().clone();
        let discovery = request.method() == http::Method::GET && uri.path() == "/localnodes";
        let authority = uri
            .authority()
            .ok_or("HTTP endpoint has no authority")?
            .as_str();
        let key = format!("{}://{}", uri.scheme_str().unwrap_or("http"), authority);
        let pooled = self.0.idle.lock().unwrap().get_mut(&key).and_then(Vec::pop);
        let mut connection = match pooled {
            Some(connection) if !connection.sender.is_closed() => connection,
            _ => self
                .connect(&uri, settings.connect_timeout())
                .await
                .map_err(|error| connect_failure(error, &endpoint_url))?,
        };
        // HTTP/1 direct connections require origin-form, and Hyper supplies Host
        // only for higher-level clients. Preserve the exact signing authority.
        if !request.headers().contains_key(http::header::HOST) {
            request
                .headers_mut()
                .insert(http::header::HOST, authority.parse()?);
        }
        *request.uri_mut() = uri
            .path_and_query()
            .map_or("/", |value| value.as_str())
            .parse()?;
        connection
            .sender
            .ready()
            .await
            .map_err(|error| connection_failure(error, &endpoint_url))?;
        let response = connection
            .sender
            .send_request(request)
            .await
            .map_err(|error| connection_failure(error, &endpoint_url))?;
        let (parts, body) = response.into_parts();
        // The pinned discovery deserializer accepts node arrays regardless of
        // status. Return a request failure so upstream retains its snapshot and
        // tries its normal candidates/seeds. Dropping the lease closes this
        // unread response; ordinary SDK service errors still reach the parser.
        if discovery && !parts.status.is_success() {
            return Err(format!("topology discovery returned HTTP {}", parts.status).into());
        }
        let bytes = body
            .collect()
            .await
            .map_err(|error| connection_failure(error, &endpoint_url))?
            .to_bytes();
        if let Some(capture) = metadata::current_capture() {
            capture.lock().unwrap().responses.push(AttemptResponse {
                status: parts.status.as_u16(),
                attempt,
                headers: parts
                    .headers
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
                    .collect(),
                body: bytes.to_vec(),
            });
        }
        // Holding this mutex also serializes pool return against close/clear.
        let mut idle = self.0.idle.lock().unwrap();
        if !self.0.cancellation.is_cancelled() && !connection.sender.is_closed() {
            let connections = idle.entry(key).or_default();
            // Native default: retain at most 200 idle connections per authority;
            // concurrent active requests do not wait for a Python pool setting.
            if connections.len() < 200 {
                connections.push(connection);
            }
        }
        let response = http::Response::from_parts(parts, SdkBody::from(bytes));
        Ok(HttpResponse::try_from(response)?)
    }
}

#[derive(Clone, Debug)]
struct Connector {
    transport: Transport,
    settings: HttpConnectorSettings,
}

impl HttpConnector for Connector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let connector = self.clone();
        HttpConnectorFuture::new(async move {
            let endpoint_url = request.uri().to_string();
            let work = connector
                .transport
                .request(request, connector.settings.clone());
            tokio::select! {
                biased;
                _ = connector.transport.0.cancellation.cancelled() => Err(ConnectorError::user("native transport is closed".into())),
                result = async {
                    match connector.settings.read_timeout() {
                        Some(duration) => match tokio::time::timeout(duration, work).await {
                            Ok(result) => result.map_err(connector_error),
                            Err(error) => Err(ConnectorError::timeout(Box::new(TransportFailure::new(
                                Category::ReadTimeout, endpoint_url, Box::new(error),
                            )))),
                        },
                        None => work.await.map_err(connector_error),
                    }
                } => result,
            }
        })
    }
}

fn connect_failure(error: BoxError, endpoint: &str) -> BoxError {
    if TransportFailure::find(error.as_ref()).is_some() {
        return error;
    }
    let category = if tls_error_retryable(error.as_ref()).is_some() {
        Category::Tls
    } else if error
        .downcast_ref::<io::Error>()
        .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut)
    {
        Category::ConnectTimeout
    } else if error.downcast_ref::<io::Error>().is_some()
        || error.downcast_ref::<hyper::Error>().is_some()
    {
        Category::EndpointConnection
    } else {
        // Local URI/configuration errors are not retryable network failures.
        return error;
    };
    Box::new(TransportFailure::new(category, endpoint, error))
}

fn connection_failure(error: hyper::Error, endpoint: &str) -> BoxError {
    let tls = tls_error_retryable(&error).is_some();
    // Hyper wraps HTTP/1 body-framing decoder errors as body I/O failures,
    // outside is_parse(). TLS also uses InvalidData, so identify it first.
    let framing = !tls
        && std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some_and(|source| {
                matches!(
                    source.kind(),
                    io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData
                )
            });
    if error.is_parse() || framing {
        return Box::new(error);
    }
    let category = if tls {
        Category::Tls
    } else {
        Category::ConnectionClosed
    };
    Box::new(TransportFailure::new(category, endpoint, Box::new(error)))
}

// Classification and retry policy are separate: certificate/hostname failures
// do not retry, while other TLS protocol I/O retains the baseline retry policy.
fn tls_error_retryable(mut error: &(dyn std::error::Error + 'static)) -> Option<bool> {
    loop {
        let tls = error.downcast_ref::<rustls::Error>().or_else(|| {
            error
                .downcast_ref::<io::Error>()
                .and_then(io::Error::get_ref)
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        });
        if let Some(tls) = tls {
            return Some(!matches!(tls, rustls::Error::InvalidCertificate(_)));
        }
        if error
            .downcast_ref::<rustls::pki_types::InvalidDnsNameError>()
            .is_some()
        {
            return Some(false);
        }
        error = error.source()?;
    }
}

fn connector_error(error: BoxError) -> ConnectorError {
    if let Some(failure) = TransportFailure::find(error.as_ref()) {
        return match failure.category {
            Category::Tls if tls_error_retryable(error.as_ref()) == Some(true) => {
                ConnectorError::io(error)
            }
            Category::Tls => ConnectorError::user(error),
            Category::ConnectTimeout | Category::ReadTimeout => ConnectorError::timeout(error),
            Category::EndpointConnection | Category::ConnectionClosed => ConnectorError::io(error),
        };
    }
    // tokio-rustls wraps certificate/hostname verification failures in an
    // InvalidData io::Error. Other I/O failures remain retryable.
    if tls_error_retryable(error.as_ref()) == Some(false) {
        ConnectorError::user(error)
    } else if error
        .downcast_ref::<io::Error>()
        .is_some_and(|error| error.kind() == io::ErrorKind::TimedOut)
    {
        ConnectorError::timeout(error)
    } else if error.downcast_ref::<io::Error>().is_some()
        || error.downcast_ref::<hyper::Error>().is_some()
    {
        ConnectorError::io(error)
    } else {
        // Certificate/hostname errors are not transient transport failures.
        ConnectorError::user(error)
    }
}

impl HttpClient for Transport {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(Connector {
            transport: self.clone(),
            settings: settings.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_classification_preserves_categories_and_wraps_once() {
        let endpoint = "https://node:8043";
        for local in ["HTTP endpoint has no host", "unsupported endpoint scheme"] {
            let error = connect_failure(local.into(), endpoint);
            assert!(TransportFailure::find(error.as_ref()).is_none());
            assert!(connector_error(error).is_user());
        }
        for kind in [io::ErrorKind::ConnectionReset, io::ErrorKind::UnexpectedEof] {
            let error = connect_failure(Box::new(io::Error::new(kind, "handshake I/O")), endpoint);
            assert_eq!(
                TransportFailure::find(error.as_ref()).unwrap().category,
                Category::EndpointConnection
            );
            assert!(connector_error(error).is_io());
        }
        for tls in [
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
            rustls::Error::General("protocol failure".into()),
        ] {
            let certificate = matches!(tls, rustls::Error::InvalidCertificate(_));
            let error = connect_failure(
                Box::new(io::Error::new(io::ErrorKind::InvalidData, tls)),
                endpoint,
            );
            assert_eq!(
                TransportFailure::find(error.as_ref()).unwrap().category,
                Category::Tls
            );
            let text = error.to_string();
            let error = connect_failure(error, "unused");
            assert_eq!(error.to_string(), text);
            assert_eq!(error.to_string().matches("tls:").count(), 1);
            assert_eq!(
                TransportFailure::find(error.as_ref()).unwrap().endpoint_url,
                endpoint
            );
            let error = connector_error(error);
            assert_eq!(error.is_user(), certificate);
            assert_eq!(error.is_io(), !certificate);
        }
    }

    #[test]
    fn certificate_classification_preserves_network_io_and_timeout_retries() {
        let certificate = io::Error::new(
            io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert!(connector_error(Box::new(certificate)).is_user());
        for kind in [io::ErrorKind::ConnectionRefused, io::ErrorKind::InvalidData] {
            assert!(connector_error(Box::new(io::Error::new(kind, "network I/O"))).is_io());
        }
        assert!(connector_error(Box::new(io::Error::new(
            io::ErrorKind::TimedOut,
            "connect timeout"
        )))
        .is_timeout());
    }
}
