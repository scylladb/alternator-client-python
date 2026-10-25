// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Owned schema v1 settings. All validation/TLS preparation precedes discovery.
use std::collections::HashMap;
use std::io::Cursor;
use std::time::{Duration, Instant};

use alternator_driver::{
    AlternatorConfig, CompressionAlgorithm, CompressionLevel, KeyRouteAffinityConfig,
    KeyRouteAffinityType, RequestCompression, ResponseCompression, ResponseCompressionAlgorithm,
    RoutingScope,
};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::Region;
use aws_smithy_types::{retry::RetryConfig, timeout::TimeoutConfig};

use crate::transport::Transport;

pub type Scope = (String, Option<String>, Option<String>);

#[derive(Clone)]
pub struct NativeConfig {
    pub schema_version: u32,
    pub seed_hosts: Vec<String>,
    pub port: u16,
    pub scheme: String,
    pub scopes: Vec<Scope>,
    pub active_interval_ms: u64,
    pub idle_interval_ms: u64,
    pub key_affinity: String,
    pub table_pk_attributes: HashMap<String, String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub region: String,
    pub request_gzip: bool,
    pub compression_min_bytes: usize,
    pub gzip_level: u32,
    pub response_compression: Vec<String>,
    pub optimize_headers: bool,
    pub user_agent: Option<String>,
    pub max_attempts: u32,
    pub connect_seconds: f64,
    pub read_seconds: f64,
    pub discovery_seconds: f64,
    pub system_roots: bool,
    pub ca_pem: Vec<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
}

fn require(valid: bool, message: &str) -> Result<(), String> {
    if valid {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

fn string(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_control)
}

fn seconds(value: f64) -> Result<Duration, String> {
    require(
        value.is_finite() && value >= 1e-9,
        "timeout must be finite positive representable seconds",
    )?;
    let duration = Duration::try_from_secs_f64(value).map_err(|error| error.to_string())?;
    // Tokio timeouts omit their timer if adding the duration overflows Instant.
    require(
        Instant::now().checked_add(duration).is_some(),
        "timeout exceeds the platform's monotonic deadline range",
    )?;
    Ok(duration)
}

impl NativeConfig {
    pub fn validate(&self) -> Result<(), String> {
        require(
            self.schema_version == 1,
            "unsupported native config schema_version",
        )?;
        require(
            !self.seed_hosts.is_empty() && self.port != 0,
            "seed_hosts and nonzero port are required",
        )?;
        for host in &self.seed_hosts {
            require(
                string(host)
                    && !host
                        .chars()
                        .any(|c| c.is_whitespace() || "/\\?#@[]".contains(c)),
                "seed_hosts must contain host-only names or bare IP addresses",
            )?;
            require(
                !host.contains(':') || host.parse::<std::net::Ipv6Addr>().is_ok(),
                "seed host has a port or invalid IPv6 address",
            )?;
        }
        require(
            matches!(self.scheme.as_str(), "http" | "https"),
            "scheme must be http or https",
        )?;
        self.routing_scope()?;
        for interval in [self.active_interval_ms, self.idle_interval_ms] {
            require(
                interval > 0,
                "polling interval must be a positive u64 integer",
            )?;
        }
        require(
            matches!(self.key_affinity.as_str(), "NONE" | "RMW" | "ANY_WRITE"),
            "invalid key_affinity",
        )?;
        require(
            self.table_pk_attributes
                .iter()
                .all(|(table, pk)| string(table) && !pk.is_empty()),
            "table/partition key names must be nonempty strings",
        )?;
        require(string(&self.region), "region must be nonempty")?;
        match (
            &self.access_key_id,
            &self.secret_access_key,
            &self.session_token,
        ) {
            (Some(key), Some(secret), token) => require(
                string(key) && string(secret) && token.as_deref().is_none_or(string),
                "invalid static credentials",
            )?,
            (None, None, None) => {}
            _ => {
                return Err(
                    "static credentials require both key and secret; token requires credentials"
                        .to_owned(),
                )
            }
        }
        require(self.gzip_level <= 9, "invalid gzip level")?;
        require(
            self.response_compression
                .iter()
                .all(|value| matches!(value.as_str(), "gzip" | "deflate")),
            "invalid response compression",
        )?;
        require(
            self.user_agent
                .as_deref()
                .is_none_or(|value| string(value) && value.is_ascii()),
            "invalid user agent",
        )?;
        require(
            self.max_attempts > 0,
            "max_attempts must include the initial request and be a positive u32 integer",
        )?;
        seconds(self.connect_seconds)?;
        seconds(self.read_seconds)?;
        seconds(self.discovery_seconds)?;
        require(
            self.client_key_pem.is_none() || self.client_cert_pem.is_some(),
            "client key requires client certificate",
        )?;
        Ok(())
    }

    fn routing_scope(&self) -> Result<RoutingScope, String> {
        require(
            !self.scopes.is_empty(),
            "scopes requires at least one built-in scope",
        )?;
        let mut scope: Option<RoutingScope> = None;
        for (kind, dc, rack) in self.scopes.iter().rev() {
            let next = match (kind.as_str(), dc.as_deref(), rack.as_deref()) {
                ("cluster", None, None) => RoutingScope::from_cluster(),
                ("datacenter", Some(dc), None) if string(dc) => {
                    RoutingScope::from_datacenter(dc.to_owned())
                }
                ("rack", Some(dc), Some(rack)) if string(dc) && string(rack) => {
                    RoutingScope::from_rack(dc.to_owned(), rack.to_owned())
                }
                _ => return Err("invalid scope tuple".to_owned()),
            };
            scope = Some(match scope {
                Some(fallback) => next.with_fallback(fallback),
                None => next,
            });
        }
        Ok(scope.unwrap())
    }

    pub fn tls_config(&self) -> Result<rustls::ClientConfig, String> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        if self.system_roots {
            let certificates = rustls_native_certs::load_native_certs();
            for certificate in certificates.certs {
                roots
                    .add(certificate)
                    .map_err(|error| format!("invalid system CA: {error}"))?;
            }
        }
        for pem in &self.ca_pem {
            let certificates = rustls_pemfile::certs(&mut Cursor::new(pem))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("invalid CA PEM: {error}"))?;
            require(
                !certificates.is_empty(),
                "custom CA PEM contains no certificates",
            )?;
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|error| format!("invalid CA: {error}"))?;
            }
        }
        require(
            !roots.is_empty(),
            "verified TLS requires at least one usable CA root",
        )?;
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())?
            .with_root_certificates(roots);
        match &self.client_cert_pem {
            None => Ok(builder.with_no_client_auth()),
            Some(pem) => {
                let certificates = rustls_pemfile::certs(&mut Cursor::new(pem))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| format!("invalid client certificate PEM: {error}"))?;
                require(
                    !certificates.is_empty(),
                    "client certificate PEM contains no certificates",
                )?;
                let key = rustls_pemfile::private_key(&mut Cursor::new(
                    self.client_key_pem.as_ref().unwrap_or(pem),
                ))
                .map_err(|error| format!("invalid client key PEM: {error}"))?
                .ok_or("client certificate requires a PEM private key")?;
                builder
                    .with_client_auth_cert(certificates, key)
                    .map_err(|error| format!("invalid client certificate/key: {error}"))
            }
        }
    }

    pub fn upstream(&self, transport: Transport) -> Result<AlternatorConfig, String> {
        self.validate()?;
        let affinity_type = match self.key_affinity.as_str() {
            "RMW" => KeyRouteAffinityType::Rmw,
            "ANY_WRITE" => KeyRouteAffinityType::AnyWrite,
            _ => KeyRouteAffinityType::None,
        };
        let mut affinity = KeyRouteAffinityConfig::builder().with_type(affinity_type);
        for (table, key) in &self.table_pk_attributes {
            affinity = affinity.with_pk_info(table, key);
        }
        let request_compression = if self.request_gzip {
            RequestCompression::enabled(
                CompressionAlgorithm::Gzip,
                CompressionLevel::new(self.gzip_level),
                self.compression_min_bytes,
            )
        } else {
            RequestCompression::disabled()
        };
        let response_compression = if self.response_compression.is_empty() {
            ResponseCompression::disabled()
        } else {
            ResponseCompression::enabled_many(self.response_compression.iter().map(|value| {
                match value.as_str() {
                    "gzip" => ResponseCompressionAlgorithm::Gzip,
                    _ => ResponseCompressionAlgorithm::Deflate,
                }
            }))
        };
        let mut builder = AlternatorConfig::builder()
            .seed_hosts(self.seed_hosts.clone())
            .port(self.port)
            .scheme(self.scheme.clone())
            .routing_scope(self.routing_scope()?)
            .active_interval(Duration::from_millis(self.active_interval_ms))
            .idle_interval(Duration::from_millis(self.idle_interval_ms))
            .discovery_timeout(seconds(self.discovery_seconds)?)
            .key_route_affinity(affinity.build())
            .request_compression(request_compression)
            .response_compression(response_compression)
            .optimize_headers(self.optimize_headers)
            .region(Region::new(self.region.clone()))
            .retry_config(RetryConfig::standard().with_max_attempts(self.max_attempts))
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(seconds(self.connect_seconds)?)
                    .read_timeout(seconds(self.read_seconds)?)
                    .operation_timeout(seconds(self.read_seconds)?)
                    .operation_attempt_timeout(seconds(self.read_seconds)?)
                    .build(),
            )
            .http_client(transport)
            .interceptor(crate::metadata::AttemptInterceptor);
        builder = match &self.user_agent {
            Some(value) => builder.user_agent(value.clone()),
            None => builder.without_user_agent(),
        };
        builder = match (&self.access_key_id, &self.secret_access_key) {
            (Some(key), Some(secret)) => {
                builder
                    .require_auth()
                    .credentials_provider(Credentials::new(
                        key.clone(),
                        secret.clone(),
                        self.session_token.clone(),
                        None,
                        "Alternator explicit static credentials",
                    ))
            }
            _ => builder.allow_no_auth(),
        };
        Ok(builder.build())
    }
}

use std::sync::Arc;
