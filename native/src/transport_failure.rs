// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Stable categories survive Smithy's ConnectorError/SDK error source wrappers.
use std::error::Error;
use std::fmt;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    EndpointConnection,
    ConnectionClosed,
    ConnectTimeout,
    ReadTimeout,
    Tls,
}
impl Category {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EndpointConnection => "endpoint_connection",
            Self::ConnectionClosed => "connection_closed",
            Self::ConnectTimeout => "connect_timeout",
            Self::ReadTimeout => "read_timeout",
            Self::Tls => "tls",
        }
    }
}

#[derive(Debug)]
pub struct TransportFailure {
    pub category: Category,
    pub endpoint_url: String,
    source: BoxError,
}
impl TransportFailure {
    pub fn new(category: Category, endpoint_url: impl Into<String>, source: BoxError) -> Self {
        Self {
            category,
            endpoint_url: endpoint_url.into(),
            source,
        }
    }
    pub fn find<'a>(mut error: &'a (dyn Error + 'static)) -> Option<&'a Self> {
        loop {
            if let Some(value) = error.downcast_ref::<Self>() {
                return Some(value);
            }
            error = error.source()?;
        }
    }
}
impl fmt::Display for TransportFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.category.as_str(), self.source)
    }
}
impl Error for TransportFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}
