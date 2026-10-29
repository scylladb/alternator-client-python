// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-operation original transport responses, before upstream decompression.
//! P5 scopes each SDK operation with `capture`, and uses the final attempt for
//! ResponseMetadata/error parsing. Discovery runs outside that task-local scope.
use std::future::Future;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct AttemptResponse {
    pub status: u16,
    /// One-based SDK attempt, including previous attempts without a response.
    pub attempt: u32,
    /// Original header octets (including Content-Encoding/Content-Length).
    pub headers: Vec<(String, Vec<u8>)>,
    /// Complete wire response; upstream owns decompression and SDK parsing.
    pub body: Vec<u8>,
}

impl AttemptResponse {
    pub fn request_id(&self) -> Option<&str> {
        self.headers
            .iter()
            .find(|(name, _)| {
                name.eq_ignore_ascii_case("x-amzn-requestid")
                    || name.eq_ignore_ascii_case("x-amz-request-id")
            })
            .and_then(|(_, value)| std::str::from_utf8(value).ok())
    }
}

#[derive(Debug, Default)]
pub struct OperationMetadata {
    /// Actual SDK attempts, including dispatch failures without an HTTP response.
    pub attempts: u32,
    /// Last routed endpoint, including attempts ending without an HTTP response.
    pub endpoint_url: Option<String>,
    pub responses: Vec<AttemptResponse>,
}

impl std::ops::Deref for OperationMetadata {
    type Target = [AttemptResponse];
    fn deref(&self) -> &Self::Target {
        &self.responses
    }
}

pub type MetadataCapture = Arc<Mutex<OperationMetadata>>;

tokio::task_local! {
    static OPERATION_METADATA: MetadataCapture;
}

pub fn current_capture() -> Option<MetadataCapture> {
    OPERATION_METADATA.try_with(Clone::clone).ok()
}

pub async fn capture<F: Future>(future: F) -> (F::Output, OperationMetadata) {
    let metadata = MetadataCapture::default();
    let output = OPERATION_METADATA.scope(metadata.clone(), future).await;
    let metadata = std::mem::take(&mut *metadata.lock().unwrap());
    (output, metadata)
}

/// Carry the SDK's actual attempt count outside headers, which may be stripped
/// by Alternator header optimization. Extensions never change the signed wire.
#[derive(Debug)]
pub struct AttemptInterceptor;

#[derive(Clone, Debug)]
pub struct AttemptNumber(pub u32);

impl aws_smithy_runtime_api::client::interceptors::Intercept for AttemptInterceptor {
    fn name(&self) -> &'static str {
        "NativeAttemptMetadata"
    }

    fn read_before_attempt(
        &self,
        _: &aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextRef<'_>,
        _: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
        cfg: &mut aws_smithy_types::config_bag::ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        let attempt = cfg
            .load::<aws_smithy_runtime_api::client::retries::RequestAttempts>()
            .map_or(1, |attempt| attempt.attempts());
        if let Some(capture) = current_capture() {
            let mut capture = capture.lock().unwrap();
            capture.attempts = capture.attempts.max(attempt);
        }
        Ok(())
    }

    fn modify_before_transmit(
        &self,
        context: &mut aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut<'_>,
        _: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
        cfg: &mut aws_smithy_types::config_bag::ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        let attempt = cfg
            .load::<aws_smithy_runtime_api::client::retries::RequestAttempts>()
            .map_or(1, |attempt| attempt.attempts());
        if let Some(capture) = current_capture() {
            capture.lock().unwrap().endpoint_url = Some(context.request().uri().to_string());
        }
        context.request_mut().add_extension(AttemptNumber(attempt));
        Ok(())
    }
}
