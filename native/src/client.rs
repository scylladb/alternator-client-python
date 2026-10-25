// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use alternator_driver::AlternatorClient;
use tokio::sync::Notify;

use crate::{
    config::NativeConfig,
    metadata::{self, OperationMetadata},
    registry::{Closed, RequestRegistry},
    transport::Transport,
};

struct Inner {
    sdk: AlternatorClient,
    transport: Transport,
    registry: Arc<RequestRegistry>,
    closing: AtomicBool,
    closed: AtomicBool,
    closed_notify: Notify,
}

#[derive(Clone)]
pub struct Client(Arc<Inner>);

impl Client {
    /// Must run on the shared runtime. No tasks or network start until all
    /// schema, certificate and upstream configuration validation has passed.
    pub async fn create(config: NativeConfig) -> Result<Self, String> {
        config.validate()?;
        let transport = Transport::new(config.tls_config()?);
        let upstream = config.upstream(transport.clone())?;
        let sdk = match AlternatorClient::try_from_conf(upstream) {
            Ok(sdk) => sdk,
            Err(error) => {
                transport.finish_close().await;
                return Err(error.to_string());
            }
        };
        Ok(Self(Arc::new(Inner {
            sdk,
            transport,
            registry: Arc::default(),
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            closed_notify: Notify::new(),
        })))
    }

    /// P5/P6/P7 run every complete SDK operation (including retry sleeps) here.
    /// Request/attempt metadata is isolated from concurrent operations/discovery.
    pub async fn run<F, Fut>(
        &self,
        operation: F,
    ) -> Result<(Fut::Output, OperationMetadata), Closed>
    where
        F: FnOnce(AlternatorClient) -> Fut,
        Fut: Future,
    {
        self.0
            .registry
            .run(async { metadata::capture(operation(self.0.sdk.clone())).await })
            .await
    }

    pub fn is_closed(&self) -> bool {
        self.0.registry.is_closed()
    }

    pub fn live_drivers(&self) -> usize {
        self.0.transport.live_drivers()
    }

    pub fn start_close(&self) {
        // Admission closes synchronously, before a close awaitable is returned.
        self.0.registry.begin_close();
        self.0.transport.begin_close();
        if !self.0.closing.swap(true, Ordering::AcqRel) {
            let client = self.clone();
            // This task outlives cancelled close waiters and never shuts down
            // the process-wide runtime or another client's transports.
            crate::runtime::shared_runtime()
                .expect("client construction initialized the runtime")
                .spawn(async move {
                    client.0.sdk.shutdown().await;
                    client.0.registry.drained().await;
                    client.0.transport.finish_close().await;
                    client.0.closed.store(true, Ordering::Release);
                    client.0.closed_notify.notify_waiters();
                });
        }
    }

    pub async fn close(&self) {
        self.start_close();
        loop {
            let notified = self.0.closed_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.closed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) == 1 && !self.0.closing.load(Ordering::Acquire) {
            self.start_close();
        }
    }
}
