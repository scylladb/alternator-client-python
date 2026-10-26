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
    registry::{Closed, OperationRegistry},
    transport::Transport,
};

struct Inner {
    sdk: AlternatorClient,
    transport: Transport,
    registry: Arc<OperationRegistry>,
    shutdown_started: AtomicBool,
    shutdown_complete: AtomicBool,
    shutdown_complete_notify: Notify,
}

#[derive(Clone)]
pub struct Client(Arc<Inner>);

impl Client {
    /// Must run on the shared runtime. No tasks or network start until all
    /// schema, certificate and upstream configuration validation has passed.
    pub async fn create(config: NativeConfig) -> Result<Self, String> {
        config.validate()?;
        let transport = Transport::new(config.tls_config()?);
        let driver_config = config.to_driver_config(transport.clone())?;
        let sdk = match AlternatorClient::try_from_conf(driver_config) {
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
            shutdown_started: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            shutdown_complete_notify: Notify::new(),
        })))
    }

    /// P5/P6/P7 run every complete SDK operation (including retry sleeps) here.
    /// Request/attempt metadata is isolated from concurrent operations/discovery.
    pub async fn run<'a, F, Fut>(
        &'a self,
        operation: F,
    ) -> Result<(Fut::Output, OperationMetadata), Closed>
    where
        F: FnOnce(&'a AlternatorClient) -> Fut,
        Fut: Future,
    {
        self.0
            .registry
            .run(async { metadata::capture(operation(&self.0.sdk)).await })
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
        if !self.0.shutdown_started.swap(true, Ordering::AcqRel) {
            let client = self.clone();
            // This task outlives cancelled close waiters and never shuts down
            // the process-wide runtime or another client's transports.
            crate::runtime::shared_runtime()
                .expect("client construction initialized the runtime")
                .spawn(async move {
                    client.0.sdk.shutdown().await;
                    client.0.registry.wait_until_drained().await;
                    client.0.transport.finish_close().await;
                    client.0.shutdown_complete.store(true, Ordering::Release);
                    client.0.shutdown_complete_notify.notify_waiters();
                });
        }
    }

    pub async fn close(&self) {
        self.start_close();
        loop {
            let notified = self.0.shutdown_complete_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.0.shutdown_complete.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) == 1 && !self.0.shutdown_started.load(Ordering::Acquire) {
            self.start_close();
        }
    }
}
