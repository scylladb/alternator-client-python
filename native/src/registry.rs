// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cancellation and admission shared by every operation of one client.
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Default)]
struct State {
    closed: bool,
    active: usize,
}

#[derive(Debug, Default)]
pub struct RequestRegistry {
    state: Mutex<State>,
    drained: Notify,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

impl std::fmt::Display for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Alternator native client is closed")
    }
}

impl std::error::Error for Closed {}

struct Registration(Arc<RequestRegistry>);

impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.active -= 1;
        if state.active == 0 {
            self.0.drained.notify_waiters();
        }
    }
}

impl RequestRegistry {
    pub fn begin_close(&self) {
        self.state.lock().unwrap().closed = true;
        self.cancellation.cancel();
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    pub async fn run<F: Future>(self: &Arc<Self>, future: F) -> Result<F::Output, Closed> {
        let registration = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err(Closed);
            }
            state.active += 1;
            Registration(self.clone())
        };
        // A dropped Python awaitable drops this future, its SDK future and guard.
        let result = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(Closed),
            result = future => Ok(result),
        };
        drop(registration);
        result
    }

    pub async fn drained(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().active == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn bounded<F: Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("registry transition timed out")
    }

    #[tokio::test]
    async fn close_cancels_registered_work_and_rejects_new_work() {
        let registry = Arc::new(RequestRegistry::default());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .run(async move {
                        entered_tx.send(()).unwrap();
                        std::future::pending::<()>().await;
                    })
                    .await
            }
        });
        bounded(entered_rx).await.unwrap();
        registry.begin_close();
        registry.begin_close();
        assert_eq!(bounded(worker).await.unwrap(), Err(Closed));
        bounded(registry.drained()).await;
        assert_eq!(registry.run(async { 7 }).await, Err(Closed));
    }

    #[tokio::test]
    async fn dropping_a_waiter_unregisters_without_closing_the_client() {
        let registry = Arc::new(RequestRegistry::default());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn({
            let registry = registry.clone();
            async move {
                registry
                    .run(async {
                        entered_tx.send(()).unwrap();
                        std::future::pending::<()>().await;
                    })
                    .await
            }
        });
        bounded(entered_rx).await.unwrap();
        worker.abort();
        let _ = bounded(worker).await;
        bounded(registry.drained()).await;
        assert_eq!(registry.run(async { 7 }).await, Ok(7));
    }
}
