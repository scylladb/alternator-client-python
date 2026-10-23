// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One process-wide runtime. Clients own their tasks and pools, never this runtime.

use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();

/// Lazy, fallible initialization; a failed initialization remains a stable error.
pub fn shared_runtime() -> Result<&'static Runtime, &'static str> {
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .enable_all()
                .thread_name("alternator-native")
                .build()
                .map_err(|error| format!("cannot start Alternator Tokio runtime: {error}"))
        })
        .as_ref()
        .map_err(String::as_str)
}

pub fn initialized() -> bool {
    RUNTIME.get().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_callers_share_one_runtime() {
        let threads: Vec<_> = (0..8)
            .map(|_| std::thread::spawn(|| shared_runtime().unwrap() as *const Runtime as usize))
            .collect();
        let ids: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(ids.iter().all(|id| *id == ids[0]));
    }

    #[test]
    fn dropping_a_clients_task_does_not_stop_shared_runtime() {
        let runtime = shared_runtime().unwrap();
        let task = runtime.spawn(std::future::pending::<()>());
        task.abort();
        runtime.block_on(async {
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(tokio::spawn(async { 42 }).await.unwrap(), 42);
        });
    }
}
