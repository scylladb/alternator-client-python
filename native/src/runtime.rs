// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One process-wide runtime. Clients own their tasks and pools, never this runtime.

use std::io;
use std::panic::{catch_unwind, UnwindSafe};
use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();

/// Lazy, fallible initialization; a failed initialization remains a stable error.
pub fn shared_runtime() -> Result<&'static Runtime, &'static str> {
    initialize_runtime(&RUNTIME, || {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("alternator-native")
            .build()
    })
}

fn initialize_runtime(
    slot: &OnceLock<Result<Runtime, String>>,
    build: impl FnOnce() -> io::Result<Runtime> + UnwindSafe,
) -> Result<&Runtime, &str> {
    slot.get_or_init(|| {
        // Tokio can panic rather than return Err when an OS worker cannot start.
        // Catch only construction, inside OnceLock, so the failure is retained.
        match catch_unwind(build) {
            Ok(result) => {
                result.map_err(|error| format!("cannot start Alternator Tokio runtime: {error}"))
            }
            Err(cause) => {
                let message = cause
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| cause.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic payload");
                Err(format!(
                    "cannot start Alternator Tokio runtime: construction panicked: {message}"
                ))
            }
        }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn ordinary_build_error_is_retained_without_retry() {
        let slot = OnceLock::new();
        let attempts = AtomicUsize::new(0);
        let first = initialize_runtime(&slot, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("injected driver failure"))
        })
        .unwrap_err();
        assert_eq!(
            first,
            "cannot start Alternator Tokio runtime: injected driver failure"
        );
        let second = initialize_runtime(&slot, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Builder::new_current_thread().build()
        })
        .unwrap_err();
        assert!(std::ptr::eq(first, second));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn construction_panic_unwinds_and_is_retained_without_retry() {
        struct PartialConstruction<'a>(&'a AtomicUsize);
        impl Drop for PartialConstruction<'_> {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let slot = OnceLock::new();
        let attempts = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let first = initialize_runtime(&slot, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            let _partial = PartialConstruction(&drops);
            std::panic::panic_any("injected OS worker failure".to_owned());
        })
        .unwrap_err();
        assert_eq!(first, "cannot start Alternator Tokio runtime: construction panicked: injected OS worker failure");
        let second = initialize_runtime(&slot, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Builder::new_current_thread().build()
        })
        .unwrap_err();
        assert!(std::ptr::eq(first, second));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn string_literal_and_non_string_panics_are_retained() {
        for (payload, expected) in [
            (true, "literal failure"),
            (false, "non-string panic payload"),
        ] {
            let slot = OnceLock::new();
            let error = initialize_runtime(&slot, || {
                if payload {
                    panic!("literal failure");
                }
                std::panic::panic_any(42_u8);
            })
            .unwrap_err();
            assert!(error.ends_with(expected));
            assert!(slot.get().unwrap().is_err());
        }
    }

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
