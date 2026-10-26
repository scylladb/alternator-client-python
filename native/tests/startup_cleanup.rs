// Copyright ScyllaDB, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, OnceLock,
        },
        time::Duration,
    };
    fn fds() -> usize {
        std::fs::read_dir("/proc/self/fd").unwrap().count()
    }
    fn threads() -> usize {
        std::fs::read_dir("/proc/self/task").unwrap().count()
    }
    fn assert_threads_joined() {
        let start = std::time::Instant::now();
        while threads() != 1 && start.elapsed() < Duration::from_millis(100) {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            threads(),
            1,
            "threads remained after bounded kernel reclamation grace"
        );
    }
    fn drive(rt: &tokio::runtime::Runtime) {
        rt.block_on(async {
            let completed = Arc::new(AtomicUsize::new(0));
            let mut tasks = vec![];
            for _ in 0..16 {
                let completed = completed.clone();
                tasks.push(tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    completed.fetch_add(1, Ordering::SeqCst);
                }));
            }
            for _ in 0..8 {
                tasks.push(tokio::spawn(async {
                    tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(2)))
                        .await
                        .unwrap();
                }));
            }
            tasks.push(tokio::spawn(async {
                tokio::task::block_in_place(|| std::thread::sleep(Duration::from_millis(1)));
            }));
            for task in tasks {
                task.await.unwrap();
            }
            assert_eq!(completed.load(Ordering::SeqCst), 16);
            let _listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        });
    }
    pub fn run() {
        let n: usize = std::env::args()
            .nth(1)
            .unwrap_or("4".into())
            .parse()
            .unwrap();
        // Upstream's full Tokio features enable process-wide signal globals.
        // Warm only a local current-thread driver before the resource baseline;
        // no worker is spawned and the first multi-thread build remains faulted.
        drop(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        assert_eq!(threads(), 1, "local driver prewarm must not spawn workers");
        let initial_fds = fds();
        let builds = AtomicUsize::new(0);
        let cache = OnceLock::new();
        let witness = Arc::new(());
        let weak = Arc::downgrade(&witness);
        let create = || {
            builds.fetch_add(1, Ordering::SeqCst);
            let hook_witness = witness.clone();
            catch_unwind(AssertUnwindSafe(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(n)
                    .enable_all()
                    .on_thread_start(move || {
                        std::hint::black_box(&hook_witness);
                    })
                    .build()
            }))
            .map_err(|payload| {
                format!(
                    "cannot start Alternator Tokio runtime: construction panicked: {}",
                    payload
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| payload.downcast_ref::<&str>().copied())
                        .unwrap_or("non-string panic payload")
                )
            })
            .and_then(|result| result.map_err(|error| error.to_string()))
        };
        eprintln!("before build threads={} fds={}", threads(), initial_fds);
        let first = cache.get_or_init(create);
        let second = cache.get_or_init(create);
        assert!(std::ptr::eq(first, second));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        match first {
            Ok(rt) => {
                eprintln!("build succeeded; driving runtime");
                drive(rt);
            }
            Err(error) => {
                eprintln!("cached error: {error}");
                assert!(error.contains(
                    "OS can\'t spawn worker thread: Resource temporarily unavailable (os error 11)"
                ));
                assert_threads_joined();
                assert_eq!(fds(), initial_fds, "FD leak after failed build");
            }
        }
        drop(cache);
        drop(witness);
        assert!(weak.upgrade().is_none(), "runtime-owned callback leaked");
        eprintln!("after cache drop threads={} fds={}", threads(), fds());
        assert_threads_joined();
        assert_eq!(fds(), initial_fds, "FD leak");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .build()
            .unwrap();
        drive(&rt);
        drop(rt);
        assert_threads_joined();
        assert_eq!(fds(), initial_fds);
        eprintln!("success: cached once, callback freed, subsequent runtime timers/TCP/blocking/block_in_place driven, all threads joined, FDs baseline");
    }
}

fn main() {
    #[cfg(target_os = "linux")]
    linux::run();
}
