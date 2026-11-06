//! The Rust host of the Latte core: it owns the database client and the event loops, runs
//! the ops the core hands out and feeds the completions back. Everything else is in
//! `latte-core`.
//!
//! Each worker thread runs one core instance on its own current-thread Tokio runtime, so a
//! completion lands on the thread that owns the instance with no cross-thread queue. All
//! workers share one client, whose connection tasks run on a separate driver runtime.
//! The `latte` binary serves CQL and the `latte-alternator` binary serves Alternator.

use std::process::exit;
use std::sync::Arc;

use latte_cli::host::run_worker;
use latte_core::exec::run::{backend, start, thread_hint, Backend, Start};
use latte_core::exec::runtime::CancelToken;
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "alternator")]
use latte_cli::alternator as db;
#[cfg(feature = "cql")]
use latte_cli::cql as db;

/// The backend this host binary serves.
#[cfg(feature = "cql")]
const HOST_BACKEND: Backend = Backend::Cql;
#[cfg(feature = "alternator")]
const HOST_BACKEND: Backend = Backend::Alternator;

/// The runtime that runs the connection tasks of the shared client.
fn driver_runtime(thread_count: usize) -> std::io::Result<Runtime> {
    Builder::new_multi_thread()
        .worker_threads(thread_count)
        .thread_name("latte-driver")
        .enable_all()
        .enable_alt_timer()
        .build()
}

/// Cancels the run on SIGINT (Ctrl-C) or SIGTERM.
fn cancel_on_signals(runtime: &Runtime, token: CancelToken) {
    runtime.spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            match signal(SignalKind::terminate()) {
                Ok(mut term) => {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = term.recv() => {}
                    }
                }
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        token.cancel();
    });
}

fn main() {
    let mut run = match start(std::env::args().collect()) {
        Start::Exit(code) => exit(code),
        Start::Driver(run) => run,
    };
    if backend() != HOST_BACKEND {
        eprintln!(
            "error: the workload needs the {} backend, and this host serves {}",
            backend().name(),
            HOST_BACKEND.name()
        );
        exit(2);
    }
    let runtime = match driver_runtime(thread_hint(&run).get()) {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("error: {e}");
            exit(128);
        }
    };
    let token = run.cancel_token();
    cancel_on_signals(&runtime, token.clone());
    let driver = match runtime.block_on(db::connect(run.connection(), run.client_id())) {
        Ok(driver) => driver,
        Err(e) => {
            eprintln!("error: {e}");
            exit(128);
        }
    };
    let settings = db::driver_settings(run.connection()).to_string();
    if let Err(e) = run.set_driver_settings(settings.as_bytes()) {
        eprintln!("error: {e}");
        exit(128);
    }
    let driver = Arc::new(driver);
    while let Some(phase) = run.begin_phase() {
        let instances: Vec<_> = (0..phase.instances)
            .filter_map(|i| run.take_instance(i))
            .collect();
        std::thread::scope(|scope| {
            for (i, instance) in instances.into_iter().enumerate() {
                let driver = Arc::clone(&driver);
                let token = token.clone();
                let spawned = std::thread::Builder::new()
                    .name(format!("latte-worker-{i}"))
                    .spawn_scoped(scope, move || run_worker(driver, instance, token));
                if let Err(e) = spawned {
                    eprintln!("error: failed to start worker thread {i}: {e}");
                }
            }
        });
        if !run.end_phase() {
            break;
        }
    }
    let code = run.finish();
    drop(driver);
    drop(runtime);
    exit(code);
}
