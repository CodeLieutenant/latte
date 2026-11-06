//! The pump loop of the Rust host, generic over the client that runs the ops.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use latte_core::clock;
use latte_core::exec::instance::{Instance, PumpStatus};
use latte_core::exec::runtime::CancelToken;
use latte_core::ops::{ClientKind, Completion, DriverError, Op, OpId};
use tokio::runtime::Builder;
use tokio::task::LocalSet;

/// A client that runs the ops of the core.
pub trait OpRunner: Send + Sync + 'static {
    fn run(&self, op: Op) -> impl Future<Output = Completion>;
}

/// Runs one instance to completion on the calling thread.
pub fn run_worker<D: OpRunner>(driver: Arc<D>, instance: Instance, token: CancelToken) {
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("error: failed to start a worker runtime: {e}");
            return;
        }
    };
    let local = LocalSet::new();
    local.block_on(&runtime, pump(driver, instance, token));
}

fn cancelled(id: OpId) -> Completion {
    Completion {
        id,
        start_ns: 0,
        end_ns: 0,
        result: Err(DriverError::new(
            0,
            ClientKind::Other,
            "Cancelled",
            "the host dropped the call after the run was cancelled",
        )),
    }
}

/// Pumps one instance until it is done or cancelled. Ops run concurrently on the driver;
/// the loop wakes on the first completion or on the deadline the core asked for. After a
/// cancel the loop drops its driver calls and reports them as cancelled.
pub async fn pump<D: OpRunner>(driver: Arc<D>, mut instance: Instance, token: CancelToken) {
    let mut ops = Vec::new();
    let mut completions = Vec::new();
    let mut in_flight = FuturesUnordered::new();
    let mut ids: HashSet<OpId> = HashSet::new();
    loop {
        if token.is_cancelled() && !ids.is_empty() {
            in_flight = FuturesUnordered::new();
            completions.extend(ids.drain().map(cancelled));
        }
        let status = instance.pump(clock::now_ns(), completions.drain(..), &mut ops);
        for op in ops.drain(..) {
            let driver = Arc::clone(&driver);
            ids.insert(op.id);
            in_flight.push(async move { driver.run(op).await });
        }
        let deadline = match status {
            PumpStatus::Done | PumpStatus::Cancelled => return,
            PumpStatus::Running { next_deadline_ns }
            | PumpStatus::Draining { next_deadline_ns } => next_deadline_ns,
        };
        // Wake at least every 100 ms, so a cancel never waits for a long deadline.
        let wait = deadline
            .map(|d| Duration::from_nanos(d.saturating_sub(clock::now_ns())))
            .unwrap_or(Duration::from_millis(100))
            .min(Duration::from_millis(100));
        if in_flight.is_empty() {
            tokio::time::sleep(wait).await;
            continue;
        }
        tokio::select! {
            Some(completion) = in_flight.next() => {
                ids.remove(&completion.id);
                completions.push(completion);
                while let Some(Some(completion)) = in_flight.next().now_or_never() {
                    ids.remove(&completion.id);
                    completions.push(completion);
                }
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
}
