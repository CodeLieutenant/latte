//! The per-op path of the core allocates nothing after warm-up: op submit, release, the
//! C ABI record write and read, completion and wake-up. Script execution (Rune) and row
//! decoding into script values are outside this path and are not measured here.

use std::alloc::Layout;
use std::rc::Rc;

use latte_core::exec::runtime::{BoxTask, CancelToken, Reactor, TaskSet};
use latte_core::ffi::{decode_completion, encode_op, LatteRecord, LatteRing};
use latte_core::native::Json;
use latte_core::ops::{AlternatorOp, Completion, OpKind, Response, RowMode, Rows, Values};
use latte_core::scylla_cql_core::serialize::row::SerializedValues;

/// Counts the allocations `f` makes on this thread, through the core's own allocator.
fn allocations(f: impl FnOnce()) -> u64 {
    latte_core::allocator::count_allocations(f)
}

fn ring() -> Box<LatteRing> {
    let layout = Layout::new::<LatteRing>();
    unsafe { Box::from_raw(std::alloc::alloc_zeroed(layout) as *mut LatteRing) }
}

/// Returns a runner of op round trips through the reactor and the C ABI records, with one
/// script task that submits `kind()` in a loop and a host that answers with `response()`.
fn round_trips(kind: fn() -> OpKind, response: fn() -> Response) -> impl FnMut(usize) {
    let reactor = Reactor::new(5_000_000_000);
    let mut tasks: TaskSet<()> = TaskSet::default();
    let task_reactor = Rc::clone(&reactor);
    let task: BoxTask<()> = Box::pin(async move {
        loop {
            let sent = task_reactor.submit(kind()).await;
            std::hint::black_box(sent);
        }
    });
    tasks.spawn(task);
    let cancel = CancelToken::new();
    let mut out = Vec::with_capacity(16);
    let mut op_ring = ring();
    let completion_arena = vec![0u8; 16];
    move |n: usize| {
        for i in 0..n {
            reactor.set_now(1 + i as u64);
            reactor.expire_deadlines();
            tasks.poll_ready(&reactor, &cancel, |_| {});
            reactor.drain_ops(&mut out, usize::MAX);
            op_ring.len = 0;
            op_ring.arena_len = 0;
            for op in out.drain(..) {
                encode_op(&mut op_ring, &op).expect("op fits the ring");
                let rec = LatteRecord {
                    id: op.id,
                    ..LatteRecord::default()
                };
                let decoded = decode_completion(&rec, &completion_arena).unwrap();
                std::hint::black_box(decoded);
                reactor.complete(Completion {
                    id: op.id,
                    start_ns: 1,
                    end_ns: 2,
                    result: Ok(response()),
                });
            }
        }
    }
}

fn assert_no_allocations(kind: fn() -> OpKind, response: fn() -> Response) {
    let mut run = round_trips(kind, response);
    run(10_000);
    let n = allocations(|| run(100_000));
    assert_eq!(n, 0, "{n} allocations in 100000 op round trips");
}

fn page(rows: u64) -> Response {
    Response::Page {
        rows: Rows {
            row_count: rows,
            ..Rows::default()
        },
        paging_state: None,
    }
}

#[test]
fn cql_write_ops_allocate_nothing() {
    assert_no_allocations(
        || OpKind::Execute {
            stmt: 0,
            values: Values::Serialized(SerializedValues::new()),
            paging_state: None,
            rows: RowMode::None,
        },
        || page(0),
    );
}

#[test]
fn cql_count_ops_allocate_nothing() {
    assert_no_allocations(
        || OpKind::Execute {
            stmt: 0,
            values: Values::Serialized(SerializedValues::new()),
            paging_state: None,
            rows: RowMode::Count,
        },
        || page(17),
    );
}

#[test]
fn alternator_ops_allocate_nothing() {
    assert_no_allocations(
        || OpKind::Alternator {
            op: AlternatorOp::PutItem,
            body: Json::Text(String::new()),
        },
        || Response::Json(Json::Text(String::new())),
    );
}
