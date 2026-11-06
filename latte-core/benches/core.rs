//! Benchmarks of the core: the pump in null mode (native API and C ABI), scaling over
//! threads, op records, values, rows, DynamoDB JSON, errors, the clock, Rune helpers and
//! cancel latency.

use std::collections::HashMap;
use std::ffi::{c_char, c_int, CString};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use latte_core::clock;
use latte_core::exec::instance::{Instance, PumpStatus};
use latte_core::exec::run::{start, Run, Start};
use latte_core::exec::workload::{FnRef, Program};
use latte_core::ffi::{self, LatteRecord, LatteRing};
use latte_core::native::Json;
use latte_core::ops::{
    ClientKind, Completion, DriverError, ErrorCategory, Op, OpKind, Response, Rows,
};
use latte_core::scylla_cql_core::frame::response::result::{
    ColumnSpec, ColumnType, NativeType, TableSpec,
};
use rune::Source;

#[cfg(feature = "cql")]
const SCRIPT: &str = "basic/write.rn";
#[cfg(feature = "alternator")]
const SCRIPT: &str = "alternator/performance.rn";

fn workload(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../workloads")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

fn run_args(threads: usize) -> Vec<String> {
    let mut args = vec![
        "latte".to_string(),
        "run".into(),
        workload(SCRIPT),
        "127.0.0.1:1".into(),
        "-d".into(),
        "1000000000".into(),
        "-w".into(),
        "0".into(),
        "-q".into(),
        "-t".into(),
        threads.to_string(),
    ];
    if cfg!(feature = "alternator") {
        args.extend(["-f".into(), "get:1.0".into()]);
    }
    args
}

fn answer(op: Op) -> Completion {
    let now = clock::now_ns();
    let response = match op.kind {
        OpKind::Prepare { .. } => Response::Prepared {
            bind_specs: vec![ColumnSpec::owned(
                "id".to_string(),
                ColumnType::Native(NativeType::BigInt),
                TableSpec::owned("latte".to_string(), "basic".to_string()),
            )],
        },
        OpKind::Alternator { .. } => Response::Json(Json::Text(String::new())),
        OpKind::Datacenters => Response::Datacenters(Vec::new()),
        _ => Response::Page {
            rows: Rows::default(),
            paging_state: None,
        },
    };
    Completion {
        id: op.id,
        start_ns: now,
        end_ns: now,
        result: Ok(response),
    }
}

/// A run in its main phase, with `threads` instances ready to pump.
fn main_phase(threads: usize) -> (Box<Run>, Vec<Instance>) {
    let Start::Driver(mut run) = start(run_args(threads)) else {
        panic!("run did not start");
    };
    loop {
        let phase = run.begin_phase().expect("a main phase");
        let mut instances: Vec<_> = (0..phase.instances)
            .map(|i| run.take_instance(i).unwrap())
            .collect();
        if phase.name == "run" {
            return (run, instances);
        }
        for instance in instances.iter_mut() {
            null_pump(instance, u64::MAX);
        }
        drop(instances);
        assert!(run.end_phase());
    }
}

/// An instance and the completions a null driver owes it, kept between bench iterations.
struct NullHost {
    instance: Instance,
    out: Vec<Op>,
    completions: Vec<Completion>,
}

impl NullHost {
    fn new(instance: Instance) -> Self {
        NullHost {
            instance,
            out: Vec::with_capacity(256),
            completions: Vec::with_capacity(256),
        }
    }

    /// Pumps until `ops` ops were answered or the instance stops.
    fn pump(&mut self, ops: u64) -> u64 {
        let mut answered = 0;
        while answered < ops {
            let status = self
                .instance
                .pump(0, self.completions.drain(..), &mut self.out);
            for op in self.out.drain(..) {
                self.completions.push(answer(op));
                answered += 1;
            }
            if matches!(status, PumpStatus::Done | PumpStatus::Cancelled) {
                break;
            }
        }
        answered
    }
}

/// Pumps with a null driver until `ops` ops were answered or the instance stops.
fn null_pump(instance: &mut Instance, ops: u64) -> u64 {
    let mut out = Vec::with_capacity(256);
    let mut completions = Vec::with_capacity(256);
    let mut answered = 0;
    while answered < ops {
        let status = instance.pump(0, completions.drain(..), &mut out);
        for op in out.drain(..) {
            completions.push(answer(op));
            answered += 1;
        }
        if matches!(status, PumpStatus::Done | PumpStatus::Cancelled) {
            break;
        }
    }
    answered
}

fn bench_pump(c: &mut Criterion) {
    let mut group = c.benchmark_group("pump_null");
    group.throughput(Throughput::Elements(1));
    let (run, instances) = main_phase(1);
    let mut host = NullHost::new(instances.into_iter().next().unwrap());
    group.bench_function("native_api", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            host.pump(iters);
            start.elapsed()
        })
    });
    drop(host);
    drop(run);

    // The C ABI: rings, records, the in-use flag and the inbox.
    let args: Vec<CString> = run_args(1)
        .into_iter()
        .map(|a| CString::new(a).unwrap())
        .collect();
    let argv: Vec<*const c_char> = args.iter().map(|a| a.as_ptr()).collect();
    unsafe {
        let mut code = 0;
        let run = ffi::latte_main(argv.len() as c_int, argv.as_ptr(), &mut code);
        let mut inst = std::ptr::null_mut();
        while ffi::latte_begin_phase(run) > 0 {
            inst = ffi::latte_take_instance(run, 0);
            // The cluster-info and prepare phases end on their own; stop at the main phase.
            let mut done = false;
            for _ in 0..100 {
                let rc = abi_pump(inst, 64);
                if rc == ffi::LATTE_DONE {
                    done = true;
                    break;
                }
            }
            if !done {
                break;
            }
            ffi::latte_instance_free(inst);
            ffi::latte_end_phase(run);
        }
        group.bench_function("c_abi", |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                let mut answered = 0;
                while answered < iters {
                    answered += abi_round(inst);
                }
                start.elapsed()
            })
        });
        ffi::latte_cancel(run);
        while abi_pump(inst, 1) != ffi::LATTE_CANCELLED {}
        ffi::latte_instance_free(inst);
        ffi::latte_end_phase(run);
        ffi::latte_finish(run);
    }
    group.finish();
}

/// One pump call through the C ABI, answering every op it hands out. Returns the ops.
unsafe fn abi_round(inst: *mut ffi::LatteInstance) -> u64 {
    let rc = ffi::latte_pump(inst, 0, std::ptr::null_mut());
    assert!(rc >= 0);
    let ops = &*ffi::latte_ops(inst);
    let done = &mut *ffi::latte_completions(inst);
    for rec in &ops.records[..ops.len as usize] {
        let reply = &mut done.records[done.len as usize];
        *reply = LatteRecord {
            id: rec.id,
            kind: ffi::LATTE_STATUS_OK,
            flags: if rec.kind == ffi::LATTE_OP_PREPARE {
                ffi::LATTE_RESP_PREPARED
            } else if rec.kind >= ffi::LATTE_OP_DDB_CREATE_TABLE {
                ffi::LATTE_RESP_JSON
            } else {
                ffi::LATTE_RESP_PAGE
            },
            ..LatteRecord::default()
        };
        if rec.kind == ffi::LATTE_OP_PREPARE {
            // One bigint bind marker named "id".
            let spec = [0u8, 1, 0, 2, b'i', b'd', 0, 2];
            let off = done.arena_len as usize;
            done.arena[off..off + spec.len()].copy_from_slice(&spec);
            reply.data_off = off as u32;
            reply.data_len = spec.len() as u32;
            done.arena_len += spec.len() as u32;
        }
        done.len += 1;
    }
    ops.len as u64
}

unsafe fn abi_pump(inst: *mut ffi::LatteInstance, rounds: usize) -> c_int {
    let mut rc = ffi::LATTE_OK;
    for _ in 0..rounds {
        abi_round(inst);
        rc = ffi::latte_pump(inst, 0, std::ptr::null_mut());
        if rc == ffi::LATTE_DONE || rc == ffi::LATTE_CANCELLED {
            break;
        }
    }
    rc
}

/// One instance per thread in null mode. The worker threads live for the whole benchmark of
/// one thread count, so a sample measures pumping only, not thread start.
fn bench_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling");
    group.sample_size(10);
    let max = std::thread::available_parallelism().map_or(4, |n| n.get());
    for threads in [1usize, 2, 4, 8].into_iter().filter(|t| *t <= max) {
        let (run, instances) = main_phase(threads);
        let mut hosts: Vec<_> = instances.into_iter().map(NullHost::new).collect();
        let start = std::sync::Barrier::new(threads + 1);
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|s| {
            let mut work = Vec::new();
            for host in hosts.iter_mut() {
                let (tx, rx) = std::sync::mpsc::channel::<u64>();
                work.push(tx);
                let addr = host as *mut NullHost as usize;
                let start = &start;
                let done_tx = done_tx.clone();
                s.spawn(move || {
                    // Each thread owns its instance for the duration of the scope.
                    let host = unsafe { &mut *(addr as *mut NullHost) };
                    while let Ok(iters) = rx.recv() {
                        if iters == 0 {
                            break;
                        }
                        start.wait();
                        host.pump(iters);
                        done_tx.send(()).unwrap();
                    }
                });
            }
            group.throughput(Throughput::Elements(threads as u64));
            group.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, _| {
                b.iter_custom(|iters| {
                    for tx in &work {
                        tx.send(iters.max(1)).unwrap();
                    }
                    start.wait();
                    let begin = Instant::now();
                    for _ in 0..threads {
                        done_rx.recv().unwrap();
                    }
                    begin.elapsed()
                })
            });
            for tx in &work {
                tx.send(0).unwrap();
            }
        });
        drop(hosts);
        drop(run);
    }
    group.finish();
}

fn bench_records(c: &mut Criterion) {
    let mut group = c.benchmark_group("records");
    let layout = std::alloc::Layout::new::<LatteRing>();
    let mut ring = unsafe { Box::from_raw(std::alloc::alloc_zeroed(layout) as *mut LatteRing) };
    let op = Op {
        id: 1,
        send_at_ns: 1,
        deadline_ns: 2,
        kind: OpKind::Query {
            cql: "SELECT * FROM t WHERE pk = 1".to_string(),
            page_size: 5000,
            paging_state: None,
            rows: latte_core::ops::RowMode::None,
        },
    };
    group.bench_function("op_record_write", |b| {
        b.iter(|| {
            ring.len = 0;
            ring.arena_len = 0;
            ffi::encode_op(&mut ring, std::hint::black_box(&op)).unwrap();
        })
    });
    let rec = LatteRecord {
        id: 1,
        kind: ffi::LATTE_STATUS_OK,
        flags: ffi::LATTE_RESP_PAGE,
        rows: 3,
        ..LatteRecord::default()
    };
    let arena = [0u8; 8];
    group.bench_function("completion_read", |b| {
        b.iter(|| ffi::decode_completion(std::hint::black_box(&rec), &arena).unwrap())
    });
    group.finish();
}

fn bench_errors_and_clock(c: &mut Criterion) {
    c.bench_function("error_mapping", |b| {
        b.iter(|| ErrorCategory::of(std::hint::black_box(0x1200), ClientKind::None, None))
    });
    c.bench_function("driver_error_build", |b| {
        b.iter(|| DriverError::new(0x1001, ClientKind::None, "Overloaded", "busy"))
    });
    c.bench_function("clock_read", |b| b.iter(clock::now_ns));
}

fn bench_rune_helpers(c: &mut Criterion) {
    let src = Source::memory(
        "pub async fn e(i) { i }\n\
         pub async fn h(i) { latte::hash(i) }\n\
         pub async fn t(i) { latte::text(i, 100) }\n\
         pub async fn t1k(i) { latte::text(i, 1000) }\n\
         pub async fn b(i) { latte::blob(i, 100) }\n\
         pub async fn b1k(i) { latte::blob(i, 1000) }\n\
         pub async fn n(i) { latte::normal(i, 10.0, 2.0) }\n",
    )
    .unwrap();
    let program = Program::new(src, HashMap::new()).unwrap();
    let mut group = c.benchmark_group("rune_helpers");
    for name in ["e", "h", "t", "t1k", "b", "b1k", "n"] {
        let f = FnRef::new(name);
        group.bench_function(name, |b| {
            let mut i = 0i64;
            b.iter(|| {
                i += 1;
                futures::executor::block_on(program.async_call(&f, (i,))).unwrap()
            })
        });
    }
    group.finish();
}

fn bench_cancel(c: &mut Criterion) {
    c.bench_function("cancel_latency", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (run, instances) = main_phase(1);
                let mut host = NullHost::new(instances.into_iter().next().unwrap());
                host.pump(1000);
                let token = run.cancel_token();
                let start = Instant::now();
                token.cancel();
                host.pump(u64::MAX);
                total += start.elapsed();
            }
            total
        })
    });
}

#[cfg(feature = "cql")]
fn bench_values_and_rows(c: &mut Criterion) {
    use latte_core::scripting::bench;
    let mut group = c.benchmark_group("cql_values");
    let cases: Vec<(&str, NativeType, rune::Value)> = vec![
        ("bigint", NativeType::BigInt, rune::Value::from(42i64)),
        ("double", NativeType::Double, rune::Value::from(1.5f64)),
        ("boolean", NativeType::Boolean, rune::Value::from(true)),
        (
            "text",
            NativeType::Text,
            rune::to_value("x".repeat(100)).unwrap(),
        ),
    ];
    for (name, typ, value) in &cases {
        let specs = vec![ColumnSpec::owned(
            "c".to_string(),
            ColumnType::Native(typ.clone()),
            TableSpec::owned(String::new(), String::new()),
        )];
        let params = rune::to_value(vec![value.clone()]).unwrap();
        group.bench_function(BenchmarkId::new("serialize", name), |b| {
            b.iter(|| bench::serialize(&params, &specs))
        });
        let serialized = bench::serialize(&params, &specs);
        let mut data = Vec::new();
        for v in serialized.iter() {
            if let latte_core::scylla_cql_core::frame::types::RawValue::Value(bytes) = v {
                latte_core::ops::push_cell(&mut data, Some(bytes));
            }
        }
        let rows = Rows {
            row_count: 1,
            specs: specs.clone(),
            data,
            objects: None,
        };
        group.bench_function(BenchmarkId::new("decode_row", name), |b| {
            b.iter(|| bench::decode_rows(&rows))
        });
    }
    group.finish();
}

#[cfg(feature = "alternator")]
fn bench_values_and_rows(c: &mut Criterion) {
    use latte_core::scripting::bench;
    let mut group = c.benchmark_group("dynamodb_json");
    let item = bench::item(10, 64);
    group.bench_function("put_item_build", |b| b.iter(|| bench::put_item_json(&item)));
    let response = bench::query_response(&item, 10);
    group.bench_function("query_parse_10_items", |b| {
        b.iter(|| bench::parse_query_response(&response))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_pump,
    bench_scaling,
    bench_records,
    bench_errors_and_clock,
    bench_values_and_rows,
    bench_rune_helpers,
    bench_cancel
);
criterion_main!(benches);
