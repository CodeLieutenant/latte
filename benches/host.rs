//! Benchmarks of the Rust host: its cost per op with a null client, binding values into the
//! driver's `RowWriter`, and mapping driver errors to the core's error list.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use latte_cli::host::{run_worker, OpRunner};
use latte_core::clock;
use latte_core::exec::run::{start, Start};
use latte_core::native::Json;
use latte_core::ops::{Completion, Op, OpKind, Response, Rows};
use latte_core::scylla_cql_core::frame::response::result::{
    ColumnSpec, ColumnType, NativeType, TableSpec,
};

/// A client that answers every op at once.
struct NullRunner;

impl OpRunner for NullRunner {
    fn run(&self, op: Op) -> impl Future<Output = Completion> {
        let now = clock::now_ns();
        let response = match op.kind {
            OpKind::Prepare { .. } => Response::Prepared {
                bind_specs: vec![ColumnSpec::owned(
                    "id".to_string(),
                    ColumnType::Native(NativeType::BigInt),
                    TableSpec::owned("latte".to_string(), "basic".to_string()),
                )],
            },
            OpKind::Alternator { .. } => Response::Json(Json::Text(Default::default())),
            OpKind::Datacenters => Response::Datacenters(Vec::new()),
            _ => Response::Page {
                rows: Rows::default(),
                paging_state: None,
            },
        };
        std::future::ready(Completion {
            id: op.id,
            start_ns: now,
            end_ns: now,
            result: Ok(response),
        })
    }
}

#[cfg(feature = "cql")]
fn args(cycles: u64) -> Vec<String> {
    let w = concat!(env!("CARGO_MANIFEST_DIR"), "/workloads/basic/write.rn");
    ["latte", "run", w, "127.0.0.1:1", "-w", "0", "-q", "-d"]
        .iter()
        .map(|s| s.to_string())
        .chain([cycles.to_string()])
        .collect()
}

#[cfg(feature = "alternator")]
fn args(cycles: u64) -> Vec<String> {
    let w = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/workloads/alternator/performance.rn"
    );
    [
        "latte-alternator",
        "run",
        w,
        "http://127.0.0.1:1",
        "-f",
        "get:1.0",
        "-w",
        "0",
        "-q",
        "-d",
    ]
    .iter()
    .map(|s| s.to_string())
    .chain([cycles.to_string()])
    .collect()
}

fn bench_host_per_op(c: &mut Criterion) {
    let mut group = c.benchmark_group("host_null");
    group.throughput(Throughput::Elements(1));
    group.sample_size(10);
    group.bench_function("per_op", |b| {
        b.iter_custom(|iters| {
            let Start::Driver(mut run) = start(args(iters.max(1))) else {
                panic!("run did not start");
            };
            let runner = Arc::new(NullRunner);
            let mut elapsed = std::time::Duration::ZERO;
            while let Some(phase) = run.begin_phase() {
                let instance = run.take_instance(0).unwrap();
                let started = Instant::now();
                run_worker(Arc::clone(&runner), instance, run.cancel_token());
                if phase.name == "run" {
                    elapsed = started.elapsed();
                }
                run.end_phase();
            }
            run.finish();
            elapsed
        })
    });
    group.finish();
}

#[cfg(feature = "cql")]
fn bench_bind_and_errors(c: &mut Criterion) {
    use latte_cli::cql::driver::driver_error;
    use latte_core::native::Values;
    use latte_core::scylla_cql_core::serialize::row::{
        RowSerializationContext, SerializeRow, SerializedValues,
    };
    use latte_core::scylla_cql_core::serialize::writers::RowWriter;
    use scylla::errors::ExecutionError;

    let specs = vec![ColumnSpec::owned(
        "id".to_string(),
        ColumnType::Native(NativeType::BigInt),
        TableSpec::owned(String::new(), String::new()),
    )];
    let ctx = RowSerializationContext::from_specs(&specs);
    let mut serialized = SerializedValues::new();
    serialized
        .add_value(&42i64, &ColumnType::Native(NativeType::BigInt))
        .unwrap();
    let values = Values::Serialized(serialized);
    let mut buf = Vec::with_capacity(64);
    c.bench_function("bind_raw_bytes_into_row_writer", |b| {
        b.iter(|| {
            buf.clear();
            let mut writer = RowWriter::new(&mut buf);
            values.serialize(&ctx, &mut writer).unwrap();
        })
    });
    c.bench_function("scylla_error_to_code", |b| {
        b.iter(|| {
            driver_error(ExecutionError::RequestTimeout(
                std::time::Duration::from_secs(1),
            ))
        })
    });
}

#[cfg(feature = "alternator")]
fn bench_bind_and_errors(c: &mut Criterion) {
    use latte_cli::alternator::ops::{item_from_json, item_to_json};
    let json = serde_json::json!({
        "pk": {"S": "key"}, "a": {"N": "1"}, "b": {"S": "x".repeat(64)}, "c": {"BOOL": true}
    });
    c.bench_function("json_to_sdk_item", |b| {
        b.iter(|| item_from_json(&json).unwrap())
    });
    let item = item_from_json(&json).unwrap();
    c.bench_function("sdk_item_to_json", |b| b.iter(|| item_to_json(&item)));
}

criterion_group!(benches, bench_host_per_op, bench_bind_and_errors);
criterion_main!(benches);
