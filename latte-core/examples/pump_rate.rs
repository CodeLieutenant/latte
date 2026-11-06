//! Prints the null-mode pump rate of `basic/write.rn` for the native API.
use latte_core::clock;
use latte_core::exec::instance::PumpStatus;
use latte_core::exec::run::{start, Start};
use latte_core::ops::{Completion, OpKind, Response, Rows};
use latte_core::scylla_cql_core::frame::response::result::{
    ColumnSpec, ColumnType, NativeType, TableSpec,
};
use std::time::Instant;

fn main() {
    let w = concat!(env!("CARGO_MANIFEST_DIR"), "/../workloads/basic/write.rn");
    let cycles: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(200_000);
    let args = [
        "latte",
        "run",
        w,
        "127.0.0.1:1",
        "-w",
        "0",
        "-q",
        "-d",
        &cycles.to_string(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let Start::Driver(mut run) = start(args) else {
        panic!()
    };
    while let Some(phase) = run.begin_phase() {
        let mut inst = run.take_instance(0).unwrap();
        let (mut ops, mut done) = (Vec::new(), Vec::new());
        let (mut n, mut pumps) = (0u64, 0u64);
        let t = Instant::now();
        loop {
            let st = inst.pump(0, done.drain(..), &mut ops);
            pumps += 1;
            for op in ops.drain(..) {
                n += 1;
                let r = match op.kind {
                    OpKind::Prepare { .. } => Response::Prepared {
                        bind_specs: vec![ColumnSpec::owned(
                            "id".into(),
                            ColumnType::Native(NativeType::BigInt),
                            TableSpec::owned(String::new(), String::new()),
                        )],
                    },
                    _ => Response::Page {
                        rows: Rows::default(),
                        paging_state: None,
                    },
                };
                done.push(Completion {
                    id: op.id,
                    start_ns: clock::now_ns(),
                    end_ns: clock::now_ns(),
                    result: Ok(r),
                });
            }
            if matches!(st, PumpStatus::Done | PumpStatus::Cancelled) {
                break;
            }
        }
        eprintln!(
            "phase {} ops {} pumps {} in {:?} = {:.0} ns/op",
            phase.name,
            n,
            pumps,
            t.elapsed(),
            t.elapsed().as_nanos() as f64 / n.max(1) as f64
        );
        drop(inst);
        run.end_phase();
    }
    run.finish();
}
