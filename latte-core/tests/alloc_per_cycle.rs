//! Allocations per cycle on the full script path in null-driver mode: the Rune call, the
//! context functions, the op and the completion. Prints the allocations of one cycle by size.

use std::path::PathBuf;

use latte_core::allocator::{count_allocations, count_allocations_by_size};
use latte_core::clock;
use latte_core::exec::instance::{Instance, PumpStatus};
use latte_core::exec::run::{start, Run, Start};
use latte_core::native::Json;
use latte_core::ops::{Completion, Op, OpKind, Response, Rows};
use latte_core::scylla_cql_core::frame::response::result::{
    ColumnSpec, ColumnType, NativeType, TableSpec,
};

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
        OpKind::Alternator { .. } => Response::Json(Json::Text(Default::default())),
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

struct Host {
    instance: Instance,
    out: Vec<Op>,
    completions: Vec<Completion>,
}

impl Host {
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

fn main_phase(script: &str, extra: &[&str]) -> (Box<Run>, Host) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../workloads")
        .join(script);
    let args = [
        "latte".to_string(),
        "run".into(),
        path.to_string_lossy().into_owned(),
        "127.0.0.1:1".into(),
        "-d".into(),
        "1000000000".into(),
        "-w".into(),
        "0".into(),
        "-q".into(),
        "-t".into(),
        "1".into(),
    ]
    .into_iter()
    .chain(extra.iter().map(|a| a.to_string()))
    .collect::<Vec<String>>();
    let Start::Driver(mut run) = start(args) else {
        panic!("run did not start");
    };
    loop {
        let phase = run.begin_phase().expect("a main phase");
        let instance = run.take_instance(0).unwrap();
        let mut host = Host {
            instance,
            out: Vec::with_capacity(1024),
            completions: Vec::with_capacity(1024),
        };
        if phase.name == "run" {
            return (run, host);
        }
        host.pump(u64::MAX);
        drop(host);
        assert!(run.end_phase());
    }
}

fn report(script: &str, extra: &[&str]) -> f64 {
    let (_run, mut host) = main_phase(script, extra);
    host.pump(50_000);
    const OPS: u64 = 20_000;
    let n = count_allocations(|| {
        host.pump(OPS);
    });
    let sizes = count_allocations_by_size(|| {
        host.pump(OPS);
    });
    let per_cycle = n as f64 / OPS as f64;
    eprintln!("{script}: {per_cycle:.2} allocations per cycle");
    for (size, count) in sizes {
        let per = count as f64 / OPS as f64;
        if per >= 0.01 {
            eprintln!("  <= {size:>4} bytes: {per:.2} per cycle");
        }
    }
    per_cycle
}

#[cfg(feature = "cql")]
#[test]
fn write_allocations_per_cycle() {
    report("basic/write.rn", &[]);
}

#[cfg(feature = "cql")]
#[test]
fn read_allocations_per_cycle() {
    report("basic/read.rn", &[]);
}

#[cfg(feature = "alternator")]
#[test]
fn alternator_get_allocations_per_cycle() {
    report("alternator/performance.rn", &["-f", "get:1.0"]);
}
