//! Runs the null-driver pump on N threads, one instance each, and prints the per-thread
//! throughput. Usage: `pump_scaling <threads> <ops per thread> [script] [concurrency]`.

use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Instant;

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
    fn new(instance: Instance) -> Self {
        Host {
            instance,
            out: Vec::with_capacity(256),
            completions: Vec::with_capacity(256),
        }
    }

    /// Pumps until `ops` ops were answered. Unsent completions stay for the next call.
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

fn pump(instance: &mut Instance, ops: u64) -> u64 {
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

fn main_phase(script: &str, threads: usize, concurrency: &str) -> (Box<Run>, Vec<Instance>) {
    let args = vec![
        "latte".to_string(),
        "run".into(),
        script.into(),
        "127.0.0.1:1".into(),
        "-d".into(),
        "1000000000".into(),
        "-w".into(),
        "0".into(),
        "-q".into(),
        "-t".into(),
        threads.to_string(),
        "-p".into(),
        concurrency.into(),
    ];
    let Start::Driver(mut run) = start(args) else {
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
            pump(instance, u64::MAX);
        }
        drop(instances);
        assert!(run.end_phase());
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let threads: usize = args.next().map_or(1, |a| a.parse().unwrap());
    let ops: u64 = args.next().map_or(2_000_000, |a| a.parse().unwrap());
    let script = args.next().unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../workloads/basic/write.rn")
            .to_string_lossy()
            .into_owned()
    });
    let concurrency = args.next().unwrap_or_else(|| "128".into());
    let (run, instances) = main_phase(&script, threads, &concurrency);
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = instances
        .into_iter()
        .map(|instance| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut host = Host::new(instance);
                host.pump(10_000);
                barrier.wait();
                let start = Instant::now();
                let n = host.pump(ops);
                let elapsed = start.elapsed();
                // The process exits right after the report; the instance is not dropped.
                std::mem::forget(host);
                (n, elapsed)
            })
        })
        .collect();
    let mut total = 0.0;
    for h in handles {
        let (n, elapsed) = h.join().unwrap();
        total += n as f64 / elapsed.as_secs_f64();
    }
    println!(
        "threads {threads}: {:.3} M ops/s per thread",
        total / threads as f64 / 1e6
    );
    drop(run);
    std::process::exit(0);
}
