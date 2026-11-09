//! Runs whole Latte commands through the core with a null driver: a host that answers
//! every op at once, without a database.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use latte_core::clock;
use latte_core::exec::instance::PumpStatus;
use latte_core::exec::run::{start, Start};
use latte_core::exec::runtime::CancelToken;
use latte_core::native::Json;
use latte_core::ops::{Completion, Op, OpKind, Response, RowMode, Rows};
use latte_core::scylla_cql_core::frame::response::result::{
    ColumnSpec, ColumnType, NativeType, TableSpec,
};

fn workload(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../workloads")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

/// How the null host answers.
#[derive(Clone, Copy)]
struct Host {
    /// Rows every page has.
    rows: u64,
    /// Gives ops the C ABI form (CQL bytes, JSON text) instead of the native one.
    abi: bool,
}

const NATIVE: Host = Host {
    rows: 0,
    abi: false,
};

/// One op in a form that does not depend on the API: kind, statement, text and value bytes.
#[derive(Debug, PartialEq, Eq)]
struct OpTrace(u32, u64, String, Vec<u8>);

fn trace(op: &Op) -> OpTrace {
    let mut bytes = Vec::new();
    let (stmt, text) = match &op.kind {
        OpKind::Prepare { stmt, cql, .. } => (*stmt, cql.clone()),
        OpKind::Query { cql, .. } => (0, cql.clone()),
        OpKind::Execute { stmt, values, .. } => {
            values.to_serialized().unwrap().write_to_request(&mut bytes);
            (*stmt, String::new())
        }
        OpKind::Alternator { op, body } => (0, format!("{} {}", op.name(), body.text())),
        _ => (0, String::new()),
    };
    OpTrace(op.kind.code(), stmt, text, bytes)
}

fn answer(op: Op, host: Host) -> Completion {
    let now = clock::now_ns();
    assert!(
        op.send_at_ns <= now,
        "op {} released {} ns before its send time",
        op.id,
        op.send_at_ns - now
    );
    assert!(
        op.deadline_ns > op.send_at_ns,
        "every op carries a deadline"
    );
    let response = match op.kind {
        OpKind::Prepare { .. } => Response::Prepared {
            bind_specs: vec![ColumnSpec::owned(
                "id".to_string(),
                ColumnType::Native(NativeType::BigInt),
                TableSpec::owned("latte".to_string(), "basic".to_string()),
            )],
        },
        OpKind::Execute { rows: mode, .. } | OpKind::Query { rows: mode, .. } => {
            let objects = (mode == RowMode::Rows).then(|| {
                (0..host.rows)
                    .map(|_| rune::Value::new(rune::runtime::Object::new()).unwrap())
                    .collect()
            });
            Response::Page {
                rows: Rows {
                    row_count: host.rows,
                    objects,
                    ..Rows::default()
                },
                paging_state: None,
            }
        }
        OpKind::Batch { .. } => Response::Done,
        OpKind::Datacenters => Response::Datacenters(vec!["dc1".to_string()]),
        OpKind::Alternator { .. } => Response::Json(Json::Text(Default::default())),
    };
    Completion {
        id: op.id,
        start_ns: now,
        end_ns: clock::now_ns(),
        result: Ok(response),
    }
}

#[allow(dead_code)] // `elapsed` is read only by the CQL tests.
struct Outcome {
    code: i32,
    ops: u64,
    elapsed: Duration,
}

/// Runs `args` with every instance pumped on its own thread. `cancel_after` cancels the run
/// from another thread after that long.
fn run_null(args: &[&str], cancel_after: Option<Duration>) -> Outcome {
    run_host(args, cancel_after, NATIVE, None)
}

/// Like [`run_null`], with a chosen host, and with every op appended to `ops` when given.
/// Pass one thread when recording ops, so their order is fixed.
fn run_host(
    args: &[&str],
    cancel_after: Option<Duration>,
    host: Host,
    record: Option<&std::sync::Mutex<Vec<OpTrace>>>,
) -> Outcome {
    let started = Instant::now();
    let args = args.iter().map(|s| s.to_string()).collect();
    let mut run = match start(args) {
        Start::Exit(code) => {
            return Outcome {
                code,
                ops: 0,
                elapsed: started.elapsed(),
            }
        }
        Start::Driver(run) => run,
    };
    let token: CancelToken = run.cancel_token();
    let canceller = cancel_after.map(|after| {
        let token = token.clone();
        std::thread::spawn(move || {
            std::thread::sleep(after);
            token.cancel();
        })
    });
    let mut answered = 0;
    while let Some(phase) = run.begin_phase() {
        let instances: Vec<_> = (0..phase.instances)
            .map(|i| run.take_instance(i).expect("each instance is taken once"))
            .collect();
        answered += std::thread::scope(|s| {
            let handles: Vec<_> = instances
                .into_iter()
                .map(|mut instance| {
                    instance.set_native(!host.abi);
                    s.spawn(move || {
                        let mut ops = Vec::new();
                        let mut completions = Vec::new();
                        let mut answered = 0u64;
                        loop {
                            let status =
                                instance.pump(clock::now_ns(), completions.drain(..), &mut ops);
                            for op in ops.drain(..) {
                                if let Some(record) = record {
                                    record.lock().unwrap().push(trace(&op));
                                }
                                completions.push(answer(op, host));
                                answered += 1;
                            }
                            if matches!(status, PumpStatus::Done | PumpStatus::Cancelled) {
                                return answered;
                            }
                            if completions.is_empty() {
                                std::thread::yield_now();
                            }
                        }
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).sum::<u64>()
        });
        if !run.end_phase() {
            break;
        }
    }
    let code = run.finish();
    if let Some(c) = canceller {
        c.join().unwrap();
    }
    Outcome {
        code,
        ops: answered,
        elapsed: started.elapsed(),
    }
}

#[cfg(feature = "cql")]
mod cql {
    use super::*;

    #[test]
    fn null_driver_runs_a_fixed_count_of_cycles() {
        let w = workload("basic/write.rn");
        let out = run_null(
            &[
                "latte",
                "run",
                &w,
                "127.0.0.1:1",
                "-d",
                "2000",
                "-w",
                "0",
                "-t",
                "2",
                "-q",
            ],
            None,
        );
        assert_eq!(out.code, 0);
        // One cluster-info query, one prepare, one insert per cycle.
        assert_eq!(out.ops, 2 + 2000);
    }

    #[test]
    fn null_driver_keeps_the_rate_limit() {
        let w = workload("basic/write.rn");
        let out = run_null(
            &[
                "latte",
                "run",
                &w,
                "127.0.0.1:1",
                "-d",
                "200",
                "-r",
                "1000",
                "-w",
                "0",
                "-q",
            ],
            None,
        );
        assert_eq!(out.code, 0);
        assert_eq!(out.ops, 2 + 200);
        // 200 cycles at 1000 per second take about 200 ms.
        assert!(out.elapsed >= Duration::from_millis(190));
    }

    #[test]
    fn cancel_mid_run_stops_promptly_and_writes_the_report() {
        let w = workload("basic/write.rn");
        let dir = tempfile::tempdir().unwrap();
        let report = dir.path().join("cancelled.json");
        let report_arg = report.to_string_lossy().into_owned();
        let out = run_null(
            &[
                "latte",
                "run",
                &w,
                "127.0.0.1:1",
                "-d",
                "60s",
                "-w",
                "0",
                "-q",
                "-o",
                &report_arg,
            ],
            Some(Duration::from_millis(300)),
        );
        assert_eq!(out.code, 130, "a cancelled run exits with 130");
        assert!(
            out.elapsed < Duration::from_secs(5),
            "cancel took {:?}",
            out.elapsed
        );
        let text = std::fs::read_to_string(&report).expect("report of the cancelled run");
        assert!(text.contains("\"cancelled\""), "report is marked cancelled");
    }

    #[test]
    fn cancel_stops_a_script_in_an_infinite_loop() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("spin.rn");
        std::fs::write(&script, "pub async fn run(ctx, i) {\n    loop {}\n}\n").unwrap();
        let script = script.to_string_lossy().into_owned();
        let out = run_null(
            &[
                "latte",
                "run",
                &script,
                "127.0.0.1:1",
                "-d",
                "60s",
                "-w",
                "0",
                "-q",
                "--rune-budget",
                "20000000",
            ],
            Some(Duration::from_millis(200)),
        );
        assert_eq!(out.code, 130);
        assert!(
            out.elapsed < Duration::from_secs(20),
            "cancel took {:?}",
            out.elapsed
        );
    }
}

#[cfg(feature = "cql")]
mod conformance {
    use super::*;

    fn op_stream(abi: bool, script: &str, extra: &[&str]) -> Vec<OpTrace> {
        let record = std::sync::Mutex::new(Vec::new());
        let mut args = vec![
            "latte",
            "run",
            script,
            "127.0.0.1:1",
            "-d",
            "300",
            "-w",
            "0",
            "-t",
            "1",
            "-p",
            "1",
            "-q",
        ];
        args.extend_from_slice(extra);
        let out = run_host(&args, None, Host { rows: 1, abi }, Some(&record));
        assert_eq!(out.code, 0);
        normalize(record.into_inner().unwrap())
    }

    /// Statement handles are unique in the process, so two runs get different ones. Numbers
    /// them by first appearance instead.
    fn normalize(mut ops: Vec<OpTrace>) -> Vec<OpTrace> {
        let mut seen = Vec::new();
        for op in &mut ops {
            if op.0 == 1 || op.0 == 3 {
                let index = seen.iter().position(|s| *s == op.1).unwrap_or_else(|| {
                    seen.push(op.1);
                    seen.len() - 1
                });
                op.1 = index as u64;
            }
        }
        ops
    }

    #[test]
    fn native_api_and_c_abi_send_the_same_ops() {
        for script in ["basic/write.rn", "basic/read.rn"] {
            let w = workload(script);
            let extra = ["-P", "row_count=100"];
            let native = op_stream(false, &w, &extra);
            let abi = op_stream(true, &w, &extra);
            assert!(native.len() > 300, "{script}: {} ops", native.len());
            assert_eq!(native, abi, "{script}: the op streams differ");
        }
    }

    fn script(dir: &std::path::Path, name: &str, body: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path.to_string_lossy().into_owned()
    }

    /// A row-count validation runs in count mode and a value read in rows mode. For the same
    /// result sizes they must pass and fail alike.
    #[test]
    fn count_mode_validation_agrees_with_rows_mode() {
        let dir = tempfile::tempdir().unwrap();
        let count = script(
            dir.path(),
            "count.rn",
            "pub async fn run(ctx, i) {\n    ctx.execute_with_validation(\"SELECT * FROM t\", [1, 1]).await?;\n}\n",
        );
        let rows = script(
            dir.path(),
            "rows.rn",
            "pub async fn run(ctx, i) {\n    let rows = ctx.execute_with_result(\"SELECT * FROM t\").await?;\n    if rows.len() != 1 {\n        ctx.signal_failure(\"wrong row count\").await?;\n    }\n}\n",
        );
        for (rows_per_page, expect_ok) in [(1, true), (0, false), (3, false)] {
            let mut codes = Vec::new();
            for w in [&count, &rows] {
                let out = run_host(
                    &[
                        "latte",
                        "run",
                        w,
                        "127.0.0.1:1",
                        "-d",
                        "50",
                        "-w",
                        "0",
                        "-q",
                        "--validation-strategy",
                        "fail-fast",
                    ],
                    None,
                    Host {
                        rows: rows_per_page,
                        abi: false,
                    },
                    None,
                );
                codes.push(out.code);
            }
            assert_eq!(
                codes[0] == 0,
                expect_ok,
                "count mode with {rows_per_page} rows"
            );
            assert_eq!(codes[0] == 0, codes[1] == 0, "count and rows mode disagree");
        }
    }
}

#[cfg(feature = "alternator")]
mod alternator {
    use super::*;

    #[test]
    fn null_driver_completes_alternator_ops() {
        let w = workload("alternator/performance.rn");
        let out = run_null(
            &[
                "latte-alternator",
                "run",
                &w,
                "http://127.0.0.1:1",
                "-f",
                "get:1.0",
                "-d",
                "500",
                "-w",
                "0",
                "-q",
            ],
            None,
        );
        assert_eq!(out.code, 0);
        // Two cluster-info requests, then one GetItem per cycle.
        assert!(out.ops >= 500, "answered {} ops", out.ops);
    }
}
