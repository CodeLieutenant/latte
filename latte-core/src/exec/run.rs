//! The whole Latte command line, split into phases that a driver host runs.
//!
//! [`start`] parses the arguments. Commands that need no database (`list`, `show`, `hdr`,
//! `plot`, `version`, `edit`) run to completion inside it. `schema`, `load` and `run` return a
//! [`Run`]: the host connects its driver with [`Run::connection`], then for each phase pumps
//! the instances that [`Run::begin_phase`] announces, and calls [`Run::end_phase`].

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{stdout, IsTerminal, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use clap::{CommandFactory, FromArgMatches};
use hdrhistogram::serialization::{interval_log, V2DeflateSerializer};
use itertools::Itertools;
use rune::Source;
use search_path::SearchPath;
use status_line::StatusLine;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use walkdir::WalkDir;

use super::cycle::BoundedCycleCounter;
use super::instance::{CycleSetup, Instance, InstanceLimits, SingleSlot};
use super::progress::Progress;
use super::runtime::{BoxTask, CancelToken, Reactor};
use super::workload::{FnRef, Program, Workload, WorkloadStats, LOAD_FN};
use super::ExecutionOptions;
use crate::config::{
    AppConfig, Command, ConnectionConf, EditCommand, HdrCommand, Interval, ListCommand,
    LoadCommand, RunCommand, SchemaCommand, ShowCommand, VersionCommand,
};
use crate::error::{LatteError, Result};
use crate::report::plot::plot_graph;
use crate::report::table::{Alignment, Table};
use crate::report::{self, PathAndSummary, Report, RunConfigCmp};
use crate::scripting::cluster_info::ClusterInfo;
use crate::scripting::context::Context;
use crate::stats::histogram::HistogramWriter;
use crate::stats::{BenchmarkCmp, BenchmarkStats, Recorder};
use crate::version::{format_version_info_human, get_formatted_version_info};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Result of [`start`].
pub enum Start {
    /// The command finished. The host exits with this code.
    Exit(i32),
    /// The command needs a database session.
    Driver(Box<Run>),
}

/// The database API a workload script talks to. A core build serves one backend, chosen by
/// the `cql` or `alternator` feature, the same way the `latte` and `latte-alternator`
/// binaries do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Cql,
    Alternator,
}

impl Backend {
    pub fn name(self) -> &'static str {
        match self {
            Backend::Cql => "cql",
            Backend::Alternator => "alternator",
        }
    }
}

/// The backend this core build serves.
pub const fn backend() -> Backend {
    #[cfg(feature = "alternator")]
    {
        Backend::Alternator
    }
    #[cfg(not(feature = "alternator"))]
    {
        Backend::Cql
    }
}

/// What a phase asks the host to run.
#[derive(Clone, Debug)]
pub struct PhaseInfo {
    pub name: &'static str,
    /// Number of instances to pump, each on its own thread or task.
    pub instances: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PhaseKind {
    ClusterInfo,
    Schema,
    Prepare,
    Erase,
    Load,
    Warmup,
    Bench,
}

impl PhaseKind {
    fn name(self) -> &'static str {
        match self {
            PhaseKind::ClusterInfo => "cluster-info",
            PhaseKind::Schema => "schema",
            PhaseKind::Prepare => "prepare",
            PhaseKind::Erase => "erase",
            PhaseKind::Load => "load",
            PhaseKind::Warmup => "warmup",
            PhaseKind::Bench => "run",
        }
    }
}

enum DriverCommand {
    Schema(SchemaCommand),
    Load(LoadCommand),
    Run(Box<RunCommand>),
}

enum Active {
    Single(PhaseKind, SingleSlot),
    Parallel(PhaseKind, Arc<PhaseSink>),
}

/// A `schema`, `load` or `run` command in progress.
pub struct Run {
    command: DriverCommand,
    program: Program,
    main: Context,
    phases: VecDeque<PhaseKind>,
    active: Option<Active>,
    instances: Vec<Option<Instance>>,
    client_id: String,
    cluster_info: Arc<Mutex<Option<ClusterInfo>>>,
    compare: Option<Report>,
    stats: Option<BenchmarkStats>,
    exit_code: i32,
    cancel: CancelToken,
    end_of_run_timeouts: Arc<AtomicU64>,
    _log_guard: Option<WorkerGuard>,
}

/// Collects the samples of every instance of a parallel phase and records one aggregate
/// sample once each instance that still runs has delivered its next one.
pub struct PhaseSink {
    state: Mutex<SinkState>,
}

struct SinkState {
    queues: Vec<VecDeque<std::result::Result<WorkloadStats, String>>>,
    closed: Vec<bool>,
    recorder: Option<Recorder>,
    first_error: Option<String>,
    print_samples: bool,
    show_progress: bool,
    progress: Arc<StatusLine<Progress>>,
}

impl PhaseSink {
    fn new(
        instances: usize,
        recorder: Recorder,
        print_samples: bool,
        show_progress: bool,
        progress: Arc<StatusLine<Progress>>,
    ) -> Self {
        PhaseSink {
            state: Mutex::new(SinkState {
                queues: (0..instances).map(|_| VecDeque::new()).collect(),
                closed: vec![false; instances],
                recorder: Some(recorder),
                first_error: None,
                print_samples,
                show_progress,
                progress,
            }),
        }
    }

    pub(crate) fn push_stats(&self, index: usize, stats: WorkloadStats) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.queues[index].push_back(Ok(stats));
        state.record_rounds(false);
    }

    pub(crate) fn push_error(&self, index: usize, error: LatteError) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.queues[index].push_back(Err(error.to_string()));
        state.record_rounds(false);
    }

    pub(crate) fn close(&self, index: usize) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed[index] = true;
        state.record_rounds(false);
    }

    /// True once any instance failed. The other instances stop starting cycles.
    pub(crate) fn is_aborted(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .first_error
            .is_some()
    }

    fn finish(&self, context: &Context) -> Result<BenchmarkStats> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.record_rounds(true);
        if let Some(e) = state.first_error.take() {
            return Err(LatteError::Worker(e));
        }
        let recorder = state.recorder.take().expect("phase finished twice");
        Ok(recorder.finish(
            context.report_metadata_snapshot(),
            context.metric_orientations_snapshot(),
        ))
    }
}

impl SinkState {
    /// Takes one item from each instance with a queued item, as long as every instance
    /// that is still open has one. With `drain` set, takes what is left after all closed.
    fn record_rounds(&mut self, drain: bool) {
        loop {
            let any = self.queues.iter().any(|q| !q.is_empty());
            let complete = self
                .queues
                .iter()
                .zip(&self.closed)
                .all(|(q, closed)| !q.is_empty() || *closed);
            if !any || !(complete || drain) {
                return;
            }
            let mut round = Vec::new();
            for q in self.queues.iter_mut() {
                match q.pop_front() {
                    Some(Ok(stats)) => round.push(stats),
                    Some(Err(e)) => {
                        if self.first_error.is_none() {
                            self.first_error = Some(e);
                        }
                    }
                    None => {}
                }
            }
            if self.first_error.is_some() || round.is_empty() {
                continue;
            }
            if let Some(recorder) = self.recorder.as_mut() {
                let aggregate = recorder.record(&round);
                if self.print_samples {
                    self.progress.set_visible(false);
                    println!("{aggregate}");
                    self.progress.set_visible(self.show_progress);
                }
            }
        }
    }
}

/// Parses the command line and runs the commands that need no database.
pub fn start(args: Vec<String>) -> Start {
    // NOTE: use 2 new lines because 'latte --version' concatenates the app name and its version
    let boxed_version = format!("\n\n{}", format_version_info_human()).into_boxed_str();
    let static_version: &'static str = Box::leak(boxed_version);
    let app_cmd = AppConfig::command().version(static_version);
    let matches = match app_cmd.try_get_matches_from(args) {
        Ok(m) => m,
        Err(e) => {
            let _ = e.print();
            return Start::Exit(e.exit_code());
        }
    };
    let config = match AppConfig::from_arg_matches(&matches) {
        Ok(c) => c,
        Err(e) => {
            let _ = e.print();
            return Start::Exit(e.exit_code());
        }
    };

    let run_id = run_id();
    let log_guard = if config.enable_logging {
        match setup_logging(run_id.as_str(), &config) {
            Ok(guard) => Some(guard),
            Err(e) => {
                eprintln!("error: {e}");
                return Start::Exit(1);
            }
        }
    } else {
        None
    };

    let result = match config.command {
        Command::Edit(c) => edit(c).map(|_| None),
        Command::List(c) => list(c).map(|_| None),
        Command::Show(c) => show(c).map(|_| None),
        Command::Hdr(c) => export_hdr_log(c).map(|_| None),
        Command::Plot(c) => futures::executor::block_on(plot_graph(c)).map(|_| None),
        Command::Version(c) => print_version(c).map(|_| None),
        Command::Schema(c) => Run::new(DriverCommand::Schema(c), log_guard).map(Some),
        Command::Load(c) => Run::new(DriverCommand::Load(c), log_guard).map(Some),
        Command::Run(mut c) => {
            c.id = Some(run_id);
            let c = c.set_timestamp_if_empty();
            Run::new(DriverCommand::Run(Box::new(c)), log_guard).map(Some)
        }
    };
    match result {
        Ok(Some(run)) => Start::Driver(Box::new(run)),
        Ok(None) => Start::Exit(0),
        Err(e) => {
            eprintln!("error: {e}");
            Start::Exit(128)
        }
    }
}

impl Run {
    fn new(command: DriverCommand, log_guard: Option<WorkerGuard>) -> Result<Run> {
        let (workload, params, connection) = match &command {
            DriverCommand::Schema(c) => (&c.workload, &c.params, &c.connection),
            DriverCommand::Load(c) => (&c.workload, &c.params, &c.connection),
            DriverCommand::Run(c) => (&c.workload, &c.params, &c.connection),
        };
        let program = load_workload_script(workload, params)?;
        let main = Context::from_connection(connection);

        let mut phases = VecDeque::new();
        phases.push_back(PhaseKind::ClusterInfo);
        let mut client_id = "schema".to_string();
        let mut compare = None;
        let mut command = command;
        match &mut command {
            DriverCommand::Schema(_) => {
                if !program.has_schema() {
                    return Err(LatteError::Configuration(
                        "Function `schema` not found in the workload script.".to_string(),
                    ));
                }
                phases.push_back(PhaseKind::Schema);
            }
            DriverCommand::Load(_) => {
                client_id = "load".to_string();
                if program.has_prepare() {
                    phases.push_back(PhaseKind::Prepare);
                }
                if program.has_erase() {
                    phases.push_back(PhaseKind::Erase);
                }
                phases.push_back(PhaseKind::Load);
            }
            DriverCommand::Run(conf) => {
                compare = match conf.baseline.as_ref() {
                    Some(p) => Some(Report::load(p).map_err(|e| {
                        LatteError::Configuration(format!(
                            "Failed to read report from {}: {e}",
                            p.display()
                        ))
                    })?),
                    None => None,
                };
                for f in &conf.functions {
                    if !program.has_function(&FnRef::new(f.name.as_str())) {
                        return Err(LatteError::Configuration(format!(
                            "Function {} not found in the workload script.",
                            f.name
                        )));
                    }
                }
                // NOTE: prefer the tags, as they are what a caller labels a particular run with,
                //       and fall back to the functions when no tag was given.
                //       The 'tag--'/'fn--' prefix says which of the two a reader is looking at.
                client_id = if conf.tags.is_empty() {
                    format!("fn--{}", conf.functions.iter().join(","))
                } else {
                    format!("tag--{}", conf.tags.join(","))
                };
                if program.has_prepare() {
                    phases.push_back(PhaseKind::Prepare);
                }
                if conf.warmup_duration.is_not_zero() {
                    phases.push_back(PhaseKind::Warmup);
                }
                phases.push_back(PhaseKind::Bench);
            }
        }

        Ok(Run {
            command,
            program,
            main,
            phases,
            active: None,
            instances: Vec::new(),
            client_id,
            cluster_info: Arc::new(Mutex::new(None)),
            compare,
            stats: None,
            exit_code: 0,
            cancel: CancelToken::new(),
            end_of_run_timeouts: Arc::new(AtomicU64::new(0)),
            _log_guard: log_guard,
        })
    }

    /// Settings the host builds its driver session from.
    pub fn connection(&self) -> &ConnectionConf {
        match &self.command {
            DriverCommand::Schema(c) => &c.connection,
            DriverCommand::Load(c) => &c.connection,
            DriverCommand::Run(c) => &c.connection,
        }
    }

    /// The client id the host advertises to the server, to tell concurrent runs apart.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Stores the effective settings of the host's driver as a JSON document. A `run`
    /// command writes them into its report.
    pub fn set_driver_settings(&mut self, json: &[u8]) -> Result<()> {
        let value: serde_json::Value = serde_json::from_slice(json)
            .map_err(|e| LatteError::Configuration(format!("driver settings: {e}")))?;
        if let DriverCommand::Run(conf) = &mut self.command {
            conf.driver_settings = Some(value);
        }
        Ok(())
    }

    /// A token that stops the run. Clones may move to other threads and signal handlers.
    pub fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    fn limits(&self) -> InstanceLimits {
        let conf = self.connection();
        InstanceLimits {
            cancel: self.cancel.clone(),
            drain_timeout: conf.request_timeout + conf.drain_margin,
            rune_budget: conf.rune_budget,
            end_of_run_timeouts: Arc::clone(&self.end_of_run_timeouts),
        }
    }

    fn new_reactor(&self) -> Rc<Reactor> {
        Reactor::new(self.connection().request_timeout.as_nanos() as u64)
    }

    /// Starts the next phase and returns what the host must pump, or `None` when the
    /// command is complete or failed.
    pub fn begin_phase(&mut self) -> Option<PhaseInfo> {
        assert!(self.active.is_none(), "end the current phase first");
        if self.exit_code != 0 || self.cancel.is_cancelled() {
            return None;
        }
        let kind = self.phases.pop_front()?;
        match self.try_begin(kind) {
            Ok(info) => Some(info),
            Err(e) => {
                self.fail(kind, e);
                None
            }
        }
    }

    /// Hands out instance `index` of the current phase. Each instance is taken once.
    pub fn take_instance(&mut self, index: usize) -> Option<Instance> {
        self.instances.get_mut(index).and_then(Option::take)
    }

    /// Ends the current phase after all its instances returned `Done`.
    /// Returns false when the phase failed; the error was printed.
    pub fn end_phase(&mut self) -> bool {
        self.instances.clear();
        let Some(active) = self.active.take() else {
            return self.exit_code == 0;
        };
        let (kind, result) = match active {
            Active::Single(_, _) if self.cancel.is_cancelled() => return false,
            Active::Single(kind, slot) => {
                let result = slot
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .unwrap_or_else(|| {
                        Err(LatteError::Configuration(format!(
                            "phase {} ended before its task finished",
                            kind.name()
                        )))
                    });
                (kind, result.and_then(|_| self.after_single(kind)))
            }
            Active::Parallel(kind, sink) => {
                if self.cancel.is_cancelled() {
                    self.main.set_report_field("cancelled", "true");
                }
                let timeouts = self.end_of_run_timeouts.load(Ordering::Relaxed);
                self.main
                    .set_report_field("end_of_run_timeouts", &timeouts.to_string());
                let result = sink.finish(&self.main);
                (
                    kind,
                    result.and_then(|stats| self.after_parallel(kind, stats)),
                )
            }
        };
        match result {
            Ok(()) => !self.cancel.is_cancelled(),
            Err(e) => {
                self.fail(kind, e);
                false
            }
        }
    }

    /// Prints the report and returns the exit code. A cancelled run still writes the report
    /// of what it measured, marked `cancelled`, and exits with 130.
    pub fn finish(mut self) -> i32 {
        if self.exit_code != 0 {
            return self.exit_code;
        }
        let cancelled = self.cancel.is_cancelled();
        let done_code = if cancelled { 130 } else { 0 };
        if cancelled {
            eprintln!("info: Run cancelled");
        }
        let stats = self.stats.take();
        if let DriverCommand::Run(conf) = self.command {
            let Some(stats) = stats else {
                return done_code;
            };
            let stats_cmp = BenchmarkCmp {
                v1: &stats,
                v2: self.compare.as_ref().map(|c| &c.result),
            };
            println!();
            println!("{}", &stats_cmp);
            if stats_cmp.v1.log.len() > 1 || cancelled {
                let conf = *conf;
                let path = conf.output.clone().unwrap_or_else(|| {
                    PathBuf::from(format!("latte-{}.json", conf.id.as_ref().unwrap()))
                });
                let report = Report::new(conf, stats);
                match report.save(&path) {
                    Ok(()) => eprintln!("info: Saved report to {}", path.display()),
                    Err(e) => {
                        eprintln!("error: Failed to save report to {}: {}", path.display(), e);
                        return 1;
                    }
                }
            }
        }
        done_code
    }

    fn fail(&mut self, kind: PhaseKind, e: LatteError) {
        match kind {
            PhaseKind::Schema => eprintln!("error: Failed to create schema: {e}"),
            PhaseKind::Prepare => eprintln!("error: Failed to prepare: {e}"),
            PhaseKind::Erase => eprintln!("error: Failed to erase: {e}"),
            _ => eprintln!("error: {e}"),
        }
        self.exit_code = match kind {
            PhaseKind::Schema | PhaseKind::Prepare | PhaseKind::Erase | PhaseKind::Load => 255,
            _ => 128,
        };
    }

    fn single(
        &mut self,
        kind: PhaseKind,
        task: impl FnOnce(Context, Program) -> BoxTask<Result<()>>,
    ) -> PhaseInfo {
        let reactor = self.new_reactor();
        let mut ctx = self.main.shallow_clone();
        ctx.set_reactor(Rc::clone(&reactor));
        let slot: SingleSlot = Arc::new(Mutex::new(None));
        let instance = Instance::single(
            reactor,
            task(ctx, self.program.clone()),
            Arc::clone(&slot),
            self.limits(),
        );
        self.instances = vec![Some(instance)];
        self.active = Some(Active::Single(kind, slot));
        PhaseInfo {
            name: kind.name(),
            instances: 1,
        }
    }

    fn try_begin(&mut self, kind: PhaseKind) -> Result<PhaseInfo> {
        match kind {
            PhaseKind::ClusterInfo => {
                eprintln!("info: Connecting to {:?}... ", self.connection().addresses);
                let slot = Arc::clone(&self.cluster_info);
                Ok(self.single(kind, move |ctx, _| {
                    Box::pin(async move {
                        let info = ctx.cluster_info().await?;
                        *slot.lock().unwrap() = info;
                        Ok(())
                    })
                }))
            }
            PhaseKind::Schema => {
                eprintln!("info: Creating schema...");
                Ok(self.single(kind, |ctx, mut program| {
                    Box::pin(async move { program.schema(&ctx).await })
                }))
            }
            PhaseKind::Prepare => {
                eprintln!("info: Preparing...");
                Ok(self.single(kind, |ctx, mut program| {
                    Box::pin(async move { program.prepare(&ctx).await })
                }))
            }
            PhaseKind::Erase => {
                eprintln!("info: Erasing data...");
                Ok(self.single(kind, |ctx, mut program| {
                    Box::pin(async move { program.erase(&ctx).await })
                }))
            }
            PhaseKind::Load => self.begin_load(),
            PhaseKind::Warmup | PhaseKind::Bench => self.begin_run_phase(kind),
        }
    }

    fn begin_load(&mut self) -> Result<PhaseInfo> {
        let DriverCommand::Load(conf) = &self.command else {
            unreachable!()
        };
        let load_count = self.main.load_cycle_count();
        if load_count > 0 && !self.program.has_load() {
            return Err(LatteError::Configuration(
                "Function `load` not found in the workload script.".to_string(),
            ));
        }
        eprintln!("info: Loading data...");
        let options = ExecutionOptions {
            duration: Interval::Count(load_count),
            cycle_range: (0, i64::MAX),
            rate: conf.rate.rate,
            rate_sine_amplitude: conf.rate.rate_sine_amplitude,
            rate_sine_period: conf.rate.rate_sine_period,
            threads: conf.threads,
            concurrency: conf.concurrency,
        };
        let show_progress = !conf.quiet;
        let workload = Workload::new(
            self.main.clone()?,
            self.program.clone(),
            &[(FnRef::new(LOAD_FN), 1.0)],
        );
        self.begin_parallel(
            PhaseKind::Load,
            "Loading...",
            options,
            Interval::Unbounded,
            workload,
            show_progress,
            false,
            None,
        )
    }

    fn begin_run_phase(&mut self, kind: PhaseKind) -> Result<PhaseInfo> {
        let DriverCommand::Run(conf) = &self.command else {
            unreachable!()
        };
        let functions: Vec<_> = conf
            .functions
            .iter()
            .map(|f| (FnRef::new(f.name.as_str()), f.weight))
            .collect();
        let show_progress = !conf.quiet;
        let warmup = kind == PhaseKind::Warmup;
        let options = ExecutionOptions {
            duration: if warmup {
                conf.warmup_duration
            } else {
                conf.run_duration
            },
            cycle_range: (conf.start_cycle, conf.end_cycle),
            rate: if warmup { None } else { conf.rate.rate },
            rate_sine_amplitude: conf.rate.rate_sine_amplitude,
            rate_sine_period: conf.rate.rate_sine_period,
            threads: conf.threads,
            concurrency: conf.concurrency,
        };
        let sampling = conf.sampling_interval;
        let generate_report = conf.generate_report;
        let hdrfile = conf.hdrfile.clone();
        if warmup {
            eprintln!("info: Warming up...");
        } else {
            eprintln!("info: Running benchmark...");
            println!(
                "{}",
                RunConfigCmp {
                    v1: conf,
                    v2: self.compare.as_ref().map(|c| &c.conf),
                }
            );
            report::print_log_header();
        }
        let workload = Workload::new(self.main.clone()?, self.program.clone(), &functions);
        if warmup {
            return self.begin_parallel(
                kind,
                "Warming up...",
                options,
                Interval::Unbounded,
                workload,
                show_progress,
                false,
                None,
            );
        }
        let hdrh_writer = match hdrfile {
            Some(ref hdrfile) => Some(open_hdr_writer(hdrfile)?),
            None => None,
        };
        self.begin_parallel(
            kind,
            "Running...",
            options,
            sampling,
            workload,
            show_progress,
            generate_report,
            hdrh_writer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_parallel(
        &mut self,
        kind: PhaseKind,
        name: &str,
        options: ExecutionOptions,
        sampling: Interval,
        workload: Workload,
        show_progress: bool,
        keep_log: bool,
        hdrh_writer: Option<Box<dyn HistogramWriter + Send>>,
    ) -> Result<PhaseInfo> {
        if options.cycle_range.1 <= options.cycle_range.0 {
            return Err(LatteError::Configuration(format!(
                "End cycle {} must not be lower than start cycle {}",
                options.cycle_range.1, options.cycle_range.0
            )));
        }
        let thread_count = options.threads.get();
        let progress = match options.duration {
            Interval::Count(count) => Progress::with_count(name.to_string(), count),
            Interval::Time(duration) => Progress::with_duration(name.to_string(), duration),
            Interval::Unbounded => unreachable!(),
        };
        let is_tty = std::io::stderr().is_terminal();
        let progress_opts = status_line::Options {
            initially_visible: show_progress,
            refresh_period: Duration::from_millis(if is_tty { 100 } else { 1000 }),
            enable_ansi_escapes: is_tty,
        };
        let progress = Arc::new(StatusLine::with_options(progress, progress_opts));
        let recorder = Recorder::start(options.rate, options.concurrency, keep_log, hdrh_writer);
        let sink = Arc::new(PhaseSink::new(
            thread_count,
            recorder,
            sampling.is_bounded(),
            show_progress,
            Arc::clone(&progress),
        ));
        let deadline = BoundedCycleCounter::new(options.duration, options.cycle_range);
        let rate_sine_frequency = 1.0 / options.rate_sine_period.as_secs_f64();
        let rate = options.rate.map(|r| r / thread_count as f64);
        let mut instances = Vec::with_capacity(thread_count);
        for index in 0..thread_count {
            let reactor = self.new_reactor();
            let mut worker = workload.clone()?;
            worker.set_reactor(Rc::clone(&reactor));
            let setup = CycleSetup {
                index,
                worker_count: thread_count,
                counter: deadline.share(),
                rate,
                rate_sine_amplitude: options.rate_sine_amplitude.unwrap_or(0.0)
                    * rate.unwrap_or(0.0),
                rate_sine_frequency,
                concurrency: options.concurrency.get(),
                sampling,
                progress: Arc::clone(&progress),
                sink: Arc::clone(&sink),
            };
            instances.push(Some(Instance::cycles(
                reactor,
                worker,
                setup,
                self.limits(),
            )));
        }
        self.instances = instances;
        self.active = Some(Active::Parallel(kind, sink));
        Ok(PhaseInfo {
            name: kind.name(),
            instances: thread_count,
        })
    }

    fn after_single(&mut self, kind: PhaseKind) -> Result<()> {
        match kind {
            PhaseKind::ClusterInfo => {
                let info = self.cluster_info.lock().unwrap().take();
                eprintln!(
                    "info: Connected to '{}' cluster running {}",
                    info.as_ref().map(|c| c.name.as_str()).unwrap_or("unknown"),
                    info.as_ref()
                        .map(|c| c.db_version.as_str())
                        .unwrap_or("unknown")
                );
                if let DriverCommand::Run(conf) = &mut self.command {
                    if let Some(info) = info {
                        conf.cluster_name = Some(info.name);
                        conf.db_version = Some(info.db_version);
                    }
                    // NOTE: Add info about the target rune functions to the context
                    //       for the more flexible tweaking of the 'prepare' rune function.
                    let functions_to_invoke: Vec<(String, f64)> = conf
                        .functions
                        .iter()
                        .map(|f| (f.name.clone(), f.weight))
                        .collect();
                    let mut obj = self
                        .main
                        .data
                        .borrow_mut::<rune::runtime::Object>()
                        .map_err(|_| {
                            LatteError::Configuration("session.data is not a Rune Object".into())
                        })?;
                    obj.insert(
                        rune::alloc::String::try_from("functions_to_invoke")?,
                        rune::to_value(functions_to_invoke)?,
                    )?;
                }
                Ok(())
            }
            PhaseKind::Schema => {
                eprintln!("info: Schema created successfully");
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn after_parallel(&mut self, kind: PhaseKind, stats: BenchmarkStats) -> Result<()> {
        match kind {
            PhaseKind::Load => {
                if stats.error_count > 0 {
                    for e in &stats.errors {
                        eprintln!("error: {e}");
                    }
                    return Err(LatteError::Configuration(
                        "Errors encountered when loading data. Some data might be missing."
                            .to_string(),
                    ));
                }
                Ok(())
            }
            PhaseKind::Bench => {
                self.stats = Some(stats);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn open_hdr_writer(hdrfile: &Path) -> Result<Box<dyn HistogramWriter + Send>> {
    if let Some(parent_dir) = hdrfile.parent() {
        fs::create_dir_all(parent_dir)
            .map_err(|e| LatteError::LogFileCreate(hdrfile.to_path_buf(), e))?;
    }
    let file =
        File::create(hdrfile).map_err(|e| LatteError::LogFileCreate(hdrfile.to_path_buf(), e))?;
    let serializer = Box::new(V2DeflateSerializer::new());
    let system_time_now = SystemTime::now();
    let writer = interval_log::IntervalLogWriterBuilder::new()
        .add_comment(format!("[Logged with Latte {VERSION}]").as_str())
        .with_start_time(system_time_now)
        .with_base_time(system_time_now)
        .with_max_value_divisor(1000000.0) // ms
        .begin_log_with(Box::leak(Box::new(file)), Box::leak(serializer))
        .unwrap();
    Ok(Box::new(writer))
}

/// Reads the workload script from a file and compiles it.
fn load_workload_script(workload: &Path, params: &[(String, String)]) -> Result<Program> {
    let workload = find_workload(workload)
        .canonicalize()
        .unwrap_or_else(|_| workload.to_path_buf());
    eprintln!("info: Loading workload script {}...", workload.display());
    let src =
        Source::from_path(&workload).map_err(|e| LatteError::ScriptRead(workload.clone(), e))?;
    let program = Program::new(src, params.iter().cloned().collect())?;
    info!(
        "Loaded workload script {}\n\tParams: {:?}",
        workload.display(),
        params,
    );
    Ok(program)
}

/// Locates the workload and returns an absolute path to it.
/// If not found, returns the original path unchanged.
/// If the workload path is relative, it is searched in the directories
/// listed by `LATTE_WORKLOAD_PATH` environment variable.
/// If the variable is not set, workload is searched in
/// `.local/share/latte/workloads` and `/usr/share/latte/workloads`.
fn find_workload(workload: &Path) -> PathBuf {
    if workload.starts_with(".") || workload.is_absolute() {
        return workload.to_path_buf();
    }
    let search_path = SearchPath::new("LATTE_WORKLOAD_PATH").unwrap_or_else(|_| {
        let relative_to_exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf))
            .map(|p| p.join("workloads"));
        SearchPath::from(
            [
                PathBuf::from(".local/share/latte/workloads"),
                PathBuf::from("/usr/share/latte/workloads"),
            ]
            .into_iter()
            .chain(relative_to_exe)
            .collect_vec(),
        )
    });
    search_path
        .find_file(workload)
        .unwrap_or_else(|| workload.to_path_buf())
}

fn load_report(path: &Path) -> Result<Report> {
    Report::load(path).map_err(|e| {
        LatteError::Configuration(format!(
            "Failed to read report from {}: {e}",
            path.display()
        ))
    })
}

fn list(conf: ListCommand) -> Result<()> {
    let max_depth = if conf.recursive { usize::MAX } else { 1 };
    let paths: Vec<PathBuf> = conf
        .output
        .iter()
        .flat_map(|path| {
            WalkDir::new(path)
                .max_depth(max_depth)
                .into_iter()
                .flatten()
        })
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| entry.path().extension() == Some(OsStr::new("json")))
        .map(|entry| entry.path().to_path_buf())
        .collect();

    // Loading reports is a bit slow, so we do it in parallel:
    let loaded: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = paths
            .iter()
            .map(|path| s.spawn(move || (path.clone(), Report::load(path))))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut reports = Vec::new();
    for report in loaded {
        match report {
            (path, Ok(report)) if should_list(&report, &conf) => {
                reports.push(PathAndSummary(path, report.summary()))
            }
            (path, Err(e)) => eprintln!("Failed to load report {}: {}", path.display(), e),
            _ => {}
        };
    }

    if !reports.is_empty() {
        reports.sort_unstable_by_key(|s| {
            (
                s.1.workload.clone(),
                s.1.functions.clone(),
                s.1.params.clone(),
                s.1.tags.clone(),
                s.1.timestamp,
            )
        });
        let mut table = Table::new(PathAndSummary::COLUMNS);
        table.align(7, Alignment::Right);
        table.align(8, Alignment::Right);
        table.align(9, Alignment::Right);
        for r in reports {
            table.push(r);
        }
        println!("{table}");
    }
    Ok(())
}

fn should_list(report: &Report, conf: &ListCommand) -> bool {
    if let Some(workload_pattern) = &conf.workload {
        if !report
            .conf
            .workload
            .to_string_lossy()
            .contains(workload_pattern)
        {
            return false;
        }
    }
    if let Some(function) = &conf.function {
        if !report
            .conf
            .functions
            .iter()
            .map(|f| &f.name)
            .contains(function)
        {
            return false;
        }
    }
    if !conf.tags.is_empty() && !conf.tags.iter().any(|t| report.conf.tags.contains(t)) {
        return false;
    }
    true
}

fn show(conf: ShowCommand) -> Result<()> {
    let report1 = load_report(&conf.report)?;
    let report2 = conf.baseline.map(|p| load_report(&p)).transpose()?;

    let config_cmp = RunConfigCmp {
        v1: &report1.conf,
        v2: report2.as_ref().map(|r| &r.conf),
    };
    println!("{config_cmp}");

    let results_cmp = BenchmarkCmp {
        v1: &report1.result,
        v2: report2.as_ref().map(|r| &r.result),
    };
    println!("{results_cmp}");
    Ok(())
}

/// Reads histograms from the report and dumps them to an hdr log
fn export_hdr_log(conf: HdrCommand) -> Result<()> {
    let tag_prefix = conf.tag.map(|t| t + ".").unwrap_or_default();
    if tag_prefix.chars().any(|c| ", \n\t".contains(c)) {
        return Err(LatteError::Configuration(
            "Hdr histogram tags are not allowed to contain commas nor whitespace.".to_string(),
        ));
    }

    let report = load_report(&conf.report)?;
    let stdout = stdout();
    let output_file: File;
    let stdout_stream;
    let mut out: Box<dyn Write> = match conf.output {
        Some(path) => {
            output_file = File::create(&path).map_err(|e| LatteError::OutputFileCreate(path, e))?;
            Box::new(output_file)
        }
        None => {
            stdout_stream = stdout.lock();
            Box::new(stdout_stream)
        }
    };

    let mut serializer = V2DeflateSerializer::new();
    let mut log_writer = interval_log::IntervalLogWriterBuilder::new()
        .add_comment(format!("[Logged with Latte {VERSION}]").as_str())
        .with_start_time(report.result.start_time.into())
        .with_base_time(report.result.start_time.into())
        .with_max_value_divisor(1000000.0) // ms
        .begin_log_with(&mut out, &mut serializer)
        .unwrap();

    for sample in &report.result.log {
        let interval_start_time = Duration::from_millis((sample.time_s * 1000.0) as u64);
        let interval_duration = Duration::from_millis((sample.duration_s * 1000.0) as u64);
        log_writer.write_histogram(
            &sample.cycle_latency.histogram.0,
            interval_start_time,
            interval_duration,
            interval_log::Tag::new(format!("{tag_prefix}cycles").as_str()),
        )?;
        log_writer.write_histogram(
            &sample.request_latency.histogram.0,
            interval_start_time,
            interval_duration,
            interval_log::Tag::new(format!("{tag_prefix}requests").as_str()),
        )?;
    }
    Ok(())
}

fn print_version(conf: VersionCommand) -> Result<()> {
    println!("{}", get_formatted_version_info(conf.json));
    Ok(())
}

fn edit(config: EditCommand) -> Result<()> {
    let workload = find_workload(&config.workload)
        .canonicalize()
        .unwrap_or_else(|_| config.workload.to_path_buf());
    File::open(&workload).map_err(|err| LatteError::ScriptRead(workload.clone(), err.into()))?;
    let editor = std::env::var("LATTE_EDITOR")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or("vi".to_string());
    std::process::Command::new(&editor)
        .current_dir(workload.parent().unwrap_or(Path::new(".")))
        .arg(workload)
        .status()
        .map_err(|e| LatteError::ExternalEditorLaunch(editor, e))?;
    Ok(())
}

fn setup_logging(run_id: &str, config: &AppConfig) -> Result<WorkerGuard> {
    let log_file = match &config.log_file {
        Some(file) if file.is_absolute() => file.clone(),
        Some(file) => config.log_dir.clone().join(file),
        None => config.log_dir.join(format!("latte-{run_id}.log")),
    };
    fs::create_dir_all(&config.log_dir)
        .map_err(|e| LatteError::LogFileCreate(log_file.clone(), e))?;
    let log_file = File::create(&log_file).map_err(|e| LatteError::LogFileCreate(log_file, e))?;
    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("LATTE_LOG")
        .from_env()
        .map_err(|e| LatteError::Configuration(e.to_string()))?
        .add_directive("rune=off".parse().unwrap()); // turn off rune tracing for performance reasons

    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(non_blocking)
        .with_env_filter(filter)
        .init();
    Ok(guard)
}

fn run_id() -> String {
    chrono::Local::now().format("%Y%m%d-%H%M%S-%3f").to_string()
}

/// Thread count that the host should use for its driver runtime.
pub fn thread_hint(run: &Run) -> NonZeroUsize {
    match &run.command {
        DriverCommand::Run(c) => c.threads,
        DriverCommand::Load(c) => c.threads,
        DriverCommand::Schema(_) => NonZeroUsize::MIN,
    }
}

/// Connection secrets that reports leave out, so that a host in another language gets them
/// with the rest of the connection settings.
pub fn connection_secrets(conf: &ConnectionConf) -> serde_json::Value {
    #[cfg(feature = "alternator")]
    {
        serde_json::json!({ "secret_access_key": conf.db.secret_access_key })
    }
    #[cfg(not(feature = "alternator"))]
    {
        serde_json::json!({ "password": conf.db.password })
    }
}
