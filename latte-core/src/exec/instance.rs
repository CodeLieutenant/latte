//! One worker of a phase. The host owns one instance per worker thread and calls
//! [`Instance::pump`] in a loop: each call takes completions in, runs the script tasks
//! that can move, starts the cycles that are due and hands new ops out.

use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use status_line::StatusLine;

use super::cycle::BoundedCycleCounter;
use super::progress::Progress;
use super::run::PhaseSink;
use super::runtime::{BoxTask, CancelToken, Reactor, TaskSet};
use super::workload::Workload;
use super::Schedule;
use crate::clock;
use crate::error::Result;
use crate::ops::{Completion, Op};
use crate::Interval;

pub(crate) enum TaskOut {
    PrepareWorker(Box<Workload>, Result<()>),
    Cycle(Result<(i64, Instant)>),
    Single(Result<()>),
}

/// What the host does after a pump call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PumpStatus {
    /// Ops are in flight or cycles are scheduled. Call `pump` again after a completion
    /// arrives, or at `next_deadline_ns` on the shared clock, whichever comes first.
    Running { next_deadline_ns: Option<u64> },
    /// The schedule ended or the run was cancelled. The core releases no new op after a
    /// cancel and waits for the ops in flight. Keep pumping as for `Running`.
    Draining { next_deadline_ns: Option<u64> },
    /// The instance finished its part of the phase.
    Done,
    /// The run was cancelled and the instance stopped.
    Cancelled,
}

pub(crate) type SingleSlot = Arc<Mutex<Option<Result<()>>>>;

/// How far ahead of its scheduled time a cycle may run its script. Its first op then waits
/// in the core until the scheduled time.
pub const SCHEDULE_LOOKAHEAD: Duration = Duration::from_millis(1);

/// Limits that every instance of a run shares.
#[derive(Clone)]
pub(crate) struct InstanceLimits {
    pub cancel: CancelToken,
    /// How long the instance waits for ops in flight once the schedule ended or the run was
    /// cancelled: the request timeout plus the drain margin.
    pub drain_timeout: Duration,
    /// Rune instructions a cycle may run; 0 for no limit.
    pub rune_budget: usize,
    /// Ops still in flight when a drain timed out.
    pub end_of_run_timeouts: Arc<AtomicU64>,
}

pub(crate) struct CycleSetup {
    pub index: usize,
    pub worker_count: usize,
    pub counter: BoundedCycleCounter,
    pub rate: Option<f64>,
    pub rate_sine_amplitude: f64,
    pub rate_sine_frequency: f64,
    pub concurrency: usize,
    pub sampling: Interval,
    pub progress: Arc<StatusLine<Progress>>,
    pub sink: Arc<PhaseSink>,
}

struct CycleState {
    setup: CycleSetup,
    workload: Option<Rc<Workload>>,
    /// Cycles finished since the progress bar last heard of them.
    ticks: u64,
    schedule: Option<Schedule>,
    in_flight: usize,
    exhausted: bool,
    failed: bool,
    chunk_count: u64,
    next_sample_at: Option<Instant>,
}

enum Mode {
    Single(SingleSlot),
    Cycles(Box<CycleState>),
}

pub struct Instance {
    reactor: Rc<Reactor>,
    tasks: TaskSet<TaskOut>,
    /// Outputs of the tasks that finished in the poll in progress. Kept between pump calls
    /// so a pump allocates nothing.
    outputs: Vec<TaskOut>,
    mode: Mode,
    limits: InstanceLimits,
    drain_deadline_ns: Option<u64>,
    finished: Option<PumpStatus>,
}

// An instance holds Rune values, which are not `Send`. A host may pump an instance from any
// thread, but never from two threads at once; the C ABI enforces that with an in-use flag.
unsafe impl Send for Instance {}

impl Instance {
    pub(crate) fn single(
        reactor: Rc<Reactor>,
        task: BoxTask<Result<()>>,
        slot: SingleSlot,
        limits: InstanceLimits,
    ) -> Self {
        let mut tasks = TaskSet::default();
        tasks.spawn(async move { TaskOut::Single(task.await) });
        Instance {
            reactor,
            tasks,
            outputs: Vec::new(),
            mode: Mode::Single(slot),
            limits,
            drain_deadline_ns: None,
            finished: None,
        }
    }

    pub(crate) fn cycles(
        reactor: Rc<Reactor>,
        mut workload: Workload,
        setup: CycleSetup,
        limits: InstanceLimits,
    ) -> Self {
        let index = setup.index as u64;
        let worker_count = setup.worker_count as u64;
        let mut tasks = TaskSet::default();
        tasks.spawn(async move {
            let result = workload.prepare_worker(index, worker_count).await;
            TaskOut::PrepareWorker(Box::new(workload), result)
        });
        Instance {
            reactor,
            tasks,
            outputs: Vec::new(),
            mode: Mode::Cycles(Box::new(CycleState {
                setup,
                workload: None,
                ticks: 0,
                schedule: None,
                in_flight: 0,
                exhausted: false,
                failed: false,
                chunk_count: 0,
                next_sample_at: None,
            })),
            limits,
            drain_deadline_ns: None,
            finished: None,
        }
    }

    /// Applies `completions`, runs every task that can move and appends new ops to `ops`.
    /// `now_ns` is a reading of the shared clock; 0 makes the instance read it itself.
    pub fn pump(
        &mut self,
        now_ns: u64,
        completions: impl IntoIterator<Item = Completion>,
        ops: &mut Vec<Op>,
    ) -> PumpStatus {
        self.pump_limited(now_ns, completions, ops, usize::MAX)
    }

    /// Like [`Instance::pump`], but releases at most `limit` ops.
    pub fn pump_limited(
        &mut self,
        now_ns: u64,
        completions: impl IntoIterator<Item = Completion>,
        ops: &mut Vec<Op>,
        limit: usize,
    ) -> PumpStatus {
        if let Some(status) = self.finished {
            return status;
        }
        let now_ns = super::runtime::pump_time(now_ns);
        let now = clock::instant_at(now_ns);
        self.reactor.set_now(now_ns);
        for completion in completions {
            self.reactor.complete(completion);
        }

        if self.limits.cancel.is_cancelled() {
            return self.pump_cancelled(now_ns);
        }

        let mut outputs = std::mem::take(&mut self.outputs);
        loop {
            self.reactor.expire_deadlines();
            self.reactor.expire_timers();
            let started = self.start_due_cycles(now);
            let polled = self
                .tasks
                .poll_ready(&self.reactor, &self.limits.cancel, |out| outputs.push(out));
            let handled = !outputs.is_empty();
            for out in outputs.drain(..) {
                self.handle(out, now);
            }
            if self.limits.cancel.is_cancelled() {
                self.outputs = outputs;
                return self.pump_cancelled(now_ns);
            }
            if !started && !polled && !handled {
                break;
            }
        }
        self.outputs = outputs;
        self.maybe_sample(now);
        if let Mode::Cycles(state) = &mut self.mode {
            state.setup.progress.add(std::mem::take(&mut state.ticks));
        }
        self.reactor.drain_ops(ops, limit);
        if let Some(status) = self.finish_if_done(now_ns) {
            return status;
        }
        let next_deadline_ns = self.next_deadline();
        if self.is_draining() {
            PumpStatus::Draining { next_deadline_ns }
        } else {
            PumpStatus::Running { next_deadline_ns }
        }
    }

    /// After a cancel: no script code runs and no op goes out. The instance waits for the
    /// ops in flight until they finish or the drain timeout passes.
    fn pump_cancelled(&mut self, now_ns: u64) -> PumpStatus {
        if !self.tasks.is_empty() {
            self.tasks.clear();
            if let Mode::Cycles(state) = &mut self.mode {
                state.in_flight = 0;
            }
        }
        let deadline = *self
            .drain_deadline_ns
            .get_or_insert_with(|| now_ns + self.limits.drain_timeout.as_nanos() as u64);
        let in_flight = self.reactor.in_flight();
        if in_flight > 0 && now_ns < deadline {
            return PumpStatus::Draining {
                next_deadline_ns: Some(deadline),
            };
        }
        if in_flight > 0 {
            self.limits
                .end_of_run_timeouts
                .fetch_add(in_flight as u64, Ordering::Relaxed);
        }
        self.reactor.clear();
        self.close(Instant::now());
        self.finished = Some(PumpStatus::Cancelled);
        PumpStatus::Cancelled
    }

    fn is_draining(&self) -> bool {
        match &self.mode {
            Mode::Cycles(state) => state.exhausted || state.failed,
            Mode::Single(_) => false,
        }
    }

    fn handle(&mut self, out: TaskOut, now: Instant) {
        let cancelled = self.limits.cancel.is_cancelled();
        match (&mut self.mode, out) {
            (Mode::Single(slot), TaskOut::Single(result)) => {
                *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
            }
            (Mode::Cycles(state), TaskOut::PrepareWorker(workload, result)) => match result {
                Ok(()) => {
                    workload.reset(now);
                    state.workload = Some(Rc::new(*workload));
                    let setup = &state.setup;
                    state.schedule = Some(Schedule::new(
                        setup.rate,
                        setup.rate_sine_amplitude,
                        setup.rate_sine_frequency,
                        now,
                    ));
                    state.next_sample_at = setup.sampling.period().map(|p| now + p);
                }
                Err(e) => {
                    state.failed = true;
                    if !cancelled {
                        state.setup.sink.push_error(state.setup.index, e);
                    }
                }
            },
            (Mode::Cycles(state), TaskOut::Cycle(result)) => {
                state.in_flight -= 1;
                state.chunk_count += 1;
                state.ticks += 1;
                if let Err(e) = result {
                    if !cancelled {
                        state.failed = true;
                        state.setup.sink.push_error(state.setup.index, e);
                    }
                }
            }
            _ => unreachable!("task output does not match the instance mode"),
        }
    }

    fn start_due_cycles(&mut self, now: Instant) -> bool {
        let Mode::Cycles(state) = &mut self.mode else {
            return false;
        };
        let (Some(workload), Some(schedule)) = (&state.workload, &mut state.schedule) else {
            return false;
        };
        if state.exhausted || state.failed || state.setup.sink.is_aborted() {
            return false;
        }
        let horizon = now + SCHEDULE_LOOKAHEAD;
        let budget = self.limits.rune_budget;
        let mut started = false;
        while state.in_flight < state.setup.concurrency {
            if self.limits.cancel.is_cancelled() {
                break;
            }
            let Some(scheduled) = schedule.due_by(now, horizon) else {
                break;
            };
            let Some(cycle) = state.setup.counter.next() else {
                state.exhausted = true;
                break;
            };
            let workload = Rc::clone(workload);
            let run = async move { TaskOut::Cycle(workload.run(cycle, scheduled).await) };
            let send_at = clock::ns_at(scheduled);
            if budget > 0 {
                self.tasks
                    .spawn_at(rune::runtime::budget::with(budget, run), send_at);
            } else {
                self.tasks.spawn_at(run, send_at);
            }
            state.in_flight += 1;
            started = true;
        }
        started
    }

    fn maybe_sample(&mut self, now: Instant) {
        let Mode::Cycles(state) = &mut self.mode else {
            return;
        };
        let Some(workload) = &state.workload else {
            return;
        };
        let sample_size = state.setup.sampling.count().unwrap_or(u64::MAX);
        let period_due = state.next_sample_at.is_some_and(|t| now >= t);
        if state.chunk_count >= sample_size || period_due {
            state
                .setup
                .sink
                .push_stats(state.setup.index, workload.take_stats(Instant::now()));
            state.chunk_count = 0;
            if let (Some(t), Some(p)) = (state.next_sample_at, state.setup.sampling.period()) {
                state.next_sample_at = Some(skip_missed(t, p, now));
            }
        }
    }

    /// Ends the instance once its work is over, or once the end-of-run drain timed out.
    fn finish_if_done(&mut self, now_ns: u64) -> Option<PumpStatus> {
        let done = match &self.mode {
            Mode::Single(_) => self.tasks.is_empty(),
            Mode::Cycles(state) => {
                let stopping = state.exhausted || state.failed || state.setup.sink.is_aborted();
                if stopping && !self.tasks.is_empty() {
                    let deadline = *self.drain_deadline_ns.get_or_insert_with(|| {
                        now_ns + self.limits.drain_timeout.as_nanos() as u64
                    });
                    if now_ns >= deadline {
                        let in_flight = self.reactor.in_flight();
                        self.limits
                            .end_of_run_timeouts
                            .fetch_add(in_flight as u64, Ordering::Relaxed);
                        self.tasks.clear();
                        self.reactor.clear();
                        true
                    } else {
                        false
                    }
                } else {
                    stopping
                }
            }
        };
        if !done {
            return None;
        }
        self.close(Instant::now());
        self.finished = Some(PumpStatus::Done);
        Some(PumpStatus::Done)
    }

    /// Hands the last sample to the phase and marks this instance closed.
    fn close(&mut self, end: Instant) {
        if let Mode::Cycles(state) = &mut self.mode {
            if let Some(workload) = &state.workload {
                state
                    .setup
                    .sink
                    .push_stats(state.setup.index, workload.take_stats(end));
            }
            state.setup.sink.close(state.setup.index);
        }
    }

    fn next_deadline(&self) -> Option<u64> {
        let timers = self.reactor.expire_timers();
        let held = self.reactor.next_send_at();
        let op_deadline = self.reactor.next_op_deadline();
        let mut schedule = None;
        let mut sample = None;
        if let Mode::Cycles(state) = &self.mode {
            if !state.exhausted && state.in_flight < state.setup.concurrency {
                schedule = state
                    .schedule
                    .as_ref()
                    .and_then(|s| s.next_due())
                    .map(|t| clock::ns_at(t.checked_sub(SCHEDULE_LOOKAHEAD).unwrap_or(t)));
            }
            sample = state.next_sample_at.map(clock::ns_at);
        }
        [
            timers,
            held,
            op_deadline,
            self.drain_deadline_ns,
            schedule,
            sample,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Chooses the form of op data: native values, rows and JSON values (the default), or
    /// the CQL bytes and JSON text of the C ABI.
    pub fn set_native(&self, native: bool) {
        self.reactor.set_native(native);
    }

    /// Number of ops the host received and has not answered.
    pub fn in_flight(&self) -> usize {
        self.reactor.in_flight()
    }
}

/// The next sample boundary after `now`, skipping the ones that passed without a pump.
fn skip_missed(mut t: Instant, period: Duration, now: Instant) -> Instant {
    while t <= now {
        t += period;
    }
    t
}
