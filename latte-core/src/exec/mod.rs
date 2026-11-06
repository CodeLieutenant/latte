//! Implementation of the main benchmarking loop

use std::f64::consts;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use crate::Interval;

pub mod cycle;
pub mod instance;
pub mod progress;
pub mod run;
pub mod runtime;
pub mod workload;

/// Infinite iterator returning floats that form a sinusoidal wave
pub struct InfiniteSinusoidalIterator {
    rate: f64,
    amplitude: f64,
    step: f64,
    start: Instant,
}

impl InfiniteSinusoidalIterator {
    pub fn new(rate: f64, amplitude: f64, frequency: f64) -> InfiniteSinusoidalIterator {
        let step = consts::PI * 2.0 * frequency;
        InfiniteSinusoidalIterator {
            rate,
            amplitude,
            step,
            start: Instant::now(),
        }
    }
}

impl Iterator for InfiniteSinusoidalIterator {
    type Item = f64;

    fn next(&mut self) -> Option<f64> {
        if self.amplitude == 0.0 {
            return Some(self.rate);
        }
        let elapsed = self.start.elapsed().as_secs_f64();
        let adjusted_rate = self.rate + self.amplitude * (self.step * elapsed).sin();
        Some(adjusted_rate)
    }
}

/// Start times of cycles under a rate limit that may follow a sine wave.
/// Without a rate limit every cycle may start at once.
pub struct Schedule {
    ticks: Option<InfiniteSinusoidalIterator>,
    next_tick: Instant,
}

impl Schedule {
    pub fn new(rate: Option<f64>, amplitude: f64, frequency: f64, start: Instant) -> Self {
        match rate {
            Some(rate) => Schedule {
                ticks: Some(InfiniteSinusoidalIterator::new(rate, amplitude, frequency)),
                next_tick: start + Duration::from_secs_f64(1.0 / rate),
            },
            None => Schedule {
                ticks: None,
                next_tick: start,
            },
        }
    }

    /// Returns the scheduled start time of the next cycle if it is due at `now`.
    /// A schedule that fell behind returns the missed ticks one by one, so the
    /// coordinated-omission correction sees the time each cycle should have started.
    pub fn due(&mut self, now: Instant) -> Option<Instant> {
        self.due_by(now, now)
    }

    /// Like [`Schedule::due`], but also returns ticks up to `horizon`, so a cycle can run its
    /// script ahead and have its first op ready at the scheduled time. Without a rate limit
    /// the tick is `now`.
    pub fn due_by(&mut self, now: Instant, horizon: Instant) -> Option<Instant> {
        let Some(ticks) = self.ticks.as_mut() else {
            return Some(now);
        };
        if horizon < self.next_tick {
            return None;
        }
        let tick = self.next_tick;
        let rate = ticks.next().unwrap_or(1.0);
        let period_ns = (1_000_000_000.0 / rate).max(1.0) as u64;
        self.next_tick += Duration::from_nanos(period_ns);
        Some(tick)
    }

    /// The time the next cycle becomes due, or `None` without a rate limit.
    pub fn next_due(&self) -> Option<Instant> {
        self.ticks.as_ref().map(|_| self.next_tick)
    }
}

/// Controls the intensity of requests sent to the server
pub struct ExecutionOptions {
    /// How long to execute
    pub duration: Interval,
    /// Range of the cycle counter
    pub cycle_range: (i64, i64),
    /// Maximum rate of requests in requests per second, `None` means no limit
    pub rate: Option<f64>,
    /// Rate sine wave amplitude
    pub rate_sine_amplitude: Option<f64>,
    /// Rate sine wave period
    pub rate_sine_period: Duration,
    /// Number of parallel threads of execution
    pub threads: NonZeroUsize,
    /// Number of outstanding async requests per each thread
    pub concurrency: NonZeroUsize,
}

#[cfg(test)]
mod test {
    use super::Schedule;
    use std::time::{Duration, Instant};

    #[test]
    fn schedule_without_rate_is_always_due() {
        let start = Instant::now();
        let mut schedule = Schedule::new(None, 0.0, 0.0, start);
        assert_eq!(schedule.due(start), Some(start));
        assert!(schedule.next_due().is_none());
    }

    #[test]
    fn schedule_emits_ticks_at_the_rate() {
        let start = Instant::now();
        let mut schedule = Schedule::new(Some(1000.0), 0.0, 0.0, start);
        assert!(schedule.due(start).is_none());
        let now = start + Duration::from_millis(10);
        let mut count = 0;
        while schedule.due(now).is_some() {
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn sinusoidal_amplitude_varies_tick_intervals() {
        let start = Instant::now();
        let mut schedule = Schedule::new(Some(1000.0), 500.0, 2.0, start);
        let mut ticks = Vec::new();
        let mut now = start;
        while ticks.len() < 50 {
            now += Duration::from_micros(100);
            while let Some(t) = schedule.due(now) {
                ticks.push(t);
            }
        }
        assert!(ticks.windows(2).all(|w| w[1] > w[0]));
    }
}
