//! One monotonic clock that the core and every host in the process share.
//!
//! Hosts read it through `latte_clock_ns` to stamp the start and end of each driver call,
//! so request latency and cycle scheduling use the same time base.
//!
//! The clock is [`quanta`]: it reads the CPU counter directly (`cntvct_el0` on aarch64, an
//! invariant time stamp counter on x86_64), calibrated once against `CLOCK_MONOTONIC`. A
//! read is one instruction plus a multiply, with no system call. Where no such counter
//! exists, `quanta` reads `CLOCK_MONOTONIC`.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

struct Clock {
    source: quanta::Clock,
    /// The raw counter value that reading 0 of the clock stands for.
    base_raw: u64,
    /// The instant that reading 0 of the clock stands for.
    base: Instant,
}

static CLOCK: OnceLock<Clock> = OnceLock::new();

fn clock() -> &'static Clock {
    CLOCK.get_or_init(|| {
        let source = quanta::Clock::new();
        let base_raw = source.raw();
        Clock {
            source,
            base_raw,
            base: Instant::now(),
        }
    })
}

/// Nanoseconds since the first read of the clock in this process.
#[inline]
pub fn now_ns() -> u64 {
    let clock = clock();
    clock
        .source
        .delta_as_nanos(clock.base_raw, clock.source.raw())
}

/// The current time as an [`Instant`] on this clock.
#[inline]
pub fn now() -> Instant {
    instant_at(now_ns())
}

/// The instant that a reading of [`now_ns`] stands for.
pub fn instant_at(ns: u64) -> Instant {
    clock().base + Duration::from_nanos(ns)
}

/// The reading of [`now_ns`] that stands for `instant`, or 0 for an instant before the base.
pub fn ns_at(instant: Instant) -> u64 {
    instant.saturating_duration_since(clock().base).as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn readings_never_go_back_across_threads() {
        let last = Arc::new(Mutex::new(0u64));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let last = Arc::clone(&last);
                std::thread::spawn(move || {
                    for _ in 0..50_000 {
                        let mut last = last.lock().unwrap();
                        let now = now_ns();
                        assert!(now >= *last, "{now} < {}", *last);
                        *last = now;
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
    }

    #[test]
    fn stays_within_one_ms_per_minute_of_the_monotonic_clock() {
        let start_ns = now_ns();
        let start = Instant::now();
        std::thread::sleep(Duration::from_secs(3));
        let clock_ns = now_ns() - start_ns;
        let monotonic_ns = start.elapsed().as_nanos() as u64;
        let drift_ns = clock_ns.abs_diff(monotonic_ns);
        // 1 ms per minute over 3 s is 50 us; allow 100 us more for the two pairs of reads.
        assert!(drift_ns < 150_000, "drift {drift_ns} ns over 3 s");
    }
}
