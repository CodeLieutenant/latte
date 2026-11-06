//! Latte as a library: the command line, Rune scripting, cycle scheduling, statistics and
//! reports. A driver host links it, runs the database ops it hands out and answers with
//! completions. See [`exec::run`] for the Rust API and [`ffi`] for the C ABI.

pub mod clock;
pub mod config;
pub mod error;
pub mod exec;
pub mod ffi;
pub mod local;
pub mod native;
pub mod ops;
pub mod report;
pub mod scripting;
pub mod stats;
pub mod version;

#[cfg(feature = "mimalloc")]
pub mod allocator;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: allocator::Allocator = allocator::Allocator;

#[cfg(all(feature = "jemalloc", not(feature = "mimalloc")))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub use config::Interval;
pub use error::Result;
pub use scylla_cql_core;
