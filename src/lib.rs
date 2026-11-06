//! The Rust host of the Latte core, as a library for the `latte` binaries and benchmarks.

#[cfg(all(feature = "cql", feature = "alternator"))]
compile_error!("Enable exactly one of the features \"cql\" and \"alternator\".");

#[cfg(feature = "alternator")]
pub mod alternator;
#[cfg(feature = "cql")]
pub mod cql;
pub mod host;
