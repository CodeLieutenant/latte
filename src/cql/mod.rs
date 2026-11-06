//! The CQL backend of the host: the `scylla` session and the ops it runs.

pub mod connect;
pub mod driver;

pub use connect::{connect, driver_settings};
pub use driver::Driver;
