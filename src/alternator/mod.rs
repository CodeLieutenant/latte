//! The Alternator backend of the host: the DynamoDB client and the ops it runs.

mod address;
mod client;
pub mod connect;
pub mod ops;

pub use connect::{connect, driver_settings};
pub use ops::Driver;
