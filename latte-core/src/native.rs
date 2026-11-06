//! The native API for a Rust host that links the core as an rlib.
//!
//! Ops and completions travel as Rust values through [`crate::exec::instance::Instance::pump`],
//! the same pump, schedule, cancel and drain path the C ABI uses. What differs is the data:
//! * bound values come as [`Values::Native`], which serializes the script values straight
//!   into the driver's `RowWriter` through `SerializeRow`;
//! * a page can carry rows the host decoded with [`CoreRow`] straight into script objects;
//! * a DynamoDB request comes as [`Json::Request`]; downcast it to [`AlternatorRequest`]
//!   and build the SDK input from its typed fields. The response goes back as a parsed
//!   `serde_json::Value`.

pub use crate::ops::{AlternatorBody, Json, NativeValues, Values};
#[cfg(feature = "alternator")]
pub use crate::scripting::alternator::model::{
    AttributeValue as AlternatorAttribute, Blob as AlternatorBlob,
    RequestFields as AlternatorRequest,
};
#[cfg(feature = "cql")]
pub use crate::scripting::native::CoreRow;
