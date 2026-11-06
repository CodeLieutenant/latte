use crate::scripting::db_error::DbError;
use hdrhistogram::serialization::interval_log::IntervalLogWriterError;
use hdrhistogram::serialization::V2DeflateSerializeError;
use rune::alloc;
use rune::runtime::{AccessError, Object, RuntimeError, Value, VmError};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LatteError {
    #[error("Context data could not be serialized: {0}")]
    ContextDataEncode(String),

    #[error("Context data could not be deserialized: {0}")]
    ContextDataDecode(#[from] rmp_serde::decode::Error),

    #[error("Database error: {0}")]
    Database(#[source] Box<DbError>),

    #[error("Failed to read file {0:?}: {1}")]
    ScriptRead(PathBuf, #[source] rune::source::FromPathError),

    #[error("Failed to load script: {0}")]
    ScriptBuildError(#[from] rune::BuildError),

    #[error("Failed to execute script function {0}: {1}")]
    ScriptExecError(String, rune::runtime::VmError),

    #[error("Function {0} returned error: {1}")]
    FunctionResult(String, String),

    #[error("{0}")]
    Diagnostics(#[from] rune::diagnostics::EmitError),

    #[error("Failed to create output file {0:?}: {1}")]
    OutputFileCreate(PathBuf, std::io::Error),

    #[error("Failed to create log file {0:?}: {1}")]
    LogFileCreate(PathBuf, std::io::Error),

    #[error("Error writing HDR log: {0}")]
    HdrLogWrite(#[from] IntervalLogWriterError<V2DeflateSerializeError>),

    #[error("Failed to launch external editor {0}: {1}")]
    ExternalEditorLaunch(String, std::io::Error),

    #[error("{0}")]
    Worker(String),

    #[error("Invalid configuration: {0}")]
    Configuration(String),

    #[error("Memory allocation failure: {0}")]
    OutOfMemory(#[from] alloc::Error),

    #[error("Rune VmError: {0}")]
    RuneVmError(#[from] VmError),

    #[error("Rune AccessError: {0}")]
    RuneAccessError(#[from] AccessError),

    #[error("Rune runtime error: {0}")]
    RuneRuntimeError(#[from] RuntimeError),
}

impl From<DbError> for LatteError {
    fn from(err: DbError) -> Self {
        LatteError::Database(Box::new(err))
    }
}

impl From<Box<DbError>> for LatteError {
    fn from(err: Box<DbError>) -> Self {
        LatteError::Database(err)
    }
}

impl LatteError {
    /// Builds the context-data serialization error, naming the top-level
    /// `data` entries that fail to serialize - rune's serializer reports no
    /// path information, so without this the user cannot tell which value is
    /// at fault.
    pub fn context_data_encode(data: &Value, cause: &rmp_serde::encode::Error) -> Self {
        let offenders = unserializable_keys(data);
        let what = if offenders.is_empty() {
            cause.to_string()
        } else {
            format!("{} - {cause}", offenders.join(", "))
        };
        LatteError::ContextDataEncode(format!("{what}. Data may only hold plain values"))
    }
}

/// Names the top-level `data` entries that cannot be serialized. Serializes
/// into a sink because entries can be huge (e.g. a packed dataset) and only
/// the verdict is needed, not the output.
fn unserializable_keys(data: &Value) -> Vec<String> {
    let Ok(obj) = data.borrow_ref::<Object>() else {
        return Vec::new();
    };
    obj.iter()
        .filter(|(_, v)| rmp_serde::encode::write(&mut std::io::sink(), v).is_err())
        .map(|(k, _)| format!("data.{k}"))
        .collect()
}

pub type Result<T> = std::result::Result<T, LatteError>;
