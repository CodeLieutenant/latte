//! The native side of the CQL op path: script values serialize straight into the driver's
//! `RowWriter`, and rows decode straight into script objects.

use super::deserialize::RuneRow;
use super::serialize::RuneQueryParams;
use rune::Value;
use scylla_cql_core::deserialize::row::{ColumnIterator, DeserializeRow};
use scylla_cql_core::deserialize::{DeserializationError, TypeCheckError};
use scylla_cql_core::frame::response::result::ColumnSpec;
use scylla_cql_core::serialize::row::{RowSerializationContext, SerializeRow};
use scylla_cql_core::serialize::writers::RowWriter;
use scylla_cql_core::serialize::SerializationError;

/// Serializes script values typed by the bind markers in `ctx`.
pub(crate) fn serialize_params(
    params: Option<&Value>,
    ctx: &RowSerializationContext<'_>,
    writer: &mut RowWriter<'_>,
) -> Result<(), SerializationError> {
    RuneQueryParams::new(params).serialize(ctx, writer)
}

/// A row decoded straight into a script object. A native host reads a page with
/// `QueryRowsResult::rows::<CoreRow>()` and hands the objects to the core.
pub struct CoreRow(pub Value);

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for CoreRow {
    fn type_check(specs: &[ColumnSpec<'_>]) -> Result<(), TypeCheckError> {
        RuneRow::type_check(specs)
    }

    fn deserialize(row: ColumnIterator<'frame, 'metadata>) -> Result<Self, DeserializationError> {
        let RuneRow(obj) = RuneRow::deserialize(row)?;
        Value::new(obj)
            .map(CoreRow)
            .map_err(DeserializationError::new)
    }
}
