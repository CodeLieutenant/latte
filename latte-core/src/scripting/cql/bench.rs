//! Entry points for the benchmarks of the CQL value path. Not a stable API.

use super::deserialize::RuneRow;
use super::serialize::RuneQueryParams;
use crate::ops::Rows;
use rune::Value;
use scylla_cql_core::deserialize::result::{RawRowIterator, TypedRowIterator};
use scylla_cql_core::deserialize::FrameSlice;
use scylla_cql_core::frame::response::result::ColumnSpec;
use scylla_cql_core::serialize::row::{RowSerializationContext, SerializedValues};

/// Serializes script values into CQL bytes, typed by `specs`, as an execute op does.
pub fn serialize(params: &Value, specs: &[ColumnSpec<'_>]) -> SerializedValues {
    let ctx = RowSerializationContext::from_specs(specs);
    SerializedValues::from_serializable(&ctx, &RuneQueryParams::new(Some(params)))
        .expect("values match their types")
}

/// Decodes every row of a page into script objects and returns the row count.
pub fn decode_rows(rows: &Rows) -> usize {
    let raw = RawRowIterator::new(
        rows.row_count as usize,
        &rows.specs,
        FrameSlice::new_borrowed(&rows.data),
    );
    TypedRowIterator::<RuneRow>::new(raw)
        .expect("rows type check")
        .inspect(|r| assert!(r.is_ok(), "row decodes"))
        .count()
}
