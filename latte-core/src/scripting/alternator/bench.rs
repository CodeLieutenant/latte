//! Entry points for the benchmarks of the DynamoDB JSON path. Not a stable API.

use super::model::{map_to_json, AttributeValue, Reply, Request};
use super::traits::into_output;
use crate::ops::AlternatorOp;
use std::collections::HashMap;

/// An item with `fields` string attributes of `len` bytes each, plus a key.
pub fn item(fields: usize, len: usize) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("key-1".to_string()));
    for i in 0..fields {
        item.insert(format!("field{i}"), AttributeValue::S("x".repeat(len)));
    }
    item
}

/// Builds the PutItem request JSON for `item`, as the `put` script function does.
pub fn put_item_json(item: &HashMap<String, AttributeValue>) -> String {
    Request::new(AlternatorOp::PutItem)
        .table_name("t")
        .set_item(Some(item.clone()))
        .to_json_string()
}

/// A Query response with `count` copies of `item`.
pub fn query_response(item: &HashMap<String, AttributeValue>, count: usize) -> String {
    let items: Vec<_> = (0..count).map(|_| map_to_json(item)).collect();
    serde_json::json!({ "Items": items, "Count": count }).to_string()
}

/// Parses a Query response into script objects and returns the item count.
pub fn parse_query_response(json: &str) -> usize {
    let reply: Reply = serde_json::from_str(json).expect("valid JSON");
    into_output(AlternatorOp::Query, reply)
        .expect("response parses")
        .0
        .len()
}
