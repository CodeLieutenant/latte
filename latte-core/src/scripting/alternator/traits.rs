//! Pagination and response parsing of DynamoDB requests.

use super::alternator_error::{AlternatorError, AlternatorErrorKind};
use super::model::{AttributeValue, Reply, Request};
use super::types::alternator_map_to_rune_object;
use crate::ops::AlternatorOp;
use rune::Value;
use serde_json::Value as Json;
use std::collections::HashMap;

/// What the next page of a paginated request continues from.
#[derive(Clone, Debug)]
pub(super) enum PaginationToken {
    /// `LastEvaluatedKey` of a Query or Scan page.
    LastEvaluatedKey(Json),
    /// `UnprocessedKeys` of a BatchGetItem response.
    UnprocessedKeys(Json),
    /// `UnprocessedItems` of a BatchWriteItem response.
    UnprocessedItems(Json),
}

pub(super) type AlternatorOutputResult =
    Result<(Vec<Value>, u64, Option<PaginationToken>), AlternatorError>;

fn non_empty(json: Option<Json>) -> Option<Json> {
    json.filter(|v| v.as_object().is_some_and(|o| !o.is_empty()))
}

fn items(
    list: Option<Vec<HashMap<String, AttributeValue>>>,
) -> Result<Vec<Value>, AlternatorError> {
    list.into_iter()
        .flatten()
        .map(alternator_map_to_rune_object)
        .collect()
}

/// Reads the items, their count and the pagination token from a parsed response.
pub(super) fn into_output(op: AlternatorOp, response: Reply) -> AlternatorOutputResult {
    match op {
        AlternatorOp::GetItem => match response.item {
            Some(item) => Ok((vec![alternator_map_to_rune_object(item)?], 1, None)),
            None => Ok((vec![], 0, None)),
        },
        AlternatorOp::Query | AlternatorOp::Scan => {
            let result = items(response.items)?;
            let len = result.len() as u64;
            let token =
                non_empty(response.last_evaluated_key).map(PaginationToken::LastEvaluatedKey);
            Ok((result, len, token))
        }
        AlternatorOp::BatchGetItem => {
            let mut result = Vec::new();
            for table_items in response
                .responses
                .into_iter()
                .flatten()
                .map(|(_, items)| items)
            {
                result.extend(items(Some(table_items))?);
            }
            let len = result.len() as u64;
            let token = non_empty(response.unprocessed_keys).map(PaginationToken::UnprocessedKeys);
            Ok((result, len, token))
        }
        AlternatorOp::BatchWriteItem => {
            let token =
                non_empty(response.unprocessed_items).map(PaginationToken::UnprocessedItems);
            Ok((vec![], 0, token))
        }
        _ => Ok((vec![], 0, None)),
    }
}

impl Request {
    /// True for the operations that return a page and a token for the next one.
    pub(super) fn has_pagination(&self) -> bool {
        matches!(
            self.op,
            AlternatorOp::Query
                | AlternatorOp::Scan
                | AlternatorOp::BatchGetItem
                | AlternatorOp::BatchWriteItem
        )
    }

    pub(super) fn get_limit_val(&self) -> Option<i32> {
        if matches!(self.op, AlternatorOp::Query | AlternatorOp::Scan) {
            self.get_limit()
        } else {
            None
        }
    }

    /// Continues the request from `token` and caps the page at `limit` items.
    pub(super) fn set_pagination(
        mut self,
        token: Option<PaginationToken>,
        limit: Option<i32>,
    ) -> Result<Self, AlternatorError> {
        match self.op {
            AlternatorOp::Query | AlternatorOp::Scan => {
                match token {
                    Some(PaginationToken::LastEvaluatedKey(key)) => {
                        self.fields_mut().exclusive_start_key = Some(key);
                    }
                    Some(other) => return Err(token_mismatch(self.op, other)),
                    None => {
                        self.fields_mut().exclusive_start_key = None;
                    }
                }
                if let Some(limit) = limit {
                    self = self.limit(limit);
                }
            }
            AlternatorOp::BatchGetItem => match token {
                Some(PaginationToken::UnprocessedKeys(keys)) => {
                    self.fields_mut().request_items = Some(keys);
                }
                Some(other) => return Err(token_mismatch(self.op, other)),
                None => {}
            },
            AlternatorOp::BatchWriteItem => match token {
                Some(PaginationToken::UnprocessedItems(items)) => {
                    self.fields_mut().request_items = Some(items);
                }
                Some(other) => return Err(token_mismatch(self.op, other)),
                None => {}
            },
            _ => {}
        }
        Ok(self)
    }
}

fn token_mismatch(op: AlternatorOp, token: PaginationToken) -> AlternatorError {
    AlternatorError::new(AlternatorErrorKind::Error(format!(
        "{} cannot continue from {token:?}",
        op.name()
    )))
}
