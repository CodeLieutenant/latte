use super::model::{map_from_json, map_to_json, AttributeValue, Request};
use super::traits::{into_output, PaginationToken};
use crate::config::ValidationStrategy;
use crate::ops::AlternatorOp;
use crate::scripting::functions_common::{extract_validation_args, ValidationArgs};
use crate::scripting::retry_error::handle_retry_error;

use super::alternator_error::{AlternatorError, AlternatorErrorKind};
use super::context::Context;
use super::types::*;
use rune::runtime::{Object, Ref, VmResult};
use rune::{ToValue, Value};
use serde_json::{json, Map, Value as Json};
use std::cmp::min;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::time::Duration;

/// Type of a key attribute in a table definition.
#[derive(Clone, Copy)]
enum ScalarAttributeType {
    N,
    S,
    B,
}

impl ScalarAttributeType {
    fn as_str(self) -> &'static str {
        match self {
            ScalarAttributeType::N => "N",
            ScalarAttributeType::S => "S",
            ScalarAttributeType::B => "B",
        }
    }
}

fn bad_input<T>(msg: impl Into<String>) -> Result<T, AlternatorError> {
    Err(AlternatorError::new(AlternatorErrorKind::BadInput(
        msg.into(),
    )))
}

fn check_invalid_params(
    params: &Object,
    function_name: &str,
    allowed_fields: &[&str],
) -> Result<(), AlternatorError> {
    for field in params.keys() {
        if !allowed_fields.contains(&field.as_str()) {
            return bad_input(format!(
                "Invalid parameter for function {}: {}. Allowed parameters: {:?}",
                function_name, field, allowed_fields
            ));
        }
    }
    Ok(())
}

/// Gets the name and type of a primary or sort key from a object.
fn extract_key_definition(
    object: &Object,
) -> Result<(String, ScalarAttributeType), AlternatorError> {
    check_invalid_params(object, "extract_key_definition", &["name", "type"])?;

    let key_name = if let Some(v) = object.get("name") {
        if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
            s.as_str().to_string()
        } else {
            return bad_input("'name' in key definition must be a string");
        }
    } else {
        return bad_input("Key definition object must have a 'name' field");
    };

    let key_type = if let Some(v) = object.get("type") {
        if let Ok(t) = v.borrow_ref::<rune::alloc::String>() {
            match t.as_str() {
                "N" => ScalarAttributeType::N,
                "S" => ScalarAttributeType::S,
                "B" => ScalarAttributeType::B,
                other => {
                    return bad_input(format!(
                        "Invalid key type: {}, only N, S, and B are allowed.",
                        other
                    ))
                }
            }
        } else {
            return bad_input("'type' in key definition must be a string");
        }
    } else {
        return bad_input("Key definition object must have a 'type' field");
    };

    Ok((key_name, key_type))
}

fn extract_attribute_names(object: &Object) -> Result<HashMap<String, String>, AlternatorError> {
    object
        .iter()
        .map(|(k, v)| {
            if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
                Ok((k.to_string(), s.as_str().to_string()))
            } else {
                bad_input("Attribute names must be strings")
            }
        })
        .collect::<Result<_, _>>()
}

/// Runs a paged request in one of the worker's call slots, so the script's future stays small.
async fn handle_request_with_pagination(
    ctx: &Context,
    builder: Request,
    auto_paginate: bool,
) -> Result<(Vec<Value>, Option<PaginationToken>), AlternatorError> {
    let owned = ctx.shallow_clone();
    let calls = Rc::clone(&ctx.calls);
    calls
        .spawn(async move { paginate(&owned, builder, auto_paginate).await })
        .await
}

async fn paginate(
    ctx: &Context,
    builder: Request,
    auto_paginate: bool,
) -> Result<(Vec<Value>, Option<PaginationToken>), AlternatorError> {
    let mut token: Option<PaginationToken> = None;
    let mut current_attempt_num = 0;
    let mut all_pages_duration = Duration::ZERO;
    let mut all_items = Vec::new();
    let mut total_item_count = 0;
    let query_limit = builder.get_limit_val();

    // Retries of a request that does not page share its fields; a page copies them once to
    // set its start key and size.
    while current_attempt_num <= ctx.retry_number {
        let current_builder = if builder.has_pagination() {
            let page_size = match query_limit {
                Some(limit) => min(ctx.get_page_size() as i32, limit - total_item_count as i32),
                None => ctx.get_page_size() as i32,
            };
            builder
                .clone()
                .set_pagination(token.clone(), Some(page_size))?
        } else {
            builder.clone()
        };

        ctx.stats.try_lock().unwrap().start_request();
        let (resp, duration) = match ctx.send_page(&current_builder).await? {
            Ok((reply, duration)) => (into_output(current_builder.op, reply), duration),
            Err(e) => (Err(AlternatorError::from(e)), Duration::ZERO),
        };

        match resp {
            Ok((page_items, item_count, next_token)) => {
                all_pages_duration += duration;
                all_items.extend(page_items);
                total_item_count += item_count;
                token = next_token;

                if let Some(limit) = query_limit {
                    if total_item_count as i32 >= limit {
                        ctx.stats
                            .try_lock()
                            .unwrap()
                            .complete_request(all_pages_duration, total_item_count);
                        return Ok((all_items, token));
                    }
                }

                if token.is_some() && builder.has_pagination() {
                    if auto_paginate {
                        current_attempt_num = 0; // reset retries for next page
                        continue;
                    } else {
                        ctx.stats
                            .try_lock()
                            .unwrap()
                            .complete_request(all_pages_duration, total_item_count);
                        return Ok((all_items, token));
                    }
                }

                ctx.stats
                    .try_lock()
                    .unwrap()
                    .complete_request(all_pages_duration, total_item_count);
                return Ok((all_items, token));
            }
            Err(e) => {
                let current_error = e;
                Box::pin(handle_retry_error(ctx, current_attempt_num, current_error)).await;
                current_attempt_num += 1;
                continue; // try again the same page
            }
        };
    }
    Err(AlternatorError::query_retries_exceeded(ctx.retry_number))
}

async fn handle_request(ctx: &Context, builder: Request) -> Result<Vec<Value>, AlternatorError> {
    Ok(handle_request_with_pagination(ctx, builder, true).await?.0)
}

/// Runs a validated request in one of the worker's call slots, so the script's future stays
/// small.
async fn handle_request_with_validation(
    ctx: &Context,
    builder: Request,
    validation: Option<ValidationArgs>,
    operation_name: &'static str,
) -> Result<Vec<Value>, AlternatorError> {
    let owned = ctx.shallow_clone();
    let calls = Rc::clone(&ctx.validated_calls);
    calls
        .spawn(async move { validate(&owned, builder, validation, operation_name).await })
        .await
}

async fn validate(
    ctx: &Context,
    builder: Request,
    validation: Option<ValidationArgs>,
    operation_name: &str,
) -> Result<Vec<Value>, AlternatorError> {
    let mut current_attempt_num: u64 = 0;
    loop {
        let (result, _) = handle_request_with_pagination(ctx, builder.clone(), true).await?;

        let validation = match validation {
            None => return Ok(result),
            Some(ref v) => v,
        };

        let item_count = result.len() as u64;
        if item_count >= validation.expected_min && item_count <= validation.expected_max {
            return Ok(result);
        }

        let current_error = AlternatorError::new(AlternatorErrorKind::ValidationError(format!(
            "{operation_name} returned {item_count} items, expected between {} and {} {}",
            validation.expected_min, validation.expected_max, validation.custom_err_msg
        )));

        match ctx.validation_strategy {
            ValidationStrategy::Retry => {
                if current_attempt_num >= ctx.retry_number {
                    return Err(current_error);
                }
                Box::pin(handle_retry_error(ctx, current_attempt_num, current_error)).await;
                current_attempt_num += 1;
            }
            ValidationStrategy::FailFast => {
                return Err(current_error);
            }
            ValidationStrategy::Ignore => {
                Box::pin(handle_retry_error(ctx, current_attempt_num, current_error)).await;
                return Ok(result);
            }
        }
    }
}

fn format_batch_result(
    items: Vec<Value>,
    token: Option<PaginationToken>,
    auto_paginate: bool,
    with_result: bool,
) -> Result<Value, AlternatorError> {
    if !with_result {
        return Ok(Value::from(()));
    }

    if auto_paginate {
        return Ok(items.to_value()?);
    }

    let mut res_map = HashMap::new();

    res_map.insert("items".to_string(), items.to_value()?);

    match token {
        Some(PaginationToken::UnprocessedKeys(u_keys)) => {
            let mut unprocessed_map: HashMap<String, Value> = HashMap::new();
            for (table_name, keys_attr) in u_keys.as_object().into_iter().flatten() {
                let keys: Vec<Value> = keys_attr
                    .get("Keys")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .map(|key| alternator_map_to_rune_object(map_from_json(key)?))
                    .collect::<Result<_, AlternatorError>>()?;
                unprocessed_map.insert(table_name.clone(), keys.to_value()?);
            }
            res_map.insert(
                "unprocessed_keys".to_string(),
                hashmap_to_rune_object(unprocessed_map)?,
            );
        }
        Some(PaginationToken::UnprocessedItems(u_items)) => {
            let mut unprocessed_map: HashMap<String, Value> = HashMap::new();
            for (table_name, reqs) in u_items.as_object().into_iter().flatten() {
                let requests: Vec<Value> = reqs
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|req| {
                        let mut req_map = HashMap::new();
                        if let Some(item) = req.get("PutRequest").and_then(|p| p.get("Item")) {
                            req_map.insert("type".to_string(), "put".to_value()?);
                            req_map.insert(
                                "item".to_string(),
                                alternator_map_to_rune_object(map_from_json(item)?)?,
                            );
                        } else if let Some(key) =
                            req.get("DeleteRequest").and_then(|d| d.get("Key"))
                        {
                            req_map.insert("type".to_string(), "delete".to_value()?);
                            req_map.insert(
                                "key".to_string(),
                                alternator_map_to_rune_object(map_from_json(key)?)?,
                            );
                        }
                        hashmap_to_rune_object(req_map)
                    })
                    .collect::<Result<_, AlternatorError>>()?;
                unprocessed_map.insert(table_name.clone(), requests.to_value()?);
            }
            res_map.insert(
                "unprocessed_items".to_string(),
                hashmap_to_rune_object(unprocessed_map)?,
            );
        }
        _ => {}
    }

    hashmap_to_rune_object(res_map)
}

/// Creates a new table.
///
/// # Arguments
/// * `table_name` - The name of the table to create.
/// * `params` - Table definition parameters. Can be a string (defining just the primary key name) or an object containing:
///   - `primary_key`: The primary key definition. Can be a string (name) or an object with `name` and `type`.
///   - `sort_key`: The sort key definition (optional). Can be a string (name) or an object with `name` and `type`.
#[rune::function(instance)]
pub async fn create_table(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    params: Value,
) -> Result<(), AlternatorError> {
    if let Ok(o) = params.borrow_ref::<Object>() {
        check_invalid_params(o.deref(), "create_table", &[PRIMARY_KEY_KEY, SORT_KEY_KEY])?;
    }

    // Extract primary key definition
    let (pk_name, pk_type) = if let Ok(s) = params.borrow_ref::<rune::alloc::String>() {
        (s.as_str().to_string(), ScalarAttributeType::S)
    } else if let Ok(o) = params.borrow_ref::<Object>() {
        match o.get(PRIMARY_KEY_KEY) {
            Some(v) if v.borrow_ref::<rune::alloc::String>().is_ok() => (
                v.borrow_ref::<rune::alloc::String>()
                    .unwrap()
                    .as_str()
                    .to_string(),
                ScalarAttributeType::S,
            ),
            Some(v) => {
                if let Ok(pk_obj) = v.borrow_ref::<Object>() {
                    extract_key_definition(&pk_obj)?
                } else {
                    return bad_input(format!("Invalid '{}' object in params", PRIMARY_KEY_KEY));
                }
            }
            _ => return bad_input(format!("Invalid '{}' object in params", PRIMARY_KEY_KEY)),
        }
    } else {
        return bad_input("Params must be a string or an object");
    };

    // Extract sort key definition if present
    let sk = if let Ok(o) = params.borrow_ref::<Object>() {
        match o.get(SORT_KEY_KEY) {
            Some(v) if v.borrow_ref::<rune::alloc::String>().is_ok() => Some((
                v.borrow_ref::<rune::alloc::String>()
                    .unwrap()
                    .as_str()
                    .to_string(),
                ScalarAttributeType::S,
            )),
            Some(v) => {
                if let Ok(sk_obj) = v.borrow_ref::<Object>() {
                    Some(extract_key_definition(&sk_obj)?)
                } else {
                    return bad_input(format!("Invalid '{}' object in params", SORT_KEY_KEY));
                }
            }
            None => None,
        }
    } else {
        None
    };

    let mut key_schema = vec![json!({"AttributeName": pk_name, "KeyType": "HASH"})];
    let mut attribute_definitions =
        vec![json!({"AttributeName": pk_name, "AttributeType": pk_type.as_str()})];
    if let Some((sk_name, sk_type)) = sk {
        key_schema.push(json!({"AttributeName": sk_name, "KeyType": "RANGE"}));
        attribute_definitions
            .push(json!({"AttributeName": sk_name, "AttributeType": sk_type.as_str()}));
    }

    let request = Request::new(AlternatorOp::CreateTable)
        .table_name(table_name.deref())
        .set("BillingMode", "PAY_PER_REQUEST")
        .set("KeySchema", key_schema)
        .set("AttributeDefinitions", attribute_definitions);
    ctx.send(&request).await??;

    wait_until_table_exists(&ctx, table_name.deref(), Duration::from_secs(15)).await?;

    Ok(())
}

/// Deletes a table.
#[rune::function(instance)]
pub async fn delete_table(ctx: Ref<Context>, table_name: Ref<str>) -> Result<(), AlternatorError> {
    let request = Request::new(AlternatorOp::DeleteTable).table_name(table_name.deref());
    ctx.send(&request).await??;

    Ok(())
}

/// Puts an item into the table.
///
/// # Arguments
/// * `table_name` - The name of the table.
/// * `item` - The item to insert. An object where keys are attribute names and values are attribute values.
/// * `options` - Optional parameters object:
///   - `condition_expression`: A condition that must be satisfied for the operation to succeed.
///   - `attribute_names`: A map of attribute name placeholders (starting with #) to actual names.
///   - `attribute_values`: A map of attribute value placeholders (starting with :) to values.
#[rune::function(instance)]
pub async fn put(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    item: Ref<Object>,
    options: Value,
) -> Result<(), AlternatorError> {
    let mut builder = Request::new(AlternatorOp::PutItem)
        .table_name(table_name.deref())
        .set_item(Some(rune_object_to_alternator_map(&item)?));

    if let Ok(opts) = options.borrow_ref::<Object>() {
        check_invalid_params(
            opts.deref(),
            "put",
            &[
                CONDITION_EXPRESSION_KEY,
                ATTRIBUTE_NAMES_KEY,
                ATTRIBUTE_VALUES_KEY,
            ],
        )?;

        if let Some(condition_expression) = opts.get(CONDITION_EXPRESSION_KEY) {
            if let Ok(ce_str) = condition_expression.borrow_ref::<rune::alloc::String>() {
                builder = builder.condition_expression(ce_str.as_str().to_string());
            } else {
                return bad_input(format!("'{}' must be a string", CONDITION_EXPRESSION_KEY));
            }
        }
        if let Some(attr_names) = opts.get(ATTRIBUTE_NAMES_KEY) {
            if let Ok(attr_names_obj) = attr_names.borrow_ref::<Object>() {
                builder = builder.set_expression_attribute_names(Some(extract_attribute_names(
                    &attr_names_obj,
                )?));
            } else {
                return bad_input(format!("'{}' must be an object", ATTRIBUTE_NAMES_KEY));
            }
        }

        if let Some(attr_values) = opts.get(ATTRIBUTE_VALUES_KEY) {
            if let Ok(attr_values_obj) = attr_values.borrow_ref::<Object>() {
                builder = builder.set_expression_attribute_values(Some(
                    rune_object_to_alternator_map(&attr_values_obj)?,
                ));
            } else {
                return bad_input(format!("'{}' must be an object", ATTRIBUTE_VALUES_KEY));
            }
        }
    }

    handle_request(&ctx, builder).await?;

    Ok(())
}

/// Deletes an item from the table.
///
/// # Arguments
/// * `table_name` - The name of the table.
/// * `key` - The primary key of the item to delete. An object containing the partition key
///   (and sort key if the table has one).
/// * `options` - Optional parameters object:
///   - `condition_expression`: A condition that must be satisfied for the operation to succeed.
///   - `attribute_names`: A map of attribute name placeholders (starting with #) to actual names.
///   - `attribute_values`: A map of attribute value placeholders (starting with :) to values.
#[rune::function(instance)]
pub async fn delete(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    key: Ref<Object>,
    options: Value,
) -> Result<(), AlternatorError> {
    let mut builder = Request::new(AlternatorOp::DeleteItem)
        .table_name(table_name.deref())
        .set_key(Some(rune_object_to_alternator_map(&key)?));

    if let Ok(opts) = options.borrow_ref::<Object>() {
        check_invalid_params(
            opts.deref(),
            "delete",
            &[
                CONDITION_EXPRESSION_KEY,
                ATTRIBUTE_NAMES_KEY,
                ATTRIBUTE_VALUES_KEY,
            ],
        )?;

        if let Some(condition_expression) = opts.get(CONDITION_EXPRESSION_KEY) {
            if let Ok(ce_str) = condition_expression.borrow_ref::<rune::alloc::String>() {
                builder = builder.condition_expression(ce_str.as_str().to_string());
            } else {
                return bad_input(format!("'{}' must be a string", CONDITION_EXPRESSION_KEY));
            }
        }
        if let Some(attr_names) = opts.get(ATTRIBUTE_NAMES_KEY) {
            if let Ok(attr_names_obj) = attr_names.borrow_ref::<Object>() {
                builder = builder.set_expression_attribute_names(Some(extract_attribute_names(
                    &attr_names_obj,
                )?));
            } else {
                return bad_input(format!("'{}' must be an object", ATTRIBUTE_NAMES_KEY));
            }
        }

        if let Some(attr_values) = opts.get(ATTRIBUTE_VALUES_KEY) {
            if let Ok(attr_values_obj) = attr_values.borrow_ref::<Object>() {
                builder = builder.set_expression_attribute_values(Some(
                    rune_object_to_alternator_map(&attr_values_obj)?,
                ));
            } else {
                return bad_input(format!("'{}' must be an object", ATTRIBUTE_VALUES_KEY));
            }
        }
    }

    handle_request(&ctx, builder).await?;

    Ok(())
}

/// Gets an item from the table.
///
/// The `options` object can be replaced with `()` if no options are needed.
///
/// If `with_result` is set to true, an `Option<Value>` containing the item if present is returned.
/// Otherwise the unit value is returned.
///
/// # Arguments
/// * `table_name` - The name of the table.
/// * `key` - The primary key of the item to get. An object containing the partition key
///   (and sort key if the table has one).
/// * `options` - Optional parameters object:
///   - `consistent_read`: Boolean to enable consistent read (default: false).
///   - `projection_expression`: A string that identifies the attributes to retrieve (optional).
///   - `attribute_names`: A map of attribute name placeholders (starting with #) to actual names (optional).
///   - `with_result`: If true, the result item is returned (default: false).
#[rune::function(instance)]
pub async fn get(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    key: Ref<Object>,
    options: Value,
) -> Result<Value, AlternatorError> {
    let mut builder = Request::new(AlternatorOp::GetItem)
        .table_name(table_name.deref())
        .set_key(Some(rune_object_to_alternator_map(&key)?));

    if let Ok(opts) = options.borrow_ref::<Object>() {
        check_invalid_params(
            opts.deref(),
            "get",
            &[
                CONSISTENT_READ_KEY,
                PROJECTION_EXPRESSION_KEY,
                ATTRIBUTE_NAMES_KEY,
                WITH_RESULT_KEY,
            ],
        )?;

        if let Some(v) = opts.get(CONSISTENT_READ_KEY) {
            builder = builder.consistent_read(match v.as_bool() {
                Ok(b) => b,
                _ => return bad_input(format!("'{}' must be a boolean", CONSISTENT_READ_KEY)),
            });
        }
        if let Some(proj) = opts.get(PROJECTION_EXPRESSION_KEY) {
            if let Ok(s) = proj.borrow_ref::<rune::alloc::String>() {
                builder = builder.projection_expression(s.as_str().to_string());
            } else {
                return bad_input(format!("'{}' must be a string", PROJECTION_EXPRESSION_KEY));
            }
        }
        if let Some(attr_names) = opts.get(ATTRIBUTE_NAMES_KEY) {
            if let Ok(obj) = attr_names.borrow_ref::<Object>() {
                builder =
                    builder.set_expression_attribute_names(Some(extract_attribute_names(&obj)?));
            } else {
                return bad_input(format!("'{}' must be an object", ATTRIBUTE_NAMES_KEY));
            }
        }
    }

    let result = handle_request(&ctx, builder).await?;

    if let Ok(opts) = options.borrow_ref::<Object>() {
        if let Some(v) = opts.get(WITH_RESULT_KEY) {
            if let Ok(b) = v.as_bool() {
                if b {
                    return Ok(result.into_iter().next().to_value()?);
                }
            } else {
                return bad_input(format!("'{}' must be a boolean", WITH_RESULT_KEY));
            }
        }
    }

    Ok(Value::from(()))
}

/// Updates an item in the table.
///
/// # Arguments
/// * `table_name` - The name of the table.
/// * `key` - The primary key of the item to update. An object containing the partition key
///   (and sort key if the table has one).
/// * `params` - Parameters for the update operation. An object containing:
///   - `update`: The update expression string.
///   - `condition_expression`: A condition that must be satisfied for the operation to succeed.
///   - `attribute_names`: A map of attribute name placeholders (starting with #) to actual names.
///   - `attribute_values`: A map of attribute value placeholders (starting with :) to values.
#[rune::function(instance)]
pub async fn update(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    key: Ref<Object>,
    params: Ref<Object>,
) -> Result<(), AlternatorError> {
    let mut builder = Request::new(AlternatorOp::UpdateItem)
        .table_name(table_name.deref())
        .set_key(Some(rune_object_to_alternator_map(&key)?));

    check_invalid_params(
        &params,
        "update",
        &[
            UPDATE_EXPRESSION_KEY,
            CONDITION_EXPRESSION_KEY,
            ATTRIBUTE_NAMES_KEY,
            ATTRIBUTE_VALUES_KEY,
        ],
    )?;

    if let Some(v) = params.get(UPDATE_EXPRESSION_KEY) {
        if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
            builder = builder.update_expression(s.as_str().to_string());
        } else {
            return bad_input(format!("'{}' must be a string", UPDATE_EXPRESSION_KEY));
        }
    }

    if let Some(v) = params.get(ATTRIBUTE_NAMES_KEY) {
        if let Ok(obj) = v.borrow_ref::<Object>() {
            builder = builder.set_expression_attribute_names(Some(extract_attribute_names(&obj)?));
        } else {
            return bad_input(format!("'{}' must be an object", ATTRIBUTE_NAMES_KEY));
        }
    }
    if let Some(v) = params.get(CONDITION_EXPRESSION_KEY) {
        if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
            builder = builder.condition_expression(s.as_str().to_string());
        } else {
            return bad_input(format!("'{}' must be a string", CONDITION_EXPRESSION_KEY));
        }
    }

    if let Some(v) = params.get(ATTRIBUTE_VALUES_KEY) {
        if let Ok(obj) = v.borrow_ref::<Object>() {
            builder =
                builder.set_expression_attribute_values(Some(rune_object_to_alternator_map(&obj)?));
        } else {
            return bad_input(format!("'{}' must be an object", ATTRIBUTE_VALUES_KEY));
        }
    }

    handle_request(&ctx, builder).await?;

    Ok(())
}

/// Batch retrieves items from one or multiple tables.
///
/// If `with_result` is set to true, the retrieved items are returned.
/// Otherwise, the unit value is returned.
///
/// # Arguments
/// * `requests` - An object mapping table names to either:
///   - A list of primary key objects (simple form), or
///   - An object containing:
///     - `keys`: A list of primary key objects (required).
///     - `projection_expression`: A string that identifies the attributes to retrieve (optional).
///     - `attribute_names`: A map of attribute name placeholders (starting with #) to actual names (optional).
/// * `options` - Optional parameters. An object containing:
///   - `consistent_read`: Boolean to enable consistent read for all tables (default: false).
///   - `with_result`: If true, the retrieved items are returned (default: false).
///   - `get_unprocessed`: If true, disables auto-pagination. When `with_result: true` returns an object with `items` and `unprocessed_keys`.
#[rune::function(instance)]
pub async fn batch_get_item(
    ctx: Ref<Context>,
    requests: Ref<Object>,
    options: Value,
) -> Result<Value, AlternatorError> {
    let mut with_result = false;
    let mut get_unprocessed = false;
    let mut consistent_read = false;

    if let Ok(opts_ref) = options.borrow_ref::<Object>() {
        check_invalid_params(
            opts_ref.deref(),
            "batch_get_item",
            &[CONSISTENT_READ_KEY, WITH_RESULT_KEY, GET_UNPROCESSED_KEY],
        )?;

        if let Some(v) = opts_ref.get(CONSISTENT_READ_KEY) {
            if let Ok(c) = v.as_bool() {
                consistent_read = c;
            } else {
                return bad_input(format!("'{}' must be a boolean", CONSISTENT_READ_KEY));
            }
        }
        if let Some(v) = opts_ref.get(WITH_RESULT_KEY) {
            if let Ok(w) = v.as_bool() {
                with_result = w;
            } else {
                return bad_input(format!("'{}' must be a boolean", WITH_RESULT_KEY));
            }
        }
        if let Some(v) = opts_ref.get(GET_UNPROCESSED_KEY) {
            if let Ok(u) = v.as_bool() {
                get_unprocessed = u;
            } else {
                return bad_input(format!("'{}' must be a boolean", GET_UNPROCESSED_KEY));
            }
        }
    }

    let request_items: Map<String, Json> = requests
        .iter()
        .map(|(table_name, table_val)| {
            // Each table entry can be either a plain list of keys or an object
            // with keys, projection_expression, and attribute_names.
            let (keys_list, projection, attr_names) =
                if let Ok(keys_vec) = table_val.borrow_ref::<rune::runtime::Vec>() {
                    let keys = keys_vec
                        .iter()
                        .map(|key_val| {
                            if let Ok(key_obj) = key_val.borrow_ref::<Object>() {
                                rune_object_to_alternator_map(&key_obj)
                            } else {
                                bad_input("Each key in the keys list must be an object")
                            }
                        })
                        .collect::<Result<_, _>>()?;
                    (keys, None, None)
                } else if let Ok(obj_ref) = table_val.borrow_ref::<Object>() {
                    check_invalid_params(
                        &obj_ref,
                        "batch_get_item request object",
                        &["keys", PROJECTION_EXPRESSION_KEY, ATTRIBUTE_NAMES_KEY],
                    )?;

                    let keys = if let Some(v) = obj_ref.get("keys") {
                        if let Ok(keys_vec) = v.borrow_ref::<rune::runtime::Vec>() {
                            keys_vec
                                .iter()
                                .map(|key_val| {
                                    if let Ok(key_obj) = key_val.borrow_ref::<Object>() {
                                        rune_object_to_alternator_map(&key_obj)
                                    } else {
                                        bad_input("Each key in the keys list must be an object")
                                    }
                                })
                                .collect::<Result<_, _>>()?
                        } else {
                            return bad_input("Table object must have a 'keys' list");
                        }
                    } else {
                        return bad_input("Table object must have a 'keys' list");
                    };

                    let projection = if let Some(v) = obj_ref.get("projection_expression") {
                        if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
                            Some(s.as_str().to_string())
                        } else {
                            return bad_input(format!(
                                "'{}' must be a string",
                                PROJECTION_EXPRESSION_KEY
                            ));
                        }
                    } else {
                        None
                    };

                    let attr_names = if let Some(v) = obj_ref.get(ATTRIBUTE_NAMES_KEY) {
                        if let Ok(names) = v.borrow_ref::<Object>() {
                            Some(extract_attribute_names(&names)?)
                        } else {
                            return bad_input("attribute_names must be an object");
                        }
                    } else {
                        None
                    };

                    (keys, projection, attr_names)
                } else {
                    return bad_input(
                        "Each table's requests must be a list of keys or an object with 'keys'",
                    );
                };

            let keys_list: Vec<HashMap<String, AttributeValue>> = keys_list;
            let mut keys_and_attributes = Map::new();
            keys_and_attributes.insert(
                "Keys".to_string(),
                Json::Array(keys_list.iter().map(map_to_json).collect()),
            );
            keys_and_attributes.insert("ConsistentRead".to_string(), Json::from(consistent_read));
            if let Some(proj) = projection {
                keys_and_attributes.insert("ProjectionExpression".to_string(), Json::from(proj));
            }
            if let Some(names) = attr_names {
                keys_and_attributes.insert(
                    "ExpressionAttributeNames".to_string(),
                    Json::Object(names.into_iter().map(|(k, v)| (k, Json::from(v))).collect()),
                );
            }
            let keys_and_attributes = Json::Object(keys_and_attributes);

            Ok((table_name.to_string(), keys_and_attributes))
        })
        .collect::<Result<_, AlternatorError>>()?;

    let builder =
        Request::new(AlternatorOp::BatchGetItem).set("RequestItems", Json::Object(request_items));
    let (result_items, token) =
        handle_request_with_pagination(&ctx, builder, !get_unprocessed).await?;
    format_batch_result(result_items, token, !get_unprocessed, with_result)
}

/// Batch writes items to one or multiple tables.
///
/// # Arguments
/// * `requests` - An object mapping table names to a list of write requests. Each request is an object containing:
///   - `type`: Either "put" or "delete".
///   - `item`: For put requests, the item object to insert.
///   - `key`: For delete requests, the key object to delete.
/// * `options` - Optional parameters. An object containing:
///   - `get_unprocessed`: If true, disables auto-pagination. Returns an object with `unprocessed_items`.
#[rune::function(instance)]
pub async fn batch_write_item(
    ctx: Ref<Context>,
    requests: Ref<Object>,
    options: Value,
) -> Result<Value, AlternatorError> {
    let mut get_unprocessed = false;

    if let Ok(opts_ref) = options.borrow_ref::<Object>() {
        check_invalid_params(opts_ref.deref(), "batch_write_item", &[GET_UNPROCESSED_KEY])?;
        if let Some(v) = opts_ref.get(GET_UNPROCESSED_KEY) {
            if let Ok(u) = v.as_bool() {
                get_unprocessed = u;
            } else {
                return bad_input(format!("'{}' must be a boolean", GET_UNPROCESSED_KEY));
            }
        }
    }

    let request_items: Map<String, Json> = requests
        .iter()
        .map(|(table_name, reqs_val)| {
            let reqs_vec = if let Ok(vec) = reqs_val.borrow_ref::<rune::runtime::Vec>() {
                vec
            } else {
                return bad_input("Each table's requests must be a list of write requests");
            };

            let writes = reqs_vec
                .iter()
                .map(|req_val| {
                    let req_ref = if let Ok(obj) = req_val.borrow_ref::<Object>() {
                        check_invalid_params(
                            obj.deref(),
                            "write_request",
                            &["type", "item", "key"],
                        )?;
                        obj
                    } else {
                        return bad_input("Each write request must be an object");
                    };

                    let req_type = match req_ref.get("type") {
                        Some(t) if t.borrow_ref::<rune::alloc::String>().is_ok() => t
                            .borrow_ref::<rune::alloc::String>()
                            .unwrap()
                            .as_str()
                            .to_string(),
                        _ => {
                            return bad_input(
                                "Write request must have a 'type' field (put or delete)",
                            );
                        }
                    };

                    match req_type.as_str() {
                        "put" => {
                            let item_obj = match req_ref.get("item") {
                                Some(v) if v.borrow_ref::<Object>().is_ok() => {
                                    v.borrow_ref::<Object>().unwrap()
                                }
                                _ => {
                                    return bad_input("Put request must have an 'item' field");
                                }
                            };

                            let item_map = rune_object_to_alternator_map(&item_obj)?;

                            Ok(json!({"PutRequest": {"Item": map_to_json(&item_map)}}))
                        }
                        "delete" => {
                            let key_obj = match req_ref.get("key") {
                                Some(v) if v.borrow_ref::<Object>().is_ok() => {
                                    v.borrow_ref::<Object>().unwrap()
                                }
                                _ => {
                                    return bad_input("Delete request must have a 'key' field");
                                }
                            };

                            let key_map = rune_object_to_alternator_map(&key_obj)?;

                            Ok(json!({"DeleteRequest": {"Key": map_to_json(&key_map)}}))
                        }
                        _ => bad_input(format!(
                            "Invalid request type: {}, must be 'put' or 'delete'",
                            req_type
                        )),
                    }
                })
                .collect::<Result<Vec<Json>, _>>()?;

            Ok((table_name.to_string(), Json::Array(writes)))
        })
        .collect::<Result<_, AlternatorError>>()?;

    let builder =
        Request::new(AlternatorOp::BatchWriteItem).set("RequestItems", Json::Object(request_items));
    let (result_items, token) =
        handle_request_with_pagination(&ctx, builder, !get_unprocessed).await?;

    format_batch_result(result_items, token, !get_unprocessed, get_unprocessed)
}

/// Queries items from the table.
///
/// Unlike `get`, which retrieves a single item by its exact primary key,
/// `query` retrieves multiple items that share the same partition key.
/// It also allows specifying conditions on the sort key and filtering the results based on non-key attributes.
///
/// If `with_result` is set to true, the query result is returned as a `Vec<Object>`.
/// Otherwise, the unit value is returned.
///
/// # Arguments
/// * `table_name` - The name of the table.
/// * `params` - Parameters for the query operation. An object containing:
///   - `query`: The key condition expression string (required).
///   - `filter`: The filter expression string (optional, applied after query).
///   - `projection_expression`: A string that identifies the attributes to retrieve (optional).
///   - `attribute_names`: A map of attribute name placeholders (starting with #) to actual names.
///   - `attribute_values`: A map of attribute value placeholders (starting with :) to values.
///   - `consistent_read`: Boolean to enable consistent read (default: false).
///   - `limit`: The maximum number of items to evaluate (optional).
///   - `validation`: An optional item count validation. Look at [extract_validation_args] for details.
///   - `with_result`: If true, the query result is returned (default: false).
#[rune::function(instance)]
pub async fn query(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    params: Ref<Object>,
) -> Result<Value, AlternatorError> {
    let mut builder = Request::new(AlternatorOp::Query).table_name(table_name.deref());

    check_invalid_params(
        &params,
        "query",
        &[
            QUERY_EXPRESSION_KEY,
            FILTER_EXPRESSION_KEY,
            PROJECTION_EXPRESSION_KEY,
            ATTRIBUTE_NAMES_KEY,
            ATTRIBUTE_VALUES_KEY,
            CONSISTENT_READ_KEY,
            LIMIT_KEY,
            VALIDATION_KEY,
            WITH_RESULT_KEY,
        ],
    )?;

    if let Some(v) = params.get(QUERY_EXPRESSION_KEY) {
        if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
            builder = builder.key_condition_expression(s.as_str().to_string());
        } else {
            return bad_input(format!("'{}' must be a string", QUERY_EXPRESSION_KEY));
        }
    }

    if let Some(v) = params.get(FILTER_EXPRESSION_KEY) {
        if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
            builder = builder.filter_expression(s.as_str().to_string());
        } else {
            return bad_input(format!("'{}' must be a string", FILTER_EXPRESSION_KEY));
        }
    }

    if let Some(proj) = params.get(PROJECTION_EXPRESSION_KEY) {
        if let Ok(s) = proj.borrow_ref::<rune::alloc::String>() {
            builder = builder.projection_expression(s.as_str().to_string());
        } else {
            return bad_input(format!("'{}' must be a string", PROJECTION_EXPRESSION_KEY));
        }
    }

    if let Some(v) = params.get(ATTRIBUTE_NAMES_KEY) {
        if let Ok(obj) = v.borrow_ref::<Object>() {
            builder = builder.set_expression_attribute_names(Some(extract_attribute_names(&obj)?));
        } else {
            return bad_input(format!("'{}' must be an object", ATTRIBUTE_NAMES_KEY));
        }
    }

    if let Some(v) = params.get(ATTRIBUTE_VALUES_KEY) {
        if let Ok(obj) = v.borrow_ref::<Object>() {
            builder =
                builder.set_expression_attribute_values(Some(rune_object_to_alternator_map(&obj)?));
        } else {
            return bad_input(format!("'{}' must be an object", ATTRIBUTE_VALUES_KEY));
        }
    }

    if let Some(v) = params.get(CONSISTENT_READ_KEY) {
        builder = builder.consistent_read(match v.as_bool() {
            Ok(b) => b,
            _ => return bad_input(format!("'{}' must be a boolean", CONSISTENT_READ_KEY)),
        });
    }

    if let Some(limit_val) = params.get(LIMIT_KEY) {
        if let Ok(i) = limit_val.as_signed() {
            builder = builder.limit(match i32::try_from(i) {
                Ok(val) => val,
                Err(_) => return bad_input(format!("'{}' is out of range", LIMIT_KEY)),
            });
        } else {
            return bad_input(format!("'{}' must be an integer", LIMIT_KEY));
        }
    }

    let validation = if let Some(v) = params.get(VALIDATION_KEY) {
        if let Ok(vec) = v.borrow_ref::<rune::runtime::Vec>() {
            Some(
                extract_validation_args(vec.to_vec())
                    .map_err(|s| AlternatorError::new(AlternatorErrorKind::BadInput(s)))?,
            )
        } else if v.clone().into_unit().is_ok() {
            None
        } else {
            return bad_input(format!("'{}' must be a list or ()", VALIDATION_KEY));
        }
    } else {
        None
    };

    let result = handle_request_with_validation(&ctx, builder, validation, "Query").await?;

    if let Some(v) = params.get(WITH_RESULT_KEY) {
        if let Ok(b) = v.as_bool() {
            if b {
                return Ok(result.to_value()?);
            }
        } else {
            return bad_input(format!("'{}' must be a boolean", WITH_RESULT_KEY));
        }
    }

    Ok(Value::from(()))
}

/// Scans items from the table.
///
/// If `with_result` is set to true, the scan result is returned as a `Vec<Object>`.
/// Otherwise, the unit value is returned.
///
/// # Arguments
/// * `table_name` - The name of the table.
/// * `params` - Parameters for the scan operation. An object containing:
///   - `filter`: The filter expression string (optional).
///   - `projection_expression`: A string that identifies the attributes to retrieve (optional).
///   - `attribute_names`: A map of attribute name placeholders (starting with #) to actual names.
///   - `attribute_values`: A map of attribute value placeholders (starting with :) to values.
///   - `consistent_read`: Boolean to enable consistent read (default: false).
///   - `limit`: The maximum number of items to evaluate (optional).
///   - `validation`: An optional item count validation. Look at [extract_validation_args] for details.
///   - `with_result`: If true, the scan result is returned (default: false).
///   - `segment`: The segment to scan (optional).
///   - `total_segments`: The total number of segments (optional).
#[rune::function(instance)]
pub async fn scan(
    ctx: Ref<Context>,
    table_name: Ref<str>,
    params: Ref<Object>,
) -> Result<Value, AlternatorError> {
    let mut builder = Request::new(AlternatorOp::Scan).table_name(table_name.deref());

    check_invalid_params(
        &params,
        "scan",
        &[
            FILTER_EXPRESSION_KEY,
            PROJECTION_EXPRESSION_KEY,
            ATTRIBUTE_NAMES_KEY,
            ATTRIBUTE_VALUES_KEY,
            CONSISTENT_READ_KEY,
            LIMIT_KEY,
            VALIDATION_KEY,
            WITH_RESULT_KEY,
            SEGMENT_KEY,
            TOTAL_SEGMENTS_KEY,
        ],
    )?;

    if let Some(v) = params.get(SEGMENT_KEY) {
        if let Ok(i) = v.as_signed() {
            builder = builder.segment(match i32::try_from(i) {
                Ok(val) => val,
                Err(_) => return bad_input(format!("'{}' is out of range", SEGMENT_KEY)),
            });
        } else {
            return bad_input(format!("'{}' must be an integer", SEGMENT_KEY));
        }
    }

    if let Some(v) = params.get(TOTAL_SEGMENTS_KEY) {
        if let Ok(i) = v.as_signed() {
            builder = builder.total_segments(match i32::try_from(i) {
                Ok(val) => val,
                Err(_) => return bad_input(format!("'{}' is out of range", TOTAL_SEGMENTS_KEY)),
            });
        } else {
            return bad_input(format!("'{}' must be an integer", TOTAL_SEGMENTS_KEY));
        }
    }

    if let Some(v) = params.get(FILTER_EXPRESSION_KEY) {
        if let Ok(s) = v.borrow_ref::<rune::alloc::String>() {
            builder = builder.filter_expression(s.as_str().to_string());
        } else {
            return bad_input(format!("'{}' must be a string", FILTER_EXPRESSION_KEY));
        }
    }

    if let Some(proj) = params.get(PROJECTION_EXPRESSION_KEY) {
        if let Ok(s) = proj.borrow_ref::<rune::alloc::String>() {
            builder = builder.projection_expression(s.as_str().to_string());
        } else {
            return bad_input(format!("'{}' must be a string", PROJECTION_EXPRESSION_KEY));
        }
    }

    if let Some(v) = params.get(ATTRIBUTE_NAMES_KEY) {
        if let Ok(obj) = v.borrow_ref::<Object>() {
            builder = builder.set_expression_attribute_names(Some(extract_attribute_names(&obj)?));
        } else {
            return bad_input(format!("'{}' must be an object", ATTRIBUTE_NAMES_KEY));
        }
    }

    if let Some(v) = params.get(ATTRIBUTE_VALUES_KEY) {
        if let Ok(obj) = v.borrow_ref::<Object>() {
            builder =
                builder.set_expression_attribute_values(Some(rune_object_to_alternator_map(&obj)?));
        } else {
            return bad_input(format!("'{}' must be an object", ATTRIBUTE_VALUES_KEY));
        }
    }

    if let Some(v) = params.get(CONSISTENT_READ_KEY) {
        builder = builder.consistent_read(match v.as_bool() {
            Ok(b) => b,
            _ => return bad_input(format!("'{}' must be a boolean", CONSISTENT_READ_KEY)),
        });
    }

    if let Some(limit_val) = params.get(LIMIT_KEY) {
        if let Ok(i) = limit_val.as_signed() {
            builder = builder.limit(match i32::try_from(i) {
                Ok(val) => val,
                Err(_) => return bad_input(format!("'{}' is out of range", LIMIT_KEY)),
            });
        } else {
            return bad_input(format!("'{}' must be an integer", LIMIT_KEY));
        }
    }

    let validation = if let Some(v) = params.get(VALIDATION_KEY) {
        if let Ok(vec) = v.borrow_ref::<rune::runtime::Vec>() {
            Some(
                extract_validation_args(vec.to_vec())
                    .map_err(|s| AlternatorError::new(AlternatorErrorKind::BadInput(s)))?,
            )
        } else if v.clone().into_unit().is_ok() {
            None
        } else {
            return bad_input(format!("'{}' must be a list or ()", VALIDATION_KEY));
        }
    } else {
        None
    };

    let result = handle_request_with_validation(&ctx, builder, validation, "Scan").await?;

    if let Some(v) = params.get(WITH_RESULT_KEY) {
        if let Ok(b) = v.as_bool() {
            if b {
                return Ok(result.to_value()?);
            }
        } else {
            return bad_input(format!("'{}' must be a boolean", WITH_RESULT_KEY));
        }
    }

    Ok(Value::from(()))
}

/// Marks a list of items as an Alternator string set.
#[rune::function]
pub fn string_set(items: Vec<Value>) -> VmResult<Value> {
    let mut obj = Object::new();
    let rune_key = rune::vm_try!(rune::alloc::String::try_from(SSET_KEY));
    let items_val = rune::vm_try!(items.to_value());
    rune::vm_try!(obj.insert(rune_key, items_val));
    VmResult::Ok(rune::vm_try!(Value::new(obj)))
}

/// Marks a list of items as an Alternator number set.
#[rune::function]
pub fn number_set(items: Vec<Value>) -> VmResult<Value> {
    let mut obj = Object::new();
    let rune_key = rune::vm_try!(rune::alloc::String::try_from(NSET_KEY));
    let items_val = rune::vm_try!(items.to_value());
    rune::vm_try!(obj.insert(rune_key, items_val));
    VmResult::Ok(rune::vm_try!(Value::new(obj)))
}

/// Marks a list of items as an Alternator binary set.
#[rune::function]
pub fn binary_set(items: Vec<Value>) -> VmResult<Value> {
    let mut obj = Object::new();
    let rune_key = rune::vm_try!(rune::alloc::String::try_from(BSET_KEY));
    let items_val = rune::vm_try!(items.to_value());
    rune::vm_try!(obj.insert(rune_key, items_val));
    VmResult::Ok(rune::vm_try!(Value::new(obj)))
}

/// Polls `DescribeTable` until the table is `ACTIVE`, for at most `timeout`.
async fn wait_until_table_exists(
    ctx: &Context,
    table_name: &str,
    timeout: Duration,
) -> Result<(), AlternatorError> {
    let deadline = std::time::Instant::now() + timeout;
    let request = Request::new(AlternatorOp::DescribeTable).table_name(table_name);
    loop {
        if let Ok((output, _)) = ctx.send(&request).await? {
            let status = output
                .get("Table")
                .and_then(|t| t.get("TableStatus"))
                .and_then(Json::as_str);
            if status == Some("ACTIVE") || status.is_none() {
                return Ok(());
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(AlternatorError::new(AlternatorErrorKind::SdkError(
                format!("table {table_name} did not become ACTIVE within {timeout:?}"),
            )));
        }
        ctx.sleep(Duration::from_millis(200)).await;
    }
}
