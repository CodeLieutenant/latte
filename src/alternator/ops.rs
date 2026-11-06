//! Runs the DynamoDB ops of the core on the Alternator client. The core sends a typed
//! request; this module builds the SDK input straight from its fields and turns the SDK
//! output back into response JSON. A request given as JSON takes the same path.

use std::collections::HashMap;

use alternator_driver::AlternatorClient;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, DeleteRequest, KeySchemaElement, KeyType,
    KeysAndAttributes, PutRequest, ScalarAttributeType, WriteRequest,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use latte_core::clock;
use latte_core::native::{AlternatorAttribute, AlternatorRequest};
use latte_core::ops::{
    AlternatorOp, ClientKind, Completion, DriverError, Json as OpJson, Op, OpKind, Response,
};
use serde_json::{json, Map, Value as Json};

/// The Alternator client, shared by every instance.
pub struct Driver {
    client: AlternatorClient,
}

type Item = HashMap<String, AttributeValue>;

fn bad_request(message: impl Into<String>) -> DriverError {
    DriverError::dynamodb(ClientKind::Other, "SerializationException", message)
}

/// Converts an SDK error into the DynamoDB error type and message the core maps.
fn sdk_error<E, R>(e: SdkError<E, R>) -> DriverError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    let kind = match &e {
        SdkError::TimeoutError(_) => ClientKind::ClientTimeout,
        SdkError::DispatchFailure(f) if f.is_timeout() => ClientKind::ClientTimeout,
        SdkError::DispatchFailure(f) if f.is_io() => ClientKind::ConnectionLost,
        SdkError::ServiceError(_) => ClientKind::None,
        _ => ClientKind::Other,
    };
    let error_type = e.code().unwrap_or("SdkError").to_string();
    let message = e
        .message()
        .map(str::to_string)
        .unwrap_or_else(|| aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string());
    DriverError::dynamodb(kind, error_type, message)
}

pub fn attribute_from_json(json: &Json) -> Result<AttributeValue, DriverError> {
    let obj = json
        .as_object()
        .filter(|o| o.len() == 1)
        .ok_or_else(|| bad_request(format!("Invalid attribute value: {json}")))?;
    let (tag, value) = obj.iter().next().unwrap();
    let string = |v: &Json| {
        v.as_str()
            .map(str::to_string)
            .ok_or_else(|| bad_request(format!("Expected a string in {json}")))
    };
    let blob = |v: &Json| -> Result<Blob, DriverError> {
        BASE64
            .decode(string(v)?)
            .map(Blob::new)
            .map_err(|e| bad_request(e.to_string()))
    };
    let array = |v: &Json| -> Result<Vec<Json>, DriverError> {
        v.as_array()
            .cloned()
            .ok_or_else(|| bad_request(format!("Expected a list in {json}")))
    };
    Ok(match tag.as_str() {
        "S" => AttributeValue::S(string(value)?),
        "N" => AttributeValue::N(string(value)?),
        "B" => AttributeValue::B(blob(value)?),
        "BOOL" => AttributeValue::Bool(value.as_bool().unwrap_or(false)),
        "NULL" => AttributeValue::Null(value.as_bool().unwrap_or(true)),
        "M" => AttributeValue::M(item_from_json(value)?),
        "L" => AttributeValue::L(
            array(value)?
                .iter()
                .map(attribute_from_json)
                .collect::<Result<_, _>>()?,
        ),
        "SS" => AttributeValue::Ss(array(value)?.iter().map(string).collect::<Result<_, _>>()?),
        "NS" => AttributeValue::Ns(array(value)?.iter().map(string).collect::<Result<_, _>>()?),
        "BS" => AttributeValue::Bs(array(value)?.iter().map(blob).collect::<Result<_, _>>()?),
        other => return Err(bad_request(format!("Unsupported attribute type {other}"))),
    })
}

pub fn attribute_to_json(value: &AttributeValue) -> Json {
    match value {
        AttributeValue::S(s) => json!({ "S": s }),
        AttributeValue::N(n) => json!({ "N": n }),
        AttributeValue::B(b) => json!({ "B": BASE64.encode(b.as_ref()) }),
        AttributeValue::Bool(b) => json!({ "BOOL": b }),
        AttributeValue::Null(b) => json!({ "NULL": b }),
        AttributeValue::M(m) => json!({ "M": item_to_json(m) }),
        AttributeValue::L(l) => json!({ "L": l.iter().map(attribute_to_json).collect::<Vec<_>>() }),
        AttributeValue::Ss(v) => json!({ "SS": v }),
        AttributeValue::Ns(v) => json!({ "NS": v }),
        AttributeValue::Bs(v) => {
            json!({ "BS": v.iter().map(|b| BASE64.encode(b.as_ref())).collect::<Vec<_>>() })
        }
        _ => json!({ "NULL": true }),
    }
}

pub fn item_from_json(json: &Json) -> Result<Item, DriverError> {
    json.as_object()
        .ok_or_else(|| bad_request(format!("Expected an attribute map: {json}")))?
        .iter()
        .map(|(k, v)| Ok((k.clone(), attribute_from_json(v)?)))
        .collect()
}

pub fn item_to_json(item: &Item) -> Json {
    Json::Object(
        item.iter()
            .map(|(k, v)| (k.clone(), attribute_to_json(v)))
            .collect(),
    )
}

/// Typed access to the fields of a request body.
/// The request of an op: a JSON object, or the typed request of the core.
enum Body<'a> {
    Json(Map<String, Json>),
    Typed(&'a AlternatorRequest),
}

/// Converts an attribute of the core to an SDK attribute.
fn sdk_attribute(value: &AlternatorAttribute) -> AttributeValue {
    match value {
        AlternatorAttribute::S(v) => AttributeValue::S(v.clone()),
        AlternatorAttribute::N(v) => AttributeValue::N(v.clone()),
        AlternatorAttribute::B(v) => AttributeValue::B(Blob::new(v.as_ref())),
        AlternatorAttribute::Bool(v) => AttributeValue::Bool(*v),
        AlternatorAttribute::Null(v) => AttributeValue::Null(*v),
        AlternatorAttribute::M(v) => AttributeValue::M(sdk_item(v)),
        AlternatorAttribute::L(v) => AttributeValue::L(v.iter().map(sdk_attribute).collect()),
        AlternatorAttribute::Ss(v) => AttributeValue::Ss(v.clone()),
        AlternatorAttribute::Ns(v) => AttributeValue::Ns(v.clone()),
        AlternatorAttribute::Bs(v) => {
            AttributeValue::Bs(v.iter().map(|b| Blob::new(b.as_ref())).collect())
        }
    }
}

fn sdk_item(item: &HashMap<String, AlternatorAttribute>) -> Item {
    item.iter()
        .map(|(k, v)| (k.clone(), sdk_attribute(v)))
        .collect()
}

impl Body<'_> {
    fn str(&self, field: &str) -> Option<String> {
        match self {
            Body::Json(m) => m.get(field).and_then(Json::as_str).map(str::to_string),
            Body::Typed(r) => match field {
                "TableName" => r.table_name.clone(),
                "ConditionExpression" => r.condition_expression.clone(),
                "UpdateExpression" => r.update_expression.clone(),
                "KeyConditionExpression" => r.key_condition_expression.clone(),
                "FilterExpression" => r.filter_expression.clone(),
                "ProjectionExpression" => r.projection_expression.clone(),
                _ => r
                    .extra
                    .get(field)
                    .and_then(Json::as_str)
                    .map(str::to_string),
            },
        }
    }

    fn bool(&self, field: &str) -> Option<bool> {
        match self {
            Body::Json(m) => m.get(field).and_then(Json::as_bool),
            Body::Typed(r) => match field {
                "ConsistentRead" => r.consistent_read,
                _ => r.extra.get(field).and_then(Json::as_bool),
            },
        }
    }

    fn i32(&self, field: &str) -> Option<i32> {
        match self {
            Body::Json(m) => m.get(field).and_then(Json::as_i64).map(|n| n as i32),
            Body::Typed(r) => match field {
                "Limit" => r.limit,
                "Segment" => r.segment,
                "TotalSegments" => r.total_segments,
                _ => r.extra.get(field).and_then(Json::as_i64).map(|n| n as i32),
            },
        }
    }

    fn item(&self, field: &str) -> Result<Option<Item>, DriverError> {
        match self {
            Body::Json(m) => m.get(field).map(item_from_json).transpose(),
            Body::Typed(r) => match field {
                "Item" => Ok(r.item.as_ref().map(sdk_item)),
                "Key" => Ok(r.key.as_ref().map(sdk_item)),
                "ExpressionAttributeValues" => {
                    Ok(r.expression_attribute_values.as_ref().map(sdk_item))
                }
                "ExclusiveStartKey" => r
                    .exclusive_start_key
                    .as_ref()
                    .map(item_from_json)
                    .transpose(),
                _ => r.extra.get(field).map(item_from_json).transpose(),
            },
        }
    }

    fn names(&self) -> Option<HashMap<String, String>> {
        match self {
            Body::Json(m) => m
                .get("ExpressionAttributeNames")
                .and_then(Json::as_object)
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                        .collect()
                }),
            Body::Typed(r) => r.expression_attribute_names.clone(),
        }
    }

    fn values(&self) -> Result<Option<Item>, DriverError> {
        self.item("ExpressionAttributeValues")
    }

    /// A field kept in wire form: `RequestItems` and the fields of the rare operations.
    fn json(&self, field: &str) -> Option<&Json> {
        match self {
            Body::Json(m) => m.get(field),
            Body::Typed(r) => match field {
                "RequestItems" => r.request_items.as_ref(),
                "ExclusiveStartKey" => r.exclusive_start_key.as_ref(),
                _ => r.extra.get(field),
            },
        }
    }

    fn table(&self) -> Result<String, DriverError> {
        self.str("TableName")
            .ok_or_else(|| bad_request("TableName is missing"))
    }
}

fn items_json(items: &[Item]) -> Json {
    Json::Array(items.iter().map(item_to_json).collect())
}

impl Driver {
    pub fn new(client: AlternatorClient) -> Self {
        Driver { client }
    }

    /// Runs one op and stamps its start and end on the shared clock.
    pub async fn run(&self, op: Op) -> Completion {
        let start_ns = clock::now_ns();
        let result = match op.kind {
            OpKind::Alternator { op: kind, body } => match body {
                OpJson::Request(request) => {
                    match request.as_any().downcast_ref::<AlternatorRequest>() {
                        Some(typed) => self.run_body(kind, Body::Typed(typed)).await,
                        None => self.run_json(kind, request.to_value()).await,
                    }
                }
                other => match other.into_value() {
                    Ok(body) => self.run_json(kind, body).await,
                    Err(e) => Err(bad_request(e.to_string())),
                },
            },
            other => Err(DriverError::new(
                0,
                ClientKind::Other,
                "UnsupportedOp",
                format!(
                    "this host runs Alternator and cannot run CQL op {}",
                    other.code()
                ),
            )),
        };
        Completion {
            id: op.id,
            start_ns,
            end_ns: clock::now_ns(),
            result: result.map(|json| Response::Json(OpJson::Value(json))),
        }
    }

    /// Runs one DynamoDB operation given as request JSON and returns the response JSON.
    pub async fn run_json(&self, op: AlternatorOp, body: Json) -> Result<Json, DriverError> {
        match body {
            Json::Object(map) => self.run_body(op, Body::Json(map)).await,
            _ => Err(bad_request("request body is not a JSON object")),
        }
    }

    /// Runs one DynamoDB operation and returns the response JSON.
    async fn run_body(&self, op: AlternatorOp, body: Body<'_>) -> Result<Json, DriverError> {
        let c = &self.client;
        match op {
            AlternatorOp::CreateTable => {
                let mut b = c
                    .create_table()
                    .table_name(body.table()?)
                    .billing_mode(BillingMode::PayPerRequest);
                for k in body
                    .json("KeySchema")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                {
                    let key_type = match k.get("KeyType").and_then(Json::as_str) {
                        Some("RANGE") => KeyType::Range,
                        _ => KeyType::Hash,
                    };
                    b = b.key_schema(
                        KeySchemaElement::builder()
                            .attribute_name(
                                k.get("AttributeName").and_then(Json::as_str).unwrap_or(""),
                            )
                            .key_type(key_type)
                            .build()
                            .map_err(|e| bad_request(e.to_string()))?,
                    );
                }
                for d in body
                    .json("AttributeDefinitions")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                {
                    let attr_type = match d.get("AttributeType").and_then(Json::as_str) {
                        Some("N") => ScalarAttributeType::N,
                        Some("B") => ScalarAttributeType::B,
                        _ => ScalarAttributeType::S,
                    };
                    b = b.attribute_definitions(
                        AttributeDefinition::builder()
                            .attribute_name(
                                d.get("AttributeName").and_then(Json::as_str).unwrap_or(""),
                            )
                            .attribute_type(attr_type)
                            .build()
                            .map_err(|e| bad_request(e.to_string()))?,
                    );
                }
                b.send().await.map_err(sdk_error)?;
                Ok(json!({}))
            }
            AlternatorOp::DeleteTable => {
                c.delete_table()
                    .table_name(body.table()?)
                    .send()
                    .await
                    .map_err(sdk_error)?;
                Ok(json!({}))
            }
            AlternatorOp::DescribeTable => {
                let out = c
                    .describe_table()
                    .table_name(body.table()?)
                    .send()
                    .await
                    .map_err(sdk_error)?;
                let status = out
                    .table()
                    .and_then(|t| t.table_status())
                    .map(|s| s.as_str().to_string());
                Ok(json!({ "Table": { "TableStatus": status } }))
            }
            AlternatorOp::PutItem => {
                c.put_item()
                    .table_name(body.table()?)
                    .set_item(body.item("Item")?)
                    .set_condition_expression(body.str("ConditionExpression"))
                    .set_expression_attribute_names(body.names())
                    .set_expression_attribute_values(body.values()?)
                    .send()
                    .await
                    .map_err(sdk_error)?;
                Ok(json!({}))
            }
            AlternatorOp::DeleteItem => {
                c.delete_item()
                    .table_name(body.table()?)
                    .set_key(body.item("Key")?)
                    .set_condition_expression(body.str("ConditionExpression"))
                    .set_expression_attribute_names(body.names())
                    .set_expression_attribute_values(body.values()?)
                    .send()
                    .await
                    .map_err(sdk_error)?;
                Ok(json!({}))
            }
            AlternatorOp::UpdateItem => {
                c.update_item()
                    .table_name(body.table()?)
                    .set_key(body.item("Key")?)
                    .set_update_expression(body.str("UpdateExpression"))
                    .set_condition_expression(body.str("ConditionExpression"))
                    .set_expression_attribute_names(body.names())
                    .set_expression_attribute_values(body.values()?)
                    .send()
                    .await
                    .map_err(sdk_error)?;
                Ok(json!({}))
            }
            AlternatorOp::GetItem => {
                let out = c
                    .get_item()
                    .table_name(body.table()?)
                    .set_key(body.item("Key")?)
                    .set_consistent_read(body.bool("ConsistentRead"))
                    .set_projection_expression(body.str("ProjectionExpression"))
                    .set_expression_attribute_names(body.names())
                    .send()
                    .await
                    .map_err(sdk_error)?;
                Ok(match out.item() {
                    Some(item) => json!({ "Item": item_to_json(item) }),
                    None => json!({}),
                })
            }
            AlternatorOp::Query => {
                let out = c
                    .query()
                    .table_name(body.table()?)
                    .set_key_condition_expression(body.str("KeyConditionExpression"))
                    .set_filter_expression(body.str("FilterExpression"))
                    .set_projection_expression(body.str("ProjectionExpression"))
                    .set_expression_attribute_names(body.names())
                    .set_expression_attribute_values(body.values()?)
                    .set_consistent_read(body.bool("ConsistentRead"))
                    .set_limit(body.i32("Limit"))
                    .set_exclusive_start_key(body.item("ExclusiveStartKey")?)
                    .send()
                    .await
                    .map_err(sdk_error)?;
                let mut response =
                    json!({ "Items": items_json(out.items()), "Count": out.count() });
                if let Some(key) = out.last_evaluated_key() {
                    response["LastEvaluatedKey"] = item_to_json(key);
                }
                Ok(response)
            }
            AlternatorOp::Scan => {
                let out = c
                    .scan()
                    .table_name(body.table()?)
                    .set_filter_expression(body.str("FilterExpression"))
                    .set_projection_expression(body.str("ProjectionExpression"))
                    .set_expression_attribute_names(body.names())
                    .set_expression_attribute_values(body.values()?)
                    .set_consistent_read(body.bool("ConsistentRead"))
                    .set_limit(body.i32("Limit"))
                    .set_segment(body.i32("Segment"))
                    .set_total_segments(body.i32("TotalSegments"))
                    .set_exclusive_start_key(body.item("ExclusiveStartKey")?)
                    .send()
                    .await
                    .map_err(sdk_error)?;
                let mut response =
                    json!({ "Items": items_json(out.items()), "Count": out.count() });
                if let Some(key) = out.last_evaluated_key() {
                    response["LastEvaluatedKey"] = item_to_json(key);
                }
                Ok(response)
            }
            AlternatorOp::BatchGetItem => {
                let mut request_items = HashMap::new();
                for (table, ka) in body
                    .json("RequestItems")
                    .and_then(Json::as_object)
                    .into_iter()
                    .flatten()
                {
                    let keys = ka
                        .get("Keys")
                        .and_then(Json::as_array)
                        .into_iter()
                        .flatten()
                        .map(item_from_json)
                        .collect::<Result<Vec<_>, _>>()?;
                    let ka_body = Body::Json(ka.as_object().cloned().unwrap_or_default());
                    let keys_and_attributes = KeysAndAttributes::builder()
                        .set_keys(Some(keys))
                        .set_consistent_read(ka_body.bool("ConsistentRead"))
                        .set_projection_expression(ka_body.str("ProjectionExpression"))
                        .set_expression_attribute_names(ka_body.names())
                        .build()
                        .map_err(|e| bad_request(e.to_string()))?;
                    request_items.insert(table.clone(), keys_and_attributes);
                }
                let out = c
                    .batch_get_item()
                    .set_request_items(Some(request_items))
                    .send()
                    .await
                    .map_err(sdk_error)?;
                let responses: Map<String, Json> = out
                    .responses()
                    .into_iter()
                    .flatten()
                    .map(|(t, items)| (t.clone(), items_json(items)))
                    .collect();
                let unprocessed: Map<String, Json> = out
                    .unprocessed_keys()
                    .into_iter()
                    .flatten()
                    .map(|(t, ka)| (t.clone(), json!({ "Keys": items_json(ka.keys()) })))
                    .collect();
                Ok(json!({ "Responses": responses, "UnprocessedKeys": unprocessed }))
            }
            AlternatorOp::BatchWriteItem => {
                let mut request_items = HashMap::new();
                for (table, writes) in body
                    .json("RequestItems")
                    .and_then(Json::as_object)
                    .into_iter()
                    .flatten()
                {
                    let mut requests = Vec::new();
                    for w in writes.as_array().into_iter().flatten() {
                        if let Some(item) = w.get("PutRequest").and_then(|p| p.get("Item")) {
                            requests.push(
                                WriteRequest::builder()
                                    .put_request(
                                        PutRequest::builder()
                                            .set_item(Some(item_from_json(item)?))
                                            .build()
                                            .map_err(|e| bad_request(e.to_string()))?,
                                    )
                                    .build(),
                            );
                        } else if let Some(key) = w.get("DeleteRequest").and_then(|d| d.get("Key"))
                        {
                            requests.push(
                                WriteRequest::builder()
                                    .delete_request(
                                        DeleteRequest::builder()
                                            .set_key(Some(item_from_json(key)?))
                                            .build()
                                            .map_err(|e| bad_request(e.to_string()))?,
                                    )
                                    .build(),
                            );
                        }
                    }
                    request_items.insert(table.clone(), requests);
                }
                let out = c
                    .batch_write_item()
                    .set_request_items(Some(request_items))
                    .send()
                    .await
                    .map_err(sdk_error)?;
                let unprocessed: Map<String, Json> = out
                    .unprocessed_items()
                    .into_iter()
                    .flatten()
                    .map(|(t, reqs)| {
                        let reqs: Vec<Json> = reqs
                            .iter()
                            .map(|r| {
                                if let Some(p) = r.put_request() {
                                    json!({ "PutRequest": { "Item": item_to_json(p.item()) } })
                                } else if let Some(d) = r.delete_request() {
                                    json!({ "DeleteRequest": { "Key": item_to_json(d.key()) } })
                                } else {
                                    json!({})
                                }
                            })
                            .collect();
                        (t.clone(), Json::Array(reqs))
                    })
                    .collect();
                Ok(json!({ "UnprocessedItems": unprocessed }))
            }
            AlternatorOp::DescribeEndpoints => {
                let out = c.describe_endpoints().send().await.map_err(sdk_error)?;
                let endpoints: Vec<Json> = out
                    .endpoints()
                    .iter()
                    .map(|e| json!({ "Address": e.address() }))
                    .collect();
                Ok(json!({ "Endpoints": endpoints }))
            }
        }
    }
}

impl crate::host::OpRunner for Driver {
    fn run(&self, op: Op) -> impl std::future::Future<Output = Completion> {
        Driver::run(self, op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_round_trip_between_json_and_sdk_types() {
        let json = json!({ "M": {
            "s": { "S": "x" }, "n": { "N": "1" }, "b": { "B": "AQID" },
            "l": { "L": [ { "BOOL": true }, { "NULL": true } ] },
            "ss": { "SS": ["a"] }, "ns": { "NS": ["2"] }, "bs": { "BS": ["AQ=="] }
        }});
        let value = attribute_from_json(&json).unwrap();
        assert_eq!(attribute_to_json(&value), json);
    }
}
