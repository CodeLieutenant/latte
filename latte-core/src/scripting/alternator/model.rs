//! DynamoDB values and requests in the JSON form of the DynamoDB wire protocol.
//!
//! The core builds the exact request body a DynamoDB client sends and parses the exact
//! response body it receives, so a host only moves JSON between the core and its client.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value as Json};
use std::collections::HashMap;
use std::rc::Rc;

use super::alternator_error::{AlternatorError, AlternatorErrorKind};
use crate::ops::AlternatorOp;

/// Binary data of a `B` or `BS` attribute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blob(Vec<u8>);

impl Blob {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Blob {
    fn from(v: Vec<u8>) -> Self {
        Blob(v)
    }
}

/// A DynamoDB attribute value.
#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    S(String),
    N(String),
    B(Blob),
    Bool(bool),
    Null(bool),
    M(HashMap<String, AttributeValue>),
    L(Vec<AttributeValue>),
    Ss(Vec<String>),
    Ns(Vec<String>),
    Bs(Vec<Blob>),
}

fn conversion(msg: impl Into<String>) -> AlternatorError {
    AlternatorError::new(AlternatorErrorKind::ConversionError(msg.into()))
}

impl AttributeValue {
    /// The wire form, such as `{"S": "x"}` or `{"N": "1"}`.
    pub fn to_json(&self) -> Json {
        let (tag, value) = match self {
            AttributeValue::S(s) => ("S", Json::from(s.as_str())),
            AttributeValue::N(n) => ("N", Json::from(n.as_str())),
            AttributeValue::B(b) => ("B", Json::from(BASE64.encode(b))),
            AttributeValue::Bool(b) => ("BOOL", Json::from(*b)),
            AttributeValue::Null(b) => ("NULL", Json::from(*b)),
            AttributeValue::M(m) => ("M", map_to_json(m)),
            AttributeValue::L(l) => ("L", Json::Array(l.iter().map(Self::to_json).collect())),
            AttributeValue::Ss(v) => ("SS", Json::from(v.clone())),
            AttributeValue::Ns(v) => ("NS", Json::from(v.clone())),
            AttributeValue::Bs(v) => (
                "BS",
                Json::Array(v.iter().map(|b| Json::from(BASE64.encode(b))).collect()),
            ),
        };
        let mut obj = Map::new();
        obj.insert(tag.to_string(), value);
        Json::Object(obj)
    }

    pub fn from_json(json: &Json) -> Result<AttributeValue, AlternatorError> {
        let obj = json
            .as_object()
            .filter(|o| o.len() == 1)
            .ok_or_else(|| conversion(format!("Invalid attribute value: {json}")))?;
        let (tag, value) = obj.iter().next().unwrap();
        let string = |v: &Json| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| conversion(format!("Expected a string in {json}")))
        };
        let blob = |v: &Json| -> Result<Blob, AlternatorError> {
            let s = string(v)?;
            BASE64
                .decode(s)
                .map(Blob)
                .map_err(|e| conversion(e.to_string()))
        };
        let array = |v: &Json| {
            v.as_array()
                .cloned()
                .ok_or_else(|| conversion(format!("Expected a list in {json}")))
        };
        Ok(match tag.as_str() {
            "S" => AttributeValue::S(string(value)?),
            "N" => AttributeValue::N(string(value)?),
            "B" => AttributeValue::B(blob(value)?),
            "BOOL" => AttributeValue::Bool(value.as_bool().unwrap_or(false)),
            "NULL" => AttributeValue::Null(value.as_bool().unwrap_or(true)),
            "M" => AttributeValue::M(map_from_json(value)?),
            "L" => AttributeValue::L(
                array(value)?
                    .iter()
                    .map(AttributeValue::from_json)
                    .collect::<Result<_, _>>()?,
            ),
            "SS" => AttributeValue::Ss(array(value)?.iter().map(string).collect::<Result<_, _>>()?),
            "NS" => AttributeValue::Ns(array(value)?.iter().map(string).collect::<Result<_, _>>()?),
            "BS" => AttributeValue::Bs(array(value)?.iter().map(blob).collect::<Result<_, _>>()?),
            other => return Err(conversion(format!("Unsupported attribute type {other}"))),
        })
    }
}

pub fn map_to_json(map: &HashMap<String, AttributeValue>) -> Json {
    Json::Object(map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect())
}

pub fn map_from_json(json: &Json) -> Result<HashMap<String, AttributeValue>, AlternatorError> {
    json.as_object()
        .ok_or_else(|| conversion(format!("Expected an attribute map: {json}")))?
        .iter()
        .map(|(k, v)| Ok((k.clone(), AttributeValue::from_json(v)?)))
        .collect()
}

/// A request for one DynamoDB operation, held as typed fields.
///
/// A C host gets the request as JSON text, written straight into its ring arena by
/// [`serde::Serialize`] with no JSON tree in between. A native host reads the fields
/// directly and builds its SDK input from them. Retries and pages share the request through
/// an `Rc`; only a page that changes the start key or the limit makes a copy.
#[derive(Clone, Debug)]
pub struct RequestFields {
    pub op: AlternatorOp,
    pub table_name: Option<String>,
    pub item: Option<HashMap<String, AttributeValue>>,
    pub key: Option<HashMap<String, AttributeValue>>,
    pub condition_expression: Option<String>,
    pub update_expression: Option<String>,
    pub key_condition_expression: Option<String>,
    pub filter_expression: Option<String>,
    pub projection_expression: Option<String>,
    pub consistent_read: Option<bool>,
    pub limit: Option<i32>,
    pub segment: Option<i32>,
    pub total_segments: Option<i32>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    /// `LastEvaluatedKey` of the previous page, in wire form.
    pub exclusive_start_key: Option<Json>,
    /// `RequestItems` of a batch operation, in wire form.
    pub request_items: Option<Json>,
    /// Fields of the rare operations, such as `CreateTable`, in wire form.
    pub extra: Map<String, Json>,
}

/// A request: one shared, reference-counted set of fields. Builder calls change the fields
/// in place while the request has one owner, and a clone shares them, so a request moves
/// through futures as one pointer.
#[derive(Clone, Debug)]
pub struct Request(Rc<RequestFields>);

impl std::ops::Deref for Request {
    type Target = RequestFields;
    fn deref(&self) -> &RequestFields {
        &self.0
    }
}

impl Request {
    /// The shared fields, as the body of an op.
    pub(super) fn body(&self) -> Rc<RequestFields> {
        Rc::clone(&self.0)
    }

    /// The fields to change, copied first when another owner shares them.
    pub(super) fn fields_mut(&mut self) -> &mut RequestFields {
        Rc::make_mut(&mut self.0)
    }

    pub fn new(op: AlternatorOp) -> Self {
        Request(Rc::new(RequestFields {
            op,
            table_name: None,
            item: None,
            key: None,
            condition_expression: None,
            update_expression: None,
            key_condition_expression: None,
            filter_expression: None,
            projection_expression: None,
            consistent_read: None,
            limit: None,
            segment: None,
            total_segments: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            exclusive_start_key: None,
            request_items: None,
            extra: Map::new(),
        }))
    }

    /// Sets a field given in wire form.
    pub fn set(mut self, field: &str, value: impl Into<Json>) -> Self {
        let value = value.into();
        match field {
            "RequestItems" => self.fields_mut().request_items = Some(value),
            "ExclusiveStartKey" => self.fields_mut().exclusive_start_key = Some(value),
            _ => {
                self.fields_mut().extra.insert(field.to_string(), value);
            }
        }
        self
    }

    pub fn table_name(mut self, name: &str) -> Self {
        self.fields_mut().table_name = Some(name.to_string());
        self
    }

    pub fn set_item(mut self, item: Option<HashMap<String, AttributeValue>>) -> Self {
        if item.is_some() {
            self.fields_mut().item = item;
        }
        self
    }

    pub fn set_key(mut self, key: Option<HashMap<String, AttributeValue>>) -> Self {
        if key.is_some() {
            self.fields_mut().key = key;
        }
        self
    }

    pub fn condition_expression(mut self, e: String) -> Self {
        self.fields_mut().condition_expression = Some(e);
        self
    }

    pub fn update_expression(mut self, e: String) -> Self {
        self.fields_mut().update_expression = Some(e);
        self
    }

    pub fn key_condition_expression(mut self, e: String) -> Self {
        self.fields_mut().key_condition_expression = Some(e);
        self
    }

    pub fn filter_expression(mut self, e: String) -> Self {
        self.fields_mut().filter_expression = Some(e);
        self
    }

    pub fn projection_expression(mut self, e: String) -> Self {
        self.fields_mut().projection_expression = Some(e);
        self
    }

    pub fn consistent_read(mut self, b: bool) -> Self {
        self.fields_mut().consistent_read = Some(b);
        self
    }

    pub fn limit(mut self, n: i32) -> Self {
        self.fields_mut().limit = Some(n);
        self
    }

    pub fn segment(mut self, n: i32) -> Self {
        self.fields_mut().segment = Some(n);
        self
    }

    pub fn total_segments(mut self, n: i32) -> Self {
        self.fields_mut().total_segments = Some(n);
        self
    }

    pub fn set_expression_attribute_names(
        mut self,
        names: Option<HashMap<String, String>>,
    ) -> Self {
        if names.is_some() {
            self.fields_mut().expression_attribute_names = names;
        }
        self
    }

    pub fn set_expression_attribute_values(
        mut self,
        values: Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        if values.is_some() {
            self.fields_mut().expression_attribute_values = values;
        }
        self
    }

    pub fn get_limit(&self) -> Option<i32> {
        self.limit
    }

    /// The request body as JSON text.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&*self.0).expect("a request always serializes")
    }
}

impl Serialize for RequestFields {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Some(v) = &self.table_name {
            map.serialize_entry("TableName", v)?;
        }
        if let Some(v) = &self.item {
            map.serialize_entry("Item", v)?;
        }
        if let Some(v) = &self.key {
            map.serialize_entry("Key", v)?;
        }
        if let Some(v) = &self.condition_expression {
            map.serialize_entry("ConditionExpression", v)?;
        }
        if let Some(v) = &self.update_expression {
            map.serialize_entry("UpdateExpression", v)?;
        }
        if let Some(v) = &self.key_condition_expression {
            map.serialize_entry("KeyConditionExpression", v)?;
        }
        if let Some(v) = &self.filter_expression {
            map.serialize_entry("FilterExpression", v)?;
        }
        if let Some(v) = &self.projection_expression {
            map.serialize_entry("ProjectionExpression", v)?;
        }
        if let Some(v) = &self.consistent_read {
            map.serialize_entry("ConsistentRead", v)?;
        }
        if let Some(v) = &self.limit {
            map.serialize_entry("Limit", v)?;
        }
        if let Some(v) = &self.segment {
            map.serialize_entry("Segment", v)?;
        }
        if let Some(v) = &self.total_segments {
            map.serialize_entry("TotalSegments", v)?;
        }
        if let Some(v) = &self.expression_attribute_names {
            map.serialize_entry("ExpressionAttributeNames", v)?;
        }
        if let Some(v) = &self.expression_attribute_values {
            map.serialize_entry("ExpressionAttributeValues", v)?;
        }
        if let Some(v) = &self.exclusive_start_key {
            map.serialize_entry("ExclusiveStartKey", v)?;
        }
        if let Some(v) = &self.request_items {
            map.serialize_entry("RequestItems", v)?;
        }
        for (k, v) in &self.extra {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl crate::ops::AlternatorBody for RequestFields {
    fn write_json(&self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        serde_json::to_writer(out, self).map_err(std::io::Error::other)
    }

    fn to_value(&self) -> Json {
        serde_json::to_value(self).expect("a request always serializes")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Writes the attribute in wire form, such as `{"S": "x"}`, with no JSON tree in between.
impl Serialize for AttributeValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        struct B64<'a>(&'a [u8]);
        impl Serialize for B64<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(&base64::display::Base64Display::new(self.0, &BASE64))
            }
        }
        struct B64Set<'a>(&'a [Blob]);
        impl Serialize for B64Set<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_seq(self.0.iter().map(|b| B64(&b.0)))
            }
        }
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            AttributeValue::S(v) => map.serialize_entry("S", v)?,
            AttributeValue::N(v) => map.serialize_entry("N", v)?,
            AttributeValue::B(v) => map.serialize_entry("B", &B64(&v.0))?,
            AttributeValue::Bool(v) => map.serialize_entry("BOOL", v)?,
            AttributeValue::Null(v) => map.serialize_entry("NULL", v)?,
            AttributeValue::M(v) => map.serialize_entry("M", v)?,
            AttributeValue::L(v) => map.serialize_entry("L", v)?,
            AttributeValue::Ss(v) => map.serialize_entry("SS", v)?,
            AttributeValue::Ns(v) => map.serialize_entry("NS", v)?,
            AttributeValue::Bs(v) => map.serialize_entry("BS", &B64Set(v))?,
        }
        map.end()
    }
}

/// Reads the attribute from wire form straight into the typed value, with no JSON tree.
impl<'de> Deserialize<'de> for AttributeValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        enum Tag {
            S,
            N,
            B,
            #[serde(rename = "BOOL")]
            Bool,
            #[serde(rename = "NULL")]
            Null,
            M,
            L,
            #[serde(rename = "SS")]
            Ss,
            #[serde(rename = "NS")]
            Ns,
            #[serde(rename = "BS")]
            Bs,
        }

        struct B64(Vec<u8>);
        impl<'de> Deserialize<'de> for B64 {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct V;
                impl serde::de::Visitor<'_> for V {
                    type Value = B64;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("a base64 string")
                    }
                    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<B64, E> {
                        BASE64.decode(v).map(B64).map_err(E::custom)
                    }
                }
                deserializer.deserialize_str(V)
            }
        }

        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = AttributeValue;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a DynamoDB attribute value")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<AttributeValue, A::Error> {
                let tag: Tag = map
                    .next_key()?
                    .ok_or_else(|| serde::de::Error::custom("empty attribute value"))?;
                let value = match tag {
                    Tag::S => AttributeValue::S(map.next_value()?),
                    Tag::N => AttributeValue::N(map.next_value()?),
                    Tag::B => AttributeValue::B(Blob(map.next_value::<B64>()?.0)),
                    Tag::Bool => AttributeValue::Bool(map.next_value()?),
                    Tag::Null => AttributeValue::Null(map.next_value()?),
                    Tag::M => AttributeValue::M(map.next_value()?),
                    Tag::L => AttributeValue::L(map.next_value()?),
                    Tag::Ss => AttributeValue::Ss(map.next_value()?),
                    Tag::Ns => AttributeValue::Ns(map.next_value()?),
                    Tag::Bs => AttributeValue::Bs(
                        map.next_value::<Vec<B64>>()?
                            .into_iter()
                            .map(|b| Blob(b.0))
                            .collect(),
                    ),
                };
                if map.next_key::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "an attribute value has exactly one type",
                    ));
                }
                Ok(value)
            }
        }
        deserializer.deserialize_map(V)
    }
}

/// The fields of a response that scripts and validation read. Every other field is
/// skipped while parsing, without being stored.
#[derive(Debug, Default, Deserialize)]
pub struct Reply {
    #[serde(rename = "Item")]
    pub item: Option<HashMap<String, AttributeValue>>,
    #[serde(rename = "Items")]
    pub items: Option<Vec<HashMap<String, AttributeValue>>>,
    #[serde(rename = "LastEvaluatedKey")]
    pub last_evaluated_key: Option<Json>,
    #[serde(rename = "Responses")]
    pub responses: Option<HashMap<String, Vec<HashMap<String, AttributeValue>>>>,
    #[serde(rename = "UnprocessedKeys")]
    pub unprocessed_keys: Option<Json>,
    #[serde(rename = "UnprocessedItems")]
    pub unprocessed_items: Option<Json>,
}

impl Reply {
    /// Parses the response body. Empty text is an empty response.
    pub fn parse(body: crate::ops::Json) -> Result<Reply, serde_json::Error> {
        match body {
            crate::ops::Json::Text(t) if t.trim().is_empty() => Ok(Reply::default()),
            crate::ops::Json::Text(t) => serde_json::from_str(&t),
            crate::ops::Json::Value(v) => serde_json::from_value(v),
            crate::ops::Json::Request(r) => serde_json::from_value(r.to_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_values_round_trip_through_json() {
        let mut m = HashMap::new();
        m.insert("n".to_string(), AttributeValue::N("42".into()));
        let value = AttributeValue::L(vec![
            AttributeValue::S("x".into()),
            AttributeValue::B(Blob::from(vec![1, 2, 3])),
            AttributeValue::Bool(true),
            AttributeValue::Null(true),
            AttributeValue::M(m),
            AttributeValue::Ss(vec!["a".into(), "b".into()]),
            AttributeValue::Ns(vec!["1".into()]),
            AttributeValue::Bs(vec![Blob::from(vec![9])]),
        ]);
        let json = value.to_json();
        assert_eq!(json["L"][1]["B"], "AQID");
        assert_eq!(AttributeValue::from_json(&json).unwrap(), value);
    }

    #[test]
    fn request_body_uses_wire_field_names() {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("a".into()));
        let r = Request::new(AlternatorOp::PutItem)
            .table_name("t")
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk)".into());
        let json: Json = serde_json::from_str(&r.to_json_string()).unwrap();
        assert_eq!(json["TableName"], "t");
        assert_eq!(json["Item"]["pk"]["S"], "a");
        assert_eq!(json["ConditionExpression"], "attribute_not_exists(pk)");
    }
}
