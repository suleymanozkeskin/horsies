//! The JSON body of an action route: no body, or a JSON object whose keys
//! the body type of the route declares.

use axum::body::to_bytes;
use axum::extract::Request;
use axum::http::header;
use serde::de::{DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

use super::common::ApiError;

/// The body of a workflow action (`pause`, `resume`, `cancel`): no body, or
/// a JSON object with no keys. Every key is unknown.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkflowActionBody {}

/// Read the body of an action route as its body type `T`
/// ([`parse_action_body`]).
///
/// Reads the request body to its end, and writes nothing. It does not bound
/// the read: [`super::MonitoringHandler::handle`] refuses a body over
/// [`super::MAX_REQUEST_BODY_BYTES`] with 413 before any route reads it, so
/// the size refusal comes before every refusal of this function. A body
/// that the transport fails to give is refused with FastAPI's 422
/// `value_error` and the transport's text.
pub(super) async fn read_action_body<T: DeserializeOwned + Default>(
    request: Request,
) -> Result<T, ApiError> {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = to_bytes(request.into_body(), usize::MAX)
        .await
        .map_err(|error| ApiError::body_validation(error.to_string(), Value::Null))?;
    parse_action_body(content_type.as_deref(), &bytes)
}

/// The body type `T` of the body `bytes`. Pure.
///
/// - No body is `T::default()`, with any content type.
/// - A body is read only when the content type starts with
///   `application/json`. Any other body is refused with FastAPI's 422
///   `value_error` "Input should be a valid JSON object".
/// - JSON text that is not an object is refused with the same issue.
/// - The first key of the object, in body order, that `T` does not declare
///   is refused with the 422 `extra_forbidden` issue at `["body", key]`
///   ([`ApiError::unknown_body_key`]): the issue names the key and the keys
///   that `T` declares.
/// - Text that is not JSON, and a known key whose value `T` refuses, are
///   refused with the 422 `value_error` issue at `["body"]` and serde's
///   text.
///
/// `T` is a struct with named fields that derives `Deserialize`: the derive
/// gives its keys to [`Deserializer::deserialize_struct`]. Another `T` is a
/// defect and panics.
fn parse_action_body<T: DeserializeOwned + Default>(
    content_type: Option<&str>,
    bytes: &[u8],
) -> Result<T, ApiError> {
    let json_content = content_type.is_some_and(|value| value.starts_with("application/json"));
    match (bytes.is_empty(), json_content) {
        (true, _) => Ok(T::default()),
        (false, false) => Err(not_an_object(bytes)),
        (false, true) => match serde_json::from_slice::<Value>(bytes) {
            Ok(Value::Object(object)) => {
                T::deserialize(DeclaredKeysOnly(object)).map_err(|refused| match refused {
                    BodyRefused::UnknownKey {
                        key,
                        value,
                        known_keys,
                    } => ApiError::unknown_body_key(key, value, known_keys),
                    BodyRefused::Invalid(message) => {
                        ApiError::body_validation(message, body_text(bytes))
                    }
                })
            }
            Ok(
                Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::String(_)
                | Value::Array(_),
            ) => Err(not_an_object(bytes)),
            Err(error) => Err(ApiError::body_validation(
                error.to_string(),
                body_text(bytes),
            )),
        },
    }
}

/// The refusal of a body that is not a JSON object.
fn not_an_object(bytes: &[u8]) -> ApiError {
    ApiError::body_validation("Input should be a valid JSON object", body_text(bytes))
}

/// The body as the `input` of a refusal: its text, with each byte sequence
/// that is not UTF-8 as U+FFFD.
fn body_text(bytes: &[u8]) -> Value {
    Value::String(String::from_utf8_lossy(bytes).into_owned())
}

/// Why a JSON object did not give a body type.
#[derive(Debug, thiserror::Error)]
enum BodyRefused {
    /// A key of the object that the body type does not declare: the first
    /// one in body order, with its value.
    #[error("unknown key {key:?}")]
    UnknownKey {
        key: String,
        value: Value,
        known_keys: &'static [&'static str],
    },
    /// Every other refusal, as serde's text: for example a value of the
    /// wrong type for a known key.
    #[error("{0}")]
    Invalid(String),
}

impl serde::de::Error for BodyRefused {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Invalid(message.to_string())
    }
}

/// A deserializer of one JSON object for a struct with named fields. It
/// refuses a key that the struct does not declare before it reads any
/// field, and reads the fields with serde_json.
struct DeclaredKeysOnly(Map<String, Value>);

impl<'de> Deserializer<'de> for DeclaredKeysOnly {
    type Error = BodyRefused;

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Self(object) = self;
        if let Some((key, value)) = object
            .iter()
            .find(|(key, _value)| !fields.contains(&key.as_str()))
        {
            return Err(BodyRefused::UnknownKey {
                key: key.clone(),
                value: value.clone(),
                known_keys: fields,
            });
        }
        Value::Object(object)
            .deserialize_struct(name, fields, visitor)
            .map_err(|refused| BodyRefused::Invalid(refused.to_string()))
    }

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unreachable!(
            "an action body type is a struct with named fields, which serde's derive reads with deserialize_struct"
        )
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }
}
