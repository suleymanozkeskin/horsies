//! Canonical content inside the rerun-input storage envelope.

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub const INPUT_ENVELOPE_VERSION: i16 = 1;
pub const INPUT_ENVELOPE_CODEC: &str = "json-utf8";
pub const INPUT_ENVELOPE_CONTENT_TYPE: &str = "application/json";
pub const INPUT_ENVELOPE_INLINE_MAX_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq)]
pub struct ReconstructedInput {
    pub args: Vec<Value>,
    pub kwargs: Map<String, Value>,
    pub options: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InputEnvelopeDecodeError {
    #[error("input envelope version {0} is unknown")]
    VersionUnknown(i16),
    #[error("input envelope is corrupt: {0}")]
    Corrupt(String),
}

pub fn encode_input_envelope_v1(
    args: &[Value],
    kwargs: &Map<String, Value>,
    options: Option<&Map<String, Value>>,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut object = Map::new();
    object.insert("args".to_owned(), Value::Array(args.to_vec()));
    object.insert("kwargs".to_owned(), Value::Object(kwargs.clone()));
    object.insert(
        "options".to_owned(),
        options.cloned().map(Value::Object).unwrap_or(Value::Null),
    );
    serde_json::to_vec(&Value::Object(object))
}

pub fn decode_input_envelope(
    version: i16,
    payload: &[u8],
    digest: &[u8],
) -> Result<ReconstructedInput, InputEnvelopeDecodeError> {
    if version != INPUT_ENVELOPE_VERSION {
        return Err(InputEnvelopeDecodeError::VersionUnknown(version));
    }
    if Sha256::digest(payload).as_slice() != digest {
        return Err(InputEnvelopeDecodeError::Corrupt(
            "payload digest disagrees with the stored digest".to_owned(),
        ));
    }
    let parsed: Value = serde_json::from_slice(payload).map_err(|error| {
        InputEnvelopeDecodeError::Corrupt(format!("payload is not JSON: {error}"))
    })?;
    let mut content = parsed
        .as_object()
        .cloned()
        .ok_or_else(|| InputEnvelopeDecodeError::Corrupt("content is not an object".to_owned()))?;
    if content.len() != 3
        || !content.contains_key("args")
        || !content.contains_key("kwargs")
        || !content.contains_key("options")
    {
        return Err(InputEnvelopeDecodeError::Corrupt(
            "content keys are not exactly args, kwargs, options".to_owned(),
        ));
    }
    let args = content
        .remove("args")
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| InputEnvelopeDecodeError::Corrupt("args is not a list".to_owned()))?;
    let kwargs = content
        .remove("kwargs")
        .and_then(|value| value.as_object().cloned())
        .ok_or_else(|| InputEnvelopeDecodeError::Corrupt("kwargs is not an object".to_owned()))?;
    let options = match content.remove("options") {
        Some(Value::Null) => None,
        Some(Value::Object(options)) => Some(options),
        _ => {
            return Err(InputEnvelopeDecodeError::Corrupt(
                "options is neither an object nor null".to_owned(),
            ));
        }
    };
    Ok(ReconstructedInput {
        args,
        kwargs,
        options,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(payload: &[u8]) -> Result<ReconstructedInput, InputEnvelopeDecodeError> {
        decode_input_envelope(1, payload, &Sha256::digest(payload))
    }

    #[test]
    fn canonical_content_is_compact_sorted_and_utf8() {
        let args = vec![Value::String("ü".to_owned())];
        let kwargs = serde_json::from_value(serde_json::json!({"z": 1, "a": 2})).unwrap();
        let payload = encode_input_envelope_v1(&args, &kwargs, None).unwrap();
        assert_eq!(
            payload,
            "{\"args\":[\"ü\"],\"kwargs\":{\"a\":2,\"z\":1},\"options\":null}".as_bytes()
        );
        assert_eq!(decode(&payload).unwrap().options, None);
    }

    #[test]
    fn digest_is_checked_before_json_and_unknown_version_is_typed() {
        assert!(matches!(
            decode_input_envelope(1, b"not json", &[0; 32]),
            Err(InputEnvelopeDecodeError::Corrupt(ref detail)) if detail.contains("digest")
        ));
        assert!(matches!(
            decode_input_envelope(2, b"{}", &Sha256::digest(b"{}")),
            Err(InputEnvelopeDecodeError::VersionUnknown(2))
        ));
    }

    #[test]
    fn foreign_content_shapes_fail_closed() {
        for payload in [
            b"[]".as_slice(),
            br#"{"args":{},"kwargs":{},"options":null}"#.as_slice(),
            br#"{"args":[],"kwargs":[],"options":null}"#.as_slice(),
            br#"{"args":[],"kwargs":{},"options":7}"#.as_slice(),
            br#"{"args":[],"kwargs":{}}"#.as_slice(),
            br#"{"args":[],"kwargs":{},"options":null,"extra":1}"#.as_slice(),
        ] {
            assert!(matches!(
                decode(payload),
                Err(InputEnvelopeDecodeError::Corrupt(_))
            ));
        }
    }
}
