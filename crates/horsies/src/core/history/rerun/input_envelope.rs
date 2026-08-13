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
    canonical_json_bytes(&Value::Object(object))
}

/// Python `json.dumps(sort_keys=True, separators=(',', ':'),
/// ensure_ascii=False)` compatible JSON bytes.
///
/// `serde_json` and CPython agree on shortest finite-number mantissas but use
/// different exponent spelling. Python requires a sign and at least two
/// exponent digits; normalize that spelling recursively while retaining
/// serde's UTF-8 string escaping and the map's sorted-key order.
pub(crate) fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    let mut output = Vec::new();
    write_canonical_json(value, &mut output)?;
    Ok(output)
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> Result<(), serde_json::Error> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::Number(number) => write_python_number(number, output)?,
        Value::String(value) => serde_json::to_writer(output, value)?,
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_canonical_json(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn write_python_number(
    number: &serde_json::Number,
    output: &mut Vec<u8>,
) -> Result<(), serde_json::Error> {
    let source = number.to_string();
    let source_is_float = source.contains('.') || source.contains(['e', 'E']);
    if !source_is_float {
        output.extend_from_slice(if source == "-0" {
            b"0"
        } else {
            source.as_bytes()
        });
        return Ok(());
    }
    // `arbitrary_precision` retains the source lexeme. Python first parses it
    // to binary64, then `repr` chooses the shortest round-tripping digits.
    let value = number
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            <serde_json::Error as serde::ser::Error>::custom(
                "JSON float is outside the finite binary64 domain",
            )
        })?;
    let rendered = serde_json::Number::from_f64(value)
        .expect("finite binary64 is a JSON number")
        .to_string();

    let (negative, unsigned) = rendered
        .strip_prefix('-')
        .map(|value| (true, value))
        .unwrap_or((false, rendered.as_str()));
    let (mantissa, explicit_exponent) = unsigned
        .split_once(['e', 'E'])
        .map(|(mantissa, exponent)| (mantissa, exponent.parse::<i32>().expect("JSON exponent")))
        .unwrap_or((unsigned, 0));
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));

    let (mut digits, normalized_exponent) = if integer.bytes().any(|byte| byte != b'0') {
        let integer = integer.trim_start_matches('0');
        (
            format!("{integer}{fraction}"),
            explicit_exponent + i32::try_from(integer.len()).expect("JSON number length") - 1,
        )
    } else if let Some(first_nonzero) = fraction.bytes().position(|byte| byte != b'0') {
        (
            fraction[first_nonzero..].to_owned(),
            explicit_exponent - i32::try_from(first_nonzero).expect("JSON number length") - 1,
        )
    } else {
        if negative {
            output.push(b'-');
        }
        output.extend_from_slice(b"0.0");
        return Ok(());
    };
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    if negative {
        output.push(b'-');
    }
    if !(-4..16).contains(&normalized_exponent) {
        output.push(digits.as_bytes()[0]);
        if digits.len() > 1 {
            output.push(b'.');
            output.extend_from_slice(&digits.as_bytes()[1..]);
        }
        output.push(b'e');
        output.push(if normalized_exponent < 0 { b'-' } else { b'+' });
        let exponent_digits = normalized_exponent.unsigned_abs().to_string();
        if exponent_digits.len() < 2 {
            output.push(b'0');
        }
        output.extend_from_slice(exponent_digits.as_bytes());
        return Ok(());
    }

    let point = normalized_exponent + 1;
    if point <= 0 {
        output.extend_from_slice(b"0.");
        output.extend(std::iter::repeat_n(
            b'0',
            usize::try_from(-point).expect("nonnegative zero count"),
        ));
        output.extend_from_slice(digits.as_bytes());
    } else if usize::try_from(point).expect("positive decimal point") >= digits.len() {
        output.extend_from_slice(digits.as_bytes());
        output.extend(std::iter::repeat_n(
            b'0',
            usize::try_from(point).expect("positive decimal point") - digits.len(),
        ));
        output.extend_from_slice(b".0");
    } else {
        let point = usize::try_from(point).expect("positive decimal point");
        output.extend_from_slice(&digits.as_bytes()[..point]);
        output.push(b'.');
        output.extend_from_slice(&digits.as_bytes()[point..]);
    }
    Ok(())
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
    validate_finite_float_domain(&parsed)?;
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

fn validate_finite_float_domain(value: &Value) -> Result<(), InputEnvelopeDecodeError> {
    match value {
        Value::Number(number) => {
            let rendered = number.to_string();
            if (rendered.contains('.') || rendered.contains(['e', 'E']))
                && !number.as_f64().is_some_and(f64::is_finite)
            {
                return Err(InputEnvelopeDecodeError::Corrupt(
                    "number is outside the finite binary64 domain".to_owned(),
                ));
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_finite_float_domain(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_finite_float_domain(value)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_hex(payload: &[u8]) -> String {
        Sha256::digest(payload)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

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

    #[test]
    fn float_exponents_are_byte_identical_to_python_canonical_json() {
        let args = vec![
            serde_json::json!(1e-7),
            serde_json::json!(1e-5),
            serde_json::json!(1.2e-5),
        ];
        let kwargs = serde_json::from_value(serde_json::json!({
            "v": 1e-6,
            "fixed": 1e15,
            "scientific": 1e16,
        }))
        .unwrap();
        let options =
            serde_json::from_value(serde_json::json!({"nested": [1e20, 1e-4, -0.0]})).unwrap();
        let payload = encode_input_envelope_v1(&args, &kwargs, Some(&options)).unwrap();
        assert_eq!(
            payload,
            br#"{"args":[1e-07,1e-05,1.2e-05],"kwargs":{"fixed":1000000000000000.0,"scientific":1e+16,"v":1e-06},"options":{"nested":[1e+20,0.0001,-0.0]}}"#
        );
        assert_eq!(
            digest_hex(&payload),
            "4780347df5020e967547d62686831d24f63b0ed02e07b4b027d118311674962b"
        );
    }

    #[test]
    fn arbitrary_precision_integers_survive_decode_and_reencode() {
        let payload = br#"{"args":[123456789012345678901234567890],"kwargs":{},"options":null}"#;
        let decoded = decode_input_envelope(1, payload, &Sha256::digest(payload)).unwrap();
        let encoded = encode_input_envelope_v1(&decoded.args, &decoded.kwargs, None).unwrap();
        assert_eq!(encoded, payload);
        assert_eq!(
            digest_hex(&encoded),
            "318d9f0872c8b55c45a1d7a2d78ce098da25a8740b5d71f577a18e1f6f621bca"
        );
    }

    #[test]
    fn source_number_lexemes_normalize_like_python_loads_then_dumps() {
        let payload = br#"{"args":[0.000010,1.2300,1e0,-0,-0.0],"kwargs":{},"options":null}"#;
        let decoded = decode_input_envelope(1, payload, &Sha256::digest(payload)).unwrap();
        let encoded = encode_input_envelope_v1(&decoded.args, &decoded.kwargs, None).unwrap();
        assert_eq!(
            encoded,
            br#"{"args":[1e-05,1.23,1.0,0,-0.0],"kwargs":{},"options":null}"#
        );
    }

    #[test]
    fn out_of_binary64_float_range_fails_closed_without_panicking() {
        let payload = br#"{"args":[1e9999],"kwargs":{},"options":null}"#;
        assert!(matches!(
            decode_input_envelope(1, payload, &Sha256::digest(payload)),
            Err(InputEnvelopeDecodeError::Corrupt(ref detail))
                if detail.contains("finite binary64")
        ));
    }
}
