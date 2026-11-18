//! A JSON value kept as the text that it was read or written with.

use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use super::strict_read::{check_json_text, StrictJsonError};

/// A JSON value as the text that it was read or written with: a value that
/// user code built, such as the output of a child workflow.
///
/// Without the `arbitrary-precision` feature, `serde_json::Value` holds an
/// integer outside the `i64` and `u64` range as an `f64`. The text keeps
/// every digit, so [`JsonText::decode`] gives the value that the writer
/// stored (`u128::MAX`, `i64::MIN - 1`), as Python reads any integer.
///
/// The text passes the strict read rule of a stored text
/// ([`super::check_json_text`]): at most [`super::MAX_JSON_DEPTH`] levels,
/// no repeated key, no non-finite number. Two values are equal when they are
/// the same text.
#[derive(Debug, Clone)]
pub struct JsonText(Box<RawValue>);

impl JsonText {
    /// The value of the JSON text `text`, without the whitespace around it.
    ///
    /// Errors: the refusal of the strict read rule. Pure.
    pub fn parse(text: &str) -> Result<Self, StrictJsonError> {
        check_json_text(text)?;
        Ok(Self(raw_value_of_checked(text)))
    }

    /// A member or an element of a text that passed the strict read rule:
    /// the part passes the rule too. Pure.
    pub(crate) fn of_checked_part(part: &RawValue) -> Self {
        Self(part.to_owned())
    }

    /// The text of the value.
    pub fn get(&self) -> &str {
        self.0.get()
    }

    /// Decode the value as `T` from its text. The strict read rule bounds the
    /// depth of the text, so serde_json's own limit of 128 levels is off, as
    /// in [`super::from_json_str`].
    ///
    /// Errors: the `serde_json::Error` of a text that does not decode as `T`.
    /// Pure.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        super::decode_checked(serde_json::Deserializer::from_str(self.get()))
    }
}

impl PartialEq for JsonText {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl Eq for JsonText {}

impl std::fmt::Display for JsonText {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.get())
    }
}

/// Writes the text as it is.
impl Serialize for JsonText {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

/// Reads the text of the value under the strict read rule of a stored text.
/// serde_json's reader gives the text, and so does a `serde_json::Value`
/// (which already holds a wide integer as an `f64`). A deserializer that
/// buffers the value (`#[serde(flatten)]`, an untagged or an internally
/// tagged enum) gives no text, and the read fails.
impl<'de> Deserialize<'de> for JsonText {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        check_json_text(raw.get()).map_err(D::Error::custom)?;
        Ok(Self(raw))
    }
}

/// The raw value of a text that passed the strict read rule. Its depth is
/// bounded, so serde_json's own recursion limit is off.
///
/// Panics: when serde_json does not read the text as one value, a defect of
/// one of the two readers.
fn raw_value_of_checked(text: &str) -> Box<RawValue> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    deserializer.disable_recursion_limit();
    let raw = Box::<RawValue>::deserialize(&mut deserializer)
        .expect("a text that passed the strict read rule reads as one JSON value");
    deserializer
        .end()
        .expect("a text that passed the strict read rule holds one JSON value");
    raw
}

#[cfg(test)]
mod tests {
    use super::JsonText;

    const U128_MAX_TEXT: &str = "340282366920938463463374607431768211455";
    const BELOW_I64_MIN_TEXT: &str = "-9223372036854775809";

    #[test]
    fn an_integer_outside_i64_and_u64_keeps_its_digits_and_decodes() {
        let wide = JsonText::parse(U128_MAX_TEXT).unwrap();
        assert_eq!(wide.get(), U128_MAX_TEXT);
        assert_eq!(wide.decode::<u128>().unwrap(), u128::MAX);
        let below = JsonText::parse(BELOW_I64_MIN_TEXT).unwrap();
        assert_eq!(below.get(), BELOW_I64_MIN_TEXT);
        assert_eq!(below.decode::<i128>().unwrap(), i128::from(i64::MIN) - 1);
    }

    #[test]
    fn the_text_is_kept_without_the_whitespace_around_it() {
        let value = JsonText::parse(" {\"a\": [1e-05, 2]}\n").unwrap();
        assert_eq!(value.get(), "{\"a\": [1e-05, 2]}");
        assert_eq!(value, JsonText::parse("{\"a\": [1e-05, 2]}").unwrap());
        assert_ne!(value, JsonText::parse("{\"a\":[1e-05,2]}").unwrap());
    }

    #[test]
    fn a_text_that_breaks_the_strict_read_rule_is_refused() {
        for text in ["", "NaN", r#"{"a":1,"a":2}"#, "[1] [2]", "1e400"] {
            assert!(JsonText::parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn a_value_serializes_as_its_text_and_reads_back() {
        let text = format!(r#"{{"wide":{U128_MAX_TEXT},"below":[{BELOW_I64_MIN_TEXT}]}}"#);
        let value = JsonText::parse(&text).unwrap();
        assert_eq!(serde_json::to_string(&value).unwrap(), text);
        let back: JsonText = serde_json::from_str(&text).unwrap();
        assert_eq!(back, value);
        assert!(serde_json::from_str::<JsonText>(r#"{"a":1,"a":2}"#).is_err());
    }
}
