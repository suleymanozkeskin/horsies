//! The strict JSON text of a value that a deserializer of any serde format
//! gives through serde's public data model (`deserialize_any`): the `input`
//! member of a schedule in a TOML or a JSON config text.

use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, Error, MapAccess, SeqAccess, Visitor};
use serde::Serialize;

use super::json_text::JsonText;
use super::strict_read::MAX_JSON_DEPTH;

/// The value that `deserializer` gives, as its strict JSON text
/// ([`json_text_of_any_format`]), or `None` for a none or a unit (`null` in
/// a JSON text). For `#[serde(deserialize_with)]` on an optional member of a
/// config.
///
/// Errors: as [`json_text_of_any_format`]. Reads only from `deserializer`.
pub(crate) fn optional_json_text_of_any_format<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<JsonText>, D::Error> {
    deserializer.deserialize_option(OptionalText)
}

/// The value that `deserializer` gives through `deserialize_any`, written as
/// strict JSON text:
/// - an integer of `i64`, `u64`, `i128` or `u128` with every digit;
/// - a float as the strict writer writes it ([`super::to_json_string`]);
/// - a text and a boolean as JSON, a none or a unit as `null`;
/// - a sequence as an array and a map with text keys as an object, in the
///   order that the format gives.
///
/// The text holds the value, not the spelling of the source: no whitespace,
/// each number in the form of the strict writer. A format that gives an
/// integer outside the `i64` and `u64` range as an `f64` (serde_json's
/// untyped read) gives a float.
///
/// Errors (the format's error, with the cause): a non-finite float; a map
/// key that is not a text; a value with no JSON form (bytes, an enum);
/// sequences, maps and wrapped values nested deeper than [`MAX_JSON_DEPTH`]
/// levels; a text that breaks the strict read rule (a repeated key). Reads
/// only from `deserializer`.
fn json_text_of_any_format<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<JsonText, D::Error> {
    let mut text = String::new();
    ValueText {
        text: &mut text,
        depth: 0,
        before: "",
    }
    .deserialize(deserializer)?;
    JsonText::parse(&text).map_err(|refused| {
        D::Error::custom(format!(
            "the value breaks the strict JSON read rule: {refused}"
        ))
    })
}

/// The visitor of an optional value: a none or a unit is no value; any
/// other value is its strict JSON text.
struct OptionalText;

impl<'de> Visitor<'de> for OptionalText {
    type Value = Option<JsonText>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value or null")
    }

    fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        json_text_of_any_format(deserializer).map(Some)
    }
}

/// Appends the strict JSON text of one value to `text`.
struct ValueText<'text> {
    text: &'text mut String,
    /// The count of sequences, maps and wrapped values around the value.
    depth: usize,
    /// Written before the value: `","` before an element that is not the
    /// first of its sequence, else `""`.
    before: &'static str,
}

impl ValueText<'_> {
    /// Appends the value inside a wrapper (an option or a newtype struct),
    /// one level deeper.
    ///
    /// Errors: the format's error, or a depth past [`MAX_JSON_DEPTH`].
    fn write_wrapped<'de, D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        let depth = nested_depth::<D::Error>(self.depth)?;
        ValueText {
            text: self.text,
            depth,
            before: "",
        }
        .deserialize(deserializer)
    }
}

impl<'de> DeserializeSeed<'de> for ValueText<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        self.text.push_str(self.before);
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ValueText<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "a JSON value: a text, a number, a boolean, a sequence or a map with text keys",
        )
    }

    fn visit_bool<E: Error>(self, value: bool) -> Result<(), E> {
        push_written(self.text, &value)
    }

    fn visit_i64<E: Error>(self, value: i64) -> Result<(), E> {
        push_written(self.text, &value)
    }

    fn visit_i128<E: Error>(self, value: i128) -> Result<(), E> {
        push_written(self.text, &value)
    }

    fn visit_u64<E: Error>(self, value: u64) -> Result<(), E> {
        push_written(self.text, &value)
    }

    fn visit_u128<E: Error>(self, value: u128) -> Result<(), E> {
        push_written(self.text, &value)
    }

    /// The strict writer refuses a non-finite float.
    fn visit_f64<E: Error>(self, value: f64) -> Result<(), E> {
        push_written(self.text, &value)
    }

    fn visit_str<E: Error>(self, value: &str) -> Result<(), E> {
        push_written(self.text, &value)
    }

    fn visit_none<E: Error>(self) -> Result<(), E> {
        self.text.push_str("null");
        Ok(())
    }

    fn visit_unit<E: Error>(self) -> Result<(), E> {
        self.text.push_str("null");
        Ok(())
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        self.write_wrapped(deserializer)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        self.write_wrapped(deserializer)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let depth = nested_depth::<A::Error>(self.depth)?;
        let text = self.text;
        text.push('[');
        let mut before = "";
        while seq
            .next_element_seed(ValueText {
                text: &mut *text,
                depth,
                before,
            })?
            .is_some()
        {
            before = ",";
        }
        text.push(']');
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let depth = nested_depth::<A::Error>(self.depth)?;
        let text = self.text;
        text.push('{');
        let mut before = "";
        while map
            .next_key_seed(KeyText {
                text: &mut *text,
                before,
            })?
            .is_some()
        {
            text.push(':');
            map.next_value_seed(ValueText {
                text: &mut *text,
                depth,
                before: "",
            })?;
            before = ",";
        }
        text.push('}');
        Ok(())
    }
}

/// Appends a map key, which must be a text, to `text` as a JSON string.
struct KeyText<'text> {
    text: &'text mut String,
    /// Written before the key: `","` before a member that is not the first
    /// of its map, else `""`.
    before: &'static str,
}

impl<'de> DeserializeSeed<'de> for KeyText<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for KeyText<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map key that is a text")
    }

    fn visit_str<E: Error>(self, key: &str) -> Result<(), E> {
        self.text.push_str(self.before);
        push_written(self.text, &key)
    }
}

/// The depth of a value inside a sequence, a map or a wrapped value that is
/// at `depth`.
///
/// Errors: a depth past [`MAX_JSON_DEPTH`], the bound of the strict read
/// rule. Pure.
fn nested_depth<E: Error>(depth: usize) -> Result<usize, E> {
    let nested = depth + 1;
    if nested > MAX_JSON_DEPTH {
        return Err(E::custom(format!(
            "the value is nested deeper than {MAX_JSON_DEPTH} levels"
        )));
    }
    Ok(nested)
}

/// Appends the strict writer's text of a scalar to `text`.
///
/// Errors: the writer's refusal (a non-finite float), with the cause.
fn push_written<T: Serialize, E: Error>(text: &mut String, value: &T) -> Result<(), E> {
    let written = super::to_json_string(value)
        .map_err(|refused| E::custom(format!("the value does not write as JSON: {refused}")))?;
    text.push_str(&written);
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::de::value::{
        BytesDeserializer, Error as ValueError, F64Deserializer, I128Deserializer, MapDeserializer,
        U128Deserializer,
    };

    use super::json_text_of_any_format;

    #[test]
    fn an_integer_of_i128_or_u128_keeps_every_digit() {
        let wide = json_text_of_any_format(U128Deserializer::<ValueError>::new(u128::MAX))
            .expect("a u128 writes");
        assert_eq!(wide.get(), "340282366920938463463374607431768211455");
        let below = json_text_of_any_format(I128Deserializer::<ValueError>::new(
            i128::from(i64::MIN) - 1,
        ))
        .expect("an i128 writes");
        assert_eq!(below.get(), "-9223372036854775809");
    }

    #[test]
    fn a_float_is_written_by_the_strict_writer_and_a_non_finite_one_is_refused() {
        let finite = json_text_of_any_format(F64Deserializer::<ValueError>::new(1e-5))
            .expect("a finite float writes");
        assert_eq!(finite.get(), "1e-05");
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let refused = json_text_of_any_format(F64Deserializer::<ValueError>::new(value))
                .expect_err("a non-finite float is refused");
            assert!(refused.to_string().contains("non-finite"), "{refused}");
        }
    }

    #[test]
    fn a_value_of_every_json_kind_writes_as_compact_text() {
        let mut deserializer = serde_json::Deserializer::from_str(
            "{ \"t\": \"\u{e9}\\n\", \"list\": [true, null, -5, 7], \"empty\": {} }",
        );
        let text = json_text_of_any_format(&mut deserializer).expect("a JSON value writes");
        assert_eq!(
            text.get(),
            "{\"t\":\"\u{e9}\\n\",\"list\":[true,null,-5,7],\"empty\":{}}"
        );
    }

    #[test]
    fn a_map_key_that_is_not_a_text_is_refused() {
        let refused: ValueError =
            json_text_of_any_format(MapDeserializer::new([(1_u8, true)].into_iter()))
                .expect_err("an integer key is refused");
        assert!(
            refused.to_string().contains("a map key that is a text"),
            "{refused}"
        );
    }

    #[test]
    fn a_value_with_no_json_form_is_refused() {
        let refused: ValueError =
            json_text_of_any_format(BytesDeserializer::new(b"ab")).expect_err("bytes are refused");
        assert!(refused.to_string().contains("byte array"), "{refused}");
    }

    #[test]
    fn a_repeated_key_is_refused_by_the_strict_read_rule() {
        let mut deserializer = serde_json::Deserializer::from_str(r#"{"a":1,"a":2}"#);
        let refused =
            json_text_of_any_format(&mut deserializer).expect_err("a repeated key is refused");
        assert!(refused.to_string().contains("repeats the key"), "{refused}");
    }
}
