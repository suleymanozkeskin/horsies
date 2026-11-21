//! Strict JSON serialization that rejects non-finite floats (NaN, ±Infinity) at
//! any depth.
//!
//! Plain `serde_json` silently encodes a non-finite `f32`/`f64` as JSON `null`,
//! which either corrupts the value or turns an `Option<f64>` into `None` on the
//! far side. These helpers fail closed instead, so a non-finite value is
//! rejected at the producer boundary rather than persisted. This mirrors
//! Python's producer-side fence (`json.dumps(allow_nan=False)` plus
//! `_validate_json_native`, PR #84).
//!
//! [`RejectNonFinite`] is a transparent [`Serializer`] adapter: it forwards
//! every operation to the wrapped serializer except `serialize_f32`/
//! `serialize_f64`, which error on a non-finite value. The same newtype wraps
//! each compound sub-serializer, and every nested value is re-wrapped via
//! [`Wrap`], so the check applies at any depth.

use serde::ser::{
    self, Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant, Serializer,
};

/// Serialize `value` to JSON bytes, rejecting any non-finite float.
///
/// The text is `serde_json::to_vec`'s compact text, with each float written
/// as Python's `json.dumps` writes it ([`PythonFloats`]).
pub fn to_json_bytes_strict<T: Serialize + ?Sized>(
    value: &T,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = Vec::with_capacity(128);
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, PythonFloats);
    value.serialize(RejectNonFinite(&mut ser))?;
    Ok(buf)
}

/// serde_json's compact formatter, except for floats: a finite float is
/// written as Python's `repr` writes it, which is the text of `json.dumps`
/// (`1e-05`, `1.5e-07`, `1e+16`, `0.0001`, `123.0`). serde_json writes the
/// same shortest digits with another notation (`0.00001`, `1.5e-7`), so a
/// float that the core stores would differ from the one Python stores.
///
/// [`RejectNonFinite`] refuses a non-finite float before the formatter
/// sees it.
struct PythonFloats;

impl serde_json::ser::Formatter for PythonFloats {
    fn write_f32<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f32,
    ) -> std::io::Result<()> {
        writer.write_all(python_float_text(&format!("{value:e}")).as_bytes())
    }

    fn write_f64<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f64,
    ) -> std::io::Result<()> {
        writer.write_all(python_float_text(&format!("{value:e}")).as_bytes())
    }
}

/// The decimal exponents that Python writes in fixed notation: `repr`
/// writes `d.ddd` times ten to an exponent from -4 to 15 without an
/// exponent (`Python/pystrtod.c`, `format_float_short`: `decpt` from -3 to
/// 16).
const PYTHON_FIXED_EXPONENTS: std::ops::RangeInclusive<i32> = -4..=15;

/// Python's `repr` text of a finite float, from Rust's `{:e}` text of the
/// same float (`-1.5e-7`, `1e16`, `0e0`).
///
/// Both give the shortest digits that read back to the float; they differ
/// only in notation. Python writes an exponent in
/// [`PYTHON_FIXED_EXPONENTS`] in fixed notation, with `.0` after an
/// integral value; any other exponent as `d.ddde±XX`, with a sign and at
/// least two exponent digits, and no `.` after a single digit. Pure.
fn python_float_text(scientific: &str) -> String {
    let (negative, unsigned) = match scientific.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, scientific),
    };
    let (mantissa, exponent) = unsigned
        .split_once('e')
        .expect("Rust's `{:e}` text of a float has an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust's `{:e}` text of a float has a decimal exponent");
    let digits: String = mantissa
        .chars()
        .filter(|&character| character != '.')
        .collect();
    let mut text = String::with_capacity(digits.len() + 8);
    if negative {
        text.push('-');
    }
    match PYTHON_FIXED_EXPONENTS.contains(&exponent) {
        true => push_fixed(&mut text, &digits, exponent),
        false => push_scientific(&mut text, &digits, exponent),
    }
    text
}

/// `digits` times ten to `exponent` (counted from the first digit) in fixed
/// notation, with at least one digit after the point.
fn push_fixed(text: &mut String, digits: &str, exponent: i32) {
    match usize::try_from(exponent) {
        Ok(power) => {
            let integer_digits = power + 1;
            match digits.len() <= integer_digits {
                true => {
                    text.push_str(digits);
                    text.extend(std::iter::repeat_n('0', integer_digits - digits.len()));
                    text.push_str(".0");
                }
                false => {
                    let (integer, fraction) = digits.split_at(integer_digits);
                    text.push_str(integer);
                    text.push('.');
                    text.push_str(fraction);
                }
            }
        }
        Err(_) => {
            // A negative exponent here is -4 to -1: 3 to 0 zeros after the point.
            let leading_zeros =
                usize::try_from(exponent.unsigned_abs() - 1).expect("a u32 fits a usize");
            text.push_str("0.");
            text.extend(std::iter::repeat_n('0', leading_zeros));
            text.push_str(digits);
        }
    }
}

/// `digits` times ten to `exponent` as `d.ddde±XX`.
fn push_scientific(text: &mut String, digits: &str, exponent: i32) {
    let (first, rest) = digits.split_at(1);
    text.push_str(first);
    if !rest.is_empty() {
        text.push('.');
        text.push_str(rest);
    }
    let sign = match exponent < 0 {
        true => '-',
        false => '+',
    };
    text.push_str(&format!("e{sign}{:02}", exponent.unsigned_abs()));
}

/// Serialize `value` to a `serde_json::Value`, rejecting any non-finite float.
///
/// Unlike `serde_json::to_value`, which routes a non-finite float to
/// `Value::Null`, this fails closed on the first NaN/±Infinity.
pub fn to_json_value_strict<T: Serialize + ?Sized>(
    value: &T,
) -> Result<serde_json::Value, serde_json::Error> {
    value.serialize(RejectNonFinite(serde_json::value::Serializer))
}

fn non_finite_error<E: ser::Error>(kind: &str, repr: &str) -> E {
    E::custom(format!(
        "non-finite {kind} float ({repr}) is not valid JSON; RFC 8259 forbids NaN and Infinity",
    ))
}

/// A transparent serializer that rejects non-finite floats and forwards
/// everything else to `S`.
struct RejectNonFinite<S>(S);

/// Wraps a value so it is (re)serialized through [`RejectNonFinite`], carrying
/// the float check into nested elements/fields/keys/values.
struct Wrap<'a, T: ?Sized>(&'a T);

impl<T: ?Sized + Serialize> Serialize for Wrap<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(RejectNonFinite(serializer))
    }
}

impl<S: Serializer> Serializer for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = RejectNonFinite<S::SerializeSeq>;
    type SerializeTuple = RejectNonFinite<S::SerializeTuple>;
    type SerializeTupleStruct = RejectNonFinite<S::SerializeTupleStruct>;
    type SerializeTupleVariant = RejectNonFinite<S::SerializeTupleVariant>;
    type SerializeMap = RejectNonFinite<S::SerializeMap>;
    type SerializeStruct = RejectNonFinite<S::SerializeStruct>;
    type SerializeStructVariant = RejectNonFinite<S::SerializeStructVariant>;

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        if v.is_finite() {
            self.0.serialize_f32(v)
        } else {
            Err(non_finite_error("f32", &v.to_string()))
        }
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        if v.is_finite() {
            self.0.serialize_f64(v)
        } else {
            Err(non_finite_error("f64", &v.to_string()))
        }
    }

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bool(v)
    }
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i8(v)
    }
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i16(v)
    }
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i32(v)
    }
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i64(v)
    }
    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_i128(v)
    }
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u8(v)
    }
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u16(v)
    }
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u32(v)
    }
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u64(v)
    }
    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_u128(v)
    }
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_char(v)
    }
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_str(v)
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bytes(v)
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_some(&Wrap(value))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_struct(name)
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, variant_index, variant)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_struct(name, &Wrap(value))
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0
            .serialize_newtype_variant(name, variant_index, variant, &Wrap(value))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(RejectNonFinite(self.0.serialize_seq(len)?))
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(RejectNonFinite(self.0.serialize_tuple(len)?))
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(RejectNonFinite(self.0.serialize_tuple_struct(name, len)?))
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(RejectNonFinite(self.0.serialize_tuple_variant(
            name,
            variant_index,
            variant,
            len,
        )?))
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(RejectNonFinite(self.0.serialize_map(len)?))
    }
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(RejectNonFinite(self.0.serialize_struct(name, len)?))
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(RejectNonFinite(self.0.serialize_struct_variant(
            name,
            variant_index,
            variant,
            len,
        )?))
    }

    fn collect_str<T: ?Sized + std::fmt::Display>(
        self,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.collect_str(value)
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

impl<S: SerializeSeq> SerializeSeq for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_element(&Wrap(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S: SerializeTuple> SerializeTuple for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_element(&Wrap(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S: SerializeTupleStruct> SerializeTupleStruct for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_field(&Wrap(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S: SerializeTupleVariant> SerializeTupleVariant for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_field(&Wrap(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S: SerializeMap> SerializeMap for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.0.serialize_key(&Wrap(key))
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_value(&Wrap(value))
    }
    fn serialize_entry<K: ?Sized + Serialize, V: ?Sized + Serialize>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error> {
        self.0.serialize_entry(&Wrap(key), &Wrap(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S: SerializeStruct> SerializeStruct for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_field(key, &Wrap(value))
    }
    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        self.0.skip_field(key)
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

impl<S: SerializeStructVariant> SerializeStructVariant for RejectNonFinite<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.0.serialize_field(key, &Wrap(value))
    }
    fn skip_field(&mut self, key: &'static str) -> Result<(), Self::Error> {
        self.0.skip_field(key)
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

#[cfg(test)]
mod tests {
    use super::{to_json_bytes_strict, to_json_value_strict};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    /// The strict encoder's text of `value`.
    fn to_json_string<T: Serialize + ?Sized>(value: &T) -> Result<String, serde_json::Error> {
        to_json_bytes_strict(value)
            .map(|bytes| String::from_utf8(bytes).expect("serde_json writes UTF-8"))
    }

    /// `serde_json/arbitrary_precision` re-routes every number through a map
    /// token, which every serde buffering path then rejects for a typed float.
    /// The flag applies to the whole build graph, so turning it on here would
    /// break `#[serde(flatten)]`, `#[serde(tag)]` and `#[serde(untagged)]`
    /// containers in consumer crates that horsies never sees. It stays behind
    /// the off-by-default `arbitrary-precision` feature.
    #[cfg(not(feature = "arbitrary-precision"))]
    #[test]
    fn default_build_leaves_serde_buffering_paths_usable_for_floats() {
        #[derive(Deserialize)]
        struct Area {
            rooms: f64,
        }

        #[derive(Deserialize)]
        #[serde(tag = "category")]
        enum Tagged {
            Flat(Area),
        }

        #[derive(Deserialize)]
        struct Flattened {
            #[serde(flatten)]
            area: Area,
        }

        let tagged: Tagged =
            serde_json::from_str(r#"{"category":"Flat","rooms":1.5}"#).expect("tagged enum");
        let Tagged::Flat(area) = tagged;
        assert_eq!(area.rooms, 1.5);

        let flattened: Flattened =
            serde_json::from_str(r#"{"rooms":1.5}"#).expect("flattened struct");
        assert_eq!(flattened.area.rooms, 1.5);
    }

    #[derive(Serialize, Deserialize)]
    struct Metric {
        name: String,
        value: f64,
    }

    #[derive(Serialize)]
    struct Nested {
        inner: Vec<Metric>,
        ratio: f32,
    }

    #[test]
    fn finite_floats_serialize_identically_to_serde_json() {
        let m = Metric {
            name: "ok".to_owned(),
            value: 1.5,
        };
        let strict = to_json_bytes_strict(&m).expect("finite serializes");
        let plain = serde_json::to_vec(&m).expect("serde_json");
        assert_eq!(strict, plain, "finite output must be byte-identical");
    }

    /// Each float is written as Python 3.13's `json.dumps` writes it (the
    /// texts come from `json.dumps(value)`), and reads back to itself.
    #[test]
    fn floats_are_written_as_python_writes_them() {
        let table: [(f64, &str); 20] = [
            (0.1, "0.1"),
            (123.0, "123.0"),
            (-0.0, "-0.0"),
            (0.0, "0.0"),
            (-2.5, "-2.5"),
            (12.5, "12.5"),
            (1_234_567.0, "1234567.0"),
            (0.0001, "0.0001"),
            (1e-05, "1e-05"),
            (2.5e-05, "2.5e-05"),
            (1.5e-07, "1.5e-07"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.2345e22, "1.2345e+22"),
            (123_456_789_012_345_680.0, "1.2345678901234568e+17"),
            (1e100, "1e+100"),
            (-1e-300, "-1e-300"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (0.300_000_000_000_000_04, "0.30000000000000004"),
        ];
        for (value, python) in table {
            let text = to_json_string(&value).expect("a finite float encodes");
            assert_eq!(text, python, "{value:e}");
            let back: f64 = serde_json::from_str(&text).expect("the text reads back");
            assert_eq!(back.to_bits(), value.to_bits(), "{python}");
        }
    }

    /// An `f32` takes the same notation, with its own shortest digits.
    #[test]
    fn an_f32_is_written_in_python_notation() {
        assert_eq!(to_json_string(&0.1_f32).unwrap(), "0.1");
        assert_eq!(to_json_string(&1e-7_f32).unwrap(), "1e-07");
        assert_eq!(to_json_string(&2.0_f32).unwrap(), "2.0");
    }

    /// A float inside a `serde_json::Value` takes the same notation.
    #[test]
    fn a_float_inside_a_json_value_is_written_in_python_notation() {
        let value = serde_json::json!({"f": 1e-05, "g": [1e16, 0.5]});
        assert_eq!(
            to_json_string(&value).unwrap(),
            r#"{"f":1e-05,"g":[1e+16,0.5]}"#
        );
    }

    #[test]
    fn f64_max_and_min_round_trip() {
        for value in [f64::MAX, f64::MIN, f64::MIN_POSITIVE, 0.0, -0.0] {
            let m = Metric {
                name: "edge".to_owned(),
                value,
            };
            let bytes = to_json_bytes_strict(&m).expect("finite edge serializes");
            let back: Metric = serde_json::from_slice(&bytes).expect("round-trip");
            assert_eq!(back.value.to_bits(), value.to_bits());
        }
    }

    #[test]
    fn nan_in_struct_field_is_rejected() {
        let m = Metric {
            name: "bad".to_owned(),
            value: f64::NAN,
        };
        let err = to_json_bytes_strict(&m).expect_err("NaN must be rejected");
        assert!(err.to_string().contains("non-finite"), "{err}");
    }

    #[test]
    fn infinity_variants_are_rejected() {
        for value in [f64::INFINITY, f64::NEG_INFINITY] {
            let m = Metric {
                name: "bad".to_owned(),
                value,
            };
            assert!(
                to_json_value_strict(&m).is_err(),
                "±Infinity must be rejected"
            );
        }
    }

    #[test]
    fn non_finite_f32_is_rejected() {
        let n = Nested {
            inner: vec![],
            ratio: f32::INFINITY,
        };
        let err = to_json_bytes_strict(&n).expect_err("non-finite f32 must be rejected");
        assert!(err.to_string().contains("non-finite"), "{err}");
    }

    #[test]
    fn non_finite_is_rejected_at_any_depth() {
        // Nested inside a Vec inside a struct.
        let n = Nested {
            inner: vec![Metric {
                name: "deep".to_owned(),
                value: f64::NAN,
            }],
            ratio: 1.0,
        };
        assert!(
            to_json_bytes_strict(&n).is_err(),
            "nested NaN must be rejected"
        );

        // Inside a map value.
        let mut map: HashMap<String, f64> = HashMap::new();
        map.insert("k".to_owned(), f64::INFINITY);
        assert!(
            to_json_value_strict(&map).is_err(),
            "map-value Inf must be rejected"
        );

        // As a bare positional value (top-level).
        assert!(
            to_json_bytes_strict(&f64::NAN).is_err(),
            "top-level NaN must be rejected"
        );
    }

    #[test]
    fn integers_and_strings_pass_through() {
        // i128/u128 must not regress to the trait's "unsupported" default.
        let big: i128 = i128::MAX;
        assert_eq!(
            to_json_bytes_strict(&big).unwrap(),
            serde_json::to_vec(&big).unwrap()
        );
        let s = "hello";
        assert_eq!(
            to_json_bytes_strict(&s).unwrap(),
            serde_json::to_vec(&s).unwrap()
        );
    }
}
