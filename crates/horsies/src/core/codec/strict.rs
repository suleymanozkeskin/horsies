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
/// Byte-for-byte identical to `serde_json::to_vec` for finite inputs.
pub fn to_json_bytes_strict<T: Serialize + ?Sized>(
    value: &T,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = Vec::with_capacity(128);
    let mut ser = serde_json::Serializer::new(&mut buf);
    value.serialize(RejectNonFinite(&mut ser))?;
    Ok(buf)
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
