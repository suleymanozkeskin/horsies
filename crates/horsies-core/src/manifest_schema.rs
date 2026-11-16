//! The manifest types as static data for clients.
//!
//! `#[derive(horsies_macros::ManifestSchema)]` on a manifest struct or enum
//! records its Python form from the type itself:
//! - a struct of named fields is a frozen dataclass;
//! - an enum with a serde `tag` is one frozen dataclass per variant, with a
//!   `Literal` tag field, and a union alias of the variants;
//! - an enum of unit variants is a `Literal` alias of its wire values.
//!
//! A field's Python type comes from [`ManifestValue`] of its Rust type. A
//! newtype with a bound implements it by hand next to its parser, so the
//! bound has one definition. `horsies-codegen` renders [`PRODUCER_MANIFEST`]
//! as `_catalog/manifest.py`.

use crate::manifest::{
    DatabaseManifest, PoolRecycle, ProducerManifest, SchemaMode, SendManifest, SessionDatabase,
    PRODUCER_MANIFEST_VERSION,
};

/// An inclusive integer range with the name of its Python constants
/// (`<name>_MIN` and `<name>_MAX`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntBound {
    pub name: &'static str,
    pub min: u64,
    pub max: u64,
}

/// The Python type of a manifest value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PythonValue {
    Bool,
    Str,
    Int(IntBound),
    /// A manifest type of the same module, by its Python name.
    Named(&'static str),
}

/// A Rust type that a manifest field can have.
pub trait ManifestValue {
    const PYTHON: PythonValue;
}

impl ManifestValue for bool {
    const PYTHON: PythonValue = PythonValue::Bool;
}

/// One field of a dataclass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestField {
    /// The field name; the JSON key is the same text.
    pub name: &'static str,
    /// The field doc, from the field's `///` lines.
    pub doc: &'static str,
    pub value: PythonValue,
}

/// One variant of a tagged enum: a dataclass whose tag field holds
/// `tag_value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaggedVariant {
    pub python_class: &'static str,
    pub tag_value: &'static str,
    pub doc: &'static str,
    pub fields: &'static [ManifestField],
}

/// One wire value of an enum of unit variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChoiceValue {
    pub value: &'static str,
    pub doc: &'static str,
}

/// How a manifest type is written in Python.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestShape {
    /// A frozen dataclass.
    Record { fields: &'static [ManifestField] },
    /// One frozen dataclass per variant and a union alias.
    Tagged {
        tag_field: &'static str,
        variants: &'static [TaggedVariant],
    },
    /// A `Literal` alias.
    Choice { values: &'static [ChoiceValue] },
}

/// One manifest type that has its own Python definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestType {
    /// The class name, or the alias name of a tagged or unit enum.
    pub python_name: &'static str,
    /// The type doc, from the type's `///` lines.
    pub doc: &'static str,
    pub shape: ManifestShape,
}

/// A manifest type that the derive recorded.
pub trait ManifestItem: ManifestValue {
    const ITEM: ManifestType;
}

/// The manifest of one process kind, as the generator renders it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestModule {
    /// The Python name of the root type, which the dump function takes.
    pub root: &'static str,
    /// The manifest version this core reads.
    pub version: u32,
    /// Every type of the manifest, in render order.
    pub types: &'static [ManifestType],
}

/// The producer manifest: the input of a `CoreHandle`.
pub const PRODUCER_MANIFEST: ManifestModule = ManifestModule {
    root: "ProducerManifest",
    version: PRODUCER_MANIFEST_VERSION,
    types: &[
        <SchemaMode as ManifestItem>::ITEM,
        <SessionDatabase as ManifestItem>::ITEM,
        <PoolRecycle as ManifestItem>::ITEM,
        <DatabaseManifest as ManifestItem>::ITEM,
        <SendManifest as ManifestItem>::ITEM,
        <ProducerManifest as ManifestItem>::ITEM,
    ],
};

impl ManifestModule {
    /// The type of `python_name`, if the module has one.
    pub fn type_named(&self, python_name: &str) -> Option<&'static ManifestType> {
        self.types
            .iter()
            .find(|manifest_type| manifest_type.python_name == python_name)
    }

    /// Every integer bound of the module's fields, each name once, in the
    /// order of first use.
    pub fn int_bounds(&self) -> Vec<IntBound> {
        let mut bounds: Vec<IntBound> = Vec::new();
        for manifest_type in self.types {
            for field in type_fields(manifest_type) {
                if let PythonValue::Int(bound) = field.value {
                    if !bounds.iter().any(|known| known.name == bound.name) {
                        bounds.push(bound);
                    }
                }
            }
        }
        bounds
    }
}

/// Every field of a type, across its variants.
pub fn type_fields(manifest_type: &ManifestType) -> Vec<ManifestField> {
    match manifest_type.shape {
        ManifestShape::Record { fields } => fields.to_vec(),
        ManifestShape::Tagged { variants, .. } => variants
            .iter()
            .flat_map(|variant| variant.fields.iter().copied())
            .collect(),
        ManifestShape::Choice { .. } => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;
    use crate::manifest::parse_producer_manifest;

    /// Most nesting levels of a sample manifest. The producer manifest has 3.
    const MAX_SAMPLE_DEPTH: usize = 8;

    /// Which end of each integer range a sample takes.
    #[derive(Clone, Copy)]
    enum IntEnd {
        Min,
        Max,
    }

    /// A sample value of `value`. Every tagged enum and every choice takes
    /// its variant at `pick` modulo its variant count.
    fn sample(value: PythonValue, pick: usize, end: IntEnd, depth: usize) -> Value {
        assert!(depth <= MAX_SAMPLE_DEPTH, "the manifest nests too deep");
        match value {
            PythonValue::Bool => Value::Bool(pick % 2 == 1),
            PythonValue::Str => Value::String("postgresql://user:secret@localhost/db".to_owned()),
            PythonValue::Int(bound) => match end {
                IntEnd::Min => Value::from(bound.min),
                IntEnd::Max => Value::from(bound.max),
            },
            PythonValue::Named(name) => {
                let manifest_type = PRODUCER_MANIFEST
                    .type_named(name)
                    .unwrap_or_else(|| panic!("{name} is not a manifest type"));
                sample_type(manifest_type, pick, end, depth + 1)
            }
        }
    }

    fn sample_type(manifest_type: &ManifestType, pick: usize, end: IntEnd, depth: usize) -> Value {
        match manifest_type.shape {
            ManifestShape::Record { fields } => {
                Value::Object(sample_fields(fields, pick, end, depth))
            }
            ManifestShape::Tagged {
                tag_field,
                variants,
            } => {
                let variant = variants[pick % variants.len()];
                let mut object = sample_fields(variant.fields, pick, end, depth);
                object.insert(
                    tag_field.to_owned(),
                    Value::String(variant.tag_value.to_owned()),
                );
                Value::Object(object)
            }
            ManifestShape::Choice { values } => {
                Value::String(values[pick % values.len()].value.to_owned())
            }
        }
    }

    fn sample_fields(
        fields: &[ManifestField],
        pick: usize,
        end: IntEnd,
        depth: usize,
    ) -> Map<String, Value> {
        fields
            .iter()
            .map(|field| (field.name.to_owned(), sample(field.value, pick, end, depth)))
            .collect()
    }

    /// The most variants of any tagged enum or choice of the manifest.
    fn most_variants() -> usize {
        PRODUCER_MANIFEST
            .types
            .iter()
            .map(|manifest_type| match manifest_type.shape {
                ManifestShape::Tagged { variants, .. } => variants.len(),
                ManifestShape::Choice { values } => values.len(),
                ManifestShape::Record { .. } => 1,
            })
            .max()
            .unwrap_or(1)
    }

    fn root() -> PythonValue {
        PythonValue::Named(PRODUCER_MANIFEST.root)
    }

    #[test]
    fn a_manifest_built_from_the_data_parses_for_every_variant_and_bound_end() {
        for pick in 0..most_variants() {
            for end in [IntEnd::Min, IntEnd::Max] {
                let text = sample(root(), pick, end, 0).to_string();
                let parsed = parse_producer_manifest(&text);
                assert!(parsed.is_ok(), "{parsed:?}\n{text}");
            }
        }
    }

    #[test]
    fn a_value_past_a_bound_is_refused() {
        let sample_text = sample(root(), 0, IntEnd::Min, 0);
        for manifest_type in PRODUCER_MANIFEST.types {
            let ManifestShape::Record { fields } = manifest_type.shape else {
                continue;
            };
            for field in fields {
                let PythonValue::Int(bound) = field.value else {
                    continue;
                };
                let outside = [
                    bound.min.checked_sub(1),
                    Some(bound.max + 1).filter(|_| bound.max < u64::from(u32::MAX)),
                ];
                for value in outside.into_iter().flatten() {
                    let mut document = sample_text.clone();
                    let changed = replace_field(&mut document, field.name, Value::from(value));
                    assert!(changed, "{} is in the sample", field.name);
                    assert!(
                        parse_producer_manifest(&document.to_string()).is_err(),
                        "{} = {value} is outside {bound:?}",
                        field.name
                    );
                }
            }
        }
    }

    /// Set every field called `name` in `document` to `value`. Bounded by
    /// the sample depth.
    fn replace_field(document: &mut Value, name: &str, value: Value) -> bool {
        fn walk(node: &mut Value, name: &str, value: &Value, depth: usize) -> bool {
            assert!(depth <= MAX_SAMPLE_DEPTH, "the manifest nests too deep");
            match node {
                Value::Object(object) => {
                    let mut changed = false;
                    for (key, child) in object.iter_mut() {
                        if key == name {
                            *child = value.clone();
                            changed = true;
                        } else {
                            changed |= walk(child, name, value, depth + 1);
                        }
                    }
                    changed
                }
                _ => false,
            }
        }
        walk(document, name, &value, 0)
    }

    #[test]
    fn every_named_type_is_in_the_module_once() {
        for manifest_type in PRODUCER_MANIFEST.types {
            let count = PRODUCER_MANIFEST
                .types
                .iter()
                .filter(|other| other.python_name == manifest_type.python_name)
                .count();
            assert_eq!(count, 1, "{}", manifest_type.python_name);
            for field in type_fields(manifest_type) {
                if let PythonValue::Named(name) = field.value {
                    assert!(PRODUCER_MANIFEST.type_named(name).is_some(), "{name}");
                }
            }
        }
        assert!(PRODUCER_MANIFEST
            .type_named(PRODUCER_MANIFEST.root)
            .is_some());
    }

    #[test]
    fn the_session_database_variants_carry_their_tags() {
        let manifest_type = PRODUCER_MANIFEST
            .type_named("SessionDatabase")
            .expect("the session database is a manifest type");
        let ManifestShape::Tagged {
            tag_field,
            variants,
        } = manifest_type.shape
        else {
            panic!("SessionDatabase is a tagged enum");
        };
        assert_eq!(tag_field, "kind");
        let tags: Vec<(&str, &str)> = variants
            .iter()
            .map(|variant| (variant.python_class, variant.tag_value))
            .collect();
        assert_eq!(
            tags,
            [
                ("SessionDatabaseSameAsDatabaseUrl", "SAME_AS_DATABASE_URL"),
                ("SessionDatabaseSeparate", "SEPARATE"),
            ]
        );
    }

    #[test]
    fn integer_bounds_are_named_once() {
        let names: Vec<&str> = PRODUCER_MANIFEST
            .int_bounds()
            .iter()
            .map(|bound| bound.name)
            .collect();
        assert_eq!(
            names,
            [
                "WHOLE_SECONDS",
                "POOL_SIZE",
                "POOL_CONNECTIONS",
                "RESERVATION_WINDOW_MICROS",
                "MANIFEST_VERSION"
            ]
        );
    }
}
