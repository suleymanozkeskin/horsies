//! The contract of the core's monitoring JSON, read into closed types.
//!
//! `core/src/monitoring/model_contract.json` holds, per model, its fields
//! and the kind of value that each field holds. A Python test writes it from
//! the Pydantic models, and a core test checks the Rust models against it.
//! The file is a fact of the core: a shape that this module does not know is
//! a defect of the file, and the read panics with the place and the cause.
//!
//! The kinds:
//!
//! - `"string"`, `"uuid"`, `"integer"`, `"number"`, `"boolean"`,
//!   `"date-time"`;
//! - `{"nullable": kind}`, `{"list": kind}`, `{"map": kind}` (an object
//!   with text keys);
//! - `{"enum": {"name": name, "values": [value, ...]}}`;
//! - `{"model": {"fields": {field: kind, ...}, "name": name}}`;
//! - `{"union": {"name": name, "tag": tag, "variants": {tag value:
//!   {"fields": {field: kind, ...}, "name": name}}}}`: objects told apart
//!   by the text of the field `tag`, which the variant fields leave out.
//!
//! Each enum, model, union and variant has the name of its Python and Rust
//! type (the class name of a variant). The keys of a kind object are in
//! code-point order, as the Python writer sorts them.
//!
//! The Python writer can also write `{"const": value}`. No model has one, so
//! this module does not read it: a `const` kind is refused like any other
//! unknown shape.

use std::collections::BTreeSet;

use horsies_core::events::WireValue;

/// The deepest nesting that the read follows, counted in kinds and field
/// lists. The contract of today nests 19 levels at its deepest: the time of
/// the placement of a claim of a segment of the timing of a node of
/// `WorkflowRunDetail`. A deeper file is refused, so every walk over the
/// read types is bounded by this number.
pub const MAX_KIND_DEPTH: usize = 32;

/// The name of the contract file in the messages of a refused read.
const CONTRACT_FILE: &str = "model_contract.json";

/// A kind that holds one JSON value with no fields of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafKind {
    /// `"string"`: a text.
    Text,
    /// `"uuid"`: a UUID text.
    Uuid,
    /// `"integer"`: a whole number.
    Integer,
    /// `"number"`: a float.
    Number,
    /// `"boolean"`: `true` or `false`.
    Boolean,
    /// `"date-time"`: a date-time text.
    DateTime,
}

/// The kind of value that one field holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractKind {
    Leaf(LeafKind),
    /// `null`, or a value of the inner kind. The inner kind is never
    /// nullable.
    Nullable(Box<ContractKind>),
    List(Box<ContractKind>),
    /// An object with text keys, each value of the inner kind.
    Map(Box<ContractKind>),
    /// The enum `name`: one of the texts, in file order. There is at least
    /// one, and no text is given twice.
    Enum {
        name: String,
        values: Vec<String>,
    },
    /// The model `name`: an object with these fields. There is at least one
    /// field.
    Model {
        name: String,
        fields: Vec<ContractField>,
    },
    /// The union `name`: an object whose field `tag` names its variant.
    /// There is at least one variant, and no variant has a field named
    /// `tag`.
    Union {
        name: String,
        tag: String,
        variants: Vec<ContractVariant>,
    },
}

/// One field of a model or of a variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractField {
    /// The key of the field in the JSON.
    pub key: String,
    pub kind: ContractKind,
}

/// One variant of a union.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractVariant {
    /// The text of the tag field that names this variant.
    pub tag_value: String,
    /// The name of the variant's type.
    pub name: String,
    /// The fields of the variant, the tag left out. A variant can have none.
    pub fields: Vec<ContractField>,
}

/// One model of the contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractModel {
    pub name: String,
    /// The fields of the model. There is at least one.
    pub fields: Vec<ContractField>,
}

/// The models of the contract `text`, in file order. Pure.
///
/// A text that is not a contract (not JSON, or a shape that this module does
/// not know) is a defect of the core's file: the read panics with the place
/// and the cause.
pub fn parse_contract(text: &str) -> Vec<ContractModel> {
    let value = text
        .parse::<WireValue>()
        .unwrap_or_else(|cause| panic!("{CONTRACT_FILE} is not JSON: {cause}"));
    let models = object_entries(CONTRACT_FILE, &value);
    assert!(!models.is_empty(), "{CONTRACT_FILE} has no model");
    models
        .into_iter()
        .map(|(name, fields)| ContractModel {
            name: name.to_owned(),
            fields: model_fields(name, fields, 0),
        })
        .collect()
}

/// The entries of the JSON object `value` at `place`, in file order.
/// Another JSON value is a defect.
fn object_entries<'a>(place: &str, value: &'a WireValue) -> Vec<(&'a str, &'a WireValue)> {
    match value {
        WireValue::Object(entries) => entries
            .iter()
            .map(|(key, entry)| (key.as_str(), entry))
            .collect(),
        WireValue::Null
        | WireValue::Bool(_)
        | WireValue::Number(_)
        | WireValue::String(_)
        | WireValue::Array(_) => panic!("{place}: an object is expected, not {value}"),
    }
}

/// The text `value` at `place`. Another JSON value is a defect.
fn text_of(place: &str, value: &WireValue) -> String {
    match value {
        WireValue::String(text) => text.clone(),
        WireValue::Null
        | WireValue::Bool(_)
        | WireValue::Number(_)
        | WireValue::Array(_)
        | WireValue::Object(_) => panic!("{place}: a text is expected, not {value}"),
    }
}

/// The fields of a model at `place`: at least one.
fn model_fields(place: &str, value: &WireValue, depth: usize) -> Vec<ContractField> {
    let fields = field_list(place, value, depth);
    assert!(!fields.is_empty(), "{place}: a model has no field");
    fields
}

/// The fields of the object `value` at `place`, in file order. The object
/// can be empty.
fn field_list(place: &str, value: &WireValue, depth: usize) -> Vec<ContractField> {
    assert!(
        depth <= MAX_KIND_DEPTH,
        "{place}: the kinds nest deeper than {MAX_KIND_DEPTH}"
    );
    object_entries(place, value)
        .into_iter()
        .map(|(key, kind)| ContractField {
            key: key.to_owned(),
            kind: parse_kind(&format!("{place}.{key}"), kind, depth + 1),
        })
        .collect()
}

/// The kind `value` at `place`: a leaf name, or an object with one key.
fn parse_kind(place: &str, value: &WireValue, depth: usize) -> ContractKind {
    assert!(
        depth <= MAX_KIND_DEPTH,
        "{place}: the kinds nest deeper than {MAX_KIND_DEPTH}"
    );
    match value {
        WireValue::String(name) => ContractKind::Leaf(leaf_kind(place, name)),
        WireValue::Object(_) => composite_kind(place, value, depth),
        WireValue::Null | WireValue::Bool(_) | WireValue::Number(_) | WireValue::Array(_) => {
            panic!("{place}: a kind is a text or an object, not {value}")
        }
    }
}

/// The leaf kind of the name `name` at `place`.
fn leaf_kind(place: &str, name: &str) -> LeafKind {
    match name {
        "string" => LeafKind::Text,
        "uuid" => LeafKind::Uuid,
        "integer" => LeafKind::Integer,
        "number" => LeafKind::Number,
        "boolean" => LeafKind::Boolean,
        "date-time" => LeafKind::DateTime,
        unknown => panic!("{place}: {unknown:?} is not a kind of the contract"),
    }
}

/// The kind of the object `value` at `place`: one key, which names the kind.
fn composite_kind(place: &str, value: &WireValue, depth: usize) -> ContractKind {
    let entries = object_entries(place, value);
    let [(name, inner)] = entries.as_slice() else {
        panic!(
            "{place}: a kind object has one key, not {}: {value}",
            entries.len()
        );
    };
    match *name {
        "nullable" => ContractKind::Nullable(Box::new(non_null_kind(place, inner, depth))),
        "list" => ContractKind::List(Box::new(parse_kind(place, inner, depth + 1))),
        "map" => ContractKind::Map(Box::new(parse_kind(place, inner, depth + 1))),
        "enum" => enum_kind(place, inner),
        "model" => model_kind(place, inner, depth + 1),
        "union" => union_kind(place, inner, depth + 1),
        unknown => panic!("{place}: {unknown:?} is not a kind of the contract"),
    }
}

/// The inner kind of a nullable kind at `place`. A nullable inner kind is a
/// defect: `null` of `null` has no meaning.
fn non_null_kind(place: &str, value: &WireValue, depth: usize) -> ContractKind {
    let kind = parse_kind(place, value, depth + 1);
    match kind {
        ContractKind::Nullable(_) => panic!("{place}: a nullable kind holds a nullable kind"),
        ContractKind::Leaf(_)
        | ContractKind::List(_)
        | ContractKind::Map(_)
        | ContractKind::Enum { .. }
        | ContractKind::Model { .. }
        | ContractKind::Union { .. } => kind,
    }
}

/// The enum at `place`: an object with the keys `name` and `values`.
fn enum_kind(place: &str, value: &WireValue) -> ContractKind {
    let entries = object_entries(place, value);
    let [("name", name), ("values", values)] = entries.as_slice() else {
        let keys: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
        panic!("{place}: an enum has the keys [\"name\", \"values\"], not {keys:?}");
    };
    ContractKind::Enum {
        name: text_of(place, name),
        values: enum_values(place, values),
    }
}

/// The texts of an enum at `place`: a non-empty list of distinct texts.
fn enum_values(place: &str, value: &WireValue) -> Vec<String> {
    let WireValue::Array(items) = value else {
        panic!("{place}: the values of an enum are a list, not {value}");
    };
    assert!(!items.is_empty(), "{place}: an enum has no value");
    let values: Vec<String> = items
        .iter()
        .map(|item| match item {
            WireValue::String(text) => text.clone(),
            WireValue::Null
            | WireValue::Bool(_)
            | WireValue::Number(_)
            | WireValue::Array(_)
            | WireValue::Object(_) => panic!("{place}: an enum value is a text, not {item}"),
        })
        .collect();
    let distinct: BTreeSet<&str> = values.iter().map(String::as_str).collect();
    assert_eq!(
        distinct.len(),
        values.len(),
        "{place}: an enum gives a value twice: {value}"
    );
    values
}

/// The model at `place`: an object with the keys `fields` and `name`.
fn model_kind(place: &str, value: &WireValue, depth: usize) -> ContractKind {
    let (name, fields) = named_fields(place, value);
    ContractKind::Model {
        name,
        fields: model_fields(place, fields, depth),
    }
}

/// The name and the field object of the named object `value` at `place`
/// (a model or a variant): the keys `fields` and `name`.
fn named_fields<'a>(place: &str, value: &'a WireValue) -> (String, &'a WireValue) {
    let entries = object_entries(place, value);
    let [("fields", fields), ("name", name)] = entries.as_slice() else {
        let keys: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
        panic!("{place}: a model or a variant has the keys [\"fields\", \"name\"], not {keys:?}");
    };
    (text_of(place, name), *fields)
}

/// The union at `place`: an object with the keys `name`, `tag` and
/// `variants`.
fn union_kind(place: &str, value: &WireValue, depth: usize) -> ContractKind {
    let entries = object_entries(place, value);
    let [("name", name), ("tag", tag_value), ("variants", variants_value)] = entries.as_slice()
    else {
        let keys: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
        panic!("{place}: a union has the keys [\"name\", \"tag\", \"variants\"], not {keys:?}");
    };
    let WireValue::String(tag) = tag_value else {
        panic!("{place}: the tag of a union is a text, not {tag_value}");
    };
    let variants: Vec<ContractVariant> = object_entries(place, variants_value)
        .into_iter()
        .map(|(variant_tag, variant_value)| {
            variant(
                &format!("{place}::{variant_tag}"),
                tag,
                variant_tag,
                variant_value,
                depth,
            )
        })
        .collect();
    assert!(!variants.is_empty(), "{place}: a union has no variant");
    ContractKind::Union {
        name: text_of(place, name),
        tag: tag.clone(),
        variants,
    }
}

/// The variant `tag_value` at `place` of a union with the tag field `tag`.
/// A variant field named `tag` is a defect: the tag is not a field.
fn variant(
    place: &str,
    tag: &str,
    tag_value: &str,
    value: &WireValue,
    depth: usize,
) -> ContractVariant {
    let (name, fields) = named_fields(place, value);
    let fields = field_list(place, fields, depth + 1);
    assert!(
        fields.iter().all(|field| field.key != tag),
        "{place}: a variant has a field named as the tag {tag:?}"
    );
    ContractVariant {
        tag_value: tag_value.to_owned(),
        name,
        fields,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The kinds of the fields of the one model of `text`.
    fn kinds_of(text: &str) -> Vec<(String, ContractKind)> {
        let models = parse_contract(text);
        assert_eq!(models.len(), 1, "one model");
        models[0]
            .fields
            .iter()
            .map(|field| (field.key.clone(), field.kind.clone()))
            .collect()
    }

    #[test]
    fn each_leaf_name_is_its_leaf_kind() {
        let kinds = kinds_of(
            r#"{"Sample": {"a": "string", "b": "uuid", "c": "integer", "d": "number",
                "e": "boolean", "f": "date-time"}}"#,
        );

        assert_eq!(
            kinds,
            vec![
                ("a".to_owned(), ContractKind::Leaf(LeafKind::Text)),
                ("b".to_owned(), ContractKind::Leaf(LeafKind::Uuid)),
                ("c".to_owned(), ContractKind::Leaf(LeafKind::Integer)),
                ("d".to_owned(), ContractKind::Leaf(LeafKind::Number)),
                ("e".to_owned(), ContractKind::Leaf(LeafKind::Boolean)),
                ("f".to_owned(), ContractKind::Leaf(LeafKind::DateTime)),
            ]
        );
    }

    #[test]
    fn a_wrapping_kind_holds_its_inner_kind() {
        let kinds = kinds_of(
            r#"{"Sample": {"a": {"nullable": "string"}, "b": {"list": {"nullable": "number"}},
                "c": {"map": "integer"}}}"#,
        );

        assert_eq!(
            kinds,
            vec![
                (
                    "a".to_owned(),
                    ContractKind::Nullable(Box::new(ContractKind::Leaf(LeafKind::Text)))
                ),
                (
                    "b".to_owned(),
                    ContractKind::List(Box::new(ContractKind::Nullable(Box::new(
                        ContractKind::Leaf(LeafKind::Number)
                    ))))
                ),
                (
                    "c".to_owned(),
                    ContractKind::Map(Box::new(ContractKind::Leaf(LeafKind::Integer)))
                ),
            ]
        );
    }

    #[test]
    fn an_enum_a_model_and_a_union_keep_their_names_and_the_file_order() {
        let kinds = kinds_of(
            r#"{"Sample": {
                "e": {"enum": {"name": "Mode", "values": ["B", "A"]}},
                "m": {"model": {"fields": {"y": "string", "x": "integer"}, "name": "Total"}},
                "u": {"union": {"name": "State", "tag": "kind", "variants": {
                    "one": {"fields": {}, "name": "StateOne"},
                    "two": {"fields": {"at": "date-time"}, "name": "StateTwo"}}}}
            }}"#,
        );

        assert_eq!(
            kinds,
            vec![
                (
                    "e".to_owned(),
                    ContractKind::Enum {
                        name: "Mode".to_owned(),
                        values: vec!["B".to_owned(), "A".to_owned()],
                    }
                ),
                (
                    "m".to_owned(),
                    ContractKind::Model {
                        name: "Total".to_owned(),
                        fields: vec![
                            ContractField {
                                key: "y".to_owned(),
                                kind: ContractKind::Leaf(LeafKind::Text),
                            },
                            ContractField {
                                key: "x".to_owned(),
                                kind: ContractKind::Leaf(LeafKind::Integer),
                            },
                        ],
                    }
                ),
                (
                    "u".to_owned(),
                    ContractKind::Union {
                        name: "State".to_owned(),
                        tag: "kind".to_owned(),
                        variants: vec![
                            ContractVariant {
                                tag_value: "one".to_owned(),
                                name: "StateOne".to_owned(),
                                fields: Vec::new(),
                            },
                            ContractVariant {
                                tag_value: "two".to_owned(),
                                name: "StateTwo".to_owned(),
                                fields: vec![ContractField {
                                    key: "at".to_owned(),
                                    kind: ContractKind::Leaf(LeafKind::DateTime),
                                }],
                            },
                        ],
                    }
                ),
            ]
        );
    }

    #[test]
    fn the_core_contract_has_27_models_within_the_depth_bound() {
        let models = parse_contract(include_str!(
            "../../../../../core/src/monitoring/model_contract.json"
        ));

        assert_eq!(models.len(), 27);
        assert_eq!(models[0].name, "Breakdown");
        assert_eq!(models[26].name, "WorkflowTaskDetail");
    }

    #[test]
    #[should_panic(expected = "model_contract.json is not JSON")]
    fn a_text_that_is_not_json_is_a_defect() {
        let _models = parse_contract("{\"Sample\": ");
    }

    #[test]
    #[should_panic(expected = "model_contract.json: an object is expected")]
    fn a_contract_that_is_not_an_object_is_a_defect() {
        let _models = parse_contract("[]");
    }

    #[test]
    #[should_panic(expected = "model_contract.json has no model")]
    fn a_contract_without_a_model_is_a_defect() {
        let _models = parse_contract("{}");
    }

    #[test]
    #[should_panic(expected = "Sample: a model has no field")]
    fn a_model_without_a_field_is_a_defect() {
        let _models = parse_contract(r#"{"Sample": {}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.at: \"date\" is not a kind of the contract")]
    fn an_unknown_leaf_name_is_a_defect() {
        let _models = parse_contract(r#"{"Sample": {"at": "date"}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.flag: \"const\" is not a kind of the contract")]
    fn a_const_kind_is_a_defect() {
        let _models = parse_contract(r#"{"Sample": {"flag": {"const": "on"}}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.count: a kind is a text or an object")]
    fn a_kind_that_is_a_number_is_a_defect() {
        let _models = parse_contract(r#"{"Sample": {"count": 1}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.count: a kind object has one key, not 2")]
    fn a_kind_object_with_two_keys_is_a_defect() {
        let _models =
            parse_contract(r#"{"Sample": {"count": {"list": "integer", "map": "integer"}}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.note: a nullable kind holds a nullable kind")]
    fn a_nullable_of_a_nullable_is_a_defect() {
        let _models =
            parse_contract(r#"{"Sample": {"note": {"nullable": {"nullable": "string"}}}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.mode: an enum has no value")]
    fn an_enum_without_a_value_is_a_defect() {
        let _models =
            parse_contract(r#"{"Sample": {"mode": {"enum": {"name": "Mode", "values": []}}}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.mode: an enum value is a text")]
    fn an_enum_value_that_is_not_a_text_is_a_defect() {
        let _models = parse_contract(
            r#"{"Sample": {"mode": {"enum": {"name": "Mode", "values": ["A", 1]}}}}"#,
        );
    }

    #[test]
    #[should_panic(expected = "Sample.mode: an enum gives a value twice")]
    fn an_enum_that_gives_a_value_twice_is_a_defect() {
        let _models = parse_contract(
            r#"{"Sample": {"mode": {"enum": {"name": "Mode", "values": ["A", "A"]}}}}"#,
        );
    }

    #[test]
    #[should_panic(
        expected = "Sample.state: a union has the keys [\"name\", \"tag\", \"variants\"]"
    )]
    fn a_union_without_its_three_keys_is_a_defect() {
        let _models =
            parse_contract(r#"{"Sample": {"state": {"union": {"name": "State", "tag": "kind"}}}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.state: the tag of a union is a text")]
    fn a_union_tag_that_is_not_a_text_is_a_defect() {
        let _models = parse_contract(
            r#"{"Sample": {"state": {"union": {"name": "State", "tag": 1,
                "variants": {"on": {"fields": {}, "name": "On"}}}}}}"#,
        );
    }

    #[test]
    #[should_panic(expected = "Sample.state: a union has no variant")]
    fn a_union_without_a_variant_is_a_defect() {
        let _models = parse_contract(
            r#"{"Sample": {"state": {"union": {"name": "State", "tag": "kind", "variants": {}}}}}"#,
        );
    }

    #[test]
    #[should_panic(expected = "Sample.state::on: a variant has a field named as the tag \"kind\"")]
    fn a_variant_field_named_as_the_tag_is_a_defect() {
        let _models = parse_contract(
            r#"{"Sample": {"state": {"union": {"name": "State", "tag": "kind",
                "variants": {"on": {"fields": {"kind": "string"}, "name": "On"}}}}}}"#,
        );
    }

    #[test]
    #[should_panic(expected = "Sample.mode: an object is expected")]
    fn an_enum_without_its_name_is_a_defect() {
        let _models = parse_contract(r#"{"Sample": {"mode": {"enum": ["A"]}}}"#);
    }

    #[test]
    #[should_panic(
        expected = "Sample.total: a model or a variant has the keys [\"fields\", \"name\"], not [\"count\"]"
    )]
    fn a_model_without_its_name_is_a_defect() {
        let _models = parse_contract(r#"{"Sample": {"total": {"model": {"count": "integer"}}}}"#);
    }

    #[test]
    #[should_panic(expected = "Sample.total: a text is expected")]
    fn a_name_that_is_not_a_text_is_a_defect() {
        let _models = parse_contract(
            r#"{"Sample": {"total": {"model": {"fields": {"count": "integer"}, "name": 1}}}}"#,
        );
    }

    #[test]
    #[should_panic(expected = "the kinds nest deeper than 32")]
    fn a_contract_deeper_than_the_bound_is_a_defect() {
        let mut kind = "\"string\"".to_owned();
        for _level in 0..=MAX_KIND_DEPTH {
            kind = format!("{{\"list\": {kind}}}");
        }

        let _models = parse_contract(&format!("{{\"Sample\": {{\"deep\": {kind}}}}}"));
    }
}
