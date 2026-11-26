//! `monitoring_models.ts`: the monitoring read types and their decoders,
//! rendered from the contract of the core's monitoring JSON
//! (`core/src/monitoring/model_contract.json`, read by [`contract`]).
//!
//! Types:
//!
//! - One readonly interface per model, with its members in contract order
//!   and named in camelCase (`in_flight` gives `inFlight`).
//! - A union is a type alias of one interface per variant. The tag member
//!   (`kind`) holds the variant's tag text in camelCase (`not_recorded` gives
//!   `"notRecorded"`). An enum is a union of its texts, which keep the core's
//!   spelling.
//! - The contract gives no name to a model, a union or an enum inside a
//!   field. Its type name is the name of the type that holds the field, then
//!   the field key in PascalCase (`TaskAttemptInfo.phases` gives
//!   `TaskAttemptInfoPhases`). The interface of a variant is the union name,
//!   then the tag text in PascalCase (`TaskAttemptInfoPhasesNotCaptured`). A
//!   list, a map and a nullable value add nothing to the name. Each inline
//!   copy of a kind gets its own name; two copies give two types with the
//!   same members.
//! - `string` and `uuid` are `string`; `integer` and `number` are `number`;
//!   `boolean` is `boolean`; `date-time` is `Temporal.Instant`. A nullable
//!   kind is `T | null`, a list `ReadonlyArray<T>`, a map
//!   `ReadonlyMap<string, T>`.
//!
//! Decoders: one exported `decode<Model>(value: WireJson):
//! DecodeResult<Model, MonitoringDecodeError>` per model, over the lossless
//! `WireJson` tree (integers are `bigint`, floats `number`, objects `Map`).
//! The decode rules, in the fixed text of [`decode_support`]:
//!
//! - A key of the contract that is not in the object is `fieldMissing`; a
//!   key that the contract does not have is `fieldUnknown`. The core writes
//!   every field, `null` included, and no other.
//! - `integer`: a `bigint` in the safe range of a `number`
//!   (±(2^53 - 1)) becomes a `number`; another `bigint` is
//!   `integerUnsafe`. `number`: a float only; the core writes each float with
//!   a fraction or an exponent.
//! - `date-time`: the core's text, `YYYY-MM-DDTHH:MM:SS` then `.` and six
//!   digits or nothing, then `Z`, with a day that its month has; it becomes a
//!   `Temporal.Instant` with its microseconds. Another text is
//!   `dateTimeMalformed`.
//! - `uuid`: lower-case hex digits in groups of 8, 4, 4, 4 and 12; another
//!   text is `uuidMalformed`.
//! - An enum text that is not a value is `enumValueUnknown`; a tag text that
//!   names no variant is `tagUnknown`; a value of another JSON kind is
//!   `kindMismatch`.
//! - The decoded records and lists are frozen.

mod contract;
mod decode_support;

use std::collections::BTreeSet;

use contract::{
    parse_contract, ContractField, ContractKind, ContractModel, ContractVariant, LeafKind,
};

use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{
    camel_case_of_snake, import_types, is_identifier, module_text, pascal_case_of_snake,
    string_literal, union_alias,
};
use crate::wire_json::WIRE_JSON_MODULE;

/// The stem of the rendered module.
pub const MONITORING_MODELS_MODULE: &str = "monitoring_models";

/// The contract of the core's monitoring JSON. The core's own contract test
/// reads the same file.
const MODEL_CONTRACT: &str = include_str!("../../../../core/src/monitoring/model_contract.json");

/// The comment on the union of the model names.
const MODEL_NAME_COMMENT: &str = "/** The models of the core's monitoring JSON. */";

/// The names that a rendered type must not take: the types of the fixed
/// text, the global types that the module uses, and the names whose
/// decoder would be a leaf decoder of the fixed text (`decodeString`).
const RESERVED_TYPE_NAMES: &[&str] = &[
    "DecodeResult",
    "WireJson",
    "MonitoringModelName",
    "MonitoringWireKind",
    "MonitoringFieldMissing",
    "MonitoringFieldUnknown",
    "MonitoringKindMismatch",
    "MonitoringIntegerUnsafe",
    "MonitoringDateTimeMalformed",
    "MonitoringUuidMalformed",
    "MonitoringEnumValueUnknown",
    "MonitoringTagUnknown",
    "MonitoringDecodeError",
    "DecodePlace",
    "Decoder",
    "Outcome",
    "AllDecoded",
    "FieldReader",
    "Temporal",
    "ReadonlyArray",
    "ReadonlyMap",
    "Array",
    "Map",
    "Set",
    "Extract",
    "String",
    "Boolean",
    "Number",
    "Integer",
    "DateTime",
    "Uuid",
];

/// Where a record type is. It decides what its decoder takes and whether
/// the record has a tag member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordPlace<'a> {
    /// A model of the contract: the module exports its decoder.
    Model,
    /// A model in a field of another type.
    Nested,
    /// A variant of the union with the tag field `tag`; the tag member
    /// holds `tag_value` in camelCase.
    Variant { tag: &'a str, tag_value: &'a str },
}

/// One named type of the module.
#[derive(Debug, Clone, PartialEq, Eq)]
enum NamedType<'a> {
    /// An interface: a model, a nested model or a variant.
    Record {
        name: String,
        fields: &'a [ContractField],
        place: RecordPlace<'a>,
    },
    /// A union of its variant interfaces, told apart by the field `tag`.
    Union {
        name: String,
        tag: &'a str,
        variants: &'a [ContractVariant],
    },
    /// A union of texts.
    Enum { name: String, values: &'a [String] },
}

impl NamedType<'_> {
    /// The type name.
    fn name(&self) -> &str {
        match self {
            Self::Record { name, .. } | Self::Union { name, .. } | Self::Enum { name, .. } => name,
        }
    }
}

/// What a decoder takes. It decides the parameters of the decoder and
/// whether the module exports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecoderInput {
    /// The whole input: the exported decoder of a model.
    Whole,
    /// A value at a place of the input.
    AtPlace,
    /// The fields of an object that a union decoder read for a variant.
    VariantFields,
}

/// The files of the monitoring read types, in a fixed order:
/// `monitoring_models.ts`. Pure.
///
/// The contract file is a fact of the core. A file that is not a contract,
/// or a name that the module cannot hold, is a defect: the render panics
/// with the place and the cause.
pub fn generated_files() -> Vec<GeneratedFile> {
    vec![catalog_file(
        MONITORING_MODELS_MODULE,
        render_module(&parse_contract(MODEL_CONTRACT)),
    )]
}

/// The text of `monitoring_models.ts` for `models`. Pure.
fn render_module(models: &[ContractModel]) -> String {
    let types = named_types(models);
    let mut sections = vec![
        import_types(&["DecodeResult", "WireJson"], WIRE_JSON_MODULE),
        model_name_union(models),
        decode_support::REFUSAL_TYPES.to_owned(),
    ];
    sections.extend(types.iter().map(type_section));
    sections.push(decode_support::DECODE_HELPERS.to_owned());
    sections.extend(types.iter().map(decoder_section));
    module_text(&sections)
}

/// `MonitoringModelName`: the union of the model names, in contract order.
/// Pure.
fn model_name_union(models: &[ContractModel]) -> String {
    let names: Vec<String> = models
        .iter()
        .map(|model| string_literal(&model.name))
        .collect();
    format!(
        "{MODEL_NAME_COMMENT}\n{}",
        union_alias("MonitoringModelName", &names)
    )
}

/// Every named type of `models`: each model, then the types in its fields,
/// depth first, in contract order. Pure. The walk is as deep as the
/// contract, which [`parse_contract`] bounds.
///
/// A type name or a member name that is not an identifier, a type name of
/// [`RESERVED_TYPE_NAMES`], and a name given twice are defects: the render
/// panics.
fn named_types(models: &[ContractModel]) -> Vec<NamedType<'_>> {
    let mut types = Vec::new();
    for model in models {
        types.push(NamedType::Record {
            name: model.name.clone(),
            fields: &model.fields,
            place: RecordPlace::Model,
        });
        push_field_types(&model.name, &model.fields, &mut types);
    }
    assert_names_distinct(&types);
    types
}

/// Push the named types in `fields` of the type `owner`.
fn push_field_types<'a>(owner: &str, fields: &'a [ContractField], types: &mut Vec<NamedType<'a>>) {
    for field in fields {
        push_kind_types(&nested_type_name(owner, &field.key), &field.kind, types);
    }
}

/// Push the named types of `kind`, whose type name is `name`.
fn push_kind_types<'a>(name: &str, kind: &'a ContractKind, types: &mut Vec<NamedType<'a>>) {
    match kind {
        ContractKind::Leaf(_) => {}
        ContractKind::Nullable(inner) | ContractKind::List(inner) | ContractKind::Map(inner) => {
            push_kind_types(name, inner, types);
        }
        ContractKind::Enum(values) => types.push(NamedType::Enum {
            name: name.to_owned(),
            values,
        }),
        ContractKind::Model(fields) => {
            types.push(NamedType::Record {
                name: name.to_owned(),
                fields,
                place: RecordPlace::Nested,
            });
            push_field_types(name, fields, types);
        }
        ContractKind::Union { tag, variants } => {
            types.push(NamedType::Union {
                name: name.to_owned(),
                tag,
                variants,
            });
            for variant in variants {
                let variant_name = nested_type_name(name, &variant.tag_value);
                types.push(NamedType::Record {
                    name: variant_name.clone(),
                    fields: &variant.fields,
                    place: RecordPlace::Variant {
                        tag,
                        tag_value: &variant.tag_value,
                    },
                });
                push_field_types(&variant_name, &variant.fields, types);
            }
        }
    }
}

/// The type name of the part `snake_part` of the type `owner`: a field key
/// or a variant tag, in PascalCase after the owner's name. Pure.
fn nested_type_name(owner: &str, snake_part: &str) -> String {
    format!("{owner}{}", pascal_case_of_snake(snake_part))
}

/// Assert that every type name is an identifier, is not reserved and is
/// given once, and that the member names of each record are identifiers
/// and distinct.
fn assert_names_distinct(types: &[NamedType<'_>]) {
    let mut taken: BTreeSet<&str> = RESERVED_TYPE_NAMES.iter().copied().collect();
    for named in types {
        let name = named.name();
        assert!(
            is_identifier(name),
            "the type name {name:?} is not an identifier"
        );
        assert!(
            taken.insert(name),
            "the type name {name} is given twice, or is a name of the module's fixed text"
        );
        match named {
            NamedType::Record {
                name,
                fields,
                place,
            } => assert_members_distinct(name, fields, *place),
            NamedType::Union { .. } | NamedType::Enum { .. } => {}
        }
    }
}

/// Assert that the members of the record `name` are identifiers and
/// distinct: the tag member of a variant, then one member per field.
fn assert_members_distinct(name: &str, fields: &[ContractField], place: RecordPlace<'_>) {
    let mut members: BTreeSet<String> = BTreeSet::new();
    let tag = tag_member(place).map(|(member, _literal)| member);
    let field_members = fields.iter().map(|field| camel_case_of_snake(&field.key));
    for member in tag.into_iter().chain(field_members) {
        assert!(
            is_identifier(&member),
            "{name}: the member name {member:?} is not an identifier"
        );
        assert!(
            !members.contains(&member),
            "{name}: two keys give the member name {member}"
        );
        let _new_member = members.insert(member);
    }
}

/// The tag member of a record: `(member name, string literal of the tag
/// text in camelCase)` for a variant; a model has none. Pure.
fn tag_member(place: RecordPlace<'_>) -> Option<(String, String)> {
    match place {
        RecordPlace::Variant { tag, tag_value } => Some((
            camel_case_of_snake(tag),
            string_literal(&camel_case_of_snake(tag_value)),
        )),
        RecordPlace::Model | RecordPlace::Nested => None,
    }
}

/// The type declaration of `named`. Pure.
fn type_section(named: &NamedType<'_>) -> String {
    match named {
        NamedType::Record {
            name,
            fields,
            place,
        } => record_interface(name, fields, *place),
        NamedType::Union { name, variants, .. } => {
            let members: Vec<String> = variants
                .iter()
                .map(|variant| nested_type_name(name, &variant.tag_value))
                .collect();
            union_alias(name, &members)
        }
        NamedType::Enum { name, values } => {
            let members: Vec<String> = values
                .iter()
                .map(String::as_str)
                .map(string_literal)
                .collect();
            union_alias(name, &members)
        }
    }
}

/// The interface of the record `name`. Pure.
fn record_interface(name: &str, fields: &[ContractField], place: RecordPlace<'_>) -> String {
    let mut lines = vec![format!("export interface {name} {{")];
    if let Some((member, literal)) = tag_member(place) {
        lines.push(format!("  readonly {member}: {literal};"));
    }
    for field in fields {
        lines.push(format!(
            "  readonly {}: {};",
            camel_case_of_snake(&field.key),
            type_expression(&nested_type_name(name, &field.key), &field.kind)
        ));
    }
    lines.push("}".to_owned());
    lines.join("\n")
}

/// The TypeScript type of `kind`, whose type name is `name` when the kind
/// is named. Pure.
fn type_expression(name: &str, kind: &ContractKind) -> String {
    match kind {
        ContractKind::Leaf(leaf) => leaf_type(*leaf).to_owned(),
        ContractKind::Nullable(inner) => format!("{} | null", type_expression(name, inner)),
        ContractKind::List(inner) => format!("ReadonlyArray<{}>", type_expression(name, inner)),
        ContractKind::Map(inner) => {
            format!("ReadonlyMap<string, {}>", type_expression(name, inner))
        }
        ContractKind::Enum(_) | ContractKind::Model(_) | ContractKind::Union { .. } => {
            name.to_owned()
        }
    }
}

/// The TypeScript type of a leaf kind. Pure.
fn leaf_type(leaf: LeafKind) -> &'static str {
    match leaf {
        LeafKind::Text | LeafKind::Uuid => "string",
        LeafKind::Integer | LeafKind::Number => "number",
        LeafKind::Boolean => "boolean",
        LeafKind::DateTime => "Temporal.Instant",
    }
}

/// The decoder of `kind`, whose type name is `name` when the kind is named:
/// a decoder function, or a call that builds one. Pure.
fn decoder_expression(name: &str, kind: &ContractKind) -> String {
    match kind {
        ContractKind::Leaf(leaf) => leaf_decoder(*leaf).to_owned(),
        ContractKind::Nullable(inner) => format!("nullableOf({})", decoder_expression(name, inner)),
        ContractKind::List(inner) => format!("listOf({})", decoder_expression(name, inner)),
        ContractKind::Map(inner) => format!("mapOf({})", decoder_expression(name, inner)),
        ContractKind::Enum(_) | ContractKind::Model(_) | ContractKind::Union { .. } => {
            format!("decode{name}")
        }
    }
}

/// The decoder of a leaf kind in the fixed text. Pure.
fn leaf_decoder(leaf: LeafKind) -> &'static str {
    match leaf {
        LeafKind::Text => "decodeString",
        LeafKind::Uuid => "decodeUuid",
        LeafKind::Integer => "decodeInteger",
        LeafKind::Number => "decodeNumber",
        LeafKind::Boolean => "decodeBoolean",
        LeafKind::DateTime => "decodeDateTime",
    }
}

/// The decoder function of `named`. Pure.
fn decoder_section(named: &NamedType<'_>) -> String {
    match named {
        NamedType::Record {
            name,
            fields,
            place,
        } => record_decoder(name, fields, *place),
        NamedType::Union {
            name,
            tag,
            variants,
        } => union_decoder(name, tag, variants),
        NamedType::Enum { name, values } => enum_decoder(name, values),
    }
}

/// The first line of the decoder of the type `name`, its parameters one
/// per line. Pure.
fn decoder_head(name: &str, input: DecoderInput) -> String {
    let (keyword, parameters) = match input {
        DecoderInput::Whole => ("export function", "  value: WireJson,\n"),
        DecoderInput::AtPlace => ("function", "  value: WireJson,\n  place: DecodePlace,\n"),
        DecoderInput::VariantFields => ("function", "  fields: FieldReader,\n"),
    };
    format!(
        "{keyword} decode{name}(\n{parameters}): DecodeResult<{name}, MonitoringDecodeError> {{"
    )
}

/// The decoder of the record `name`. Pure.
fn record_decoder(name: &str, fields: &[ContractField], place: RecordPlace<'_>) -> String {
    let mut lines = Vec::new();
    match place {
        RecordPlace::Model => {
            lines.push(decoder_head(name, DecoderInput::Whole));
            let root = format!("{{ model: {}, path: \"$\" }}", string_literal(name));
            lines.extend(read_object_lines(&root));
            lines.extend(record_body(name, fields, place, "fields.value"));
        }
        RecordPlace::Nested => {
            lines.push(decoder_head(name, DecoderInput::AtPlace));
            lines.extend(read_object_lines("place"));
            lines.extend(record_body(name, fields, place, "fields.value"));
        }
        RecordPlace::Variant { .. } => {
            lines.push(decoder_head(name, DecoderInput::VariantFields));
            lines.extend(record_body(name, fields, place, "fields"));
        }
    }
    lines.push("}".to_owned());
    lines.join("\n")
}

/// The lines that read the input value of a decoder as an object at the
/// place `place_expression`, and return a refusal. Pure.
fn read_object_lines(place_expression: &str) -> Vec<String> {
    vec![
        format!("  const fields = readObject(value, {place_expression});"),
        String::new(),
        "  if (!fields.ok) {".to_owned(),
        "    return fields;".to_owned(),
        "  }".to_owned(),
        String::new(),
    ]
}

/// The lines that decode the fields of the record `name` through the field
/// reader `reader`, refuse an unknown key and return the frozen record.
/// Pure.
fn record_body(
    name: &str,
    fields: &[ContractField],
    place: RecordPlace<'_>,
    reader: &str,
) -> Vec<String> {
    let mut lines = Vec::new();
    if !fields.is_empty() {
        lines.push("  const results = {".to_owned());
        for field in fields {
            lines.push(format!(
                "    {}: {reader}.read({}, {}),",
                camel_case_of_snake(&field.key),
                string_literal(&field.key),
                decoder_expression(&nested_type_name(name, &field.key), &field.kind)
            ));
        }
        lines.extend(
            [
                "  };",
                "",
                "  if (!allDecoded(results)) {",
                "    return firstFailure(results);",
                "  }",
                "",
            ]
            .map(str::to_owned),
        );
    }
    lines.push(format!("  const unread = {reader}.unread();"));
    lines.extend(
        [
            "",
            "  if (!unread.ok) {",
            "    return unread;",
            "  }",
            "",
            "  return ok(",
            "    Object.freeze({",
        ]
        .map(str::to_owned),
    );
    if let Some((member, literal)) = tag_member(place) {
        lines.push(format!("      {member}: {literal},"));
    }
    for field in fields {
        let member = camel_case_of_snake(&field.key);
        lines.push(format!("      {member}: results.{member}.value,"));
    }
    lines.extend(["    }),", "  );"].map(str::to_owned));
    lines
}

/// The decoder of the union `name`: it reads the tag field `tag` and calls
/// the decoder of the variant that the tag names. Pure.
fn union_decoder(name: &str, tag: &str, variants: &[ContractVariant]) -> String {
    let tag_key = string_literal(tag);
    let mut lines = vec![decoder_head(name, DecoderInput::AtPlace)];
    lines.extend(read_object_lines("place"));
    lines.extend([
        format!("  const tag = fields.value.read({tag_key}, decodeString);"),
        String::new(),
        "  if (!tag.ok) {".to_owned(),
        "    return tag;".to_owned(),
        "  }".to_owned(),
        String::new(),
        "  switch (tag.value) {".to_owned(),
    ]);
    for variant in variants {
        lines.push(format!("    case {}:", string_literal(&variant.tag_value)));
        lines.push(format!(
            "      return decode{}(fields.value);",
            nested_type_name(name, &variant.tag_value)
        ));
    }
    lines.push("    default:".to_owned());
    lines.push(format!(
        "      return tagUnknown(fields.value, {tag_key}, tag.value);"
    ));
    lines.extend(["  }", "}"].map(str::to_owned));
    lines.join("\n")
}

/// The decoder of the enum `name`: one `case` per text. Pure.
fn enum_decoder(name: &str, values: &[String]) -> String {
    let mut lines = vec![
        decoder_head(name, DecoderInput::AtPlace),
        "  switch (value) {".to_owned(),
    ];
    for value in values {
        lines.push(format!("    case {}:", string_literal(value)));
    }
    lines.extend(
        [
            "      return ok(value);",
            "    default:",
            "      return enumValueUnknown(value, place);",
            "  }",
            "}",
        ]
        .map(str::to_owned),
    );
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    /// The name, the type text and the decoder text of each named type of
    /// the contract `text`.
    fn rendered(text: &str) -> Vec<(String, String, String)> {
        let models = parse_contract(text);
        named_types(&models)
            .iter()
            .map(|named| {
                (
                    named.name().to_owned(),
                    type_section(named),
                    decoder_section(named),
                )
            })
            .collect()
    }

    /// The type text and the decoder text of the one named type `name`.
    fn texts_of(rendered: &[(String, String, String)], name: &str) -> (String, String) {
        let (_, type_text, decoder_text) = rendered
            .iter()
            .find(|(rendered_name, _, _)| rendered_name == name)
            .unwrap_or_else(|| panic!("no named type {name}"));
        (type_text.clone(), decoder_text.clone())
    }

    const SAMPLE_COUNT_DECODER: &str = r#"export function decodeSample(
  value: WireJson,
): DecodeResult<Sample, MonitoringDecodeError> {
  const fields = readObject(value, { model: "Sample", path: "$" });

  if (!fields.ok) {
    return fields;
  }

  const results = {
    count: fields.value.read("count", decodeInteger),
  };

  if (!allDecoded(results)) {
    return firstFailure(results);
  }

  const unread = fields.value.unread();

  if (!unread.ok) {
    return unread;
  }

  return ok(
    Object.freeze({
      count: results.count.value,
    }),
  );
}"#;

    #[test]
    fn each_leaf_kind_has_its_type_and_its_leaf_decoder() {
        let rendered = rendered(
            r#"{"Sample": {"at": "date-time", "count": "integer", "id": "uuid", "name": "string",
                "ok": "boolean", "share": "number"}}"#,
        );

        assert_eq!(rendered.len(), 1);
        assert_eq!(
            texts_of(&rendered, "Sample"),
            (
                r#"export interface Sample {
  readonly at: Temporal.Instant;
  readonly count: number;
  readonly id: string;
  readonly name: string;
  readonly ok: boolean;
  readonly share: number;
}"#
                .to_owned(),
                r#"export function decodeSample(
  value: WireJson,
): DecodeResult<Sample, MonitoringDecodeError> {
  const fields = readObject(value, { model: "Sample", path: "$" });

  if (!fields.ok) {
    return fields;
  }

  const results = {
    at: fields.value.read("at", decodeDateTime),
    count: fields.value.read("count", decodeInteger),
    id: fields.value.read("id", decodeUuid),
    name: fields.value.read("name", decodeString),
    ok: fields.value.read("ok", decodeBoolean),
    share: fields.value.read("share", decodeNumber),
  };

  if (!allDecoded(results)) {
    return firstFailure(results);
  }

  const unread = fields.value.unread();

  if (!unread.ok) {
    return unread;
  }

  return ok(
    Object.freeze({
      at: results.at.value,
      count: results.count.value,
      id: results.id.value,
      name: results.name.value,
      ok: results.ok.value,
      share: results.share.value,
    }),
  );
}"#
                .to_owned(),
            )
        );
    }

    #[test]
    fn a_map_a_nullable_value_and_a_list_wrap_the_type_and_the_decoder() {
        let rendered = rendered(
            r#"{"Sample": {"limits": {"map": "integer"}, "note": {"nullable": "string"},
                "spans": {"list": {"nullable": "number"}}, "tags": {"list": "string"}}}"#,
        );

        let (type_text, decoder_text) = texts_of(&rendered, "Sample");

        assert_eq!(rendered.len(), 1);
        assert_eq!(
            type_text,
            r#"export interface Sample {
  readonly limits: ReadonlyMap<string, number>;
  readonly note: string | null;
  readonly spans: ReadonlyArray<number | null>;
  readonly tags: ReadonlyArray<string>;
}"#
        );
        assert!(
            decoder_text.contains(
                r#"  const results = {
    limits: fields.value.read("limits", mapOf(decodeInteger)),
    note: fields.value.read("note", nullableOf(decodeString)),
    spans: fields.value.read("spans", listOf(nullableOf(decodeNumber))),
    tags: fields.value.read("tags", listOf(decodeString)),
  };
"#
            ),
            "{decoder_text}"
        );
    }

    #[test]
    fn an_enum_is_a_union_of_its_texts_with_a_switch_decoder() {
        let rendered =
            rendered(r#"{"Sample": {"mode": {"enum": ["SHARED_ASYNC", "all_serving"]}}}"#);

        assert_eq!(
            texts_of(&rendered, "SampleMode"),
            (
                r#"export type SampleMode =
  | "SHARED_ASYNC"
  | "all_serving";"#
                    .to_owned(),
                r#"function decodeSampleMode(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<SampleMode, MonitoringDecodeError> {
  switch (value) {
    case "SHARED_ASYNC":
    case "all_serving":
      return ok(value);
    default:
      return enumValueUnknown(value, place);
  }
}"#
                .to_owned(),
            )
        );
        assert!(texts_of(&rendered, "Sample")
            .0
            .contains("  readonly mode: SampleMode;\n"));
    }

    #[test]
    fn a_union_has_one_interface_per_variant_with_its_tag_in_camel_case() {
        let rendered = rendered(
            r#"{"Sample": {"state": {"union": {"tag": "kind", "variants": {
                "named": {"name": "string"}, "not_recorded": {}}}}}}"#,
        );

        let names: Vec<&str> = rendered.iter().map(|(name, _, _)| name.as_str()).collect();

        assert_eq!(
            names,
            [
                "Sample",
                "SampleState",
                "SampleStateNamed",
                "SampleStateNotRecorded"
            ]
        );
        assert_eq!(
            texts_of(&rendered, "SampleState"),
            (
                r#"export type SampleState =
  | SampleStateNamed
  | SampleStateNotRecorded;"#
                    .to_owned(),
                r#"function decodeSampleState(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<SampleState, MonitoringDecodeError> {
  const fields = readObject(value, place);

  if (!fields.ok) {
    return fields;
  }

  const tag = fields.value.read("kind", decodeString);

  if (!tag.ok) {
    return tag;
  }

  switch (tag.value) {
    case "named":
      return decodeSampleStateNamed(fields.value);
    case "not_recorded":
      return decodeSampleStateNotRecorded(fields.value);
    default:
      return tagUnknown(fields.value, "kind", tag.value);
  }
}"#
                .to_owned(),
            )
        );
    }

    #[test]
    fn a_variant_decodes_the_fields_that_its_union_read() {
        let rendered = rendered(
            r#"{"Sample": {"state": {"union": {"tag": "kind", "variants": {
                "named": {"name": "string"}, "not_recorded": {}}}}}}"#,
        );

        assert_eq!(
            texts_of(&rendered, "SampleStateNamed"),
            (
                r#"export interface SampleStateNamed {
  readonly kind: "named";
  readonly name: string;
}"#
                .to_owned(),
                r#"function decodeSampleStateNamed(
  fields: FieldReader,
): DecodeResult<SampleStateNamed, MonitoringDecodeError> {
  const results = {
    name: fields.read("name", decodeString),
  };

  if (!allDecoded(results)) {
    return firstFailure(results);
  }

  const unread = fields.unread();

  if (!unread.ok) {
    return unread;
  }

  return ok(
    Object.freeze({
      kind: "named",
      name: results.name.value,
    }),
  );
}"#
                .to_owned(),
            )
        );
    }

    #[test]
    fn a_variant_without_fields_holds_only_its_tag() {
        let rendered = rendered(
            r#"{"Sample": {"state": {"union": {"tag": "kind", "variants": {
                "named": {"name": "string"}, "not_recorded": {}}}}}}"#,
        );

        assert_eq!(
            texts_of(&rendered, "SampleStateNotRecorded"),
            (
                r#"export interface SampleStateNotRecorded {
  readonly kind: "notRecorded";
}"#
                .to_owned(),
                r#"function decodeSampleStateNotRecorded(
  fields: FieldReader,
): DecodeResult<SampleStateNotRecorded, MonitoringDecodeError> {
  const unread = fields.unread();

  if (!unread.ok) {
    return unread;
  }

  return ok(
    Object.freeze({
      kind: "notRecorded",
    }),
  );
}"#
                .to_owned(),
            )
        );
    }

    #[test]
    fn a_nested_model_takes_the_name_of_its_field_and_decodes_at_its_place() {
        let rendered = rendered(r#"{"Sample": {"total": {"model": {"count": "integer"}}}}"#);

        let (type_text, decoder_text) = texts_of(&rendered, "SampleTotal");

        assert_eq!(
            type_text,
            "export interface SampleTotal {\n  readonly count: number;\n}"
        );
        assert!(
            decoder_text.starts_with(
                r#"function decodeSampleTotal(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<SampleTotal, MonitoringDecodeError> {
  const fields = readObject(value, place);
"#
            ),
            "{decoder_text}"
        );
        assert!(texts_of(&rendered, "Sample")
            .1
            .contains("    total: fields.value.read(\"total\", decodeSampleTotal),\n"));
    }

    #[test]
    fn the_module_has_the_import_the_model_names_the_types_the_helpers_and_the_decoders() {
        let models = parse_contract(r#"{"Sample": {"count": "integer"}}"#);

        let text = render_module(&models);

        assert_eq!(
            text,
            module_text(&[
                "import type { DecodeResult, WireJson } from \"./wire_json.ts\";".to_owned(),
                "/** The models of the core's monitoring JSON. */\nexport type MonitoringModelName = \"Sample\";"
                    .to_owned(),
                decode_support::REFUSAL_TYPES.to_owned(),
                "export interface Sample {\n  readonly count: number;\n}".to_owned(),
                decode_support::DECODE_HELPERS.to_owned(),
                SAMPLE_COUNT_DECODER.to_owned(),
            ])
        );
    }

    #[test]
    fn every_model_of_the_core_contract_is_rendered_once_with_an_exported_decoder() {
        let models = parse_contract(MODEL_CONTRACT);

        let text = render_module(&models);

        assert_eq!(models.len(), 27);
        for model in &models {
            let interface = format!("\nexport interface {} {{\n", model.name);
            let decoder = format!("\nexport function decode{}(\n", model.name);
            assert_eq!(text.matches(&interface).count(), 1, "{}", model.name);
            assert_eq!(text.matches(&decoder).count(), 1, "{}", model.name);
        }
        assert_eq!(text.matches("\nexport function ").count(), models.len());
    }

    #[test]
    fn the_nested_types_of_the_core_contract_take_the_names_of_their_paths() {
        let models = parse_contract(MODEL_CONTRACT);

        let names: Vec<String> = named_types(&models)
            .iter()
            .map(|named| named.name().to_owned())
            .collect();

        assert_eq!(names.len(), 305);
        for expected in [
            "TaskAttemptInfoPhases",
            "TaskAttemptInfoPhasesNotCaptured",
            "TaskAttemptInfoPhasesCapturedRecordedByWorkerPlacement",
            "TaskDetailAttempts",
            "WorkerStateInfoProfileNotRecorded",
            "BudgetLoadInfoBudget",
            "WorkflowRunDetailNodesTimingExactSegmentsPhaseClaimedClaimRecordedPlacementPlaced",
        ] {
            assert!(names.iter().any(|name| name == expected), "{expected}");
        }
    }

    #[test]
    fn the_files_are_one_catalog_module_that_imports_the_wire_json_types() {
        let files = generated_files();

        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].relative_path(),
            Path::new("src/_catalog/monitoring_models.ts")
        );
        assert!(files[0].text().starts_with(&format!(
            "{GENERATED_HEADER}\n\nimport type {{ DecodeResult, WireJson }} from \"./wire_json.ts\";\n"
        )));
    }

    #[test]
    #[should_panic(expected = "the type name ABC is given twice")]
    fn a_type_name_that_two_paths_give_is_a_defect() {
        let models = parse_contract(
            r#"{"A": {"b_c": {"model": {"x": "string"}}}, "AB": {"c": {"model": {"x": "string"}}}}"#,
        );

        let _types = named_types(&models);
    }

    #[test]
    #[should_panic(
        expected = "the type name String is given twice, or is a name of the module's fixed text"
    )]
    fn a_type_name_of_the_fixed_text_is_a_defect() {
        let models = parse_contract(r#"{"String": {"x": "string"}}"#);

        let _types = named_types(&models);
    }

    #[test]
    #[should_panic(expected = "the type name \"Task-Info\" is not an identifier")]
    fn a_type_name_that_is_not_an_identifier_is_a_defect() {
        let models = parse_contract(r#"{"Task-Info": {"x": "string"}}"#);

        let _types = named_types(&models);
    }

    #[test]
    #[should_panic(expected = "Sample: two keys give the member name taskId")]
    fn two_keys_that_give_one_member_name_are_a_defect() {
        let models = parse_contract(r#"{"Sample": {"task_id": "string", "taskId": "string"}}"#);

        let _types = named_types(&models);
    }

    #[test]
    #[should_panic(expected = "Sample: the member name \"1st\" is not an identifier")]
    fn a_member_name_that_is_not_an_identifier_is_a_defect() {
        let models = parse_contract(r#"{"Sample": {"1st": "string"}}"#);

        let _types = named_types(&models);
    }
}
