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
//! - Each model, union, variant and enum has the name that the contract
//!   gives it: the name of its Python and Rust type (`AttemptPhasesInfo`),
//!   the Python class name for a variant (`AttemptPhasesNotCapturedInfo`). A type
//!   that several fields hold is rendered once; two kinds of one name with
//!   different shapes are a defect of the contract.
//! - `string` and `uuid` are `string`; `integer` and `number` are `number`;
//!   `boolean` is `boolean`; `date-time` is `Temporal.Instant`. A nullable
//!   kind is `T | null`, a list `ReadonlyArray<T>`, a map
//!   `ReadonlyMap<string, T>`.
//!
//! Decoders: one exported `decode<Model>(value: WireJson):
//! DecodeResult<Model, MonitoringDecodeError>` per model, over the lossless
//! `WireJson` tree (integers are `bigint`, floats `number`, objects `Map`).
//! It reads the input at the place `$` of the model. Each named type has a
//! module-private `read<Name>` decoder of a value at a place of the input;
//! the one of a variant reads the fields that its union read. The decode
//! rules, in the fixed text of [`decode_support`]:
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

use std::collections::{BTreeMap, BTreeSet};

use contract::{
    parse_contract, ContractField, ContractKind, ContractModel, ContractVariant, LeafKind,
};

use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{
    camel_case_of_snake, import_types, is_identifier, module_text, string_literal, union_alias,
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
/// decoder would be a function of the fixed text (`decodeString`,
/// `readObject`).
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
    "Object",
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
    /// A model in a field of another type that is not a model of the
    /// contract.
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

/// What a decoder takes. It decides the name and the parameters of the
/// decoder and whether the module exports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecoderInput {
    /// The whole input: the exported decoder of a model.
    Whole,
    /// A value at a place of the input.
    AtPlace,
    /// The fields of an object that a union decoder read for a variant.
    VariantFields,
}

/// Whether [`TypeWalk::push_new`] added a type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pushed {
    /// No type had the name: the type is added.
    New,
    /// A type of the name and the same shape is known.
    Known,
}

/// The walk of [`named_types`]: the fields of each model of the contract by
/// name, and the types found so far.
struct TypeWalk<'a> {
    models: BTreeMap<&'a str, &'a [ContractField]>,
    types: Vec<NamedType<'a>>,
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

/// Every named type of `models`, once: each model, then the types in its
/// fields that no earlier type gave, depth first, in contract order. A
/// model of the contract in a field of another type is not walked there:
/// its own turn renders it. Pure. The walk is as deep as the contract,
/// which [`parse_contract`] bounds.
///
/// One name for two shapes, a type name or a member name that is not an
/// identifier, and a type name of [`RESERVED_TYPE_NAMES`] are defects: the
/// render panics.
fn named_types(models: &[ContractModel]) -> Vec<NamedType<'_>> {
    let mut walk = TypeWalk {
        models: models
            .iter()
            .map(|model| (model.name.as_str(), model.fields.as_slice()))
            .collect(),
        types: Vec::new(),
    };
    for model in models {
        walk.types.push(NamedType::Record {
            name: model.name.clone(),
            fields: &model.fields,
            place: RecordPlace::Model,
        });
        walk.push_field_types(&model.fields);
    }
    assert_names_distinct(&walk.types);
    walk.types
}

impl<'a> TypeWalk<'a> {
    /// Push the named types in `fields` that are not known.
    fn push_field_types(&mut self, fields: &'a [ContractField]) {
        for field in fields {
            self.push_kind_types(&field.kind);
        }
    }

    /// Push the named types of `kind` that are not known. A model of the
    /// contract must have the fields of the contract's model.
    fn push_kind_types(&mut self, kind: &'a ContractKind) {
        match kind {
            ContractKind::Leaf(_) => {}
            ContractKind::Nullable(inner)
            | ContractKind::List(inner)
            | ContractKind::Map(inner) => {
                self.push_kind_types(inner);
            }
            ContractKind::Enum { name, values } => {
                let _pushed = self.push_new(NamedType::Enum {
                    name: name.clone(),
                    values,
                });
            }
            ContractKind::Model { name, fields } => match self.models.get(name.as_str()) {
                Some(model_fields) => assert_eq!(
                    *model_fields,
                    fields.as_slice(),
                    "the contract gives the model {name} two shapes"
                ),
                None => self.push_record(name, fields, RecordPlace::Nested),
            },
            ContractKind::Union {
                name,
                tag,
                variants,
            } => match self.push_new(NamedType::Union {
                name: name.clone(),
                tag,
                variants,
            }) {
                Pushed::New => {
                    for variant in variants {
                        let place = RecordPlace::Variant {
                            tag,
                            tag_value: &variant.tag_value,
                        };
                        self.push_record(&variant.name, &variant.fields, place);
                    }
                }
                Pushed::Known => {}
            },
        }
    }

    /// Push the record `name` and the types of its fields, unless the
    /// record is known.
    fn push_record(&mut self, name: &str, fields: &'a [ContractField], place: RecordPlace<'a>) {
        let record = NamedType::Record {
            name: name.to_owned(),
            fields,
            place,
        };
        match self.push_new(record) {
            Pushed::New => self.push_field_types(fields),
            Pushed::Known => {}
        }
    }

    /// Add `named` when no type has its name. A known name must have the
    /// same shape: two shapes of one name are a defect.
    fn push_new(&mut self, named: NamedType<'a>) -> Pushed {
        match self
            .types
            .iter()
            .position(|known| known.name() == named.name())
        {
            Some(index) => {
                assert_eq!(
                    self.types[index],
                    named,
                    "the contract gives the type {} two shapes",
                    named.name()
                );
                Pushed::Known
            }
            None => {
                self.types.push(named);
                Pushed::New
            }
        }
    }
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
                .map(|variant| variant.name.clone())
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
            type_expression(&field.kind)
        ));
    }
    lines.push("}".to_owned());
    lines.join("\n")
}

/// The TypeScript type of `kind`. Pure.
fn type_expression(kind: &ContractKind) -> String {
    match kind {
        ContractKind::Leaf(leaf) => leaf_type(*leaf).to_owned(),
        ContractKind::Nullable(inner) => format!("{} | null", type_expression(inner)),
        ContractKind::List(inner) => format!("ReadonlyArray<{}>", type_expression(inner)),
        ContractKind::Map(inner) => format!("ReadonlyMap<string, {}>", type_expression(inner)),
        ContractKind::Enum { name, .. }
        | ContractKind::Model { name, .. }
        | ContractKind::Union { name, .. } => name.clone(),
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

/// The decoder of `kind` at a place: a decoder function, or a call that
/// builds one. Pure.
fn decoder_expression(kind: &ContractKind) -> String {
    match kind {
        ContractKind::Leaf(leaf) => leaf_decoder(*leaf).to_owned(),
        ContractKind::Nullable(inner) => format!("nullableOf({})", decoder_expression(inner)),
        ContractKind::List(inner) => format!("listOf({})", decoder_expression(inner)),
        ContractKind::Map(inner) => format!("mapOf({})", decoder_expression(inner)),
        ContractKind::Enum { name, .. }
        | ContractKind::Model { name, .. }
        | ContractKind::Union { name, .. } => format!("read{name}"),
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

/// The decoder functions of `named`: for a model, its exported decoder and
/// its decoder at a place. Pure.
fn decoder_section(named: &NamedType<'_>) -> String {
    match named {
        NamedType::Record {
            name,
            fields,
            place: RecordPlace::Model,
        } => format!(
            "{}\n\n{}",
            model_decoder(name),
            object_decoder(name, fields, RecordPlace::Model)
        ),
        NamedType::Record {
            name,
            fields,
            place: place @ RecordPlace::Nested,
        } => object_decoder(name, fields, *place),
        NamedType::Record {
            name,
            fields,
            place: place @ RecordPlace::Variant { .. },
        } => variant_decoder(name, fields, *place),
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
        DecoderInput::Whole => ("export function decode", "  value: WireJson,\n"),
        DecoderInput::AtPlace => (
            "function read",
            "  value: WireJson,\n  place: DecodePlace,\n",
        ),
        DecoderInput::VariantFields => ("function read", "  fields: FieldReader,\n"),
    };
    format!("{keyword}{name}(\n{parameters}): DecodeResult<{name}, MonitoringDecodeError> {{")
}

/// The exported decoder of the model `name`: its decoder at the place `$`
/// of the model. Pure.
fn model_decoder(name: &str) -> String {
    [
        decoder_head(name, DecoderInput::Whole),
        format!(
            "  return read{name}(value, {{ model: {}, path: \"$\" }});",
            string_literal(name)
        ),
        "}".to_owned(),
    ]
    .join("\n")
}

/// The decoder of the record `name` at a place: it reads the value as an
/// object. Pure.
fn object_decoder(name: &str, fields: &[ContractField], place: RecordPlace<'_>) -> String {
    let mut lines = vec![decoder_head(name, DecoderInput::AtPlace)];
    lines.extend(read_object_lines());
    lines.extend(record_body(fields, place, "fields.value"));
    lines.push("}".to_owned());
    lines.join("\n")
}

/// The decoder of the variant `name`: it reads the fields that its union
/// read. Pure.
fn variant_decoder(name: &str, fields: &[ContractField], place: RecordPlace<'_>) -> String {
    let mut lines = vec![decoder_head(name, DecoderInput::VariantFields)];
    lines.extend(record_body(fields, place, "fields"));
    lines.push("}".to_owned());
    lines.join("\n")
}

/// The lines that read the input value of a decoder as an object at its
/// place, and return a refusal. Pure.
fn read_object_lines() -> Vec<String> {
    Vec::from(
        [
            "  const fields = readObject(value, place);",
            "",
            "  if (!fields.ok) {",
            "    return fields;",
            "  }",
            "",
        ]
        .map(str::to_owned),
    )
}

/// The lines that decode `fields` through the field reader `reader`, refuse
/// an unknown key and return the frozen record. Pure.
fn record_body(fields: &[ContractField], place: RecordPlace<'_>, reader: &str) -> Vec<String> {
    let mut lines = Vec::new();
    if !fields.is_empty() {
        lines.push("  const results = {".to_owned());
        for field in fields {
            lines.push(format!(
                "    {}: {reader}.read({}, {}),",
                camel_case_of_snake(&field.key),
                string_literal(&field.key),
                decoder_expression(&field.kind)
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
    lines.extend(read_object_lines());
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
        lines.push(format!("      return read{}(fields.value);", variant.name));
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

    /// The type names of the contract `text`, in module order.
    fn names_of(text: &str) -> Vec<String> {
        rendered(text)
            .into_iter()
            .map(|(name, _, _)| name)
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

    /// A contract with the union `State` of the variants `Named` and
    /// `NotRecorded`.
    const UNION_CONTRACT: &str = r#"{"Sample": {"state": {"union": {"name": "State", "tag": "kind",
        "variants": {"named": {"fields": {"name": "string"}, "name": "Named"},
        "not_recorded": {"fields": {}, "name": "NotRecorded"}}}}}}"#;

    const SAMPLE_COUNT_DECODER: &str = r#"export function decodeSample(
  value: WireJson,
): DecodeResult<Sample, MonitoringDecodeError> {
  return readSample(value, { model: "Sample", path: "$" });
}

function readSample(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<Sample, MonitoringDecodeError> {
  const fields = readObject(value, place);

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

        let (type_text, decoder_text) = texts_of(&rendered, "Sample");

        assert_eq!(rendered.len(), 1);
        assert_eq!(
            type_text,
            r#"export interface Sample {
  readonly at: Temporal.Instant;
  readonly count: number;
  readonly id: string;
  readonly name: string;
  readonly ok: boolean;
  readonly share: number;
}"#
        );
        assert!(
            decoder_text.contains(
                r#"  const results = {
    at: fields.value.read("at", decodeDateTime),
    count: fields.value.read("count", decodeInteger),
    id: fields.value.read("id", decodeUuid),
    name: fields.value.read("name", decodeString),
    ok: fields.value.read("ok", decodeBoolean),
    share: fields.value.read("share", decodeNumber),
  };
"#
            ),
            "{decoder_text}"
        );
        assert!(
            decoder_text.contains(
                r#"    Object.freeze({
      at: results.at.value,
      count: results.count.value,
      id: results.id.value,
      name: results.name.value,
      ok: results.ok.value,
      share: results.share.value,
    }),
"#
            ),
            "{decoder_text}"
        );
    }

    #[test]
    fn a_model_has_an_exported_decoder_that_reads_at_the_place_of_the_model() {
        let rendered = rendered(r#"{"Sample": {"count": "integer"}}"#);

        assert_eq!(texts_of(&rendered, "Sample").1, SAMPLE_COUNT_DECODER);
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
        let rendered = rendered(
            r#"{"Sample": {"mode": {"enum": {"name": "Mode", "values": ["SHARED_ASYNC", "all_serving"]}}}}"#,
        );

        assert_eq!(
            texts_of(&rendered, "Mode"),
            (
                r#"export type Mode =
  | "SHARED_ASYNC"
  | "all_serving";"#
                    .to_owned(),
                r#"function readMode(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<Mode, MonitoringDecodeError> {
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
            .contains("  readonly mode: Mode;\n"));
    }

    #[test]
    fn a_union_has_one_interface_per_variant_with_its_tag_in_camel_case() {
        let rendered = rendered(UNION_CONTRACT);

        let names: Vec<&str> = rendered.iter().map(|(name, _, _)| name.as_str()).collect();

        assert_eq!(names, ["Sample", "State", "Named", "NotRecorded"]);
        assert_eq!(
            texts_of(&rendered, "State"),
            (
                r#"export type State =
  | Named
  | NotRecorded;"#
                    .to_owned(),
                r#"function readState(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<State, MonitoringDecodeError> {
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
      return readNamed(fields.value);
    case "not_recorded":
      return readNotRecorded(fields.value);
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
        let rendered = rendered(UNION_CONTRACT);

        assert_eq!(
            texts_of(&rendered, "Named"),
            (
                r#"export interface Named {
  readonly kind: "named";
  readonly name: string;
}"#
                .to_owned(),
                r#"function readNamed(
  fields: FieldReader,
): DecodeResult<Named, MonitoringDecodeError> {
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
        let rendered = rendered(UNION_CONTRACT);

        assert_eq!(
            texts_of(&rendered, "NotRecorded"),
            (
                r#"export interface NotRecorded {
  readonly kind: "notRecorded";
}"#
                .to_owned(),
                r#"function readNotRecorded(
  fields: FieldReader,
): DecodeResult<NotRecorded, MonitoringDecodeError> {
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
    fn a_nested_model_takes_its_contract_name_and_reads_at_its_place() {
        let rendered = rendered(
            r#"{"Sample": {"total": {"model": {"fields": {"count": "integer"}, "name": "Counts"}}}}"#,
        );

        let (type_text, decoder_text) = texts_of(&rendered, "Counts");

        assert_eq!(
            type_text,
            "export interface Counts {\n  readonly count: number;\n}"
        );
        assert!(
            decoder_text.starts_with(
                r#"function readCounts(
  value: WireJson,
  place: DecodePlace,
): DecodeResult<Counts, MonitoringDecodeError> {
  const fields = readObject(value, place);
"#
            ),
            "{decoder_text}"
        );
        assert!(texts_of(&rendered, "Sample")
            .0
            .contains("  readonly total: Counts;\n"));
        assert!(texts_of(&rendered, "Sample")
            .1
            .contains("    total: fields.value.read(\"total\", readCounts),\n"));
    }

    #[test]
    fn a_type_that_two_fields_hold_is_rendered_once() {
        let names = names_of(
            r#"{"Sample": {
                "first": {"model": {"fields": {"count": "integer"}, "name": "Counts"}},
                "second": {"list": {"model": {"fields": {"count": "integer"}, "name": "Counts"}}}}}"#,
        );

        assert_eq!(names, ["Sample", "Counts"]);
    }

    #[test]
    fn a_model_of_the_contract_in_a_field_is_rendered_at_its_own_turn() {
        let rendered = rendered(
            r#"{"Report": {"rows": {"list": {"model": {"fields": {"count": "integer"}, "name": "Row"}}}},
                "Row": {"count": "integer"}}"#,
        );

        let names: Vec<&str> = rendered.iter().map(|(name, _, _)| name.as_str()).collect();

        assert_eq!(names, ["Report", "Row"]);
        assert!(texts_of(&rendered, "Report")
            .1
            .contains("    rows: fields.value.read(\"rows\", listOf(readRow)),\n"));
        assert!(texts_of(&rendered, "Row")
            .1
            .starts_with("export function decodeRow(\n"));
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
    fn the_types_of_the_core_contract_have_their_python_and_rust_names_once() {
        let models = parse_contract(MODEL_CONTRACT);

        let names: Vec<String> = named_types(&models)
            .iter()
            .map(|named| named.name().to_owned())
            .collect();

        assert_eq!(names.len(), 118);
        for expected in [
            "TaskAttemptDetail",
            "AttemptPhasesInfo",
            "AttemptPhasesNotCapturedInfo",
            "AttemptPlacementInfo",
            "TimingSegmentInfo",
            "SnapshotProfile",
            "Execution",
            "StreamCoverageState",
            "StreamWait",
            "SlotHealth",
            "DatabaseLiveness",
            "DownSlot",
            "ErrorFacet",
            "ExecutionClassReport",
            "TerminalNodeStatus",
        ] {
            assert!(names.iter().any(|name| name == expected), "{expected}");
        }
        assert!(!names.iter().any(|name| name == "TaskDetailAttempts"));
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
    #[should_panic(expected = "the contract gives the type Counts two shapes")]
    fn a_name_that_two_shapes_give_is_a_defect() {
        let _names = names_of(
            r#"{"Sample": {
                "first": {"model": {"fields": {"count": "integer"}, "name": "Counts"}},
                "second": {"model": {"fields": {"count": "number"}, "name": "Counts"}}}}"#,
        );
    }

    #[test]
    #[should_panic(expected = "the contract gives the model Row two shapes")]
    fn a_model_of_the_contract_with_other_fields_in_a_field_is_a_defect() {
        let _names = names_of(
            r#"{"Report": {"row": {"model": {"fields": {"count": "number"}, "name": "Row"}}},
                "Row": {"count": "integer"}}"#,
        );
    }

    #[test]
    #[should_panic(expected = "the type name Row is given twice")]
    fn a_type_name_that_two_kinds_give_is_a_defect() {
        let _names = names_of(
            r#"{"Report": {"mode": {"enum": {"name": "Row", "values": ["A"]}}},
                "Row": {"count": "integer"}}"#,
        );
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
