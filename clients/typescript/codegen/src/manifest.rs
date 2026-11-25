//! Render a manifest module of horsies-core as a TypeScript module of
//! `src/_catalog/`: the types of the manifest, its integer bounds, and one
//! converter per record and tagged type that gives the wire value
//! (`WireJson`) of a value of the type.
//!
//! The types are internal: the package fills them from the user's config.
//! Their member names are the wire keys (snake_case), so a converter needs
//! no name map.
//!
//! - A record is a readonly interface.
//! - A tagged type is one interface per variant, with the tag member and
//!   its value, and the union of the variants.
//! - A choice is the union of its wire values. It has no converter: a
//!   choice value is its own wire value.
//! - An integer is a `number` when the `max` of its bound is a safe integer,
//!   else a `bigint`. Each bound is a pair of constants of the same type,
//!   `<NAME>_MIN` and `<NAME>_MAX`.
//! - A list is a `ReadonlyArray`. A map is a `ReadonlyMap<string, T>`: its
//!   keys keep their order, and a key `__proto__` is a plain key.
//! - An optional value is `T | null`: Python's form writes `null` for no
//!   value, and the converter does too.
//! - A time of day is a `Temporal.PlainTime`. Its wire value is its text,
//!   `HH:MM:SS` for a whole second. The converter does not cut a fraction of
//!   a second: the core refuses it.
//! - A variant that holds the fields of a record type extends that record.
//! - A field with a member default has a constant of the default,
//!   `<HOLDER>_<FIELD>_DEFAULT`, named by the record or variant that holds
//!   the field. The type has the member all the same: Python's form writes
//!   every member, and so does the converter.
//!
//! A converter writes the members of a value in the model's order, the tag
//! first for a variant. In the wire value an integer is a `bigint` and an
//! object is a `Map`. The package writes the text with its codec writer.

use horsies_core::manifest_schema::{
    IntBound, ManifestField, ManifestModule, ManifestShape, ManifestType, MemberDefault,
    TaggedVariant, VariantFields, PRODUCER_MANIFEST, SCHEDULER_MANIFEST, SCHEDULE_PATTERN,
    WORKER_MANIFEST, WORKFLOW_SPEC_DATA,
};

use crate::model::{
    field_value_type, manifest_type_name, tagged_variant_type_name, ItemType, ValueType,
};
use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{
    bigint_literal, camel_case_of_pascal, import_types, integer_literal, is_identifier,
    is_safe_integer, module_text, property_key, string_literal, union_alias,
};
use crate::wire_json::{WIRE_JSON_MODULE, WIRE_JSON_TYPE};

/// The stem of the producer manifest module.
pub const PRODUCER_MANIFEST_MODULE: &str = "manifest";

/// The stem of the scheduler manifest module.
pub const SCHEDULER_MANIFEST_MODULE: &str = "scheduler_manifest";

/// The stem of the worker manifest module.
pub const WORKER_MANIFEST_MODULE: &str = "worker_manifest";

/// The stem of the workflow spec data module.
pub const WORKFLOW_SPEC_MODULE: &str = "workflow_spec";

/// The stem of the schedule pattern module.
pub const SCHEDULE_PATTERN_MODULE: &str = "schedule_pattern";

/// The constant of the version that the core reads.
const VERSION_CONSTANT: &str = "MANIFEST_VERSION";

/// The end of a converter name: `producerManifestToWire`.
const CONVERTER_SUFFIX: &str = "ToWire";

/// The module function that gives the wire `bigint` of a `number` integer.
/// A value that is not a safe integer is a defect of the caller: the
/// package fills the manifest from checked config.
const INTEGER_HELPER: &str = "integerToWire";

/// The text of [`INTEGER_HELPER`].
const INTEGER_HELPER_TEXT: &str =
    "function integerToWire(value: number, subject: string): bigint {\n  \
     if (!Number.isSafeInteger(value)) {\n    \
     throw new Error(`integerToWire: ${subject} is ${String(value)}, not a safe integer`);\n  \
     }\n\
     \n  \
     return BigInt(value);\n\
     }";

/// The indent of a statement of a function body.
const BODY_INDENT: &str = "  ";

/// The indent of a statement of a `case` of a converter's `switch`.
const CASE_INDENT: &str = "      ";

/// The files of the manifest modules, in a fixed order. Pure.
pub fn generated_files() -> Vec<GeneratedFile> {
    [
        (PRODUCER_MANIFEST_MODULE, PRODUCER_MANIFEST),
        (SCHEDULER_MANIFEST_MODULE, SCHEDULER_MANIFEST),
        (WORKER_MANIFEST_MODULE, WORKER_MANIFEST),
        (WORKFLOW_SPEC_MODULE, WORKFLOW_SPEC_DATA),
        (SCHEDULE_PATTERN_MODULE, SCHEDULE_PATTERN),
    ]
    .iter()
    .map(|(stem, module)| catalog_file(stem, render_manifest_module(module)))
    .collect()
}

/// The text of the TypeScript module of `module`: the import of the wire
/// type, the version, the bounds, the member defaults, the types, the
/// integer helper when a field holds a `number` integer, and the
/// converters. Pure.
///
/// Panics when the model breaks a rule that the text needs: the root is a
/// choice or no type of the module, a field names no type of the module, a
/// record has no field, a variant holds the fields of a type that is not a
/// record, or a member default is on a field that is not an integer.
pub fn render_manifest_module(module: &ManifestModule) -> String {
    let root = find_type(module, module.root);
    match root.shape {
        ManifestShape::Record { .. } | ManifestShape::Tagged { .. } => {}
        ManifestShape::Choice { .. } => panic!(
            "the root {} of a manifest module is a choice: a choice has no converter",
            module.root
        ),
    }
    let mut sections = vec![import_types(&[WIRE_JSON_TYPE], WIRE_JSON_MODULE)];
    sections.push(format!(
        "export const {VERSION_CONSTANT} = {};",
        integer_literal(i128::from(module.version))
    ));
    sections.extend(module.int_bounds().into_iter().map(bound_constants));
    sections.extend(member_default_constants(module));
    for manifest_type in module.types {
        sections.extend(type_declarations(manifest_type));
    }
    if uses_integer_helper(module) {
        sections.push(INTEGER_HELPER_TEXT.to_owned());
    }
    sections.extend(
        module
            .types
            .iter()
            .filter_map(|manifest_type| converter(module, manifest_type)),
    );
    module_text(&sections)
}

/// How a TypeScript value holds the integers of one bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntegerType {
    /// Every integer of the bound is a safe integer.
    Number,
    /// The bound passes `Number.MAX_SAFE_INTEGER`.
    BigInt,
}

/// The TypeScript type of the integers of `bound`. Every bound starts at 0
/// or above, so its `max` decides. Pure.
fn integer_type(bound: IntBound) -> IntegerType {
    match is_safe_integer(i128::from(bound.max)) {
        true => IntegerType::Number,
        false => IntegerType::BigInt,
    }
}

/// The literal of `value`, an integer of `bound`, in the type of the
/// bound. Pure.
fn integer_value_literal(bound: IntBound, value: u64) -> String {
    match integer_type(bound) {
        IntegerType::Number => integer_literal(i128::from(value)),
        IntegerType::BigInt => bigint_literal(i128::from(value)),
    }
}

/// The constants `<NAME>_MIN` and `<NAME>_MAX` of `bound`, as one section.
/// Pure.
fn bound_constants(bound: IntBound) -> String {
    format!(
        "export const {name}_MIN = {min};\nexport const {name}_MAX = {max};",
        name = bound.name,
        min = integer_value_literal(bound, bound.min),
        max = integer_value_literal(bound, bound.max),
    )
}

/// The type of `module` named `name`. Pure.
///
/// A name that the module does not list is a defect of the model.
fn find_type(module: &ManifestModule, name: &str) -> &'static ManifestType {
    module
        .types
        .iter()
        .find(|manifest_type| manifest_type_name(manifest_type) == name)
        .unwrap_or_else(|| {
            panic!(
                "{name} is not a type of the manifest module of {}",
                module.root
            )
        })
}

/// How a field holds its items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Container {
    /// The field is one item.
    Single,
    /// The field is a list of items.
    List,
    /// The field maps text keys to items.
    Map,
    /// The field is an item or no value.
    Optional,
}

/// How a field of type `value` holds its items, and the item type. Pure.
fn field_parts(value: ValueType) -> (Container, ItemType) {
    match value {
        ValueType::Bool => (Container::Single, ItemType::Bool),
        ValueType::Str => (Container::Single, ItemType::Str),
        ValueType::Int(bound) => (Container::Single, ItemType::Int(bound)),
        ValueType::Named(name) => (Container::Single, ItemType::Named(name)),
        ValueType::TimeOfDay => (Container::Single, ItemType::TimeOfDay),
        ValueType::List(item) => (Container::List, item),
        ValueType::Map(item) => (Container::Map, item),
        ValueType::Optional(item) => (Container::Optional, item),
    }
}

/// `holder.field`, the subject of a field in the text of the integer
/// helper. Pure.
fn field_subject(holder: &str, field: &ManifestField) -> String {
    format!("{holder}.{}", field.name)
}

/// The TypeScript type of an item. Pure.
fn item_type_text(item: ItemType) -> String {
    match item {
        ItemType::Bool => "boolean".to_owned(),
        ItemType::Str => "string".to_owned(),
        ItemType::Int(bound) => match integer_type(bound) {
            IntegerType::Number => "number".to_owned(),
            IntegerType::BigInt => "bigint".to_owned(),
        },
        ItemType::Named(name) => name.to_owned(),
        ItemType::TimeOfDay => "Temporal.PlainTime".to_owned(),
    }
}

/// The TypeScript type of a field. Pure.
fn field_type_text(field: &ManifestField) -> String {
    let (container, item) = field_parts(field_value_type(field));
    let item_text = item_type_text(item);
    match container {
        Container::Single => item_text,
        Container::List => format!("ReadonlyArray<{item_text}>"),
        Container::Map => format!("ReadonlyMap<string, {item_text}>"),
        Container::Optional => format!("{item_text} | null"),
    }
}

/// The member `name: Type` of a field. Pure.
fn field_member(field: &ManifestField) -> String {
    format!("{}: {}", property_key(field.name), field_type_text(field))
}

/// `export interface <heading> {` with one `readonly` member per line.
/// Pure.
fn interface(heading: &str, members: &[String]) -> String {
    let mut text = format!("export interface {heading} {{\n");
    for member in members {
        text.push_str(&format!("  readonly {member};\n"));
    }
    text.push('}');
    text
}

/// The declarations of `manifest_type`, one section each: an interface for
/// a record; one interface per variant and their union for a tagged type;
/// the union of the values for a choice. Pure.
fn type_declarations(manifest_type: &ManifestType) -> Vec<String> {
    let name = manifest_type_name(manifest_type);
    match manifest_type.shape {
        ManifestShape::Record { fields } => vec![record_interface(name, fields)],
        ManifestShape::Tagged {
            tag_field,
            variants,
        } => {
            let mut declarations: Vec<String> = variants
                .iter()
                .map(|variant| variant_interface(tag_field, variant))
                .collect();
            let members: Vec<String> = variants
                .iter()
                .map(|variant| tagged_variant_type_name(variant).to_owned())
                .collect();
            declarations.push(union_alias(name, &members));
            declarations
        }
        ManifestShape::Choice { values } => {
            let members: Vec<String> = values
                .iter()
                .map(|choice| string_literal(choice.value))
                .collect();
            vec![union_alias(name, &members)]
        }
    }
}

/// The interface of the record `name`. A record without a field is a
/// defect of the model: an empty interface has no TypeScript form. Pure.
fn record_interface(name: &str, fields: &[ManifestField]) -> String {
    assert!(
        !fields.is_empty(),
        "the record {name} has no field: an empty interface has no TypeScript form"
    );
    let members: Vec<String> = fields.iter().map(field_member).collect();
    interface(name, &members)
}

/// The interface of one variant: the tag member, then the variant's own
/// fields; for a variant that holds the fields of a record, the tag member
/// of an interface that extends the record. Pure.
fn variant_interface(tag_field: &str, variant: &TaggedVariant) -> String {
    let name = tagged_variant_type_name(variant);
    let tag = format!(
        "{}: {}",
        property_key(tag_field),
        string_literal(variant.tag_value)
    );
    match variant.fields {
        VariantFields::Own(fields) => {
            let members: Vec<String> = std::iter::once(tag)
                .chain(fields.iter().map(field_member))
                .collect();
            interface(name, &members)
        }
        VariantFields::OfRecord(record) => interface(&format!("{name} extends {record}"), &[tag]),
    }
}

/// The name of the converter of the type `type_name`:
/// `producerManifestToWire`. Pure.
fn converter_name(type_name: &str) -> String {
    format!("{}{CONVERTER_SUFFIX}", camel_case_of_pascal(type_name))
}

/// The first line of the converter of `type_name`, up to the body. Pure.
fn converter_signature(type_name: &str) -> String {
    format!(
        "export function {}(value: {type_name}): ReadonlyMap<string, {WIRE_JSON_TYPE}>",
        converter_name(type_name)
    )
}

/// How an item becomes its wire value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ItemWire {
    /// The item is its own wire value: a boolean, a text, a `bigint`, or a
    /// choice value.
    AsIs,
    /// The item needs a conversion.
    Converted(Conversion),
}

/// The conversion of an item to its wire value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Conversion {
    /// A `number` integer: [`INTEGER_HELPER`] gives its `bigint`.
    SafeInteger,
    /// A record or a tagged value: the converter of this name.
    Converter(String),
    /// A time of day: its text.
    TimeOfDayText,
}

/// How an item of `module` becomes its wire value. Pure.
fn item_wire(module: &ManifestModule, item: ItemType) -> ItemWire {
    match item {
        ItemType::Bool | ItemType::Str => ItemWire::AsIs,
        ItemType::Int(bound) => match integer_type(bound) {
            IntegerType::Number => ItemWire::Converted(Conversion::SafeInteger),
            IntegerType::BigInt => ItemWire::AsIs,
        },
        ItemType::Named(name) => match find_type(module, name).shape {
            ManifestShape::Record { .. } | ManifestShape::Tagged { .. } => {
                ItemWire::Converted(Conversion::Converter(converter_name(name)))
            }
            ManifestShape::Choice { .. } => ItemWire::AsIs,
        },
        ItemType::TimeOfDay => ItemWire::Converted(Conversion::TimeOfDayText),
    }
}

/// The wire expression of `access`, an item, by `conversion`. Pure.
fn conversion_expression(conversion: &Conversion, access: &str, subject: &str) -> String {
    match conversion {
        Conversion::SafeInteger => {
            format!("{INTEGER_HELPER}({access}, {})", string_literal(subject))
        }
        Conversion::Converter(name) => format!("{name}({access})"),
        Conversion::TimeOfDayText => format!("{access}.toString()"),
    }
}

/// The wire expression of the field `holder.name`, whose value is the
/// expression `access`. Pure.
fn field_wire_expression(
    module: &ManifestModule,
    holder: &str,
    field: &ManifestField,
    access: &str,
) -> String {
    let subject = field_subject(holder, field);
    let (container, item) = field_parts(field_value_type(field));
    match item_wire(module, item) {
        ItemWire::AsIs => access.to_owned(),
        ItemWire::Converted(conversion) => match container {
            Container::Single => conversion_expression(&conversion, access, &subject),
            Container::List => format!(
                "{access}.map((item) => {})",
                conversion_expression(&conversion, "item", &subject)
            ),
            Container::Map => format!(
                "new Map(Array.from({access}, ([key, item]) => [key, {}] as const))",
                conversion_expression(&conversion, "item", &subject)
            ),
            Container::Optional => format!(
                "{access} === null ? null : {}",
                conversion_expression(&conversion, access, &subject)
            ),
        },
    }
}

/// Whether a field of `module` holds a `number` integer, so the module
/// needs [`INTEGER_HELPER`]. Pure.
fn uses_integer_helper(module: &ManifestModule) -> bool {
    module
        .types
        .iter()
        .flat_map(held_fields)
        .any(|(_holder, field)| {
            let (_container, item) = field_parts(field_value_type(&field));
            item_wire(module, item) == ItemWire::Converted(Conversion::SafeInteger)
        })
}

/// Each field that `manifest_type` defines, with the record or the variant
/// that holds it. A variant that holds the fields of a record type adds
/// none: that record defines them. Pure.
fn held_fields(manifest_type: &ManifestType) -> Vec<(&'static str, ManifestField)> {
    match manifest_type.shape {
        ManifestShape::Record { fields } => {
            let holder = manifest_type_name(manifest_type);
            fields.iter().map(|field| (holder, *field)).collect()
        }
        ManifestShape::Tagged { variants, .. } => variants
            .iter()
            .flat_map(|variant| {
                let holder = tagged_variant_type_name(variant);
                let fields: &'static [ManifestField] = match variant.fields {
                    VariantFields::Own(fields) => fields,
                    VariantFields::OfRecord(_) => &[],
                };
                fields.iter().map(move |field| (holder, *field))
            })
            .collect(),
        ManifestShape::Choice { .. } => Vec::new(),
    }
}

/// `PascalCase` to `SCREAMING_SNAKE_CASE`: `HourlySchedule` gives
/// `HOURLY_SCHEDULE`. An underscore goes before each upper-case letter but
/// the first. Pure.
fn screaming_snake_case(pascal: &str) -> String {
    let mut text = String::with_capacity(pascal.len() * 2);
    for (position, character) in pascal.char_indices() {
        if position > 0 && character.is_ascii_uppercase() {
            text.push('_');
        }
        text.push(character.to_ascii_uppercase());
    }
    text
}

/// The constant of the member default of the field `holder.name`, or none
/// for a field without a default. Pure.
///
/// A default on a field that is not an integer is a defect of the model:
/// the core records a default on an integer field only.
fn member_default_constant(holder: &str, field: &ManifestField) -> Option<String> {
    match field.default {
        MemberDefault::NoDefault => None,
        MemberDefault::Int(default) => match field_value_type(field) {
            ValueType::Int(bound) => Some(format!(
                "export const {}_{}_DEFAULT = {};",
                screaming_snake_case(holder),
                field.name.to_ascii_uppercase(),
                integer_value_literal(bound, default)
            )),
            value @ (ValueType::Bool
            | ValueType::Str
            | ValueType::Named(_)
            | ValueType::List(_)
            | ValueType::Map(_)
            | ValueType::Optional(_)
            | ValueType::TimeOfDay) => panic!(
                "{holder}.{} has a member default, but its value {value:?} is not an integer",
                field.name
            ),
        },
    }
}

/// The member default constants of `module`, one section each, in the
/// model's order. Pure.
fn member_default_constants(module: &ManifestModule) -> Vec<String> {
    module
        .types
        .iter()
        .flat_map(held_fields)
        .filter_map(|(holder, field)| member_default_constant(holder, &field))
        .collect()
}

/// `object.name`, or `object["name"]` for a name that is not an
/// identifier. Pure.
fn member_access(object: &str, name: &str) -> String {
    match is_identifier(name) {
        true => format!("{object}.{name}"),
        false => format!("{object}[{}]", string_literal(name)),
    }
}

/// The entry `["name", <wire expression>]` of the field `holder.name` of
/// the converted `value`. Pure.
fn member_entry(module: &ManifestModule, holder: &str, field: &ManifestField) -> String {
    let access = member_access("value", field.name);
    format!(
        "[{}, {}]",
        string_literal(field.name),
        field_wire_expression(module, holder, field, &access)
    )
}

/// A `Map` of wire members: on one line for one entry, else one entry per
/// line. `indent` is the indent of the line where the `Map` starts. Pure.
fn map_literal(entries: &[String], indent: &str) -> String {
    match entries {
        [] => format!("new Map<string, {WIRE_JSON_TYPE}>()"),
        [single] => format!("new Map<string, {WIRE_JSON_TYPE}>([{single}])"),
        several => {
            let mut text = format!("new Map<string, {WIRE_JSON_TYPE}>([\n");
            for entry in several {
                text.push_str(&format!("{indent}  {entry},\n"));
            }
            text.push_str(&format!("{indent}])"));
            text
        }
    }
}

/// The converter of `manifest_type`, or none for a choice: a choice value
/// is its own wire value. Pure.
fn converter(module: &ManifestModule, manifest_type: &ManifestType) -> Option<String> {
    let name = manifest_type_name(manifest_type);
    match manifest_type.shape {
        ManifestShape::Record { fields } => Some(record_converter(module, name, fields)),
        ManifestShape::Tagged {
            tag_field,
            variants,
        } => Some(tagged_converter(module, name, tag_field, variants)),
        ManifestShape::Choice { .. } => None,
    }
}

/// The converter of the record `name`: a `Map` of every field in the
/// model's order. Pure.
fn record_converter(module: &ManifestModule, name: &str, fields: &[ManifestField]) -> String {
    let entries: Vec<String> = fields
        .iter()
        .map(|field| member_entry(module, name, field))
        .collect();
    format!(
        "{} {{\n{BODY_INDENT}return {};\n}}",
        converter_signature(name),
        map_literal(&entries, BODY_INDENT)
    )
}

/// The converter of the tagged type `name`: a `switch` on the tag with one
/// `case` per variant, and a `default` that the type makes unreachable.
/// Pure.
fn tagged_converter(
    module: &ManifestModule,
    name: &str,
    tag_field: &str,
    variants: &[TaggedVariant],
) -> String {
    let mut text = format!(
        "{} {{\n  switch ({}) {{\n",
        converter_signature(name),
        member_access("value", tag_field)
    );
    for variant in variants {
        text.push_str(&format!(
            "    case {}:\n{CASE_INDENT}return {};\n",
            string_literal(variant.tag_value),
            variant_map(module, tag_field, variant)
        ));
    }
    text.push_str(
        "    default: {\n      const unreachable: never = value;\n\n      return unreachable;\n    }\n  }\n}",
    );
    text
}

/// The wire `Map` of one variant: the tag, then the variant's own fields,
/// or the members of the record whose fields the variant holds. Pure.
fn variant_map(module: &ManifestModule, tag_field: &str, variant: &TaggedVariant) -> String {
    let name = tagged_variant_type_name(variant);
    let tag = format!(
        "[{}, {}]",
        string_literal(tag_field),
        string_literal(variant.tag_value)
    );
    match variant.fields {
        VariantFields::Own(fields) => {
            let entries: Vec<String> = std::iter::once(tag)
                .chain(fields.iter().map(|field| member_entry(module, name, field)))
                .collect();
            map_literal(&entries, CASE_INDENT)
        }
        VariantFields::OfRecord(record) => {
            let entries = [
                tag,
                format!("...{}(value)", held_record_converter(module, name, record)),
            ];
            map_literal(&entries, CASE_INDENT)
        }
    }
}

/// The converter of `record`, whose fields the variant `variant` holds. A
/// name that is not a record type of the module is a defect of the model.
/// Pure.
fn held_record_converter(module: &ManifestModule, variant: &str, record: &str) -> String {
    match find_type(module, record).shape {
        ManifestShape::Record { .. } => converter_name(record),
        ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => panic!(
            "{variant} holds the fields of {record}, which is not a record type of the module"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;

    use horsies_core::manifest_schema::{ChoiceValue, MemberDefault, PythonItem, PythonValue};

    use super::*;
    use crate::ts_text::{GENERATED_HEADER, MAX_SAFE_INTEGER};

    const PRIORITY: IntBound = IntBound {
        name: "PRIORITY",
        min: 1,
        max: 100_000,
    };

    const BYTES: IntBound = IntBound {
        name: "BYTES",
        min: 1,
        max: u64::MAX,
    };

    /// The largest bound whose integers are `number`.
    const SAFE: IntBound = IntBound {
        name: "SAFE",
        min: 0,
        max: MAX_SAFE_INTEGER as u64,
    };

    fn field(name: &'static str, value: PythonValue) -> ManifestField {
        ManifestField {
            name,
            doc: "",
            value,
            default: MemberDefault::NoDefault,
        }
    }

    const MODE: ManifestType = ManifestType {
        python_name: "Mode",
        doc: "",
        shape: ManifestShape::Choice {
            values: &[
                ChoiceValue {
                    value: "ONE",
                    doc: "",
                },
                ChoiceValue {
                    value: "TWO",
                    doc: "",
                },
            ],
        },
    };

    const LIMIT: ManifestType = ManifestType {
        python_name: "Limit",
        doc: "",
        shape: ManifestShape::Tagged {
            tag_field: "kind",
            variants: &[
                TaggedVariant {
                    python_class: "LimitBytes",
                    tag_value: "BYTES",
                    doc: "",
                    fields: VariantFields::Own(&[ManifestField {
                        name: "bytes",
                        doc: "",
                        value: PythonValue::Int(BYTES),
                        default: MemberDefault::NoDefault,
                    }]),
                },
                TaggedVariant {
                    python_class: "LimitNoLimit",
                    tag_value: "NO_LIMIT",
                    doc: "",
                    fields: VariantFields::Own(&[]),
                },
            ],
        },
    };

    const QUEUE: ManifestType = ManifestType {
        python_name: "Queue",
        doc: "",
        shape: ManifestShape::Record {
            fields: &[
                ManifestField {
                    name: "name",
                    doc: "The doc is not rendered.",
                    value: PythonValue::Str,
                    default: MemberDefault::NoDefault,
                },
                ManifestField {
                    name: "priority",
                    doc: "",
                    value: PythonValue::Int(PRIORITY),
                    default: MemberDefault::NoDefault,
                },
            ],
        },
    };

    const ROOT: ManifestType = ManifestType {
        python_name: "Root",
        doc: "",
        shape: ManifestShape::Record {
            fields: &[
                ManifestField {
                    name: "manifest_version",
                    doc: "",
                    value: PythonValue::Int(PRIORITY),
                    default: MemberDefault::NoDefault,
                },
                ManifestField {
                    name: "mode",
                    doc: "",
                    value: PythonValue::Named("Mode"),
                    default: MemberDefault::NoDefault,
                },
                ManifestField {
                    name: "enabled",
                    doc: "",
                    value: PythonValue::Bool,
                    default: MemberDefault::NoDefault,
                },
                ManifestField {
                    name: "limit",
                    doc: "",
                    value: PythonValue::Named("Limit"),
                    default: MemberDefault::NoDefault,
                },
                ManifestField {
                    name: "queues",
                    doc: "",
                    value: PythonValue::List(PythonItem::Named("Queue")),
                    default: MemberDefault::NoDefault,
                },
                ManifestField {
                    name: "names",
                    doc: "",
                    value: PythonValue::List(PythonItem::Str),
                    default: MemberDefault::NoDefault,
                },
                ManifestField {
                    name: "priorities",
                    doc: "",
                    value: PythonValue::List(PythonItem::Int(PRIORITY)),
                    default: MemberDefault::NoDefault,
                },
            ],
        },
    };

    /// A module of every form that the producer and scheduler manifests
    /// use.
    const SAMPLE: ManifestModule = ManifestModule {
        root: "Root",
        version: 3,
        reader: "",
        reader_short: "",
        types: &[MODE, LIMIT, QUEUE, ROOT],
    };

    #[test]
    fn a_record_is_a_readonly_interface_with_the_wire_keys() {
        assert_eq!(
            type_declarations(&QUEUE),
            ["export interface Queue {\n  readonly name: string;\n  readonly priority: number;\n}"]
        );
    }

    #[test]
    fn a_tagged_type_is_one_interface_per_variant_and_their_union() {
        assert_eq!(
            type_declarations(&LIMIT),
            [
                "export interface LimitBytes {\n  readonly kind: \"BYTES\";\n  readonly bytes: bigint;\n}",
                "export interface LimitNoLimit {\n  readonly kind: \"NO_LIMIT\";\n}",
                "export type Limit =\n  | LimitBytes\n  | LimitNoLimit;",
            ]
        );
    }

    #[test]
    fn a_choice_is_the_union_of_its_values() {
        assert_eq!(
            type_declarations(&MODE),
            ["export type Mode =\n  | \"ONE\"\n  | \"TWO\";"]
        );
    }

    #[test]
    fn a_bound_is_a_pair_of_constants_of_the_type_of_its_integers() {
        assert_eq!(
            bound_constants(PRIORITY),
            "export const PRIORITY_MIN = 1;\nexport const PRIORITY_MAX = 100_000;"
        );
        assert_eq!(
            bound_constants(SAFE),
            "export const SAFE_MIN = 0;\nexport const SAFE_MAX = 9_007_199_254_740_991;"
        );
        assert_eq!(
            bound_constants(BYTES),
            "export const BYTES_MIN = 1n;\nexport const BYTES_MAX = 18_446_744_073_709_551_615n;"
        );
    }

    #[test]
    fn a_record_converter_writes_every_field_in_the_model_order() {
        let ManifestShape::Record { fields } = ROOT.shape else {
            panic!("the sample root is a record");
        };
        assert_eq!(
            record_converter(&SAMPLE, "Root", fields),
            "export function rootToWire(value: Root): ReadonlyMap<string, WireJson> {\n  \
             return new Map<string, WireJson>([\n    \
             [\"manifest_version\", integerToWire(value.manifest_version, \"Root.manifest_version\")],\n    \
             [\"mode\", value.mode],\n    \
             [\"enabled\", value.enabled],\n    \
             [\"limit\", limitToWire(value.limit)],\n    \
             [\"queues\", value.queues.map((item) => queueToWire(item))],\n    \
             [\"names\", value.names],\n    \
             [\"priorities\", value.priorities.map((item) => integerToWire(item, \"Root.priorities\"))],\n  \
             ]);\n\
             }"
        );
    }

    #[test]
    fn a_tagged_converter_switches_on_the_tag_and_writes_it_first() {
        let ManifestShape::Tagged {
            tag_field,
            variants,
        } = LIMIT.shape
        else {
            panic!("the sample limit is a tagged type");
        };
        assert_eq!(
            tagged_converter(&SAMPLE, "Limit", tag_field, variants),
            "export function limitToWire(value: Limit): ReadonlyMap<string, WireJson> {\n  \
             switch (value.kind) {\n    \
             case \"BYTES\":\n      \
             return new Map<string, WireJson>([\n        \
             [\"kind\", \"BYTES\"],\n        \
             [\"bytes\", value.bytes],\n      \
             ]);\n    \
             case \"NO_LIMIT\":\n      \
             return new Map<string, WireJson>([[\"kind\", \"NO_LIMIT\"]]);\n    \
             default: {\n      \
             const unreachable: never = value;\n\
             \n      \
             return unreachable;\n    \
             }\n  \
             }\n\
             }"
        );
    }

    #[test]
    fn a_choice_has_no_converter() {
        assert_eq!(converter(&SAMPLE, &MODE), None);
    }

    #[test]
    fn a_module_has_its_sections_in_order() {
        let text = render_manifest_module(&SAMPLE);
        let order = [
            "import type { WireJson } from \"./wire_json.ts\";\n\nexport const MANIFEST_VERSION = 3;\n",
            "\nexport const BYTES_MIN = 1n;\n",
            "\nexport const PRIORITY_MIN = 1;\nexport const PRIORITY_MAX = 100_000;\n",
            "\nexport type Mode =\n",
            "\nexport interface LimitBytes {\n",
            "\nexport interface Queue {\n",
            "\nexport interface Root {\n",
            "\nfunction integerToWire(value: number, subject: string): bigint {\n",
            "\nexport function limitToWire(",
            "\nexport function queueToWire(",
            "\nexport function rootToWire(",
        ];
        let positions: Vec<usize> = order
            .iter()
            .map(|part| {
                text.find(part)
                    .unwrap_or_else(|| panic!("{part} is in\n{text}"))
            })
            .collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        assert_eq!(positions, sorted);
        assert!(text.starts_with(&format!("{GENERATED_HEADER}\n\nimport type")));
        assert!(text.ends_with("\n}\n"));
    }

    #[test]
    fn a_module_without_a_number_integer_has_no_integer_helper() {
        let module = ManifestModule {
            root: "Limit",
            version: 1,
            reader: "",
            reader_short: "",
            types: &[LIMIT],
        };
        assert!(!uses_integer_helper(&module));
        assert!(!render_manifest_module(&module).contains(INTEGER_HELPER));
        assert!(uses_integer_helper(&SAMPLE));
    }

    #[test]
    #[should_panic(expected = "the root Mode of a manifest module is a choice")]
    fn a_choice_root_is_a_defect() {
        let module = ManifestModule {
            root: "Mode",
            version: 1,
            reader: "",
            reader_short: "",
            types: &[MODE],
        };
        let _refused = render_manifest_module(&module);
    }

    #[test]
    #[should_panic(expected = "Missing is not a type of the manifest module of Root")]
    fn a_field_that_names_no_type_of_the_module_is_a_defect() {
        let module = ManifestModule {
            root: "Root",
            version: 1,
            reader: "",
            reader_short: "",
            types: &[ManifestType {
                python_name: "Root",
                doc: "",
                shape: ManifestShape::Record {
                    fields: &[ManifestField {
                        name: "missing",
                        doc: "",
                        value: PythonValue::Named("Missing"),
                        default: MemberDefault::NoDefault,
                    }],
                },
            }],
        };
        let _refused = render_manifest_module(&module);
    }

    #[test]
    #[should_panic(expected = "the record Empty has no field")]
    fn a_record_without_a_field_is_a_defect() {
        let _refused = record_interface("Empty", &[]);
    }

    #[test]
    fn a_map_field_is_a_readonly_map_from_text_keys() {
        assert_eq!(
            field_type_text(&field("ages", PythonValue::Map(PythonItem::Int(PRIORITY)))),
            "ReadonlyMap<string, number>"
        );
        assert_eq!(
            field_type_text(&field(
                "limits",
                PythonValue::Map(PythonItem::Named("Limit"))
            )),
            "ReadonlyMap<string, Limit>"
        );
    }

    #[test]
    fn a_map_member_is_a_new_map_of_the_same_keys_with_each_item_converted() {
        assert_eq!(
            member_entry(
                &SAMPLE,
                "Root",
                &field("ages", PythonValue::Map(PythonItem::Int(PRIORITY)))
            ),
            "[\"ages\", new Map(Array.from(value.ages, ([key, item]) => \
             [key, integerToWire(item, \"Root.ages\")] as const))]"
        );
        assert_eq!(
            member_entry(
                &SAMPLE,
                "Root",
                &field("limits", PythonValue::Map(PythonItem::Named("Limit")))
            ),
            "[\"limits\", new Map(Array.from(value.limits, ([key, item]) => \
             [key, limitToWire(item)] as const))]"
        );
    }

    #[test]
    fn a_map_of_items_that_are_their_own_wire_value_is_the_wire_value() {
        for (name, value) in [
            ("classes", PythonValue::Map(PythonItem::Str)),
            ("modes", PythonValue::Map(PythonItem::Named("Mode"))),
            ("sizes", PythonValue::Map(PythonItem::Int(BYTES))),
        ] {
            assert_eq!(
                member_entry(&SAMPLE, "Root", &field(name, value)),
                format!("[\"{name}\", value.{name}]")
            );
        }
    }

    #[test]
    fn an_optional_field_is_its_item_type_or_null() {
        assert_eq!(
            field_type_text(&field("note", PythonValue::Optional(PythonItem::Str))),
            "string | null"
        );
        assert_eq!(
            field_type_text(&field(
                "seconds",
                PythonValue::Optional(PythonItem::Int(PRIORITY))
            )),
            "number | null"
        );
    }

    #[test]
    fn an_optional_member_is_null_or_the_converted_item() {
        assert_eq!(
            member_entry(
                &SAMPLE,
                "Root",
                &field("seconds", PythonValue::Optional(PythonItem::Int(PRIORITY)))
            ),
            "[\"seconds\", value.seconds === null ? null : \
             integerToWire(value.seconds, \"Root.seconds\")]"
        );
        assert_eq!(
            member_entry(
                &SAMPLE,
                "Root",
                &field("note", PythonValue::Optional(PythonItem::Str))
            ),
            "[\"note\", value.note]"
        );
    }

    #[test]
    fn a_time_of_day_is_a_plain_time_whose_wire_value_is_its_text() {
        assert_eq!(
            field_type_text(&field("at", PythonValue::TimeOfDay)),
            "Temporal.PlainTime"
        );
        assert_eq!(
            member_entry(&SAMPLE, "Root", &field("at", PythonValue::TimeOfDay)),
            "[\"at\", value.at.toString()]"
        );
        assert_eq!(
            member_entry(
                &SAMPLE,
                "Root",
                &field("times", PythonValue::List(PythonItem::TimeOfDay))
            ),
            "[\"times\", value.times.map((item) => item.toString())]"
        );
    }

    const DAILY: ManifestType = ManifestType {
        python_name: "Daily",
        doc: "",
        shape: ManifestShape::Record {
            fields: &[ManifestField {
                name: "time",
                doc: "",
                value: PythonValue::TimeOfDay,
                default: MemberDefault::NoDefault,
            }],
        },
    };

    const PATTERN_DAILY: TaggedVariant = TaggedVariant {
        python_class: "PatternDaily",
        tag_value: "daily",
        doc: "",
        fields: VariantFields::OfRecord("Daily"),
    };

    /// A module whose tagged root has a variant that holds a record.
    const PATTERN: ManifestModule = ManifestModule {
        root: "Pattern",
        version: 1,
        reader: "",
        reader_short: "",
        types: &[
            DAILY,
            ManifestType {
                python_name: "Pattern",
                doc: "",
                shape: ManifestShape::Tagged {
                    tag_field: "type",
                    variants: &[PATTERN_DAILY],
                },
            },
        ],
    };

    #[test]
    fn a_variant_that_holds_a_record_extends_the_record_with_its_tag() {
        assert_eq!(
            variant_interface("type", &PATTERN_DAILY),
            "export interface PatternDaily extends Daily {\n  readonly type: \"daily\";\n}"
        );
    }

    #[test]
    fn a_variant_that_holds_a_record_writes_the_tag_then_the_record_members() {
        assert_eq!(
            variant_map(&PATTERN, "type", &PATTERN_DAILY),
            "new Map<string, WireJson>([\n        \
             [\"type\", \"daily\"],\n        \
             ...dailyToWire(value),\n      \
             ])"
        );
    }

    #[test]
    #[should_panic(
        expected = "PatternMode holds the fields of Mode, which is not a record type of the module"
    )]
    fn a_variant_that_holds_the_fields_of_a_choice_is_a_defect() {
        let variant = TaggedVariant {
            python_class: "PatternMode",
            tag_value: "mode",
            doc: "",
            fields: VariantFields::OfRecord("Mode"),
        };
        let _refused = variant_map(&SAMPLE, "type", &variant);
    }

    #[test]
    fn a_member_default_is_a_constant_named_by_its_holder_and_field() {
        let second = ManifestField {
            name: "second",
            doc: "",
            value: PythonValue::Int(PRIORITY),
            default: MemberDefault::Int(7),
        };
        assert_eq!(
            member_default_constant("HourlySchedule", &second),
            Some("export const HOURLY_SCHEDULE_SECOND_DEFAULT = 7;".to_owned())
        );
        let step = ManifestField {
            name: "max_step",
            doc: "",
            value: PythonValue::Int(BYTES),
            default: MemberDefault::Int(10_000),
        };
        assert_eq!(
            member_default_constant("CronEnumTermWeekdayEnumRange", &step),
            Some(
                "export const CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_MAX_STEP_DEFAULT = 10_000n;"
                    .to_owned()
            )
        );
    }

    #[test]
    fn a_field_without_a_member_default_has_no_constant() {
        assert_eq!(
            member_default_constant("Root", &field("enabled", PythonValue::Bool)),
            None
        );
    }

    #[test]
    #[should_panic(
        expected = "Root.note has a member default, but its value Str is not an integer"
    )]
    fn a_member_default_on_a_field_that_is_not_an_integer_is_a_defect() {
        let note = ManifestField {
            name: "note",
            doc: "",
            value: PythonValue::Str,
            default: MemberDefault::Int(1),
        };
        let _refused = member_default_constant("Root", &note);
    }

    #[test]
    fn the_defaults_of_a_module_come_from_records_and_variants_in_model_order() {
        let module = ManifestModule {
            root: "Root",
            version: 1,
            reader: "",
            reader_short: "",
            types: &[
                ManifestType {
                    python_name: "Term",
                    doc: "",
                    shape: ManifestShape::Tagged {
                        tag_field: "kind",
                        variants: &[TaggedVariant {
                            python_class: "TermRange",
                            tag_value: "range",
                            doc: "",
                            fields: VariantFields::Own(&[ManifestField {
                                name: "step",
                                doc: "",
                                value: PythonValue::Int(PRIORITY),
                                default: MemberDefault::Int(1),
                            }]),
                        }],
                    },
                },
                ManifestType {
                    python_name: "Root",
                    doc: "",
                    shape: ManifestShape::Record {
                        fields: &[
                            ManifestField {
                                name: "term",
                                doc: "",
                                value: PythonValue::Named("Term"),
                                default: MemberDefault::NoDefault,
                            },
                            ManifestField {
                                name: "second",
                                doc: "",
                                value: PythonValue::Int(PRIORITY),
                                default: MemberDefault::Int(0),
                            },
                        ],
                    },
                },
            ],
        };
        assert_eq!(
            member_default_constants(&module),
            [
                "export const TERM_RANGE_STEP_DEFAULT = 1;",
                "export const ROOT_SECOND_DEFAULT = 0;",
            ]
        );
    }

    /// Each declared name of `text`: `export const`, `export interface`,
    /// `export type`, `export function` and `function`.
    fn declared_names(text: &str) -> Vec<&str> {
        const KEYWORDS: [&str; 5] = [
            "export const ",
            "export interface ",
            "export type ",
            "export function ",
            "function ",
        ];
        text.lines()
            .filter_map(|line| {
                KEYWORDS
                    .iter()
                    .find_map(|keyword| line.strip_prefix(keyword))
                    .and_then(|rest| rest.split([' ', '(', '<']).next())
            })
            .collect()
    }

    /// Check the module text of a real manifest: every type, every bound
    /// pair and every converter once, the root converter, and no name twice.
    fn assert_module_text(module: &ManifestModule, text: &str) {
        let names = declared_names(text);
        let unique: BTreeSet<&str> = names.iter().copied().collect();
        assert_eq!(unique.len(), names.len(), "a name is declared twice");
        for manifest_type in module.types {
            let name = manifest_type_name(manifest_type);
            let declaration = match manifest_type.shape {
                ManifestShape::Record { .. } => format!("\nexport interface {name} {{\n"),
                ManifestShape::Tagged { .. } | ManifestShape::Choice { .. } => {
                    format!("\nexport type {name} =")
                }
            };
            assert!(text.contains(&declaration), "{name}");
            let converter_start = format!(
                "\nexport function {}(value: {name}): ",
                converter_name(name)
            );
            match manifest_type.shape {
                ManifestShape::Record { .. } | ManifestShape::Tagged { .. } => {
                    assert!(text.contains(&converter_start), "the converter of {name}");
                }
                ManifestShape::Choice { .. } => {
                    assert!(!text.contains(&converter_start), "a converter of {name}");
                }
            }
        }
        for bound in module.int_bounds() {
            assert!(
                text.contains(&format!("\n{}\n", bound_constants(bound))),
                "{}",
                bound.name
            );
        }
        assert!(text.contains(&format!(
            "\nexport const MANIFEST_VERSION = {};\n",
            module.version
        )));
    }

    #[test]
    fn the_producer_manifest_module_has_every_type_bound_and_converter_once() {
        let text = render_manifest_module(&PRODUCER_MANIFEST);
        assert_module_text(&PRODUCER_MANIFEST, &text);
        assert!(text.contains(
            "\nexport function producerManifestToWire(value: ProducerManifest): \
             ReadonlyMap<string, WireJson> {\n"
        ));
        assert!(text.contains("\nexport const POOL_SIZE_MAX = 10_000;\n"));
        assert!(text.contains(
            "\nexport interface SessionDatabaseSeparate {\n  readonly kind: \"SEPARATE\";\n  \
             readonly url: string;\n}\n"
        ));
        assert!(text.contains(
            "\nexport type SchemaMode =\n  | \"MIGRATE_AND_VALIDATE\"\n  | \"OBSERVE_ONLY\"\n  \
             | \"ASSUME_INITIALIZED\";\n"
        ));
    }

    #[test]
    fn the_scheduler_manifest_module_has_every_type_bound_and_converter_once() {
        let text = render_manifest_module(&SCHEDULER_MANIFEST);
        assert_module_text(&SCHEDULER_MANIFEST, &text);
        assert!(text.contains(
            "\nexport function schedulerManifestToWire(value: SchedulerManifest): \
             ReadonlyMap<string, WireJson> {\n"
        ));
        assert!(text.contains("  readonly schedules: ReadonlyArray<ScheduleManifest>;\n"));
        assert!(text.contains("  readonly pattern_json: string;\n"));
        assert!(text.contains(
            "    [\"schedules\", value.schedules.map((item) => scheduleManifestToWire(item))],\n"
        ));
    }

    #[test]
    fn the_worker_manifest_module_has_every_type_bound_and_converter_once() {
        let text = render_manifest_module(&WORKER_MANIFEST);
        assert_module_text(&WORKER_MANIFEST, &text);
        for part in [
            "\nexport function workerManifestToWire(value: WorkerManifest): \
             ReadonlyMap<string, WireJson> {\n",
            "\n  readonly profiles: ReadonlyMap<string, WorkerProfileManifest>;\n",
            "\n    [\"profiles\", new Map(Array.from(value.profiles, ([key, item]) => \
             [key, workerProfileManifestToWire(item)] as const))],\n",
            "\nexport interface QueuesCustom {\n  readonly kind: \"CUSTOM\";\n  \
             readonly queues: ReadonlyArray<CustomQueueManifest>;\n}\n",
            "\nexport type ChildProgram =\n  | ChildProgramPython\n  | ChildProgramRust;\n",
            "\nexport interface ChildProgramRust {\n  readonly kind: \"RUST\";\n}\n",
        ] {
            assert!(text.contains(part), "{part}");
        }
    }

    #[test]
    fn the_workflow_spec_module_has_every_type_bound_and_converter_once() {
        let text = render_manifest_module(&WORKFLOW_SPEC_DATA);
        assert_module_text(&WORKFLOW_SPEC_DATA, &text);
        for part in [
            "\nexport function workflowSpecDataToWire(value: WorkflowSpecData): \
             ReadonlyMap<string, WireJson> {\n",
            "\n  readonly args_from: ReadonlyMap<string, number>;\n",
            "\n    [\"args_from\", new Map(Array.from(value.args_from, ([key, item]) => \
             [key, integerToWire(item, \"SpecNode.args_from\")] as const))],\n",
            "\n    [\"queue_classes\", value.queue_classes],\n",
            "\nexport type SpecOnError =\n  | \"FAIL\"\n  | \"PAUSE\";\n",
        ] {
            assert!(text.contains(part), "{part}");
        }
    }

    #[test]
    fn only_the_paused_age_and_the_payload_bounds_pass_the_safe_range() {
        let modules = [
            PRODUCER_MANIFEST,
            SCHEDULER_MANIFEST,
            WORKER_MANIFEST,
            WORKFLOW_SPEC_DATA,
        ];
        let bigint_bounds: Vec<&str> = modules
            .iter()
            .flat_map(ManifestModule::int_bounds)
            .filter(|bound| integer_type(*bound) == IntegerType::BigInt)
            .map(|bound| bound.name)
            .collect();
        assert_eq!(bigint_bounds, ["PAUSED_AGE_MICROS", "PAYLOAD_BYTES"]);
        let text = render_manifest_module(&WORKER_MANIFEST);
        for part in [
            "\nexport const PAUSED_AGE_MICROS_MIN = 1n;\n\
             export const PAUSED_AGE_MICROS_MAX = 31_536_000_000_000_000n;\n",
            "\nexport const PAYLOAD_BYTES_MIN = 1n;\n\
             export const PAYLOAD_BYTES_MAX = 18_446_744_073_709_551_615n;\n",
            "\n  readonly micros: bigint;\n",
            "\n  readonly bytes: bigint;\n",
            "\n        [\"micros\", value.micros],\n",
            "\n        [\"bytes\", value.bytes],\n",
        ] {
            assert!(text.contains(part), "{part}");
        }
    }

    #[test]
    fn the_schedule_pattern_module_has_every_type_bound_default_and_converter_once() {
        let text = render_manifest_module(&SCHEDULE_PATTERN);
        assert_module_text(&SCHEDULE_PATTERN, &text);
        let bounds: Vec<&str> = SCHEDULE_PATTERN
            .int_bounds()
            .iter()
            .map(|bound| bound.name)
            .collect();
        assert_eq!(
            bounds,
            [
                "INTERVAL_SECONDS",
                "INTERVAL_MINUTES",
                "INTERVAL_HOURS",
                "INTERVAL_DAYS",
                "HOURLY_MINUTE",
                "HOURLY_SECOND",
                "MONTHLY_DAY",
                "CRON_NUMERIC_STEP",
                "CRON_NUMERIC_VALUE",
                "CRON_ENUM_STEP",
            ]
        );
        let defaults: Vec<&str> = text
            .lines()
            .filter(|line| line.contains("_DEFAULT = "))
            .collect();
        assert_eq!(
            defaults,
            [
                "export const HOURLY_SCHEDULE_SECOND_DEFAULT = 0;",
                "export const CRON_NUMERIC_TERM_RANGE_STEP_DEFAULT = 1;",
                "export const CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_STEP_DEFAULT = 1;",
                "export const CRON_ENUM_TERM_MONTH_ENUM_RANGE_STEP_DEFAULT = 1;",
            ]
        );
        for part in [
            "\nexport interface SchedulePatternInterval extends IntervalSchedule {\n  \
             readonly type: \"interval\";\n}\n",
            "\n  readonly seconds: number | null;\n",
            "\n  readonly time: Temporal.PlainTime;\n",
            "\nexport type Weekday =\n  | \"monday\"\n",
            "\nexport function schedulePatternToWire(value: SchedulePattern): \
             ReadonlyMap<string, WireJson> {\n  switch (value.type) {\n",
            "\n        [\"type\", \"cron\"],\n        ...cronScheduleToWire(value),\n",
        ] {
            assert!(text.contains(part), "{part}");
        }
    }

    #[test]
    fn the_files_are_the_five_manifest_modules() {
        let files = generated_files();
        let paths: Vec<&Path> = files.iter().map(GeneratedFile::relative_path).collect();
        assert_eq!(
            paths,
            [
                Path::new("src/_catalog/manifest.ts"),
                Path::new("src/_catalog/scheduler_manifest.ts"),
                Path::new("src/_catalog/worker_manifest.ts"),
                Path::new("src/_catalog/workflow_spec.ts"),
                Path::new("src/_catalog/schedule_pattern.ts"),
            ]
        );
        let modules = [
            PRODUCER_MANIFEST,
            SCHEDULER_MANIFEST,
            WORKER_MANIFEST,
            WORKFLOW_SPEC_DATA,
            SCHEDULE_PATTERN,
        ];
        for (file, module) in files.iter().zip(modules.iter()) {
            assert_eq!(file.text(), render_manifest_module(module));
        }
    }
}
