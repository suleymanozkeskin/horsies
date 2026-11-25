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
//! - A list is a `ReadonlyArray`.
//!
//! A converter writes the members of a value in the model's order, the tag
//! first for a variant. In the wire value an integer is a `bigint` and an
//! object is a `Map`. The package writes the text with its codec writer.

use horsies_core::manifest_schema::{
    type_fields, IntBound, ManifestField, ManifestModule, ManifestShape, ManifestType,
    TaggedVariant, VariantFields, PRODUCER_MANIFEST, SCHEDULER_MANIFEST,
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
    ]
    .iter()
    .map(|(stem, module)| catalog_file(stem, render_manifest_module(module)))
    .collect()
}

/// The text of the TypeScript module of `module`: the import of the wire
/// type, the version, the bounds, the types, the integer helper when a
/// field holds a `number` integer, and the converters. Pure.
///
/// Panics when the model breaks a rule that the text needs: the root is a
/// choice or no type of the module, a field names no type of the module,
/// or a record has no field.
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
}

/// How a field of type `value` holds its items, and the item type. Pure.
///
/// Panics for a form that this renderer has no TypeScript form for;
/// `subject` names the field.
fn field_parts(value: ValueType, subject: &str) -> (Container, ItemType) {
    match value {
        ValueType::Bool => (Container::Single, ItemType::Bool),
        ValueType::Str => (Container::Single, ItemType::Str),
        ValueType::Int(bound) => (Container::Single, ItemType::Int(bound)),
        ValueType::Named(name) => (Container::Single, ItemType::Named(name)),
        ValueType::TimeOfDay => (Container::Single, ItemType::TimeOfDay),
        ValueType::List(item) => (Container::List, item),
        ValueType::Map(_) => no_typescript_form("a map", subject),
        ValueType::Optional(_) => no_typescript_form("an optional value", subject),
    }
}

/// Stop the render: the model holds `form` at `subject`, and this renderer
/// has no TypeScript form for it.
fn no_typescript_form(form: &str, subject: &str) -> ! {
    panic!("{subject} is {form}: the manifest renderer has no TypeScript form for it")
}

/// `holder.field`, the subject of a field in panics and in the text of the
/// integer helper. Pure.
fn field_subject(holder: &str, field: &ManifestField) -> String {
    format!("{holder}.{}", field.name)
}

/// The TypeScript type of an item. Pure.
fn item_type_text(item: ItemType, subject: &str) -> String {
    match item {
        ItemType::Bool => "boolean".to_owned(),
        ItemType::Str => "string".to_owned(),
        ItemType::Int(bound) => match integer_type(bound) {
            IntegerType::Number => "number".to_owned(),
            IntegerType::BigInt => "bigint".to_owned(),
        },
        ItemType::Named(name) => name.to_owned(),
        ItemType::TimeOfDay => no_typescript_form("a time of day", subject),
    }
}

/// The TypeScript type of the field `holder.name`. Pure.
fn field_type_text(holder: &str, field: &ManifestField) -> String {
    let subject = field_subject(holder, field);
    let (container, item) = field_parts(field_value_type(field), &subject);
    let item_text = item_type_text(item, &subject);
    match container {
        Container::Single => item_text,
        Container::List => format!("ReadonlyArray<{item_text}>"),
    }
}

/// The member `name: Type` of the field `holder.name`. Pure.
fn field_member(holder: &str, field: &ManifestField) -> String {
    format!(
        "{}: {}",
        property_key(field.name),
        field_type_text(holder, field)
    )
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
    let members: Vec<String> = fields
        .iter()
        .map(|field| field_member(name, field))
        .collect();
    interface(name, &members)
}

/// The interface of one variant: the tag member, then the variant's own
/// fields. Pure.
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
                .chain(fields.iter().map(|field| field_member(name, field)))
                .collect();
            interface(name, &members)
        }
        VariantFields::OfRecord(record) => {
            no_typescript_form(&format!("a variant that holds the record {record}"), name)
        }
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
}

/// How an item of `module` becomes its wire value. Pure.
fn item_wire(module: &ManifestModule, item: ItemType, subject: &str) -> ItemWire {
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
        ItemType::TimeOfDay => no_typescript_form("a time of day", subject),
    }
}

/// The wire expression of `access`, an item, by `conversion`. Pure.
fn conversion_expression(conversion: &Conversion, access: &str, subject: &str) -> String {
    match conversion {
        Conversion::SafeInteger => {
            format!("{INTEGER_HELPER}({access}, {})", string_literal(subject))
        }
        Conversion::Converter(name) => format!("{name}({access})"),
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
    let (container, item) = field_parts(field_value_type(field), &subject);
    match item_wire(module, item, &subject) {
        ItemWire::AsIs => access.to_owned(),
        ItemWire::Converted(conversion) => match container {
            Container::Single => conversion_expression(&conversion, access, &subject),
            Container::List => format!(
                "{access}.map((item) => {})",
                conversion_expression(&conversion, "item", &subject)
            ),
        },
    }
}

/// Whether a field of `module` holds a `number` integer, so the module
/// needs [`INTEGER_HELPER`]. Pure.
fn uses_integer_helper(module: &ManifestModule) -> bool {
    module.types.iter().any(|manifest_type| {
        let holder = manifest_type_name(manifest_type);
        type_fields(manifest_type).iter().any(|field| {
            let subject = field_subject(holder, field);
            let (_container, item) = field_parts(field_value_type(field), &subject);
            item_wire(module, item, &subject) == ItemWire::Converted(Conversion::SafeInteger)
        })
    })
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

/// The wire `Map` of one variant: the tag, then the variant's own fields.
/// Pure.
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
            no_typescript_form(&format!("a variant that holds the record {record}"), name)
        }
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
    #[should_panic(expected = "Root.ages is a map: the manifest renderer has no TypeScript form")]
    fn a_map_field_has_no_typescript_form_here() {
        let _refused = field_type_text(
            "Root",
            &field("ages", PythonValue::Map(PythonItem::Int(PRIORITY))),
        );
    }

    #[test]
    #[should_panic(
        expected = "Root.note is an optional value: the manifest renderer has no TypeScript form"
    )]
    fn an_optional_field_has_no_typescript_form_here() {
        let _refused = field_type_text(
            "Root",
            &field("note", PythonValue::Optional(PythonItem::Str)),
        );
    }

    #[test]
    #[should_panic(
        expected = "Root.at is a time of day: the manifest renderer has no TypeScript form"
    )]
    fn a_time_of_day_has_no_typescript_form_here() {
        let _refused = field_type_text("Root", &field("at", PythonValue::TimeOfDay));
    }

    #[test]
    #[should_panic(
        expected = "PatternDaily is a variant that holds the record Daily: the manifest renderer \
                    has no TypeScript form"
    )]
    fn a_variant_that_holds_a_record_has_no_typescript_form_here() {
        let variant = TaggedVariant {
            python_class: "PatternDaily",
            tag_value: "daily",
            doc: "",
            fields: VariantFields::OfRecord("Daily"),
        };
        let _refused = variant_interface("type", &variant);
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
    fn every_bound_of_the_producer_and_scheduler_manifests_is_a_number() {
        for module in [PRODUCER_MANIFEST, SCHEDULER_MANIFEST] {
            for bound in module.int_bounds() {
                assert_eq!(integer_type(bound), IntegerType::Number, "{}", bound.name);
            }
        }
    }

    #[test]
    fn the_files_are_the_producer_and_scheduler_modules() {
        let files = generated_files();
        let paths: Vec<&Path> = files.iter().map(GeneratedFile::relative_path).collect();
        assert_eq!(
            paths,
            [
                Path::new("src/_catalog/manifest.ts"),
                Path::new("src/_catalog/scheduler_manifest.ts"),
            ]
        );
        assert_eq!(files[0].text(), render_manifest_module(&PRODUCER_MANIFEST));
        assert_eq!(files[1].text(), render_manifest_module(&SCHEDULER_MANIFEST));
    }
}
