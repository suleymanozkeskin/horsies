//! The error catalogs of horsies-core as TypeScript modules: one module per
//! module of `ERROR_CATALOGS`, in name order. A module holds, in order:
//!
//! 1. the type imports;
//! 2. the code subsets, the records that fields hold (`ERROR_RECORDS`), one
//!    readonly interface per class, the union of each catalog and each union
//!    of `ERROR_UNIONS`;
//! 3. the native forms that the bridge gives: per record and per class an
//!    interface with the model's slot names, and per catalog their union;
//! 4. the converters: per record and per catalog one function from the
//!    native form to the frozen public value.
//!
//! A class whose code only a blocking call form gives
//! (`CallForms::BlockingFormOnly`, read through `CodeCatalog::member_named`)
//! is left out of every interface, union and converter: every call of the
//! TypeScript client is async, so no call gives it.
//!
//! A field with `SlotRepr::Hidden` and the `exception` field are
//! non-enumerable properties of the value: `JSON.stringify`, `console.log`
//! and `structuredClone` do not show them, as the Python client's `repr`
//! does not show them.

use std::collections::{BTreeMap, BTreeSet};

use horsies_core::catalog::{CallForms, CatalogMember, CodeCatalog, CodeEnum};
use horsies_core::error_catalog::{
    code_subsets, CodeSubset, ErrorCatalog, ErrorClass, ErrorUnion, RecordClass, RecordField,
    RecordFieldType, SlotRepr, SlotType, ERROR_CATALOGS, ERROR_RECORDS, ERROR_UNIONS,
    RETENTION_CLASS_KEY, TASK_SEND_PAYLOAD,
};
use horsies_core::TaskStatus;

use crate::catalogs::{catalog_type_reference, property_access};
use crate::error_detail::{
    interface_text, InterfaceMember, CORE_CAUSE_TYPE, ERROR_DETAIL_MODULE, NO_CAUSE_TYPE,
};
use crate::model::{
    class_slot, class_slots, code_subset_module, error_catalog_module, error_class_type_name,
    error_union_module, record_field_type, record_module, record_type_name, CauseSlot, ClassSlot,
};
use crate::output::{catalog_file, GeneratedFile};
use crate::retention_class::new_type_reference;
use crate::ts_text::{
    bigint_literal, camel_case_of_pascal, camel_case_of_snake, import_types, module_text,
    property_key, string_literal, union_alias,
};

/// The member of the code of a class, and the instance slot of a code
/// subset.
const CODE_SLOT: &str = "code";

/// The member of the retry rule of a class that never clears.
const RETRYABLE_SLOT: &str = "retryable";

/// The member and the instance slot of the cause.
const EXCEPTION_SLOT: &str = "exception";

/// The member of a native value that names its class.
const NATIVE_CLASS_MEMBER: &str = "class";

/// The member of a native value that holds its instance slots.
const NATIVE_SLOTS_MEMBER: &str = "slots";

/// The prefix of the name of a native form.
const NATIVE_PREFIX: &str = "Native";

/// The suffix of the name of a converter.
const CONVERTER_SUFFIX: &str = "FromNative";

/// The parameter of a converter.
const RAW_PARAMETER: &str = "raw";

/// The module function that throws on a class name that the build does not
/// know.
const UNKNOWN_CLASS_FUNCTION: &str = "unknownClass";

/// The nanoseconds of one microsecond. The bridge gives an instant as a
/// `bigint` of epoch microseconds, the stored precision.
const NANOSECONDS_PER_MICROSECOND: i128 = 1_000;

/// The module constant of [`NANOSECONDS_PER_MICROSECOND`].
const NANOSECONDS_PER_MICROSECOND_CONSTANT: &str = "NANOSECONDS_PER_MICROSECOND";

/// The module function from epoch microseconds to an instant.
const INSTANT_FUNCTION: &str = "instantOfMicroseconds";

/// The module function from epoch microseconds or `null` to an instant or
/// `null`.
const OPTIONAL_INSTANT_FUNCTION: &str = "optionalInstantOfMicroseconds";

const NATIVE_UNION_COMMENT: &str =
    "/** The value that the bridge gives: the class name and the instance slots by core name. */";

const CONVERTER_COMMENT: &str =
    "/** The frozen value of `raw`. A class name that this build does not know throws. */";

/// The error modules, in name order. Pure.
pub fn generated_files() -> Vec<GeneratedFile> {
    error_modules(ERROR_CATALOGS, code_subsets(), ERROR_RECORDS, ERROR_UNIONS)
        .iter()
        .map(|(module, contents)| catalog_file(module, module_text(&module_sections(contents))))
        .collect()
}

/// What one error module holds, each list in the model's order.
#[derive(Debug, Default)]
struct ErrorModule {
    catalogs: Vec<ErrorCatalog>,
    subsets: Vec<CodeSubset>,
    records: Vec<RecordClass>,
    unions: Vec<ErrorUnion>,
}

/// The catalogs, subsets, records and unions grouped by module, modules in
/// name order. Pure.
fn error_modules(
    catalogs: &[ErrorCatalog],
    subsets: Vec<CodeSubset>,
    records: &[RecordClass],
    unions: &[ErrorUnion],
) -> BTreeMap<&'static str, ErrorModule> {
    let mut modules: BTreeMap<&'static str, ErrorModule> = BTreeMap::new();
    for catalog in catalogs {
        let module = modules.entry(error_catalog_module(catalog)).or_default();
        module.catalogs.push(*catalog);
    }
    for subset in subsets {
        let module = modules.entry(code_subset_module(&subset)).or_default();
        module.subsets.push(subset);
    }
    for record in records {
        let module = modules.entry(record_module(record)).or_default();
        module.records.push(*record);
    }
    for union in unions {
        let module = modules.entry(error_union_module(union)).or_default();
        module.unions.push(*union);
    }
    modules
}

/// One catalog and the classes of it that the TypeScript client renders.
struct RenderedCatalog {
    catalog: ErrorCatalog,
    classes: Vec<&'static ErrorClass>,
}

/// The call forms that give `class`: the forms of the member of its code
/// slot, or the one form of every member of its code subset. Pure.
///
/// A class that does not start with its code slot, a code that is not a
/// member of `codes`, a subset that is not in `subsets`, and a subset whose
/// members have different call forms are defects of the model.
fn class_call_forms(class: &ErrorClass, codes: &CodeCatalog, subsets: &[CodeSubset]) -> CallForms {
    let class_name = error_class_type_name(class);
    match class.slots.first().map(class_slot) {
        Some(ClassSlot::CodeMember { member }) => {
            code_member_named(codes, member, class_name).forms
        }
        Some(ClassSlot::CodeSubset { subset }) => {
            subset_call_forms(class_name, subset, codes, subsets)
        }
        Some(ClassSlot::NeverRetryable | ClassSlot::Field { .. } | ClassSlot::Cause { .. })
        | None => panic!("the class {class_name} does not start with its code slot"),
    }
}

/// The one call form of the members of the subset `subset_name`. Pure.
fn subset_call_forms(
    class_name: &str,
    subset_name: &str,
    codes: &CodeCatalog,
    subsets: &[CodeSubset],
) -> CallForms {
    let subset = subsets
        .iter()
        .find(|known| known.name == subset_name)
        .unwrap_or_else(|| {
            panic!("the code subset {subset_name} of {class_name} is not a subset of its module")
        });
    let mut forms: Vec<CallForms> = Vec::new();
    for value in &subset.members {
        let member = codes
            .members
            .iter()
            .find(|member| member.value == *value)
            .unwrap_or_else(|| {
                panic!(
                    "the code {value} of the subset {subset_name} is not a member of its catalog"
                )
            });
        if !forms.contains(&member.forms) {
            forms.push(member.forms);
        }
    }
    match forms.as_slice() {
        [one] => *one,
        [] => panic!("the code subset {subset_name} of {class_name} has no member"),
        [_, _, ..] => panic!("the code subset {subset_name} of {class_name} mixes call forms"),
    }
}

/// The member `member` of `codes`, which the class `class_name` names.
/// Pure. A missing member is a defect of the model.
fn code_member_named(
    codes: &CodeCatalog,
    member: &str,
    class_name: &str,
) -> &'static CatalogMember {
    codes.member_named(member).unwrap_or_else(|| {
        panic!(
            "the code {member} of {class_name} is not a member of {}",
            catalog_type_reference(codes).1
        )
    })
}

/// The classes of `catalog` that any call form gives, in catalog order.
/// Pure.
fn rendered_catalog(catalog: &ErrorCatalog, subsets: &[CodeSubset]) -> RenderedCatalog {
    let classes = catalog
        .classes
        .iter()
        .filter(
            |class| match class_call_forms(class, &catalog.code_catalog, subsets) {
                CallForms::AnyForm => true,
                CallForms::BlockingFormOnly => false,
            },
        )
        .collect();
    RenderedCatalog {
        catalog: *catalog,
        classes,
    }
}

/// The sections of one error module. Pure.
fn module_sections(contents: &ErrorModule) -> Vec<String> {
    let rendered: Vec<RenderedCatalog> = contents
        .catalogs
        .iter()
        .map(|catalog| rendered_catalog(catalog, &contents.subsets))
        .collect();
    let classes: Vec<&ErrorClass> = rendered
        .iter()
        .flat_map(|entry| entry.classes.iter().copied())
        .collect();
    assert_payload_records_in_module(&classes, &contents.records);
    let mut sections: Vec<String> = module_imports(&classes, &contents.records)
        .into_iter()
        .collect();
    sections.extend(type_sections(contents, &rendered));
    sections.extend(native_sections(contents, &rendered));
    sections.extend(function_sections(contents, &rendered));
    sections
}

/// A class that holds the send payload is in the module of the payload
/// record, so the module defines the record. Another module is a defect of
/// the model.
fn assert_payload_records_in_module(classes: &[&ErrorClass], records: &[RecordClass]) {
    for class in classes {
        for slot in class_slots(class) {
            let holds_payload = matches!(
                slot,
                ClassSlot::Field {
                    slot_type: SlotType::TaskSendPayload,
                    ..
                }
            );
            assert!(
                !holds_payload || records.contains(&TASK_SEND_PAYLOAD),
                "the class {} holds {}, which is not a record of its module",
                error_class_type_name(class),
                record_type_name(&TASK_SEND_PAYLOAD)
            );
        }
    }
}

/// The type imports of the module, or `None` when it imports nothing: one
/// line per module, modules and names in name order. Pure.
fn module_imports(classes: &[&ErrorClass], records: &[RecordClass]) -> Option<String> {
    let needed: BTreeSet<(&'static str, &'static str)> = classes
        .iter()
        .flat_map(|class| class_slots(class))
        .flat_map(slot_imports)
        .chain(
            records
                .iter()
                .flat_map(|record| record.fields.iter())
                .filter_map(record_import),
        )
        .collect();
    let mut imports: BTreeMap<&'static str, Vec<&'static str>> = BTreeMap::new();
    for (module, name) in needed {
        imports.entry(module).or_default().push(name);
    }
    let lines: Vec<String> = imports
        .into_iter()
        .map(|(module, names)| import_types(&names, module))
        .collect();
    match lines.as_slice() {
        [] => None,
        [_, ..] => Some(lines.join("\n")),
    }
}

/// The type of another module that one record field names, as
/// `(module, name)`. Pure.
fn record_import(field: &RecordField) -> Option<(&'static str, &'static str)> {
    match record_field_type(field) {
        RecordFieldType::OptionalRetentionClassKey => {
            Some(new_type_reference(&RETENTION_CLASS_KEY))
        }
        RecordFieldType::Str
        | RecordFieldType::Int
        | RecordFieldType::DateTime
        | RecordFieldType::OptionalStr
        | RecordFieldType::OptionalInt
        | RecordFieldType::OptionalDateTime => None,
    }
}

/// The types of other modules that one slot names, as `(module, name)`.
/// Pure.
fn slot_imports(slot: ClassSlot) -> Vec<(&'static str, &'static str)> {
    match slot {
        ClassSlot::Cause { cause_type } => match cause_type {
            CauseSlot::Required => vec![(ERROR_DETAIL_MODULE, CORE_CAUSE_TYPE)],
            CauseSlot::Optional => vec![
                (ERROR_DETAIL_MODULE, CORE_CAUSE_TYPE),
                (ERROR_DETAIL_MODULE, NO_CAUSE_TYPE),
            ],
        },
        ClassSlot::Field { slot_type, .. } => match slot_type {
            SlotType::TaskStatus => vec![task_status_reference()],
            SlotType::Str
            | SlotType::Bool
            | SlotType::Int
            | SlotType::Uuid
            | SlotType::UuidText
            | SlotType::TaskSendPayload => Vec::new(),
        },
        ClassSlot::CodeMember { .. } | ClassSlot::CodeSubset { .. } | ClassSlot::NeverRetryable => {
            Vec::new()
        }
    }
}

/// The module and the type name of the code catalog `TaskStatus`. Pure.
fn task_status_reference() -> (&'static str, &'static str) {
    catalog_type_reference(&<TaskStatus as CodeEnum>::CATALOG)
}

/// The public types: subsets, records, classes, catalog unions, then the
/// unions of `ERROR_UNIONS`. Pure.
fn type_sections(contents: &ErrorModule, rendered: &[RenderedCatalog]) -> Vec<String> {
    let mut sections: Vec<String> = contents.subsets.iter().map(subset_type).collect();
    sections.extend(contents.records.iter().map(record_interface));
    for entry in rendered {
        sections.extend(
            entry
                .classes
                .iter()
                .map(|class| class_interface(class, &entry.catalog.code_catalog)),
        );
    }
    for entry in rendered {
        let members: Vec<String> = entry
            .classes
            .iter()
            .map(|class| error_class_type_name(class).to_owned())
            .collect();
        sections.push(union_alias(entry.catalog.union, &members));
    }
    sections.extend(
        contents
            .unions
            .iter()
            .map(|union| error_union(union, rendered)),
    );
    sections
}

/// The literal union of the wire values of a code subset. Pure.
fn subset_type(subset: &CodeSubset) -> String {
    let members: Vec<String> = subset
        .members
        .iter()
        .map(|value| string_literal(value))
        .collect();
    union_alias(subset.name, &members)
}

/// A union of `ERROR_UNIONS` with its rendered members, in its order. Pure.
///
/// A member that is not a class of the module is a defect of the model.
fn error_union(union: &ErrorUnion, rendered: &[RenderedCatalog]) -> String {
    let mut members: Vec<String> = Vec::new();
    for member in union.members {
        let known = rendered.iter().any(|entry| {
            entry
                .catalog
                .classes
                .iter()
                .any(|class| error_class_type_name(class) == *member)
        });
        assert!(
            known,
            "the union {} names {member}, which is not a class of its module",
            union.name
        );
        let kept = rendered.iter().any(|entry| {
            entry
                .classes
                .iter()
                .any(|class| error_class_type_name(class) == *member)
        });
        if kept {
            members.push((*member).to_owned());
        }
    }
    union_alias(union.name, &members)
}

/// The interface of one class: its slots in model order. Pure.
fn class_interface(class: &ErrorClass, codes: &CodeCatalog) -> String {
    let members: Vec<InterfaceMember> = class_slots(class)
        .into_iter()
        .map(|slot| public_member(class, slot, codes))
        .collect();
    interface_text(error_class_type_name(class), &members)
}

/// The member of one slot in the public interface. Pure.
fn public_member(class: &ErrorClass, slot: ClassSlot, codes: &CodeCatalog) -> InterfaceMember {
    match slot {
        ClassSlot::CodeMember { member } => InterfaceMember::new(
            CODE_SLOT,
            string_literal(code_member_named(codes, member, error_class_type_name(class)).value),
        ),
        ClassSlot::CodeSubset { subset } => InterfaceMember::new(CODE_SLOT, subset),
        ClassSlot::NeverRetryable => InterfaceMember::new(RETRYABLE_SLOT, "false"),
        ClassSlot::Field {
            name, slot_type, ..
        } => InterfaceMember::new(public_name(name), public_field_type(slot_type)),
        ClassSlot::Cause { cause_type } => {
            InterfaceMember::new(EXCEPTION_SLOT, cause_type_text(cause_type))
        }
    }
}

/// The public property key of the slot or field `name`: camelCase. Pure.
fn public_name(name: &str) -> String {
    property_key(&camel_case_of_snake(name))
}

/// The TypeScript type of a field of the public value. Pure.
fn public_field_type(slot_type: SlotType) -> String {
    match slot_type {
        SlotType::Str | SlotType::Uuid | SlotType::UuidText => "string".to_owned(),
        SlotType::Bool => "boolean".to_owned(),
        SlotType::Int => "bigint".to_owned(),
        SlotType::TaskStatus => task_status_reference().1.to_owned(),
        SlotType::TaskSendPayload => record_type_name(&TASK_SEND_PAYLOAD).to_owned(),
    }
}

/// The TypeScript type of a field of the native value: the public type,
/// except that a record is its native form. Pure.
fn native_field_type(slot_type: SlotType) -> String {
    match slot_type {
        SlotType::TaskSendPayload => native_name(record_type_name(&TASK_SEND_PAYLOAD)),
        SlotType::Str
        | SlotType::Bool
        | SlotType::Int
        | SlotType::Uuid
        | SlotType::UuidText
        | SlotType::TaskStatus => public_field_type(slot_type),
    }
}

/// `CoreCause`, or `CoreCause | NoCause`. Pure.
fn cause_type_text(cause_type: CauseSlot) -> String {
    match cause_type {
        CauseSlot::Required => CORE_CAUSE_TYPE.to_owned(),
        CauseSlot::Optional => format!("{CORE_CAUSE_TYPE} | {NO_CAUSE_TYPE}"),
    }
}

/// The public interface of a record: its fields in camelCase. Pure.
fn record_interface(record: &RecordClass) -> String {
    let members: Vec<InterfaceMember> = record
        .fields
        .iter()
        .map(|field| {
            InterfaceMember::new(
                public_name(field.name),
                public_record_type(record_field_type(field)),
            )
        })
        .collect();
    interface_text(record_type_name(record), &members)
}

/// The TypeScript type of a field of a public record. Pure.
fn public_record_type(field_type: RecordFieldType) -> String {
    match field_type {
        RecordFieldType::Str => "string".to_owned(),
        RecordFieldType::Int => "number".to_owned(),
        RecordFieldType::DateTime => "Temporal.Instant".to_owned(),
        RecordFieldType::OptionalStr => "string | null".to_owned(),
        RecordFieldType::OptionalInt => "number | null".to_owned(),
        RecordFieldType::OptionalDateTime => "Temporal.Instant | null".to_owned(),
        RecordFieldType::OptionalRetentionClassKey => {
            format!("{} | null", new_type_reference(&RETENTION_CLASS_KEY).1)
        }
    }
}

/// The TypeScript type of a field of a native record: an instant is a
/// `bigint` of epoch microseconds. Pure.
fn native_record_type(field_type: RecordFieldType) -> String {
    match field_type {
        RecordFieldType::DateTime => "bigint".to_owned(),
        RecordFieldType::OptionalDateTime => "bigint | null".to_owned(),
        RecordFieldType::Str
        | RecordFieldType::Int
        | RecordFieldType::OptionalStr
        | RecordFieldType::OptionalInt
        | RecordFieldType::OptionalRetentionClassKey => public_record_type(field_type),
    }
}

/// `Native<name>`: the name of the native form of a type. Pure.
fn native_name(type_name: &str) -> String {
    format!("{NATIVE_PREFIX}{type_name}")
}

/// `<name in camelCase>FromNative`: the converter to the type. Pure.
fn converter_name(type_name: &str) -> String {
    format!("{}{CONVERTER_SUFFIX}", camel_case_of_pascal(type_name))
}

/// The native forms: records, classes, then one union per catalog. Pure.
fn native_sections(contents: &ErrorModule, rendered: &[RenderedCatalog]) -> Vec<String> {
    let mut sections: Vec<String> = contents
        .records
        .iter()
        .map(native_record_interface)
        .collect();
    for entry in rendered {
        sections.extend(
            entry
                .classes
                .iter()
                .map(|class| native_class_interface(class)),
        );
    }
    for entry in rendered {
        let members: Vec<String> = entry
            .classes
            .iter()
            .map(|class| native_name(error_class_type_name(class)))
            .collect();
        sections.push(format!(
            "{NATIVE_UNION_COMMENT}\n{}",
            union_alias(&native_name(entry.catalog.union), &members)
        ));
    }
    sections
}

/// The native interface of a record: its fields by their core names. Pure.
fn native_record_interface(record: &RecordClass) -> String {
    let members: Vec<InterfaceMember> = record
        .fields
        .iter()
        .map(|field| {
            InterfaceMember::new(
                property_key(field.name),
                native_record_type(record_field_type(field)),
            )
        })
        .collect();
    interface_text(&native_name(record_type_name(record)), &members)
}

/// The native interface of a class: `class`, the class name, and `slots`,
/// its instance slots by their core names. A class without instance slots
/// has no `slots` member. Pure.
fn native_class_interface(class: &ErrorClass) -> String {
    let class_name = error_class_type_name(class);
    let slots: Vec<InterfaceMember> = class_slots(class)
        .into_iter()
        .filter_map(native_slot)
        .collect();
    let mut members = vec![InterfaceMember::new(
        NATIVE_CLASS_MEMBER,
        string_literal(class_name),
    )];
    match slots.as_slice() {
        [] => {}
        [_, ..] => members.push(InterfaceMember::new(
            NATIVE_SLOTS_MEMBER,
            nested_object_type(&slots),
        )),
    }
    interface_text(&native_name(class_name), &members)
}

/// The member of one instance slot of the native value, or `None` for a
/// class value (the code of one member, a fixed `retryable`). Pure.
fn native_slot(slot: ClassSlot) -> Option<InterfaceMember> {
    match slot {
        ClassSlot::CodeMember { .. } | ClassSlot::NeverRetryable => None,
        ClassSlot::CodeSubset { subset } => Some(InterfaceMember::new(CODE_SLOT, subset)),
        ClassSlot::Field {
            name, slot_type, ..
        } => Some(InterfaceMember::new(
            property_key(name),
            native_field_type(slot_type),
        )),
        ClassSlot::Cause { cause_type } => Some(InterfaceMember::new(
            EXCEPTION_SLOT,
            cause_type_text(cause_type),
        )),
    }
}

/// An object type inside an interface member: one readonly member per line,
/// indented one level more than the member. Pure.
fn nested_object_type(members: &[InterfaceMember]) -> String {
    let mut text = String::from("{\n");
    for member in members {
        text.push_str(&format!(
            "    readonly {}: {};\n",
            member.name, member.type_text
        ));
    }
    text.push_str("  }");
    text
}

/// The functions: the instant helpers that the records need, the
/// unknown-class helper, the record converters, then the catalog
/// converters. Pure.
fn function_sections(contents: &ErrorModule, rendered: &[RenderedCatalog]) -> Vec<String> {
    let mut sections = instant_helper_sections(&contents.records);
    match rendered {
        [] => {}
        [_, ..] => sections.push(unknown_class_function()),
    }
    sections.extend(contents.records.iter().map(record_converter));
    sections.extend(
        rendered
            .iter()
            .map(|entry| catalog_converter(&entry.catalog, &entry.classes)),
    );
    sections
}

/// The constant and the functions that convert the instants of `records`:
/// none without an instant field, the optional form only with an optional
/// instant field. Pure.
fn instant_helper_sections(records: &[RecordClass]) -> Vec<String> {
    let field_types: Vec<RecordFieldType> = records
        .iter()
        .flat_map(|record| record.fields.iter().map(record_field_type))
        .collect();
    let optional = field_types.contains(&RecordFieldType::OptionalDateTime);
    let required = field_types.contains(&RecordFieldType::DateTime);
    let mut sections = Vec::new();
    if required || optional {
        sections.push(format!(
            "const {NANOSECONDS_PER_MICROSECOND_CONSTANT} = {};",
            bigint_literal(NANOSECONDS_PER_MICROSECOND)
        ));
        sections.push(format!(
            "function {INSTANT_FUNCTION}(microseconds: bigint): Temporal.Instant {{\n  \
             return Temporal.Instant.fromEpochNanoseconds(microseconds * \
             {NANOSECONDS_PER_MICROSECOND_CONSTANT});\n}}"
        ));
    }
    if optional {
        sections.push(format!(
            "function {OPTIONAL_INSTANT_FUNCTION}(microseconds: bigint | null): \
             Temporal.Instant | null {{\n  \
             if (microseconds === null) {{\n    return null;\n  }}\n\n  \
             return {INSTANT_FUNCTION}(microseconds);\n}}"
        ));
    }
    sections
}

/// The module function that throws on a class name that this build does
/// not know. Pure.
fn unknown_class_function() -> String {
    format!(
        "function {UNKNOWN_CLASS_FUNCTION}(converter: string, {RAW_PARAMETER}: \
         {{ readonly {NATIVE_CLASS_MEMBER}: string }}): never {{\n  \
         throw new Error(`${{converter}}: unknown class name \
         ${{{RAW_PARAMETER}.{NATIVE_CLASS_MEMBER}}}`);\n}}"
    )
}

/// The converter of a record: each field by its camelCase name, an instant
/// from its epoch microseconds; the value is frozen. Pure.
fn record_converter(record: &RecordClass) -> String {
    let name = record_type_name(record);
    let mut text = format!(
        "export function {}({RAW_PARAMETER}: {}): {name} {{\n  const value: {name} = {{\n",
        converter_name(name),
        native_name(name)
    );
    for field in record.fields {
        text.push_str(&format!(
            "    {}: {},\n",
            public_name(field.name),
            record_value(field)
        ));
    }
    text.push_str("  };\n\n  return Object.freeze(value);\n}");
    text
}

/// The expression of one field of a record converter. Pure.
fn record_value(field: &RecordField) -> String {
    let read = property_access(RAW_PARAMETER, field.name);
    match record_field_type(field) {
        RecordFieldType::DateTime => format!("{INSTANT_FUNCTION}({read})"),
        RecordFieldType::OptionalDateTime => format!("{OPTIONAL_INSTANT_FUNCTION}({read})"),
        RecordFieldType::Str
        | RecordFieldType::Int
        | RecordFieldType::OptionalStr
        | RecordFieldType::OptionalInt
        | RecordFieldType::OptionalRetentionClassKey => read,
    }
}

/// The converter of a catalog: one `case` per rendered class, and a
/// `default` arm that throws on a class name that this build does not know.
/// Pure.
fn catalog_converter(catalog: &ErrorCatalog, classes: &[&ErrorClass]) -> String {
    let function = converter_name(catalog.union);
    let mut text = format!(
        "{CONVERTER_COMMENT}\nexport function {function}({RAW_PARAMETER}: {}): {} {{\n  \
         switch ({RAW_PARAMETER}.{NATIVE_CLASS_MEMBER}) {{\n",
        native_name(catalog.union),
        catalog.union
    );
    for class in classes {
        text.push_str(&converter_case(class, &catalog.code_catalog));
        text.push_str("\n\n");
    }
    text.push_str(&format!(
        "    default: {{\n      const unreachable: never = {RAW_PARAMETER};\n\n      \
         return {UNKNOWN_CLASS_FUNCTION}({}, unreachable);\n    }}\n  }}\n}}",
        string_literal(&function)
    ));
    text
}

/// The `case` of one class: the value with its slots in model order, then
/// the frozen value with its hidden slots non-enumerable. Pure.
fn converter_case(class: &ErrorClass, codes: &CodeCatalog) -> String {
    let class_name = error_class_type_name(class);
    let slots = class_slots(class);
    let mut text = format!(
        "    case {}: {{\n      const value: {class_name} = {{\n",
        string_literal(class_name)
    );
    for slot in &slots {
        let (member, value) = slot_value(class, *slot, codes);
        text.push_str(&format!("        {member}: {value},\n"));
    }
    text.push_str("      };\n\n");
    let hidden: Vec<String> = slots
        .iter()
        .filter_map(|slot| hidden_member(*slot))
        .collect();
    text.push_str(&frozen_return(&hidden));
    text.push_str("\n    }");
    text
}

/// The member name and the expression of one slot in a converter. Pure.
fn slot_value(class: &ErrorClass, slot: ClassSlot, codes: &CodeCatalog) -> (String, String) {
    let slots = format!("{RAW_PARAMETER}.{NATIVE_SLOTS_MEMBER}");
    match slot {
        ClassSlot::CodeMember { member } => (
            CODE_SLOT.to_owned(),
            string_literal(code_member_named(codes, member, error_class_type_name(class)).value),
        ),
        ClassSlot::CodeSubset { .. } => (CODE_SLOT.to_owned(), property_access(&slots, CODE_SLOT)),
        ClassSlot::NeverRetryable => (RETRYABLE_SLOT.to_owned(), "false".to_owned()),
        ClassSlot::Field {
            name, slot_type, ..
        } => (public_name(name), field_value(&slots, name, slot_type)),
        ClassSlot::Cause { .. } => (
            EXCEPTION_SLOT.to_owned(),
            format!(
                "Object.freeze({{ ...{} }})",
                property_access(&slots, EXCEPTION_SLOT)
            ),
        ),
    }
}

/// The expression of a field in a converter: the native value, or the
/// converted record. Pure.
fn field_value(slots: &str, name: &str, slot_type: SlotType) -> String {
    let read = property_access(slots, name);
    match slot_type {
        SlotType::TaskSendPayload => format!(
            "{}({read})",
            converter_name(record_type_name(&TASK_SEND_PAYLOAD))
        ),
        SlotType::Str
        | SlotType::Bool
        | SlotType::Int
        | SlotType::Uuid
        | SlotType::UuidText
        | SlotType::TaskStatus => read,
    }
}

/// The public name of a slot that the value hides: a `Hidden` field or the
/// cause. Pure.
fn hidden_member(slot: ClassSlot) -> Option<String> {
    match slot {
        ClassSlot::CodeMember { .. } | ClassSlot::CodeSubset { .. } | ClassSlot::NeverRetryable => {
            None
        }
        ClassSlot::Field { name, display, .. } => match display {
            SlotRepr::Shown => None,
            SlotRepr::Hidden => Some(public_name(name)),
        },
        ClassSlot::Cause { .. } => Some(EXCEPTION_SLOT.to_owned()),
    }
}

/// The `return` of a converter case: the frozen value, with the `hidden`
/// properties made non-enumerable first. Pure.
fn frozen_return(hidden: &[String]) -> String {
    match hidden {
        [] => "      return Object.freeze(value);".to_owned(),
        [_, ..] => {
            let mut text = String::from(
                "      return Object.freeze(\n        Object.defineProperties(value, {\n",
            );
            for name in hidden {
                text.push_str(&format!("          {name}: {{ enumerable: false }},\n"));
            }
            text.push_str("        }),\n      );");
            text
        }
    }
}

#[cfg(test)]
mod tests;
