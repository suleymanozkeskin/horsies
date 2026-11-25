//! Render the error classes of horsies-core as Python modules under
//! `_catalog/`: per module of `ERROR_CATALOGS`, the `Literal` subsets of its
//! code fields, the record classes that its fields hold (`ERROR_RECORDS`),
//! one frozen dataclass per class, the union of each catalog, and the
//! per-operation unions of `ERROR_UNIONS`, in that order. Also
//! `_catalog/error_detail.py` with `NoCause`, the field type that several
//! catalogs share, one module per `NewType` of `TEXT_NEW_TYPES`, and one
//! module per module of `EXCEPTION_CLASSES` with its `Exception`
//! subclasses.

use std::collections::BTreeMap;

use horsies_core::catalog::CodeCatalog;
use horsies_core::error_catalog::{
    code_subsets, CodeSubset, ErrorCatalog, ErrorClass, ErrorSlot, ErrorUnion, ExceptionClass,
    ExceptionType, FieldDefault, RecordClass, RecordField, RecordFieldType, SlotRepr, SlotType,
    TextNewType, DEFAULT_RETENTION_CLASS, ERROR_CATALOGS, ERROR_RECORDS, ERROR_UNIONS,
    EXCEPTION_CLASSES, RETENTION_CLASS_KEY,
};

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};

/// The module of `NoCause`, inside `_catalog`.
pub const ERROR_DETAIL_MODULE: &str = "error_detail";

/// The doc of `NoCause` (Python `horsies.core.types.error_detail`).
const NO_CAUSE_DOC: &str = "No Python exception caused this failure: a check or a comparison found it.\n\nIt has no fields, so every instance is equal and ``case NoCause():``\nmatches it. Match on the cause field before a ``raise ... from``, an\n``exc_info=`` or an error tracker call.";

/// The doc of the `error_detail` module.
const ERROR_DETAIL_MODULE_DOC: &str = "Typed values that several error catalogs share.\n\nEach value is a plain frozen type. The type lives here so that no error\ncatalog depends on another one.";

/// The catalog module of the code enum `TaskStatus`.
const STATUS_MODULE: &str = "status";

/// What one error module holds.
pub struct ErrorModule {
    pub catalogs: Vec<ErrorCatalog>,
    pub subsets: Vec<CodeSubset>,
    pub records: Vec<RecordClass>,
    pub unions: Vec<ErrorUnion>,
}

/// The error modules, in name order, from the core's records.
pub fn error_modules() -> BTreeMap<&'static str, ErrorModule> {
    let mut modules: BTreeMap<&'static str, ErrorModule> = BTreeMap::new();
    for catalog in ERROR_CATALOGS {
        module_entry(&mut modules, catalog.python_module)
            .catalogs
            .push(*catalog);
    }
    for subset in code_subsets() {
        module_entry(&mut modules, subset.python_module)
            .subsets
            .push(subset);
    }
    for record in ERROR_RECORDS {
        module_entry(&mut modules, record.python_module)
            .records
            .push(*record);
    }
    for union in ERROR_UNIONS {
        module_entry(&mut modules, union.python_module)
            .unions
            .push(*union);
    }
    modules
}

fn module_entry<'a>(
    modules: &'a mut BTreeMap<&'static str, ErrorModule>,
    module: &'static str,
) -> &'a mut ErrorModule {
    modules.entry(module).or_insert_with(|| ErrorModule {
        catalogs: Vec::new(),
        subsets: Vec::new(),
        records: Vec::new(),
        unions: Vec::new(),
    })
}

/// The exception-class modules, in name order, each with its classes in
/// the order of `EXCEPTION_CLASSES`.
pub fn exception_modules() -> BTreeMap<&'static str, Vec<ExceptionClass>> {
    let mut modules: BTreeMap<&'static str, Vec<ExceptionClass>> = BTreeMap::new();
    for class in EXCEPTION_CLASSES {
        modules.entry(class.python_module).or_default().push(*class);
    }
    modules
}

/// The text of one exception-class module: an `Exception` subclass per
/// class, whose body is its doc. It imports nothing.
pub fn render_exception_module(module: &str, classes: &[ExceptionClass]) -> String {
    let names: Vec<&str> = classes.iter().map(|class| class.python_class).collect();
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str(&format!(
        "\"\"\"Exception classes of the `{module}` area: {}.\"\"\"\n",
        names.join(", ")
    ));
    let exported: Vec<String> = names.iter().copied().map(string_literal).collect();
    text.push_str(&format!("\n__all__ = [{}]\n", exported.join(", ")));
    for class in classes {
        text.push_str("\n\n");
        text.push_str(&render_exception_class(class));
    }
    text
}

/// One `Exception` subclass. A class without a doc has `pass` as its body.
fn render_exception_class(class: &ExceptionClass) -> String {
    let mut text = format!("class {}(Exception):\n", class.python_class);
    match docstring(class.doc, "    ") {
        Some(doc) => text.push_str(&doc),
        None => text.push_str("    pass\n"),
    }
    text
}

/// The text of `_catalog/error_detail.py`.
pub fn render_error_detail_module() -> String {
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str(&docstring(ERROR_DETAIL_MODULE_DOC, "").unwrap_or_default());
    text.push_str("\nfrom __future__ import annotations\n\nfrom dataclasses import dataclass\n\n");
    text.push_str("__all__ = ['NoCause']\n\n\n");
    text.push_str("@dataclass(slots=True, frozen=True)\nclass NoCause:\n");
    text.push_str(&docstring(NO_CAUSE_DOC, "    ").unwrap_or_default());
    text
}

/// The text of one error module.
pub fn render_error_module(module: &str, contents: &ErrorModule) -> String {
    let classes: Vec<&ErrorClass> = contents
        .catalogs
        .iter()
        .flat_map(|catalog| catalog.classes.iter())
        .collect();
    let mut text = format!("{GENERATED_HEADER}\n");
    let union_names: Vec<&str> = contents
        .catalogs
        .iter()
        .map(|catalog| catalog.union)
        .collect();
    text.push_str(&format!(
        "\"\"\"Error classes of the `{module}` area: {}.\"\"\"\n\n",
        union_names.join(", ")
    ));
    text.push_str(&render_imports(contents, &classes));
    let mut exported: Vec<&str> = contents.subsets.iter().map(|subset| subset.name).collect();
    exported.extend(contents.records.iter().map(|record| record.python_class));
    exported.extend(classes.iter().map(|class| class.python_class));
    exported.extend(union_names.iter().copied());
    exported.extend(contents.unions.iter().map(|union| union.name));
    let exported: Vec<String> = exported.into_iter().map(string_literal).collect();
    text.push_str(&format!("\n__all__ = [{}]\n", exported.join(", ")));
    for subset in &contents.subsets {
        text.push_str("\n\n");
        text.push_str(&render_subset(subset));
    }
    for record in &contents.records {
        text.push_str("\n\n");
        text.push_str(&render_record(record));
    }
    for catalog in &contents.catalogs {
        for class in catalog.classes {
            text.push_str("\n\n");
            text.push_str(&render_class(class, &catalog.code_catalog));
        }
    }
    for catalog in &contents.catalogs {
        text.push_str("\n\n");
        let members: Vec<&str> = catalog
            .classes
            .iter()
            .map(|class| class.python_class)
            .collect();
        text.push_str(&render_union(catalog.union, catalog.union_doc, &members));
    }
    for union in &contents.unions {
        text.push_str("\n\n");
        text.push_str(&render_union(union.name, union.doc, union.members));
    }
    text
}

/// The imports of a module: the standard library, then the catalog modules
/// of the code enums and the field types. A catalog module imports nothing
/// from the package outside `_catalog`.
fn render_imports(contents: &ErrorModule, classes: &[&ErrorClass]) -> String {
    let slots: Vec<&ErrorSlot> = classes
        .iter()
        .flat_map(|class| class.slots.iter())
        .collect();
    let uses_uuid = slots.iter().any(|slot| {
        matches!(
            slot,
            ErrorSlot::Field {
                python_type: SlotType::Uuid,
                ..
            }
        )
    });
    let uses_status = slots.iter().any(|slot| {
        matches!(
            slot,
            ErrorSlot::Field {
                python_type: SlotType::TaskStatus,
                ..
            }
        )
    });
    let uses_no_cause = slots.iter().any(|slot| {
        matches!(
            slot,
            ErrorSlot::Exception {
                python_type: ExceptionType::ExceptionOrNoCause
            }
        )
    });
    let record_fields: Vec<&RecordField> = contents
        .records
        .iter()
        .flat_map(|record| record.fields.iter())
        .collect();
    let uses_datetime = record_fields.iter().any(|field| {
        matches!(
            field.python_type,
            RecordFieldType::DateTime | RecordFieldType::OptionalDateTime
        )
    });
    let uses_retention_key = record_fields.iter().any(|field| {
        matches!(
            field.python_type,
            RecordFieldType::OptionalRetentionClassKey
        )
    });
    let uses_default_retention_key = record_fields
        .iter()
        .any(|field| field.default == FieldDefault::DefaultRetentionClassKey);
    let mut text = String::from("from __future__ import annotations\n\n");
    if uses_uuid {
        text.push_str("import uuid\n");
    }
    text.push_str("from dataclasses import dataclass, field\n");
    if uses_datetime {
        text.push_str("from datetime import datetime\n");
    }
    match contents.subsets.is_empty() {
        true => text.push_str("from typing import ClassVar\n"),
        false => text.push_str("from typing import ClassVar, Literal\n"),
    }
    let mut catalog_imports: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for catalog in &contents.catalogs {
        add_import(
            &mut catalog_imports,
            catalog.code_catalog.python_module,
            catalog.code_catalog.python_class,
        );
    }
    if uses_status {
        add_import(&mut catalog_imports, STATUS_MODULE, "TaskStatus");
    }
    if uses_no_cause {
        add_import(&mut catalog_imports, ERROR_DETAIL_MODULE, "NoCause");
    }
    if uses_retention_key {
        add_import(
            &mut catalog_imports,
            RETENTION_CLASS_KEY.python_module,
            RETENTION_CLASS_KEY.name,
        );
    }
    if uses_default_retention_key {
        add_import(
            &mut catalog_imports,
            RETENTION_CLASS_KEY.python_module,
            DEFAULT_RETENTION_CLASS.name,
        );
    }
    text.push('\n');
    for (module, names) in &catalog_imports {
        text.push_str(&format!(
            "from horsies._catalog.{module} import {}\n",
            names.join(", ")
        ));
    }
    text
}

/// Add `name` of `module` to the imports, once, names in order.
fn add_import<'a>(imports: &mut BTreeMap<&'a str, Vec<&'a str>>, module: &'a str, name: &'a str) {
    let names = imports.entry(module).or_default();
    if !names.contains(&name) {
        names.push(name);
        names.sort_unstable();
    }
}

/// A `Literal` subset of a code catalog, with its doc after it.
fn render_subset(subset: &CodeSubset) -> String {
    let mut text = format!("type {} = Literal[\n", subset.name);
    for member in &subset.members {
        text.push_str(&format!(
            "    {}.{member},\n",
            subset.code_catalog.python_class
        ));
    }
    text.push_str("]\n");
    text.push_str(&docstring(subset.doc, "").unwrap_or_default());
    text
}

/// One frozen dataclass.
fn render_class(class: &ErrorClass, code: &CodeCatalog) -> String {
    let mut text = format!(
        "@dataclass(slots=True, frozen=True)\nclass {}:\n",
        class.python_class
    );
    if let Some(doc) = docstring(class.doc, "    ") {
        text.push_str(&doc);
        text.push('\n');
    }
    for slot in class.slots {
        text.push_str("    ");
        text.push_str(&render_slot(slot, code));
        text.push('\n');
    }
    text
}

/// The line of one slot, without its indent.
fn render_slot(slot: &ErrorSlot, code: &CodeCatalog) -> String {
    match slot {
        ErrorSlot::CodeMember { member } => format!(
            "code: ClassVar[{class}] = {class}.{member}",
            class = code.python_class
        ),
        ErrorSlot::CodeSubset { subset } => format!("code: {subset}"),
        ErrorSlot::NeverRetryable => "retryable: ClassVar[bool] = False".to_owned(),
        ErrorSlot::Field {
            name,
            python_type,
            repr,
        } => match repr {
            SlotRepr::Shown => format!("{name}: {}", annotation(*python_type)),
            SlotRepr::Hidden => format!("{name}: {} = field(repr=False)", annotation(*python_type)),
        },
        ErrorSlot::Exception { python_type } => match python_type {
            ExceptionType::Exception => "exception: BaseException = field(repr=False)".to_owned(),
            ExceptionType::ExceptionOrNoCause => {
                "exception: BaseException | NoCause = field(repr=False)".to_owned()
            }
        },
    }
}

/// The annotation text of a field type.
fn annotation(python_type: SlotType) -> &'static str {
    match python_type {
        SlotType::Str | SlotType::UuidText => "str",
        SlotType::Bool => "bool",
        SlotType::Int => "int",
        SlotType::Uuid => "uuid.UUID",
        SlotType::TaskStatus => "TaskStatus",
        SlotType::TaskSendPayload => "TaskSendPayload",
    }
}

/// One frozen record class: its doc, then each field with the comment
/// lines of its note, its annotation and its default.
fn render_record(record: &RecordClass) -> String {
    let mut text = format!(
        "@dataclass(slots=True, frozen=True)\nclass {}:\n",
        record.python_class
    );
    if let Some(doc) = docstring(record.doc, "    ") {
        text.push_str(&doc);
        text.push('\n');
    }
    for field in record.fields {
        for line in field.note.lines().filter(|line| !line.trim().is_empty()) {
            text.push_str(&format!("    # {}\n", line.trim()));
        }
        let annotation = record_annotation(field.python_type);
        match field.default {
            FieldDefault::Required => {
                text.push_str(&format!("    {}: {annotation}\n", field.name));
            }
            FieldDefault::NoneValue => {
                text.push_str(&format!("    {}: {annotation} = None\n", field.name));
            }
            FieldDefault::DefaultRetentionClassKey => text.push_str(&format!(
                "    {}: {annotation} = {}\n",
                field.name, DEFAULT_RETENTION_CLASS.name
            )),
        }
    }
    text
}

/// The annotation text of a record field type.
fn record_annotation(python_type: RecordFieldType) -> &'static str {
    match python_type {
        RecordFieldType::Str => "str",
        RecordFieldType::Int => "int",
        RecordFieldType::DateTime => "datetime",
        RecordFieldType::OptionalStr => "str | None",
        RecordFieldType::OptionalInt => "int | None",
        RecordFieldType::OptionalDateTime => "datetime | None",
        RecordFieldType::OptionalRetentionClassKey => "RetentionClassKey | None",
    }
}

/// The text of the module of one `NewType` of `str`: the type, then each
/// of its constants, each with its doc.
pub fn render_new_type_module(new_type: &TextNewType) -> String {
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str(&format!(
        "\"\"\"The `{}` type of horsies-core and its named values.\"\"\"\n",
        new_type.name
    ));
    text.push_str("\nfrom __future__ import annotations\n\nfrom typing import Final, NewType\n\n");
    let mut exported = vec![string_literal(new_type.name)];
    exported.extend(
        new_type
            .constants
            .iter()
            .map(|constant| string_literal(constant.name)),
    );
    text.push_str(&format!("__all__ = [{}]\n\n\n", exported.join(", ")));
    text.push_str(&format!(
        "{name} = NewType({}, str)\n",
        string_literal(new_type.name),
        name = new_type.name
    ));
    text.push_str(&docstring(new_type.doc, "").unwrap_or_default());
    for constant in new_type.constants {
        text.push_str(&format!(
            "\n{}: Final = {}({})\n",
            constant.name,
            new_type.name,
            string_literal(constant.value)
        ));
        text.push_str(&docstring(constant.doc, "").unwrap_or_default());
    }
    text
}

/// A union alias of `members`, with its doc after it.
fn render_union(name: &str, doc: &str, members: &[&str]) -> String {
    let mut text = match members {
        [single] => format!("type {name} = {single}\n"),
        _ => {
            let mut text = format!("type {name} = (\n");
            for (position, member) in members.iter().enumerate() {
                match position {
                    0 => text.push_str(&format!("    {member}\n")),
                    _ => text.push_str(&format!("    | {member}\n")),
                }
            }
            text.push_str(")\n");
            text
        }
    };
    text.push_str(&docstring(doc, "").unwrap_or_default());
    text
}

#[cfg(test)]
mod tests {
    use horsies_core::catalog::{CallForms, CatalogMember, PythonEnumBase, TerminalStates};

    use super::*;

    const CODES: CodeCatalog = CodeCatalog {
        python_module: "broker",
        python_class: "BrokerErrorCode",
        python_base: PythonEnumBase::StrEnum,
        doc: "",
        members: &[CatalogMember {
            name: "PAYLOAD_MISMATCH",
            value: "PAYLOAD_MISMATCH",
            doc: "",
            forms: CallForms::AnyForm,
        }],
        terminal_states: TerminalStates::NotALifecycle,
    };

    const MISMATCH: ErrorClass = ErrorClass {
        python_class: "BrokerPayloadMismatch",
        doc: "Nothing was written.",
        slots: &[
            ErrorSlot::CodeMember {
                member: "PAYLOAD_MISMATCH",
            },
            ErrorSlot::NeverRetryable,
            ErrorSlot::Field {
                name: "message",
                python_type: SlotType::Str,
                repr: SlotRepr::Hidden,
            },
            ErrorSlot::Field {
                name: "owner_task_id",
                python_type: SlotType::Uuid,
                repr: SlotRepr::Shown,
            },
            ErrorSlot::Exception {
                python_type: ExceptionType::ExceptionOrNoCause,
            },
        ],
    };

    #[test]
    fn a_class_renders_its_doc_and_its_slots_in_order() {
        assert_eq!(
            render_class(&MISMATCH, &CODES),
            "@dataclass(slots=True, frozen=True)\nclass BrokerPayloadMismatch:\n    \"\"\"Nothing was written.\"\"\"\n\n    code: ClassVar[BrokerErrorCode] = BrokerErrorCode.PAYLOAD_MISMATCH\n    retryable: ClassVar[bool] = False\n    message: str = field(repr=False)\n    owner_task_id: uuid.UUID\n    exception: BaseException | NoCause = field(repr=False)\n"
        );
    }

    #[test]
    fn a_union_of_several_classes_spans_lines_and_one_class_does_not() {
        assert_eq!(
            render_union("AnyFailure", "Every failure.", &["A", "B"]),
            "type AnyFailure = (\n    A\n    | B\n)\n\"\"\"Every failure.\"\"\"\n"
        );
        assert_eq!(
            render_union("OneFailure", "One failure.", &["A"]),
            "type OneFailure = A\n\"\"\"One failure.\"\"\"\n"
        );
    }

    #[test]
    fn a_subset_lists_its_members_on_the_code_class() {
        let subset = CodeSubset {
            python_module: "broker_errors",
            name: "BrokerOperationFailedCode",
            doc: "The operation codes.",
            code_catalog: CODES,
            members: vec!["SCHEMA_INIT_FAILED", "CLOSE_FAILED"],
        };
        assert_eq!(
            render_subset(&subset),
            "type BrokerOperationFailedCode = Literal[\n    BrokerErrorCode.SCHEMA_INIT_FAILED,\n    BrokerErrorCode.CLOSE_FAILED,\n]\n\"\"\"The operation codes.\"\"\"\n"
        );
    }

    #[test]
    fn a_module_imports_only_from_the_catalog_and_the_standard_library() {
        for (module, contents) in error_modules() {
            let text = render_error_module(module, &contents);
            for line in text.lines().filter(|line| line.starts_with("from horsies")) {
                assert!(
                    line.starts_with("from horsies._catalog."),
                    "{module}: {line}"
                );
            }
            assert!(text.starts_with(GENERATED_HEADER), "{module}");
        }
    }

    #[test]
    fn the_broker_module_exports_the_subset_the_classes_and_the_unions() {
        let modules = error_modules();
        let broker = &modules["broker_errors"];
        let text = render_error_module("broker_errors", broker);
        assert!(text.contains("__all__ = ['BrokerOperationFailedCode', 'BrokerOperationFailed', "));
        assert!(text.contains("'BrokerCoreError', 'BrokerCoreEnqueueError']\n"));
        assert!(text.contains("from horsies._catalog.broker import BrokerErrorCode\n"));
        assert!(text.contains("from horsies._catalog.error_detail import NoCause\n"));
        assert!(text.contains("\n    code: BrokerOperationFailedCode\n"));
    }

    #[test]
    fn the_send_module_defines_the_payload_record_before_the_classes() {
        let modules = error_modules();
        let send = &modules["task_send_errors"];
        let text = render_error_module("task_send_errors", send);
        assert!(text.contains("__all__ = ['TaskSendPayload', 'TaskSendSuppressed', "));
        assert!(text.contains("'TaskSendIdempotencyKeyConflict', 'TaskSendCoreError']\n"));
        assert!(text.contains("import uuid\nfrom dataclasses import dataclass, field\nfrom datetime import datetime\nfrom typing import ClassVar\n"));
        assert!(text.contains(
            "from horsies._catalog.retention_class import DEFAULT_RETENTION_CLASS_KEY, RetentionClassKey\n"
        ));
        assert!(text.contains("from horsies._catalog.task_send import TaskSendErrorCode\n"));
        let record = text
            .find("class TaskSendPayload:")
            .expect("the record is rendered");
        let first_class = text
            .find("class TaskSendSuppressed:")
            .expect("the classes are rendered");
        assert!(record < first_class);
        assert!(text.contains("    payload: TaskSendPayload = field(repr=False)\n"));
        assert!(text.contains("    size_bytes: int\n"));
    }

    #[test]
    fn a_record_renders_its_notes_its_annotations_and_its_defaults() {
        let text = render_record(&horsies_core::error_catalog::TASK_SEND_PAYLOAD);
        assert!(text.starts_with(
            "@dataclass(slots=True, frozen=True)\nclass TaskSendPayload:\n    \"\"\"Serialized envelope for idempotent retry.\n"
        ));
        assert!(text.contains(
            "    task_options: str | None\n    # The caller's idempotency key, distinct from task identity. Riding the\n"
        ));
        assert!(text.contains("    good_until: datetime | None\n"));
        assert!(text.contains("    idempotency_key: str | None = None\n"));
        assert!(text.ends_with(
            "    retention_class_key: RetentionClassKey | None = DEFAULT_RETENTION_CLASS_KEY\n"
        ));
    }

    #[test]
    fn the_retention_class_key_module_holds_the_type_and_the_default_key() {
        let text = render_new_type_module(&RETENTION_CLASS_KEY);
        assert!(text.starts_with(GENERATED_HEADER));
        assert!(text.contains("__all__ = ['RetentionClassKey', 'DEFAULT_RETENTION_CLASS_KEY']\n"));
        assert!(text.contains("\nRetentionClassKey = NewType('RetentionClassKey', str)\n"));
        assert!(text.contains(
            "\nDEFAULT_RETENTION_CLASS_KEY: Final = RetentionClassKey('standard_30d')\n"
        ));
    }

    #[test]
    fn no_cause_is_a_frozen_class_without_fields() {
        let text = render_error_detail_module();
        assert!(text.starts_with(GENERATED_HEADER));
        assert!(text.contains("__all__ = ['NoCause']\n"));
        assert!(text.ends_with(" tracker call.\n    \"\"\"\n"), "{text}");
    }

    /// `OutcomeDecodeError` keeps its name, its base and Python's doc
    /// (`horsies.core.lifecycle.outcomes`), and its module imports nothing.
    #[test]
    fn an_exception_class_is_an_exception_subclass_with_its_doc() {
        let modules = exception_modules();
        let classes = &modules["lifecycle_errors"];
        assert_eq!(
            render_exception_module("lifecycle_errors", classes),
            format!(
                "{GENERATED_HEADER}\n\
                 \"\"\"Exception classes of the `lifecycle_errors` area: OutcomeDecodeError.\"\"\"\n\
                 \n__all__ = ['OutcomeDecodeError']\n\
                 \n\nclass OutcomeDecodeError(Exception):\n\
                 \x20   \"\"\"The returned row does not satisfy the wire contract.\n\
                 \n\
                 \x20   Never raised for a task outcome — only when the database returned\n\
                 \x20   something this code cannot interpret, which means one side is running\n\
                 \x20   against a contract the other does not implement.\n\
                 \x20   \"\"\"\n"
            )
        );
    }

    #[test]
    fn a_class_without_a_doc_has_pass_as_its_body() {
        let class = ExceptionClass {
            python_module: "probe_errors",
            python_class: "ProbeError",
            doc: "",
        };
        assert_eq!(
            render_exception_class(&class),
            "class ProbeError(Exception):\n    pass\n"
        );
    }

    /// An exception-class module never shares its file with an error module.
    #[test]
    fn exception_modules_and_error_modules_have_different_names() {
        let error_module_names: Vec<&str> = error_modules().into_keys().collect();
        for module in exception_modules().keys() {
            assert!(!error_module_names.contains(module), "{module}");
            assert_ne!(*module, ERROR_DETAIL_MODULE);
        }
    }
}
