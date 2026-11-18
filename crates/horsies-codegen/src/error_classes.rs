//! Render the error classes of horsies-core as Python modules under
//! `_catalog/`: per module of `ERROR_CATALOGS`, the `Literal` subsets of its
//! code fields, one frozen dataclass per class, the union of each catalog,
//! and the per-operation unions of `ERROR_UNIONS`, in that order. Also
//! `_catalog/error_detail.py` with `NoCause`, the field type that several
//! catalogs share.

use std::collections::BTreeMap;

use horsies_core::catalog::CodeCatalog;
use horsies_core::error_catalog::{
    code_subsets, CodeSubset, ErrorCatalog, ErrorClass, ErrorSlot, ErrorUnion, ExceptionType,
    SlotRepr, SlotType, ERROR_CATALOGS, ERROR_UNIONS,
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
        unions: Vec::new(),
    })
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
    exported.extend(classes.iter().map(|class| class.python_class));
    exported.extend(union_names.iter().copied());
    exported.extend(contents.unions.iter().map(|union| union.name));
    let exported: Vec<String> = exported.into_iter().map(string_literal).collect();
    text.push_str(&format!("\n__all__ = [{}]\n", exported.join(", ")));
    for subset in &contents.subsets {
        text.push_str("\n\n");
        text.push_str(&render_subset(subset));
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
    let mut text = String::from("from __future__ import annotations\n\n");
    if uses_uuid {
        text.push_str("import uuid\n");
    }
    text.push_str("from dataclasses import dataclass, field\n");
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
        SlotType::Uuid => "uuid.UUID",
        SlotType::TaskStatus => "TaskStatus",
    }
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
    use horsies_core::catalog::{CatalogMember, PythonEnumBase, TerminalStates};

    use super::*;

    const CODES: CodeCatalog = CodeCatalog {
        python_module: "broker",
        python_class: "BrokerErrorCode",
        python_base: PythonEnumBase::StrEnum,
        doc: "",
        members: &[CatalogMember {
            value: "PAYLOAD_MISMATCH",
            doc: "",
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
    fn no_cause_is_a_frozen_class_without_fields() {
        let text = render_error_detail_module();
        assert!(text.starts_with(GENERATED_HEADER));
        assert!(text.contains("__all__ = ['NoCause']\n"));
        assert!(text.ends_with(" tracker call.\n    \"\"\"\n"), "{text}");
    }
}
