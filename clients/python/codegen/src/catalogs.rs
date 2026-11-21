//! Render the code catalogs of horsies-core as Python modules under
//! `_catalog/`: one module per `python_module`, one enum class per catalog,
//! in the order of `CODE_CATALOGS`. Each member is `NAME = 'value'` with its
//! doc. A lifecycle status class also gets the `is_terminal` property, and
//! the module gets the `frozenset` constant of its terminal members right
//! after the class.

use std::collections::BTreeMap;

use horsies_core::catalog::{CodeCatalog, PythonEnumBase, TerminalStates};

use crate::python_text::{docstring, string_literal, GENERATED_HEADER};

/// The folder of the catalog modules, inside the package folder.
pub const CATALOG_FOLDER: &str = "_catalog";

/// The docstring of the generated `is_terminal` property.
const IS_TERMINAL_DOC: &str =
    "Whether this status represents a final state (no further transitions).";

/// The catalogs grouped by module, modules in name order, catalogs in
/// the given order.
pub fn group_by_module(catalogs: &[CodeCatalog]) -> BTreeMap<&'static str, Vec<CodeCatalog>> {
    let mut modules: BTreeMap<&'static str, Vec<CodeCatalog>> = BTreeMap::new();
    for catalog in catalogs {
        modules
            .entry(catalog.python_module)
            .or_default()
            .push(*catalog);
    }
    modules
}

/// The text of `_catalog/__init__.py`.
pub fn render_package_init(modules: &BTreeMap<&'static str, Vec<CodeCatalog>>) -> String {
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str("\"\"\"Code catalogs of horsies-core, one module per area.\n\nModules: ");
    let names: Vec<&str> = modules.keys().copied().collect();
    text.push_str(&names.join(", "));
    text.push_str(".\n\"\"\"\n");
    text
}

/// The text of one catalog module.
pub fn render_module(module: &str, catalogs: &[CodeCatalog]) -> String {
    let classes: Vec<&str> = catalogs
        .iter()
        .map(|catalog| catalog.python_class)
        .collect();
    let mut text = format!("{GENERATED_HEADER}\n");
    text.push_str(&format!(
        "\"\"\"Code catalogs of the `{module}` area: {}.\"\"\"\n\n",
        classes.join(", ")
    ));
    text.push_str("from __future__ import annotations\n\nfrom enum import Enum\n\n");
    let exported: Vec<String> = catalogs
        .iter()
        .flat_map(exported_names)
        .map(string_literal)
        .collect();
    text.push_str(&format!("__all__ = [{}]\n", exported.join(", ")));
    for catalog in catalogs {
        text.push_str("\n\n");
        text.push_str(&render_class(catalog));
    }
    text
}

/// The module names of one catalog: the class, then the terminal constant
/// of a lifecycle.
fn exported_names(catalog: &CodeCatalog) -> Vec<&'static str> {
    match catalog.terminal_states {
        TerminalStates::Lifecycle { constant, .. } => vec![catalog.python_class, constant],
        TerminalStates::NotALifecycle => vec![catalog.python_class],
    }
}

fn render_class(catalog: &CodeCatalog) -> String {
    let base = match catalog.python_base {
        PythonEnumBase::StrEnum => "str, Enum",
        PythonEnumBase::PlainEnum => "Enum",
    };
    let mut text = format!("class {}({base}):\n", catalog.python_class);
    let class_doc = docstring(catalog.doc, "    ");
    let has_doc = class_doc.is_some();
    if let Some(doc) = class_doc {
        text.push_str(&doc);
    }
    if catalog.members.is_empty() && !has_doc {
        text.push_str("    pass\n");
    }
    for (position, member) in catalog.members.iter().enumerate() {
        // A blank line before each member, but not at the top of a class
        // body without a docstring.
        if position > 0 || has_doc {
            text.push('\n');
        }
        text.push_str(&format!(
            "    {} = {}\n",
            member.name,
            string_literal(member.value)
        ));
        if let Some(doc) = docstring(member.doc, "    ") {
            text.push_str(&doc);
        }
    }
    match catalog.terminal_states {
        TerminalStates::Lifecycle { constant, members } => {
            text.push_str(&render_is_terminal(constant));
            text.push_str("\n\n");
            text.push_str(&render_terminal_constant(
                catalog.python_class,
                constant,
                members,
            ));
        }
        TerminalStates::NotALifecycle => {}
    }
    text
}

/// The `is_terminal` property, at the end of a class body.
fn render_is_terminal(constant: &str) -> String {
    format!(
        "\n    @property\n    def is_terminal(self) -> bool:\n        \"\"\"{IS_TERMINAL_DOC}\"\"\"\n        return self in {constant}\n"
    )
}

/// The module constant of the terminal members of `class`.
fn render_terminal_constant(class: &str, constant: &str, members: &[&str]) -> String {
    let mut text = format!("{constant}: frozenset[{class}] = frozenset(\n    {{\n");
    for member in members {
        text.push_str(&format!("        {class}.{member},\n"));
    }
    text.push_str("    }\n)\n");
    text
}

#[cfg(test)]
mod tests {
    use horsies_core::catalog::{CatalogMember, CodeEnum, CODE_CATALOGS};
    use horsies_core::ErrorCode;

    use super::*;

    const MEMBERS: &[CatalogMember] = &[
        CatalogMember {
            name: "PENDING",
            value: "PENDING",
            doc: "Waits for a worker.",
        },
        CatalogMember {
            name: "RUNNING",
            value: "RUNNING",
            doc: "",
        },
    ];

    fn catalog(module: &'static str, class: &'static str, base: PythonEnumBase) -> CodeCatalog {
        CodeCatalog {
            python_module: module,
            python_class: class,
            python_base: base,
            doc: "Task status.",
            members: MEMBERS,
            terminal_states: TerminalStates::NotALifecycle,
        }
    }

    fn lifecycle_catalog() -> CodeCatalog {
        CodeCatalog {
            terminal_states: TerminalStates::Lifecycle {
                constant: "STATUS_TERMINAL_STATES",
                members: &["RUNNING"],
            },
            ..catalog("status", "Status", PythonEnumBase::StrEnum)
        }
    }

    #[test]
    fn a_str_enum_renders_its_members_and_docs() {
        let rendered = render_class(&catalog("status", "Status", PythonEnumBase::StrEnum));
        assert_eq!(
            rendered,
            "class Status(str, Enum):\n    \"\"\"Task status.\"\"\"\n\n    PENDING = 'PENDING'\n    \"\"\"Waits for a worker.\"\"\"\n\n    RUNNING = 'RUNNING'\n"
        );
    }

    #[test]
    fn a_lifecycle_renders_is_terminal_and_its_constant() {
        let rendered = render_class(&lifecycle_catalog());
        assert_eq!(
            rendered,
            "class Status(str, Enum):\n    \"\"\"Task status.\"\"\"\n\n    PENDING = 'PENDING'\n    \"\"\"Waits for a worker.\"\"\"\n\n    RUNNING = 'RUNNING'\n\n    @property\n    def is_terminal(self) -> bool:\n        \"\"\"Whether this status represents a final state (no further transitions).\"\"\"\n        return self in STATUS_TERMINAL_STATES\n\n\nSTATUS_TERMINAL_STATES: frozenset[Status] = frozenset(\n    {\n        Status.RUNNING,\n    }\n)\n"
        );
    }

    #[test]
    fn a_module_exports_the_terminal_constant_after_its_class() {
        let catalogs = [
            lifecycle_catalog(),
            catalog("status", "Outcome", PythonEnumBase::StrEnum),
        ];
        let rendered = render_module("status", &catalogs);
        assert!(rendered.contains("__all__ = ['Status', 'STATUS_TERMINAL_STATES', 'Outcome']\n"));
    }

    #[test]
    fn a_member_whose_value_is_not_its_name_renders_name_equals_value() {
        let codes = CodeCatalog {
            members: &[CatalogMember {
                name: "WORKFLOW_NO_NAME",
                value: "HRS-001",
                doc: "The workflow has no name.",
            }],
            ..catalog("error_codes", "ErrorCode", PythonEnumBase::StrEnum)
        };
        assert_eq!(
            render_class(&codes),
            "class ErrorCode(str, Enum):\n    \"\"\"Task status.\"\"\"\n\n    WORKFLOW_NO_NAME = 'HRS-001'\n    \"\"\"The workflow has no name.\"\"\"\n"
        );
    }

    /// The `error_codes` module holds one member per core `ErrorCode`: the
    /// variant name in SCREAMING_SNAKE_CASE, its `HRS-nnn` code and its
    /// short text, in declaration order, and no other member.
    #[test]
    fn the_error_code_module_renders_every_hrs_code_with_its_name_and_text() {
        let catalog = <ErrorCode as CodeEnum>::CATALOG;
        let modules = group_by_module(CODE_CATALOGS);
        let rendered = render_module("error_codes", &modules["error_codes"]);
        assert!(rendered.contains("\n\n\nclass ErrorCode(str, Enum):\n"));
        let mut expected_lines = Vec::new();
        for (code, member) in <ErrorCode as CodeEnum>::VARIANTS
            .iter()
            .zip(catalog.members)
        {
            let name = screaming_snake_case(&format!("{code:?}"));
            let line = format!("    {name} = '{}'\n", code.as_code_str());
            let doc = docstring(member.doc, "    ").expect("each code has its short text");
            assert!(rendered.contains(&format!("{line}{doc}")), "{line}");
            expected_lines.push(line);
        }
        let member_lines: Vec<String> = rendered
            .lines()
            .filter(|line| line.contains(" = 'HRS-"))
            .map(|line| format!("{line}\n"))
            .collect();
        assert_eq!(member_lines, expected_lines);
    }

    /// The Python name rule of the derive: an underscore before each
    /// uppercase letter but the first, then all uppercase.
    fn screaming_snake_case(variant: &str) -> String {
        let mut name = String::with_capacity(variant.len() + 8);
        for (position, character) in variant.char_indices() {
            if position > 0 && character.is_uppercase() {
                name.push('_');
            }
            name.push(character.to_ascii_uppercase());
        }
        name
    }

    #[test]
    fn a_plain_enum_has_the_plain_base() {
        let rendered = render_class(&catalog("status", "Status", PythonEnumBase::PlainEnum));
        assert!(rendered.starts_with("class Status(Enum):\n"));
    }

    #[test]
    fn catalogs_group_by_module_in_name_order() {
        let catalogs = [
            catalog("status", "TaskStatus", PythonEnumBase::PlainEnum),
            catalog("broker", "BrokerErrorCode", PythonEnumBase::StrEnum),
            catalog("status", "WorkflowStatus", PythonEnumBase::StrEnum),
        ];
        let modules = group_by_module(&catalogs);
        let names: Vec<&str> = modules.keys().copied().collect();
        assert_eq!(names, ["broker", "status"]);
        let status: Vec<&str> = modules["status"]
            .iter()
            .map(|catalog| catalog.python_class)
            .collect();
        assert_eq!(status, ["TaskStatus", "WorkflowStatus"]);
    }

    #[test]
    fn a_module_starts_with_the_header_and_exports_its_classes() {
        let catalogs = [catalog("status", "TaskStatus", PythonEnumBase::PlainEnum)];
        let rendered = render_module("status", &catalogs);
        assert!(rendered.starts_with(GENERATED_HEADER));
        assert!(rendered.contains("__all__ = ['TaskStatus']\n"));
        assert!(rendered.contains("\n\n\nclass TaskStatus(Enum):\n"));
    }
}
