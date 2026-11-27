//! The code catalogs of horsies-core as TypeScript modules: one module per
//! catalog module of the model, in name order. The catalogs of a module keep
//! the order of `CODE_CATALOGS`.
//!
//! A catalog is an `as const` object from member name to wire value, and the
//! literal union type of the same name. A lifecycle also gets a
//! `ReadonlySet` constant of its terminal members, with the model's constant
//! name. Every member is rendered: the call-form mark of a member does not
//! remove it from its catalog. The renderer writes no member docs.

use std::collections::BTreeMap;

use horsies_core::catalog::{CodeCatalog, TerminalStates, CODE_CATALOGS};

use crate::model::{catalog_module, catalog_type_name};
use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{is_identifier, module_text, property_key, string_literal};

/// The catalog modules, in name order. Pure.
pub fn generated_files() -> Vec<GeneratedFile> {
    catalog_modules(CODE_CATALOGS)
        .into_iter()
        .map(|(module, catalogs)| catalog_file(module, module_text(&module_sections(&catalogs))))
        .collect()
}

/// The module stem and the type name of `catalog`, for a renderer that names
/// the type of a catalog from another module: `("status", "TaskStatus")`.
/// Pure.
pub fn catalog_type_reference(catalog: &CodeCatalog) -> (&'static str, &'static str) {
    (catalog_module(catalog), catalog_type_name(catalog))
}

/// The read of the property `name` of `object`: `TaskStatus.COMPLETED` for
/// an identifier, else `object["name"]`. Pure.
pub fn property_access(object: &str, name: &str) -> String {
    match is_identifier(name) {
        true => format!("{object}.{name}"),
        false => format!("{object}[{}]", string_literal(name)),
    }
}

/// The catalogs grouped by module: modules in name order, the catalogs of a
/// module in the order of `catalogs`. Pure.
fn catalog_modules(catalogs: &[CodeCatalog]) -> BTreeMap<&'static str, Vec<CodeCatalog>> {
    let mut modules: BTreeMap<&'static str, Vec<CodeCatalog>> = BTreeMap::new();
    for catalog in catalogs {
        modules
            .entry(catalog_module(catalog))
            .or_default()
            .push(*catalog);
    }
    modules
}

/// The sections of one catalog module: per catalog, its object, its type
/// and, for a lifecycle, its terminal set. Pure.
fn module_sections(catalogs: &[CodeCatalog]) -> Vec<String> {
    let mut sections = Vec::new();
    for catalog in catalogs {
        sections.push(catalog_object(catalog));
        sections.push(catalog_type(catalog));
        match catalog.terminal_states {
            TerminalStates::Lifecycle { constant, members } => {
                sections.push(terminal_set(catalog, constant, members));
            }
            TerminalStates::NotALifecycle => {}
        }
    }
    sections
}

/// `export const X = { NAME: "value", ... } as const;`, one member per line.
/// Pure.
///
/// A catalog without members is a defect of the model: its type would be
/// `never`.
fn catalog_object(catalog: &CodeCatalog) -> String {
    let name = catalog_type_name(catalog);
    assert!(
        !catalog.members.is_empty(),
        "the code catalog {name} has no member, so its type is never"
    );
    let mut text = format!("export const {name} = {{\n");
    for member in catalog.members {
        text.push_str(&format!(
            "  {}: {},\n",
            property_key(member.name),
            string_literal(member.value)
        ));
    }
    text.push_str("} as const;");
    text
}

/// The literal union of the values of the catalog object of the same name.
/// Pure.
fn catalog_type(catalog: &CodeCatalog) -> String {
    let name = catalog_type_name(catalog);
    format!("export type {name} = (typeof {name})[keyof typeof {name}];")
}

/// The `ReadonlySet` constant of the terminal members of a lifecycle. Pure.
///
/// A terminal member that is not a member of the catalog is a defect of the
/// model.
fn terminal_set(catalog: &CodeCatalog, constant: &str, members: &[&str]) -> String {
    let name = catalog_type_name(catalog);
    let mut text = format!("export const {constant}: ReadonlySet<{name}> = new Set<{name}>([\n");
    for member in members {
        assert!(
            catalog.member_named(member).is_some(),
            "the terminal member {member} of {constant} is not a member of {name}"
        );
        text.push_str(&format!("  {},\n", property_access(name, member)));
    }
    text.push_str("]);");
    text
}

#[cfg(test)]
mod tests {
    use horsies_core::catalog::{CallForms, CatalogMember, CodeEnum, PythonEnumBase};
    use horsies_core::{RetrievalCode, TaskStatus};

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    const MEMBERS: &[CatalogMember] = &[
        CatalogMember {
            name: "PENDING",
            value: "PENDING",
            doc: "Waits for a worker.",
            forms: CallForms::AnyForm,
        },
        CatalogMember {
            name: "DONE",
            value: "DONE",
            doc: "",
            forms: CallForms::BlockingFormOnly,
        },
    ];

    fn catalog(module: &'static str, class: &'static str) -> CodeCatalog {
        CodeCatalog {
            python_module: module,
            python_class: class,
            python_base: PythonEnumBase::StrEnum,
            doc: "A status.",
            members: MEMBERS,
            terminal_states: TerminalStates::NotALifecycle,
        }
    }

    fn lifecycle(members: &'static [&'static str]) -> CodeCatalog {
        CodeCatalog {
            terminal_states: TerminalStates::Lifecycle {
                constant: "STATUS_TERMINAL_STATES",
                members,
            },
            ..catalog("status", "Status")
        }
    }

    #[test]
    fn a_catalog_is_an_object_of_its_members_and_their_literal_union() {
        assert_eq!(
            module_sections(&[catalog("status", "Status")]),
            [
                "export const Status = {\n  PENDING: \"PENDING\",\n  DONE: \"DONE\",\n} as const;",
                "export type Status = (typeof Status)[keyof typeof Status];",
            ]
        );
    }

    #[test]
    fn a_lifecycle_has_the_set_of_its_terminal_members_after_its_type() {
        let sections = module_sections(&[lifecycle(&["DONE"])]);
        assert_eq!(
            sections.last().map(String::as_str),
            Some(
                "export const STATUS_TERMINAL_STATES: ReadonlySet<Status> = new Set<Status>([\n  \
                 Status.DONE,\n]);"
            )
        );
        assert_eq!(sections.len(), 3);
    }

    #[test]
    #[should_panic(expected = "the terminal member GONE of STATUS_TERMINAL_STATES is not a member")]
    fn a_terminal_member_outside_the_catalog_is_a_defect() {
        let _sections = module_sections(&[lifecycle(&["GONE"])]);
    }

    #[test]
    #[should_panic(expected = "the code catalog Empty has no member")]
    fn a_catalog_without_members_is_a_defect() {
        let empty = CodeCatalog {
            members: &[],
            ..catalog("status", "Empty")
        };
        let _sections = module_sections(&[empty]);
    }

    #[test]
    fn a_member_whose_value_is_not_its_name_keeps_its_name_as_the_key() {
        let codes = CodeCatalog {
            members: &[CatalogMember {
                name: "WORKFLOW_NO_NAME",
                value: "HRS-001",
                doc: "",
                forms: CallForms::AnyForm,
            }],
            ..catalog("error_codes", "ErrorCode")
        };
        assert_eq!(
            catalog_object(&codes),
            "export const ErrorCode = {\n  WORKFLOW_NO_NAME: \"HRS-001\",\n} as const;"
        );
    }

    #[test]
    fn catalogs_group_by_module_in_name_order() {
        let modules = catalog_modules(&[
            catalog("status", "TaskStatus"),
            catalog("broker", "DatabaseErrorCode"),
            catalog("status", "WorkflowStatus"),
        ]);
        let names: Vec<&str> = modules.keys().copied().collect();
        assert_eq!(names, ["broker", "status"]);
        let status: Vec<&str> = modules["status"].iter().map(catalog_type_name).collect();
        assert_eq!(status, ["TaskStatus", "WorkflowStatus"]);
    }

    #[test]
    fn a_property_read_quotes_a_name_that_is_not_an_identifier() {
        assert_eq!(property_access("Status", "DONE"), "Status.DONE");
        assert_eq!(property_access("Status", "HRS-001"), "Status[\"HRS-001\"]");
    }

    #[test]
    fn a_catalog_type_reference_is_its_module_and_its_type_name() {
        assert_eq!(
            catalog_type_reference(&<TaskStatus as CodeEnum>::CATALOG),
            ("status", "TaskStatus")
        );
    }

    /// The nine modules of the model, in name order, each with the header.
    #[test]
    fn the_model_renders_the_nine_catalog_modules_in_name_order() {
        let files = generated_files();
        let paths: Vec<String> = files
            .iter()
            .map(|file| file.relative_path().display().to_string())
            .collect();
        let expected: Vec<String> = [
            "broker",
            "error_codes",
            "history",
            "monitoring",
            "status",
            "task_codes",
            "task_send",
            "workflow_handle",
            "workflow_start",
        ]
        .iter()
        .map(|stem| format!("src/_catalog/{stem}.ts"))
        .collect();
        assert_eq!(paths, expected);
        for file in &files {
            assert!(file.text().starts_with(GENERATED_HEADER));
        }
    }

    /// Every member of every catalog has its line, the members that only a
    /// blocking form gives included.
    #[test]
    fn the_model_renders_every_member_of_every_catalog() {
        let texts: Vec<String> = generated_files()
            .iter()
            .map(|file| file.text().to_owned())
            .collect();
        let all_text = texts.concat();
        let mut members = 0;
        for catalog in CODE_CATALOGS {
            for member in catalog.members {
                let line = format!(
                    "\n  {}: {},\n",
                    property_key(member.name),
                    string_literal(member.value)
                );
                assert!(
                    all_text.contains(&line),
                    "{}.{}",
                    catalog_type_name(catalog),
                    member.name
                );
                members += 1;
            }
        }
        assert_eq!(members, 210);
        let retrieval = <RetrievalCode as CodeEnum>::CATALOG;
        let async_context = retrieval
            .member_named("ASYNC_CONTEXT")
            .expect("RetrievalCode has ASYNC_CONTEXT");
        assert_eq!(async_context.forms, CallForms::BlockingFormOnly);
        assert!(all_text.contains("\n  ASYNC_CONTEXT: \"ASYNC_CONTEXT\",\n"));
    }

    #[test]
    fn the_model_renders_the_three_terminal_sets_in_the_status_module() {
        let files = generated_files();
        let status = files
            .iter()
            .find(|file| file.relative_path().ends_with("status.ts"))
            .expect("the status module is rendered");
        assert!(status.text().contains(
            "\nexport const TASK_TERMINAL_STATES: ReadonlySet<TaskStatus> = \
             new Set<TaskStatus>([\n  TaskStatus.COMPLETED,\n  TaskStatus.FAILED,\n  \
             TaskStatus.CANCELLED,\n  \
             TaskStatus.EXPIRED,\n]);\n"
        ));
        assert!(status
            .text()
            .contains("\nexport const WORKFLOW_TERMINAL_STATES: ReadonlySet<WorkflowStatus> = "));
        assert!(status.text().contains(
            "\nexport const WORKFLOW_TASK_TERMINAL_STATES: ReadonlySet<WorkflowTaskStatus> = "
        ));
        assert_eq!(status.text().matches(": ReadonlySet<").count(), 3);
    }
}
