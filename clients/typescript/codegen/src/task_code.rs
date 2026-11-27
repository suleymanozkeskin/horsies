//! `task_code.ts`: the built-in task codes, the reserved code names and
//! the metric label of an undeclared code.
//!
//! `BuiltInTaskCode` is the union of the literal types of the catalogs of
//! the core's `BUILT_IN_TASK_CODES`; this module holds no list of its own.
//! A member that only a blocking call form gives stays in it: a read decodes
//! every code that the core can store.

use std::collections::BTreeMap;

use horsies_core::core::task::error::RESERVED_NON_TASK_CODES;
use horsies_core::events::{CodeUnion, BUILT_IN_TASK_CODES};
use horsies_core::UNDECLARED_LABEL;

use crate::catalogs::{catalog_type_reference, property_access};
use crate::model::code_union_alias_name;
use crate::output::{catalog_file, GeneratedFile};
use crate::ts_text::{import_values, module_text, string_literal, union_alias};

/// The stem of the module, for a renderer that imports from it.
pub const TASK_CODE_MODULE: &str = "task_code";

/// The constant of the values of the built-in codes (the core's union
/// constant).
const BUILT_IN_CODES_CONSTANT: &str = "BUILT_IN_TASK_CODES";

/// The constant of the reserved names that are not task codes (the core's
/// constant).
const RESERVED_CODES_CONSTANT: &str = "RESERVED_NON_TASK_CODES";

/// The constant of the metric label of an undeclared code (the core's
/// constant).
const UNDECLARED_LABEL_CONSTANT: &str = "UNDECLARED_LABEL";

/// `task_code.ts`. Pure.
pub fn generated_files() -> Vec<GeneratedFile> {
    vec![catalog_file(
        TASK_CODE_MODULE,
        module_text(&task_code_sections(
            &BUILT_IN_TASK_CODES,
            &RESERVED_NON_TASK_CODES,
            UNDECLARED_LABEL,
        )),
    )]
}

/// The sections of the module: the imports of the catalogs, the built-in
/// code type, the built-in code values, the reserved names and the
/// undeclared label. Pure.
fn task_code_sections(
    built_in: &CodeUnion,
    reserved: &[&str],
    undeclared_label: &str,
) -> Vec<String> {
    vec![
        catalog_imports(built_in),
        built_in_code_type(built_in),
        built_in_code_values(built_in),
        reserved_codes(reserved),
        format!(
            "export const {UNDECLARED_LABEL_CONSTANT} = {};",
            string_literal(undeclared_label)
        ),
    ]
}

/// One value import per module of the catalogs of `union`, modules in name
/// order and names in name order. A value import also gives the type of the
/// same name. Pure.
fn catalog_imports(union: &CodeUnion) -> String {
    let mut modules: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for catalog in union.catalogs {
        let (module, type_name) = catalog_type_reference(catalog);
        modules.entry(module).or_default().push(type_name);
    }
    let imports: Vec<String> = modules
        .into_iter()
        .map(|(module, mut names)| {
            names.sort_unstable();
            import_values(&names, module)
        })
        .collect();
    imports.join("\n")
}

/// The union of the literal types of the catalogs of `union`, in its order.
/// Pure.
fn built_in_code_type(union: &CodeUnion) -> String {
    let members: Vec<String> = union
        .catalogs
        .iter()
        .map(|catalog| catalog_type_reference(catalog).1.to_owned())
        .collect();
    union_alias(code_union_alias_name(union), &members)
}

/// The frozen tuple of every member of the catalogs of `union`, catalogs in
/// its order and members in catalog order. The type is the readonly tuple of
/// the member values (the module has isolated declarations, so the type is
/// written out); the elements are the catalog members. Pure.
fn built_in_code_values(union: &CodeUnion) -> String {
    let mut values = Vec::new();
    let mut elements = Vec::new();
    for catalog in union.catalogs {
        let (_module, type_name) = catalog_type_reference(catalog);
        for member in catalog.members {
            values.push(string_literal(member.value));
            elements.push(property_access(type_name, member.name));
        }
    }
    format!(
        "/** The built-in task error codes of horsies: the members of the core's built-in code \
         catalogs, in order. */\n{}",
        frozen_tuple(BUILT_IN_CODES_CONSTANT, &values, &elements)
    )
}

/// The frozen readonly tuple of the reserved names, typed by their literals.
/// No reserved name gives the empty tuple. Pure.
fn reserved_codes(reserved: &[&str]) -> String {
    let literals: Vec<String> = reserved.iter().map(|name| string_literal(name)).collect();
    format!(
        "/** Code texts that are reserved and are not task codes. */\n{}",
        frozen_tuple(RESERVED_CODES_CONSTANT, &literals, &literals)
    )
}

/// `export const NAME: readonly [<types>] = Object.freeze([<elements>] as
/// const);`: the types on one line, as the declaration file writes them;
/// the elements one per line; an empty list is `readonly []` and `[]`.
/// Pure.
fn frozen_tuple(name: &str, types: &[String], elements: &[String]) -> String {
    format!(
        "export const {name}: readonly [{}] = Object.freeze({} as const);",
        types.join(", "),
        bracket_list(elements)
    )
}

/// `[]`, or `[` then one item per line with a trailing comma, then `]`.
/// Pure.
fn bracket_list(items: &[String]) -> String {
    match items {
        [] => "[]".to_owned(),
        several => {
            let lines: String = several.iter().map(|item| format!("  {item},\n")).collect();
            format!("[\n{lines}]")
        }
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::catalog::{
        CallForms, CatalogMember, CodeCatalog, PythonEnumBase, TerminalStates,
    };

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    const fn catalog(
        module: &'static str,
        class: &'static str,
        members: &'static [CatalogMember],
    ) -> CodeCatalog {
        CodeCatalog {
            python_module: module,
            python_class: class,
            python_base: PythonEnumBase::StrEnum,
            doc: "",
            members,
            terminal_states: TerminalStates::NotALifecycle,
        }
    }

    const fn member(name: &'static str, forms: CallForms) -> CatalogMember {
        CatalogMember {
            name,
            value: name,
            doc: "",
            forms,
        }
    }

    const OUTCOME_MEMBERS: &[CatalogMember] = &[member("TASK_TIMEOUT", CallForms::AnyForm)];

    const WAIT_MEMBERS: &[CatalogMember] = &[member("ASYNC_CONTEXT", CallForms::BlockingFormOnly)];

    const CONTRACT_MEMBERS: &[CatalogMember] = &[member("NO_TYPE_AVAILABLE", CallForms::AnyForm)];

    /// Three catalogs in two modules; the second module name sorts first.
    const SAMPLE: CodeUnion = CodeUnion {
        python_name: "SampleCode",
        doc: "",
        catalogs: &[
            catalog("task_codes", "OutcomeCode", OUTCOME_MEMBERS),
            catalog("more_codes", "WaitCode", WAIT_MEMBERS),
            catalog("task_codes", "ContractCode", CONTRACT_MEMBERS),
        ],
    };

    #[test]
    fn the_module_has_the_imports_the_types_the_values_and_the_label() {
        assert_eq!(
            module_text(&task_code_sections(&SAMPLE, &["STOPPED"], "undeclared")),
            format!(
                "{GENERATED_HEADER}\n\
                 \n\
                 import {{ WaitCode }} from \"./more_codes.ts\";\n\
                 import {{ ContractCode, OutcomeCode }} from \"./task_codes.ts\";\n\
                 \n\
                 export type SampleCode =\n  | OutcomeCode\n  | WaitCode\n  | ContractCode;\n\
                 \n\
                 /** The built-in task error codes of horsies: the members of the core's \
                 built-in code catalogs, in order. */\n\
                 export const BUILT_IN_TASK_CODES: readonly [\"TASK_TIMEOUT\", \
                 \"ASYNC_CONTEXT\", \"NO_TYPE_AVAILABLE\"] = Object.freeze([\n  \
                 OutcomeCode.TASK_TIMEOUT,\n  WaitCode.ASYNC_CONTEXT,\n  \
                 ContractCode.NO_TYPE_AVAILABLE,\n] as const);\n\
                 \n\
                 /** Code texts that are reserved and are not task codes. */\n\
                 export const RESERVED_NON_TASK_CODES: readonly [\"STOPPED\"] = \
                 Object.freeze([\n  \"STOPPED\",\n] as const);\n\
                 \n\
                 export const UNDECLARED_LABEL = \"undeclared\";\n"
            )
        );
    }

    #[test]
    fn several_reserved_names_are_a_tuple_of_their_literals() {
        assert_eq!(
            reserved_codes(&["A", "B"]),
            "/** Code texts that are reserved and are not task codes. */\n\
             export const RESERVED_NON_TASK_CODES: readonly [\"A\", \"B\"] = \
             Object.freeze([\n  \"A\",\n  \"B\",\n] as const);"
        );
    }

    #[test]
    fn no_reserved_name_is_the_empty_tuple() {
        assert_eq!(
            reserved_codes(&[]),
            "/** Code texts that are reserved and are not task codes. */\n\
             export const RESERVED_NON_TASK_CODES: readonly [] = Object.freeze([] as const);"
        );
    }

    /// The four catalogs of the core, in its order, under the core's union
    /// name.
    #[test]
    fn the_model_s_built_in_code_is_the_union_of_its_four_catalogs() {
        assert_eq!(
            built_in_code_type(&BUILT_IN_TASK_CODES),
            "export type BuiltInTaskCode =\n  | OperationalCode\n  | ContractCode\n  \
             | RetrievalCode\n  | OutcomeCode;"
        );
        assert_eq!(
            catalog_imports(&BUILT_IN_TASK_CODES),
            "import { ContractCode, OperationalCode, OutcomeCode, RetrievalCode } from \
             \"./task_codes.ts\";"
        );
    }

    /// 37 members, `RetrievalCode.ASYNC_CONTEXT` included: the blocking-form
    /// mark does not remove a built-in code.
    #[test]
    fn the_model_s_built_in_code_values_hold_every_member() {
        let text = built_in_code_values(&BUILT_IN_TASK_CODES);
        let members: usize = BUILT_IN_TASK_CODES
            .catalogs
            .iter()
            .map(|catalog| catalog.members.len())
            .sum();
        assert_eq!(members, 37);
        assert_eq!(text.lines().count(), members + 3);
        assert!(text.contains(" \"ASYNC_CONTEXT\", "));
        assert!(text.contains("\n  RetrievalCode.ASYNC_CONTEXT,\n"));
        assert!(text
            .contains("\nexport const BUILT_IN_TASK_CODES: readonly [\"UNHANDLED_EXCEPTION\", "));
        assert!(text.contains(" \"WORKFLOW_OUTPUT_SKIPPED\"] = Object.freeze([\n"));
        assert!(text.ends_with("\n] as const);"));
    }

    #[test]
    fn the_model_renders_one_file_with_the_reserved_name_and_the_label() {
        let files = generated_files();
        let [file] = files.as_slice() else {
            panic!("one file, got {}", files.len());
        };
        assert_eq!(
            file.relative_path().display().to_string(),
            "src/_catalog/task_code.ts"
        );
        assert!(file.text().contains(
            "\nexport const RESERVED_NON_TASK_CODES: readonly [\"WORKFLOW_STOPPED\"] = \
             Object.freeze([\n  \"WORKFLOW_STOPPED\",\n] as const);\n"
        ));
        assert!(file
            .text()
            .ends_with("\nexport const UNDECLARED_LABEL = \"undeclared\";\n"));
    }
}
