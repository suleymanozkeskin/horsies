//! The event contract of horsies-core (`horsies_core::events::EVENT_CONTRACT`)
//! as the TypeScript module `src/_catalog/events.ts`:
//!
//! - `EVENT_CONTRACT_VERSION`;
//! - each closed type, in the model's render order: a literal union of its
//!   wire values for a choice; one interface per variant and their union
//!   for a variant type;
//! - one interface per event type, the union `AnyEvent` and the tuple
//!   `EVENT_TYPES`;
//! - the refusals of the decoder and `decodeEvent` (see [`decoder`]).
//!
//! Every record has the member `kind`. For an event type it is the wire tag
//! (`"AttemptEnded"`); for a variant it is the wire tag in camelCase
//! (`"reportedByChild"`), with one fixed exception
//! ([`types::LIBRARY_CODE_VARIANT`]). Field names are in camelCase. A field
//! with `FieldAccess::Internal` is not a member of its interface.
//!
//! A closed type that another rendered module declares with the same name
//! and the same wire values ([`SHARED_TYPES`]) is imported from that module,
//! not declared again, so the main entry point of the package exports one
//! type of each name. The render checks that the two declarations agree.

mod decoder;
mod types;

use horsies_core::events::{
    CodeUnion, EventContract, EventField, NamedShape, ValueForm, BUILT_IN_TASK_CODES,
    EVENT_CONTRACT,
};

use crate::model::code_union_alias_name;
use crate::monitoring_models::{self, MONITORING_MODELS_MODULE};
use crate::output::{catalog_file, GeneratedFile};
use crate::task_code::TASK_CODE_MODULE;
use crate::ts_text::{import_types, module_text};
use crate::wire_json::WIRE_JSON_MODULE;

/// The stem of the events module: `src/_catalog/events.ts`.
pub const EVENTS_MODULE: &str = "events";

/// A closed type of the event contract that another rendered module
/// declares with the same name and the same wire values: `events.ts`
/// imports it from that module and declares none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedType {
    /// The name of the type, in both modules.
    pub name: &'static str,
    /// The stem of the module that declares it.
    pub module: &'static str,
}

/// The shared closed types: the execution class, the core's one `Execution`
/// (`horsies_core::Execution`), which the monitoring models hold too.
pub const SHARED_TYPES: &[SharedType] = &[SharedType {
    name: "Execution",
    module: MONITORING_MODELS_MODULE,
}];

/// Whether `events.ts` imports the closed type `name` from another module.
/// Pure.
pub fn is_shared(name: &str) -> bool {
    SHARED_TYPES.iter().any(|shared| shared.name == name)
}

/// The files of the event contract: `events.ts`. The render panics when the
/// model breaks a fact that the module relies on: the variant of the tag
/// exception, a code union that `task_code.ts` does not render, a field
/// name that is no member name, or a shared type that its module declares
/// otherwise.
pub fn generated_files() -> Vec<GeneratedFile> {
    types::assert_library_code_variant(&EVENT_CONTRACT);
    let monitoring: Vec<(&str, String)> = monitoring_models::generated_files()
        .iter()
        .map(|file| (MONITORING_MODELS_MODULE, file.text().to_owned()))
        .collect();
    assert_shared_types(&EVENT_CONTRACT, &monitoring);
    vec![catalog_file(
        EVENTS_MODULE,
        module_text(&module_sections(&EVENT_CONTRACT)),
    )]
}

/// The sections of `events.ts` for `contract`: the imports, the types, then
/// the decoder. Pure.
fn module_sections(contract: &EventContract) -> Vec<String> {
    let mut sections = Vec::new();
    let imports = import_lines(contract);
    if !imports.is_empty() {
        sections.push(imports.join("\n"));
    }
    sections.extend(types::type_sections(contract));
    sections.extend(decoder::decoder_sections(contract));
    sections
}

/// The type-only imports, in module-stem order: each shared type that the
/// contract has, from its module (`monitoring_models.ts`); the code union
/// types from `task_code.ts`; then the JSON value and the result from
/// `wire_json.ts`. Pure.
fn import_lines(contract: &EventContract) -> Vec<String> {
    let mut lines: Vec<String> = SHARED_TYPES
        .iter()
        .filter(|shared| {
            contract
                .value_types
                .iter()
                .any(|named| named.name == shared.name)
        })
        .map(|shared| import_types(&[shared.name], shared.module))
        .collect();
    lines.extend(
        code_unions(contract)
            .into_iter()
            .map(|union| import_types(&[code_union_type(union)], TASK_CODE_MODULE)),
    );
    lines.push(import_types(
        &["DecodeResult", "WireJson"],
        WIRE_JSON_MODULE,
    ));
    lines
}

/// Every field of the contract: the fields of the events, then the fields
/// of the variants of the closed types. Pure.
fn contract_fields(contract: &EventContract) -> Vec<EventField> {
    let mut fields: Vec<EventField> = contract
        .events
        .iter()
        .flat_map(|event| event.fields.iter().copied())
        .collect();
    for named in contract.value_types {
        match named.shape {
            NamedShape::Variants { variants } => fields.extend(
                variants
                    .iter()
                    .flat_map(|variant| variant.fields.iter().copied()),
            ),
            NamedShape::Choice { .. } => {}
        }
    }
    fields
}

/// The code unions that a field of the contract holds, each once, in the
/// order of first use. Pure.
fn code_unions(contract: &EventContract) -> Vec<&'static CodeUnion> {
    let mut unions: Vec<&'static CodeUnion> = Vec::new();
    for field in contract_fields(contract) {
        match field.form {
            ValueForm::Codes(union) => {
                if !unions.contains(&union) {
                    unions.push(union);
                }
            }
            ValueForm::Uuid
            | ValueForm::Text { .. }
            | ValueForm::WholeNumber { .. }
            | ValueForm::Duration
            | ValueForm::Time
            | ValueForm::Named(_) => {}
        }
    }
    unions
}

/// Panic unless each shared type is a closed type of `contract` and its
/// module, whose text `module_texts` gives by stem, declares it as
/// `events.ts` would: the same name, the same wire values, in the same
/// order.
fn assert_shared_types(contract: &EventContract, module_texts: &[(&str, String)]) {
    for shared in SHARED_TYPES {
        let Some(named) = contract
            .value_types
            .iter()
            .find(|named| named.name == shared.name)
        else {
            panic!(
                "events.ts: the contract has no closed type {}, which it imports from {}.ts",
                shared.name, shared.module,
            );
        };
        let declaration = types::declaration_text(named);
        let declared = module_texts.iter().any(|(stem, text)| {
            *stem == shared.module && text.contains(&format!("\n{declaration}\n"))
        });
        assert!(
            declared,
            "events.ts: {}.ts does not declare {} as the event contract does:\n{declaration}",
            shared.module, shared.name,
        );
    }
}

/// The type name of a code union, as `task_code.ts` exports it.
///
/// `task_code.ts` renders one code union, [`BUILT_IN_TASK_CODES`]. Another
/// union is a defect: `events.ts` would import a name that no module
/// exports. Pure; panics on another union.
fn code_union_type(union: &CodeUnion) -> &'static str {
    assert!(
        *union == BUILT_IN_TASK_CODES,
        "events.ts: a field holds the code union {} of {} catalogs, which is not \
         BUILT_IN_TASK_CODES, the one union that task_code.ts renders",
        code_union_alias_name(union),
        union.catalogs.len(),
    );
    code_union_alias_name(union)
}

#[cfg(test)]
mod tests {
    use horsies_core::events::{EventType, FieldAccess, NamedType};

    use super::*;
    use crate::ts_text::GENERATED_HEADER;

    const CODE_FIELD: EventField = EventField {
        name: "code",
        doc: "",
        form: ValueForm::Codes(&BUILT_IN_TASK_CODES),
        access: FieldAccess::Public,
    };

    const AT_FIELD: EventField = EventField {
        name: "at",
        doc: "",
        form: ValueForm::Time,
        access: FieldAccess::Public,
    };

    /// A code union that is not [`BUILT_IN_TASK_CODES`]: the same name, no
    /// catalog.
    const OTHER_CODES: CodeUnion = CodeUnion {
        catalogs: &[],
        ..BUILT_IN_TASK_CODES
    };

    fn contract(events: &'static [EventType], value_types: &'static [NamedType]) -> EventContract {
        EventContract {
            version: 7,
            events,
            value_types,
        }
    }

    #[test]
    fn the_events_module_is_one_catalog_file() {
        let files = generated_files();
        assert_eq!(files.len(), 1);
        let file = &files[0];
        assert_eq!(
            file.relative_path(),
            std::path::Path::new("src/_catalog/events.ts")
        );
        assert!(file.text().starts_with(GENERATED_HEADER));
        assert_eq!(file.text(), module_text(&module_sections(&EVENT_CONTRACT)));
    }

    #[test]
    fn a_contract_with_a_code_field_imports_the_code_union_type_once() {
        const EVENTS: &[EventType] = &[EventType {
            name: "Probe",
            doc: "",
            fields: &[CODE_FIELD, CODE_FIELD],
        }];
        assert_eq!(
            import_lines(&contract(EVENTS, &[])),
            [
                "import type { BuiltInTaskCode } from \"./task_code.ts\";",
                "import type { DecodeResult, WireJson } from \"./wire_json.ts\";",
            ]
        );
    }

    #[test]
    fn a_contract_without_a_code_field_imports_only_the_wire_types() {
        const EVENTS: &[EventType] = &[EventType {
            name: "Probe",
            doc: "",
            fields: &[AT_FIELD],
        }];
        assert_eq!(
            import_lines(&contract(EVENTS, &[])),
            ["import type { DecodeResult, WireJson } from \"./wire_json.ts\";"]
        );
    }

    #[test]
    fn the_real_module_is_the_imports_the_types_then_the_decoder() {
        assert_eq!(code_unions(&EVENT_CONTRACT), [&BUILT_IN_TASK_CODES]);
        let sections = module_sections(&EVENT_CONTRACT);
        assert_eq!(
            sections[0],
            "import type { Execution } from \"./monitoring_models.ts\";\n\
             import type { BuiltInTaskCode } from \"./task_code.ts\";\n\
             import type { DecodeResult, WireJson } from \"./wire_json.ts\";"
        );
        assert!(sections[1].ends_with(&format!(
            "export const EVENT_CONTRACT_VERSION = {};",
            EVENT_CONTRACT.version
        )));
        let types_end = sections
            .iter()
            .position(|section| section.contains("export const EVENT_TYPES = ["))
            .expect("the module has EVENT_TYPES");
        assert!(sections[types_end + 1].contains("export type JsonKind ="));
        assert!(sections
            .last()
            .is_some_and(|section| section.contains("export function decodeEvent(")));
    }

    #[test]
    fn the_contract_fields_are_the_event_fields_then_the_variant_fields() {
        let fields = contract_fields(&EVENT_CONTRACT);
        let event_fields: usize = EVENT_CONTRACT
            .events
            .iter()
            .map(|event| event.fields.len())
            .sum();
        assert!(fields.len() > event_fields);
        assert_eq!(fields[0], EVENT_CONTRACT.events[0].fields[0]);
    }

    #[test]
    fn each_shared_type_is_declared_by_its_module_as_the_contract_has_it() {
        let monitoring: Vec<(&str, String)> = monitoring_models::generated_files()
            .iter()
            .map(|file| (MONITORING_MODELS_MODULE, file.text().to_owned()))
            .collect();
        assert_shared_types(&EVENT_CONTRACT, &monitoring);
        assert!(is_shared("Execution"));
        assert!(!is_shared("StopReason"));
    }

    #[test]
    #[should_panic(
        expected = "monitoring_models.ts does not declare Execution as the event contract does"
    )]
    fn a_shared_type_that_its_module_declares_otherwise_fails_the_render() {
        let other = "\nexport type Execution =\n  | \"ISOLATED\";\n".to_owned();
        assert_shared_types(&EVENT_CONTRACT, &[(MONITORING_MODELS_MODULE, other)]);
    }

    #[test]
    fn the_real_module_imports_execution_and_declares_none() {
        let text = module_text(&module_sections(&EVENT_CONTRACT));
        assert!(text.contains("import type { Execution } from \"./monitoring_models.ts\";"));
        assert!(!text.contains("export type Execution ="));
        assert!(text.contains("readonly execution: Execution;"));
    }

    #[test]
    #[should_panic(expected = "of 0 catalogs, which is not BUILT_IN_TASK_CODES")]
    fn a_code_union_that_task_code_does_not_render_is_a_defect() {
        let _never = code_union_type(&OTHER_CODES);
    }
}
