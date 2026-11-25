//! The types of `events.ts`: the contract version, the closed types, one
//! interface per event type, the union `AnyEvent` and the tuple
//! `EVENT_TYPES`.

use horsies_core::events::{
    AttemptErrorCode, EventContract, EventField, EventNamedValue, EventType, FieldAccess,
    NamedShape, NamedType, ValueForm, VariantType,
};

use super::code_union_type;
use crate::model::event_variant_type_name;
use crate::ts_text::{
    camel_case_of_pascal, camel_case_of_snake, integer_literal, is_identifier, is_safe_integer,
    string_literal, union_alias,
};

/// The member that tells the records of a union apart.
pub const KIND_MEMBER: &str = "kind";

/// The union of the event types.
pub const ANY_EVENT: &str = "AnyEvent";

/// The doc of `EVENT_CONTRACT_VERSION`.
const VERSION_DOC: &str = "/** The version of the event contract that this module reads. */";

/// The doc of `AnyEvent`.
const ANY_EVENT_DOC: &str = "/**
 * One event of the contract. A handler takes the events of one type, never this
 * union: a later contract version can add a type.
 */";

/// The doc of `EVENT_TYPES`.
const EVENT_TYPES_DOC: &str =
    "/** The names of the event types, in the order of the core's union. */";

/// A variant whose kind is not its wire tag in camelCase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TagException {
    /// The closed type of the variant.
    pub type_name: &'static str,
    /// The wire tag of the variant.
    pub variant_tag: &'static str,
    /// The kind of the variant's interface.
    pub kind: &'static str,
}

/// The one tag exception: the built-in code of an `AttemptErrorCode` has the
/// kind `library`, the name that the reads of a task error code give the
/// same idea (`{ kind: "library", code }`).
pub const LIBRARY_CODE_VARIANT: TagException = TagException {
    type_name: <AttemptErrorCode as EventNamedValue>::NAMED.name,
    variant_tag: "BuiltIn",
    kind: "library",
};

/// How TypeScript holds a whole number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WholeNumberType {
    /// Both ends of the range are inside the safe range of a `number`.
    Number,
    /// An end of the range is outside the safe range of a `number`.
    BigInt,
}

/// The TypeScript type of the whole numbers from `min` to `max`. Pure.
pub fn whole_number_type(min: i64, max: i64) -> WholeNumberType {
    match is_safe_integer(i128::from(min)) && is_safe_integer(i128::from(max)) {
        true => WholeNumberType::Number,
        false => WholeNumberType::BigInt,
    }
}

/// Panic unless `contract` has the variant of [`LIBRARY_CODE_VARIANT`]. A
/// core rename of the variant then fails the render, before any output
/// changes; a rename of the type fails the build of this renderer.
pub fn assert_library_code_variant(contract: &EventContract) {
    let exception = LIBRARY_CODE_VARIANT;
    let Some(named) = contract
        .value_types
        .iter()
        .find(|named| named.name == exception.type_name)
    else {
        panic!(
            "events.ts: the contract has no closed type {}, which holds the variant of the kind {}",
            exception.type_name, exception.kind,
        );
    };
    let has_variant = match named.shape {
        NamedShape::Variants { variants } => variants
            .iter()
            .any(|variant| variant.tag == exception.variant_tag),
        NamedShape::Choice { .. } => false,
    };
    assert!(
        has_variant,
        "events.ts: the closed type {} has no variant {}, which has the kind {}",
        exception.type_name, exception.variant_tag, exception.kind,
    );
}

/// The kind of a variant: its wire tag in camelCase, or the kind of
/// [`LIBRARY_CODE_VARIANT`]. Pure.
pub fn variant_kind(named: &NamedType, variant: &VariantType) -> String {
    let exception = LIBRARY_CODE_VARIANT;
    match named.name == exception.type_name && variant.tag == exception.variant_tag {
        true => exception.kind.to_owned(),
        false => camel_case_of_pascal(variant.tag),
    }
}

/// The member name of a field: its name in camelCase.
///
/// Panics when the name is not an identifier, or when it is [`KIND_MEMBER`],
/// the member that every record has.
pub fn member_name(field: &EventField) -> String {
    let name = camel_case_of_snake(field.name);
    assert!(
        is_identifier(&name) && name != KIND_MEMBER,
        "events.ts: the field {} has the member name {name}, which is not an identifier \
         or is the member `{KIND_MEMBER}` of every record",
        field.name,
    );
    name
}

/// The fields of `fields` that are members of the interface: the public
/// fields, in wire order. Pure.
pub fn public_fields(fields: &[EventField]) -> Vec<&EventField> {
    fields
        .iter()
        .filter_map(|field| match field.access {
            FieldAccess::Public => Some(field),
            FieldAccess::Internal => None,
        })
        .collect()
}

/// The TypeScript type of a value form. Pure; panics on a code union that
/// `task_code.ts` does not render.
pub fn form_type(form: ValueForm) -> &'static str {
    match form {
        ValueForm::Uuid | ValueForm::Text { .. } => "string",
        ValueForm::WholeNumber { min, max } => match whole_number_type(min, max) {
            WholeNumberType::Number => "number",
            WholeNumberType::BigInt => "bigint",
        },
        ValueForm::Duration => "Temporal.Duration",
        ValueForm::Time => "Temporal.Instant",
        ValueForm::Codes(union) => code_union_type(union),
        ValueForm::Named(name) => name,
    }
}

/// The sections of the types, in the model's order: the contract version,
/// each closed type, each event type, `AnyEvent` and `EVENT_TYPES`. Pure.
pub fn type_sections(contract: &EventContract) -> Vec<String> {
    let mut sections = vec![format!(
        "{VERSION_DOC}\nexport const EVENT_CONTRACT_VERSION = {};",
        integer_literal(i128::from(contract.version))
    )];
    for named in contract.value_types {
        sections.extend(named_type_sections(named));
    }
    for event in contract.events {
        sections.push(record_interface(event.name, event.name, event.fields));
    }
    sections.push(any_event_section(contract.events));
    sections.push(event_types_section(contract.events));
    sections
}

/// A choice as the union of its wire values; a variant type as one
/// interface per variant, then their union. Pure.
fn named_type_sections(named: &NamedType) -> Vec<String> {
    match named.shape {
        NamedShape::Choice { values } => {
            let literals: Vec<String> = values
                .iter()
                .map(|value| string_literal(value.value))
                .collect();
            vec![union_alias(named.name, &literals)]
        }
        NamedShape::Variants { variants } => {
            let mut sections: Vec<String> = variants
                .iter()
                .map(|variant| {
                    record_interface(
                        event_variant_type_name(variant),
                        &variant_kind(named, variant),
                        variant.fields,
                    )
                })
                .collect();
            let members: Vec<String> = variants
                .iter()
                .map(|variant| event_variant_type_name(variant).to_owned())
                .collect();
            sections.push(union_alias(named.name, &members));
            sections
        }
    }
}

/// The interface of a record: the member `kind` with the literal `kind`,
/// then each public field. Pure.
fn record_interface(type_name: &str, kind: &str, fields: &[EventField]) -> String {
    let mut text = format!(
        "export interface {type_name} {{\n  readonly {KIND_MEMBER}: {};\n",
        string_literal(kind)
    );
    for field in public_fields(fields) {
        text.push_str(&format!(
            "  readonly {}: {};\n",
            member_name(field),
            form_type(field.form)
        ));
    }
    text.push('}');
    text
}

/// `AnyEvent`: the union of the event types. Pure.
fn any_event_section(events: &[EventType]) -> String {
    let members: Vec<String> = events.iter().map(|event| event.name.to_owned()).collect();
    format!("{ANY_EVENT_DOC}\n{}", union_alias(ANY_EVENT, &members))
}

/// `EVENT_TYPES`: the names of the event types, as a tuple. Pure.
fn event_types_section(events: &[EventType]) -> String {
    let mut text = format!("{EVENT_TYPES_DOC}\nexport const EVENT_TYPES = [\n");
    for event in events {
        text.push_str(&format!("  {},\n", string_literal(event.name)));
    }
    text.push_str("] as const;");
    text
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use horsies_core::events::{ChoiceValue, BUILT_IN_TASK_CODES, EVENT_CONTRACT};

    use super::*;
    use crate::model::event_variant;

    const AT: EventField = EventField {
        name: "at",
        doc: "",
        form: ValueForm::Time,
        access: FieldAccess::Public,
    };

    const TASK_NAME: EventField = EventField {
        name: "task_name",
        doc: "",
        form: ValueForm::Text { max_chars: 255 },
        access: FieldAccess::Public,
    };

    const TRACE: EventField = EventField {
        name: "trace",
        doc: "",
        form: ValueForm::Text { max_chars: 8 },
        access: FieldAccess::Internal,
    };

    const CODE: EventField = EventField {
        name: "code",
        doc: "",
        form: ValueForm::Codes(&BUILT_IN_TASK_CODES),
        access: FieldAccess::Public,
    };

    const MODE: NamedType = NamedType {
        name: "Mode",
        doc: "",
        shape: NamedShape::Choice {
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

    const START: NamedType = NamedType {
        name: "Start",
        doc: "",
        shape: NamedShape::Variants {
            variants: &[
                event_variant("Reported", "StartReported", &[AT]),
                event_variant("NotReported", "StartNotReported", &[]),
            ],
        },
    };

    /// A sample closed type with the name and the variant of the tag
    /// exception.
    const ERROR_CODE: NamedType = NamedType {
        name: "AttemptErrorCode",
        doc: "",
        shape: NamedShape::Variants {
            variants: &[
                event_variant("BuiltIn", "AttemptErrorCodeBuiltIn", &[CODE]),
                event_variant("UserOverBound", "AttemptErrorCodeUserOverBound", &[]),
            ],
        },
    };

    const PROBE: EventType = EventType {
        name: "Probe",
        doc: "",
        fields: &[AT, TASK_NAME, TRACE],
    };

    const SECOND: EventType = EventType {
        name: "Second",
        doc: "",
        fields: &[AT],
    };

    fn field_of(form: ValueForm) -> EventField {
        EventField {
            name: "value",
            doc: "",
            form,
            access: FieldAccess::Public,
        }
    }

    #[test]
    fn a_choice_is_the_union_of_its_wire_values() {
        assert_eq!(
            named_type_sections(&MODE),
            ["export type Mode =\n  | \"ONE\"\n  | \"TWO\";"]
        );
    }

    #[test]
    fn a_variant_type_is_one_interface_per_variant_then_their_union() {
        assert_eq!(
            named_type_sections(&START),
            [
                "export interface StartReported {\n  readonly kind: \"reported\";\n  \
                 readonly at: Temporal.Instant;\n}",
                "export interface StartNotReported {\n  readonly kind: \"notReported\";\n}",
                "export type Start =\n  | StartReported\n  | StartNotReported;",
            ]
        );
    }

    #[test]
    fn the_built_in_variant_of_the_attempt_error_code_has_the_kind_library() {
        assert_eq!(
            named_type_sections(&ERROR_CODE),
            [
                "export interface AttemptErrorCodeBuiltIn {\n  readonly kind: \"library\";\n  \
                 readonly code: BuiltInTaskCode;\n}",
                "export interface AttemptErrorCodeUserOverBound {\n  \
                 readonly kind: \"userOverBound\";\n}",
                "export type AttemptErrorCode =\n  | AttemptErrorCodeBuiltIn\n  \
                 | AttemptErrorCodeUserOverBound;",
            ]
        );
    }

    #[test]
    fn a_variant_named_built_in_in_another_type_keeps_its_tag_in_camel_case() {
        let variant = event_variant("BuiltIn", "StartBuiltIn", &[]);
        assert_eq!(variant_kind(&START, &variant), "builtIn");
    }

    #[test]
    fn the_real_contract_has_the_variant_of_the_tag_exception() {
        assert_library_code_variant(&EVENT_CONTRACT);
        let text = type_sections(&EVENT_CONTRACT).join("\n\n");
        assert!(text.contains(
            "export interface AttemptErrorCodeBuiltIn {\n  readonly kind: \"library\";\n  \
             readonly code: BuiltInTaskCode;\n}"
        ));
        assert!(text.contains(
            "export interface AttemptErrorCodeUser {\n  readonly kind: \"user\";\n  \
             readonly code: string;\n}"
        ));
        assert!(text.contains(
            "export interface AttemptErrorCodeUserOverBound {\n  \
             readonly kind: \"userOverBound\";\n}"
        ));
    }

    #[test]
    #[should_panic(expected = "has no variant BuiltIn, which has the kind library")]
    fn a_contract_whose_error_code_has_no_built_in_variant_fails_the_render() {
        const RENAMED: NamedType = NamedType {
            name: "AttemptErrorCode",
            doc: "",
            shape: NamedShape::Variants {
                variants: &[event_variant("Library", "AttemptErrorCodeLibrary", &[CODE])],
            },
        };
        assert_library_code_variant(&EventContract {
            version: 7,
            events: &[SECOND],
            value_types: &[RENAMED],
        });
    }

    #[test]
    #[should_panic(expected = "the contract has no closed type AttemptErrorCode")]
    fn a_contract_without_the_error_code_type_fails_the_render() {
        assert_library_code_variant(&EventContract {
            version: 7,
            events: &[SECOND],
            value_types: &[START],
        });
    }

    #[test]
    fn an_event_is_an_interface_with_its_tag_as_kind_and_camel_case_members() {
        assert_eq!(
            record_interface(PROBE.name, PROBE.name, PROBE.fields),
            "export interface Probe {\n  readonly kind: \"Probe\";\n  \
             readonly at: Temporal.Instant;\n  readonly taskName: string;\n}"
        );
    }

    #[test]
    fn an_internal_field_is_not_a_member() {
        assert_eq!(public_fields(PROBE.fields), [&AT, &TASK_NAME]);
        assert!(!record_interface(PROBE.name, PROBE.name, PROBE.fields).contains("trace"));
    }

    #[test]
    fn each_value_form_has_its_typescript_type() {
        let cases = [
            (ValueForm::Uuid, "string"),
            (ValueForm::Text { max_chars: 8 }, "string"),
            (ValueForm::WholeNumber { min: 1, max: 100 }, "number"),
            (ValueForm::Duration, "Temporal.Duration"),
            (ValueForm::Time, "Temporal.Instant"),
            (ValueForm::Codes(&BUILT_IN_TASK_CODES), "BuiltInTaskCode"),
            (ValueForm::Named("Execution"), "Execution"),
        ];
        for (form, expected) in cases {
            assert_eq!(form_type(form), expected, "{form:?}");
        }
    }

    #[test]
    fn a_whole_number_past_the_safe_range_is_a_bigint() {
        let safe_max = 9_007_199_254_740_991;
        assert_eq!(
            whole_number_type(-safe_max, safe_max),
            WholeNumberType::Number
        );
        assert_eq!(whole_number_type(0, safe_max + 1), WholeNumberType::BigInt);
        assert_eq!(whole_number_type(-safe_max - 1, 0), WholeNumberType::BigInt);
        assert_eq!(
            form_type(ValueForm::WholeNumber {
                min: 0,
                max: i64::MAX
            }),
            "bigint"
        );
    }

    #[test]
    #[should_panic(expected = "the field kind has the member name kind")]
    fn a_field_named_kind_is_a_defect() {
        let _never = member_name(&EventField {
            name: "kind",
            ..field_of(ValueForm::Uuid)
        });
    }

    #[test]
    #[should_panic(expected = "which is not an identifier")]
    fn a_field_name_that_is_no_identifier_is_a_defect() {
        let _never = member_name(&EventField {
            name: "2nd_value",
            ..field_of(ValueForm::Uuid)
        });
    }

    #[test]
    fn the_union_and_the_tuple_list_every_event_type_in_order() {
        let sample = EventContract {
            version: 7,
            events: &[PROBE, SECOND],
            value_types: &[MODE],
        };
        assert_eq!(
            type_sections(&sample),
            [
                "/** The version of the event contract that this module reads. */\n\
                 export const EVENT_CONTRACT_VERSION = 7;",
                "export type Mode =\n  | \"ONE\"\n  | \"TWO\";",
                "export interface Probe {\n  readonly kind: \"Probe\";\n  \
                 readonly at: Temporal.Instant;\n  readonly taskName: string;\n}",
                "export interface Second {\n  readonly kind: \"Second\";\n  \
                 readonly at: Temporal.Instant;\n}",
                "/**\n * One event of the contract. A handler takes the events of one type, \
                 never this\n * union: a later contract version can add a type.\n */\n\
                 export type AnyEvent =\n  | Probe\n  | Second;",
                "/** The names of the event types, in the order of the core's union. */\n\
                 export const EVENT_TYPES = [\n  \"Probe\",\n  \"Second\",\n] as const;",
            ]
        );
    }

    #[test]
    fn every_event_type_and_closed_type_of_the_real_contract_is_rendered_once() {
        let sections = type_sections(&EVENT_CONTRACT);
        let text = sections.join("\n\n");
        let count = |head: &str| {
            sections
                .iter()
                .filter(|section| section.starts_with(head))
                .count()
        };
        for event in EVENT_CONTRACT.events {
            assert_eq!(count(&format!("export interface {} {{\n", event.name)), 1);
            assert!(
                text.contains(&format!("\n  | {}", event.name)),
                "{}",
                event.name
            );
            assert!(
                text.contains(&format!("\n  \"{}\",", event.name)),
                "{}",
                event.name
            );
        }
        for named in EVENT_CONTRACT.value_types {
            assert_eq!(
                count(&format!("export type {} =", named.name)),
                1,
                "{}",
                named.name
            );
            match named.shape {
                NamedShape::Variants { variants } => {
                    for variant in variants {
                        let head =
                            format!("export interface {} {{\n", event_variant_type_name(variant));
                        assert_eq!(count(&head), 1, "{head}");
                    }
                }
                NamedShape::Choice { .. } => {}
            }
        }
        assert!(text.starts_with(&format!(
            "{VERSION_DOC}\nexport const EVENT_CONTRACT_VERSION = {};",
            EVENT_CONTRACT.version
        )));
    }

    #[test]
    fn the_kinds_of_each_real_closed_type_are_unique() {
        for named in EVENT_CONTRACT.value_types {
            match named.shape {
                NamedShape::Variants { variants } => {
                    let kinds: BTreeSet<String> = variants
                        .iter()
                        .map(|variant| variant_kind(named, variant))
                        .collect();
                    assert_eq!(kinds.len(), variants.len(), "{}", named.name);
                }
                NamedShape::Choice { .. } => {}
            }
        }
    }

    #[test]
    fn the_real_contract_leaves_out_every_internal_field() {
        let text = type_sections(&EVENT_CONTRACT).join("\n\n");
        assert!(text.contains(
            "export interface AttemptEndDecidedByWorker {\n  \
             readonly kind: \"decidedByWorker\";\n  readonly reason: WorkerDecision;\n}"
        ));
    }
}
