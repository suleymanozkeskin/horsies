//! Tests of the error catalog renderer: the full text of a sample module,
//! one test per outcome of the call-form rule and of the model checks, and
//! assertions on the real model.

use horsies_core::catalog::{EnumValues, TerminalStates};
use horsies_core::error_catalog::{ErrorSlot, ExceptionType, FieldDefault};

use super::*;
use crate::ts_text::GENERATED_HEADER;

const fn member(name: &'static str, forms: CallForms) -> CatalogMember {
    CatalogMember {
        name,
        value: name,
        doc: "",
        forms,
    }
}

const CODE_MEMBERS: &[CatalogMember] = &[
    member("ENQUEUE_FAILED", CallForms::AnyForm),
    member("PAYLOAD_MISMATCH", CallForms::AnyForm),
    member("PING_FAILED", CallForms::AnyForm),
    member("CLOSE_FAILED", CallForms::AnyForm),
    member("ASYNC_CONTEXT", CallForms::BlockingFormOnly),
    member("WAIT_FAILED", CallForms::BlockingFormOnly),
];

const CODES: CodeCatalog = CodeCatalog {
    module: "probe",
    type_name: "ProbeErrorCode",
    values: EnumValues::EqualText,
    doc: "",
    members: CODE_MEMBERS,
    terminal_states: TerminalStates::NotALifecycle,
};

const MESSAGE: ErrorSlot = ErrorSlot::Field {
    name: "message",
    slot_type: SlotType::Str,
    repr: SlotRepr::Hidden,
};

const ENQUEUE_FAILED: ErrorClass = ErrorClass {
    type_name: "ProbeEnqueueFailed",
    doc: "",
    slots: &[
        ErrorSlot::CodeMember {
            member: "ENQUEUE_FAILED",
        },
        ErrorSlot::Field {
            name: "task_id",
            slot_type: SlotType::Uuid,
            repr: SlotRepr::Shown,
        },
        MESSAGE,
        ErrorSlot::Field {
            name: "retryable",
            slot_type: SlotType::Bool,
            repr: SlotRepr::Shown,
        },
        ErrorSlot::Exception {
            cause_type: ExceptionType::ExceptionOrNoCause,
        },
    ],
};

const PAYLOAD_MISMATCH: ErrorClass = ErrorClass {
    type_name: "ProbePayloadMismatch",
    doc: "",
    slots: &[
        ErrorSlot::CodeMember {
            member: "PAYLOAD_MISMATCH",
        },
        ErrorSlot::NeverRetryable,
        MESSAGE,
        ErrorSlot::Field {
            name: "size_bytes",
            slot_type: SlotType::Int,
            repr: SlotRepr::Shown,
        },
    ],
};

const OPERATION_FAILED: ErrorClass = ErrorClass {
    type_name: "ProbeOperationFailed",
    doc: "",
    slots: &[
        ErrorSlot::CodeSubset {
            subset: "ProbeOperationCode",
        },
        MESSAGE,
        ErrorSlot::Field {
            name: "current_status",
            slot_type: SlotType::TaskStatus,
            repr: SlotRepr::Shown,
        },
        ErrorSlot::Exception {
            cause_type: ExceptionType::Exception,
        },
    ],
};

const ASYNC_CONTEXT: ErrorClass = ErrorClass {
    type_name: "ProbeAsyncContext",
    doc: "",
    slots: &[
        ErrorSlot::CodeMember {
            member: "ASYNC_CONTEXT",
        },
        ErrorSlot::NeverRetryable,
        MESSAGE,
    ],
};

const CATALOG: ErrorCatalog = ErrorCatalog {
    module: "probe_errors",
    code_catalog: CODES,
    union: "ProbeCoreError",
    union_doc: "",
    classes: &[
        ENQUEUE_FAILED,
        PAYLOAD_MISMATCH,
        OPERATION_FAILED,
        ASYNC_CONTEXT,
    ],
};

const UNION: ErrorUnion = ErrorUnion {
    module: "probe_errors",
    name: "ProbeEnqueueError",
    doc: "",
    members: &[
        "ProbeEnqueueFailed",
        "ProbeAsyncContext",
        "ProbePayloadMismatch",
    ],
};

fn subset(name: &'static str, members: Vec<&'static str>) -> CodeSubset {
    CodeSubset {
        module: "probe_errors",
        name,
        doc: "",
        code_catalog: CODES,
        members,
    }
}

fn operation_subset() -> CodeSubset {
    subset("ProbeOperationCode", vec!["PING_FAILED", "CLOSE_FAILED"])
}

fn sample_module() -> ErrorModule {
    ErrorModule {
        catalogs: vec![CATALOG],
        subsets: vec![operation_subset()],
        records: Vec::new(),
        unions: vec![UNION],
    }
}

/// A sample class with `slots`, its code slot first.
fn subset_class(slots: &'static [ErrorSlot]) -> ErrorClass {
    ErrorClass {
        type_name: "ProbeSubsetClass",
        doc: "",
        slots,
    }
}

fn sample_subsets() -> Vec<CodeSubset> {
    vec![
        operation_subset(),
        subset("ProbeWaitCode", vec!["ASYNC_CONTEXT", "WAIT_FAILED"]),
        subset("ProbeMixedCode", vec!["PING_FAILED", "WAIT_FAILED"]),
        subset("ProbeEmptyCode", Vec::new()),
        subset("ProbeStrangerCode", vec!["NOT_A_CODE"]),
    ]
}

const SAMPLE_MODULE_BODY: &str = r#"
import type { CoreCause, NoCause } from "./error_detail.ts";
import type { TaskStatus } from "./status.ts";

export type ProbeOperationCode =
  | "PING_FAILED"
  | "CLOSE_FAILED";

export interface ProbeOperationCodeKind {
  readonly PING_FAILED: "pingFailed";
  readonly CLOSE_FAILED: "closeFailed";
}

export interface ProbeEnqueueFailed {
  readonly kind: "enqueueFailed";
  readonly code: "ENQUEUE_FAILED";
  readonly taskId: string;
  readonly message: string;
  readonly retryable: boolean;
  readonly cause: CoreCause | NoCause;
}

export interface ProbePayloadMismatch {
  readonly kind: "payloadMismatch";
  readonly code: "PAYLOAD_MISMATCH";
  readonly retryable: false;
  readonly message: string;
  readonly sizeBytes: bigint;
}

export type ProbeOperationFailed<Code extends ProbeOperationCode = ProbeOperationCode> = {
  readonly [Member in Code]: {
    readonly kind: ProbeOperationCodeKind[Member];
    readonly code: Member;
    readonly message: string;
    readonly currentStatus: TaskStatus;
    readonly cause: CoreCause;
  };
}[Code];

export type ProbeCoreError =
  | ProbeEnqueueFailed
  | ProbePayloadMismatch
  | ProbeOperationFailed;

export type ProbeEnqueueError =
  | ProbeEnqueueFailed
  | ProbePayloadMismatch;

export interface NativeProbeEnqueueFailed {
  readonly class: "ProbeEnqueueFailed";
  readonly slots: {
    readonly task_id: string;
    readonly message: string;
    readonly retryable: boolean;
    readonly cause: CoreCause | NoCause;
  };
}

export interface NativeProbePayloadMismatch {
  readonly class: "ProbePayloadMismatch";
  readonly slots: {
    readonly message: string;
    readonly size_bytes: bigint;
  };
}

export interface NativeProbeOperationFailed {
  readonly class: "ProbeOperationFailed";
  readonly slots: {
    readonly code: ProbeOperationCode;
    readonly message: string;
    readonly current_status: TaskStatus;
    readonly cause: CoreCause;
  };
}

/** The value that the bridge gives: the class name and the instance slots by core name. */
export type NativeProbeCoreError =
  | NativeProbeEnqueueFailed
  | NativeProbePayloadMismatch
  | NativeProbeOperationFailed;

function unknownClass(converter: string, raw: { readonly class: string }): never {
  throw new Error(`${converter}: unknown class name ${raw.class}`);
}

function unknownCode(converter: string, code: string): never {
  throw new Error(`${converter}: unknown code ${code}`);
}

/** The frozen value of `raw`, its `kind` named after its code. A code that this build does not know throws. */
function probeOperationFailedFromNative(raw: NativeProbeOperationFailed): ProbeOperationFailed {
  switch (raw.slots.code) {
    case "PING_FAILED": {
      const value: ProbeOperationFailed<"PING_FAILED"> = {
        kind: "pingFailed",
        code: raw.slots.code,
        message: raw.slots.message,
        currentStatus: raw.slots.current_status,
        cause: Object.freeze({ ...raw.slots.cause }),
      };

      return Object.freeze(
        Object.defineProperties(value, {
          message: { enumerable: false },
          cause: { enumerable: false },
        }),
      );
    }

    case "CLOSE_FAILED": {
      const value: ProbeOperationFailed<"CLOSE_FAILED"> = {
        kind: "closeFailed",
        code: raw.slots.code,
        message: raw.slots.message,
        currentStatus: raw.slots.current_status,
        cause: Object.freeze({ ...raw.slots.cause }),
      };

      return Object.freeze(
        Object.defineProperties(value, {
          message: { enumerable: false },
          cause: { enumerable: false },
        }),
      );
    }

    default: {
      const unreachable: never = raw.slots.code;

      return unknownCode("probeOperationFailedFromNative", unreachable);
    }
  }
}

/** The frozen value of `raw`. A class name that this build does not know throws. */
export function probeCoreErrorFromNative(raw: NativeProbeCoreError): ProbeCoreError {
  switch (raw.class) {
    case "ProbeEnqueueFailed": {
      const value: ProbeEnqueueFailed = {
        kind: "enqueueFailed",
        code: "ENQUEUE_FAILED",
        taskId: raw.slots.task_id,
        message: raw.slots.message,
        retryable: raw.slots.retryable,
        cause: Object.freeze({ ...raw.slots.cause }),
      };

      return Object.freeze(
        Object.defineProperties(value, {
          message: { enumerable: false },
          cause: { enumerable: false },
        }),
      );
    }

    case "ProbePayloadMismatch": {
      const value: ProbePayloadMismatch = {
        kind: "payloadMismatch",
        code: "PAYLOAD_MISMATCH",
        retryable: false,
        message: raw.slots.message,
        sizeBytes: raw.slots.size_bytes,
      };

      return Object.freeze(
        Object.defineProperties(value, {
          message: { enumerable: false },
        }),
      );
    }

    case "ProbeOperationFailed":
      return probeOperationFailedFromNative(raw);

    default: {
      const unreachable: never = raw;

      return unknownClass("probeCoreErrorFromNative", unreachable);
    }
  }
}
"#;

#[test]
fn a_module_has_its_types_its_native_forms_and_its_converter() {
    assert_eq!(
        module_text(&module_sections(&sample_module())),
        format!("{GENERATED_HEADER}\n{SAMPLE_MODULE_BODY}")
    );
}

#[test]
fn a_code_member_that_any_form_gives_is_rendered() {
    assert_eq!(
        class_call_forms(&ENQUEUE_FAILED, &CODES, &[]),
        CallForms::AnyForm
    );
}

#[test]
fn a_code_member_that_only_a_blocking_form_gives_is_left_out() {
    assert_eq!(
        class_call_forms(&ASYNC_CONTEXT, &CODES, &[]),
        CallForms::BlockingFormOnly
    );
    let rendered = rendered_catalog(&CATALOG, &[operation_subset()]);
    let names: Vec<&str> = rendered
        .classes
        .iter()
        .map(|class| error_class_type_name(class))
        .collect();
    assert_eq!(
        names,
        [
            "ProbeEnqueueFailed",
            "ProbePayloadMismatch",
            "ProbeOperationFailed"
        ]
    );
}

#[test]
fn a_subset_that_any_form_gives_is_rendered() {
    assert_eq!(
        class_call_forms(
            &subset_class(&[ErrorSlot::CodeSubset {
                subset: "ProbeOperationCode"
            }]),
            &CODES,
            &sample_subsets()
        ),
        CallForms::AnyForm
    );
}

#[test]
fn a_subset_that_only_a_blocking_form_gives_is_left_out() {
    assert_eq!(
        class_call_forms(
            &subset_class(&[ErrorSlot::CodeSubset {
                subset: "ProbeWaitCode"
            }]),
            &CODES,
            &sample_subsets()
        ),
        CallForms::BlockingFormOnly
    );
}

#[test]
#[should_panic(expected = "the code subset ProbeMixedCode of ProbeSubsetClass mixes call forms")]
fn a_subset_with_two_call_forms_is_a_defect() {
    let _forms = class_call_forms(
        &subset_class(&[ErrorSlot::CodeSubset {
            subset: "ProbeMixedCode",
        }]),
        &CODES,
        &sample_subsets(),
    );
}

#[test]
#[should_panic(expected = "the code subset ProbeEmptyCode of ProbeSubsetClass has no member")]
fn a_subset_without_members_is_a_defect() {
    let _forms = class_call_forms(
        &subset_class(&[ErrorSlot::CodeSubset {
            subset: "ProbeEmptyCode",
        }]),
        &CODES,
        &sample_subsets(),
    );
}

#[test]
#[should_panic(expected = "the code NOT_A_CODE of the subset ProbeStrangerCode is not a member")]
fn a_subset_code_outside_the_catalog_is_a_defect() {
    let _forms = class_call_forms(
        &subset_class(&[ErrorSlot::CodeSubset {
            subset: "ProbeStrangerCode",
        }]),
        &CODES,
        &sample_subsets(),
    );
}

#[test]
#[should_panic(expected = "the code subset ProbeWaitCode of ProbeSubsetClass is not a subset")]
fn a_subset_outside_the_module_is_a_defect() {
    let _forms = class_call_forms(
        &subset_class(&[ErrorSlot::CodeSubset {
            subset: "ProbeWaitCode",
        }]),
        &CODES,
        &[operation_subset()],
    );
}

#[test]
#[should_panic(expected = "the code GONE of ProbeGone is not a member of ProbeErrorCode")]
fn a_code_member_outside_the_catalog_is_a_defect() {
    let gone = ErrorClass {
        type_name: "ProbeGone",
        doc: "",
        slots: &[ErrorSlot::CodeMember { member: "GONE" }],
    };
    let _forms = class_call_forms(&gone, &CODES, &[]);
}

#[test]
#[should_panic(expected = "the class ProbeNoCode does not start with its code slot")]
fn a_class_without_its_code_slot_first_is_a_defect() {
    let no_code = ErrorClass {
        type_name: "ProbeNoCode",
        doc: "",
        slots: &[MESSAGE],
    };
    let _forms = class_call_forms(&no_code, &CODES, &[]);
}

#[test]
#[should_panic(expected = "the union ProbeStrangerError names ProbeStranger, which is not a class")]
fn a_union_member_outside_the_module_is_a_defect() {
    let stranger = ErrorUnion {
        module: "probe_errors",
        name: "ProbeStrangerError",
        doc: "",
        members: &["ProbeStranger"],
    };
    let rendered = [rendered_catalog(&CATALOG, &[operation_subset()])];
    let _union = error_union(&stranger, &rendered);
}

#[test]
#[should_panic(expected = "the class ProbeHolder holds TaskSendPayload, which is not a record")]
fn a_payload_class_outside_the_payload_module_is_a_defect() {
    let holder = ErrorClass {
        type_name: "ProbeHolder",
        doc: "",
        slots: &[
            ErrorSlot::CodeMember {
                member: "ENQUEUE_FAILED",
            },
            ErrorSlot::Field {
                name: "payload",
                slot_type: SlotType::TaskSendPayload,
                repr: SlotRepr::Hidden,
            },
        ],
    };
    assert_payload_records_in_module(&[&holder], &[]);
}

#[test]
fn a_class_without_instance_slots_has_no_slots_member() {
    let bare = ErrorClass {
        type_name: "ProbeBare",
        doc: "",
        slots: &[
            ErrorSlot::CodeMember {
                member: "PAYLOAD_MISMATCH",
            },
            ErrorSlot::NeverRetryable,
        ],
    };
    assert_eq!(
        native_class_interface(&bare),
        "export interface NativeProbeBare {\n  readonly class: \"ProbeBare\";\n}"
    );
}

/// A class whose state may clear has `retryable: true` in its public
/// interface and in its converter, and no `retryable` in its native slots.
#[test]
fn a_class_whose_state_may_clear_has_a_retryable_that_is_always_true() {
    let conflict = ErrorClass {
        type_name: "ProbeConflict",
        doc: "",
        slots: &[
            ErrorSlot::CodeMember {
                member: "PAYLOAD_MISMATCH",
            },
            ErrorSlot::AlwaysRetryable,
            MESSAGE,
        ],
    };
    assert_eq!(
        class_type(&conflict, &CODES, "Probe", &[]),
        "export interface ProbeConflict {\n  readonly kind: \"conflict\";\n  \
         readonly code: \"PAYLOAD_MISMATCH\";\n  \
         readonly retryable: true;\n  readonly message: string;\n}"
    );
    assert_eq!(
        native_class_interface(&conflict),
        "export interface NativeProbeConflict {\n  readonly class: \"ProbeConflict\";\n  \
         readonly slots: {\n    readonly message: string;\n  };\n}"
    );
    assert!(value_case(
        "\"ProbeConflict\"",
        "ProbeConflict",
        "conflict",
        &conflict,
        &CODES
    )
    .contains(
        "      const value: ProbeConflict = {\n        kind: \"conflict\",\n        \
             code: \"PAYLOAD_MISMATCH\",\n        \
             retryable: true,\n        message: raw.slots.message,\n      };\n"
    ));
}

#[test]
fn a_class_kind_is_its_name_without_the_family_prefix() {
    assert_eq!(family_prefix(&CATALOG), "Probe");
    assert_eq!(class_kind(&PAYLOAD_MISMATCH, "Probe"), "payloadMismatch");
}

#[test]
fn a_code_kind_is_the_code_in_camel_case() {
    assert_eq!(code_kind("DATABASE_PING_FAILED"), "databasePingFailed");
}

#[test]
fn the_family_prefix_is_every_word_that_the_class_names_share() {
    let catalog = ErrorCatalog {
        classes: &[
            ErrorClass {
                type_name: "WorkflowHandleNotFound",
                doc: "",
                slots: &[],
            },
            ErrorClass {
                type_name: "WorkflowHandleNotPaused",
                doc: "",
                slots: &[],
            },
            ErrorClass {
                type_name: "WorkflowHandleAsyncContext",
                doc: "",
                slots: &[],
            },
        ],
        ..CATALOG
    };
    assert_eq!(family_prefix(&catalog), "WorkflowHandle");
}

#[test]
#[should_panic(expected = "the catalog ProbeCoreError has fewer than two classes")]
fn a_catalog_with_one_class_has_no_family_prefix() {
    let catalog = ErrorCatalog {
        classes: &[ENQUEUE_FAILED],
        ..CATALOG
    };
    let _prefix = family_prefix(&catalog);
}

#[test]
#[should_panic(expected = "the class names of the catalog ProbeCoreError share no first word")]
fn a_catalog_whose_class_names_share_no_word_is_a_defect() {
    const STRANGER: ErrorClass = ErrorClass {
        type_name: "StrangerFailed",
        doc: "",
        slots: &[],
    };
    let catalog = ErrorCatalog {
        classes: &[ENQUEUE_FAILED, STRANGER],
        ..CATALOG
    };
    let _prefix = family_prefix(&catalog);
}

#[test]
#[should_panic(expected = "the class Probe is only the family prefix of its catalog")]
fn a_class_that_is_only_the_prefix_is_a_defect() {
    let bare = ErrorClass {
        type_name: "Probe",
        doc: "",
        slots: &[],
    };
    let _kind = class_kind(&bare, "Probe");
}

#[test]
fn a_value_without_hidden_slots_is_frozen_as_it_is() {
    assert_eq!(frozen_return(&[]), "      return Object.freeze(value);");
}

#[test]
fn a_module_without_imported_types_has_no_import_section() {
    let bare = ErrorClass {
        type_name: "ProbeBare",
        doc: "",
        slots: &[ErrorSlot::CodeMember {
            member: "PAYLOAD_MISMATCH",
        }],
    };
    assert_eq!(module_imports(&[&bare], &[]), None);
}

#[test]
fn catalogs_subsets_records_and_unions_group_by_module_in_name_order() {
    let other = ErrorCatalog {
        module: "another_errors",
        ..CATALOG
    };
    let modules = error_modules(&[CATALOG, other], vec![operation_subset()], &[], &[UNION]);
    let names: Vec<&str> = modules.keys().copied().collect();
    assert_eq!(names, ["another_errors", "probe_errors"]);
    let probe = &modules["probe_errors"];
    assert_eq!(probe.catalogs, [CATALOG]);
    assert_eq!(probe.subsets, [operation_subset()]);
    assert_eq!(probe.unions, [UNION]);
    assert!(modules["another_errors"].subsets.is_empty());
}

const RECORD: RecordClass = RecordClass {
    module: "probe_errors",
    type_name: "ProbePayload",
    doc: "",
    fields: &[
        record_field("task_name", RecordFieldType::Str),
        record_field("priority", RecordFieldType::Int),
        record_field("sent_at", RecordFieldType::DateTime),
        record_field("input_json", RecordFieldType::OptionalStr),
        record_field("delay_seconds", RecordFieldType::OptionalInt),
        record_field("good_until", RecordFieldType::OptionalDateTime),
        record_field(
            "retention_class_key",
            RecordFieldType::OptionalRetentionClassKey,
        ),
    ],
};

const fn record_field(name: &'static str, field_type: RecordFieldType) -> RecordField {
    RecordField {
        name,
        field_type,
        default: FieldDefault::Required,
        note: "",
    }
}

#[test]
fn a_record_has_its_public_interface_in_camel_case() {
    assert_eq!(
        record_interface(&RECORD),
        "export interface ProbePayload {\n  \
         readonly taskName: string;\n  \
         readonly priority: number;\n  \
         readonly sentAt: Temporal.Instant;\n  \
         readonly inputJson: string | null;\n  \
         readonly delaySeconds: number | null;\n  \
         readonly goodUntil: Temporal.Instant | null;\n  \
         readonly retentionClassKey: RetentionClassKey | null;\n\
         }"
    );
}

#[test]
fn a_native_record_has_its_core_names_and_instants_in_microseconds() {
    assert_eq!(
        native_record_interface(&RECORD),
        "export interface NativeProbePayload {\n  \
         readonly task_name: string;\n  \
         readonly priority: number;\n  \
         readonly sent_at: bigint;\n  \
         readonly input_json: string | null;\n  \
         readonly delay_seconds: number | null;\n  \
         readonly good_until: bigint | null;\n  \
         readonly retention_class_key: RetentionClassKey | null;\n\
         }"
    );
}

#[test]
fn a_record_converter_converts_each_field_and_freezes_the_value() {
    assert_eq!(
        record_converter(&RECORD),
        "export function probePayloadFromNative(raw: NativeProbePayload): ProbePayload {\n  \
         const value: ProbePayload = {\n    \
         taskName: raw.task_name,\n    \
         priority: raw.priority,\n    \
         sentAt: instantOfMicroseconds(raw.sent_at),\n    \
         inputJson: raw.input_json,\n    \
         delaySeconds: raw.delay_seconds,\n    \
         goodUntil: optionalInstantOfMicroseconds(raw.good_until),\n    \
         retentionClassKey: raw.retention_class_key,\n  \
         };\n\
         \n  \
         return Object.freeze(value);\n\
         }"
    );
}

#[test]
fn an_optional_instant_field_needs_both_instant_helpers() {
    assert_eq!(
        instant_helper_sections(&[RECORD]),
        [
            "const NANOSECONDS_PER_MICROSECOND = 1000n;",
            "function instantOfMicroseconds(microseconds: bigint): Temporal.Instant {\n  \
             return Temporal.Instant.fromEpochNanoseconds(microseconds * \
             NANOSECONDS_PER_MICROSECOND);\n}",
            "function optionalInstantOfMicroseconds(microseconds: bigint | null): \
             Temporal.Instant | null {\n  \
             if (microseconds === null) {\n    return null;\n  }\n\n  \
             return instantOfMicroseconds(microseconds);\n}",
        ]
    );
}

#[test]
fn a_required_instant_field_needs_only_the_instant_helper() {
    const SENT_AT: &[RecordField] = &[record_field("sent_at", RecordFieldType::DateTime)];
    let required = RecordClass {
        fields: SENT_AT,
        ..RECORD
    };
    let sections = instant_helper_sections(&[required]);
    assert_eq!(sections.len(), 2);
    assert!(sections[1].starts_with("function instantOfMicroseconds("));
}

#[test]
fn a_record_without_instants_needs_no_instant_helper() {
    const TASK_NAME: &[RecordField] = &[record_field("task_name", RecordFieldType::Str)];
    let plain = RecordClass {
        fields: TASK_NAME,
        ..RECORD
    };
    assert!(instant_helper_sections(&[plain]).is_empty());
}

/// The classes of the model that the renderer leaves out: exactly the four
/// `AsyncContext` classes, the classes whose code member only a blocking
/// form gives.
#[test]
fn the_model_leaves_out_the_four_async_context_classes_and_no_other() {
    let subsets = code_subsets();
    let mut left_out: Vec<&str> = Vec::new();
    for catalog in ERROR_CATALOGS {
        let rendered = rendered_catalog(catalog, &subsets);
        for class in catalog.classes {
            if !rendered.classes.contains(&class) {
                left_out.push(error_class_type_name(class));
            }
        }
    }
    assert_eq!(
        left_out,
        [
            "DatabaseAsyncContext",
            "WorkflowHandleAsyncContext",
            "TaskSendAsyncContext",
            "WorkflowStartAsyncContext",
        ]
    );
}

/// No rendered module names a left-out class; every other class has its
/// interface, its native interface and its `case`.
#[test]
fn the_model_s_modules_convert_every_rendered_class_and_name_no_other() {
    let texts: Vec<String> = generated_files()
        .iter()
        .map(|file| file.text().to_owned())
        .collect();
    let all_text = texts.concat();
    let subsets = code_subsets();
    let mut rendered_classes = 0;
    for catalog in ERROR_CATALOGS {
        let rendered = rendered_catalog(catalog, &subsets);
        for class in catalog.classes {
            let name = error_class_type_name(class);
            let marks = match class_code_subset(class) {
                Some(_) => [
                    format!("export type {name}<"),
                    format!("export interface Native{name} {{\n"),
                    format!("    case \"{name}\":\n      return "),
                ],
                None => [
                    format!("export interface {name} {{\n"),
                    format!("export interface Native{name} {{\n"),
                    format!("    case \"{name}\": {{\n"),
                ],
            };
            let kept = rendered.classes.contains(&class);
            for mark in &marks {
                assert_eq!(all_text.contains(mark.as_str()), kept, "{name}: {mark}");
            }
            assert_eq!(all_text.contains(&format!("\"{name}\"")), kept, "{name}");
            if kept {
                rendered_classes += 1;
            }
        }
    }
    assert_eq!(rendered_classes, 31);
}

#[test]
fn the_model_renders_the_five_error_modules_in_name_order() {
    let paths: Vec<String> = generated_files()
        .iter()
        .map(|file| file.relative_path().display().to_string())
        .collect();
    let expected: Vec<String> = [
        "broker_errors",
        "monitoring_errors",
        "task_send_errors",
        "workflow_handle_errors",
        "workflow_start_errors",
    ]
    .iter()
    .map(|stem| format!("src/_catalog/{stem}.ts"))
    .collect();
    assert_eq!(paths, expected);
}

/// The module text of `stem` in the model's output.
fn model_module(stem: &str) -> String {
    generated_files()
        .into_iter()
        .find(|file| file.relative_path().ends_with(format!("{stem}.ts")))
        .unwrap_or_else(|| panic!("{stem} is rendered"))
        .text()
        .to_owned()
}

#[test]
fn the_model_s_send_module_has_the_payload_record_and_its_converter() {
    let text = model_module("task_send_errors");
    assert!(text.starts_with(&format!(
        "{GENERATED_HEADER}\n\nimport type {{ CoreCause, NoCause }} from \"./error_detail.ts\";\n\
         import type {{ RetentionClassKey }} from \"./retention_class.ts\";\n\n\
         export interface TaskSendPayload {{\n"
    )));
    assert!(text.contains("\n    sentAt: instantOfMicroseconds(raw.sent_at),\n"));
    assert!(text.contains("\n    goodUntil: optionalInstantOfMicroseconds(raw.good_until),\n"));
    assert!(text.contains("\n        payload: taskSendPayloadFromNative(raw.slots.payload),\n"));
    assert!(text.contains("\n  readonly sizeBytes: bigint;\n  readonly limitBytes: bigint;\n"));
    assert!(text.contains(
        "\nexport interface TaskSendIdempotencyKeyConflict {\n  \
         readonly kind: \"idempotencyKeyConflict\";\n  \
         readonly code: \"IDEMPOTENCY_KEY_CONFLICT\";\n  readonly retryable: false;\n  \
         readonly taskId: string;\n"
    ));
    assert!(text.contains(
        "\n        kind: \"idempotencyKeyConflict\",\n        \
                       code: \"IDEMPOTENCY_KEY_CONFLICT\",\n        retryable: false,\n"
    ));
    assert!(text.contains(
        "\nexport function taskSendCoreErrorFromNative(raw: NativeTaskSendCoreError): \
         TaskSendCoreError {\n"
    ));
}

#[test]
fn the_model_s_monitoring_module_has_two_converters_and_imports_the_status() {
    let text = model_module("monitoring_errors");
    assert!(text.contains(
        "\nimport type { CoreCause } from \"./error_detail.ts\";\n\
         import type { TaskStatus } from \"./status.ts\";\n"
    ));
    assert!(text.contains("\n  readonly currentStatus: TaskStatus;\n"));
    assert!(text.contains("\nexport function monitoringQueryCoreErrorFromNative("));
    assert!(text.contains("\nexport function taskActionCoreErrorFromNative("));
}

#[test]
fn the_model_s_broker_module_has_the_subset_and_both_unions() {
    let text = model_module("broker_errors");
    assert!(text.contains(
        "\nexport type DatabaseOperationFailedCode =\n  | \"SCHEMA_INIT_FAILED\"\n  \
         | \"TASK_INFO_QUERY_FAILED\"\n"
    ));
    assert!(text.contains("\n  readonly DATABASE_PING_FAILED: \"databasePingFailed\";\n"));
    assert!(text.contains(
        "\nexport type DatabaseOperationFailed<Code extends DatabaseOperationFailedCode = \
         DatabaseOperationFailedCode> = {\n"
    ));
    assert!(text.contains(
        "\nexport type DatabaseCoreEnqueueError =\n  | DatabaseEnqueueFailed\n  \
         | DatabasePayloadMismatch\n  | DatabaseIdempotencyKeyConflict\n  \
         | DatabaseInvalidTaskOptions\n  | DatabaseInvalidInput;\n"
    ));
}
