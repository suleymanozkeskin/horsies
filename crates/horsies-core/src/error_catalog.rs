//! The error classes of the core's catalogs, as static data for clients.
//!
//! An error enum of the core has one variant per Python class that the core
//! produces. `#[derive(horsies_core_derive::ErrorCatalog)]` records, per
//! variant, the Python class: its name, its doc (the variant's `///` lines),
//! and its slots in order. `horsies-codegen` renders the frozen dataclasses
//! and the union of each catalog from [`ERROR_CATALOGS`], so a class is
//! named once, here. The classes that only a Python client produces stay in
//! Python; each public union of Python is a generated union plus them.
//!
//! The slots of a class, in order:
//!
//! 1. `code`: a `ClassVar` member of the code catalog, or an instance field
//!    typed by a `Literal` subset of it ([`CodeSubset`]);
//! 2. `retryable: ClassVar[bool] = False` for a class that never clears;
//! 3. the fields of the variant, in declaration order, each with its
//!    Python type and whether `repr` shows it;
//! 4. `exception`, the core's exception class of the cause, for a class
//!    that has one.
//!
//! Adding an error enum: derive `ErrorCatalog` on it, add it to
//! [`ERROR_CATALOGS`], and name its per-operation unions in
//! [`ERROR_UNIONS`].
//!
//! A field type of a class that is a record of its own (the payload of a
//! send) is a [`RecordClass`] in [`ERROR_RECORDS`], and a `NewType` that a
//! record field holds is a [`TextNewType`] in [`TEXT_NEW_TYPES`]. They are
//! written by hand here, in Python's shape: the core's own structs hold the
//! same facts in Rust types.

use crate::catalog::{CodeCatalog, CodeEnum};
use crate::core::history::ddl::classes::DEFAULT_RETENTION_CLASS_KEY;
use crate::core::task::send_types::TaskSendError;
use crate::monitoring::{MonitoringQueryError, TaskActionError};
use crate::{BrokerOperationError, BrokerOperationFailedCode, HandleOperationError};

/// The Python type of an instance field of a generated class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotType {
    /// `str`.
    Str,
    /// `bool`.
    Bool,
    /// `int`.
    Int,
    /// `uuid.UUID`.
    Uuid,
    /// `str` holding the text of a UUID: a workflow id of the Python client.
    UuidText,
    /// `TaskStatus`, the code catalog of `horsies._catalog.status`.
    TaskStatus,
    /// `TaskSendPayload`, the record [`TASK_SEND_PAYLOAD`] in the module of
    /// the class.
    TaskSendPayload,
}

/// Whether the generated `repr` of a class shows a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRepr {
    Shown,
    /// `field(repr=False)`: a message or a cause, which can be long or hold
    /// data of the caller.
    Hidden,
}

/// The Python type of the `exception` slot. The runtime class is the core's
/// exception class of the cause (`horsies._core.CoreFailure` and its
/// subclasses); the declared type is Python's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceptionType {
    /// `BaseException`.
    Exception,
    /// `BaseException | NoCause`.
    ExceptionOrNoCause,
}

/// One slot of a generated class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSlot {
    /// `code: ClassVar[<CodeEnum>] = <CodeEnum>.<member>`.
    CodeMember { member: &'static str },
    /// `code: <subset>`: an instance field typed by a `Literal` subset of
    /// the code catalog, named by [`CodeSubset::name`].
    CodeSubset { subset: &'static str },
    /// `retryable: ClassVar[bool] = False`: the state never clears.
    NeverRetryable,
    /// An instance field of the variant.
    Field {
        name: &'static str,
        python_type: SlotType,
        repr: SlotRepr,
    },
    /// `exception: <type> = field(repr=False)`, built from the cause of the
    /// variant (`failure_cause()`).
    Exception { python_type: ExceptionType },
}

/// The Python class of one variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorClass {
    pub python_class: &'static str,
    /// The class doc, from the variant's `///` lines.
    pub doc: &'static str,
    /// The slots in their Python order.
    pub slots: &'static [ErrorSlot],
}

/// The classes of one error enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorCatalog {
    /// The module under `horsies._catalog` that holds the classes.
    pub python_module: &'static str,
    /// The code catalog of the `code` slots.
    pub code_catalog: CodeCatalog,
    /// The name of the union of every class of the enum.
    pub union: &'static str,
    pub union_doc: &'static str,
    /// The classes in variant order.
    pub classes: &'static [ErrorClass],
}

/// An error enum whose classes the derive recorded.
pub trait ErrorEnum: 'static {
    /// The code catalog of the enum's `code`.
    type Code: CodeEnum;
    /// The classes of the enum.
    const ERROR_CATALOG: ErrorCatalog;
    /// The catalog code of this value.
    fn error_code(&self) -> Self::Code;
    /// The Python class of this value's variant.
    fn python_class(&self) -> &'static str;
}

/// A union of some classes of one catalog: the failures of one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorUnion {
    /// The module of the catalog of its classes.
    pub python_module: &'static str,
    pub name: &'static str,
    pub doc: &'static str,
    /// The class names, each a class of the catalog.
    pub members: &'static [&'static str],
}

/// A `Literal` subset of a code catalog: the type of a `code` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeSubset {
    /// The module of the catalog that uses it.
    pub python_module: &'static str,
    pub name: &'static str,
    pub doc: &'static str,
    /// The code catalog of the members.
    pub code_catalog: CodeCatalog,
    /// The wire values of the members, in their order.
    pub members: Vec<&'static str>,
}

/// Every error catalog that the Python package gets, in render order.
pub const ERROR_CATALOGS: &[ErrorCatalog] = &[
    <BrokerOperationError as ErrorEnum>::ERROR_CATALOG,
    <HandleOperationError as ErrorEnum>::ERROR_CATALOG,
    <MonitoringQueryError as ErrorEnum>::ERROR_CATALOG,
    <TaskActionError as ErrorEnum>::ERROR_CATALOG,
    <TaskSendError as ErrorEnum>::ERROR_CATALOG,
];

/// The Python type of a field of a generated record class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFieldType {
    /// `str`.
    Str,
    /// `int`.
    Int,
    /// `datetime`.
    DateTime,
    /// `str | None`.
    OptionalStr,
    /// `int | None`.
    OptionalInt,
    /// `datetime | None`.
    OptionalDateTime,
    /// `RetentionClassKey | None`, the [`RETENTION_CLASS_KEY`] new type.
    OptionalRetentionClassKey,
}

/// The default of a field of a generated record class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldDefault {
    /// No default: the caller gives the value.
    Required,
    /// `None`.
    NoneValue,
    /// [`DEFAULT_RETENTION_CLASS`], a constant of [`RETENTION_CLASS_KEY`].
    DefaultRetentionClassKey,
}

/// One field of a generated record class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordField {
    pub name: &'static str,
    pub python_type: RecordFieldType,
    pub default: FieldDefault,
    /// Comment lines above the field; empty for none.
    pub note: &'static str,
}

/// A frozen dataclass that a field of an error class holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordClass {
    /// The module under `horsies._catalog` that holds the record.
    pub python_module: &'static str,
    pub python_class: &'static str,
    pub doc: &'static str,
    /// The fields in their Python order: each field with a default comes
    /// after every field without one.
    pub fields: &'static [RecordField],
}

/// A named value of a [`TextNewType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewTypeConstant {
    pub name: &'static str,
    pub doc: &'static str,
    pub value: &'static str,
}

/// A `NewType` of `str`, with its named values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextNewType {
    /// The module under `horsies._catalog` that holds it.
    pub python_module: &'static str,
    pub name: &'static str,
    pub doc: &'static str,
    pub constants: &'static [NewTypeConstant],
}

/// The key of a retention class, and the key of the default class.
pub const RETENTION_CLASS_KEY: TextNewType = TextNewType {
    python_module: "retention_class",
    name: "RetentionClassKey",
    doc: "The key of a finite period class. ``period_class_key`` of\n\
          ``horsies.core.history.periods`` builds it.\n\
          \n\
          The enqueue paths take this type, so a raw string key is a type error at\n\
          a typed call site, not a row that has no partition later. Like\n\
          ``RetentionDays``, it is static only: ``RetentionClassKey('x')`` runs.",
    constants: &[DEFAULT_RETENTION_CLASS],
};

/// The key of the class that an enqueue stamps when the caller names none.
pub const DEFAULT_RETENTION_CLASS: NewTypeConstant = NewTypeConstant {
    name: "DEFAULT_RETENTION_CLASS_KEY",
    doc: "The class every enqueue stamps unless the caller says otherwise:\n\
          ``standard_30d``, an immutable 30-day finite class.\n\
          \n\
          Forever is chosen only by an explicit ``None``: a forever default would\n\
          exempt every unconfigured task from retention.",
    value: DEFAULT_RETENTION_CLASS_KEY,
};

/// Every `NewType` that the Python package gets, in render order.
pub const TEXT_NEW_TYPES: &[TextNewType] = &[RETENTION_CLASS_KEY];

/// The payload of a send that the retry methods send again (Python
/// `TaskSendPayload`). The core's [`crate::core::task::send_types::TaskSendPayload`]
/// holds the same facts with a resolved retention.
pub const TASK_SEND_PAYLOAD: RecordClass = RecordClass {
    python_module: <TaskSendError as ErrorEnum>::ERROR_CATALOG.python_module,
    python_class: "TaskSendPayload",
    doc: "Serialized envelope for idempotent retry.\n\
          \n\
          All value fields are pre-serialized to avoid round-trip issues.\n\
          The retry methods extract this payload directly — no user args needed.",
    fields: &[
        RecordField {
            name: "task_name",
            python_type: RecordFieldType::Str,
            default: FieldDefault::Required,
            note: "",
        },
        RecordField {
            name: "queue_name",
            python_type: RecordFieldType::Str,
            default: FieldDefault::Required,
            note: "",
        },
        RecordField {
            name: "priority",
            python_type: RecordFieldType::Int,
            default: FieldDefault::Required,
            note: "",
        },
        RecordField {
            name: "input_json",
            python_type: RecordFieldType::OptionalStr,
            default: FieldDefault::Required,
            note: "The task input as JSON text: the keyword arguments as one object, or\n\
                   None for a send with none (stored as ``{}``).",
        },
        RecordField {
            name: "sent_at",
            python_type: RecordFieldType::DateTime,
            default: FieldDefault::Required,
            note: "",
        },
        RecordField {
            name: "good_until",
            python_type: RecordFieldType::OptionalDateTime,
            default: FieldDefault::Required,
            note: "",
        },
        RecordField {
            name: "enqueue_delay_seconds",
            python_type: RecordFieldType::OptionalInt,
            default: FieldDefault::Required,
            note: "",
        },
        RecordField {
            name: "task_options",
            python_type: RecordFieldType::OptionalStr,
            default: FieldDefault::Required,
            note: "",
        },
        RecordField {
            name: "idempotency_key",
            python_type: RecordFieldType::OptionalStr,
            default: FieldDefault::NoneValue,
            note: "The caller's idempotency key, distinct from task identity. Riding the\n\
                   payload keeps the uncertain-commit resend keyed: a replayed send\n\
                   claims with the same key it first sent.",
        },
        RecordField {
            name: "retention_class_key",
            python_type: RecordFieldType::OptionalRetentionClassKey,
            default: FieldDefault::DefaultRetentionClassKey,
            note: "The retention class snapshotted at enqueue. ``None`` means forever,\n\
                   so the field defaults to the immutable 30-day class rather than to\n\
                   ``None``: a payload that never had the field set must not read as\n\
                   a forever request on replay.",
        },
    ],
};

/// Every record class that the Python package gets, in render order.
pub const ERROR_RECORDS: &[RecordClass] = &[TASK_SEND_PAYLOAD];

/// The unions of the failures of one operation that the core produces, in
/// render order. Python's public union of the operation adds its
/// client-only classes.
pub const ERROR_UNIONS: &[ErrorUnion] = &[ErrorUnion {
    python_module: <BrokerOperationError as ErrorEnum>::ERROR_CATALOG.python_module,
    name: "BrokerCoreEnqueueError",
    doc: "Every failure of an enqueue that the core gives.",
    members: &[
        "BrokerEnqueueFailed",
        "BrokerPayloadMismatch",
        "BrokerIdempotencyKeyConflict",
        "BrokerInvalidTaskOptions",
        "BrokerInvalidInput",
    ],
}];

/// The `Literal` subsets that the `code` field of a class holds.
pub fn code_subsets() -> Vec<CodeSubset> {
    vec![CodeSubset {
        python_module: <BrokerOperationError as ErrorEnum>::ERROR_CATALOG.python_module,
        name: "BrokerOperationFailedCode",
        doc: "The operation codes of ``BrokerOperationFailed``.",
        code_catalog: <crate::BrokerErrorCode as CodeEnum>::CATALOG,
        members: BrokerOperationFailedCode::ALL
            .iter()
            .map(|code| code.broker_code().wire_value())
            .collect(),
    }]
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// Every class name, across the catalogs.
    fn class_names() -> Vec<&'static str> {
        ERROR_CATALOGS
            .iter()
            .flat_map(|catalog| catalog.classes.iter().map(|class| class.python_class))
            .collect()
    }

    #[test]
    fn each_class_is_named_once() {
        let names = class_names();
        let unique: HashSet<&str> = names.iter().copied().collect();
        assert_eq!(unique.len(), names.len(), "{names:?}");
    }

    #[test]
    fn each_class_code_is_a_member_of_its_catalog() {
        for catalog in ERROR_CATALOGS {
            let members: HashSet<&str> = catalog
                .code_catalog
                .members
                .iter()
                .map(|member| member.value)
                .collect();
            for class in catalog.classes {
                match class.slots.first() {
                    Some(ErrorSlot::CodeMember { member }) => {
                        assert!(members.contains(member), "{}: {member}", class.python_class);
                    }
                    Some(ErrorSlot::CodeSubset { subset }) => {
                        assert!(
                            code_subsets().iter().any(|known| known.name == *subset),
                            "{}: {subset}",
                            class.python_class
                        );
                    }
                    other => panic!("{} starts with {other:?}, not its code", class.python_class),
                }
            }
        }
    }

    #[test]
    fn the_cause_is_the_last_slot() {
        for catalog in ERROR_CATALOGS {
            for class in catalog.classes {
                let exceptions: Vec<usize> = class
                    .slots
                    .iter()
                    .enumerate()
                    .filter(|(_, slot)| matches!(slot, ErrorSlot::Exception { .. }))
                    .map(|(position, _)| position)
                    .collect();
                match exceptions.as_slice() {
                    [] => {}
                    [position] => {
                        assert_eq!(*position, class.slots.len() - 1, "{}", class.python_class)
                    }
                    more => panic!("{} has {} exception slots", class.python_class, more.len()),
                }
            }
        }
    }

    #[test]
    fn each_union_member_is_a_class_of_its_catalog() {
        for union in ERROR_UNIONS {
            let catalog = ERROR_CATALOGS
                .iter()
                .find(|catalog| catalog.python_module == union.python_module)
                .unwrap_or_else(|| panic!("{}: no catalog in {}", union.name, union.python_module));
            for member in union.members {
                assert!(
                    catalog
                        .classes
                        .iter()
                        .any(|class| class.python_class == *member),
                    "{}: {member}",
                    union.name
                );
            }
        }
    }

    #[test]
    fn the_operation_codes_are_python_s_subset_in_python_s_order() {
        let [subset] = code_subsets()
            .try_into()
            .unwrap_or_else(|subsets: Vec<CodeSubset>| panic!("one subset, got {}", subsets.len()));
        assert_eq!(
            subset.members,
            [
                "SCHEMA_INIT_FAILED",
                "TASK_INFO_QUERY_FAILED",
                "MONITORING_QUERY_FAILED",
                "DB_PING_FAILED",
                "WORKER_PING_FAILED",
                "CLEANUP_FAILED",
                "CLOSE_FAILED",
                "LISTENER_START_FAILED",
                "LISTENER_SUBSCRIBE_FAILED",
            ]
        );
    }

    /// The slots of two broker classes, as Python declares them
    /// (`python/horsies/core/brokers/result_types.py`).
    #[test]
    fn a_class_has_python_s_slots_in_python_s_order() {
        let classes = <BrokerOperationError as ErrorEnum>::ERROR_CATALOG.classes;
        let class = |name: &str| {
            classes
                .iter()
                .find(|class| class.python_class == name)
                .unwrap_or_else(|| panic!("no class {name}"))
        };
        let message = ErrorSlot::Field {
            name: "message",
            python_type: SlotType::Str,
            repr: SlotRepr::Hidden,
        };
        assert_eq!(
            class("BrokerOperationFailed").slots,
            [
                ErrorSlot::CodeSubset {
                    subset: "BrokerOperationFailedCode"
                },
                message,
                ErrorSlot::Field {
                    name: "retryable",
                    python_type: SlotType::Bool,
                    repr: SlotRepr::Shown,
                },
                ErrorSlot::Exception {
                    python_type: ExceptionType::Exception
                },
            ]
        );
        assert_eq!(
            class("BrokerIdempotencyKeyConflict").slots,
            [
                ErrorSlot::CodeMember {
                    member: "IDEMPOTENCY_KEY_CONFLICT"
                },
                ErrorSlot::NeverRetryable,
                message,
                ErrorSlot::Field {
                    name: "owner_task_id",
                    python_type: SlotType::Uuid,
                    repr: SlotRepr::Shown,
                },
            ]
        );
        assert!(class("BrokerOperationFailed")
            .doc
            .starts_with("A broker operation failed with a database, driver or connection error."));
    }

    /// The slots of two send classes, as Python declares them
    /// (`python/horsies/core/models/task_send_types.py`).
    #[test]
    fn a_send_class_has_python_s_slots_in_python_s_order() {
        let classes = <TaskSendError as ErrorEnum>::ERROR_CATALOG.classes;
        let class = |name: &str| {
            classes
                .iter()
                .find(|class| class.python_class == name)
                .unwrap_or_else(|| panic!("no class {name}"))
        };
        let message = ErrorSlot::Field {
            name: "message",
            python_type: SlotType::Str,
            repr: SlotRepr::Hidden,
        };
        assert_eq!(
            class("TaskSendEnqueueFailed").slots,
            [
                ErrorSlot::CodeMember {
                    member: "ENQUEUE_FAILED"
                },
                ErrorSlot::Field {
                    name: "task_id",
                    python_type: SlotType::Uuid,
                    repr: SlotRepr::Shown,
                },
                ErrorSlot::Field {
                    name: "payload",
                    python_type: SlotType::TaskSendPayload,
                    repr: SlotRepr::Hidden,
                },
                message,
                ErrorSlot::Field {
                    name: "retryable",
                    python_type: SlotType::Bool,
                    repr: SlotRepr::Shown,
                },
                ErrorSlot::Exception {
                    python_type: ExceptionType::ExceptionOrNoCause
                },
            ]
        );
        assert_eq!(
            class("TaskSendPayloadTooLarge").slots,
            [
                ErrorSlot::CodeMember {
                    member: "PAYLOAD_TOO_LARGE"
                },
                message,
                ErrorSlot::Field {
                    name: "size_bytes",
                    python_type: SlotType::Int,
                    repr: SlotRepr::Shown,
                },
                ErrorSlot::Field {
                    name: "limit_bytes",
                    python_type: SlotType::Int,
                    repr: SlotRepr::Shown,
                },
            ]
        );
        let names: Vec<&str> = classes.iter().map(|class| class.python_class).collect();
        assert_eq!(
            names,
            [
                "TaskSendSuppressed",
                "TaskSendAsyncContext",
                "TaskSendValidationFailed",
                "TaskSendEnqueueFailed",
                "TaskSendPayloadMismatch",
                "TaskSendPayloadTooLarge",
                "TaskSendIdempotencyKeyConflict",
            ]
        );
    }

    /// A dataclass refuses a field without a default after one with a
    /// default.
    #[test]
    fn a_record_field_with_a_default_follows_every_required_one() {
        for record in ERROR_RECORDS {
            let first_default = record
                .fields
                .iter()
                .position(|field| field.default != FieldDefault::Required)
                .unwrap_or(record.fields.len());
            assert!(
                record.fields[first_default..]
                    .iter()
                    .all(|field| field.default != FieldDefault::Required),
                "{}",
                record.python_class
            );
        }
    }

    /// A class that holds the payload record is in the record's module, so
    /// the module defines the record and imports nothing for it.
    #[test]
    fn the_payload_slot_is_in_the_module_of_the_payload_record() {
        for catalog in ERROR_CATALOGS {
            let holds_payload = catalog.classes.iter().any(|class| {
                class.slots.iter().any(|slot| {
                    matches!(
                        slot,
                        ErrorSlot::Field {
                            python_type: SlotType::TaskSendPayload,
                            ..
                        }
                    )
                })
            });
            match holds_payload {
                true => assert_eq!(catalog.python_module, TASK_SEND_PAYLOAD.python_module),
                false => {}
            }
        }
    }

    /// Python's payload fields, in Python's order, with the default of the
    /// two that have one.
    #[test]
    fn the_payload_record_has_python_s_fields() {
        let fields: Vec<(&str, RecordFieldType, FieldDefault)> = TASK_SEND_PAYLOAD
            .fields
            .iter()
            .map(|field| (field.name, field.python_type, field.default))
            .collect();
        assert_eq!(
            fields,
            [
                ("task_name", RecordFieldType::Str, FieldDefault::Required),
                ("queue_name", RecordFieldType::Str, FieldDefault::Required),
                ("priority", RecordFieldType::Int, FieldDefault::Required),
                (
                    "input_json",
                    RecordFieldType::OptionalStr,
                    FieldDefault::Required
                ),
                ("sent_at", RecordFieldType::DateTime, FieldDefault::Required),
                (
                    "good_until",
                    RecordFieldType::OptionalDateTime,
                    FieldDefault::Required
                ),
                (
                    "enqueue_delay_seconds",
                    RecordFieldType::OptionalInt,
                    FieldDefault::Required
                ),
                (
                    "task_options",
                    RecordFieldType::OptionalStr,
                    FieldDefault::Required
                ),
                (
                    "idempotency_key",
                    RecordFieldType::OptionalStr,
                    FieldDefault::NoneValue
                ),
                (
                    "retention_class_key",
                    RecordFieldType::OptionalRetentionClassKey,
                    FieldDefault::DefaultRetentionClassKey
                ),
            ]
        );
    }

    /// The derive's class of a value is the class at the index of its
    /// variant, and its code is the class's code.
    #[test]
    fn a_value_names_its_class_and_its_code() {
        let failed = BrokerOperationError::OperationFailed {
            code: BrokerOperationFailedCode::CloseFailed,
            message: "close failed".to_owned(),
            retryable: true,
            cause: crate::FailureCause::Connection,
        };
        assert_eq!(failed.python_class(), "BrokerOperationFailed");
        assert_eq!(failed.error_code(), crate::BrokerErrorCode::CloseFailed);
        let conflict = BrokerOperationError::IdempotencyKeyConflict {
            message: "held".to_owned(),
            owner_task_id: uuid::Uuid::nil(),
        };
        assert_eq!(conflict.python_class(), "BrokerIdempotencyKeyConflict");
        assert_eq!(
            conflict.error_code(),
            crate::BrokerErrorCode::IdempotencyKeyConflict
        );
        let refused = TaskActionError::NotCancellable {
            message: "refused".to_owned(),
            task_id: uuid::Uuid::nil(),
            current_status: crate::TaskStatus::Completed,
        };
        assert_eq!(refused.python_class(), "TaskActionNotCancellable");
        assert_eq!(
            refused.error_code(),
            crate::monitoring::TaskActionErrorCode::TaskNotCancellable
        );
    }
}
