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
//! 2. `retryable: ClassVar[bool] = False` for a class whose call a repeat
//!    does not complete, or `retryable: ClassVar[bool] = True` for a class
//!    whose same call, unchanged, can succeed later;
//! 3. the fields of the variant, in declaration order, each with its
//!    Python type and whether `repr` shows it;
//! 4. `cause`, the core's exception class of the cause, for a class
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
//!
//! A client builds the class of a value from [`ErrorClassValues`]
//! (`#[derive(horsies_core_derive::ErrorClassValues)]`): the values of the
//! instance slots, by name, in the class's order. The `cause` slot is
//! built from the variant's typed cause (`#[error_slot(cause)]`, an
//! [`ExceptionCause`]), so the exception keeps the cause's own text. A
//! refusal whose Python exception is a class that the client defines (a
//! `ValueError` of a rule, a `SerializationError` of a JSON text) names
//! that class ([`ClientExceptionClass`]).
//!
//! [`EXCEPTION_CLASSES`] are the exception classes that the core defines
//! for its clients (for example `OutcomeDecodeError`): `Exception`
//! subclasses whose one argument is their message.

use uuid::Uuid;

use crate::broker::{BrokerCheckCause, BrokerError, BrokerOperationSource, BrokerRefusalCause};
use crate::catalog::{CodeCatalog, CodeEnum};
use crate::core::history::ddl::classes::DEFAULT_RETENTION_CLASS_KEY;
use crate::core::lifecycle::OutcomeDecodeError;
use crate::core::task::send_types::{TaskSendError, TaskSendPayload};
use crate::core::workflow::start_types::{StartRefusalCause, WorkflowStartError};
use crate::failure_cause::FailureCause;
use crate::monitoring::{
    HistoryContractSource, MonitoringQueryError, MonitoringQueryErrorSource, TaskActionError,
};
use crate::workflow_engine::error::WorkflowError;
use crate::{DatabaseOperationError, DatabaseOperationFailedCode, WorkflowHandleError};

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

/// The Python type of the `cause` slot. The runtime class is the class
/// that the cause names ([`ExceptionValue`]): the core's exception class of
/// the cause (`horsies._core.CoreFailure` and its subclasses), a generated
/// exception class, or a class that the client defines; the declared type
/// is Python's.
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
    /// `retryable: ClassVar[bool] = False`: a repeat of the same call does
    /// not complete it. The caller must change the call.
    NeverRetryable,
    /// `retryable: ClassVar[bool] = True`: a repeat of the same call,
    /// unchanged, can succeed later. It does not say that a repeat is safe.
    AlwaysRetryable,
    /// An instance field of the variant.
    Field {
        name: &'static str,
        python_type: SlotType,
        repr: SlotRepr,
    },
    /// `cause: <type> = field(repr=False)`, built from the cause of the
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

/// An error enum whose instance slot values the derive generates
/// (`#[derive(horsies_core_derive::ErrorClassValues)]`). Each variant with
/// a `cause` slot carries its typed cause.
pub trait ErrorClassValues: ErrorEnum {
    /// The values of the instance slots of this value's class
    /// ([`ErrorEnum::python_class`]), by name, in the class's slot order:
    /// the `code` field of a `Literal` subset, the fields, then
    /// `cause`. The class values (`code` of one member, the fixed
    /// `retryable` of a class) are not instance slots. Pure.
    fn slot_values(&self) -> Vec<SlotValue<'_>>;
}

/// The value of one instance slot of a generated class, with its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotValue<'a> {
    pub name: &'static str,
    pub value: SlotData<'a>,
}

/// The value of an instance slot, by the Python type of the slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotData<'a> {
    /// `str`.
    Str(&'a str),
    /// `bool`.
    Bool(bool),
    /// `int`: any integer field of 64 bits or less, widened.
    Int(i128),
    /// `TaskSendPayload`, the record [`TASK_SEND_PAYLOAD`]: a client builds
    /// it from [`TaskSendPayload::record_values`].
    TaskSendPayload(&'a TaskSendPayload),
    /// `uuid.UUID`.
    Uuid(Uuid),
    /// `str` holding the text of the UUID.
    UuidText(Uuid),
    /// A member of a code catalog: the Python enum class of `catalog`,
    /// called with the wire value `value`.
    CodeMember {
        catalog: CodeCatalog,
        value: &'static str,
    },
    /// The `cause` slot.
    Exception(ExceptionValue),
}

impl<'a> SlotData<'a> {
    /// The member of `code` in its catalog. Pure.
    pub fn member<C: CodeEnum>(code: &C) -> Self {
        Self::CodeMember {
            catalog: C::CATALOG,
            value: code.wire_value(),
        }
    }
}

/// The exception that a client builds from a typed cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExceptionValue {
    /// The core's exception class of `cause` (Python: the `CoreFailure`
    /// leaf classes of `horsies._core`), with the operation that failed as
    /// its subject. `text` is the cause's own text; for a server error the
    /// class carries the server's primary message instead.
    CoreFailure {
        text: String,
        retryable: bool,
        cause: FailureCause,
    },
    /// An instance of a generated exception class with `text` as its one
    /// argument.
    Generated { class: ExceptionClass, text: String },
    /// An instance of an exception class that the client defines, not the
    /// core, with `text` as its one argument: the exception of a refusal
    /// that the client's reference raised itself.
    ClientClass {
        class: ClientExceptionClass,
        text: String,
    },
    /// No exception: a check of the core found the failure (Python:
    /// `horsies._catalog.error_detail.NoCause()`). Only a slot of the type
    /// `BaseException | NoCause` holds it.
    NoCause,
}

/// An exception class that the client defines, which the cause of a
/// refusal names. The core holds no definition of it: the client maps each
/// member to its own class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientExceptionClass {
    /// A rule refused a value; the text is the rule (Python: the built-in
    /// `ValueError`).
    ValueError,
    /// A text does not read as JSON; the text is the reader's error
    /// (Python: `horsies.core.codec.json_io.SerializationError`).
    SerializationError,
}

/// The typed cause of a variant with a `cause` slot: the value that
/// the client builds the exception from.
pub trait ExceptionCause {
    /// The exception of this cause. Pure.
    fn exception_value(&self) -> ExceptionValue;
}

/// A boxed cause is the exception of the cause in the box: a variant keeps
/// a large cause in a `Box` so that its error stays small.
impl<T: ExceptionCause + ?Sized> ExceptionCause for Box<T> {
    fn exception_value(&self) -> ExceptionValue {
        T::exception_value(self)
    }
}

/// A broker error is the core's class of its cause, with its text and its
/// retry rule.
impl ExceptionCause for BrokerError {
    fn exception_value(&self) -> ExceptionValue {
        ExceptionValue::CoreFailure {
            text: self.to_string(),
            retryable: self.is_retryable(),
            cause: self.failure_cause(),
        }
    }
}

/// A workflow engine error is the core's class of its cause, with its text
/// and its retry rule. A broker error inside it is that broker error, with
/// the broker error's own text: the exception of a failed connect and of a
/// failed statement of a workflow step is the same as for any broker
/// operation.
impl ExceptionCause for WorkflowError {
    fn exception_value(&self) -> ExceptionValue {
        match self {
            Self::Broker(error) => error.exception_value(),
            Self::Serialization(_)
            | Self::WorkflowNotFound { .. }
            | Self::InvalidStatus(_)
            | Self::Validation(_)
            | Self::StepContract(_)
            | Self::ChildSpecSource(_) => ExceptionValue::CoreFailure {
                text: self.to_string(),
                retryable: self.is_retryable(),
                cause: self.failure_cause(),
            },
        }
    }
}

/// What a broker operation met is the core's class of its cause, with its
/// own text and its retry rule; a broker error inside it is that broker
/// error's exception.
impl ExceptionCause for BrokerOperationSource {
    fn exception_value(&self) -> ExceptionValue {
        match self {
            Self::Broker(error) => error.exception_value(),
            Self::ClassRead(_)
            | Self::Snapshot(_)
            | Self::Listen(_)
            | Self::TaskInfoMoved { .. }
            | Self::ArgumentUnreadable(_) => ExceptionValue::CoreFailure {
                text: self.to_string(),
                retryable: self.is_retryable(),
                cause: self.failure_cause(),
            },
        }
    }
}

/// The error of a monitoring read is the core's class of its cause, with
/// its text and its retry rule.
impl ExceptionCause for MonitoringQueryErrorSource {
    fn exception_value(&self) -> ExceptionValue {
        ExceptionValue::CoreFailure {
            text: self.to_string(),
            retryable: self.is_retryable(),
            cause: self.failure_cause(),
        }
    }
}

/// The error that refused a value is its broker error's exception; a check
/// of the core is `NoCause()`.
impl ExceptionCause for BrokerCheckCause {
    fn exception_value(&self) -> ExceptionValue {
        match self {
            Self::Error(error) => error.exception_value(),
            Self::Check => ExceptionValue::NoCause,
        }
    }
}

/// A check of the core is `NoCause()`; a text that does not read as JSON
/// is the client's JSON error with the reader's text; a rule is the
/// client's value error with the rule's text.
impl ExceptionCause for BrokerRefusalCause {
    fn exception_value(&self) -> ExceptionValue {
        match self {
            Self::Check => ExceptionValue::NoCause,
            Self::JsonUnreadable(reader_error) => ExceptionValue::ClientClass {
                class: ClientExceptionClass::SerializationError,
                text: reader_error.clone(),
            },
            Self::ValueRule(rule) => ExceptionValue::ClientClass {
                class: ClientExceptionClass::ValueError,
                text: rule.clone(),
            },
        }
    }
}

/// The workflow error that refused a start is that error's exception; a
/// check of the client or the core is `NoCause()`.
impl ExceptionCause for StartRefusalCause {
    fn exception_value(&self) -> ExceptionValue {
        match self {
            Self::Error(error) => error.exception_value(),
            Self::Check => ExceptionValue::NoCause,
        }
    }
}

/// Stored history data outside the read contract is a
/// `HistoryContractError` with the error's text.
impl ExceptionCause for HistoryContractSource {
    fn exception_value(&self) -> ExceptionValue {
        ExceptionValue::Generated {
            class: HISTORY_CONTRACT_ERROR,
            text: self.to_string(),
        }
    }
}

/// A row outside the wire contract is an `OutcomeDecodeError` with the
/// error's text.
impl ExceptionCause for OutcomeDecodeError {
    fn exception_value(&self) -> ExceptionValue {
        ExceptionValue::Generated {
            class: OUTCOME_DECODE_ERROR,
            text: self.to_string(),
        }
    }
}

/// An exception class of the core for its clients: an `Exception` subclass
/// whose one argument is its message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExceptionClass {
    /// The module under `horsies._catalog` that holds the class.
    pub python_module: &'static str,
    pub python_class: &'static str,
    /// The class doc.
    pub doc: &'static str,
}

/// A row that the database returned does not satisfy the wire contract
/// (Python `horsies.core.lifecycle.outcomes.OutcomeDecodeError`;
/// [`OutcomeDecodeError`] in the core).
pub const OUTCOME_DECODE_ERROR: ExceptionClass = ExceptionClass {
    python_module: "lifecycle_errors",
    python_class: "OutcomeDecodeError",
    doc: "The returned row does not satisfy the wire contract.\n\n\
          Never raised for a task outcome — only when the database returned\n\
          something this code cannot interpret, which means one side is running\n\
          against a contract the other does not implement.",
};

/// The database state violates an invariant of the history reads (Python
/// `horsies.core.history.errors.HistoryContractError`; the core's
/// [`HistoryContractSource`]).
pub const HISTORY_CONTRACT_ERROR: ExceptionClass = ExceptionClass {
    python_module: "history_errors",
    python_class: "HistoryContractError",
    doc: "The database state violates an invariant this subsystem relies on.",
};

/// Every exception class of the core for the Python package, in render
/// order.
pub const EXCEPTION_CLASSES: &[ExceptionClass] = &[OUTCOME_DECODE_ERROR, HISTORY_CONTRACT_ERROR];

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
    <DatabaseOperationError as ErrorEnum>::ERROR_CATALOG,
    <WorkflowHandleError as ErrorEnum>::ERROR_CATALOG,
    <MonitoringQueryError as ErrorEnum>::ERROR_CATALOG,
    <TaskActionError as ErrorEnum>::ERROR_CATALOG,
    <TaskSendError as ErrorEnum>::ERROR_CATALOG,
    <WorkflowStartError as ErrorEnum>::ERROR_CATALOG,
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

/// The value of one field of a generated record class, by the field's
/// Python type ([`RecordFieldType`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordValue<'a> {
    /// `str`.
    Str(&'a str),
    /// `int`.
    Int(i64),
    /// `datetime`, aware, in UTC.
    DateTime(chrono::DateTime<chrono::Utc>),
    /// `str | None`.
    OptionalStr(Option<&'a str>),
    /// `int | None`.
    OptionalInt(Option<i64>),
    /// `datetime | None`, aware, in UTC.
    OptionalDateTime(Option<chrono::DateTime<chrono::Utc>>),
    /// `RetentionClassKey | None`: the key text, or `None` for forever.
    OptionalRetentionClassKey(Option<String>),
}

/// The value of one field of a generated record class, with its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFieldValue<'a> {
    pub name: &'static str,
    pub value: RecordValue<'a>,
}

/// The unions of the failures of one operation that the core produces, in
/// render order. Python's public union of the operation adds its
/// client-only classes.
pub const ERROR_UNIONS: &[ErrorUnion] = &[ErrorUnion {
    python_module: <DatabaseOperationError as ErrorEnum>::ERROR_CATALOG.python_module,
    name: "DatabaseCoreEnqueueError",
    doc: "Every failure of an enqueue that the core gives.",
    members: &[
        "DatabaseEnqueueFailed",
        "DatabasePayloadMismatch",
        "DatabaseIdempotencyKeyConflict",
        "DatabaseInvalidTaskOptions",
        "DatabaseInvalidInput",
    ],
}];

/// The `Literal` subsets that the `code` field of a class holds.
pub fn code_subsets() -> Vec<CodeSubset> {
    vec![CodeSubset {
        python_module: <DatabaseOperationError as ErrorEnum>::ERROR_CATALOG.python_module,
        name: "DatabaseOperationFailedCode",
        doc: "The operation codes of ``DatabaseOperationFailed``.",
        code_catalog: <crate::DatabaseErrorCode as CodeEnum>::CATALOG,
        members: DatabaseOperationFailedCode::ALL
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
                .map(|member| member.name)
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
                        assert_eq!(*position, class.slots.len() - 1, "{}", class.python_class);
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
                "DATABASE_PING_FAILED",
                "WORKER_PING_FAILED",
                "CLEANUP_FAILED",
                "CLOSE_FAILED",
                "LISTENER_START_FAILED",
                "LISTENER_SUBSCRIBE_FAILED",
            ]
        );
    }

    /// The slots of two broker classes, as Python declares them
    /// (`clients/python/horsies/core/brokers/result_types.py`).
    #[test]
    fn a_class_has_python_s_slots_in_python_s_order() {
        let classes = <DatabaseOperationError as ErrorEnum>::ERROR_CATALOG.classes;
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
            class("DatabaseOperationFailed").slots,
            [
                ErrorSlot::CodeSubset {
                    subset: "DatabaseOperationFailedCode"
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
            class("DatabaseIdempotencyKeyConflict").slots,
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
        assert!(class("DatabaseOperationFailed")
            .doc
            .starts_with("A broker operation failed with a database, driver or connection error."));
    }

    /// The slots of three send classes, as Python declares them
    /// (`clients/python/horsies/core/models/task_send_types.py`).
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
        assert_eq!(
            class("TaskSendIdempotencyKeyConflict").slots,
            [
                ErrorSlot::CodeMember {
                    member: "IDEMPOTENCY_KEY_CONFLICT"
                },
                ErrorSlot::NeverRetryable,
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
                    name: "owner_task_id",
                    python_type: SlotType::Uuid,
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

    /// The payload's values name the fields of its record, in the record's
    /// order, each in the value form of the field's type. The default class
    /// is its key, and forever is `None`.
    #[test]
    fn the_payload_values_are_the_fields_of_its_record() {
        let payload = |retention: crate::core::config::EnqueueRetention| TaskSendPayload {
            task_name: "report".to_owned(),
            queue_name: "default".to_owned(),
            priority: 50,
            input_json: r#"{"x":1}"#.to_owned(),
            sent_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
            good_until: None,
            enqueue_delay_seconds: Some(30),
            task_options: None,
            idempotency_key: Some("key".to_owned()),
            retention,
        };
        let defaulted = payload(crate::core::config::EnqueueRetention::Default);
        let values = defaulted.record_values();
        let names: Vec<&str> = values.iter().map(|field| field.name).collect();
        let record_names: Vec<&str> = TASK_SEND_PAYLOAD
            .fields
            .iter()
            .map(|field| field.name)
            .collect();
        assert_eq!(names, record_names);
        for (value, field) in values.iter().zip(TASK_SEND_PAYLOAD.fields) {
            let in_its_form = matches!(
                (&value.value, field.python_type),
                (RecordValue::Str(_), RecordFieldType::Str)
                    | (RecordValue::Int(_), RecordFieldType::Int)
                    | (RecordValue::DateTime(_), RecordFieldType::DateTime)
                    | (RecordValue::OptionalStr(_), RecordFieldType::OptionalStr)
                    | (RecordValue::OptionalInt(_), RecordFieldType::OptionalInt)
                    | (
                        RecordValue::OptionalDateTime(_),
                        RecordFieldType::OptionalDateTime
                    )
                    | (
                        RecordValue::OptionalRetentionClassKey(_),
                        RecordFieldType::OptionalRetentionClassKey
                    )
            );
            assert!(
                in_its_form,
                "{}: {:?} for {:?}",
                field.name, value.value, field.python_type
            );
        }
        assert_eq!(
            values.last().map(|field| &field.value),
            Some(&RecordValue::OptionalRetentionClassKey(Some(
                DEFAULT_RETENTION_CLASS_KEY.to_owned()
            )))
        );
        let forever = payload(crate::core::config::EnqueueRetention::Forever);
        assert_eq!(
            forever
                .record_values()
                .last()
                .map(|field| field.value.clone()),
            Some(RecordValue::OptionalRetentionClassKey(None))
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
        let failed = DatabaseOperationError::OperationFailed {
            code: DatabaseOperationFailedCode::CloseFailed,
            message: "close failed".to_owned(),
            retryable: true,
            cause: Box::new(crate::BrokerOperationSource::Broker(
                crate::BrokerError::ConnectionFailed("lost".to_owned()),
            )),
        };
        assert_eq!(failed.python_class(), "DatabaseOperationFailed");
        assert_eq!(failed.error_code(), crate::DatabaseErrorCode::CloseFailed);
        let conflict = DatabaseOperationError::IdempotencyKeyConflict {
            message: "held".to_owned(),
            owner_task_id: uuid::Uuid::nil(),
        };
        assert_eq!(conflict.python_class(), "DatabaseIdempotencyKeyConflict");
        assert_eq!(
            conflict.error_code(),
            crate::DatabaseErrorCode::IdempotencyKeyConflict
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

    /// A statement that the server refused with a statement timeout
    /// (SQLSTATE 57014), with the server's primary message.
    async fn statement_timeout() -> BrokerError {
        BrokerError::Statement(crate::db::StatementFailure::StatementRefused {
            statement: "test_statement",
            cause: crate::db::RefusalCause::Server(Box::new(
                crate::test_support::raised_server_error(
                    "57014",
                    "canceling statement due to statement timeout",
                )
                .await,
            )),
        })
    }

    const TASK: Uuid = Uuid::from_u128(0x3f9c_1a2b_4d5e_4f60_8a71_b2c3_d4e5_f607);

    /// The instance slots of `class`, by name, in order. The class values
    /// (`code` of one member, the fixed `retryable` of a class) are not
    /// instance slots.
    fn instance_slot_names(class: &ErrorClass) -> Vec<&'static str> {
        class
            .slots
            .iter()
            .filter_map(|slot| match slot {
                ErrorSlot::CodeMember { .. }
                | ErrorSlot::NeverRetryable
                | ErrorSlot::AlwaysRetryable => None,
                ErrorSlot::CodeSubset { .. } => Some("code"),
                ErrorSlot::Field { name, .. } => Some(*name),
                ErrorSlot::Exception { .. } => Some("cause"),
            })
            .collect()
    }

    /// One value of each variant of the task action catalog.
    fn every_task_action_error() -> Vec<TaskActionError> {
        vec![
            TaskActionError::NotFound {
                message: format!("Task {TASK} does not exist."),
                task_id: TASK,
            },
            TaskActionError::NotCancellable {
                message: format!("Task {TASK} cannot be cancelled from status COMPLETED."),
                task_id: TASK,
                current_status: crate::TaskStatus::Completed,
            },
            TaskActionError::WorkflowTask {
                message: format!(
                    "Task {TASK} belongs to a workflow; use the workflow actions instead."
                ),
                task_id: TASK,
                current_status: crate::TaskStatus::Pending,
            },
            TaskActionError::cancel_failed(
                TASK,
                BrokerError::ConnectionFailed("refused".to_owned()),
            ),
            TaskActionError::StoredDataInvalid {
                message: "Task cancel failed: stored data does not parse: \
                          terminalization outcome decode failed: observed_status NOT_KNOWN"
                    .to_owned(),
                task_id: TASK,
                cause: OutcomeDecodeError("observed_status NOT_KNOWN".to_owned()),
            },
        ]
    }

    /// The values of each task action name the instance slots of its class,
    /// in the class's order, and the values cover every class.
    #[test]
    fn the_values_of_each_task_action_name_the_instance_slots_of_its_class() {
        let catalog = <TaskActionError as ErrorEnum>::ERROR_CATALOG;
        let errors = every_task_action_error();
        for error in &errors {
            let class = catalog
                .classes
                .iter()
                .find(|class| class.python_class == error.python_class())
                .unwrap_or_else(|| panic!("no class {}", error.python_class()));
            let names: Vec<&str> = error.slot_values().iter().map(|slot| slot.name).collect();
            assert_eq!(names, instance_slot_names(class), "{}", class.python_class);
        }
        let covered: HashSet<&str> = errors.iter().map(|error| error.python_class()).collect();
        let classes: HashSet<&str> = catalog
            .classes
            .iter()
            .map(|class| class.python_class)
            .collect();
        assert_eq!(covered, classes);
    }

    /// A refusal gives its message, its task and its status as a member of
    /// the status catalog.
    #[test]
    fn a_refusal_gives_its_message_its_task_and_its_status_as_a_member() {
        let refused = TaskActionError::NotCancellable {
            message: "refused".to_owned(),
            task_id: TASK,
            current_status: crate::TaskStatus::Completed,
        };
        assert_eq!(
            refused.slot_values(),
            vec![
                SlotValue {
                    name: "message",
                    value: SlotData::Str("refused"),
                },
                SlotValue {
                    name: "task_id",
                    value: SlotData::Uuid(TASK),
                },
                SlotValue {
                    name: "current_status",
                    value: SlotData::CodeMember {
                        catalog: <crate::TaskStatus as CodeEnum>::CATALOG,
                        value: "COMPLETED",
                    },
                },
            ]
        );
    }

    /// Python's `TaskActionDatabaseOperationFailed` of a cancel that met a broker
    /// error: the message holds the error's text, and the exception is the
    /// core's class of each cause with the same text and the error's retry
    /// rule.
    #[tokio::test]
    async fn a_database_failure_builds_its_exception_from_its_cause_for_each_cause() {
        let cases = [
            (
                statement_timeout().await,
                FailureCause::Server(crate::ServerErrorFacts::of_code(
                    "57014",
                    "canceling statement due to statement timeout".to_owned(),
                )),
                true,
            ),
            (
                BrokerError::ConnectionFailed("connection refused".to_owned()),
                FailureCause::Connection,
                true,
            ),
            (
                BrokerError::ClaimContract("ordinality 3 has no answer".to_owned()),
                FailureCause::ContractViolation,
                false,
            ),
            (
                BrokerError::InvalidIdempotencyReservationWindow("0 s".to_owned()),
                FailureCause::PreconditionFailed,
                false,
            ),
        ];
        for (cause, expected_cause, expected_retryable) in cases {
            let text = cause.to_string();
            let failed = TaskActionError::cancel_failed(TASK, cause);
            let message = format!("Task cancel failed: {text}");
            assert_eq!(failed.message(), message);
            assert_eq!(failed.retryable(), expected_retryable, "{text}");
            assert_eq!(failed.failure_cause(), expected_cause, "{text}");
            assert_eq!(
                failed.slot_values(),
                vec![
                    SlotValue {
                        name: "message",
                        value: SlotData::Str(&message),
                    },
                    SlotValue {
                        name: "retryable",
                        value: SlotData::Bool(expected_retryable),
                    },
                    SlotValue {
                        name: "task_id",
                        value: SlotData::Uuid(TASK),
                    },
                    SlotValue {
                        name: "cause",
                        value: SlotData::Exception(ExceptionValue::CoreFailure {
                            text: text.clone(),
                            retryable: expected_retryable,
                            cause: expected_cause.clone(),
                        }),
                    },
                ],
                "{text}"
            );
        }
    }

    /// Python's `TaskActionStoredDataInvalid`: the exception is
    /// `OutcomeDecodeError` with the decode error's text.
    #[test]
    fn stored_data_that_does_not_parse_builds_an_outcome_decode_error() {
        let cause = OutcomeDecodeError("observed_status NOT_KNOWN".to_owned());
        assert_eq!(
            cause.exception_value(),
            ExceptionValue::Generated {
                class: OUTCOME_DECODE_ERROR,
                text: "terminalization outcome decode failed: observed_status NOT_KNOWN".to_owned(),
            }
        );
        let invalid = every_task_action_error()
            .into_iter()
            .find(|error| matches!(error, TaskActionError::StoredDataInvalid { .. }))
            .expect("the list holds the stored data refusal");
        assert_eq!(
            invalid.slot_values().last(),
            Some(&SlotValue {
                name: "cause",
                value: SlotData::Exception(cause.exception_value()),
            })
        );
        assert_eq!(invalid.failure_cause(), FailureCause::ContractViolation);
        assert!(!invalid.retryable());
    }

    /// The exception classes are Python's: `OutcomeDecodeError` with its
    /// doc, and no two classes share a name.
    #[test]
    fn the_exception_classes_are_named_once_with_python_s_doc() {
        let names: HashSet<&str> = EXCEPTION_CLASSES
            .iter()
            .map(|class| class.python_class)
            .collect();
        assert_eq!(names.len(), EXCEPTION_CLASSES.len());
        assert!(OUTCOME_DECODE_ERROR
            .doc
            .starts_with("The returned row does not satisfy the wire contract.\n\nNever raised"));
        assert_eq!(
            HISTORY_CONTRACT_ERROR.doc,
            "The database state violates an invariant this subsystem relies on."
        );
    }

    /// One value of each variant of the monitoring query catalog: a failed
    /// connect, and a history row that breaks the read contract.
    fn every_monitoring_query_error() -> Vec<MonitoringQueryError> {
        vec![
            MonitoringQueryError::connect_failed(
                "task list query",
                BrokerError::ConnectionFailed("connection refused".to_owned()),
            ),
            MonitoringQueryError::history(
                "task detail history read",
                crate::HistoryError::Contract("bad row".to_owned()),
            ),
        ]
    }

    #[test]
    fn the_values_of_each_monitoring_query_name_the_instance_slots_of_its_class() {
        let catalog = <MonitoringQueryError as ErrorEnum>::ERROR_CATALOG;
        let errors = every_monitoring_query_error();
        for error in &errors {
            let class = catalog
                .classes
                .iter()
                .find(|class| class.python_class == error.python_class())
                .unwrap_or_else(|| panic!("no class {}", error.python_class()));
            let names: Vec<&str> = error.slot_values().iter().map(|slot| slot.name).collect();
            assert_eq!(names, instance_slot_names(class), "{}", class.python_class);
        }
        let covered: HashSet<&str> = errors.iter().map(|error| error.python_class()).collect();
        let classes: HashSet<&str> = catalog
            .classes
            .iter()
            .map(|class| class.python_class)
            .collect();
        assert_eq!(covered, classes);
    }

    /// Python's `MonitoringQueryDatabaseOperationFailed` of a failed connect: the
    /// message names the read and holds the error's text; the exception is
    /// the core's class of the cause with the same text.
    #[test]
    fn a_monitoring_database_failure_builds_its_exception_from_its_source() {
        let failed = MonitoringQueryError::connect_failed(
            "task list query",
            BrokerError::ConnectionFailed("connection refused".to_owned()),
        );
        assert_eq!(
            failed.slot_values(),
            vec![
                SlotValue {
                    name: "message",
                    value: SlotData::Str(
                        "task list query failed: connection failed: connection refused"
                    ),
                },
                SlotValue {
                    name: "retryable",
                    value: SlotData::Bool(true),
                },
                SlotValue {
                    name: "cause",
                    value: SlotData::Exception(ExceptionValue::CoreFailure {
                        text: "connection failed: connection refused".to_owned(),
                        retryable: true,
                        cause: FailureCause::Connection,
                    }),
                },
            ]
        );
    }

    /// Python's `MonitoringQueryHistoryContractViolation`: the message names
    /// the read and holds the error's text; the exception is a
    /// `HistoryContractError` with that text.
    #[test]
    fn a_history_contract_violation_builds_a_history_contract_error() {
        let cause = crate::HistoryError::Contract("bad row".to_owned());
        let text = cause.to_string();
        let violation = MonitoringQueryError::history("task detail history read", cause);
        assert_eq!(
            violation.slot_values(),
            vec![
                SlotValue {
                    name: "message",
                    value: SlotData::Str(&format!("task detail history read failed: {text}")),
                },
                SlotValue {
                    name: "cause",
                    value: SlotData::Exception(ExceptionValue::Generated {
                        class: HISTORY_CONTRACT_ERROR,
                        text,
                    }),
                },
            ]
        );
        assert_eq!(violation.failure_cause(), FailureCause::ContractViolation);
    }

    const WORKFLOW: Uuid = Uuid::from_u128(0x7a1b_2c3d_4e5f_4061_8273_9a4b_5c6d_7e8f);

    /// A refused connect, as the broker reports it.
    fn refused() -> BrokerError {
        BrokerError::ConnectionFailed("refused".to_owned())
    }

    /// One value of each variant of the handle catalog.
    fn every_handle_error() -> Vec<WorkflowHandleError> {
        vec![
            WorkflowHandleError::not_found(WORKFLOW),
            WorkflowHandleError::connect_failed(
                &crate::HandleOperation::Status,
                WORKFLOW,
                refused(),
            ),
            WorkflowHandleError::StoredDataInvalid {
                message: format!("Status failed for workflow {WORKFLOW}: invalid status: BOGUS"),
                workflow_id: WORKFLOW,
                cause: WorkflowError::InvalidStatus("BOGUS".to_owned()),
            },
            WorkflowHandleError::ResumeRecoveryFailed {
                message: format!(
                    "Workflow {WORKFLOW} resumed; the recovery pass for its tree failed: \
                     broker error: connection failed: refused"
                ),
                workflow_id: WORKFLOW,
                cause: WorkflowError::Broker(refused()),
            },
            WorkflowHandleError::AsyncContext {
                message: "sync call inside a loop".to_owned(),
                workflow_id: WORKFLOW,
            },
        ]
    }

    /// The values of each handle error name the instance slots of its
    /// class, in the class's order, and the values cover every class.
    #[test]
    fn the_values_of_each_handle_error_name_the_instance_slots_of_its_class() {
        let catalog = <WorkflowHandleError as ErrorEnum>::ERROR_CATALOG;
        let errors = every_handle_error();
        for error in &errors {
            let class = catalog
                .classes
                .iter()
                .find(|class| class.python_class == error.python_class())
                .unwrap_or_else(|| panic!("no class {}", error.python_class()));
            let names: Vec<&str> = error.slot_values().iter().map(|slot| slot.name).collect();
            assert_eq!(names, instance_slot_names(class), "{}", class.python_class);
        }
        let covered: HashSet<&str> = errors.iter().map(|error| error.python_class()).collect();
        let classes: HashSet<&str> = catalog
            .classes
            .iter()
            .map(|class| class.python_class)
            .collect();
        assert_eq!(covered, classes);
    }

    /// Python's `WorkflowHandleDatabaseOperationFailed` of a failed connect: the message
    /// names the operation, the workflow id is its text, and the exception
    /// is the core's class of the broker error with the broker error's own
    /// text.
    #[test]
    fn a_handle_database_failure_builds_its_exception_from_its_cause() {
        let failed = WorkflowHandleError::connect_failed(
            &crate::HandleOperation::Status,
            WORKFLOW,
            refused(),
        );
        let message = format!("Status failed for workflow {WORKFLOW}: connection failed: refused");
        assert_eq!(
            failed.slot_values(),
            vec![
                SlotValue {
                    name: "message",
                    value: SlotData::Str(&message),
                },
                SlotValue {
                    name: "retryable",
                    value: SlotData::Bool(true),
                },
                SlotValue {
                    name: "workflow_id",
                    value: SlotData::UuidText(WORKFLOW),
                },
                SlotValue {
                    name: "cause",
                    value: SlotData::Exception(ExceptionValue::CoreFailure {
                        text: "connection failed: refused".to_owned(),
                        retryable: true,
                        cause: FailureCause::Connection,
                    }),
                },
            ]
        );
    }

    /// The exception of each cause of a workflow engine error: a broker
    /// error inside it is that broker error with its own text; every other
    /// error is the core's class of its cause with the error's text and its
    /// retry rule.
    #[tokio::test]
    async fn a_workflow_error_builds_the_core_class_of_its_cause() {
        let statement = statement_timeout().await;
        let statement_text = statement.to_string();
        assert_eq!(
            WorkflowError::Broker(statement).exception_value(),
            ExceptionValue::CoreFailure {
                text: statement_text,
                retryable: true,
                cause: FailureCause::Server(crate::ServerErrorFacts::of_code(
                    "57014",
                    "canceling statement due to statement timeout".to_owned(),
                )),
            }
        );
        assert_eq!(
            WorkflowError::Broker(refused()).exception_value(),
            ExceptionValue::CoreFailure {
                text: "connection failed: refused".to_owned(),
                retryable: true,
                cause: FailureCause::Connection,
            }
        );
        assert_eq!(
            WorkflowError::InvalidStatus("BOGUS".to_owned()).exception_value(),
            ExceptionValue::CoreFailure {
                text: "invalid status: BOGUS".to_owned(),
                retryable: false,
                cause: FailureCause::ContractViolation,
            }
        );
        let not_run = WorkflowError::ChildSpecSource(
            crate::workflow_engine::child_spec::ChildSpecUnavailable::NotRun {
                subject: "child build".to_owned(),
                cause: "callback queue full".to_owned(),
            },
        );
        assert_eq!(
            not_run.exception_value(),
            ExceptionValue::CoreFailure {
                text: not_run.to_string(),
                retryable: true,
                cause: not_run.failure_cause(),
            }
        );
    }

    /// Python's `WorkflowHandleResumeRecoveryFailed`: the exception has the text of
    /// the failure of the recovery pass alone, as in 0.7, not the message
    /// that names the resume.
    #[test]
    fn a_failed_recovery_pass_builds_its_exception_from_its_cause_alone() {
        let failed = every_handle_error()
            .into_iter()
            .find(|error| matches!(error, WorkflowHandleError::ResumeRecoveryFailed { .. }))
            .expect("the list holds the failed recovery pass");
        assert_eq!(
            failed.slot_values().last(),
            Some(&SlotValue {
                name: "cause",
                value: SlotData::Exception(ExceptionValue::CoreFailure {
                    text: "connection failed: refused".to_owned(),
                    retryable: true,
                    cause: FailureCause::Connection,
                }),
            })
        );
        assert!(!failed.retryable());
    }

    /// One value of each variant of the broker catalog, each built by the
    /// constructor of its code where it has one.
    fn every_broker_error() -> Vec<DatabaseOperationError> {
        vec![
            DatabaseOperationError::database_ping_failed(refused()),
            DatabaseOperationError::EnqueueFailed {
                message: "enqueue unconfirmed".to_owned(),
                retryable: true,
                cause: Box::new(BrokerCheckCause::Error(refused())),
            },
            DatabaseOperationError::PayloadMismatch {
                message: "payload mismatch".to_owned(),
            },
            DatabaseOperationError::IdempotencyKeyConflict {
                message: "held".to_owned(),
                owner_task_id: TASK,
            },
            DatabaseOperationError::task_info_invalid_json(
                TASK,
                &"history result digest is absent",
                BrokerCheckCause::Check,
            ),
            DatabaseOperationError::InvalidTaskOptions {
                message: "options refused".to_owned(),
                cause: BrokerRefusalCause::Check,
            },
            DatabaseOperationError::invalid_input(
                "limit must be positive when set, got 0".to_owned(),
            ),
            DatabaseOperationError::AsyncContext {
                message: "sync call inside a loop".to_owned(),
            },
            DatabaseOperationError::ListenerDisabled {
                message: "no listener".to_owned(),
            },
            DatabaseOperationError::WaitInsideTask {
                message: "wait inside a task".to_owned(),
            },
        ]
    }

    /// The values of each broker error name the instance slots of its
    /// class, in the class's order, and the values cover every class.
    #[test]
    fn the_values_of_each_broker_error_name_the_instance_slots_of_its_class() {
        let catalog = <DatabaseOperationError as ErrorEnum>::ERROR_CATALOG;
        let errors = every_broker_error();
        for error in &errors {
            let class = catalog
                .classes
                .iter()
                .find(|class| class.python_class == error.python_class())
                .unwrap_or_else(|| panic!("no class {}", error.python_class()));
            let names: Vec<&str> = error.slot_values().iter().map(|slot| slot.name).collect();
            assert_eq!(names, instance_slot_names(class), "{}", class.python_class);
        }
        let covered: HashSet<&str> = errors.iter().map(|error| error.python_class()).collect();
        let classes: HashSet<&str> = catalog
            .classes
            .iter()
            .map(|class| class.python_class)
            .collect();
        assert_eq!(covered, classes);
    }

    /// Python 0.7's `DatabaseOperationFailed` of a failed database ping: the
    /// code as a member of the subset, Python's message, the retry rule of
    /// the broker error, and the core's class of the broker error with its
    /// own text.
    #[test]
    fn a_failed_operation_gives_its_code_its_message_and_the_exception_of_its_source() {
        let failed = DatabaseOperationError::database_ping_failed(refused());
        assert_eq!(
            failed.slot_values(),
            vec![
                SlotValue {
                    name: "code",
                    value: SlotData::CodeMember {
                        catalog: <crate::DatabaseErrorCode as CodeEnum>::CATALOG,
                        value: "DATABASE_PING_FAILED",
                    },
                },
                SlotValue {
                    name: "message",
                    value: SlotData::Str("ping_database failed: connection failed: refused"),
                },
                SlotValue {
                    name: "retryable",
                    value: SlotData::Bool(true),
                },
                SlotValue {
                    name: "cause",
                    value: SlotData::Exception(ExceptionValue::CoreFailure {
                        text: "connection failed: refused".to_owned(),
                        retryable: true,
                        cause: FailureCause::Connection,
                    }),
                },
            ]
        );
    }

    /// The exception of each source that is not a broker error: the core's
    /// class of its cause, with the source's own text and its retry rule.
    #[test]
    fn each_source_builds_the_core_class_of_its_cause() {
        let refused_read = DatabaseOperationError::monitoring_query_refused(
            "get_class_coverage",
            crate::broker::class_coverage::ClassReadRefused {
                reason: "a live snapshot holds no budgets".to_owned(),
            },
        );
        assert_eq!(
            refused_read.message(),
            "get_class_coverage refused: a live snapshot holds no budgets"
        );
        assert!(!refused_read.retryable());
        let unreadable = DatabaseOperationError::monitoring_query_failed(
            "list_worker_states",
            BrokerOperationSource::Snapshot(crate::broker::health::WorkerStateUnreadable {
                worker_id: "w1".to_owned(),
                snapshot_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
                column: crate::broker::health::WorkerStateColumn::SlotsDown,
                reason: "not a list".to_owned(),
            }),
        );
        assert!(!unreadable.retryable());
        let moved = DatabaseOperationError::task_info_query_failed(
            TASK,
            BrokerOperationSource::TaskInfoMoved {
                task_id: TASK,
                passes: 3,
            },
        );
        assert_eq!(
            moved.message(),
            format!(
                "get_task_info failed for {TASK}: task {TASK} was absent from the live table \
                 and live in the history read on 3 passes in a row"
            )
        );
        assert!(moved.retryable());
        for (error, cause) in [
            (&refused_read, FailureCause::ContractViolation),
            (&unreadable, FailureCause::ContractViolation),
            (&moved, FailureCause::PreconditionFailed),
        ] {
            let DatabaseOperationError::OperationFailed { cause: source, .. } = error else {
                panic!("a failed read gives OperationFailed");
            };
            assert_eq!(
                source.exception_value(),
                ExceptionValue::CoreFailure {
                    text: source.to_string(),
                    retryable: error.retryable(),
                    cause: cause.clone(),
                }
            );
            assert_eq!(error.failure_cause(), cause);
        }
    }

    /// A check of the core gives `NoCause()`; an error that refused the
    /// value gives the core's class of its cause.
    #[test]
    fn a_check_gives_no_cause_and_an_error_gives_its_class() {
        let refused_argument = DatabaseOperationError::invalid_input(
            "limit must be positive when set, got 0".to_owned(),
        );
        assert_eq!(
            refused_argument.slot_values(),
            vec![
                SlotValue {
                    name: "message",
                    value: SlotData::Str("limit must be positive when set, got 0"),
                },
                SlotValue {
                    name: "cause",
                    value: SlotData::Exception(ExceptionValue::NoCause),
                },
            ]
        );
        assert_eq!(
            refused_argument.failure_cause(),
            FailureCause::PreconditionFailed
        );
        let parse_error =
            serde_json::from_str::<serde_json::Value>("{").expect_err("the text is not JSON");
        let unreadable = DatabaseOperationError::task_info_invalid_json(
            TASK,
            &"serialization error",
            BrokerCheckCause::Error(BrokerError::Serialization(parse_error)),
        );
        let DatabaseOperationError::InvalidJsonPayload { cause, .. } = &unreadable else {
            panic!("a stored value that does not parse gives InvalidJsonPayload");
        };
        assert!(matches!(
            cause.exception_value(),
            ExceptionValue::CoreFailure {
                retryable: false,
                cause: FailureCause::ContractViolation,
                ..
            }
        ));
        assert_eq!(unreadable.failure_cause(), FailureCause::ContractViolation);
        assert_eq!(
            unreadable.message(),
            format!("get_task_info failed for {TASK}: serialization error")
        );
    }

    /// The task name of the enqueue tests.
    const SENT_TASK: &str = "send.task";

    /// The values of a refused or failed enqueue: its class, its message
    /// and the exception of its cause.
    fn enqueue_values(error: &DatabaseOperationError) -> (&'static str, &str, ExceptionValue) {
        let exception = match error.slot_values().pop() {
            Some(SlotValue {
                name: "cause",
                value: SlotData::Exception(exception),
            }) => exception,
            other => panic!("{} ends with {other:?}", error.python_class()),
        };
        (error.python_class(), error.message(), exception)
    }

    /// Python's refusal of a send before any statement: the code of the
    /// refused part, Python's message of a rule of the version-1
    /// command or of the scoped idempotency key, and the cause that
    /// Python's broker gave: the reader's `SerializationError`, `NoCause()`
    /// for a rule that refused a value that reads, the rule's `ValueError`.
    #[test]
    fn an_enqueue_refusal_has_the_code_of_its_part_and_python_s_cause() {
        use crate::broker::{EnqueueRefusal, EnqueueRefusalCause};
        use crate::core::history::enqueue::SendPart;

        let serialization = |text: &str| ExceptionValue::ClientClass {
            class: ClientExceptionClass::SerializationError,
            text: text.to_owned(),
        };
        let value_error = |text: &str| ExceptionValue::ClientClass {
            class: ClientExceptionClass::ValueError,
            text: text.to_owned(),
        };
        for (part, class, code) in [
            (
                SendPart::Input,
                "DatabaseInvalidInput",
                crate::DatabaseErrorCode::InvalidInput,
            ),
            (
                SendPart::TaskOptions,
                "DatabaseInvalidTaskOptions",
                crate::DatabaseErrorCode::InvalidTaskOptions,
            ),
        ] {
            let cases = [
                (
                    EnqueueRefusalCause::JsonUnreadable("trailing characters".to_owned()),
                    "enqueue contract violation: the text was refused",
                    serialization("trailing characters"),
                ),
                (
                    EnqueueRefusalCause::RuleRefused,
                    "enqueue contract violation: the text was refused",
                    ExceptionValue::NoCause,
                ),
                (
                    EnqueueRefusalCause::CommandRuleRefused(
                        "priority must be between 1 and 100".to_owned(),
                    ),
                    "enqueue input refused for send.task: priority must be between 1 and 100",
                    value_error("priority must be between 1 and 100"),
                ),
                (
                    EnqueueRefusalCause::KeyRuleRefused(
                        "idempotency key must be non-empty".to_owned(),
                    ),
                    "idempotency key refused for send.task: idempotency key must be non-empty",
                    value_error("idempotency key must be non-empty"),
                ),
            ];
            for (cause, message, exception) in cases {
                let refusal = EnqueueRefusal {
                    part,
                    message: "the text was refused".to_owned(),
                    cause,
                };
                let refused = DatabaseOperationError::enqueue_error(
                    SENT_TASK,
                    BrokerError::EnqueueContract(refusal.clone()),
                );
                assert_eq!(
                    enqueue_values(&refused),
                    (class, message, exception),
                    "{refusal:?}"
                );
                assert_eq!(refused.code(), code, "{refusal:?}");
                assert!(!refused.retryable(), "{refusal:?}");
                assert_eq!(refused.failure_cause(), FailureCause::PreconditionFailed);
                assert!(std::error::Error::source(&refused).is_none());
            }
        }
    }

    /// Python's `EnqueueCommandV1` refuses an empty retention class key with
    /// its rule; a key that is not empty and that no class derives reached
    /// the CHECK of the insert in Python and failed the enqueue, not
    /// retryable. The exception of that failure is the core's class of the
    /// refusal, with the refusal's own text.
    #[test]
    fn an_empty_retention_key_is_invalid_input_and_an_underived_one_a_failed_enqueue() {
        let empty = DatabaseOperationError::enqueue_error(
            SENT_TASK,
            BrokerError::EnqueueContract(crate::broker::EnqueueRefusal::empty_retention_class_key()),
        );
        assert_eq!(
            enqueue_values(&empty),
            (
                "DatabaseInvalidInput",
                "enqueue input refused for send.task: retention_class_key must be non-empty",
                ExceptionValue::ClientClass {
                    class: ClientExceptionClass::ValueError,
                    text: "retention_class_key must be non-empty".to_owned(),
                },
            )
        );

        let refused_key = crate::EnqueueRetention::from_class_key("standard_0d")
            .expect_err("a period of 0 days is below the bounds");
        let refused_text = refused_key.to_string();
        let underived = DatabaseOperationError::enqueue_error(
            SENT_TASK,
            BrokerError::RetentionClassKey(refused_key),
        );
        let message = format!("Failed to enqueue task send.task: {refused_text}");
        assert!(
            message.starts_with(
                "Failed to enqueue task send.task: retention class key \"standard_0d\""
            ),
            "{message}"
        );
        assert_eq!(
            enqueue_values(&underived),
            (
                "DatabaseEnqueueFailed",
                message.as_str(),
                ExceptionValue::CoreFailure {
                    text: refused_text,
                    retryable: false,
                    cause: FailureCause::PreconditionFailed,
                },
            )
        );
        assert!(!underived.retryable());
    }

    /// A value that does not serialize and a reservation window out of
    /// range refuse a fact of the call: `INVALID_INPUT` with the error's
    /// text and `NoCause()`.
    #[test]
    fn an_unserializable_value_and_a_window_out_of_range_are_invalid_input_with_no_cause() {
        let parse_error =
            serde_json::from_str::<serde_json::Value>("{").expect_err("the text is not JSON");
        for error in [
            BrokerError::Serialization(parse_error),
            BrokerError::InvalidIdempotencyReservationWindow("0 s".to_owned()),
        ] {
            let text = error.to_string();
            let refused = DatabaseOperationError::enqueue_error(SENT_TASK, error);
            assert_eq!(
                enqueue_values(&refused),
                (
                    "DatabaseInvalidInput",
                    text.as_str(),
                    ExceptionValue::NoCause
                )
            );
            assert!(!refused.retryable(), "{text}");
        }
    }

    /// A task id that holds another command is `PAYLOAD_MISMATCH` with the
    /// error's text; a key that another task holds is
    /// `IDEMPOTENCY_KEY_CONFLICT` with Python's text and the owner.
    #[test]
    fn a_payload_mismatch_and_a_key_conflict_keep_the_error_s_text_and_the_owner() {
        let mismatch = DatabaseOperationError::enqueue_error(
            SENT_TASK,
            BrokerError::PayloadMismatch { task_id: TASK },
        );
        assert_eq!(mismatch.python_class(), "DatabasePayloadMismatch");
        assert_eq!(
            mismatch.slot_values(),
            vec![SlotValue {
                name: "message",
                value: SlotData::Str(&format!(
                    "payload mismatch for task_id {TASK}: the existing task has a different \
                     command or idempotency key"
                )),
            }]
        );
        assert!(!mismatch.retryable());

        let owner = Uuid::from_u128(0x0198_c0de_0000_7000_8000_0000_0000_00ff);
        let conflict = DatabaseOperationError::enqueue_error(
            SENT_TASK,
            BrokerError::IdempotencyKeyConflict {
                task_name: SENT_TASK.to_owned(),
                task_id: owner,
            },
        );
        assert_eq!(conflict.python_class(), "DatabaseIdempotencyKeyConflict");
        assert_eq!(
            conflict.slot_values(),
            vec![
                SlotValue {
                    name: "message",
                    value: SlotData::Str(&format!(
                        "idempotency key for send.task is reserved by task {owner} with a \
                         different command"
                    )),
                },
                SlotValue {
                    name: "owner_task_id",
                    value: SlotData::Uuid(owner),
                },
            ]
        );
        assert!(!conflict.retryable());
    }

    /// Python's broker found a conflicting row gone before the compare by a
    /// data check: `ENQUEUE_FAILED` with Python's text, not retryable, and
    /// `NoCause()`.
    #[test]
    fn a_conflict_whose_row_was_gone_is_a_failed_enqueue_with_no_cause() {
        let gone = DatabaseOperationError::enqueue_error(
            SENT_TASK,
            BrokerError::EnqueueConflictUnverifiable {
                task_id: TASK,
                task_name: SENT_TASK.to_owned(),
            },
        );
        let message = format!(
            "task_id {TASK} conflict detected but row disappeared before verification for \
             send.task; cannot verify payload identity"
        );
        assert_eq!(
            enqueue_values(&gone),
            (
                "DatabaseEnqueueFailed",
                message.as_str(),
                ExceptionValue::NoCause
            )
        );
        assert!(!gone.retryable());
        assert_eq!(gone.failure_cause(), FailureCause::PreconditionFailed);
    }

    /// Every other error of an enqueue is `ENQUEUE_FAILED` with Python's
    /// message (`Failed to enqueue task <name>: <error>`), the error's retry
    /// rule, and the core's class of the error with its own text.
    #[test]
    fn another_enqueue_error_is_a_failed_enqueue_with_python_s_message_and_its_class() {
        let cases = [
            (refused(), FailureCause::Connection, true),
            (
                BrokerError::Statement(crate::db::StatementFailure::NotSent {
                    statement: "enqueue_task",
                    cause: crate::db::NotSentCause::Checkout(
                        crate::db::CheckoutFailure::TimedOut {
                            role: crate::db::PoolRole::Main,
                            waited: std::time::Duration::from_secs(30),
                        },
                    ),
                }),
                FailureCause::Connection,
                true,
            ),
            (
                BrokerError::ClaimContract("ordinality 3 has no answer".to_owned()),
                FailureCause::ContractViolation,
                false,
            ),
        ];
        for (error, cause, retryable) in cases {
            let text = error.to_string();
            let failed = DatabaseOperationError::enqueue_error(SENT_TASK, error);
            let message = format!("Failed to enqueue task send.task: {text}");
            assert_eq!(
                failed.slot_values(),
                vec![
                    SlotValue {
                        name: "message",
                        value: SlotData::Str(&message),
                    },
                    SlotValue {
                        name: "retryable",
                        value: SlotData::Bool(retryable),
                    },
                    SlotValue {
                        name: "cause",
                        value: SlotData::Exception(ExceptionValue::CoreFailure {
                            text: text.clone(),
                            retryable,
                            cause: cause.clone(),
                        }),
                    },
                ],
                "{text}"
            );
            assert_eq!(failed.python_class(), "DatabaseEnqueueFailed");
            assert_eq!(failed.failure_cause(), cause, "{text}");
            assert!(std::error::Error::source(&failed).is_some());
        }
    }

    /// The cause of a refusal builds Python's exception of it: `NoCause()`
    /// for a check, `SerializationError` with the reader's text, and
    /// `ValueError` with the rule's text.
    #[test]
    fn a_refusal_cause_builds_python_s_exception_of_it() {
        assert_eq!(
            BrokerRefusalCause::Check.exception_value(),
            ExceptionValue::NoCause
        );
        assert_eq!(
            BrokerRefusalCause::JsonUnreadable("expected value".to_owned()).exception_value(),
            ExceptionValue::ClientClass {
                class: ClientExceptionClass::SerializationError,
                text: "expected value".to_owned(),
            }
        );
        assert_eq!(
            BrokerRefusalCause::ValueRule("task_name must be non-empty".to_owned())
                .exception_value(),
            ExceptionValue::ClientClass {
                class: ClientExceptionClass::ValueError,
                text: "task_name must be non-empty".to_owned(),
            }
        );
    }
}
