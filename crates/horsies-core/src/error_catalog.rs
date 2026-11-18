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
//! A client builds the class of a value from [`ErrorClassValues`]
//! (`#[derive(horsies_core_derive::ErrorClassValues)]`): the values of the
//! instance slots, by name, in the class's order. The `exception` slot is
//! built from the variant's typed cause (`#[error_slot(cause)]`, an
//! [`ExceptionCause`]), so the exception keeps the cause's own text.
//!
//! [`EXCEPTION_CLASSES`] are the exception classes that the core defines
//! for its clients (for example `OutcomeDecodeError`): `Exception`
//! subclasses whose one argument is their message.

use uuid::Uuid;

use crate::broker::BrokerError;
use crate::catalog::{CodeCatalog, CodeEnum};
use crate::core::lifecycle::OutcomeDecodeError;
use crate::failure_cause::FailureCause;
use crate::monitoring::{MonitoringQueryError, TaskActionError};
use crate::{BrokerOperationError, BrokerOperationFailedCode, HandleOperationError};

/// The Python type of an instance field of a generated class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotType {
    /// `str`.
    Str,
    /// `bool`.
    Bool,
    /// `uuid.UUID`.
    Uuid,
    /// `str` holding the text of a UUID: a workflow id of the Python client.
    UuidText,
    /// `TaskStatus`, the code catalog of `horsies._catalog.status`.
    TaskStatus,
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

/// An error enum whose instance slot values the derive generates
/// (`#[derive(horsies_core_derive::ErrorClassValues)]`). Each variant with
/// an `exception` slot carries its typed cause.
pub trait ErrorClassValues: ErrorEnum {
    /// The values of the instance slots of this value's class
    /// ([`ErrorEnum::python_class`]), by name, in the class's slot order:
    /// the `code` field of a `Literal` subset, the fields, then
    /// `exception`. The class values (`code` of one member, `retryable`
    /// of a class that never clears) are not instance slots. Pure.
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
    /// The `exception` slot.
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
}

/// The typed cause of a variant with an `exception` slot: the value that
/// the client builds the exception from.
pub trait ExceptionCause {
    /// The exception of this cause. Pure.
    fn exception_value(&self) -> ExceptionValue;
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

/// Every exception class of the core for the Python package, in render
/// order.
pub const EXCEPTION_CLASSES: &[ExceptionClass] = &[OUTCOME_DECODE_ERROR];

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
];

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

    /// A server error of the driver that carries a SQLSTATE and a primary
    /// message, as the server sends it.
    #[derive(Debug)]
    struct ServerError {
        code: &'static str,
        message: &'static str,
    }

    impl std::fmt::Display for ServerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl std::error::Error for ServerError {}

    impl sqlx::error::DatabaseError for ServerError {
        fn message(&self) -> &str {
            self.message
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(std::borrow::Cow::Borrowed(self.code))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    const TASK: Uuid = Uuid::from_u128(0x3f9c_1a2b_4d5e_4f60_8a71_b2c3_d4e5_f607);

    /// The instance slots of `class`, by name, in order. The class values
    /// (`code` of one member, `retryable` of a class that never clears)
    /// are not instance slots.
    fn instance_slot_names(class: &ErrorClass) -> Vec<&'static str> {
        class
            .slots
            .iter()
            .filter_map(|slot| match slot {
                ErrorSlot::CodeMember { .. } | ErrorSlot::NeverRetryable => None,
                ErrorSlot::CodeSubset { .. } => Some("code"),
                ErrorSlot::Field { name, .. } => Some(*name),
                ErrorSlot::Exception { .. } => Some("exception"),
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

    /// Python's `TaskActionDbOperationFailed` of a cancel that met a broker
    /// error: the message holds the error's text, and the exception is the
    /// core's class of each cause with the same text and the error's retry
    /// rule.
    #[test]
    fn a_database_failure_builds_its_exception_from_its_cause_for_each_cause() {
        let cases = [
            (
                BrokerError::Database(sqlx::Error::Database(Box::new(ServerError {
                    code: "57014",
                    message: "canceling statement due to statement timeout",
                }))),
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
                        name: "exception",
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
                name: "exception",
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
    }
}
