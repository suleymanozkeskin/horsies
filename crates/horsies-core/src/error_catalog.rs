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

use crate::catalog::{CodeCatalog, CodeEnum};
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
}
