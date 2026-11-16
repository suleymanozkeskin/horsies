//! The closed code catalogs of the core, as static data for clients.
//!
//! Each catalog is a code enum with `#[derive(horsies_macros::CodeCatalog)]`.
//! The derive records the members, their wire values and their docs.
//! `horsies-codegen` reads [`CODE_CATALOGS`] and renders the Python enum
//! classes, so a code is defined once, here in Rust.
//!
//! Adding a code enum to the Python package: derive `CodeCatalog` on it, add
//! it to [`CODE_CATALOGS`], and add a test below that pins its wire values
//! to the enum's own text form.

use crate::monitoring::{MonitoringQueryErrorCode, TaskActionErrorCode};
use crate::{
    BrokerErrorCode, ContractCode, HandleErrorCode, OperationalErrorCode, OutcomeCode,
    RetrievalCode, TaskAbsenceReason, TaskAttemptOutcome, TaskSendErrorCode, TaskStatus,
    WorkflowStartErrorCode, WorkflowStatus, WorkflowTaskStatus,
};

/// The Python base class of a generated enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PythonEnumBase {
    /// `class X(str, Enum)`: members compare equal to their text.
    StrEnum,
    /// `class X(Enum)`.
    PlainEnum,
}

/// One member of a catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogMember {
    /// The wire value; the Python member name is the same text.
    pub value: &'static str,
    /// The member doc, from the variant's `///` lines.
    pub doc: &'static str,
}

/// One closed code catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodeCatalog {
    /// The module under `horsies._catalog` that holds the class.
    pub python_module: &'static str,
    pub python_class: &'static str,
    pub python_base: PythonEnumBase,
    /// The class doc, from the enum's `///` lines.
    pub doc: &'static str,
    /// The members in declaration order.
    pub members: &'static [CatalogMember],
}

/// A code enum whose catalog the derive recorded.
pub trait CodeEnum: Sized + 'static {
    /// The catalog of the enum.
    const CATALOG: CodeCatalog;
    /// Every variant, in declaration order, at the index of its member.
    const VARIANTS: &'static [Self];
    /// The wire value of this variant.
    fn wire_value(&self) -> &'static str;
}

/// Every catalog that the Python package gets, in render order.
pub const CODE_CATALOGS: &[CodeCatalog] = &[
    <BrokerErrorCode as CodeEnum>::CATALOG,
    <TaskSendErrorCode as CodeEnum>::CATALOG,
    <WorkflowStartErrorCode as CodeEnum>::CATALOG,
    <HandleErrorCode as CodeEnum>::CATALOG,
    <OperationalErrorCode as CodeEnum>::CATALOG,
    <ContractCode as CodeEnum>::CATALOG,
    <RetrievalCode as CodeEnum>::CATALOG,
    <OutcomeCode as CodeEnum>::CATALOG,
    <TaskStatus as CodeEnum>::CATALOG,
    <TaskAttemptOutcome as CodeEnum>::CATALOG,
    <WorkflowStatus as CodeEnum>::CATALOG,
    <WorkflowTaskStatus as CodeEnum>::CATALOG,
    <MonitoringQueryErrorCode as CodeEnum>::CATALOG,
    <TaskActionErrorCode as CodeEnum>::CATALOG,
    <TaskAbsenceReason as CodeEnum>::CATALOG,
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// The derive's wire values equal the enum's own text form, member by
    /// member and in order.
    fn assert_wire_values<E: CodeEnum>(text_form: impl Fn(&E) -> String) {
        assert_eq!(
            E::VARIANTS.len(),
            E::CATALOG.members.len(),
            "{}",
            E::CATALOG.python_class
        );
        for (variant, member) in E::VARIANTS.iter().zip(E::CATALOG.members) {
            assert_eq!(variant.wire_value(), member.value);
            assert_eq!(
                text_form(variant),
                member.value,
                "{}",
                E::CATALOG.python_class
            );
        }
    }

    #[test]
    fn broker_send_start_and_handle_codes_match_their_display() {
        assert_wire_values::<BrokerErrorCode>(ToString::to_string);
        assert_wire_values::<TaskSendErrorCode>(ToString::to_string);
        assert_wire_values::<WorkflowStartErrorCode>(ToString::to_string);
        assert_wire_values::<HandleErrorCode>(ToString::to_string);
    }

    #[test]
    fn built_in_task_codes_match_their_display() {
        assert_wire_values::<OperationalErrorCode>(ToString::to_string);
        assert_wire_values::<ContractCode>(ToString::to_string);
        assert_wire_values::<RetrievalCode>(ToString::to_string);
        assert_wire_values::<OutcomeCode>(ToString::to_string);
    }

    #[test]
    fn statuses_match_their_display() {
        assert_wire_values::<TaskStatus>(ToString::to_string);
        assert_wire_values::<TaskAttemptOutcome>(ToString::to_string);
        assert_wire_values::<WorkflowStatus>(ToString::to_string);
        assert_wire_values::<WorkflowTaskStatus>(ToString::to_string);
    }

    #[test]
    fn monitoring_codes_match_their_serde_form() {
        assert_wire_values::<MonitoringQueryErrorCode>(|code| {
            serde_plain::to_string(code).expect("a unit variant serializes")
        });
        assert_wire_values::<TaskActionErrorCode>(|code| {
            serde_plain::to_string(code).expect("a unit variant serializes")
        });
    }

    #[test]
    fn absence_reasons_match_their_text() {
        assert_wire_values::<TaskAbsenceReason>(|reason| reason.as_str().to_owned());
    }

    /// The core catalog holds exactly `python`, the members of the Python
    /// catalog of the same name, in Python's order.
    fn assert_python_members<E: CodeEnum>(python: &[&str]) {
        let core: Vec<&str> = E::CATALOG
            .members
            .iter()
            .map(|member| member.value)
            .collect();
        assert_eq!(core, python, "{}", E::CATALOG.python_class);
    }

    #[test]
    fn broker_send_start_and_monitoring_catalogs_equal_python_s() {
        // horsies/core/brokers/result_types.py
        assert_python_members::<BrokerErrorCode>(&[
            "SCHEMA_INIT_FAILED",
            "ENQUEUE_FAILED",
            "PAYLOAD_MISMATCH",
            "IDEMPOTENCY_KEY_CONFLICT",
            "TASK_INFO_QUERY_FAILED",
            "MONITORING_QUERY_FAILED",
            "DB_PING_FAILED",
            "WORKER_PING_FAILED",
            "CLEANUP_FAILED",
            "CLOSE_FAILED",
            "LISTENER_START_FAILED",
            "LISTENER_SUBSCRIBE_FAILED",
            "NO_BROKER",
            "NO_TYPE_AVAILABLE",
            "INVALID_JSON_PAYLOAD",
            "INVALID_TASK_OPTIONS",
            "EVENT_LOOP_LIMIT",
            "ASYNC_CONTEXT",
            "LOOP_RUNNER_FAILED",
            "INVALID_INPUT",
            "LISTENER_DISABLED",
            "WAIT_INSIDE_TASK",
        ]);
        // horsies/core/models/task_send_types.py
        assert_python_members::<TaskSendErrorCode>(&[
            "SEND_SUPPRESSED",
            "ASYNC_CONTEXT",
            "VALIDATION_FAILED",
            "ENQUEUE_FAILED",
            "PAYLOAD_MISMATCH",
            "IDEMPOTENCY_KEY_CONFLICT",
            "PAYLOAD_TOO_LARGE",
            "EVENT_LOOP_LIMIT",
            "LOOP_RUNNER_FAILED",
        ]);
        // horsies/core/workflows/start_types.py
        assert_python_members::<WorkflowStartErrorCode>(&[
            "BROKER_NOT_CONFIGURED",
            "VALIDATION_FAILED",
            "WORKFLOW_ID_CONFLICT",
            "ENQUEUE_FAILED",
            "LOOP_RUNNER_FAILED",
            "EVENT_LOOP_LIMIT",
            "ASYNC_CONTEXT",
        ]);
        // horsies/monitoring/errors.py
        assert_python_members::<MonitoringQueryErrorCode>(&[
            "DB_OPERATION_FAILED",
            "EVENT_LOOP_LIMIT",
            "HISTORY_CONTRACT_VIOLATION",
        ]);
        // horsies/monitoring/task_actions.py
        assert_python_members::<TaskActionErrorCode>(&[
            "TASK_NOT_FOUND",
            "TASK_NOT_CANCELLABLE",
            "TASK_IS_WORKFLOW_TASK",
            "DB_OPERATION_FAILED",
            "EVENT_LOOP_LIMIT",
            "STORED_DATA_INVALID",
        ]);
    }

    #[test]
    fn status_and_absence_catalogs_equal_python_s() {
        // horsies/core/types/status.py
        assert_python_members::<TaskStatus>(&[
            "PENDING",
            "CLAIMED",
            "RUNNING",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "EXPIRED",
        ]);
        assert_python_members::<TaskAttemptOutcome>(&["COMPLETED", "FAILED", "WORKER_FAILURE"]);
        // horsies/core/models/workflow/enums.py
        assert_python_members::<WorkflowStatus>(&[
            "PENDING",
            "RUNNING",
            "COMPLETED",
            "FAILED",
            "PAUSED",
            "CANCELLED",
            "EXPIRED",
        ]);
        assert_python_members::<WorkflowTaskStatus>(&[
            "PENDING",
            "READY",
            "ENQUEUED",
            "RUNNING",
            "COMPLETED",
            "FAILED",
            "SKIPPED",
        ]);
        // horsies/core/history/reads/detail.py
        assert_python_members::<TaskAbsenceReason>(&[
            "PREDATES_RETAINED_HISTORY",
            "WITHIN_RETAINED_HISTORY",
            "NOT_CLASSIFIED",
        ]);
    }

    #[test]
    fn the_handle_catalog_is_python_s_plus_internal_failed() {
        // horsies/core/models/workflow/handle_types.py, then the one member
        // that Python does not have: the workflow engine still folds some
        // errors into INTERNAL_FAILED.
        assert_python_members::<HandleErrorCode>(&[
            "WORKFLOW_NOT_FOUND",
            "DB_OPERATION_FAILED",
            "STORED_DATA_INVALID",
            "RESUME_RECOVERY_FAILED",
            "LOOP_RUNNER_FAILED",
            "EVENT_LOOP_LIMIT",
            "ASYNC_CONTEXT",
            "INTERNAL_FAILED",
        ]);
    }

    #[test]
    fn class_names_are_unique_and_values_are_unique_per_class() {
        let mut classes = HashSet::new();
        for catalog in CODE_CATALOGS {
            assert!(
                classes.insert(catalog.python_class),
                "{}",
                catalog.python_class
            );
            let mut values = HashSet::new();
            for member in catalog.members {
                assert!(
                    values.insert(member.value),
                    "{}.{}",
                    catalog.python_class,
                    member.value
                );
            }
        }
    }

    #[test]
    fn module_and_member_names_are_python_identifiers() {
        let identifier = |text: &str| {
            text.chars()
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
                && text
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        };
        for catalog in CODE_CATALOGS {
            assert!(
                identifier(catalog.python_module),
                "{}",
                catalog.python_module
            );
            assert!(identifier(catalog.python_class), "{}", catalog.python_class);
            for member in catalog.members {
                assert!(identifier(member.value), "{}", member.value);
            }
        }
    }
}
