//! The closed code catalogs of the core, as static data for clients.
//!
//! Each catalog is a code enum with `#[derive(horsies_core_derive::CodeCatalog)]`.
//! The derive records the members, their wire values and their docs.
//! `horsies-codegen` reads [`CODE_CATALOGS`] and renders the Python enum
//! classes, so a code is defined once, here in Rust.
//!
//! Adding a code enum to the Python package: derive `CodeCatalog` on it, add
//! it to [`CODE_CATALOGS`], and add a test below that pins its wire values
//! to the enum's own text form. A lifecycle status also marks its terminal
//! variants, and a test pins them to the enum's own terminal rule.
//! [`ErrorCode`] has no other text form: its `HRS-nnn` text is its catalog
//! value.

use crate::monitoring::{MonitoringQueryErrorCode, TaskActionErrorCode};
use crate::{
    BrokerErrorCode, ContractCode, ErrorCode, HandleErrorCode, OperationalErrorCode, OutcomeCode,
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
    /// The Python member name: the variant name in SCREAMING_SNAKE_CASE.
    pub name: &'static str,
    /// The wire value: the Python member name, unless the variant sets
    /// `#[code_catalog(value = "...")]`.
    pub value: &'static str,
    /// The member doc, from the variant's `///` lines.
    pub doc: &'static str,
}

/// The terminal members of a catalog, from the `#[code_catalog(terminal)]`
/// variants of its enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalStates {
    /// The catalog is not a lifecycle: no member is terminal, and the Python
    /// class has no `is_terminal`.
    NotALifecycle,
    /// The catalog is a lifecycle status. The Python class gets the
    /// `is_terminal` property and a module constant with these members.
    Lifecycle {
        /// The Python name of the `frozenset` constant.
        constant: &'static str,
        /// The Python member names of the terminal members, in declaration
        /// order.
        members: &'static [&'static str],
    },
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
    pub terminal_states: TerminalStates,
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
    <ErrorCode as CodeEnum>::CATALOG,
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
        // clients/python/horsies/core/brokers/result_types.py
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
            "ASYNC_CONTEXT",
            "INVALID_INPUT",
            "LISTENER_DISABLED",
            "WAIT_INSIDE_TASK",
        ]);
        // clients/python/horsies/core/models/task_send_types.py
        assert_python_members::<TaskSendErrorCode>(&[
            "SEND_SUPPRESSED",
            "ASYNC_CONTEXT",
            "VALIDATION_FAILED",
            "ENQUEUE_FAILED",
            "PAYLOAD_MISMATCH",
            "IDEMPOTENCY_KEY_CONFLICT",
            "PAYLOAD_TOO_LARGE",
        ]);
        // clients/python/horsies/core/workflows/start_types.py
        assert_python_members::<WorkflowStartErrorCode>(&[
            "BROKER_NOT_CONFIGURED",
            "VALIDATION_FAILED",
            "WORKFLOW_ID_CONFLICT",
            "ENQUEUE_FAILED",
            "ASYNC_CONTEXT",
        ]);
        // clients/python/horsies/monitoring/errors.py
        assert_python_members::<MonitoringQueryErrorCode>(&[
            "DB_OPERATION_FAILED",
            "HISTORY_CONTRACT_VIOLATION",
        ]);
        // clients/python/horsies/monitoring/task_actions.py
        assert_python_members::<TaskActionErrorCode>(&[
            "TASK_NOT_FOUND",
            "TASK_NOT_CANCELLABLE",
            "TASK_IS_WORKFLOW_TASK",
            "DB_OPERATION_FAILED",
            "STORED_DATA_INVALID",
        ]);
    }

    #[test]
    fn built_in_task_code_catalogs_equal_python_s() {
        // clients/python/horsies/core/models/tasks.py; WORKFLOW_START_INCOMPLETE is the one
        // approved addition, last in both catalogs.
        assert_python_members::<OperationalErrorCode>(&[
            "UNHANDLED_EXCEPTION",
            "TASK_EXCEPTION",
            "WORKER_CRASHED",
            "BROKER_ERROR",
            "WORKER_RESOLUTION_ERROR",
            "WORKER_SERIALIZATION_ERROR",
            "RESULT_DESERIALIZATION_ERROR",
            "WORKFLOW_ENQUEUE_FAILED",
            "SUBWORKFLOW_LOAD_FAILED",
            "WORKFLOW_RECOVERY_LIMIT_REACHED",
            "SUBWORKFLOW_DEPTH_LIMIT_REACHED",
            "TASK_BLOCKED_LOOP",
            "NEIGHBOUR_BLOCKED_LOOP",
            "WORKFLOW_START_INCOMPLETE",
        ]);
        assert_python_members::<ContractCode>(&[
            "RETURN_TYPE_MISMATCH",
            "WORKFLOW_CTX_MISSING_ID",
            "NO_TYPE_AVAILABLE",
        ]);
        assert_python_members::<RetrievalCode>(&[
            "WAIT_TIMEOUT",
            "TASK_NOT_FOUND",
            "WORKFLOW_NOT_FOUND",
            "RESULT_NOT_AVAILABLE",
            "RESULT_NOT_READY",
            "ASYNC_CONTEXT",
            "INVALID_INPUT",
            "WAIT_INSIDE_TASK",
        ]);
        assert_python_members::<OutcomeCode>(&[
            "TASK_CANCELLED",
            "TASK_EXPIRED",
            "TASK_TIMEOUT",
            "WORKFLOW_PAUSED",
            "WORKFLOW_EXPIRED",
            "WORKFLOW_FAILED",
            "WORKFLOW_CANCELLED",
            "UPSTREAM_SKIPPED",
            "SUBWORKFLOW_FAILED",
            "WORKFLOW_SUCCESS_CASE_NOT_MET",
            "WORKFLOW_OUTPUT_SKIPPED",
        ]);
    }

    #[test]
    fn the_all_lists_of_the_task_code_families_follow_the_catalog() {
        let catalog = |catalog: CodeCatalog| -> Vec<String> {
            catalog
                .members
                .iter()
                .map(|member| member.value.to_owned())
                .collect()
        };
        assert_eq!(
            OperationalErrorCode::ALL
                .map(|code| code.to_string())
                .to_vec(),
            catalog(<OperationalErrorCode as CodeEnum>::CATALOG)
        );
        assert_eq!(
            ContractCode::ALL.map(|code| code.to_string()).to_vec(),
            catalog(<ContractCode as CodeEnum>::CATALOG)
        );
        assert_eq!(
            RetrievalCode::ALL.map(|code| code.to_string()).to_vec(),
            catalog(<RetrievalCode as CodeEnum>::CATALOG)
        );
        assert_eq!(
            OutcomeCode::ALL.map(|code| code.to_string()).to_vec(),
            catalog(<OutcomeCode as CodeEnum>::CATALOG)
        );
    }

    #[test]
    fn status_and_absence_catalogs_equal_python_s() {
        // clients/python/horsies/core/types/status.py
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
        // clients/python/horsies/core/models/workflow/enums.py
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
        // clients/python/horsies/_catalog/history.py
        assert_python_members::<TaskAbsenceReason>(&[
            "PREDATES_RETAINED_HISTORY",
            "WITHIN_RETAINED_HISTORY",
            "NOT_CLASSIFIED",
        ]);
    }

    #[test]
    fn the_handle_catalog_is_python_s() {
        // clients/python/horsies/core/models/workflow/handle_types.py
        assert_python_members::<HandleErrorCode>(&[
            "WORKFLOW_NOT_FOUND",
            "DB_OPERATION_FAILED",
            "STORED_DATA_INVALID",
            "RESUME_RECOVERY_FAILED",
            "ASYNC_CONTEXT",
        ]);
    }

    /// The constant and the terminal members of a lifecycle catalog.
    fn lifecycle(catalog: CodeCatalog) -> (&'static str, &'static [&'static str]) {
        match catalog.terminal_states {
            TerminalStates::Lifecycle { constant, members } => (constant, members),
            TerminalStates::NotALifecycle => {
                panic!("{} records no terminal members", catalog.python_class)
            }
        }
    }

    /// The terminal members that the derive recorded are the names of the
    /// variants for which `is_terminal` holds, in declaration order.
    fn assert_terminal_members_follow<E: CodeEnum>(is_terminal: impl Fn(&E) -> bool) {
        let rule: Vec<&str> = E::VARIANTS
            .iter()
            .zip(E::CATALOG.members)
            .filter(|(variant, _member)| is_terminal(variant))
            .map(|(_variant, member)| member.name)
            .collect();
        let (_constant, members) = lifecycle(E::CATALOG);
        assert_eq!(members, rule.as_slice(), "{}", E::CATALOG.python_class);
    }

    #[test]
    fn status_terminal_members_follow_the_core_rule() {
        assert_terminal_members_follow::<TaskStatus>(|status| status.is_terminal());
        assert_terminal_members_follow::<WorkflowStatus>(|status| status.is_terminal());
        assert_terminal_members_follow::<WorkflowTaskStatus>(|status| status.is_terminal());
    }

    #[test]
    fn status_terminal_members_equal_python_s() {
        // clients/python/horsies/core/types/status.py
        let (constant, members) = lifecycle(<TaskStatus as CodeEnum>::CATALOG);
        assert_eq!(constant, "TASK_TERMINAL_STATES");
        assert_eq!(members, ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"]);
        // clients/python/horsies/core/models/workflow/enums.py
        let (constant, members) = lifecycle(<WorkflowStatus as CodeEnum>::CATALOG);
        assert_eq!(constant, "WORKFLOW_TERMINAL_STATES");
        assert_eq!(members, ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"]);
        let (constant, members) = lifecycle(<WorkflowTaskStatus as CodeEnum>::CATALOG);
        assert_eq!(constant, "WORKFLOW_TASK_TERMINAL_STATES");
        assert_eq!(members, ["COMPLETED", "FAILED", "SKIPPED"]);
    }

    #[test]
    fn only_the_three_status_catalogs_are_lifecycles() {
        let lifecycles: Vec<&str> = CODE_CATALOGS
            .iter()
            .filter(|catalog| match catalog.terminal_states {
                TerminalStates::Lifecycle { .. } => true,
                TerminalStates::NotALifecycle => false,
            })
            .map(|catalog| catalog.python_class)
            .collect();
        assert_eq!(
            lifecycles,
            ["TaskStatus", "WorkflowStatus", "WorkflowTaskStatus"]
        );
    }

    #[test]
    fn class_names_are_unique_and_names_and_values_are_unique_per_class() {
        let mut classes = HashSet::new();
        for catalog in CODE_CATALOGS {
            assert!(
                classes.insert(catalog.python_class),
                "{}",
                catalog.python_class
            );
            let mut names = HashSet::new();
            let mut values = HashSet::new();
            for member in catalog.members {
                assert!(
                    names.insert(member.name),
                    "{}.{}",
                    catalog.python_class,
                    member.name
                );
                assert!(
                    values.insert(member.value),
                    "{}.{} = {}",
                    catalog.python_class,
                    member.name,
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
                assert!(identifier(member.name), "{}", member.name);
            }
            match catalog.terminal_states {
                TerminalStates::Lifecycle { constant, .. } => {
                    assert!(identifier(constant), "{constant}");
                }
                TerminalStates::NotALifecycle => {}
            }
        }
    }
}
