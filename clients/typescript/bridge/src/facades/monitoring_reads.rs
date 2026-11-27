//! The monitoring reads through the core (`horsies_core::monitoring`).
//!
//! - A read answers the JSON text of its core model, as the Python bridge
//!   does (`MonitoringJson`); the package decodes the text once into its
//!   read types, and the web handler writes the same text.
//! - An expected failure is the raw record of the core's
//!   `MonitoringQueryError` (`MonitoringQueryDbOperationFailed` or
//!   `MonitoringQueryHistoryContractViolation`), with the read as the
//!   subject of its exception. A failed connect is
//!   `MonitoringQueryDbOperationFailed` with the text `<read> failed:
//!   <cause>` and the retry rule of the cause.
//! - A query value that the core refuses is answered at once, with no
//!   connect and no statement (`monitoring_query`).
//! - Each read sends one statement and writes nothing. A monitoring read
//!   does not install the schema, as the reads of the Python and Rust
//!   clients do not.

use std::future::Future;
use std::sync::Arc;

use horsies_core::monitoring::{
    get_task_detail, get_workflow_node, get_workflow_run, list_schedules, list_tasks,
    list_workflow_names, list_workflow_runs, task_breakdown, task_facets, task_status_counts,
    MonitoringQueryError, TaskAbsenceReason, TaskDetailRead,
};
use horsies_core::PostgresBroker;
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::CallKind;
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::Envelope;
use crate::facades::answer::answered_at_once;
use crate::facades::monitoring_query::{
    task_breakdown_query, task_facets_query, task_list_query, task_status_counts_query,
    whole_number, workflow_runs_query, QueryField, QueryStop, TaskBreakdownArg, TaskFacetsArg,
    TaskListArg, TaskStatusCountsArg, WorkflowRunsArg,
};
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::values::boundary::{text_of_uuid, uuid_of_text, BoundaryRefusal};
use crate::values::catalog_values::RawCatalogError;

/// The rule of a workflow task index.
const NODE_INDEX_RULE: &str = "a whole number from -2147483648 to 2147483647";

/// The outcome of one list, aggregate or workflow read.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MonitoringReadOutcome {
    /// The JSON text of the core model. A workflow run or node that no row
    /// holds is the text `null`.
    Json { text: String },
    /// The core refused a value of the query; no connect and no statement
    /// ran.
    Refused { field: QueryField, reason: String },
    /// The raw record of the core's `MonitoringQueryError`.
    Failed { error: RawCatalogError },
}

/// Why no table holds a task, by its wire value.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskAbsenceReasonName {
    PredatesRetainedHistory,
    WithinRetainedHistory,
    NotClassified,
}

impl From<TaskAbsenceReason> for TaskAbsenceReasonName {
    fn from(reason: TaskAbsenceReason) -> Self {
        match reason {
            TaskAbsenceReason::PredatesRetainedHistory => Self::PredatesRetainedHistory,
            TaskAbsenceReason::WithinRetainedHistory => Self::WithinRetainedHistory,
            TaskAbsenceReason::NotClassified => Self::NotClassified,
        }
    }
}

/// The outcome of the task detail read.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskDetailOutcome {
    /// The JSON text of the core's `TaskDetail`.
    Json { text: String },
    /// Neither the live table nor the history holds the task.
    Absent {
        task_id: String,
        reason: TaskAbsenceReasonName,
    },
    /// The raw record of the core's `MonitoringQueryError`.
    Failed { error: RawCatalogError },
}

/// The subject of the task detail read.
const DETAIL_SUBJECT: &str = "task detail query";

/// The JSON text of a read's model, or the read's error.
type EncodedRead = Result<Result<String, serde_json::Error>, MonitoringQueryError>;

/// The task status counts of the window: all seven statuses, in a fixed
/// order. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
pub fn read_task_status_counts<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    query: TaskStatusCountsArg,
) -> napi::Result<Object<'env>> {
    let subject = "task status counts query";
    query_read(
        env,
        handle,
        subject,
        || task_status_counts_query(query),
        move |broker, query| async move {
            task_status_counts(&broker, &query)
                .await
                .map(|counts| serde_json::to_string(&counts))
        },
    )
}

/// The facets of the window: the workers, task names, queues and error
/// codes with their counts. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn read_task_facets<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    query: TaskFacetsArg,
) -> napi::Result<Object<'env>> {
    let subject = "task facets query";
    query_read(
        env,
        handle,
        subject,
        || task_facets_query(&query),
        move |broker, query| async move {
            task_facets(&broker, &query)
                .await
                .map(|facets| serde_json::to_string(&facets))
        },
    )
}

/// The task counts by worker, task name or queue. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
pub fn read_task_breakdown<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    query: TaskBreakdownArg,
) -> napi::Result<Object<'env>> {
    let subject = "task breakdown query";
    query_read(
        env,
        handle,
        subject,
        || task_breakdown_query(query),
        move |broker, query| async move {
            task_breakdown(&broker, &query)
                .await
                .map(|breakdown| serde_json::to_string(&breakdown))
        },
    )
}

/// One page of the task list, and the count of the matching tasks. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
pub fn read_task_list<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    query: TaskListArg,
) -> napi::Result<Object<'env>> {
    let subject = "task list query";
    query_read(
        env,
        handle,
        subject,
        || task_list_query(query),
        move |broker, query| async move {
            list_tasks(&broker, &query)
                .await
                .map(|page| serde_json::to_string(&page))
        },
    )
}

/// The names of the workflows that have runs, at most 10,000. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
pub fn read_workflow_names<'env>(
    env: &'env Env,
    handle: &CoreHandle,
) -> napi::Result<Object<'env>> {
    let subject = "workflow names query";
    query_read(
        env,
        handle,
        subject,
        || Ok(()),
        move |broker, ()| async move {
            list_workflow_names(&broker)
                .await
                .map(|names| serde_json::to_string(&names))
        },
    )
}

/// The newest workflow runs, by name and status. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
pub fn read_workflow_runs<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    query: WorkflowRunsArg,
) -> napi::Result<Object<'env>> {
    let subject = "workflow runs query";
    query_read(
        env,
        handle,
        subject,
        || workflow_runs_query(query),
        move |broker, query| async move {
            list_workflow_runs(&broker, &query)
                .await
                .map(|runs| serde_json::to_string(&runs))
        },
    )
}

/// One workflow run with its nodes and edges; the text `null` when no row
/// holds the run. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn read_workflow_run<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
) -> napi::Result<Object<'env>> {
    let subject = "workflow run detail query";
    let build = || uuid_of_text("workflowId", &workflow_id).map_err(QueryStop::Defect);
    query_read(
        env,
        handle,
        subject,
        build,
        move |broker, workflow_id| async move {
            get_workflow_run(&broker, workflow_id)
                .await
                .map(|run| serde_json::to_string(&run))
        },
    )
}

/// One node of a workflow run; the text `null` when no row holds it.
/// Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn read_workflow_node<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    workflow_id: String,
    node_index: f64,
) -> napi::Result<Object<'env>> {
    let subject = "workflow node detail query";
    let build = || node_address(&workflow_id, node_index).map_err(QueryStop::Defect);
    query_read(
        env,
        handle,
        subject,
        build,
        move |broker, (workflow_id, node_index)| async move {
            get_workflow_node(&broker, workflow_id, node_index)
                .await
                .map(|node| serde_json::to_string(&node))
        },
    )
}

/// The state of every schedule, at most 10,000. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringReadOutcome>>")]
pub fn read_schedules<'env>(env: &'env Env, handle: &CoreHandle) -> napi::Result<Object<'env>> {
    let subject = "schedule state query";
    query_read(
        env,
        handle,
        subject,
        || Ok(()),
        move |broker, ()| async move {
            list_schedules(&broker)
                .await
                .map(|schedules| serde_json::to_string(&schedules))
        },
    )
}

/// One task with its attempts, or why no table holds it. Reads.
#[napi(ts_return_type = "Promise<NativeEnvelope<TaskDetailOutcome>>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn read_task_detail<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    task_id: String,
) -> napi::Result<Object<'env>> {
    match guarded_start(DETAIL_SUBJECT, || Ok(uuid_of_text("taskId", &task_id))) {
        Err(thrown) => answered_at_once(env, Envelope::<TaskDetailOutcome>::Thrown(thrown)),
        Ok(Err(refusal)) => {
            answered_at_once(env, Envelope::<TaskDetailOutcome>::Thrown(refusal.into()))
        }
        Ok(Ok(task_id)) => {
            let connection = handle.connection();
            spawn_operation(env, DETAIL_SUBJECT, CallKind::Read, move |_core| {
                task_detail_operation(connection, task_id)
            })
        }
    }
}

/// Build the query of a read on the JavaScript thread, then run the read on
/// the core runtime. A refused query answers at once; a defect throws
/// `CLIENT_DEFECT`.
fn query_read<'env, Query, Build, Read, Encoded>(
    env: &'env Env,
    handle: &CoreHandle,
    subject: &'static str,
    build: Build,
    read: Read,
) -> napi::Result<Object<'env>>
where
    Query: Send + 'static,
    Build: FnOnce() -> Result<Query, QueryStop>,
    Read: FnOnce(Arc<PostgresBroker>, Query) -> Encoded + Send + 'static,
    Encoded: Future<Output = EncodedRead> + Send + 'static,
{
    match guarded_start(subject, || Ok(build())) {
        Err(thrown) => answered_at_once(env, Envelope::<MonitoringReadOutcome>::Thrown(thrown)),
        Ok(Err(QueryStop::Defect(refusal))) => answered_at_once(
            env,
            Envelope::<MonitoringReadOutcome>::Thrown(refusal.into()),
        ),
        Ok(Err(QueryStop::Refused(refusal))) => answered_at_once(
            env,
            Envelope::Value(MonitoringReadOutcome::Refused {
                field: refusal.field,
                reason: refusal.reason,
            }),
        ),
        Ok(Ok(query)) => {
            let connection = handle.connection();
            spawn_operation(env, subject, CallKind::Read, move |_core| {
                json_read(connection, subject, move |broker| read(broker, query))
            })
        }
    }
}

/// Connect the handle's broker, run `read`, and give the JSON text of its
/// model or the raw record of its failure.
async fn json_read<Read, Encoded>(
    connection: BrokerConnection,
    subject: &'static str,
    read: Read,
) -> MonitoringReadOutcome
where
    Read: FnOnce(Arc<PostgresBroker>) -> Encoded,
    Encoded: Future<Output = EncodedRead>,
{
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return read_failed(
                subject,
                &MonitoringQueryError::connect_failed(subject, error),
            )
        }
    };
    match read(broker).await {
        Ok(encoded) => MonitoringReadOutcome::Json {
            text: model_text(subject, encoded),
        },
        Err(error) => read_failed(subject, &error),
    }
}

/// The failure of the read `subject`. Pure.
fn read_failed(subject: &str, error: &MonitoringQueryError) -> MonitoringReadOutcome {
    MonitoringReadOutcome::Failed {
        error: RawCatalogError::of(subject, error),
    }
}

/// The JSON text of a model. A core model that does not encode is a defect
/// of the core: it panics, and the guard of the call answers `CORE_PANIC`.
fn model_text(subject: &str, encoded: Result<String, serde_json::Error>) -> String {
    match encoded {
        Ok(text) => text,
        Err(error) => panic!("the model of the {subject} does not encode as JSON: {error}"),
    }
}

/// The task detail read on the core runtime.
async fn task_detail_operation(connection: BrokerConnection, task_id: Uuid) -> TaskDetailOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return TaskDetailOutcome::Failed {
                error: RawCatalogError::of(
                    DETAIL_SUBJECT,
                    &MonitoringQueryError::connect_failed(DETAIL_SUBJECT, error),
                ),
            }
        }
    };
    detail_outcome(get_task_detail(&broker, task_id).await)
}

/// The outcome of the core's task detail answer. Pure.
fn detail_outcome(read: Result<TaskDetailRead, MonitoringQueryError>) -> TaskDetailOutcome {
    match read {
        Ok(TaskDetailRead::Found(detail)) => TaskDetailOutcome::Json {
            text: model_text(DETAIL_SUBJECT, serde_json::to_string(&detail)),
        },
        Ok(TaskDetailRead::NotFound(absent)) => TaskDetailOutcome::Absent {
            task_id: text_of_uuid(absent.task_id),
            reason: absent.reason.into(),
        },
        Err(error) => TaskDetailOutcome::Failed {
            error: RawCatalogError::of(DETAIL_SUBJECT, &error),
        },
    }
}

/// The workflow id and the node index of a node. Pure.
fn node_address(workflow_id: &str, node_index: f64) -> Result<(Uuid, i32), BoundaryRefusal> {
    let workflow_id = uuid_of_text("workflowId", workflow_id)?;
    let index = whole_number("nodeIndex", node_index)?;
    let index =
        i32::try_from(index).map_err(|_| BoundaryRefusal::new("nodeIndex", NODE_INDEX_RULE))?;
    Ok((workflow_id, index))
}

#[cfg(test)]
mod tests {
    use horsies_core::monitoring::{
        HistoryContractSource, MonitoringQueryErrorSource, TaskDetailNotFound,
    };
    use horsies_core::{BrokerError, HistoryError};

    use super::*;

    #[test]
    fn a_task_that_no_table_holds_is_absent_with_its_reason() {
        let task_id = Uuid::from_u128(7);
        let outcome = detail_outcome(Ok(TaskDetailRead::NotFound(TaskDetailNotFound {
            task_id,
            reason: TaskAbsenceReason::WithinRetainedHistory,
        })));
        assert_eq!(
            outcome,
            TaskDetailOutcome::Absent {
                task_id: text_of_uuid(task_id),
                reason: TaskAbsenceReasonName::WithinRetainedHistory,
            }
        );
    }

    #[test]
    fn each_absence_reason_of_the_core_has_its_name() {
        let cases = [
            (
                TaskAbsenceReason::PredatesRetainedHistory,
                TaskAbsenceReasonName::PredatesRetainedHistory,
            ),
            (
                TaskAbsenceReason::WithinRetainedHistory,
                TaskAbsenceReasonName::WithinRetainedHistory,
            ),
            (
                TaskAbsenceReason::NotClassified,
                TaskAbsenceReasonName::NotClassified,
            ),
        ];
        for (reason, name) in cases {
            assert_eq!(TaskAbsenceReasonName::from(reason), name);
        }
    }

    #[test]
    fn a_failed_detail_read_is_the_raw_record_of_the_core_error() {
        let error =
            MonitoringQueryError::connect_failed(DETAIL_SUBJECT, BrokerError::ListenerClosed);
        match detail_outcome(Err(error)) {
            TaskDetailOutcome::Failed { error } => {
                assert_eq!(error.class, "MonitoringQueryDatabaseOperationFailed");
            }
            other => panic!("a failed read, got {other:?}"),
        }
    }

    #[test]
    fn a_failed_read_names_its_class_and_slots() {
        let error =
            MonitoringQueryError::connect_failed("task list query", BrokerError::ListenerClosed);
        match read_failed("task list query", &error) {
            MonitoringReadOutcome::Failed { error } => {
                assert_eq!(error.class, "MonitoringQueryDatabaseOperationFailed");
                let names: Vec<&str> = error.slots.iter().map(|slot| slot.name.as_str()).collect();
                assert_eq!(names, ["message", "retryable", "cause"]);
            }
            other => panic!("a failed read, got {other:?}"),
        }
    }

    #[test]
    fn a_history_contract_violation_is_its_raw_record_with_no_retryable_slot() {
        let error = MonitoringQueryError::HistoryContractViolation {
            message: "history contract violation: status DONE".to_owned(),
            cause: HistoryContractSource(MonitoringQueryErrorSource::History(
                HistoryError::Contract("status DONE".to_owned()),
            )),
        };
        match read_failed("task list query", &error) {
            MonitoringReadOutcome::Failed { error } => {
                assert_eq!(error.class, "MonitoringQueryHistoryContractViolation");
                let names: Vec<&str> = error.slots.iter().map(|slot| slot.name.as_str()).collect();
                assert_eq!(names, ["message", "cause"]);
            }
            other => panic!("a failed read, got {other:?}"),
        }
    }

    #[test]
    fn a_node_address_is_a_uuid_and_an_i32() {
        let text = "01890a5d-ac96-774b-bcce-b302099a8057";
        let (workflow_id, index) = node_address(text, 3.0).expect("a valid address");
        assert_eq!(text_of_uuid(workflow_id), text);
        assert_eq!(index, 3);
    }

    #[test]
    fn a_node_index_outside_i32_is_a_defect() {
        let text = "01890a5d-ac96-774b-bcce-b302099a8057";
        assert_eq!(
            node_address(text, 2_147_483_648.0),
            Err(BoundaryRefusal::new("nodeIndex", NODE_INDEX_RULE))
        );
    }

    #[test]
    #[should_panic(expected = "does not encode as JSON")]
    fn a_model_that_does_not_encode_panics() {
        let error = serde_json::from_str::<u8>("x").expect_err("x is not JSON");
        let _text = model_text("task list query", Err(error));
    }
}
