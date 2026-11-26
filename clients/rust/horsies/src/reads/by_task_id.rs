//! Reads by task id on the app, as the Python app offers them:
//! `get_result`, `get_raw_result` and `get_task_info`.
//!
//! A result wait checks its limit first (`InvalidInput` above
//! `MAX_WAIT_LIMIT_MS`). In a process that runs task bodies it is refused
//! with `WaitInsideTask`. Both come before any statement. The task info is
//! not a wait.

use std::sync::Arc;

use horsies_core::handle::CoreHandle;
use horsies_core::{
    child_producer, process_role, ChildProducer, ProcessRole, RawResultRead, RawResultRecord,
    TaskStatus, WaitLimit,
};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::handle::{checked_task_wait, read_task_info};
use crate::process_state::refuse_in_a_fork;
use crate::{
    CustomErrorCode, Horsies, ReadError, TaskHandle, TaskInfo, TaskInfoParts, TaskOutcome,
};

/// The stored result of a task, not read with any type (Python
/// `get_raw_result`): for tools that read results without the task's
/// types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredResult {
    pub task_id: Uuid,
    pub task_name: String,
    /// The terminal status of the task.
    pub status: TaskStatus,
    /// The stored result envelope as JSON text:
    /// `{"__h_task_result__": true, "ok": <value>, "err": <error>}`.
    pub envelope_json: String,
}

impl Horsies {
    /// A handle of task `task_id`, sent earlier, read with the output type
    /// `T` and the code type `E` (Python `TaskHandle(task_id, app)`). Starts
    /// nothing.
    pub fn task_handle_of<T, E>(&self, task_id: Uuid) -> TaskHandle<T, E> {
        TaskHandle::new(task_id, self.reads_handle())
    }

    /// The outcome of task `task_id`, read with `T` and `E`, waiting up to
    /// `limit` while it has not finished (Python `get_result`). Nothing is
    /// kept between calls.
    ///
    /// Errors: [`ReadError`], as `TaskHandle::get`.
    pub async fn get_result<T, E>(
        &self,
        task_id: Uuid,
        limit: WaitLimit,
    ) -> Result<TaskOutcome<T, E>, ReadError>
    where
        T: DeserializeOwned + Clone,
        E: CustomErrorCode,
    {
        self.task_handle_of::<T, E>(task_id).get(limit).await
    }

    /// The stored result of task `task_id`, waiting up to `limit` while it
    /// has not finished (Python `get_raw_result`).
    ///
    /// Errors: `InvalidInput` for a `Within` limit above
    /// `MAX_WAIT_LIMIT_MS`, as the first check; `WaitInsideTask` in a
    /// process that runs task bodies; both before any statement.
    /// `TaskNotFound`; `WaitTimedOut`; `ResultNotAvailable` for a terminal
    /// task that stored no result; `Database`.
    pub async fn get_raw_result(
        &self,
        task_id: Uuid,
        limit: WaitLimit,
    ) -> Result<StoredResult, ReadError> {
        let wait = checked_task_wait(task_id, limit)?;
        if process_role() == ProcessRole::RunsTaskBodies {
            return Err(ReadError::wait_inside_task(&format!(
                "Horsies.get_raw_result() for task {task_id}"
            )));
        }
        refuse_in_a_fork(format_args!("Horsies::get_raw_result for task {task_id}"));
        let broker = self
            .reads_handle()
            .initialized_broker()
            .await
            .map_err(|error| ReadError::of_broker(&error))?;
        let record = broker
            .get_raw_result_record(task_id, wait)
            .await
            .map_err(|error| ReadError::of_operation(task_id, error))?;
        match record {
            RawResultRead::Absent { reason } => Err(ReadError::TaskNotFound { task_id, reason }),
            RawResultRead::Found(record) => stored_result(record),
        }
    }

    /// The info of task `task_id` with the `parts` asked for (Python
    /// `get_task_info`): the live row or the history record, attempts
    /// newest first, the stored result read as a `TaskResult`.
    ///
    /// Errors: `TaskNotFound`; `StoredResultUnreadable` for a stored result
    /// that is not a task result; `Database`.
    pub async fn get_task_info(
        &self,
        task_id: Uuid,
        parts: TaskInfoParts,
    ) -> Result<TaskInfo, ReadError> {
        read_task_info(&self.reads_handle(), task_id, parts).await
    }

    /// The handle a read goes through: the child's producer handle in a
    /// worker child, else the app's.
    pub(crate) fn reads_handle(&self) -> Arc<CoreHandle> {
        match child_producer() {
            ChildProducer::InChild(handle) => handle,
            ChildProducer::NotInAChild => Arc::clone(self.handle()),
        }
    }
}

/// The stored result of a record, or why there is none. Pure.
fn stored_result(record: RawResultRecord) -> Result<StoredResult, ReadError> {
    let task_id = record.task_id;
    match (record.status, record.raw_result) {
        (
            status @ (TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::Expired
            | TaskStatus::Cancelled),
            Some(envelope),
        ) => Ok(StoredResult {
            task_id,
            task_name: record.task_name,
            status,
            envelope_json: envelope.as_str().to_owned(),
        }),
        (
            status @ (TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::Expired
            | TaskStatus::Cancelled),
            None,
        ) => Err(ReadError::ResultNotAvailable { task_id, status }),
        (status @ (TaskStatus::Pending | TaskStatus::Claimed | TaskStatus::Running), _) => {
            Err(ReadError::WaitTimedOut { task_id, status })
        }
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::StoredEnvelopeText;
    use serde_json::{json, Value};

    use super::*;

    fn envelope(value: &Value) -> StoredEnvelopeText {
        StoredEnvelopeText::parse(value.to_string())
            .unwrap()
            .expect("an envelope is an object")
    }

    fn record(status: TaskStatus, raw_result: Option<&Value>) -> RawResultRecord {
        RawResultRecord {
            task_id: Uuid::from_u128(9),
            task_name: "add".to_owned(),
            status,
            raw_result: raw_result.map(envelope),
        }
    }

    #[test]
    fn a_terminal_task_gives_its_envelope_text() {
        let stored = stored_result(record(
            TaskStatus::Completed,
            Some(&json!({"__h_task_result__": true, "ok": 3, "err": null})),
        ))
        .unwrap();
        assert_eq!(stored.status, TaskStatus::Completed);
        let read: Value = serde_json::from_str(&stored.envelope_json).unwrap();
        assert_eq!(read["ok"], json!(3));
    }

    #[test]
    fn a_terminal_task_without_a_result_is_not_available() {
        for status in [
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::Expired,
            TaskStatus::Cancelled,
        ] {
            assert_eq!(
                stored_result(record(status, None)),
                Err(ReadError::ResultNotAvailable {
                    task_id: Uuid::from_u128(9),
                    status
                })
            );
        }
    }

    #[test]
    fn an_unfinished_task_at_the_limit_timed_out() {
        assert_eq!(
            stored_result(record(TaskStatus::Running, None)),
            Err(ReadError::WaitTimedOut {
                task_id: Uuid::from_u128(9),
                status: TaskStatus::Running
            })
        );
    }
}
