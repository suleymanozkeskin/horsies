//! The operator cancel of one task that no workflow holds:
//! `horsies_cancel_task` (`horsies_core::monitoring::cancel_task`).
//!
//! - The outcome is the cancelled record, or the raw record of the core's
//!   `TaskActionError` (`TaskActionNotFound`, `TaskActionNotCancellable`,
//!   `TaskActionIsWorkflowTask`, `TaskActionDatabaseOperationFailed` or
//!   `TaskActionStoredDataInvalid`), with `cancel_task` as the subject of its
//!   exception. The core gives each class its message.
//! - A failed connect is `TaskActionDatabaseOperationFailed`: nothing was
//!   sent.
//! - The cancel writes in one statement. A stop of the core during the
//!   statement leaves its outcome unknown (`CallKind::Write`); a repeat tells
//!   which: a committed cancel answers `TaskActionNotCancellable` with
//!   `CANCELLED`.

use horsies_core::catalog::CodeEnum;
use horsies_core::monitoring::{
    cancel_task, CancelPolicy as CoreCancelPolicy, TaskActionError, TaskActionResult, TaskCancelled,
};
use napi::bindgen_prelude::Object;
use napi::Env;
use napi_derive::napi;
use uuid::Uuid;

use crate::call::guard::CallKind;
use crate::call::operation::{guarded_start, spawn_operation};
use crate::call::thrown::Envelope;
use crate::facades::answer::answered_at_once;
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::values::boundary::{text_of_uuid, uuid_of_text};
use crate::values::records::Outcome;

/// The subject of the cancel and of the exception of its failure.
const SUBJECT: &str = "cancel_task";

/// Which statuses a cancel moves to `CANCELLED`.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelPolicy {
    /// `PENDING`, `CLAIMED` and `RUNNING`. A running body sees its signal
    /// abort with the cause `OPERATOR`.
    IncludeRunning,
    /// `PENDING` and `CLAIMED` only: no user code ran.
    PendingOrClaimedOnly,
}

impl CancelPolicy {
    /// The core's cancel policy of the same name. Pure.
    const fn core_policy(self) -> CoreCancelPolicy {
        match self {
            Self::IncludeRunning => CoreCancelPolicy::IncludeRunning,
            Self::PendingOrClaimedOnly => CoreCancelPolicy::PendingOrClaimedOnly,
        }
    }
}

/// The task that the cancel moved to `CANCELLED`. `was_status` is the wire
/// value of the status it left.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskCancelledRecord {
    pub task_id: String,
    pub was_status: String,
}

/// Cancel one task that no workflow holds. Writes, in one statement.
#[napi(
    js_name = "cancelTask",
    ts_return_type = "Promise<NativeEnvelope<NativeOutcome<TaskCancelledRecord>>>"
)]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn cancel_task_of_handle<'env>(
    env: &'env Env,
    handle: &CoreHandle,
    task_id: String,
    policy: CancelPolicy,
) -> napi::Result<Object<'env>> {
    match guarded_start(SUBJECT, || Ok(uuid_of_text("taskId", &task_id))) {
        Err(thrown) => answered_at_once(
            env,
            Envelope::<Outcome<TaskCancelledRecord>>::Thrown(thrown),
        ),
        Ok(Err(refusal)) => answered_at_once(
            env,
            Envelope::<Outcome<TaskCancelledRecord>>::Thrown(refusal.into()),
        ),
        Ok(Ok(task_id)) => {
            let connection = handle.connection();
            spawn_operation(env, SUBJECT, CallKind::Write, move |_core| {
                cancel_operation(connection, task_id, policy)
            })
        }
    }
}

/// Connect the handle's broker and run the cancel.
async fn cancel_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    policy: CancelPolicy,
) -> Outcome<TaskCancelledRecord> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return Outcome::failed(SUBJECT, &TaskActionError::cancel_failed(task_id, error))
        }
    };
    cancel_outcome(cancel_task(&broker, task_id, policy.core_policy()).await)
}

/// The outcome of the core's answer to a cancel. Pure.
fn cancel_outcome(answer: TaskActionResult<TaskCancelled>) -> Outcome<TaskCancelledRecord> {
    match answer {
        Ok(cancelled) => Outcome::Done(TaskCancelledRecord {
            task_id: text_of_uuid(cancelled.task_id),
            was_status: cancelled.was_status.wire_value().to_owned(),
        }),
        Err(error) => Outcome::failed(SUBJECT, &error),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::{BrokerError, TaskStatus};

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(3)
    }

    #[test]
    fn a_cancel_is_a_record_with_the_status_as_its_wire_value() {
        let outcome = cancel_outcome(Ok(TaskCancelled {
            task_id: task(),
            was_status: TaskStatus::Running,
        }));
        assert_eq!(
            outcome,
            Outcome::Done(TaskCancelledRecord {
                task_id: text_of_uuid(task()),
                was_status: "RUNNING".to_owned(),
            })
        );
    }

    #[test]
    fn a_refused_cancel_is_the_raw_record_of_its_class() {
        let refused = TaskActionError::cancel_failed(task(), BrokerError::ListenerClosed);
        match cancel_outcome(Err(refused)) {
            Outcome::Failed(error) => {
                assert_eq!(error.class, "TaskActionDatabaseOperationFailed");
                let names: Vec<&str> = error.slots.iter().map(|slot| slot.name.as_str()).collect();
                assert_eq!(names, ["message", "retryable", "task_id", "cause"]);
            }
            Outcome::Done(record) => panic!("a refused cancel, got {record:?}"),
        }
    }

    #[test]
    fn each_policy_is_the_core_policy_of_the_same_name() {
        assert_eq!(
            CancelPolicy::IncludeRunning.core_policy(),
            CoreCancelPolicy::IncludeRunning
        );
        assert_eq!(
            CancelPolicy::PendingOrClaimedOnly.core_policy(),
            CoreCancelPolicy::PendingOrClaimedOnly
        );
    }
}
