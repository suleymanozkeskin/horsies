//! The start of a run in a worker's child through the core:
//! `horsies_start_attempt` (`PostgresBroker::start_attempt`).
//!
//! A Python child (isolated, shared async, or a seat of a shared thread
//! child) starts each run with one call on its `CoreChildHandle`. The call
//! carries the time left before the deadline of the run, when it has one:
//! the core checks it after the pool gave the connection, as Python's
//! `start_check`. The outcome is `AttemptStarted`, `AttemptMissed` (the
//! path after the miss stays with the caller), `DeadlineBeforeStart`, or
//! `StartFailed`.
//!
//! The call begins a start attempt, with the bounds of
//! `horsies_core::broker::child_writes` (`START_ATTEMPT_BOUND`): the
//! checkout and the statement end by the deadline of the attempt, and a
//! statement past its time gets a cancel request. The miss path calls send
//! the time since the attempt began (`attempt_elapsed_ms`), and end by the
//! same deadline ([`miss_path_deadline`]).

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use horsies_core::core::lifecycle::OwnedClaim;
use horsies_core::{
    BrokerError, RunWorkflow, StartAttempt, StartOutcome, StartWindow, WindowedStart, WriteDeadline,
};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::handle::BrokerConnection;

/// The row is RUNNING (and, for a workflow task, its node RUNNING), from
/// `started_at` on the database clock. The start writes no runner beat.
/// `workflow` is the node that links the task.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptStarted {
    pub task_id: Uuid,
    pub started_at: DateTime<Utc>,
    /// `RunWorkflowNode`, or `'NOT_IN_WORKFLOW'`.
    pub workflow: StartedWorkflow,
}

/// The workflow node that links a started task: the node at `node_index` of
/// the workflow `workflow_id`, in the tree whose top-level workflow is
/// `root_workflow_id`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunWorkflowNode {
    pub workflow_id: Uuid,
    pub node_index: u32,
    pub root_workflow_id: Uuid,
}

/// The workflow of a started task, as Python reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartedWorkflow {
    /// `'NOT_IN_WORKFLOW'`.
    NotInWorkflow,
    /// A `RunWorkflowNode`.
    InWorkflow(RunWorkflowNode),
}

impl StartedWorkflow {
    /// The Python form of the workflow of a start. Pure.
    pub fn of(workflow: RunWorkflow) -> Self {
        match workflow {
            RunWorkflow::NotInWorkflow => Self::NotInWorkflow,
            RunWorkflow::InWorkflow {
                workflow_id,
                node_index,
                root_workflow_id,
            } => Self::InWorkflow(RunWorkflowNode {
                workflow_id,
                node_index: node_index.get(),
                root_workflow_id,
            }),
        }
    }
}

impl<'py> IntoPyObject<'py> for StartedWorkflow {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::NotInWorkflow => "NOT_IN_WORKFLOW".into_bound_py_any(py),
            Self::InWorkflow(node) => node.into_bound_py_any(py),
        }
    }
}

/// The start wrote nothing. `reason` is the first reason that holds, in the
/// function's order: `DEADLINE_PASSED`, `NOT_HELD`, `LEASE_EXPIRED`,
/// `WORKFLOW_PAUSED`, `WORKFLOW_CANCELLED` or `NODE_NOT_LINKED` (Python
/// `StartMiss`). The path after the miss is the caller's.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct AttemptMissed {
    pub task_id: Uuid,
    pub reason: String,
}

/// The deadline of the run passed while the start waited for a pooled
/// connection. No statement ran: the row stays CLAIMED, and the timeout of
/// the worker returns it to the queue.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct DeadlineBeforeStart {
    pub task_id: Uuid,
}

/// The start failed: a failed connect, client family check or checkout
/// (nothing sent), a failed statement (it wrote nothing, or it committed
/// and its reply was lost, so the row may be RUNNING), or a row outside the
/// function's contract (not retryable). Python ends the run
/// `OWNERSHIP_UNCONFIRMED`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StartFailed {
    pub task_id: Uuid,
    /// The cause, with its subject.
    pub message: String,
    pub retryable: bool,
}

/// The outcome of one start.
pub enum StartRecord {
    Started(AttemptStarted),
    Missed(AttemptMissed),
    DeadlineBefore(DeadlineBeforeStart),
    Failed(StartFailed),
}

impl<'py> IntoPyObject<'py> for StartRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Started(started) => started.into_bound_py_any(py),
            Self::Missed(missed) => missed.into_bound_py_any(py),
            Self::DeadlineBefore(passed) => passed.into_bound_py_any(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The window of a start from the time left before the deadline of its run
/// at the call: `None` is a run with no deadline. Reads the monotonic
/// clock once.
pub fn start_window(deadline_in_ms: Option<u64>) -> StartWindow {
    match deadline_in_ms {
        Some(left_ms) => match Instant::now().checked_add(Duration::from_millis(left_ms)) {
            Some(deadline) => StartWindow::Until(deadline),
            // A deadline past the clock's range never passes.
            None => StartWindow::Open,
        },
        None => StartWindow::Open,
    }
}

/// The deadline of the miss path of a start attempt that began
/// `attempt_elapsed_ms` before the call, as the Python caller measured it:
/// the rest of the attempt's bound. Reads the monotonic clock once.
pub fn miss_path_deadline(attempt_elapsed_ms: u64) -> WriteDeadline {
    WriteDeadline::rest_of_start_attempt(Duration::from_millis(attempt_elapsed_ms))
}

/// The start command of one call. Pure.
pub fn start_command(
    task_id: Uuid,
    worker_id: String,
    claimed_at: DateTime<Utc>,
    worker_pid: i32,
    worker_hostname: String,
) -> StartAttempt {
    StartAttempt::of_process(
        task_id,
        OwnedClaim {
            worker_id,
            claimed_at,
        },
        worker_pid,
        worker_hostname,
    )
}

fn failed(task_id: Uuid, error: &BrokerError) -> StartRecord {
    StartRecord::Failed(StartFailed {
        task_id,
        message: error.to_string(),
        retryable: error.is_retryable(),
    })
}

/// The record of a start. Pure.
fn start_record(task_id: Uuid, start: Result<WindowedStart, BrokerError>) -> StartRecord {
    match start {
        Ok(WindowedStart::Ran(StartOutcome::Started {
            started_at,
            workflow,
        })) => StartRecord::Started(AttemptStarted {
            task_id,
            started_at,
            workflow: StartedWorkflow::of(workflow),
        }),
        Ok(WindowedStart::Ran(StartOutcome::Missed(miss))) => StartRecord::Missed(AttemptMissed {
            task_id,
            reason: miss.as_str().to_owned(),
        }),
        Ok(WindowedStart::WindowClosed) => {
            StartRecord::DeadlineBefore(DeadlineBeforeStart { task_id })
        }
        Err(error) => failed(task_id, &error),
    }
}

/// Start one claimed task within `deadline`, the deadline of the start
/// attempt that the call begins. Writes, in one statement; a caller that
/// leaves early does not stop it, and the row may then be RUNNING.
pub async fn start_attempt_operation(
    connection: BrokerConnection,
    command: StartAttempt,
    window: StartWindow,
    deadline: WriteDeadline,
) -> StartRecord {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return failed(command.task_id, &error),
    };
    start_record(
        command.task_id,
        broker.start_attempt(&command, window, deadline).await,
    )
}

#[cfg(test)]
mod tests {
    use horsies_core::StartMiss;

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(7)
    }

    #[test]
    fn each_start_outcome_gives_its_record() {
        let started_at = Utc::now();
        assert!(matches!(
            start_record(
                task(),
                Ok(WindowedStart::Ran(StartOutcome::Started {
                    started_at,
                    workflow: RunWorkflow::NotInWorkflow,
                }))
            ),
            StartRecord::Started(AttemptStarted {
                started_at: at,
                workflow: StartedWorkflow::NotInWorkflow,
                ..
            }) if at == started_at
        ));
        let StartRecord::Missed(missed) = start_record(
            task(),
            Ok(WindowedStart::Ran(StartOutcome::Missed(
                StartMiss::LeaseExpired,
            ))),
        ) else {
            panic!("a miss gives AttemptMissed");
        };
        assert_eq!(missed.reason, "LEASE_EXPIRED");
        assert!(matches!(
            start_record(task(), Ok(WindowedStart::WindowClosed)),
            StartRecord::DeadlineBefore(DeadlineBeforeStart { task_id }) if task_id == task()
        ));
    }

    /// A started workflow task gives its workflow, the index of its node
    /// and the root of its tree.
    #[test]
    fn a_started_workflow_task_gives_its_node() {
        let workflow_id = Uuid::from_u128(0xf004);
        let root_workflow_id = Uuid::from_u128(0xf001);
        let StartRecord::Started(started) = start_record(
            task(),
            Ok(WindowedStart::Ran(StartOutcome::Started {
                started_at: Utc::now(),
                workflow: RunWorkflow::InWorkflow {
                    workflow_id,
                    node_index: horsies_core::WorkflowNodeIndex::try_from(3)
                        .expect("3 is a node index"),
                    root_workflow_id,
                },
            })),
        ) else {
            panic!("a start gives AttemptStarted");
        };
        assert_eq!(
            started.workflow,
            StartedWorkflow::InWorkflow(RunWorkflowNode {
                workflow_id,
                node_index: 3,
                root_workflow_id,
            })
        );
    }

    #[test]
    fn a_failed_start_keeps_its_cause_and_retry_rule() {
        let StartRecord::Failed(failed) = start_record(
            task(),
            Err(BrokerError::ClaimContract(
                "horsies_start_attempt returned 0 rows".to_owned(),
            )),
        ) else {
            panic!("a failed start gives StartFailed");
        };
        assert!(failed.message.contains("0 rows"), "{}", failed.message);
        assert!(!failed.retryable);
    }

    #[test]
    fn the_miss_path_ends_with_the_attempt_that_it_belongs_to() {
        let left = miss_path_deadline(25_000)
            .left()
            .expect("15 s of the attempt are left");
        assert!(left <= Duration::from_secs(15), "{left:?}");
        assert_eq!(
            miss_path_deadline(
                u64::try_from(horsies_core::START_ATTEMPT_BOUND.as_millis()).expect("40 000 ms")
            )
            .left(),
            None
        );
        assert_eq!(miss_path_deadline(u64::MAX).left(), None);
    }

    #[test]
    fn a_run_with_no_deadline_has_an_open_window() {
        assert_eq!(start_window(None), StartWindow::Open);
        assert!(matches!(start_window(Some(1_000)), StartWindow::Until(_)));
        // `u64::MAX` ms is about 1.8e16 s. Where `Instant` holds that much
        // (an `i64` of seconds on macOS and Linux) the window ends that far
        // ahead; where the sum overflows, the window is open. In both cases
        // no run sees it close.
        let longer_than_any_run = Duration::from_secs(1_000 * 365 * 24 * 3_600);
        match start_window(Some(u64::MAX)) {
            StartWindow::Open => {}
            StartWindow::Until(deadline) => {
                assert!(deadline > Instant::now() + longer_than_any_run);
            }
        }
    }
}
