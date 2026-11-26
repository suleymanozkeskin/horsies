//! The stop of a claimed workflow task whose start missed, on the statement
//! layer: `horsies_workflow_task_stop_before_start`
//! (`0005_workflow_functions.sql`), one statement in autocommit. The caller
//! sends it after the start statement ended, so it holds no row lock of the
//! start.
//!
//! The reason of the start's miss only chooses that this call runs: the
//! function judges the task again under its own locks (the workflow row
//! `FOR SHARE`, the task advisory lock, the task row, the node row), in this
//! order:
//! - the deadline passed: the claim expires (`EXPIRE_CLAIMED`, the stored
//!   `TASK_EXPIRED` error of the core), [`StopBeforeStart::Expired`];
//! - the workflow is PAUSED: the claim is abandoned (`PAUSE_ABANDON_CLAIM`),
//!   and the node of an abandoned task is READY again with no task,
//!   [`StopBeforeStart::Abandoned`];
//! - the workflow is CANCELLED: the claim is cancelled
//!   (`WORKFLOW_CANCEL_CLAIM`, also a row requeued to PENDING), and the node
//!   of a cancelled task is skipped, [`StopBeforeStart::Cancelled`];
//! - the claim is still startable and a node links it live in a RUNNING
//!   workflow: nothing is written; a resume came first, and the caller runs
//!   the start once more, [`StopBeforeStart::Resumed`];
//! - the claim is held and no node links the task live: with
//!   [`OrphanSelfHeal::On`] the claim is cancelled (`CANCEL_ORPHAN`),
//!   [`StopBeforeStart::OrphanCancelled`]; with [`OrphanSelfHeal::Off`]
//!   nothing is written, [`StopBeforeStart::NodeNotLinked`];
//! - else nothing is written: [`StopBeforeStart::NotHeld`], with the
//!   classification of `horsies_terminalization_miss`.
//!
//! The caller is the child of a worker: the core child host and the Python
//! child (`CoreChildHandle.stop_workflow_task_before_start`). The stop is
//! the miss path of a start attempt, so it runs within the deadline of that
//! attempt ([`crate::broker::child_writes`]).

use uuid::Uuid;

use crate::broker::child_writes::{query_within, WriteDeadline};
use crate::broker::core_rows::{column, one_row, StatementLog};
use crate::broker::error::BrokerError;
use crate::broker::expiry_and_orphans::prestart_expiry_result;
use crate::broker::postgres::PostgresBroker;
use crate::core::lifecycle::OwnedClaim;
use crate::db::{CorePool, ParameterType, TypedStatement};

/// `horsies_workflow_task_stop_before_start`. Parameters in order: the task,
/// the worker, the claim generation, the stored result of an expiry, the
/// orphan self-heal. One row: the step that ran and the outcome of its move.
static STOP_BEFORE_START: TypedStatement = TypedStatement::new(
    "workflow_task_stop_before_start",
    "SELECT outcome, move_outcome \
     FROM horsies_workflow_task_stop_before_start($1, $2, $3, $4, $5)",
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Text,
        ParameterType::Bool,
    ],
);

/// Whether the stop of a claimed workflow task that no node links live
/// cancels the task (the app's `terminate_orphaned_workflow_tasks`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanSelfHeal {
    /// Cancel the task with `horsies_cancel_owned_orphan`.
    On,
    /// Leave the row CLAIMED for inspection.
    Off,
}

impl OrphanSelfHeal {
    /// The setting of `terminate_orphaned_workflow_tasks`. Pure.
    pub fn of_recovery_flag(terminate_orphaned_workflow_tasks: bool) -> Self {
        match terminate_orphaned_workflow_tasks {
            true => Self::On,
            false => Self::Off,
        }
    }

    /// The `p_orphan_self_heal` argument of the function. Pure.
    fn as_argument(self) -> bool {
        match self {
            Self::On => true,
            Self::Off => false,
        }
    }
}

/// The outcome of the move that a step of the stop ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopMove {
    /// The move applied: the row is in history.
    Applied,
    /// The same move committed before: the row is in history.
    AlreadyApplied,
    /// Another claim generation or owner holds the row; nothing is written.
    LostClaim,
    /// The row is not in a state that the move takes; nothing is written.
    SourceStateConflict,
    /// The row is absent; nothing is written.
    TaskAbsent,
}

impl StopMove {
    /// The wire spelling of the function. Pure.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::AlreadyApplied => "ALREADY_APPLIED",
            Self::LostClaim => "LOST_CLAIM",
            Self::SourceStateConflict => "SOURCE_STATE_CONFLICT",
            Self::TaskAbsent => "TASK_ABSENT",
        }
    }

    /// The move outcome of a wire spelling, or `None` for another text.
    /// Pure.
    fn parse(text: &str) -> Option<Self> {
        match text {
            "APPLIED" => Some(Self::Applied),
            "ALREADY_APPLIED" => Some(Self::AlreadyApplied),
            "LOST_CLAIM" => Some(Self::LostClaim),
            "SOURCE_STATE_CONFLICT" => Some(Self::SourceStateConflict),
            "TASK_ABSENT" => Some(Self::TaskAbsent),
            _ => None,
        }
    }
}

/// What the stop did: the step that ran, with the outcome of its move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopBeforeStart {
    /// The deadline passed: the claim expired, in this call
    /// ([`StopMove::Applied`]) or before it ([`StopMove::AlreadyApplied`]).
    /// A workflow task owes its phase 2.
    Expired(StopMove),
    /// The workflow is PAUSED: the abandon ran with this outcome.
    Abandoned(StopMove),
    /// The workflow is CANCELLED: the cancel ran with this outcome.
    Cancelled(StopMove),
    /// A resume came first: the claim is still startable. Nothing is written.
    Resumed,
    /// No node links the task live, and the orphan self-heal is on: the
    /// orphan cancel ran with this outcome.
    OrphanCancelled(StopMove),
    /// No node links the task live, and the orphan self-heal is off.
    /// Nothing is written.
    NodeNotLinked,
    /// The claim is not held: the outcome of the miss classifier. Nothing is
    /// written.
    NotHeld(StopMove),
}

impl StopBeforeStart {
    /// The step's wire spelling of the function. Pure.
    pub const fn outcome_str(self) -> &'static str {
        match self {
            Self::Expired(_) => "EXPIRED",
            Self::Abandoned(_) => "ABANDONED",
            Self::Cancelled(_) => "CANCELLED",
            Self::Resumed => "RESUMED",
            Self::OrphanCancelled(_) => "ORPHAN_CANCELLED",
            Self::NodeNotLinked => "NODE_NOT_LINKED",
            Self::NotHeld(_) => "NOT_HELD",
        }
    }

    /// The outcome of the move of the step, or `None` for a step that runs
    /// no move. Pure.
    pub const fn move_outcome(self) -> Option<StopMove> {
        match self {
            Self::Expired(moved)
            | Self::Abandoned(moved)
            | Self::Cancelled(moved)
            | Self::OrphanCancelled(moved)
            | Self::NotHeld(moved) => Some(moved),
            Self::Resumed | Self::NodeNotLinked => None,
        }
    }
}

/// The stop of one row of the function. Pure.
///
/// Errors: the cause, when the row is outside the function's contract: an
/// unknown step or move outcome, a move outcome on `RESUMED` or
/// `NODE_NOT_LINKED`, none on another step, or an `EXPIRED` whose expiry
/// did not end the row.
fn decode_stop(outcome: &str, move_outcome: Option<&str>) -> Result<StopBeforeStart, String> {
    let moved = match move_outcome {
        Some(text) => match StopMove::parse(text) {
            Some(moved) => Some(moved),
            None => return Err(format!("unknown move outcome {text:?}")),
        },
        None => None,
    };
    match (outcome, moved) {
        ("EXPIRED", Some(moved @ (StopMove::Applied | StopMove::AlreadyApplied))) => {
            Ok(StopBeforeStart::Expired(moved))
        }
        ("ABANDONED", Some(moved)) => Ok(StopBeforeStart::Abandoned(moved)),
        ("CANCELLED", Some(moved)) => Ok(StopBeforeStart::Cancelled(moved)),
        ("RESUMED", None) => Ok(StopBeforeStart::Resumed),
        ("ORPHAN_CANCELLED", Some(moved)) => Ok(StopBeforeStart::OrphanCancelled(moved)),
        ("NODE_NOT_LINKED", None) => Ok(StopBeforeStart::NodeNotLinked),
        ("NOT_HELD", Some(moved)) => Ok(StopBeforeStart::NotHeld(moved)),
        (other, moved) => Err(format!(
            "outcome {other:?} with move outcome {:?} is outside the function's contract",
            moved.map(StopMove::as_str)
        )),
    }
}

/// Stop the claimed workflow task `task_id` of `fence` on `pool`: one
/// `horsies_workflow_task_stop_before_start` statement in autocommit, as a
/// child write within `deadline`, the deadline of the start attempt whose
/// miss path it is ([`crate::broker::child_writes::query_within`]), with the
/// core's stored `TASK_EXPIRED` error for an expiry (the task and worker ids
/// as `data`).
///
/// Errors: [`BrokerError::Serialization`] for the stored error (nothing
/// sent); [`BrokerError::Statement`] (nothing sent: a failed checkout, or
/// `deadline` passed first; nothing written: refused, a cancel at the bound
/// included; or `OutcomeUnknown`: the same stop sent again reports a
/// committed move as `AlreadyApplied`, or the row as not held);
/// [`BrokerError::RowDecode`] and [`BrokerError::TerminalizationContract`]
/// for a row outside the function's contract.
pub(crate) async fn stop_before_start(
    pool: &CorePool,
    log: StatementLog,
    task_id: Uuid,
    fence: &OwnedClaim,
    orphan_self_heal: OrphanSelfHeal,
    deadline: WriteDeadline,
) -> Result<StopBeforeStart, BrokerError> {
    let expiry_result = prestart_expiry_result(task_id, &fence.worker_id)?;
    let self_heal = orphan_self_heal.as_argument();
    let statement = &STOP_BEFORE_START;
    let bound = statement
        .bind()
        .value(&task_id)
        .value(&fence.worker_id)
        .value(&fence.claimed_at)
        .value(&expiry_result)
        .value(&self_heal)
        .finish();
    log.log(statement);
    let rows = query_within(pool, &bound, deadline).await?;
    let row = one_row(rows, statement)?;
    let outcome = column::<String>(&row, statement, "outcome")?;
    let move_outcome = column::<Option<String>>(&row, statement, "move_outcome")?;
    decode_stop(&outcome, move_outcome.as_deref()).map_err(|cause| {
        BrokerError::TerminalizationContract(format!(
            "horsies_workflow_task_stop_before_start for task {task_id}: {cause}"
        ))
    })
}

impl PostgresBroker {
    /// Stop a claimed workflow task whose start missed
    /// (`horsies_workflow_task_stop_before_start`): one statement in
    /// autocommit on the main core pool, checked out as reserved work, fenced
    /// to `fence` (this worker, the claim generation of the start), within
    /// `deadline`, the rest of the start attempt whose miss path it is
    /// ([`WriteDeadline::rest_of_start_attempt`]). The function judges the
    /// task again under its own locks; see the module documentation for the
    /// steps and [`StopBeforeStart`] for the outcomes.
    ///
    /// Errors: [`BrokerError::PoolSettings`] and the client family check's
    /// errors (nothing sent); [`BrokerError::Serialization`] (nothing sent);
    /// [`BrokerError::Statement`] (nothing sent when `deadline` passed
    /// first; nothing written, a cancel at the bound included; or
    /// `OutcomeUnknown`); [`BrokerError::RowDecode`] and
    /// [`BrokerError::TerminalizationContract`] for a row outside the
    /// function's contract.
    pub async fn stop_workflow_task_before_start(
        &self,
        task_id: Uuid,
        fence: &OwnedClaim,
        orphan_self_heal: OrphanSelfHeal,
        deadline: WriteDeadline,
    ) -> Result<StopBeforeStart, BrokerError> {
        self.ensure_client_family_for_write().await?;
        let (open, _write) = self.core_pools_for_write()?;
        stop_before_start(
            open.main(),
            self.statement_log(),
            task_id,
            fence,
            orphan_self_heal,
            deadline,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_stop, OrphanSelfHeal, StopBeforeStart, StopMove};

    #[test]
    fn each_step_of_the_function_decodes_to_its_outcome() {
        assert_eq!(
            decode_stop("EXPIRED", Some("APPLIED")),
            Ok(StopBeforeStart::Expired(StopMove::Applied))
        );
        assert_eq!(
            decode_stop("EXPIRED", Some("ALREADY_APPLIED")),
            Ok(StopBeforeStart::Expired(StopMove::AlreadyApplied))
        );
        assert_eq!(
            decode_stop("ABANDONED", Some("LOST_CLAIM")),
            Ok(StopBeforeStart::Abandoned(StopMove::LostClaim))
        );
        assert_eq!(
            decode_stop("CANCELLED", Some("APPLIED")),
            Ok(StopBeforeStart::Cancelled(StopMove::Applied))
        );
        assert_eq!(decode_stop("RESUMED", None), Ok(StopBeforeStart::Resumed));
        assert_eq!(
            decode_stop("ORPHAN_CANCELLED", Some("APPLIED")),
            Ok(StopBeforeStart::OrphanCancelled(StopMove::Applied))
        );
        assert_eq!(
            decode_stop("NODE_NOT_LINKED", None),
            Ok(StopBeforeStart::NodeNotLinked)
        );
        assert_eq!(
            decode_stop("NOT_HELD", Some("TASK_ABSENT")),
            Ok(StopBeforeStart::NotHeld(StopMove::TaskAbsent))
        );
    }

    #[test]
    fn each_outcome_keeps_its_wire_spelling() {
        for (outcome, moved) in [
            ("EXPIRED", Some("APPLIED")),
            ("ABANDONED", Some("SOURCE_STATE_CONFLICT")),
            ("CANCELLED", Some("ALREADY_APPLIED")),
            ("RESUMED", None),
            ("ORPHAN_CANCELLED", Some("LOST_CLAIM")),
            ("NODE_NOT_LINKED", None),
            ("NOT_HELD", Some("LOST_CLAIM")),
        ] {
            let stop = decode_stop(outcome, moved).expect("a row of the contract");
            assert_eq!(stop.outcome_str(), outcome);
            assert_eq!(stop.move_outcome().map(StopMove::as_str), moved);
        }
    }

    #[test]
    fn a_row_outside_the_contract_is_refused() {
        for (outcome, moved) in [
            ("STOPPED", Some("APPLIED")),
            ("ABANDONED", Some("MOVED")),
            ("ABANDONED", None),
            ("RESUMED", Some("APPLIED")),
            ("NODE_NOT_LINKED", Some("APPLIED")),
            ("EXPIRED", Some("LOST_CLAIM")),
            ("EXPIRED", None),
        ] {
            assert!(
                decode_stop(outcome, moved).is_err(),
                "{outcome} {moved:?} is outside the contract"
            );
        }
    }

    #[test]
    fn the_recovery_flag_gives_the_self_heal() {
        assert_eq!(OrphanSelfHeal::of_recovery_flag(true), OrphanSelfHeal::On);
        assert_eq!(OrphanSelfHeal::of_recovery_flag(false), OrphanSelfHeal::Off);
        assert!(OrphanSelfHeal::On.as_argument());
        assert!(!OrphanSelfHeal::Off.as_argument());
    }
}
