//! The claim pass of the worker parent with the claim per execution class.
//!
//! One pass:
//! 1. One `horsies_claim` statement with a budget per class and the routes
//!    of the plan ([`ClaimPassRequest::classes`]).
//! 2. When the pass took workflow tasks: the rows of paused or cancelled
//!    workflows are dropped (the existing workflow filter).
//! 3. The class check of each row against the task this worker registered
//!    ([`check_row_class`]). The rows whose class does not fit go back to
//!    the queue as `ISOLATED` in one release statement, fenced to their claim
//!    generations, with a `class` wake per queue; they are not dispatched. A
//!    row whose claim this worker lost is neither returned nor dispatched.
//! 4. The other rows, in claim order, are for dispatch.
//!
//! [`ClaimPassCounts`] says whether work can remain that no wake would
//! announce, so the loop runs the next pass at once (the repass rule).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::claims::{
    claim_pass, release_claims, ClaimPass, ClaimPassRequest, ClaimTake, ClaimedRow, ReleaseBatch,
    ReleaseClaims, ReleaseOutcome, ReleasePlacement, MAX_RELEASED_CLAIMS,
};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::task::Execution;

#[cfg(test)]
mod tests;

/// The most claim passes in a row that run again with no wait because each
/// took fewer rows than it planned. A transaction that holds planned rows
/// for longer leaves the worker to its next wake: a NOTIFY, a capacity
/// release, or the poll interval.
pub const SHORT_PASS_REPASS_LIMIT: u32 = 3;

/// The execution class of each task this worker registered, by task name.
/// The Python host gives it as data with the worker's tasks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegisteredClasses {
    classes: HashMap<String, Execution>,
}

impl RegisteredClasses {
    pub fn new(classes: HashMap<String, Execution>) -> RegisteredClasses {
        RegisteredClasses { classes }
    }

    /// The registered task names, in no set order.
    pub fn task_names(&self) -> impl Iterator<Item = &str> {
        self.classes.keys().map(String::as_str)
    }

    fn class_of(&self, task_name: &str) -> Option<Execution> {
        self.classes.get(task_name).copied()
    }
}

/// The class stored on a claimed row against the registered task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowClassCheck {
    /// A child of the row's route can run the registered function.
    Fits,
    /// The row asks for a shared class that the registered function does
    /// not have (a deploy changed the function after the send).
    Mismatch {
        row_class: Execution,
        registered_class: Execution,
    },
    /// This worker registered no task of the row's name. The child fails
    /// the row with its usual error, as it does without a class.
    TaskNotRegistered,
}

/// Compare the class stored on a claimed row with the registered task. An
/// `ISOLATED` row fits any function: an isolated child runs both kinds. A
/// row of a shared class fits only a function of that class. Pure.
pub fn check_row_class(
    row_class: Execution,
    task_name: &str,
    registered: &RegisteredClasses,
) -> RowClassCheck {
    let registered_class = match registered.class_of(task_name) {
        Some(registered_class) => registered_class,
        None => return RowClassCheck::TaskNotRegistered,
    };
    match (row_class, registered_class == row_class) {
        (Execution::Isolated, _) | (Execution::SharedAsync | Execution::SharedThread, true) => {
            RowClassCheck::Fits
        }
        (Execution::SharedAsync | Execution::SharedThread, false) => RowClassCheck::Mismatch {
            row_class,
            registered_class,
        },
    }
}

/// Whether `max_claim_batch` caps a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimBatchLimit {
    /// `max_claim_batch` is above 0: a pass can stop with free slots.
    CapsThePass,
    /// `max_claim_batch` is 0: the pass fills the budget.
    NoCap,
}

/// What one claim pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimPassCounts {
    /// Rows the claim statement took.
    pub claimed: usize,
    /// Rows of those that the checks after the claim gave up: rows of a
    /// paused or cancelled workflow, and rows returned for their class (or
    /// whose claim was lost before the return). They used claim budget and
    /// hold no slot.
    pub dropped: usize,
    /// Whether the claim took every row of its plan.
    pub take: ClaimTake,
}

impl ClaimPassCounts {
    /// Whether ready work can remain that no wake would announce.
    ///
    /// A dropped row used budget that ready work could have had, and frees
    /// no slot, so no capacity release follows. A capped pass that took rows
    /// can stop with free slots. A short take can stop with free slots while
    /// rows that its plan did not choose are ready. In each case the NOTIFY
    /// of that work was already consumed. A short take asks for another pass
    /// only while fewer than [`SHORT_PASS_REPASS_LIMIT`] passes before it in
    /// a row were short. Pure.
    pub fn left_unannounced_work(&self, limit: ClaimBatchLimit, short_passes_before: u32) -> bool {
        let capped_pass_took_rows = match limit {
            ClaimBatchLimit::CapsThePass => self.claimed > 0,
            ClaimBatchLimit::NoCap => false,
        };
        let short_take_repasses = match self.take {
            ClaimTake::Short { .. } => short_passes_before < SHORT_PASS_REPASS_LIMIT,
            ClaimTake::Full => false,
        };
        self.dropped > 0 || capped_pass_took_rows || short_take_repasses
    }

    /// How many passes in a row, ending with this one, took fewer rows than
    /// they planned. Pure.
    pub fn short_passes_after(&self, short_passes_before: u32) -> u32 {
        match self.take {
            ClaimTake::Short { .. } => short_passes_before.saturating_add(1),
            ClaimTake::Full => 0,
        }
    }
}

/// The rows of a pass to dispatch, in claim order, and what the pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedForDispatch {
    pub counts: ClaimPassCounts,
    pub rows: Vec<ClaimedRow>,
}

/// The database steps of a claim pass: the worker's broker, or a scripted
/// set in a test.
pub trait ClaimPassSteps: Send + Sync {
    /// One `horsies_claim` statement.
    fn claim(
        &self,
        request: &ClaimPassRequest,
    ) -> impl Future<Output = Result<ClaimPass, BrokerError>> + Send;

    /// The ids of the claimed workflow tasks whose workflow is paused or
    /// cancelled; the filter writes what such a row needs.
    fn non_runnable_workflow_tasks(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
    ) -> impl Future<Output = Result<Vec<Uuid>, BrokerError>> + Send;

    /// Return the claims to the queue as `ISOLATED`, fenced to their claim
    /// generations. One outcome per claim, in input order.
    fn return_as_isolated(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
    ) -> impl Future<Output = Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError>> + Send;
}

/// Run one claim pass with `steps` and give the rows to dispatch.
///
/// Errors: the [`BrokerError`] of the first step that failed. The claim
/// committed before a later step fails: rows not yet returned or dispatched
/// return to the queue when their lease expires.
pub async fn claim_for_dispatch<S: ClaimPassSteps>(
    steps: &S,
    request: &ClaimPassRequest,
    registered: &RegisteredClasses,
) -> Result<ClaimedForDispatch, BrokerError> {
    let pass = steps.claim(request).await?;
    let claimed = pass.claimed.len();
    let runnable = drop_non_runnable(steps, pass.claimed).await?;
    let rows = return_rows_of_another_class(steps, runnable, registered).await?;
    Ok(ClaimedForDispatch {
        counts: ClaimPassCounts {
            claimed,
            dropped: claimed - rows.len(),
            take: pass.take,
        },
        rows,
    })
}

/// Drop the rows of paused or cancelled workflows. Only workflow tasks can
/// belong to one, so a pass with none runs no statement.
async fn drop_non_runnable<S: ClaimPassSteps>(
    steps: &S,
    rows: Vec<ClaimedRow>,
) -> Result<Vec<ClaimedRow>, BrokerError> {
    match rows.iter().any(|row| row.is_workflow_task) {
        false => Ok(rows),
        true => {
            let claims: Vec<(Uuid, DateTime<Utc>)> =
                rows.iter().map(|row| (row.id, row.claimed_at)).collect();
            let dropped = steps.non_runnable_workflow_tasks(&claims).await?;
            Ok(rows
                .into_iter()
                .filter(|row| !dropped.contains(&row.id))
                .collect())
        }
    }
}

/// Return each row whose class does not fit its registered task, in one
/// release as `ISOLATED`. Rows that fit, and rows of a task this worker did
/// not register, come back in their order. Logs one warning per returned
/// row.
async fn return_rows_of_another_class<S: ClaimPassSteps>(
    steps: &S,
    rows: Vec<ClaimedRow>,
    registered: &RegisteredClasses,
) -> Result<Vec<ClaimedRow>, BrokerError> {
    let mut fitting = Vec::with_capacity(rows.len());
    let mut mismatched: Vec<(ClaimedRow, Execution)> = Vec::new();
    for row in rows {
        match check_row_class(row.execution, &row.task_name, registered) {
            RowClassCheck::Fits | RowClassCheck::TaskNotRegistered => fitting.push(row),
            RowClassCheck::Mismatch {
                registered_class, ..
            } => mismatched.push((row, registered_class)),
        }
    }
    if mismatched.is_empty() {
        return Ok(fitting);
    }
    let claims: Vec<(Uuid, DateTime<Utc>)> = mismatched
        .iter()
        .map(|(row, _)| (row.id, row.claimed_at))
        .collect();
    let outcomes = steps.return_as_isolated(&claims).await?;
    for ((row, registered_class), (_, outcome)) in mismatched.iter().zip(&outcomes) {
        let done = match outcome {
            ReleaseOutcome::Released => "wrote ISOLATED on the row and returned it to the queue",
            ReleaseOutcome::NotHeld => "the claim was lost, so the row was left as it is",
        };
        tracing::warn!(
            task_id = %row.id,
            "Task {} ({}) stores execution {}, and the registered function is {} on this \
             worker; {done}",
            row.id,
            row.task_name,
            row.execution.as_str(),
            registered_class.as_str(),
        );
    }
    Ok(fitting)
}

/// The claim pass steps of a worker: the 0004 claim and release on the
/// worker pool of its broker, and the existing workflow filter.
pub struct BrokerClaimSteps {
    broker: Arc<PostgresBroker>,
    worker_id: String,
}

impl BrokerClaimSteps {
    pub fn new(broker: Arc<PostgresBroker>, worker_id: String) -> BrokerClaimSteps {
        BrokerClaimSteps { broker, worker_id }
    }
}

impl ClaimPassSteps for BrokerClaimSteps {
    async fn claim(&self, request: &ClaimPassRequest) -> Result<ClaimPass, BrokerError> {
        let (open, _write) = self.broker.core_pools_for_write()?;
        claim_pass(open.main(), self.broker.statement_log(), request).await
    }

    async fn non_runnable_workflow_tasks(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
    ) -> Result<Vec<Uuid>, BrokerError> {
        self.broker
            .filter_non_runnable_workflow_tasks(claims, &self.worker_id)
            .await
    }

    /// One release statement per [`MAX_RELEASED_CLAIMS`] claims.
    async fn return_as_isolated(
        &self,
        claims: &[(Uuid, DateTime<Utc>)],
    ) -> Result<Vec<(Uuid, ReleaseOutcome)>, BrokerError> {
        let (open, _write) = self.broker.core_pools_for_write()?;
        let mut outcomes = Vec::with_capacity(claims.len());
        for chunk in claims.chunks(MAX_RELEASED_CLAIMS) {
            let batch = ReleaseBatch::new(chunk).expect(
                "one claim pass returns each task once, and a chunk holds at most \
                 MAX_RELEASED_CLAIMS claims",
            );
            let command = ReleaseClaims {
                worker_id: self.worker_id.clone(),
                batch,
                placement: ReleasePlacement::Isolated,
            };
            outcomes
                .extend(release_claims(open.main(), self.broker.statement_log(), &command).await?);
        }
        Ok(outcomes)
    }
}
