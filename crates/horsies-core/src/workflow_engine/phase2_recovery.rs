//! Bounded recovery of workflow progression from terminalization evidence.
//!
//! One pass reads the phase-2 records past the grace window and under the
//! attempt bound, oldest first, and recovers each in its own transaction on
//! the main core pool with the reserve ([`CheckoutClass::Reserved`]):
//! `horsies_workflow_phase2_recover` with the child starts it owes. A record
//! whose recovery raises persistently is counted by
//! `horsies_workflow_phase2_record_raise`; at the bound its workflow fails
//! closed with `WORKFLOW_RECOVERY_LIMIT_REACHED` and its evidence is
//! quarantined. Python: `drive_phase2_recovery`
//! (`core/workflows/phase2_recovery.py:381-576`).

use uuid::Uuid;

use crate::broker::core_rows::{column, first_row, one_row, RowDecodeError};
use crate::broker::PostgresBroker;
use crate::core::lifecycle::TerminalizationKind;
use crate::core::types::status::TaskStatus;
use crate::db::{CheckoutClass, ParameterType, Row, TypedStatement};
use crate::workflow_engine::recovery_limit::{
    recovery_limit_error, recovery_limit_payloads, run_catching_panic, CandidateFailure,
    FailureClass, RecoveryCase,
};
use crate::workflow_engine::steps::{
    advance_parents, phase2_recover, run_alone, ChildBuild, CommittedStep, NodeFinishDisposition,
    StepContractError,
};

use super::WorkflowError;

/// The phase-2 records older than the grace window (`$1` milliseconds) and
/// under the attempt bound (`$2`), oldest first, at most `$3`. The record
/// kind is read by `horsies_workflow_phase2_recover`, not here. Python:
/// `DISCOVER_PENDING_SQL` (`core/workflows/phase2_recovery.py:72-82`).
static DISCOVER_PENDING: TypedStatement = TypedStatement::new(
    "phase2_discover_pending",
    "SELECT task_id
     FROM horsies_workflow_phase2_pending
     WHERE created_at < now() - $1 * interval '1 millisecond'
       AND attempt_count < $2
     ORDER BY created_at, task_id
     LIMIT $3",
    &[
        ParameterType::Int8,
        ParameterType::Int4,
        ParameterType::Int8,
    ],
);

/// The count of the records at or over the attempt bound `$1`, which
/// discovery no longer selects. Python: `COUNT_OVER_ATTEMPT_BOUND_SQL`
/// (`core/workflows/phase2_recovery.py:100-103`).
static COUNT_OVER_ATTEMPT_BOUND: TypedStatement = TypedStatement::new(
    "phase2_count_over_attempt_bound",
    "SELECT count(*) AS over_attempt_bound
     FROM horsies_workflow_phase2_pending
     WHERE attempt_count >= $1",
    &[ParameterType::Int4],
);

/// The workflow and the node of a phase-2 record, for the limit error of a
/// raise. Python: `RAISED_ROW_CONTEXT_SQL`
/// (`core/workflows/phase2_recovery.py:179-187`).
static RAISED_RECORD_CONTEXT: TypedStatement = TypedStatement::new(
    "phase2_raised_record_context",
    "SELECT p.workflow_id, n.task_index
     FROM horsies_workflow_phase2_pending p
     JOIN horsies_workflow_tasks n ON n.id = p.workflow_node_row_id
     WHERE p.task_id = $1",
    &[ParameterType::Uuid],
);

/// `horsies_workflow_phase2_record_raise(task_id, attempt_bound, limit_error,
/// limit_node_result)`: counts one persistent raise of a record, in one
/// statement in autocommit.
static RECORD_RAISE: TypedStatement = TypedStatement::new(
    "horsies_workflow_phase2_record_raise",
    "SELECT outcome, attempt_count, limit_outcome, quarantine_verdict
     FROM horsies_workflow_phase2_record_raise($1, $2, $3, $4)",
    &[
        ParameterType::Uuid,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
    ],
);

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct Phase2RecoverySummary {
    pub considered: u64,
    pub applied: u64,
    pub already_applied: u64,
    /// The node was written and its workflow had ended
    /// (`APPLIED_WORKFLOW_ENDED`).
    pub superseded: u64,
    /// The record was gone and its node is not terminal (`PENDING_ABSENT`):
    /// nothing written.
    pub pending_absent: u64,
    pub retained: u64,
    pub failed: u64,
    pub quarantined: u64,
    /// Persistent raises whose attempt was stored (`COUNTED`,
    /// `LIMIT_REACHED`).
    pub raised_counted: u64,
    pub over_attempt_bound: i64,
    pub retained_details: Vec<String>,
    pub quarantine_refusals: Vec<String>,
}

/// What `horsies_workflow_phase2_record_raise` did with one raise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RaiseRecord {
    /// The record is gone; nothing written.
    PendingAbsent,
    /// The attempt was added; the record is under the bound.
    Counted { attempt_count: i32 },
    /// The attempt reached the bound with a RUNNING workflow: the recovery
    /// limit was written, the attempt added, and the evidence quarantined
    /// (`quarantine_verdict`: the verdict text, `VERDICT[: detail]`).
    LimitReached {
        attempt_count: i32,
        limit: Phase2Limit,
        quarantine_verdict: String,
    },
    /// The attempt would reach the bound and the workflow is not RUNNING;
    /// nothing written. A later pass after a resume raises again.
    NotRecorded { attempt_count: i32 },
}

/// What the recovery limit of a raised record did to its workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase2Limit {
    /// No live node remained: the workflow is FAILED
    /// (`LIMIT_REACHED_FAILED`).
    WorkflowFailed,
    /// A live node remains: the workflow stays RUNNING with its error, and
    /// the last node ends it (`LIMIT_REACHED_LIVE_NODES`).
    LiveNodesFinish,
}

/// What a quarantine verdict means for the record. Python:
/// `core/history/phase2/quarantine.py:174-185`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuarantineResult {
    /// `REPOINTED`, `PENDING_GONE`, `ALREADY_QUARANTINED`.
    Quarantined,
    /// `NODE_ROW_ABSENT`, `SOURCE_ABSENT`, `COPY_VERIFICATION_FAILED`.
    Refused,
    /// A verdict outside the six of `horsies_phase2_quarantine_one`.
    OutsideContract,
}

/// A phase-2 record names a kind whose terminal move writes no phase-2
/// record. The pending CHECK admits five kinds, so this is a value that the
/// table refuses or that a later schema added.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("phase-2 record names terminalization kind {kind:?}, which writes no phase-2 record")]
pub(crate) struct PendingKindWithoutPhase2 {
    pub kind: String,
}

/// The task status that the terminal move wrote with the kind of a phase-2
/// record (Python `terminal_status_of_pending_kind`).
///
/// The move pairs each kind with one terminal status and refuses any other
/// pair, and only five kinds write a phase-2 record: `COMPLETE_LOCKED` is
/// COMPLETED; `FAIL_RUNNING` and `FAIL_STALE` are FAILED; `EXPIRE_CLAIMED`
/// and `EXPIRE_PENDING` are EXPIRED. No cancel kind writes a record, so a
/// record never stands for a CANCELLED task. Every other value, known kind
/// or not, is `PendingKindWithoutPhase2`. Pure.
pub(crate) fn terminal_status_of_pending_kind(
    kind: &str,
) -> Result<TaskStatus, PendingKindWithoutPhase2> {
    match TerminalizationKind::parse(kind) {
        Some(TerminalizationKind::CompleteLocked) => Ok(TaskStatus::Completed),
        Some(TerminalizationKind::FailRunning | TerminalizationKind::FailStale) => {
            Ok(TaskStatus::Failed)
        }
        Some(TerminalizationKind::ExpireClaimed | TerminalizationKind::ExpirePending) => {
            Ok(TaskStatus::Expired)
        }
        Some(
            TerminalizationKind::CompleteFused
            | TerminalizationKind::CancelAdmin
            | TerminalizationKind::CancelOrphan
            | TerminalizationKind::CancelOrphanSweep
            | TerminalizationKind::PauseAbandonClaim
            | TerminalizationKind::PauseAbandonClaimBatch
            | TerminalizationKind::PauseAbandonWorkflow
            | TerminalizationKind::WorkflowCancelClaim
            | TerminalizationKind::WorkflowCancelClaimBatch
            | TerminalizationKind::WorkflowCancelWorkflow,
        )
        | None => Err(PendingKindWithoutPhase2 {
            kind: kind.to_owned(),
        }),
    }
}

/// The capacity wake that phase 2 sends (Python `QueueWake | NoQueueWake`,
/// `core/workflows/engine.py:1766-1780`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase2Wake {
    /// `pg_notify(channel, payload)`. Inside a transaction, PostgreSQL
    /// delivers it at COMMIT and drops it at rollback.
    Queue { channel: String, payload: String },
    /// The caller sends no wake.
    NoWake,
}

/// One bounded pass over the phase-2 records, with the children of the owed
/// child starts built by `build`.
///
/// - Discovery ([`DISCOVER_PENDING`]) and the count over the bound
///   ([`COUNT_OVER_ATTEMPT_BOUND`]) run on the main core pool of `broker`
///   with the reserve ([`CheckoutClass::Reserved`]), one statement each in
///   autocommit.
/// - Each record runs [`phase2_recover`] in its own transaction on the main
///   core pool of `broker` with the reserve, with the child starts it owes.
///   The parent advances that it owes run after its commit
///   ([`advance_parents`]); a failed advance is logged and left to workflow
///   recovery (case 1.6).
/// - A record whose recovery fails (an error, or a panic caught at the
///   record) counts in `failed` and is logged. A persistent failure is
///   counted on the record ([`record_raise`]); a transient one (a retryable
///   database error) is not, so an outage cannot march records to the bound.
/// - The pass never stops at a record: the records behind it still run.
///
/// Errors (nothing was recovered):
/// - [`WorkflowError::Validation`] for a grace above `i64::MAX` milliseconds
///   or an attempt bound above `i32::MAX` (the config bounds them to one hour
///   and to 3-1000);
/// - [`WorkflowError::Broker`] when discovery or the count fails;
/// - [`WorkflowError::StepContract`] for a row that does not decode.
pub(crate) async fn drive_phase2_recovery_with(
    broker: &PostgresBroker,
    build: ChildBuild<'_>,
    grace_ms: u64,
    max_rows: i64,
    quarantine_after_attempts: u32,
) -> Result<Phase2RecoverySummary, WorkflowError> {
    let grace_ms = i64::try_from(grace_ms).map_err(|_| {
        WorkflowError::Validation(format!(
            "phase-2 grace of {grace_ms} ms is above the integer range"
        ))
    })?;
    let attempt_bound = i32::try_from(quarantine_after_attempts).map_err(|_| {
        WorkflowError::Validation(format!(
            "phase-2 attempt bound {quarantine_after_attempts} is above the integer range"
        ))
    })?;
    let candidate_rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &DISCOVER_PENDING
            .bind()
            .value(&grace_ms)
            .value(&attempt_bound)
            .value(&max_rows)
            .finish(),
    )
    .await?;
    let candidates = candidate_rows
        .iter()
        .map(|row| column::<Uuid>(row, &DISCOVER_PENDING, "task_id"))
        .collect::<Result<Vec<Uuid>, RowDecodeError>>()
        .map_err(StepContractError::from)?;
    let count_rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &COUNT_OVER_ATTEMPT_BOUND
            .bind()
            .value(&attempt_bound)
            .finish(),
    )
    .await?;
    let over_attempt_bound: i64 = one_row(count_rows, &COUNT_OVER_ATTEMPT_BOUND)
        .and_then(|row| column(&row, &COUNT_OVER_ATTEMPT_BOUND, "over_attempt_bound"))
        .map_err(StepContractError::from)?;
    let mut summary = Phase2RecoverySummary {
        over_attempt_bound,
        ..Phase2RecoverySummary::default()
    };
    for task_id in candidates {
        summary.considered += 1;
        match run_catching_panic(phase2_recover(broker, task_id, attempt_bound, build)).await {
            Ok(committed) => {
                count_recovered(&mut summary, task_id, &committed);
                advance_after_recovery(broker, task_id, committed, build).await;
            }
            Err(failure) => {
                summary.failed += 1;
                tracing::error!(
                    task_id = %task_id,
                    error_type = failure.error_type(),
                    error = %failure.detail(),
                    "Phase-2 recovery failed for task {task_id}: {}: {}",
                    failure.error_type(),
                    failure.detail(),
                );
                match failure.class() {
                    FailureClass::Transient => {}
                    FailureClass::Persistent => {
                        let record = record_raise(broker, task_id, attempt_bound, &failure).await;
                        count_raise(&mut summary, task_id, record);
                    }
                }
            }
        }
    }
    Ok(summary)
}

/// Counts one committed record into the summary, and logs a retained record
/// and its quarantine. Pure, apart from the log.
fn count_recovered(
    summary: &mut Phase2RecoverySummary,
    task_id: Uuid,
    committed: &CommittedStep<NodeFinishDisposition>,
) {
    match committed.disposition {
        NodeFinishDisposition::Applied => summary.applied += 1,
        NodeFinishDisposition::AppliedWorkflowEnded => summary.superseded += 1,
        NodeFinishDisposition::AlreadyApplied => summary.already_applied += 1,
        NodeFinishDisposition::PendingAbsent => summary.pending_absent += 1,
        NodeFinishDisposition::Retained(failure) => {
            summary.retained += 1;
            let detail = format!("{task_id}: {}", failure.as_str());
            // Verbatim: an operator acts on the disposition, and a count
            // alone hides a population that never disposes.
            tracing::error!("Phase-2 recovery retained evidence — {detail}");
            summary.retained_details.push(detail);
            // The function quarantined the record at the bound; `detail`
            // holds the verdict then.
            match committed.detail.as_deref() {
                None => {}
                Some(verdict) => count_quarantine(summary, task_id, verdict),
            }
        }
    }
}

/// Counts one quarantine verdict (`VERDICT[: detail]`) into the summary.
/// Pure, apart from the log.
fn count_quarantine(summary: &mut Phase2RecoverySummary, task_id: Uuid, verdict: &str) {
    match quarantine_result(verdict) {
        QuarantineResult::Quarantined => {
            summary.quarantined += 1;
            tracing::error!(
                task_id = %task_id,
                verdict,
                "Phase-2 recovery quarantined the record at the attempt bound",
            );
        }
        QuarantineResult::Refused => {
            let refusal = format!("{task_id}: quarantine refused with {verdict}");
            tracing::error!("Phase-2 recovery {refusal}");
            summary.quarantine_refusals.push(refusal);
        }
        QuarantineResult::OutsideContract => {
            let refusal = format!("{task_id}: quarantine verdict outside the contract: {verdict}");
            tracing::error!("Phase-2 recovery {refusal}");
            summary.quarantine_refusals.push(refusal);
        }
    }
}

/// The meaning of a verdict text (`VERDICT[: detail]`). Pure.
fn quarantine_result(verdict_text: &str) -> QuarantineResult {
    let verdict = verdict_text
        .split_once(": ")
        .map_or(verdict_text, |(verdict, _)| verdict);
    match verdict {
        "REPOINTED" | "PENDING_GONE" | "ALREADY_QUARANTINED" => QuarantineResult::Quarantined,
        "NODE_ROW_ABSENT" | "SOURCE_ABSENT" | "COPY_VERIFICATION_FAILED" => {
            QuarantineResult::Refused
        }
        _ => QuarantineResult::OutsideContract,
    }
}

/// Runs the parent advances that a committed record owes, on the reserve.
/// A failure or a panic is logged; workflow recovery (case 1.6) advances
/// the parent node later. Nothing is counted on the record: it committed.
async fn advance_after_recovery(
    broker: &PostgresBroker,
    task_id: Uuid,
    committed: CommittedStep<NodeFinishDisposition>,
    build: ChildBuild<'_>,
) {
    if committed.parent_advances.is_empty() {
        return;
    }
    let advanced = run_catching_panic(async {
        advance_parents(
            broker,
            CheckoutClass::Reserved,
            committed.parent_advances,
            build,
        )
        .await
        .map_err(|failure| failure.error)
    })
    .await;
    if let Err(failure) = advanced {
        tracing::error!(
            task_id = %task_id,
            error_type = failure.error_type(),
            error = %failure.detail(),
            "Phase-2 recovery of task {task_id}: the parent advance failed: {}: {}; workflow \
             recovery advances it",
            failure.error_type(),
            failure.detail(),
        );
    }
}

/// Counts one persistent raise of a record: the context read (the workflow
/// and the node of the record, for the limit error), then
/// `horsies_workflow_phase2_record_raise`, each one statement on the main
/// core pool with the reserve. Python: `_record_raised_row`
/// (`core/workflows/phase2_recovery.py:594-678`).
///
/// The limit error names `attempt_bound` passes: the function writes the
/// limit when the attempt count after the raise reaches the bound, and
/// discovery returns only records under the bound, so the count after the
/// raise is the bound.
///
/// Errors: [`WorkflowError::Broker`] when a statement failed (nothing
/// recorded, or its outcome is unknown); [`WorkflowError::StepContract`] for
/// a row outside the contract; [`WorkflowError::Serialization`] when the
/// limit error does not encode.
async fn record_raise(
    broker: &PostgresBroker,
    task_id: Uuid,
    attempt_bound: i32,
    failure: &CandidateFailure,
) -> Result<RaiseRecord, WorkflowError> {
    let rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &RAISED_RECORD_CONTEXT.bind().value(&task_id).finish(),
    )
    .await?;
    let Some(row) = first_row(rows) else {
        return Ok(RaiseRecord::PendingAbsent);
    };
    let workflow_id: Uuid =
        column(&row, &RAISED_RECORD_CONTEXT, "workflow_id").map_err(StepContractError::from)?;
    let task_index: i32 =
        column(&row, &RAISED_RECORD_CONTEXT, "task_index").map_err(StepContractError::from)?;
    let payloads = recovery_limit_payloads(&recovery_limit_error(
        workflow_id,
        RecoveryCase::Phase2Progression { task_index },
        failure,
        attempt_bound,
    ))?;
    let rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &RECORD_RAISE
            .bind()
            .value(&task_id)
            .value(&attempt_bound)
            .value(&payloads.workflow_error_json)
            .value(&payloads.node_result_json)
            .finish(),
    )
    .await?;
    Ok(raise_record(rows)?)
}

/// Decodes the one row of `horsies_workflow_phase2_record_raise`. Pure.
pub(crate) fn raise_record(rows: Vec<Row>) -> Result<RaiseRecord, StepContractError> {
    let row = one_row(rows, &RECORD_RAISE)?;
    let outcome: String = column(&row, &RECORD_RAISE, "outcome")?;
    match outcome.as_str() {
        "PENDING_ABSENT" => Ok(RaiseRecord::PendingAbsent),
        "COUNTED" => Ok(RaiseRecord::Counted {
            attempt_count: column(&row, &RECORD_RAISE, "attempt_count")?,
        }),
        "NOT_RECORDED" => Ok(RaiseRecord::NotRecorded {
            attempt_count: column(&row, &RECORD_RAISE, "attempt_count")?,
        }),
        "LIMIT_REACHED" => {
            let limit_text: String = column(&row, &RECORD_RAISE, "limit_outcome")?;
            let limit = match limit_text.as_str() {
                "LIMIT_REACHED_FAILED" => Phase2Limit::WorkflowFailed,
                "LIMIT_REACHED_LIVE_NODES" => Phase2Limit::LiveNodesFinish,
                _ => {
                    return Err(RowDecodeError::Shape {
                        statement: RECORD_RAISE.name(),
                        reason: format!(
                            "LIMIT_REACHED with the limit outcome {limit_text:?}; the contract \
                             is LIMIT_REACHED_FAILED or LIMIT_REACHED_LIVE_NODES"
                        ),
                    }
                    .into())
                }
            };
            Ok(RaiseRecord::LimitReached {
                attempt_count: column(&row, &RECORD_RAISE, "attempt_count")?,
                limit,
                quarantine_verdict: column(&row, &RECORD_RAISE, "quarantine_verdict")?,
            })
        }
        _ => Err(StepContractError::UnknownOutcome {
            function: RECORD_RAISE.name(),
            outcome,
        }),
    }
}

/// Counts what the raise record did into the summary, as Python's
/// `_RaisedRowRecorded` and `_RaisedRowNotRecorded`. Pure, apart from the
/// log.
fn count_raise(
    summary: &mut Phase2RecoverySummary,
    task_id: Uuid,
    record: Result<RaiseRecord, WorkflowError>,
) {
    match record {
        Ok(RaiseRecord::Counted { attempt_count }) => {
            summary.raised_counted += 1;
            tracing::warn!(
                task_id = %task_id,
                attempt_count,
                "phase-2 recovery counted a persistent raise of the record",
            );
        }
        Ok(RaiseRecord::LimitReached {
            attempt_count,
            limit,
            quarantine_verdict,
        }) => {
            summary.raised_counted += 1;
            tracing::error!(
                task_id = %task_id,
                attempt_count,
                limit = ?limit,
                "phase-2 recovery limit reached; the workflow fails closed",
            );
            count_quarantine(summary, task_id, &quarantine_verdict);
        }
        Ok(RaiseRecord::NotRecorded { attempt_count }) => tracing::warn!(
            task_id = %task_id,
            attempt_count,
            "phase-2 recovery raise at the bound not recorded: the workflow is not RUNNING",
        ),
        Ok(RaiseRecord::PendingAbsent) => {}
        Err(error) => tracing::error!(
            task_id = %task_id,
            %error,
            "Phase-2 recovery could not record the raise of task {task_id}: {error}",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::history::phase2::consumption::{consume_phase2, Phase2DispositionKind};
    use crate::core::lifecycle::TerminalizationOutcome;
    use crate::test_support::child_specs::TestChildSpecs;
    use crate::test_support::terminal_writes::{self, TerminalFailure};
    use serial_test::serial;
    use sqlx::PgPool;

    /// One recovery pass with the given grace, cap and attempt bound, and no
    /// child specs.
    async fn recovery_pass(
        pool: &PgPool,
        grace_ms: u64,
        max_rows: i64,
        attempt_bound: u32,
    ) -> Result<Phase2RecoverySummary, WorkflowError> {
        drive_phase2_recovery_with(
            &PostgresBroker::from_pool(pool.clone()),
            ChildBuild::Client(&TestChildSpecs::empty()),
            grace_ms,
            max_rows,
            attempt_bound,
        )
        .await
    }

    async fn seed_workflow_task(
        pool: &PgPool,
        workflow_id: Uuid,
        task_id: Uuid,
        with_dependent: bool,
    ) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_phase2', 'RUNNING', 'fail', NULL,
                       'test.p7.phase2.v1', 0, $1, NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("seed workflow");
        sqlx::query(
            "INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
                started_at, claimed_at, claimed_by_worker_id, is_workflow_task, retry_count,
                max_retries, command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input, created_at, updated_at, execution,
                worker_pid, worker_hostname, worker_process_name
            ) VALUES (
                $1, 'p7_phase2_root', 'default', 100, 'null', 'RUNNING', NOW(), NOW(), NOW(),
                NOW(), 'p7-worker', TRUE, 0, 0, NULL, NULL, 'forever', FALSE, NOW(), NOW(),
                'ISOLATED', 4242, 'p7-host', 'p7-process'
            )",
        )
        .bind(task_id)
        .execute(pool)
        .await
        .expect("seed task");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, task_id,
                created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'root', 'p7_phase2_root', 'null', 'default', 100, '{}', FALSE,
                'all', 'RUNNING', FALSE, $3, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(task_id)
        .execute(pool)
        .await
        .expect("seed workflow task");
        if with_dependent {
            sqlx::query(
                "INSERT INTO horsies_workflow_tasks (
                    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
                    priority, dependencies, allow_failed_deps, join_type, status,
                    is_subworkflow, created_at, max_retries, execution, retention_class_key
                ) VALUES (
                    'NONE', $1, $2, 1, 'dependent', 'p7_phase2_dependent', 'null', 'default', 100,
                    '{0}', FALSE, 'all', 'PENDING', FALSE, NOW(), 0, 'ISOLATED',
                    'standard_30d'
                )",
            )
            .bind(Uuid::new_v4())
            .bind(workflow_id)
            .execute(pool)
            .await
            .expect("seed dependent");
        }
    }

    async fn terminalize_workflow_task(pool: &PgPool, task_id: Uuid) {
        let result = serde_json::to_string(&crate::core::TaskResult::Ok(
            serde_json::json!({"value": 7}),
        ))
        .unwrap();
        let outcomes = terminal_writes::complete_workflow_task(pool, task_id, "p7-worker", &result)
            .await
            .expect("terminalize workflow task");
        assert!(matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ));
    }

    async fn cleanup_case(pool: &PgPool, workflow_id: Uuid, task_id: Uuid) {
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .expect("remove the pending recovery fixture");
        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks
                WHERE workflow_id = $1 AND task_id IS NOT NULL
             )",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_task_history WHERE task_id = $1")
            .bind(task_id)
            .execute(pool)
            .await
            .ok();
    }

    #[test]
    fn a_complete_locked_record_stands_for_a_completed_task() {
        assert_eq!(
            terminal_status_of_pending_kind("COMPLETE_LOCKED"),
            Ok(TaskStatus::Completed)
        );
    }

    #[test]
    fn a_running_or_stale_failure_record_stands_for_a_failed_task() {
        for kind in ["FAIL_RUNNING", "FAIL_STALE"] {
            assert_eq!(
                terminal_status_of_pending_kind(kind),
                Ok(TaskStatus::Failed),
                "{kind}"
            );
        }
    }

    #[test]
    fn an_expiry_record_stands_for_an_expired_task() {
        for kind in ["EXPIRE_CLAIMED", "EXPIRE_PENDING"] {
            assert_eq!(
                terminal_status_of_pending_kind(kind),
                Ok(TaskStatus::Expired),
                "{kind}"
            );
        }
    }

    /// Every kind that writes no phase-2 record, and a value that is no kind,
    /// is refused with the value named.
    #[test]
    fn every_other_kind_writes_no_phase_two_record() {
        let record_kinds = [
            TerminalizationKind::CompleteLocked,
            TerminalizationKind::FailRunning,
            TerminalizationKind::FailStale,
            TerminalizationKind::ExpireClaimed,
            TerminalizationKind::ExpirePending,
        ];
        let other_kinds = TerminalizationKind::ALL
            .into_iter()
            .filter(|kind| !record_kinds.contains(kind))
            .map(TerminalizationKind::as_str);
        for kind in other_kinds.chain(["KIND_FROM_THE_FUTURE", ""]) {
            assert_eq!(
                terminal_status_of_pending_kind(kind),
                Err(PendingKindWithoutPhase2 {
                    kind: kind.to_owned()
                }),
                "{kind}"
            );
        }
    }

    /// Python `tests/unit/test_phase2_recovery_policy.py::TestDurableDispositions::test_only_the_three_durable_ones_are_listed`.
    /// The discovery order, the attempt bound and the cap are behaviour:
    /// `discovery_is_oldest_first_strictly_graced_and_capped`.
    #[test]
    fn durable_vocabulary_is_exact() {
        assert_eq!(Phase2DispositionKind::DURABLE.len(), 3);
        assert!(Phase2DispositionKind::AppliedToNode.is_durable());
        assert!(!Phase2DispositionKind::SourceAbsent.is_durable());
    }

    /// A discovery that fails (here: a server that refuses connections) is a
    /// broker error, not a summary of zero records.
    #[tokio::test]
    async fn discovery_failure_is_typed_instead_of_published_as_zero_health() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@127.0.0.1:1/horsies_unreachable")
            .unwrap();
        let error = recovery_pass(&pool, 0, 1, 3)
            .await
            .expect_err("a failed discovery/count query must cross the containment seam");
        assert!(matches!(error, WorkflowError::Broker(_)), "{error:?}");
    }

    #[tokio::test]
    #[serial]
    async fn recovery_contains_retaining_row_advances_healthy_row_and_quarantines_at_bound() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(&pool)
            .await
            .expect("clear pending rows before global recovery");
        let bad_workflow = Uuid::new_v4();
        let bad_task = Uuid::new_v4();
        let good_workflow = Uuid::new_v4();
        let good_task = Uuid::new_v4();
        seed_workflow_task(&pool, bad_workflow, bad_task, false).await;
        seed_workflow_task(&pool, good_workflow, good_task, false).await;
        terminalize_workflow_task(&pool, bad_task).await;
        terminalize_workflow_task(&pool, good_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET result_digest = decode(repeat('ff', 32), 'hex'),
                 created_at = NOW() - INTERVAL '2 hours'
             WHERE task_id = $1",
        )
        .bind(bad_task)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET created_at = NOW() - INTERVAL '1 hour'
             WHERE task_id = $1",
        )
        .bind(good_task)
        .execute(&pool)
        .await
        .unwrap();

        // The function refuses a bound under 3; the smallest bound takes three passes.
        let first = recovery_pass(&pool, 0, 2, 3)
            .await
            .expect("first recovery pass");
        assert_eq!(first.considered, 2);
        assert_eq!(first.applied, 1, "{first:?}");
        assert_eq!(first.retained, 1);
        assert_eq!(first.failed, 0);
        assert_eq!(first.quarantined, 0);

        let second = recovery_pass(&pool, 0, 2, 3)
            .await
            .expect("second recovery pass");
        assert_eq!(second.considered, 1);
        assert_eq!(second.retained, 1);
        assert!(second.quarantine_refusals.is_empty(), "{second:?}");

        let third = recovery_pass(&pool, 0, 2, 3)
            .await
            .expect("third recovery pass");
        assert_eq!(third.considered, 1);
        assert_eq!(third.retained, 1);
        assert_eq!(third.quarantined, 0);
        assert_eq!(third.quarantine_refusals.len(), 1, "{third:?}");
        let (source, attempts): (String, i32) = sqlx::query_as(
            "SELECT recovery_source, attempt_count
             FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        )
        .bind(bad_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(source, "HISTORY");
        assert_eq!(attempts, 3);

        let excluded = recovery_pass(&pool, 0, 2, 3)
            .await
            .expect("over-bound recovery pass");
        assert_eq!(excluded.considered, 0);
        assert_eq!(excluded.over_attempt_bound, 1);
        cleanup_case(&pool, bad_workflow, bad_task).await;
        cleanup_case(&pool, good_workflow, good_task).await;
    }

    #[tokio::test]
    #[serial]
    async fn recovery_rolls_back_one_failed_row_counts_its_raise_and_advances_the_next_candidate() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(&pool)
            .await
            .expect("clear pending rows before global recovery");
        let bad_workflow = Uuid::new_v4();
        let bad_task = Uuid::new_v4();
        let good_workflow = Uuid::new_v4();
        let good_task = Uuid::new_v4();
        seed_workflow_task(&pool, bad_workflow, bad_task, false).await;
        seed_workflow_task(&pool, good_workflow, good_task, false).await;
        terminalize_workflow_task(&pool, bad_task).await;
        terminalize_workflow_task(&pool, good_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET created_at = NOW() - CASE task_id
                 WHEN $1 THEN INTERVAL '2 hours' ELSE INTERVAL '1 hour' END
             WHERE task_id IN ($1, $2)",
        )
        .bind(bad_task)
        .bind(good_task)
        .execute(&pool)
        .await
        .unwrap();
        // A persistent raise (P0001, the RAISE default): it rolls the row
        // back and adds one attempt (#819). A transient raise adds none
        // (`a_transient_raise_adds_no_attempt`).
        sqlx::query(&format!(
            "CREATE OR REPLACE FUNCTION p7_phase2_inject_row_failure() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{bad_workflow}'::uuid THEN
                     RAISE EXCEPTION 'injected phase-2 row failure';
                 END IF;
                 RETURN NEW;
             END
             $body$",
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER p7_phase2_inject_row_failure
             BEFORE UPDATE ON horsies_workflow_tasks FOR EACH ROW
             EXECUTE FUNCTION p7_phase2_inject_row_failure()",
        )
        .execute(&pool)
        .await
        .unwrap();

        let summary = recovery_pass(&pool, 0, 2, 3)
            .await
            .expect("contained recovery pass");
        assert_eq!(summary.considered, 2, "{summary:?}");
        assert_eq!(summary.failed, 1, "{summary:?}");
        assert_eq!(summary.applied, 1, "{summary:?}");
        assert_eq!(summary.retained, 0, "{summary:?}");
        // The raise is persistent (P0001): its attempt is counted on the record.
        assert_eq!(summary.raised_counted, 1, "{summary:?}");
        let states: (String, i32, i64, String, i64) = sqlx::query_as(
            "SELECT
                 (SELECT status FROM horsies_workflow_tasks
                  WHERE workflow_id = $1 AND task_index = 0),
                 (SELECT attempt_count FROM horsies_workflow_phase2_pending
                  WHERE task_id = $2),
                 (SELECT count(*) FROM horsies_task_history WHERE task_id = $2),
                 (SELECT status FROM horsies_workflow_tasks
                  WHERE workflow_id = $3 AND task_index = 0),
                 (SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $4)",
        )
        .bind(bad_workflow)
        .bind(bad_task)
        .bind(good_workflow)
        .bind(good_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            states,
            ("RUNNING".to_owned(), 1, 1, "COMPLETED".to_owned(), 0)
        );
        let failure_class: Option<String> = sqlx::query_scalar(
            "SELECT last_failure_class FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        )
        .bind(bad_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(failure_class.as_deref(), Some("RAISED"));

        sqlx::query("DROP TRIGGER p7_phase2_inject_row_failure ON horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION p7_phase2_inject_row_failure()")
            .execute(&pool)
            .await
            .unwrap();
        cleanup_case(&pool, bad_workflow, bad_task).await;
        cleanup_case(&pool, good_workflow, good_task).await;
    }

    /// Python `tests/integration/test_task_history_phase2_consumption.py::TestDurableDispositions::test_terminal_workflow_supersedes_without_touching_the_node`:
    /// the supersede rule of `horsies_phase2_consume`, the function that
    /// Python's phase 2 calls: the record of a node of an ended workflow is
    /// deleted and the node is not written. The core's phase 2 writes the
    /// node instead (`horsies_workflow_node_finish`;
    /// `engine/task_complete_tests.rs::a_node_of_an_ended_workflow_completes_and_enqueues_nothing`).
    #[tokio::test]
    #[serial]
    async fn terminal_workflow_supersedes_pending_evidence_without_touching_the_node() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        for workflow_status in ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"] {
            let workflow_id = Uuid::new_v4();
            let task_id = Uuid::new_v4();
            seed_workflow_task(&pool, workflow_id, task_id, false).await;
            terminalize_workflow_task(&pool, task_id).await;
            sqlx::query(
                "UPDATE horsies_workflows
                 SET status = $2, completed_at = NOW(), updated_at = NOW()
                 WHERE id = $1",
            )
            .bind(workflow_id)
            .bind(workflow_status)
            .execute(&pool)
            .await
            .unwrap();

            let mut transaction = pool.begin().await.unwrap();
            let disposition = consume_phase2(&mut transaction, task_id, "COMPLETED")
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            assert_eq!(
                disposition.disposition,
                Phase2DispositionKind::SupersededByWorkflowTerminal,
                "{workflow_status}",
            );
            let state: (String, i64) = sqlx::query_as(
                "SELECT wt.status,
                        (SELECT count(*) FROM horsies_workflow_phase2_pending p
                         WHERE p.task_id = $2)
                 FROM horsies_workflow_tasks wt
                 WHERE wt.workflow_id = $1 AND wt.task_index = 0",
            )
            .bind(workflow_id)
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(state, ("RUNNING".to_owned(), 0), "{workflow_status}");
            cleanup_case(&pool, workflow_id, task_id).await;
        }
    }

    #[tokio::test]
    #[serial]
    async fn every_retaining_disposition_preserves_available_evidence() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;

        let mut transaction = pool.begin().await.unwrap();
        let absent = consume_phase2(&mut transaction, Uuid::new_v4(), "FAILED")
            .await
            .unwrap();
        assert_eq!(absent.disposition, Phase2DispositionKind::PendingAbsent);
        transaction.rollback().await.unwrap();

        let source_absent_workflow = Uuid::new_v4();
        let source_absent_task = Uuid::new_v4();
        seed_workflow_task(&pool, source_absent_workflow, source_absent_task, false).await;
        terminalize_workflow_task(&pool, source_absent_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET history_anchor = history_anchor + INTERVAL '90 days'
             WHERE task_id = $1",
        )
        .bind(source_absent_task)
        .execute(&pool)
        .await
        .unwrap();
        let mut transaction = pool.begin().await.unwrap();
        let absent_source = consume_phase2(&mut transaction, source_absent_task, "FAILED")
            .await
            .unwrap();
        assert_eq!(
            absent_source.disposition,
            Phase2DispositionKind::SourceAbsent
        );
        transaction.commit().await.unwrap();
        let still_pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
        )
        .bind(source_absent_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(still_pending);
        cleanup_case(&pool, source_absent_workflow, source_absent_task).await;

        let version_workflow = Uuid::new_v4();
        let version_task = Uuid::new_v4();
        seed_workflow_task(&pool, version_workflow, version_task, false).await;
        terminalize_workflow_task(&pool, version_task).await;
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET history_schema_version = 2 WHERE task_id = $1",
        )
        .bind(version_task)
        .execute(&pool)
        .await
        .unwrap();
        let mut transaction = pool.begin().await.unwrap();
        let version = consume_phase2(&mut transaction, version_task, "FAILED")
            .await
            .unwrap();
        assert_eq!(
            version.disposition,
            Phase2DispositionKind::SourceVersionConflict
        );
        transaction.commit().await.unwrap();
        let still_pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
        )
        .bind(version_task)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(still_pending);
        cleanup_case(&pool, version_workflow, version_task).await;

        let state_workflow = Uuid::new_v4();
        let state_task = Uuid::new_v4();
        seed_workflow_task(&pool, state_workflow, state_task, false).await;
        terminalize_workflow_task(&pool, state_task).await;
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query(
            "ALTER TABLE horsies_workflow_phase2_pending
             DROP CONSTRAINT horsies_workflow_phase2_pending_node_fkey",
        )
        .execute(transaction.as_mut())
        .await
        .unwrap();
        sqlx::query(
            "DELETE FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 0",
        )
        .bind(state_workflow)
        .execute(transaction.as_mut())
        .await
        .unwrap();
        let state = consume_phase2(&mut transaction, state_task, "FAILED")
            .await
            .unwrap();
        assert_eq!(
            state.disposition,
            Phase2DispositionKind::SourceStateConflict
        );
        let still_pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = $1)",
        )
        .bind(state_task)
        .fetch_one(transaction.as_mut())
        .await
        .unwrap();
        assert!(still_pending);
        transaction.rollback().await.unwrap();
        cleanup_case(&pool, state_workflow, state_task).await;
    }

    /// The recovery reads the record's kind (the baseline record has no
    /// status column) and recovers a `FAIL_RUNNING` record with the FAILED
    /// node status (Python `core/workflows/phase2_recovery.py`, discovery and
    /// `terminal_status_of_pending_kind`).
    #[tokio::test]
    #[serial]
    async fn recovery_reads_the_kind_of_a_failure_record_and_fails_its_node() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(&pool)
            .await
            .expect("clear pending rows before global recovery");
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        seed_workflow_task(&pool, workflow_id, task_id, false).await;
        let failure = serde_json::to_string(&crate::core::TaskResult::<serde_json::Value>::Err(
            crate::core::TaskError::new("NODE_FAILURE", "the node failed")
                .expect("a user error code"),
        ))
        .expect("encode the failure result");
        let outcomes = terminal_writes::fail_running_task(
            &pool,
            task_id,
            "p7-worker",
            &TerminalFailure::of_task(&failure, Some("NODE_FAILURE"), Some("the node failed")),
        )
        .await
        .expect("fail the workflow task");
        assert!(matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ));
        let kind: String = sqlx::query_scalar(
            "UPDATE horsies_workflow_phase2_pending
             SET created_at = NOW() - INTERVAL '1 hour'
             WHERE task_id = $1
             RETURNING terminalization_kind",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("age the phase-2 record");
        assert_eq!(kind, "FAIL_RUNNING");

        let summary = drive_phase2_recovery_with(
            &PostgresBroker::from_pool(pool.clone()),
            ChildBuild::Client(&TestChildSpecs::empty()),
            0,
            1,
            25,
        )
        .await
        .expect("recovery pass");

        let node_status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflow_tasks WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .expect("read the node");
        let records: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .expect("count the phase-2 records");
        cleanup_case(&pool, workflow_id, task_id).await;
        assert_eq!(summary.considered, 1, "{summary:?}");
        assert_eq!(summary.applied, 1, "{summary:?}");
        assert_eq!(summary.failed, 0, "{summary:?}");
        assert_eq!(node_status, "FAILED");
        assert_eq!(records, 0);
    }

    #[tokio::test]
    #[serial]
    async fn discovery_is_oldest_first_strictly_graced_and_capped() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let mut cases = Vec::new();
        for age in ["3 hours", "2 hours", "1 minute"] {
            let workflow_id = Uuid::new_v4();
            let task_id = Uuid::new_v4();
            seed_workflow_task(&pool, workflow_id, task_id, false).await;
            terminalize_workflow_task(&pool, task_id).await;
            sqlx::query(
                "UPDATE horsies_workflow_phase2_pending
                 SET result_digest = decode(repeat('ff', 32), 'hex'),
                     created_at = NOW() - $2::interval
                 WHERE task_id = $1",
            )
            .bind(task_id)
            .bind(age)
            .execute(&pool)
            .await
            .unwrap();
            cases.push((workflow_id, task_id));
        }

        let summary = recovery_pass(&pool, 60 * 60 * 1_000, 1, 25)
            .await
            .expect("graced recovery pass");
        assert_eq!(summary.considered, 1);
        assert_eq!(summary.retained, 1);
        let attempts: Vec<i32> = sqlx::query_scalar(
            "SELECT attempt_count FROM horsies_workflow_phase2_pending
             WHERE task_id = ANY($1::uuid[])
             ORDER BY created_at, task_id",
        )
        .bind(
            cases
                .iter()
                .map(|(_, task_id)| *task_id)
                .collect::<Vec<_>>(),
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(attempts, vec![1, 0, 0]);
        for (workflow_id, task_id) in cases {
            cleanup_case(&pool, workflow_id, task_id).await;
        }
    }

    /// The quarantine step of the recovery loop on the baseline function
    /// (#861): a row whose consumption keeps retaining is quarantined at the
    /// attempt bound, and its pending row points at the verified copy.
    /// Python: `python/horsies/core/workflows/phase2_recovery.py:681-725`
    /// (`_quarantine_exhausted_row`). The history row and the pending row
    /// agree on schema version 2: the consumer refuses it
    /// (`SOURCE_VERSION_CONFLICT`), and the copy check accepts it. Two earlier
    /// passes retained the row; this pass reaches the bound of 3, the smallest
    /// bound that `horsies_workflow_phase2_recover` takes (3..1000).
    #[tokio::test]
    #[serial]
    async fn the_recovery_loop_quarantines_a_retaining_row_at_the_attempt_bound() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(&pool)
            .await
            .expect("clear pending rows before global recovery");
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        seed_workflow_task(&pool, workflow_id, task_id, false).await;
        terminalize_workflow_task(&pool, task_id).await;
        sqlx::query(
            "UPDATE horsies_task_history SET history_schema_version = 2 WHERE task_id = $1",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("set the history schema version");
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET history_schema_version = 2
             WHERE task_id = $1",
        )
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("set the pending schema version");
        store_prior_attempts(&pool, task_id, 2, "SOURCE_VERSION_CONFLICT").await;

        let summary = drive_phase2_recovery_with(
            &PostgresBroker::from_pool(pool.clone()),
            ChildBuild::Client(&TestChildSpecs::empty()),
            0,
            1,
            3,
        )
        .await
        .expect("recovery pass");

        assert_eq!(summary.considered, 1, "{summary:?}");
        assert_eq!(summary.failed, 0, "{summary:?}");
        assert_eq!(summary.retained, 1, "{summary:?}");
        assert_eq!(summary.quarantined, 1, "{summary:?}");
        assert!(summary.quarantine_refusals.is_empty(), "{summary:?}");
        let (source, attempts, failure_class, copies): (String, i32, Option<String>, i64) =
            sqlx::query_as(
                "SELECT p.recovery_source, p.attempt_count, p.last_failure_class,
                        (SELECT count(*) FROM horsies_workflow_phase2_quarantine AS q
                         WHERE q.task_id = p.task_id)
                 FROM horsies_workflow_phase2_pending AS p
                 WHERE p.task_id = $1",
            )
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the pending row");
        assert_eq!(source, "QUARANTINE");
        assert_eq!(attempts, 3);
        assert_eq!(failure_class.as_deref(), Some("SOURCE_VERSION_CONFLICT"));
        assert_eq!(copies, 1);
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    // -----------------------------------------------------------------------
    // horsies_workflow_phase2_recover and horsies_workflow_phase2_record_raise
    // -----------------------------------------------------------------------

    /// A trigger that raises with `sqlstate` when a node of `workflow_id` is
    /// written COMPLETED: the recovery step of the record raises, and a limit
    /// write (FAILED) does not.
    async fn fail_completion_of(pool: &PgPool, name: &str, workflow_id: Uuid, sqlstate: &str) {
        sqlx::query(&format!(
            "CREATE OR REPLACE FUNCTION {name}() RETURNS trigger
             LANGUAGE plpgsql AS $body$
             BEGIN
                 IF NEW.workflow_id = '{workflow_id}'::uuid AND NEW.status = 'COMPLETED' THEN
                     RAISE EXCEPTION 'injected phase-2 recovery raise' USING ERRCODE = '{sqlstate}';
                 END IF;
                 RETURN NEW;
             END
             $body$",
        ))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            "CREATE TRIGGER {name} BEFORE UPDATE ON horsies_workflow_tasks
             FOR EACH ROW EXECUTE FUNCTION {name}()",
        ))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn drop_trigger(pool: &PgPool, name: &str) {
        sqlx::query(&format!("DROP TRIGGER {name} ON horsies_workflow_tasks"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP FUNCTION {name}()"))
            .execute(pool)
            .await
            .unwrap();
    }

    /// A completed workflow task whose phase-2 record is the only one, past
    /// any grace, with `attempts` stored. Earlier attempts were raises: a
    /// record with attempts holds the time and the class of the last one
    /// (`horsies_workflow_phase2_pending_attempts`).
    async fn lone_record(pool: &PgPool, workflow_id: Uuid, task_id: Uuid, attempts: i32) {
        sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
            .execute(pool)
            .await
            .expect("clear pending rows before global recovery");
        seed_workflow_task(pool, workflow_id, task_id, false).await;
        terminalize_workflow_task(pool, task_id).await;
        store_prior_attempts(pool, task_id, attempts, "RAISED").await;
    }

    /// Stores `attempts` earlier attempts on the record of `task_id`, the last
    /// one of `failure_class`, and ages the record past any grace. With no
    /// attempt, the time and the class stay NULL.
    async fn store_prior_attempts(
        pool: &PgPool,
        task_id: Uuid,
        attempts: i32,
        failure_class: &str,
    ) {
        sqlx::query(
            "UPDATE horsies_workflow_phase2_pending
             SET attempt_count = $2,
                 last_attempt_at = CASE WHEN $2 = 0 THEN NULL ELSE NOW() - INTERVAL '30 minutes' END,
                 last_failure_class = CASE WHEN $2 = 0 THEN NULL ELSE $3 END,
                 created_at = NOW() - INTERVAL '1 hour'
             WHERE task_id = $1",
        )
        .bind(task_id)
        .bind(attempts)
        .bind(failure_class)
        .execute(pool)
        .await
        .unwrap();
    }

    /// The attempt count and failure class of a record.
    async fn record_attempts(pool: &PgPool, task_id: Uuid) -> (i32, Option<String>) {
        sqlx::query_as(
            "SELECT attempt_count, last_failure_class
             FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A persistent raise at the bound fails the RUNNING workflow closed with
    /// `WORKFLOW_RECOVERY_LIMIT_REACHED`, fails the record's node, adds the
    /// attempt and quarantines the evidence.
    #[tokio::test]
    #[serial]
    async fn a_raise_at_the_bound_fails_the_workflow_closed_and_quarantines() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        lone_record(&pool, workflow_id, task_id, 2).await;
        fail_completion_of(&pool, "phase2_limit_raise", workflow_id, "P0001").await;

        let summary = recovery_pass(&pool, 0, 10, 3).await;
        drop_trigger(&pool, "phase2_limit_raise").await;
        let summary = summary.expect("recovery pass");

        assert_eq!(
            (summary.failed, summary.raised_counted, summary.quarantined),
            (1, 1, 1),
            "{summary:?}"
        );
        let (status, error_json, node_status): (String, Option<String>, String) = sqlx::query_as(
            "SELECT w.status, w.error, n.status FROM horsies_workflows w
             JOIN horsies_workflow_tasks n ON n.workflow_id = w.id AND n.task_index = 0
             WHERE w.id = $1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            (status.as_str(), node_status.as_str()),
            ("FAILED", "FAILED")
        );
        let error: crate::core::TaskError =
            serde_json::from_str(&error_json.expect("the limit stores the workflow error"))
                .unwrap();
        assert_eq!(
            error.error_code.to_string(),
            "WORKFLOW_RECOVERY_LIMIT_REACHED"
        );
        assert_eq!(
            error.data.as_ref().and_then(|data| data.get("case")),
            Some(&serde_json::json!("phase2_progression"))
        );
        let (source, attempts): (String, i32) = sqlx::query_as(
            "SELECT recovery_source, attempt_count
             FROM horsies_workflow_phase2_pending WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((source.as_str(), attempts), ("QUARANTINE", 3));
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    /// A transient raise (40001, retryable) adds no attempt: an outage does
    /// not march a record to the bound.
    #[tokio::test]
    #[serial]
    async fn a_transient_raise_adds_no_attempt() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        lone_record(&pool, workflow_id, task_id, 0).await;
        fail_completion_of(&pool, "phase2_transient_raise", workflow_id, "40001").await;

        let summary = recovery_pass(&pool, 0, 10, 3).await;
        drop_trigger(&pool, "phase2_transient_raise").await;
        let summary = summary.expect("recovery pass");

        assert_eq!(
            (summary.failed, summary.raised_counted),
            (1, 0),
            "{summary:?}"
        );
        assert_eq!(record_attempts(&pool, task_id).await, (0, None));
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    /// A raise that would reach the bound while the workflow is PAUSED is not
    /// recorded: nothing is written, and a pass after a resume raises again.
    #[tokio::test]
    #[serial]
    async fn a_raise_at_the_bound_of_a_paused_workflow_is_not_recorded() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker = PostgresBroker::from_pool(pool.clone());
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        lone_record(&pool, workflow_id, task_id, 2).await;
        sqlx::query("UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        let failure = CandidateFailure::Error(WorkflowError::Validation("raised".to_owned()));

        let record = record_raise(&broker, task_id, 3, &failure)
            .await
            .expect("record the raise");

        assert_eq!(record, RaiseRecord::NotRecorded { attempt_count: 2 });
        assert_eq!(
            record_attempts(&pool, task_id).await,
            (2, Some("RAISED".to_owned()))
        );
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    /// Under the bound a raise is counted; a record that is gone is
    /// `PENDING_ABSENT`.
    #[tokio::test]
    #[serial]
    async fn a_raise_under_the_bound_is_counted_and_a_gone_record_is_absent() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker = PostgresBroker::from_pool(pool.clone());
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        lone_record(&pool, workflow_id, task_id, 0).await;
        let failure = CandidateFailure::Error(WorkflowError::Validation("raised".to_owned()));

        assert_eq!(
            record_raise(&broker, task_id, 3, &failure)
                .await
                .expect("record the raise"),
            RaiseRecord::Counted { attempt_count: 1 }
        );
        assert_eq!(
            record_attempts(&pool, task_id).await,
            (1, Some("RAISED".to_owned()))
        );
        assert_eq!(
            record_raise(&broker, Uuid::new_v4(), 3, &failure)
                .await
                .expect("record the raise of a gone record"),
            RaiseRecord::PendingAbsent
        );
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    /// The recovery of a record that promotes a sub-workflow node owes its
    /// child start: with no registration under the child's key, the node fails
    /// in the record's transaction.
    #[tokio::test]
    #[serial]
    async fn recovery_runs_the_child_start_that_its_record_owes() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        lone_record(&pool, workflow_id, task_id, 0).await;
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, allow_failed_deps, join_type, status,
                is_subworkflow, sub_workflow_name, sub_definition_key, sub_workflow_params,
                created_at
            ) VALUES (
                'NONE', $1, $2, 1, 'child', 'p7_phase2_child', '{}', 'default', 100, '{0}', FALSE,
                'all', 'PENDING', TRUE, 'p7_phase2_child', 'tests.p7.phase2.child.v1',
                'NOT_GIVEN', NOW()
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("seed the sub-workflow dependent");

        let summary = recovery_pass(&pool, 0, 10, 3).await.expect("recovery pass");

        assert_eq!((summary.applied, summary.failed), (1, 0), "{summary:?}");
        let (status, result): (String, Option<String>) = sqlx::query_as(
            "SELECT status, result FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "FAILED");
        let stored: crate::core::TaskResult<serde_json::Value> =
            serde_json::from_str(&result.expect("the failed node stores its error")).unwrap();
        let crate::core::TaskResult::Err(error) = stored else {
            panic!("the failed node stores an error");
        };
        assert_eq!(error.error_code.to_string(), "SUBWORKFLOW_LOAD_FAILED");
        cleanup_case(&pool, workflow_id, task_id).await;
    }

    #[test]
    fn each_recovered_disposition_counts_where_python_counts_it() {
        let task_id = Uuid::new_v4();
        let committed = |disposition, detail: Option<&str>| CommittedStep {
            disposition,
            detail: detail.map(str::to_owned),
            parent_advances: crate::workflow_engine::steps::OwedParentAdvances::default(),
        };
        let mut summary = Phase2RecoverySummary::default();
        count_recovered(
            &mut summary,
            task_id,
            &committed(NodeFinishDisposition::Applied, None),
        );
        count_recovered(
            &mut summary,
            task_id,
            &committed(NodeFinishDisposition::AppliedWorkflowEnded, None),
        );
        count_recovered(
            &mut summary,
            task_id,
            &committed(NodeFinishDisposition::AlreadyApplied, None),
        );
        count_recovered(
            &mut summary,
            task_id,
            &committed(NodeFinishDisposition::PendingAbsent, None),
        );
        assert_eq!(
            (
                summary.applied,
                summary.superseded,
                summary.already_applied,
                summary.pending_absent,
                summary.retained
            ),
            (1, 1, 1, 1, 0)
        );

        let retained = NodeFinishDisposition::Retained(
            crate::workflow_engine::steps::SourceFailure::DigestMismatch,
        );
        count_recovered(&mut summary, task_id, &committed(retained, None));
        count_recovered(
            &mut summary,
            task_id,
            &committed(retained, Some("REPOINTED")),
        );
        count_recovered(
            &mut summary,
            task_id,
            &committed(
                retained,
                Some("COPY_VERIFICATION_FAILED: digest of the copy differs"),
            ),
        );
        assert_eq!((summary.retained, summary.quarantined), (3, 1));
        assert_eq!(
            summary.retained_details,
            vec![format!("{task_id}: SOURCE_DIGEST_MISMATCH"); 3]
        );
        assert_eq!(
            summary.quarantine_refusals,
            vec![format!(
                "{task_id}: quarantine refused with COPY_VERIFICATION_FAILED: \
                 digest of the copy differs"
            )]
        );
    }

    #[test]
    fn every_quarantine_verdict_text_reads_and_others_are_outside_the_contract() {
        for verdict in [
            "REPOINTED",
            "PENDING_GONE: no pending row; locator drained",
            "ALREADY_QUARANTINED: x",
        ] {
            assert_eq!(
                quarantine_result(verdict),
                QuarantineResult::Quarantined,
                "{verdict}"
            );
        }
        for verdict in [
            "NODE_ROW_ABSENT: x",
            "SOURCE_ABSENT: history row absent at locator",
            "COPY_VERIFICATION_FAILED",
        ] {
            assert_eq!(
                quarantine_result(verdict),
                QuarantineResult::Refused,
                "{verdict}"
            );
        }
        assert_eq!(
            quarantine_result("REPOINTED_LATER"),
            QuarantineResult::OutsideContract
        );
    }

    static RAISE_ROW: TypedStatement = TypedStatement::new(
        "test_phase2_raise_row",
        "SELECT $1::text AS outcome, $2::integer AS attempt_count,
                $3::text AS limit_outcome, $4::text AS quarantine_verdict",
        &[
            ParameterType::Text,
            ParameterType::Int4,
            ParameterType::Text,
            ParameterType::Text,
        ],
    );

    async fn raise_row(
        broker: &PostgresBroker,
        outcome: &str,
        attempt_count: Option<i32>,
        limit_outcome: Option<&str>,
        verdict: Option<&str>,
    ) -> Vec<Row> {
        let outcome = outcome.to_owned();
        let limit_outcome = limit_outcome.map(str::to_owned);
        let verdict = verdict.map(str::to_owned);
        broker
            .query_main(
                &RAISE_ROW
                    .bind()
                    .value(&outcome)
                    .value(&attempt_count)
                    .value(&limit_outcome)
                    .value(&verdict)
                    .finish(),
            )
            .await
            .expect("literal statement runs")
    }

    #[tokio::test]
    #[serial]
    async fn each_raise_record_outcome_reads_into_its_variant() {
        let broker =
            PostgresBroker::from_pool(crate::broker::terminalization_matrix::migrated_pool().await);

        let cases = [
            (
                raise_row(&broker, "PENDING_ABSENT", None, None, None).await,
                RaiseRecord::PendingAbsent,
            ),
            (
                raise_row(&broker, "COUNTED", Some(1), None, None).await,
                RaiseRecord::Counted { attempt_count: 1 },
            ),
            (
                raise_row(&broker, "NOT_RECORDED", Some(2), None, None).await,
                RaiseRecord::NotRecorded { attempt_count: 2 },
            ),
            (
                raise_row(
                    &broker,
                    "LIMIT_REACHED",
                    Some(3),
                    Some("LIMIT_REACHED_FAILED"),
                    Some("REPOINTED"),
                )
                .await,
                RaiseRecord::LimitReached {
                    attempt_count: 3,
                    limit: Phase2Limit::WorkflowFailed,
                    quarantine_verdict: "REPOINTED".to_owned(),
                },
            ),
            (
                raise_row(
                    &broker,
                    "LIMIT_REACHED",
                    Some(3),
                    Some("LIMIT_REACHED_LIVE_NODES"),
                    Some("REPOINTED"),
                )
                .await,
                RaiseRecord::LimitReached {
                    attempt_count: 3,
                    limit: Phase2Limit::LiveNodesFinish,
                    quarantine_verdict: "REPOINTED".to_owned(),
                },
            ),
        ];
        for (rows, expected) in cases {
            assert_eq!(raise_record(rows).expect("a row of the contract"), expected);
        }
        let refused_limit = raise_record(
            raise_row(
                &broker,
                "LIMIT_REACHED",
                Some(3),
                Some("REFUSED"),
                Some("REPOINTED"),
            )
            .await,
        );
        assert!(
            matches!(
                refused_limit,
                Err(StepContractError::Row(RowDecodeError::Shape { .. }))
            ),
            "{refused_limit:?}"
        );
        let unknown = raise_record(raise_row(&broker, "SKIPPED", None, None, None).await);
        assert!(
            matches!(unknown, Err(StepContractError::UnknownOutcome { .. })),
            "{unknown:?}"
        );
        let counted_without_count =
            raise_record(raise_row(&broker, "COUNTED", None, None, None).await);
        assert!(
            matches!(
                counted_without_count,
                Err(StepContractError::Row(RowDecodeError::Column { .. }))
            ),
            "{counted_without_count:?}"
        );
    }

    /// Python `tests/unit/test_phase2_recovery_policy.py::TestRecoveredFailureResult::test_the_stored_error_survives`
    /// and `tests/unit/test_task_error_stored_decode.py::TestErrorWithNoMessage::test_phase2_recovery_returns_the_error`:
    /// phase-2 recovery writes the failed node with the error that the task
    /// stored, also an error with a code and no message.
    #[tokio::test]
    #[serial]
    async fn recovery_writes_the_stored_error_of_a_failed_task_into_its_node() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let with_message =
            crate::core::TaskError::new("NODE_FAILURE", "the real one").expect("a user error code");
        let without_message = crate::core::TaskError {
            message: None,
            ..crate::core::TaskError::new("RATE_LIMITED", "dropped").expect("a user error code")
        };
        for (code, message, error) in [
            ("NODE_FAILURE", Some("the real one"), with_message),
            ("RATE_LIMITED", None, without_message),
        ] {
            sqlx::query("DELETE FROM horsies_workflow_phase2_pending")
                .execute(&pool)
                .await
                .expect("clear pending rows before global recovery");
            let workflow_id = Uuid::new_v4();
            let task_id = Uuid::new_v4();
            seed_workflow_task(&pool, workflow_id, task_id, false).await;
            let stored =
                serde_json::to_string(&crate::core::TaskResult::<serde_json::Value>::Err(error))
                    .expect("encode the failure result");
            let outcomes = terminal_writes::fail_running_task(
                &pool,
                task_id,
                "p7-worker",
                &TerminalFailure::of_task(&stored, Some(code), None),
            )
            .await
            .expect("fail the workflow task");
            assert!(
                matches!(
                    outcomes.as_slice(),
                    [TerminalizationOutcome::Applied { .. }]
                ),
                "{code}: {outcomes:?}"
            );
            sqlx::query(
                "UPDATE horsies_workflow_phase2_pending
                 SET created_at = NOW() - INTERVAL '1 hour'
                 WHERE task_id = $1",
            )
            .bind(task_id)
            .execute(&pool)
            .await
            .expect("age the phase-2 record");

            let summary = drive_phase2_recovery_with(
                &PostgresBroker::from_pool(pool.clone()),
                ChildBuild::Client(&TestChildSpecs::empty()),
                0,
                1,
                25,
            )
            .await
            .expect("recovery pass");

            let (node_status, node_result): (String, Option<String>) = sqlx::query_as(
                "SELECT status, result FROM horsies_workflow_tasks WHERE task_id = $1",
            )
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("read the node");
            cleanup_case(&pool, workflow_id, task_id).await;
            assert_eq!(summary.applied, 1, "{code}: {summary:?}");
            assert_eq!(node_status, "FAILED", "{code}");
            let node_result = node_result.expect("the node stores the task's result");
            let stored_error = match serde_json::from_str::<
                crate::core::TaskResult<serde_json::Value>,
            >(&node_result)
            .expect("the node result is an envelope")
            {
                crate::core::TaskResult::Err(error) => error,
                crate::core::TaskResult::Ok(value) => panic!("{code}: ok {value}"),
            };
            assert_eq!(stored_error.error_code.to_string(), code);
            assert_eq!(stored_error.message.as_deref(), message, "{code}");
        }
    }
}
