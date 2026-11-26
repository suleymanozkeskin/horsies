//! The owner-gone step of the reaper: the liveness read of each pass, the
//! memory of when and at which generation each worker was first read
//! without its liveness lock, and the recovery of the RUNNING and CLAIMED
//! rows of a worker judged gone.
//!
//! - Each pass reads the owners of CLAIMED and RUNNING rows whose liveness
//!   lock no session holds, each with the generation of its latest
//!   acquisition (`horsies_unheld_worker_owners`). The read takes no lock.
//! - A worker writes a new generation at each acquisition of its lock, and
//!   counts the acquisition only after that write committed. Two reads at
//!   least the gone span apart that return an owner at one generation judge
//!   it ([`crate::worker::liveness::OWNER_GONE_SPAN`]: the worker's regain
//!   bound plus a margin), when no read of this reaper between them found
//!   its lock held or returned another generation. A read of the
//!   generations after the later read (`horsies_worker_liveness_generations`)
//!   must still find that generation: a read takes the generation from its
//!   snapshot, which comes before its read of the lock table.
//! - Such an owner is gone. A live worker takes its lock again within its
//!   regain bound, or it fails closed before the margin ends, so one loss of
//!   a live worker cannot span the two reads. Each regain between them
//!   commits a new generation, so two losses with a regain between them do
//!   not count as one loss. An owner with no recorded generation is never
//!   judged.
//! - With stale running recovery on, each RUNNING row of a gone owner goes
//!   through `horsies_finish_stale_attempt` with the evidence `OWNER_GONE`;
//!   with the stale claim requeue on, its CLAIMED rows go through
//!   `horsies_requeue_stale_claims` with the same evidence. Each statement
//!   judges again under the row lock that the row still belongs to the
//!   owner: at the claim generation that the scan read (RUNNING), or claimed
//!   before the owner was first read unheld (CLAIMED).
//! - The memory belongs to one reaper loop, in its process.
//! - At each retention sweep the reaper deletes the liveness rows of ended
//!   workers ([`forget_ended_workers`]). A deleted row judges no owner gone.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use super::{after_stale_attempt, candidate_log, stale_retry, StaleRecovery};
use crate::broker::core_rows::StatementLog;
use crate::broker::stale_recovery::{
    finish_stale_attempt, forget_ended_worker_liveness, read_liveness_generations,
    read_unheld_owners, requeue_gone_owner_pass, scan_gone_owner_running, FinishStaleAttempt,
    GoneOwner, GoneOwnerCandidate, LivenessGeneration, OwnerClaim, OwnerGeneration, OwnerListReach,
    RecoveredTask, RecoveryBatch, StaleClaimsRequeuePass, StaleEvidence, UnheldOwnerRead,
    STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS, STALE_RUNNING_SCAN_BATCH, UNHELD_OWNER_BATCH,
};
use crate::broker::BrokerError;
use crate::core::codec::task_result_to_json_string;
use crate::core::config::recovery::RecoveryConfig;
use crate::core::python_text::python_isoformat_utc;
use crate::core::{OperationalErrorCode, TaskError, TaskErrorKind, TaskResult};
use crate::db::CorePool;
use crate::log_targets::STALE_RUNNING_PASS;
use crate::telemetry::labels::RecoveryAction;
use crate::telemetry::worker::WorkerTelemetry;

/// Liveness rows that one sweep of [`forget_ended_workers`] deletes at most.
const LIVENESS_FORGET_BATCH: RecoveryBatch = match RecoveryBatch::new(1_000) {
    Some(batch) => batch,
    None => panic!("1000 rows is a recovery batch"),
};

/// Owners whose first unheld read one reaper keeps at most. A read that
/// would pass it clears the memory first: a judgement then only comes later,
/// never earlier.
const MAX_REMEMBERED_OWNERS: usize = 10_000;

/// The first read of a run of reads that found an owner's lock free at one
/// generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FirstUnheldRead {
    /// The end of the read: the latest instant at which it can have found
    /// the lock free.
    ended: Instant,
    /// The database time of the read. Only rows that the owner claimed
    /// before it are recovered by the owner's evidence.
    read_at: DateTime<Utc>,
    /// The owner's generation at the read. Every read of the run returned
    /// the owner at this generation.
    generation: LivenessGeneration,
}

/// An owner that two reads at least the span apart found unheld at one
/// generation. It is gone when a read of the generations after the later
/// read still finds that generation ([`UnheldOwners::confirm`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JudgedOwner {
    pub(crate) worker_id: String,
    pub(crate) generation: LivenessGeneration,
    /// The database time of the first read of the run.
    pub(crate) unheld_since: DateTime<Utc>,
}

/// When this reaper first read each owner's liveness lock free, in the run
/// of its reads that found it free at one generation.
#[derive(Debug, Default)]
pub(crate) struct UnheldOwners {
    first_read: BTreeMap<String, FirstUnheldRead>,
}

impl UnheldOwners {
    /// Record one liveness read that started at `started` and ended at
    /// `ended`, and return the owners it judges: each owner of the read at a
    /// recorded generation whose run at that generation began with a read
    /// that ended at least `span` before `started`.
    ///
    /// An owner that the read covered and did not return was held at the
    /// read (or owns no row any more): its run ends. A cut read covers the
    /// owners up to its last one, in worker id order (byte order, as the
    /// read's C collation), so the run of an owner past the cut goes on. An
    /// owner returned at another generation took its lock again since the
    /// first read of its run: a new run starts at this read. An owner
    /// returned with no recorded generation has no run.
    pub(crate) fn record(
        &mut self,
        read: &UnheldOwnerRead,
        started: Instant,
        ended: Instant,
        span: Duration,
    ) -> Vec<JudgedOwner> {
        let returned: BTreeMap<&str, OwnerGeneration> = read
            .owners
            .iter()
            .map(|owner| (owner.worker_id.as_str(), owner.generation))
            .collect();
        let covered_through: Option<&str> = match read.reach {
            OwnerListReach::Complete => None,
            OwnerListReach::Cut => read.owners.last().map(|owner| owner.worker_id.as_str()),
        };
        self.first_read
            .retain(|worker_id, first| match returned.get(worker_id.as_str()) {
                Some(OwnerGeneration::Recorded(generation)) => *generation == first.generation,
                Some(OwnerGeneration::NotRecorded) => false,
                None => covered_through.is_some_and(|last| worker_id.as_str() > last),
            });
        if self.first_read.len().saturating_add(read.owners.len()) > MAX_REMEMBERED_OWNERS {
            self.first_read.clear();
        }
        let mut judged = Vec::new();
        for owner in &read.owners {
            let generation = match owner.generation {
                OwnerGeneration::Recorded(generation) => generation,
                OwnerGeneration::NotRecorded => continue,
            };
            let first =
                *self
                    .first_read
                    .entry(owner.worker_id.clone())
                    .or_insert(FirstUnheldRead {
                        ended,
                        read_at: owner.read_at,
                        generation,
                    });
            if started.saturating_duration_since(first.ended) >= span {
                judged.push(JudgedOwner {
                    worker_id: owner.worker_id.clone(),
                    generation,
                    unheld_since: first.read_at,
                });
            }
        }
        judged
    }

    /// The judged owners that are gone: each one whose generation in
    /// `current`, read after the read that judged it, is still its judged
    /// generation. The run of every other judged owner ends: it took its
    /// lock again, or its liveness row was deleted, after that read.
    pub(crate) fn confirm(
        &mut self,
        judged: Vec<JudgedOwner>,
        current: &BTreeMap<String, LivenessGeneration>,
    ) -> Vec<GoneOwner> {
        let mut gone = Vec::with_capacity(judged.len());
        for owner in judged {
            match current.get(&owner.worker_id) {
                Some(generation) if *generation == owner.generation => gone.push(GoneOwner {
                    worker_id: owner.worker_id,
                    generation: owner.generation,
                    unheld_since: owner.unheld_since,
                }),
                Some(_) | None => {
                    let _ended: Option<FirstUnheldRead> = self.first_read.remove(&owner.worker_id);
                }
            }
        }
        gone
    }

    /// The owners whose run of unheld reads goes on, for tests.
    #[cfg(test)]
    pub(crate) fn remembered(&self) -> Vec<String> {
        self.first_read.keys().cloned().collect()
    }
}

/// What one owner-gone step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OwnerGoneStep {
    /// Owners that the step judged gone.
    pub(crate) gone_owners: u64,
    /// RUNNING rows of gone owners whose crash retry or failure committed.
    pub(crate) running_recovered: u64,
    /// CLAIMED rows of gone owners that went back to PENDING.
    pub(crate) claims_requeued: u64,
}

/// One owner-gone step on `pool`: the liveness read, the judgement of
/// `memory` with `span`, the read of the generations of the judged owners
/// (only when the read judged one), then for each gone owner the recovery
/// of its RUNNING rows (with `fail_stale_running`) and of its CLAIMED
/// rows (with `requeue_stale_claimed`). Each statement runs alone on
/// `pool` as reserved work and commits alone. A recovery statement that
/// fails is logged, and the step goes on with the next one. Each row whose
/// recovery committed is told to `telemetry` (`RunningRetried`,
/// `RunningFailed`, `ClaimRequeued`).
///
/// Errors: the [`BrokerError`] of the liveness read (the memory is
/// unchanged) or of the read of the generations (the memory holds the
/// liveness read; the next pass judges again). Nothing was written.
pub(crate) async fn owner_gone_step(
    pool: &CorePool,
    log: StatementLog,
    config: &RecoveryConfig,
    memory: &mut UnheldOwners,
    span: Duration,
    telemetry: &WorkerTelemetry,
) -> Result<OwnerGoneStep, BrokerError> {
    let started = Instant::now();
    let read = read_unheld_owners(pool, log, UNHELD_OWNER_BATCH).await?;
    let ended = Instant::now();
    let judged = memory.record(&read, started, ended, span);
    let gone = match judged.is_empty() {
        true => Vec::new(),
        false => {
            let worker_ids: Vec<String> =
                judged.iter().map(|owner| owner.worker_id.clone()).collect();
            let current = read_liveness_generations(pool, log, &worker_ids).await?;
            memory.confirm(judged, &current)
        }
    };
    let mut step = OwnerGoneStep {
        gone_owners: u64::try_from(gone.len()).unwrap_or(u64::MAX),
        running_recovered: 0,
        claims_requeued: 0,
    };
    for owner in &gone {
        tracing::warn!(
            target: STALE_RUNNING_PASS,
            worker_id = %owner.worker_id,
            generation = %owner.generation,
            unheld_since = %owner.unheld_since,
            "worker {} is gone: its liveness lock is free since {}, at generation {}",
            owner.worker_id,
            owner.unheld_since,
            owner.generation,
        );
        if config.fail_stale_running {
            step.running_recovered += recover_gone_owner_running(pool, log, owner, telemetry).await;
        }
        if config.requeue_stale_claimed {
            step.claims_requeued += requeue_gone_owner_claims(pool, log, owner, telemetry).await;
        }
    }
    Ok(step)
}

/// Delete one batch of liveness rows of ended workers on `pool`
/// (`horsies_forget_ended_worker_liveness`): rows of workers that own no
/// CLAIMED or RUNNING row and whose liveness lock no session holds, at most
/// [`LIVENESS_FORGET_BATCH`]. The statement runs alone as reserved work and
/// commits alone; a full batch leaves the rest to the next sweep.
///
/// Returns the number of rows deleted.
///
/// Errors: the statement's [`BrokerError`]; nothing was deleted, or the
/// outcome is unknown.
pub(crate) async fn forget_ended_workers(
    pool: &CorePool,
    log: StatementLog,
) -> Result<u32, BrokerError> {
    forget_ended_worker_liveness(pool, log, LIVENESS_FORGET_BATCH).await
}

/// Recover the RUNNING rows of a gone owner: at most one scan batch per
/// step; the rest is read again at the next pass, where the owner is still
/// gone. Returns the rows whose crash retry or failure committed, each
/// told to `telemetry`.
async fn recover_gone_owner_running(
    pool: &CorePool,
    log: StatementLog,
    owner: &GoneOwner,
    telemetry: &WorkerTelemetry,
) -> u64 {
    let candidates = match scan_gone_owner_running(pool, log, owner, STALE_RUNNING_SCAN_BATCH).await
    {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::error!(
                target: STALE_RUNNING_PASS,
                worker_id = %owner.worker_id,
                %error,
                "Reaper failed to read the RUNNING tasks of gone worker {}: {error}",
                owner.worker_id,
            );
            return 0;
        }
    };
    let mut recovered: u64 = 0;
    for candidate in &candidates {
        let attempt = candidate_log(&candidate.facts);
        let command = match gone_owner_attempt_command(candidate, owner) {
            Ok(command) => command,
            Err(error) => {
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.facts.task_id,
                    attempt = attempt.attempt.get(),
                    trace_id = attempt.trace_id.log_field(),
                    %error,
                    "Failed to serialize crash result for task {}: {error}",
                    candidate.facts.task_id,
                );
                continue;
            }
        };
        match finish_stale_attempt(pool, log, &command).await {
            Ok(outcome) => match after_stale_attempt(pool, log, &command, attempt, outcome).await {
                StaleRecovery::Recovered(action) => {
                    recovered += 1;
                    let facts = &candidate.facts;
                    telemetry.task_recovered(
                        facts.task_id,
                        &facts.task_name,
                        &facts.queue_name,
                        action,
                    );
                }
                StaleRecovery::NotRecovered => {}
            },
            Err(error) => {
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.facts.task_id,
                    attempt = attempt.attempt.get(),
                    trace_id = attempt.trace_id.log_field(),
                    worker_id = %owner.worker_id,
                    %error,
                    "Reaper failed to recover task {} of gone worker {}: {error}",
                    candidate.facts.task_id,
                    owner.worker_id,
                );
            }
        }
    }
    recovered
}

/// Requeue the CLAIMED rows of a gone owner; returns the rows requeued,
/// with those of the batches before a failure. Each row of each committed
/// batch is told to `telemetry` as `ClaimRequeued`.
async fn requeue_gone_owner_claims(
    pool: &CorePool,
    log: StatementLog,
    owner: &GoneOwner,
    telemetry: &WorkerTelemetry,
) -> u64 {
    let told = |requeued: &[RecoveredTask]| {
        telemetry.tasks_recovered(requeued, RecoveryAction::ClaimRequeued);
    };
    match requeue_gone_owner_pass(pool, log, owner, told).await {
        Ok(StaleClaimsRequeuePass::Drained { requeued }) => requeued,
        Ok(StaleClaimsRequeuePass::BoundReached { requeued }) => {
            tracing::warn!(
                worker_id = %owner.worker_id,
                requeued,
                batches = STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS,
                "the requeue of a gone worker's claims reached its per-pass bound; the next pass \
                 goes on",
            );
            requeued
        }
        Err(failed) => {
            tracing::error!(
                worker_id = %owner.worker_id,
                error = %failed,
                "Reaper failed to requeue the claims of gone worker {}: {failed}",
                owner.worker_id,
            );
            failed.requeued_before
        }
    }
}

/// The statement for one RUNNING row of a gone owner: the evidence
/// `OWNER_GONE` with the row's claim generation, the retry that the task's
/// policy admits for `WORKER_CRASHED`, and the crash result and reason.
///
/// Errors: the crash result does not serialize.
pub(crate) fn gone_owner_attempt_command(
    candidate: &GoneOwnerCandidate,
    owner: &GoneOwner,
) -> Result<FinishStaleAttempt, serde_json::Error> {
    let facts = &candidate.facts;
    let error_code = OperationalErrorCode::WorkerCrashed.to_string();
    let unheld_since = python_isoformat_utc(owner.unheld_since);
    let failed_reason =
        format!("Worker process crashed (its liveness lock is free since {unheld_since})");
    let task_error = TaskError {
        exception: None,
        error_code: OperationalErrorCode::WorkerCrashed.into(),
        message: Some(failed_reason.clone()),
        kind: TaskErrorKind::Plain,
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "liveness_lock_free_since": unheld_since,
                "worker_pid": facts.worker_pid,
                "worker_hostname": facts.worker_hostname,
                "worker_id": facts.claimed_by_worker_id,
                "started_at": python_isoformat_utc(facts.started_at),
                "detected_at": python_isoformat_utc(facts.db_now),
            }),
        )),
    };
    let result_json = task_result_to_json_string(&TaskResult::<()>::Err(task_error))?;
    Ok(FinishStaleAttempt {
        task_id: facts.task_id,
        evidence: StaleEvidence::OwnerGone(OwnerClaim {
            worker_id: owner.worker_id.clone(),
            claimed_at: candidate.claimed_at,
        }),
        retry: stale_retry(facts, &error_code),
        result_json,
        error_code,
        failed_reason: failed_reason.clone(),
        attempt_message: failed_reason.clone(),
        attempt_reason: failed_reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::stale_recovery::{StaleCandidate, StaleRetry, UnheldOwner};
    use chrono::TimeZone;
    use uuid::Uuid;

    const SPAN: Duration = Duration::from_secs(75);

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + seconds, 0)
            .single()
            .expect("a valid test time")
    }

    fn recorded(generation: i64) -> OwnerGeneration {
        OwnerGeneration::Recorded(LivenessGeneration(generation))
    }

    /// A read at database time `seconds` that returns `owners`, each with
    /// its generation.
    fn read(
        owners: &[(&str, OwnerGeneration)],
        seconds: i64,
        reach: OwnerListReach,
    ) -> UnheldOwnerRead {
        UnheldOwnerRead {
            owners: owners
                .iter()
                .map(|(worker_id, generation)| UnheldOwner {
                    worker_id: (*worker_id).to_owned(),
                    generation: *generation,
                    read_at: at(seconds),
                })
                .collect(),
            reach,
        }
    }

    fn judged_ids(judged: &[JudgedOwner]) -> Vec<&str> {
        judged
            .iter()
            .map(|owner| owner.worker_id.as_str())
            .collect()
    }

    /// The first read of an owner judges nothing; a read at least the span
    /// after the end of the first one, at the same generation, judges it,
    /// with the database time of the first read as its claim bound.
    #[test]
    fn an_owner_unheld_in_two_reads_a_span_apart_at_one_generation_is_judged() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();

        let first = memory.record(
            &read(&[("w-a", recorded(4))], 0, OwnerListReach::Complete),
            start,
            start + Duration::from_millis(5),
            SPAN,
        );
        let early = start + SPAN;
        let too_early = memory.record(
            &read(&[("w-a", recorded(4))], 75, OwnerListReach::Complete),
            early,
            early,
            SPAN,
        );
        let late = start + Duration::from_millis(5) + SPAN;
        let judged = memory.record(
            &read(&[("w-a", recorded(4))], 76, OwnerListReach::Complete),
            late,
            late,
            SPAN,
        );

        assert!(first.is_empty());
        assert!(
            too_early.is_empty(),
            "the span counts from the end of the first read"
        );
        assert_eq!(
            judged,
            vec![JudgedOwner {
                worker_id: "w-a".to_owned(),
                generation: LivenessGeneration(4),
                unheld_since: at(0),
            }]
        );
    }

    /// The sequence of #1923: the first read finds the lock free (loss 1),
    /// the worker takes it again with a new generation and loses it again
    /// (loss 2), and a read a span later finds it free. The later read
    /// returns the new generation, so it judges nothing and starts a new
    /// run; the owner is judged only a span after that read.
    #[test]
    fn a_new_generation_between_two_reads_starts_the_run_again() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();

        let _loss_one = memory.record(
            &read(&[("w-a", recorded(4))], 0, OwnerListReach::Complete),
            start,
            start,
            SPAN,
        );
        let loss_two_at = start + SPAN;
        let loss_two = memory.record(
            &read(&[("w-a", recorded(9))], 75, OwnerListReach::Complete),
            loss_two_at,
            loss_two_at,
            SPAN,
        );
        let later = loss_two_at + SPAN;
        let judged = memory.record(
            &read(&[("w-a", recorded(9))], 150, OwnerListReach::Complete),
            later,
            later,
            SPAN,
        );

        assert!(
            loss_two.is_empty(),
            "a regain between the two reads changed the generation"
        );
        assert_eq!(
            judged,
            vec![JudgedOwner {
                worker_id: "w-a".to_owned(),
                generation: LivenessGeneration(9),
                unheld_since: at(75),
            }]
        );
    }

    /// An owner with no liveness row has no generation to judge by: no run
    /// starts, and a run whose owner lost its row ends.
    #[test]
    fn an_owner_with_no_recorded_generation_is_never_judged() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();

        let _first = memory.record(
            &read(
                &[("w-a", OwnerGeneration::NotRecorded), ("w-b", recorded(2))],
                0,
                OwnerListReach::Complete,
            ),
            start,
            start,
            SPAN,
        );
        let late = start + SPAN;
        let judged = memory.record(
            &read(
                &[
                    ("w-a", OwnerGeneration::NotRecorded),
                    ("w-b", OwnerGeneration::NotRecorded),
                ],
                75,
                OwnerListReach::Complete,
            ),
            late,
            late,
            SPAN,
        );

        assert!(judged.is_empty(), "{judged:?}");
        assert!(memory.remembered().is_empty());
    }

    /// A read that finds the lock held again (the owner is not in a complete
    /// list) ends the run: the next unheld read starts a new one, even at
    /// the same generation.
    #[test]
    fn a_read_that_finds_the_lock_held_starts_the_run_again() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();

        let _first = memory.record(
            &read(&[("w-a", recorded(4))], 0, OwnerListReach::Complete),
            start,
            start,
            SPAN,
        );
        let held = start + Duration::from_secs(30);
        let _held = memory.record(&read(&[], 30, OwnerListReach::Complete), held, held, SPAN);
        let again = start + Duration::from_secs(60);
        let _again = memory.record(
            &read(&[("w-a", recorded(4))], 60, OwnerListReach::Complete),
            again,
            again,
            SPAN,
        );
        let late = start + SPAN + Duration::from_secs(10);
        let judged = memory.record(
            &read(&[("w-a", recorded(4))], 85, OwnerListReach::Complete),
            late,
            late,
            SPAN,
        );

        assert!(
            judged.is_empty(),
            "the new run started 60 s after the first read"
        );
        let later = again + SPAN;
        let judged = memory.record(
            &read(&[("w-a", recorded(4))], 135, OwnerListReach::Complete),
            later,
            later,
            SPAN,
        );
        assert_eq!(judged_ids(&judged), vec!["w-a"]);
        assert_eq!(judged[0].unheld_since, at(60));
    }

    /// A cut read covers the owners up to its last one: an owner before the
    /// cut that it did not return ends its run, an owner past the cut keeps
    /// it.
    #[test]
    fn a_cut_read_ends_only_the_runs_that_it_covered() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();
        let _first = memory.record(
            &read(
                &[
                    ("w-a", recorded(1)),
                    ("w-b", recorded(2)),
                    ("w-d", recorded(3)),
                ],
                0,
                OwnerListReach::Complete,
            ),
            start,
            start,
            SPAN,
        );

        let next = start + Duration::from_secs(30);
        let _cut = memory.record(
            &read(
                &[("w-a", recorded(1)), ("w-c", recorded(5))],
                30,
                OwnerListReach::Cut,
            ),
            next,
            next,
            SPAN,
        );

        assert_eq!(memory.remembered(), vec!["w-a", "w-c", "w-d"]);
    }

    /// The read of the generations after the judging read keeps only the
    /// owners still at their judged generation: an owner that took its lock
    /// again since, or whose row was deleted, is not gone, and its run ends.
    #[test]
    fn only_an_owner_still_at_its_judged_generation_is_gone() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();
        let owners = [
            ("w-a", recorded(1)),
            ("w-b", recorded(2)),
            ("w-c", recorded(3)),
        ];
        let _first = memory.record(
            &read(&owners, 0, OwnerListReach::Complete),
            start,
            start,
            SPAN,
        );
        let late = start + SPAN;
        let judged = memory.record(
            &read(&owners, 75, OwnerListReach::Complete),
            late,
            late,
            SPAN,
        );
        let current = BTreeMap::from([
            ("w-a".to_owned(), LivenessGeneration(1)),
            ("w-b".to_owned(), LivenessGeneration(7)),
        ]);

        let gone = memory.confirm(judged, &current);

        assert_eq!(
            gone,
            vec![GoneOwner {
                worker_id: "w-a".to_owned(),
                generation: LivenessGeneration(1),
                unheld_since: at(0),
            }]
        );
        assert_eq!(memory.remembered(), vec!["w-a"]);
    }

    /// The command for a RUNNING row of a gone owner fences on the owner and
    /// the claim generation that the scan read, carries the crash code, and
    /// names the time the owner's lock was first read free.
    #[test]
    fn the_command_of_a_gone_owner_fences_on_its_claim_and_names_the_free_lock() {
        let claimed_at = at(-10);
        let candidate = GoneOwnerCandidate {
            facts: StaleCandidate {
                task_id: Uuid::from_u128(7),
                task_name: "gone.task".to_owned(),
                queue_name: "default".to_owned(),
                trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
                retry_count: 0,
                max_retries: 0,
                task_options: None,
                good_until: None,
                claimed_by_worker_id: Some("w-a".to_owned()),
                worker_pid: Some(41),
                worker_hostname: Some("host-a".to_owned()),
                worker_process_name: Some("worker".to_owned()),
                started_at: at(-5),
                db_now: at(80),
            },
            claimed_at,
        };
        let owner = GoneOwner {
            worker_id: "w-a".to_owned(),
            generation: LivenessGeneration(3),
            unheld_since: at(0),
        };

        let command =
            gone_owner_attempt_command(&candidate, &owner).expect("the crash result encodes");

        let reason = format!(
            "Worker process crashed (its liveness lock is free since {})",
            python_isoformat_utc(at(0))
        );
        assert_eq!(command.task_id, Uuid::from_u128(7));
        assert_eq!(
            command.evidence,
            StaleEvidence::OwnerGone(OwnerClaim {
                worker_id: "w-a".to_owned(),
                claimed_at,
            })
        );
        assert_eq!(command.retry, StaleRetry::NoRetry);
        assert_eq!(command.error_code, "WORKER_CRASHED");
        assert_eq!(command.failed_reason, reason);
        assert_eq!(command.attempt_message, reason);
        assert!(
            command.result_json.contains("liveness_lock_free_since"),
            "{}",
            command.result_json
        );
    }
}
