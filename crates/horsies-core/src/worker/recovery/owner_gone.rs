//! The owner-gone step of the reaper: the liveness read of each pass, the
//! memory of when each worker was first read without its liveness lock, and
//! the recovery of the RUNNING and CLAIMED rows of a worker judged gone.
//!
//! - Each pass reads the owners of CLAIMED and RUNNING rows whose liveness
//!   lock no session holds (`horsies_unheld_worker_owners`). The read takes
//!   no lock.
//! - An owner is gone when two reads at least the gone span apart return it
//!   ([`crate::worker::liveness::OWNER_GONE_SPAN`]: the worker's regain bound
//!   plus a margin), and no read of this reaper between them found its lock
//!   held. A live worker takes its lock again within its regain bound, or it
//!   fails closed before the margin ends, so the two reads cannot fall in one
//!   loss of a live worker. Two losses with a regain between two consecutive
//!   reads of this reaper are not told apart from one loss (#1923).
//! - With stale running recovery on, each RUNNING row of a gone owner goes
//!   through `horsies_finish_stale_attempt` with the evidence `OWNER_GONE`;
//!   with the stale claim requeue on, its CLAIMED rows go through
//!   `horsies_requeue_stale_claims` with the same evidence. Each statement
//!   judges again under the row lock that the row still belongs to the
//!   owner: at the claim generation that the scan read (RUNNING), or claimed
//!   before the owner was first read unheld (CLAIMED).
//! - The memory belongs to one reaper loop, in its process.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use super::{after_stale_attempt, stale_retry, StaleRecovery};
use crate::broker::core_rows::StatementLog;
use crate::broker::stale_recovery::{
    finish_stale_attempt, read_unheld_owners, requeue_gone_owner_pass, scan_gone_owner_running,
    FinishStaleAttempt, GoneOwner, GoneOwnerCandidate, OwnerClaim, OwnerListReach,
    StaleClaimsRequeuePass, StaleEvidence, UnheldOwnerRead,
    STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS, STALE_RUNNING_SCAN_BATCH, UNHELD_OWNER_BATCH,
};
use crate::broker::BrokerError;
use crate::core::codec::task_result_to_json_string;
use crate::core::config::recovery::RecoveryConfig;
use crate::core::python_text::python_isoformat_utc;
use crate::core::{OperationalErrorCode, TaskError, TaskErrorKind, TaskResult};
use crate::db::CorePool;
use crate::log_targets::STALE_RUNNING_PASS;

/// Owners whose first unheld read one reaper keeps at most. A read that
/// would pass it clears the memory first: a judgement then only comes later,
/// never earlier.
const MAX_REMEMBERED_OWNERS: usize = 10_000;

/// The first read of a run of reads that found an owner's lock free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FirstUnheldRead {
    /// The end of the read: the latest instant at which it can have found
    /// the lock free.
    ended: Instant,
    /// The database time of the read. Only rows that the owner claimed
    /// before it are recovered by the owner's evidence.
    read_at: DateTime<Utc>,
}

/// When this reaper first read each owner's liveness lock free, in the run
/// of its reads that found it free.
#[derive(Debug, Default)]
pub(crate) struct UnheldOwners {
    first_read: BTreeMap<String, FirstUnheldRead>,
}

impl UnheldOwners {
    /// Record one liveness read that started at `started` and ended at
    /// `ended`, and return the owners it judges gone: each owner of the read
    /// whose first unheld read ended at least `span` before `started`.
    ///
    /// An owner that the read covered and did not return was held at the
    /// read (or owns no row any more): its run ends. A cut read covers the
    /// owners up to its last one, in worker id order (byte order, as the
    /// read's C collation), so the run of an owner past the cut goes on.
    pub(crate) fn record(
        &mut self,
        read: &UnheldOwnerRead,
        started: Instant,
        ended: Instant,
        span: Duration,
    ) -> Vec<GoneOwner> {
        let unheld: BTreeSet<&str> = read
            .owners
            .iter()
            .map(|owner| owner.worker_id.as_str())
            .collect();
        let covered_through: Option<&str> = match read.reach {
            OwnerListReach::Complete => None,
            OwnerListReach::Cut => read.owners.last().map(|owner| owner.worker_id.as_str()),
        };
        self.first_read.retain(|worker_id, _| {
            unheld.contains(worker_id.as_str())
                || covered_through.is_some_and(|last| worker_id.as_str() > last)
        });
        if self.first_read.len().saturating_add(read.owners.len()) > MAX_REMEMBERED_OWNERS {
            self.first_read.clear();
        }
        let mut gone = Vec::new();
        for owner in &read.owners {
            let first =
                *self
                    .first_read
                    .entry(owner.worker_id.clone())
                    .or_insert(FirstUnheldRead {
                        ended,
                        read_at: owner.read_at,
                    });
            if started.saturating_duration_since(first.ended) >= span {
                gone.push(GoneOwner {
                    worker_id: owner.worker_id.clone(),
                    unheld_since: first.read_at,
                });
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
/// `memory` with `span`, then for each gone owner the recovery of its
/// RUNNING rows (with `auto_fail_stale_running`) and of its CLAIMED rows
/// (with `auto_requeue_stale_claimed`). Each statement runs alone on `pool`
/// as reserved work and commits alone. A recovery statement that fails is
/// logged, and the step goes on with the next one.
///
/// Errors: the liveness read's [`BrokerError`]; the memory is unchanged and
/// nothing was written.
pub(crate) async fn owner_gone_step(
    pool: &CorePool,
    log: StatementLog,
    config: &RecoveryConfig,
    memory: &mut UnheldOwners,
    span: Duration,
) -> Result<OwnerGoneStep, BrokerError> {
    let started = Instant::now();
    let read = read_unheld_owners(pool, log, UNHELD_OWNER_BATCH).await?;
    let ended = Instant::now();
    let gone = memory.record(&read, started, ended, span);
    let mut step = OwnerGoneStep {
        gone_owners: u64::try_from(gone.len()).unwrap_or(u64::MAX),
        running_recovered: 0,
        claims_requeued: 0,
    };
    for owner in &gone {
        tracing::warn!(
            target: STALE_RUNNING_PASS,
            worker_id = %owner.worker_id,
            unheld_since = %owner.unheld_since,
            "worker {} is gone: its liveness lock is free since {}",
            owner.worker_id,
            owner.unheld_since,
        );
        if config.auto_fail_stale_running {
            step.running_recovered += recover_gone_owner_running(pool, log, owner).await;
        }
        if config.auto_requeue_stale_claimed {
            step.claims_requeued += requeue_gone_owner_claims(pool, log, owner).await;
        }
    }
    Ok(step)
}

/// Recover the RUNNING rows of a gone owner: at most one scan batch per
/// step; the rest is read again at the next pass, where the owner is still
/// gone. Returns the rows whose crash retry or failure committed.
async fn recover_gone_owner_running(pool: &CorePool, log: StatementLog, owner: &GoneOwner) -> u64 {
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
        let command = match gone_owner_attempt_command(candidate, owner) {
            Ok(command) => command,
            Err(error) => {
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.facts.task_id,
                    %error,
                    "Failed to serialize crash result for task {}: {error}",
                    candidate.facts.task_id,
                );
                continue;
            }
        };
        match finish_stale_attempt(pool, log, &command).await {
            Ok(outcome) => match after_stale_attempt(pool, log, &command, outcome).await {
                StaleRecovery::Recovered => recovered += 1,
                StaleRecovery::NotRecovered => {}
            },
            Err(error) => {
                tracing::error!(
                    target: STALE_RUNNING_PASS,
                    task_id = %candidate.facts.task_id,
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
/// with those of the batches before a failure.
async fn requeue_gone_owner_claims(pool: &CorePool, log: StatementLog, owner: &GoneOwner) -> u64 {
    match requeue_gone_owner_pass(pool, log, owner).await {
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

    fn read(owners: &[&str], seconds: i64, reach: OwnerListReach) -> UnheldOwnerRead {
        UnheldOwnerRead {
            owners: owners
                .iter()
                .map(|worker_id| UnheldOwner {
                    worker_id: (*worker_id).to_owned(),
                    read_at: at(seconds),
                })
                .collect(),
            reach,
        }
    }

    fn gone_ids(gone: &[GoneOwner]) -> Vec<&str> {
        gone.iter().map(|owner| owner.worker_id.as_str()).collect()
    }

    /// The first read of an owner judges nothing; a read at least the span
    /// after the end of the first one judges it gone, with the database time
    /// of the first read as its claim bound.
    #[test]
    fn an_owner_unheld_in_two_reads_a_span_apart_is_gone() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();

        let first = memory.record(
            &read(&["w-a"], 0, OwnerListReach::Complete),
            start,
            start + Duration::from_millis(5),
            SPAN,
        );
        let early = start + SPAN;
        let too_early = memory.record(
            &read(&["w-a"], 75, OwnerListReach::Complete),
            early,
            early,
            SPAN,
        );
        let late = start + Duration::from_millis(5) + SPAN;
        let judged = memory.record(
            &read(&["w-a"], 76, OwnerListReach::Complete),
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
            vec![GoneOwner {
                worker_id: "w-a".to_owned(),
                unheld_since: at(0),
            }]
        );
    }

    /// A read that finds the lock held again (the owner is not in a complete
    /// list) ends the run: the next unheld read starts a new one.
    #[test]
    fn a_read_that_finds_the_lock_held_starts_the_run_again() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();

        let _first = memory.record(
            &read(&["w-a"], 0, OwnerListReach::Complete),
            start,
            start,
            SPAN,
        );
        let held = start + Duration::from_secs(30);
        let _held = memory.record(&read(&[], 30, OwnerListReach::Complete), held, held, SPAN);
        let again = start + Duration::from_secs(60);
        let _again = memory.record(
            &read(&["w-a"], 60, OwnerListReach::Complete),
            again,
            again,
            SPAN,
        );
        let late = start + SPAN + Duration::from_secs(10);
        let judged = memory.record(
            &read(&["w-a"], 85, OwnerListReach::Complete),
            late,
            late,
            SPAN,
        );

        assert!(
            judged.is_empty(),
            "the new run started 60 s after the first read"
        );
        let later = again + SPAN;
        let gone = memory.record(
            &read(&["w-a"], 135, OwnerListReach::Complete),
            later,
            later,
            SPAN,
        );
        assert_eq!(gone_ids(&gone), vec!["w-a"]);
        assert_eq!(gone[0].unheld_since, at(60));
    }

    /// A cut read covers the owners up to its last one: an owner before the
    /// cut that it did not return ends its run, an owner past the cut keeps
    /// it.
    #[test]
    fn a_cut_read_ends_only_the_runs_that_it_covered() {
        let mut memory = UnheldOwners::default();
        let start = Instant::now();
        let _first = memory.record(
            &read(&["w-a", "w-b", "w-d"], 0, OwnerListReach::Complete),
            start,
            start,
            SPAN,
        );

        let next = start + Duration::from_secs(30);
        let _cut = memory.record(
            &read(&["w-a", "w-c"], 30, OwnerListReach::Cut),
            next,
            next,
            SPAN,
        );

        assert_eq!(memory.remembered(), vec!["w-a", "w-c", "w-d"]);
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
