//! The writer of FAIL_STALE, `horsies_finish_stale_attempt` (Python
//! `TestFailStale`).

use super::*;
use crate::core::lifecycle::outcomes::ObservedClaim;
use crate::core::lifecycle::{GuardEvidence, TerminalizationKind};
use crate::core::types::status::TaskStatus;

/// `value` milliseconds as a span of time.
fn milliseconds(value: i32) -> chrono::Duration {
    chrono::Duration::milliseconds(i64::from(value))
}

/// `value` seconds as a span of time.
fn seconds(value: i32) -> chrono::Duration {
    chrono::Duration::seconds(i64::from(value))
}

/// No heartbeat at all: staleness is judged from started_at.
#[tokio::test]
async fn a_silent_runner_is_failed() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let now = database_now(&session).await;
    age(
        &session,
        task_id,
        Some(now - seconds(STALE_AFTER_SECONDS * 2)),
        None,
    )
    .await;

    match fail_stale(&session, task_id).await {
        TerminalizationOutcome::Applied { kind, observed, .. } => {
            assert_eq!(kind, TerminalizationKind::FailStale);
            assert_eq!(observed.worker_id.as_deref(), Some(WORKER));
        }
        other => panic!("expected Applied: {other:?}"),
    }
}

#[tokio::test]
async fn a_stale_heartbeat_is_failed() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let now = database_now(&session).await;
    age(
        &session,
        task_id,
        Some(now - seconds(STALE_AFTER_SECONDS * 4)),
        None,
    )
    .await;
    let _leaf = heartbeat(
        &session,
        task_id,
        now - seconds(STALE_AFTER_SECONDS * 2),
        "runner",
    )
    .await;

    let outcome = fail_stale(&session, task_id).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::Applied { .. }),
        "{outcome:?}"
    );
}

/// [`a_stale_heartbeat_is_failed`] seeded 30 s after the start of the first
/// hour that the coverage pass covers: the heartbeat 120 s before that
/// instant is in the hour before, which no leaf holds until the heartbeat
/// helper creates it. Both seeded times are before the start of that hour,
/// so both are in the past at the evaluation, whatever the minute of the
/// run.
#[tokio::test]
async fn a_stale_heartbeat_seeded_at_thirty_seconds_past_the_hour_is_failed() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let seeded_at = first_heartbeat_hour(&session).await + seconds(30);
    age(
        &session,
        task_id,
        Some(seeded_at - seconds(STALE_AFTER_SECONDS * 4)),
        None,
    )
    .await;
    let leaf = heartbeat(
        &session,
        task_id,
        seeded_at - seconds(STALE_AFTER_SECONDS * 2),
        "runner",
    )
    .await;
    assert_eq!(
        leaf,
        HeartbeatLeaf::Created,
        "the heartbeat is in the hour before the first covered hour"
    );

    let outcome = fail_stale(&session, task_id).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::Applied { .. }),
        "{outcome:?}"
    );
}

/// The refusal carries what the guard judged, not just that it refused.
#[tokio::test]
async fn a_recent_heartbeat_refuses_with_the_evidence() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let now = database_now(&session).await;
    age(
        &session,
        task_id,
        Some(now - seconds(STALE_AFTER_SECONDS * 2)),
        None,
    )
    .await;
    let _leaf = heartbeat(&session, task_id, now - seconds(1), "runner").await;

    match fail_stale(&session, task_id).await {
        TerminalizationOutcome::SourceStateConflict {
            observed,
            evidence: GuardEvidence::Staleness(evidence),
            ..
        } => {
            assert_eq!(observed.status, Some(TaskStatus::Running));
            let last_heartbeat_at = evidence
                .last_heartbeat_at
                .expect("the evidence carries the heartbeat");
            assert!(evidence.started_at.is_some());
            assert_eq!(evidence.finalizing_at, None);
            assert_eq!(evidence.stale_after_ms, i64::from(STALE_AFTER_MS));
            assert_eq!(
                evidence.finalizing_stale_after_ms,
                i64::from(FINALIZING_STALE_AFTER_MS)
            );
            // The refusal is reconstructible from its own evidence: the
            // heartbeat is inside the freshness window that the guard judged
            // it against.
            assert!(last_heartbeat_at >= evidence.evaluated_at - milliseconds(STALE_AFTER_MS));
        }
        other => panic!("expected a conflict with staleness evidence: {other:?}"),
    }
}

/// The millisecond thresholds reach the SQL guard unchanged. The start, the
/// heartbeat and the call run in one transaction, so `NOW()` is one instant
/// for the seed and for `evaluated_at`, and the heartbeat is exactly 1250 ms
/// before the evaluation.
#[tokio::test]
async fn fractional_second_threshold_is_not_truncated() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;

    session.run_script("BEGIN", "BEGIN").await;
    let now = database_now(&session).await;
    age(&session, task_id, Some(now - seconds(10)), None).await;
    let _leaf = heartbeat(&session, task_id, now - milliseconds(1_250), "runner").await;
    let outcome = fail_stale_after(&session, task_id, 1_500, 2_750).await;
    session.run_script("COMMIT", "COMMIT").await;

    match outcome {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Staleness(evidence),
            ..
        } => {
            assert_eq!(evidence.stale_after_ms, 1_500);
            assert_eq!(evidence.finalizing_stale_after_ms, 2_750);
            let last_heartbeat_at = evidence
                .last_heartbeat_at
                .expect("the evidence carries the heartbeat");
            assert_eq!(
                last_heartbeat_at,
                evidence.evaluated_at - milliseconds(1_250)
            );
        }
        other => panic!("expected a conflict with staleness evidence: {other:?}"),
    }
}

/// Only the heartbeats of the runner prove that the runner is alive.
#[tokio::test]
async fn a_heartbeat_from_another_role_does_not_count() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let now = database_now(&session).await;
    age(
        &session,
        task_id,
        Some(now - seconds(STALE_AFTER_SECONDS * 2)),
        None,
    )
    .await;
    let _leaf = heartbeat(&session, task_id, now - seconds(1), "claimer").await;

    let outcome = fail_stale(&session, task_id).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::Applied { .. }),
        "{outcome:?}"
    );
}

/// RUNNING with no started_at cannot be judged stale. The evidence says
/// why: a NULL started_at, so the guard had no instant to measure silence
/// from. The decoder requires `evaluated_at`, so the decode asserts it.
#[tokio::test]
async fn a_row_that_never_started_refuses() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    age(&session, task_id, None, None).await;

    match fail_stale(&session, task_id).await {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Staleness(evidence),
            ..
        } => {
            assert_eq!(evidence.started_at, None);
            assert_eq!(evidence.last_heartbeat_at, None);
        }
        other => panic!("expected a conflict with staleness evidence: {other:?}"),
    }
}

/// A finalizer inside its window holds the row against the stale failure.
/// The evidence explains this refusal on its own: the runner is silent past
/// its threshold, so only the arm of the finalizer can have refused, and
/// the finalizing instant is inside the window it was judged against.
#[tokio::test]
async fn an_active_finalizer_defers() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let now = database_now(&session).await;
    age(
        &session,
        task_id,
        Some(now - seconds(STALE_AFTER_SECONDS * 2)),
        Some(now - seconds(1)),
    )
    .await;

    match fail_stale(&session, task_id).await {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::Staleness(evidence),
            ..
        } => {
            let finalizing_at = evidence
                .finalizing_at
                .expect("the evidence carries the finalizing instant");
            assert!(
                finalizing_at >= evidence.evaluated_at - milliseconds(FINALIZING_STALE_AFTER_MS)
            );
            let started_at = evidence.started_at.expect("the evidence carries the start");
            assert!(started_at < evidence.evaluated_at - milliseconds(STALE_AFTER_MS));
            assert_eq!(evidence.last_heartbeat_at, None);
        }
        other => panic!("expected a conflict with staleness evidence: {other:?}"),
    }
}

#[tokio::test]
async fn a_stale_finalizer_does_not_defer() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let now = database_now(&session).await;
    age(
        &session,
        task_id,
        Some(now - seconds(STALE_AFTER_SECONDS * 2)),
        Some(now - seconds(FINALIZING_STALE_AFTER_SECONDS * 2)),
    )
    .await;

    let outcome = fail_stale(&session, task_id).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::Applied { .. }),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn replay_is_already_applied() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let now = database_now(&session).await;
    age(
        &session,
        task_id,
        Some(now - seconds(STALE_AFTER_SECONDS * 2)),
        None,
    )
    .await;
    let _first = fail_stale(&session, task_id).await;

    match fail_stale(&session, task_id).await {
        TerminalizationOutcome::AlreadyApplied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::FailStale);
        }
        other => panic!("expected AlreadyApplied: {other:?}"),
    }
}

#[tokio::test]
async fn a_locked_failure_is_a_conflict_not_a_replay() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    force_terminal(&session, task_id, "FAILED", "FAIL_RUNNING").await;

    match fail_stale(&session, task_id).await {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(evidence),
            ..
        } => {
            assert_eq!(evidence.committed_kind, TerminalizationKind::FailRunning);
        }
        other => panic!("expected a conflict with a foreign terminalization: {other:?}"),
    }
}

/// Not RUNNING, so no staleness was judged and no staleness evidence is
/// claimed.
#[tokio::test]
async fn the_source_status_is_wrong() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;

    match fail_stale(&session, task_id).await {
        TerminalizationOutcome::SourceStateConflict {
            observed, evidence, ..
        } => {
            assert_eq!(observed.status, Some(TaskStatus::Claimed));
            assert_eq!(
                evidence,
                GuardEvidence::Claim(ObservedClaim {
                    worker_id: Some(WORKER.to_owned()),
                    claimed_at: Some(generation()),
                })
            );
        }
        other => panic!("expected SourceStateConflict: {other:?}"),
    }
}

#[tokio::test]
async fn a_task_that_does_not_exist() {
    let session = covered().await;
    let outcome = fail_stale(&session, Uuid::new_v4()).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::TaskAbsent { .. }),
        "{outcome:?}"
    );
}
