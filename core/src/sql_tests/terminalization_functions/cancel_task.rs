//! The writer of CANCEL_ADMIN, `horsies_cancel_task` (Python
//! `TestCancelTask`), the attempt that it records for a RUNNING task and the
//! cancel notice to the worker (migration 1003), and the read of the
//! cancelled runs of a worker, `horsies_cancelled_runs`.

use std::time::Duration;

use super::*;
use crate::core::history::archive::attempts::{
    decode_attempt_archive, AttemptArchive, ClaimRecord, Finalizing, OpenAttempt, PhaseRecord,
    RecordedBy,
};
use crate::core::lifecycle::{GuardEvidence, TerminalizationKind};
use crate::core::types::status::TaskStatus;

/// The channel of the cancel notice of a RUNNING task.
const CANCEL_CHANNEL: &str = "horsies_task_cancel";

/// How long a case waits for the notifications of a committed statement.
const NOTIFY_WAIT: Duration = Duration::from_millis(500);

/// The most pairs that one call of `horsies_cancelled_runs` takes.
const CANCELLED_RUNS_MAX_PAIRS: usize = 1000;

/// The read of the cancelled runs of a worker. Parameters: `$1` task ids,
/// `$2` claim generations.
const CANCELLED_RUNS_SQL: &str = "
    SELECT task_id, claimed_at
    FROM horsies_cancelled_runs(CAST($1 AS uuid[]), CAST($2 AS timestamptz[]))
";

/// The due time and the start of the live row. Parameter `$1` is the task
/// id.
const DUE_AND_START_SQL: &str = "SELECT enqueued_at, started_at FROM horsies_tasks WHERE id = $1";

/// The claim count, the first claim and the finalizing marker of the live
/// row. Parameters: `$1` task id, `$2` count, `$3` first claim, `$4`
/// marker.
const CLAIMS_AND_MARKER_SQL: &str = "
    UPDATE horsies_tasks
    SET attempt_claims = $2, first_claimed_at = $3, finalizing_at = CAST($4 AS timestamptz),
        finalizing_by_worker_id =
            CASE WHEN CAST($4 AS timestamptz) IS NULL THEN NULL ELSE claimed_by_worker_id END
    WHERE id = $1
";

/// An attempt row of a task with the outcome `$2`, `will_retry` `$3`, and
/// a phase record of the writer `$4` with `placed_at` `$5` and
/// `last_seen_at` `$6`. Parameter `$1` is the task id.
const RAW_ATTEMPT_SQL: &str = "
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message,
        failed_reason, worker_id, worker_hostname, worker_pid, worker_process_name,
        phase_record_version, enqueued_at, claimed_at, attempt_claims, first_claimed_at, placed_at,
        finalizing_at, recorded_by, last_seen_at
    )
    VALUES (
        $1, 1, $2, $3, NOW(), NOW(), 'TASK_CANCELLED', NULL, NULL, 'w', 'h', 1, 'p',
        1, NOW(), NOW(), NULL, NULL, CAST($5 AS timestamptz), NULL, $4, CAST($6 AS timestamptz)
    )
";

/// Listens for the cancel notices on `session`.
async fn listen_for_cancels(session: &Session) {
    session
        .run_script(
            "listen for cancel notices",
            &format!("LISTEN {CANCEL_CHANNEL}"),
        )
        .await;
}

/// The cancel notices that arrived within [`NOTIFY_WAIT`], as payloads.
async fn cancel_notices(session: &Session) -> Vec<String> {
    tokio::time::sleep(NOTIFY_WAIT).await;
    session
        .take_notifications()
        .into_iter()
        .filter(|(channel, _)| channel == CANCEL_CHANNEL)
        .map(|(_, payload)| payload)
        .collect()
}

/// The payload of the cancel notice of `task_id` at the claim generation
/// `claimed_at`: the id and the epoch microseconds of the generation.
fn cancel_payload(task_id: Uuid, claimed_at: DateTime<Utc>) -> String {
    format!("{task_id} {}", claimed_at.timestamp_micros())
}

/// The attempt archive of the history row of `task_id`, with its payload
/// text.
async fn archive_of(session: &Session, task_id: Uuid) -> (AttemptArchive, String) {
    let row = session
        .client()
        .query_one(ARCHIVED_ATTEMPTS_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read the attempt archive: {}", describe(&error)));
    let version: i16 = row.get(0);
    let payload: Vec<u8> = row.get(1);
    let digest: Vec<u8> = row.get(2);
    let archive = decode_attempt_archive(
        version,
        JSON_UTF8_CODEC,
        JSON_CONTENT_TYPE,
        &payload,
        &digest,
    )
    .unwrap_or_else(|error| panic!("decode the attempt archive: {error}"));
    let text = String::from_utf8(payload).expect("the archive is UTF-8");
    (archive, text)
}

/// The pairs that `horsies_cancelled_runs` returns for `pairs`.
async fn cancelled_runs(
    session: &Session,
    pairs: &[(Uuid, DateTime<Utc>)],
) -> Result<Vec<(Uuid, DateTime<Utc>)>, tokio_postgres::Error> {
    let task_ids: Vec<Uuid> = pairs.iter().map(|(task_id, _)| *task_id).collect();
    let claimed_ats: Vec<DateTime<Utc>> = pairs.iter().map(|(_, claimed_at)| *claimed_at).collect();
    let rows = session
        .client()
        .query(CANCELLED_RUNS_SQL, &[&task_ids, &claimed_ats])
        .await?;
    Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
}

#[tokio::test]
async fn applies_with_the_operator_owned_literals_and_pre_image() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;

    let terminal_at = match cancel_admin(&session, task_id, false).await {
        TerminalizationOutcome::Applied {
            kind,
            terminal_at,
            observed,
            ..
        } => {
            assert_eq!(kind, TerminalizationKind::CancelAdmin);
            assert_eq!(observed.status, Some(TaskStatus::Claimed));
            assert_eq!(observed.worker_id.as_deref(), Some(WORKER));
            assert_eq!(observed.claimed_at, Some(generation()));
            terminal_at
        }
        other => panic!("expected Applied: {other:?}"),
    };

    let row = task_row(&session, task_id).await;
    assert_eq!(row.get::<_, String>("status"), "CANCELLED");
    assert_eq!(
        row.get::<_, Option<String>>("error_code").as_deref(),
        Some(CANCEL_ADMIN_ERROR_CODE)
    );
    assert_eq!(
        row.get::<_, Option<String>>("failed_reason").as_deref(),
        Some(CANCEL_ADMIN_REASON)
    );
    assert!(row.get::<_, Option<DateTime<Utc>>>("failed_at").is_some());
    assert_eq!(
        row.get::<_, Option<DateTime<Utc>>>("terminal_at"),
        Some(terminal_at)
    );
    // No claim state survives the move; the history row keeps the claim
    // instant as provenance.
    assert!(!row.get::<_, bool>("claimed"));
    assert_eq!(row.get::<_, Option<String>>("claimed_by_worker_id"), None);
    assert_eq!(
        row.get::<_, Option<DateTime<Utc>>>("claimed_at"),
        Some(generation())
    );
    assert_eq!(row.get::<_, Option<DateTime<Utc>>>("finalizing_at"), None);
    assert_eq!(
        row.get::<_, Option<String>>("finalizing_by_worker_id"),
        None
    );
}

#[tokio::test]
async fn running_requires_explicit_permission() {
    let session = covered().await;

    let refused_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    match cancel_admin(&session, refused_id, false).await {
        TerminalizationOutcome::SourceStateConflict { observed, .. } => {
            assert_eq!(observed.status, Some(TaskStatus::Running));
        }
        other => panic!("expected SourceStateConflict: {other:?}"),
    }

    let allowed_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    match cancel_admin(&session, allowed_id, true).await {
        TerminalizationOutcome::Applied { observed, .. } => {
            assert_eq!(observed.status, Some(TaskStatus::Running));
        }
        other => panic!("expected Applied: {other:?}"),
    }
}

#[tokio::test]
async fn a_workflow_backing_task_is_never_administratively_cancelled() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", true)).await;

    let outcome = cancel_admin(&session, task_id, false).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::SourceStateConflict { .. }),
        "{outcome:?}"
    );
    assert_eq!(status_of(&session, task_id).await, "CLAIMED");
}

#[tokio::test]
async fn replay_is_already_applied() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;
    let _first = cancel_admin(&session, task_id, false).await;

    match cancel_admin(&session, task_id, false).await {
        TerminalizationOutcome::AlreadyApplied { kind, .. } => {
            assert_eq!(kind, TerminalizationKind::CancelAdmin);
        }
        other => panic!("expected AlreadyApplied: {other:?}"),
    }
}

#[tokio::test]
async fn a_finished_task_of_another_kind_is_never_overwritten() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("CLAIMED", false)).await;
    force_terminal(&session, task_id, "CANCELLED", "CANCEL_ORPHAN").await;

    match cancel_admin(&session, task_id, true).await {
        TerminalizationOutcome::SourceStateConflict {
            evidence: GuardEvidence::ForeignTerminalization(evidence),
            ..
        } => {
            assert_eq!(evidence.committed_kind, TerminalizationKind::CancelOrphan);
        }
        other => panic!("expected a conflict with a foreign terminalization: {other:?}"),
    }
}

#[tokio::test]
async fn absence_is_an_outcome() {
    let session = covered().await;
    let outcome = cancel_admin(&session, Uuid::new_v4(), false).await;
    assert!(
        matches!(outcome, TerminalizationOutcome::TaskAbsent { .. }),
        "{outcome:?}"
    );
}

/// A RUNNING cancel records the running attempt in the same statement: one
/// CANCELLED attempt with the phase record of the cancel (recorded by
/// CANCEL, no placement, no last sign of life), archived in the history row
/// with no open attempt. One cancel notice names the task and its claim
/// generation.
#[tokio::test]
async fn a_running_cancel_records_the_cancelled_attempt_and_sends_one_notice() {
    let session = covered().await;
    listen_for_cancels(&session).await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let first_claimed_at = generation() - chrono::Duration::seconds(30);
    let marked_at = generation() + chrono::Duration::seconds(5);
    let _changed = run(
        &session,
        "set the claims and the marker",
        CLAIMS_AND_MARKER_SQL,
        &[&task_id, &2_i32, &first_claimed_at, &Some(marked_at)],
    )
    .await;
    let due_and_start = session
        .client()
        .query_one(DUE_AND_START_SQL, &[&task_id])
        .await
        .unwrap_or_else(|error| panic!("read the live row: {}", describe(&error)));
    let enqueued_at: DateTime<Utc> = due_and_start.get(0);
    let started_at: DateTime<Utc> = due_and_start.get(1);

    let terminal_at = match cancel_admin(&session, task_id, true).await {
        TerminalizationOutcome::Applied {
            kind,
            terminal_at,
            observed,
            ..
        } => {
            assert_eq!(kind, TerminalizationKind::CancelAdmin);
            assert_eq!(observed.status, Some(TaskStatus::Running));
            terminal_at
        }
        other => panic!("expected Applied: {other:?}"),
    };

    let (archive, payload) = archive_of(&session, task_id).await;
    let [record] = archive.attempts() else {
        panic!("one attempt: {:?}", archive.attempts());
    };
    assert_eq!(record.attempt(), 1);
    assert_eq!(record.outcome(), "CANCELLED");
    assert!(!record.will_retry());
    assert_eq!(record.started_at(), started_at);
    assert_eq!(record.finished_at(), terminal_at);
    assert_eq!(record.error_code(), Some(CANCEL_ADMIN_ERROR_CODE));
    assert_eq!(record.error_message(), Some(CANCEL_ADMIN_REASON));
    assert_eq!(record.failed_reason(), None);
    assert_eq!(record.worker_id(), Some(WORKER));
    assert_eq!(record.worker_hostname(), Some("terminalization-host"));
    assert_eq!(record.worker_pid(), Some(4242));
    assert_eq!(record.worker_process_name(), Some("terminalization-proc"));
    match record.phases() {
        PhaseRecord::Captured(phases) => {
            assert_eq!(phases.enqueued_at(), enqueued_at);
            assert_eq!(phases.claimed_at(), generation());
            assert_eq!(
                phases.claims(),
                ClaimRecord::Counted {
                    count: std::num::NonZeroU32::new(2).expect("two claims"),
                    first_claimed_at,
                }
            );
            assert_eq!(phases.finalizing(), Finalizing::Marked { at: marked_at });
            assert_eq!(phases.recorded_by(), RecordedBy::Cancel);
        }
        PhaseRecord::NotCaptured => panic!("the cancelled attempt has no phase record"),
    }
    assert_eq!(archive.open(), OpenAttempt::Ended);
    assert!(
        payload.ends_with(r#""CANCEL",null]],null]"#),
        "the writer CANCEL at position 19, no last sign of life, no open attempt: {payload}"
    );
    assert_eq!(
        cancel_notices(&session).await,
        vec![cancel_payload(task_id, generation())]
    );
}

/// A RUNNING retry keeps its earlier attempts: the cancelled attempt is the
/// attempt after the retry count, and the claims of a row from before the
/// lifecycle record stay not counted.
#[tokio::test]
async fn a_running_cancel_after_a_retry_records_the_next_attempt() {
    let session = covered().await;
    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    requeued_after_failed_attempt(&session, task_id, ATTEMPT_ONE_REASON).await;

    let _applied = cancel_admin(&session, task_id, true).await;

    let (archive, _payload) = archive_of(&session, task_id).await;
    let outcomes: Vec<(i32, &str, bool)> = archive
        .attempts()
        .iter()
        .map(|record| (record.attempt(), record.outcome(), record.will_retry()))
        .collect();
    assert_eq!(outcomes, vec![(1, "FAILED", true), (2, "CANCELLED", false)]);
    match archive.attempts()[1].phases() {
        PhaseRecord::Captured(phases) => {
            assert_eq!(phases.claims(), ClaimRecord::NotCounted);
            assert_eq!(phases.finalizing(), Finalizing::NotMarked);
            assert_eq!(phases.recorded_by(), RecordedBy::Cancel);
        }
        PhaseRecord::NotCaptured => panic!("the cancelled attempt has no phase record"),
    }
    assert_eq!(archive.open(), OpenAttempt::Ended);
}

/// A PENDING or CLAIMED cancel writes no attempt (no attempt ran) and sends
/// no cancel notice: the history row keeps the open attempt.
#[tokio::test]
async fn a_pending_or_claimed_cancel_writes_no_attempt_and_sends_no_notice() {
    let session = covered().await;
    listen_for_cancels(&session).await;
    for status in ["PENDING", "CLAIMED"] {
        let seed_row = match status {
            "PENDING" => Seed {
                status,
                worker_id: None,
                claimed_at: None,
                is_workflow_task: false,
            },
            _ => Seed::claimed_by_this_worker(status, false),
        };
        let task_id = seed(&session, &seed_row).await;

        let outcome = cancel_admin(&session, task_id, true).await;

        assert!(
            matches!(outcome, TerminalizationOutcome::Applied { .. }),
            "{status}: {outcome:?}"
        );
        let (archive, _payload) = archive_of(&session, task_id).await;
        assert!(archive.attempts().is_empty(), "{status}: {archive:?}");
        assert!(
            matches!(archive.open(), OpenAttempt::Open(_)),
            "{status}: {archive:?}"
        );
        assert_eq!(
            cancel_notices(&session).await,
            Vec::<String>::new(),
            "{status}"
        );
    }
}

/// A refused cancel (RUNNING without the opt-in, a workflow task) and a
/// replay of an applied cancel write nothing and send no cancel notice; the
/// applied RUNNING cancel before the replay sends exactly one.
#[tokio::test]
async fn a_refused_or_replayed_cancel_sends_no_notice() {
    let session = covered().await;
    listen_for_cancels(&session).await;

    let running = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let refused = cancel_admin(&session, running, false).await;
    assert!(
        matches!(refused, TerminalizationOutcome::SourceStateConflict { .. }),
        "{refused:?}"
    );
    let workflow = workflow_task(&session).await;
    let refused_workflow = cancel_admin(&session, workflow, true).await;
    assert!(
        matches!(
            refused_workflow,
            TerminalizationOutcome::SourceStateConflict { .. }
        ),
        "{refused_workflow:?}"
    );
    assert_eq!(cancel_notices(&session).await, Vec::<String>::new());
    let live_attempts = attempt_history(&session, running).await;
    assert!(live_attempts.is_empty(), "{live_attempts:?}");

    let _applied = cancel_admin(&session, running, true).await;
    let replay = cancel_admin(&session, running, true).await;
    assert!(
        matches!(replay, TerminalizationOutcome::AlreadyApplied { .. }),
        "{replay:?}"
    );
    assert_eq!(
        cancel_notices(&session).await,
        vec![cancel_payload(running, generation())]
    );
}

/// One case of the attempt checks: its name, `will_retry`, `placed_at`,
/// `last_seen_at`, and the constraint that refuses the row.
type AttemptCheckCase = (
    &'static str,
    bool,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    &'static str,
);

/// The attempt outcome CANCELLED never retries, and the writer CANCEL holds
/// no placement and no last sign of life
/// (`horsies_task_attempts_outcome_known`,
/// `horsies_task_attempts_phase_record_shape`).
#[tokio::test]
async fn the_attempt_checks_refuse_a_retried_cancel_and_a_cancel_with_worker_facts() {
    let session = covered().await;
    let no_time: Option<DateTime<Utc>> = None;
    let cases: [AttemptCheckCase; 3] = [
        (
            "a cancelled attempt that retries",
            true,
            no_time,
            no_time,
            "horsies_task_attempts_outcome_known",
        ),
        (
            "a cancel with a placement",
            false,
            Some(generation()),
            no_time,
            "horsies_task_attempts_phase_record_shape",
        ),
        (
            "a cancel with a last sign of life",
            false,
            no_time,
            Some(generation()),
            "horsies_task_attempts_phase_record_shape",
        ),
    ];
    for (case, will_retry, placed_at, last_seen_at, constraint) in cases {
        let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
        let inserted = session
            .client()
            .execute(
                RAW_ATTEMPT_SQL,
                &[
                    &task_id,
                    &"CANCELLED",
                    &will_retry,
                    &"CANCEL",
                    &placed_at,
                    &last_seen_at,
                ],
            )
            .await;
        let error = inserted.expect_err(case);
        let database_error = error
            .as_db_error()
            .unwrap_or_else(|| panic!("{case}: not a server error: {error}"));
        assert_eq!(
            database_error.code(),
            &SqlState::CHECK_VIOLATION,
            "{case}: {}",
            describe(&error)
        );
        assert_eq!(database_error.constraint(), Some(constraint), "{case}");
    }

    let task_id = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let accepted = run(
        &session,
        "a cancelled attempt recorded by the cancel",
        RAW_ATTEMPT_SQL,
        &[
            &task_id,
            &"CANCELLED",
            &false,
            &"CANCEL",
            &no_time,
            &no_time,
        ],
    )
    .await;
    assert_eq!(accepted, 1);
}

/// The read of the cancelled runs of a worker returns the pair of a task that
/// the cancel moved at that claim generation, once, in input order; not a
/// live task, a task of another claim generation, a task that another
/// statement ended, or an id that neither table holds.
#[tokio::test]
async fn the_cancelled_runs_are_the_pairs_that_the_cancel_moved() {
    let session = covered().await;
    let cancelled = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let _applied = cancel_admin(&session, cancelled, true).await;
    let live = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    let failed = seed(&session, &Seed::claimed_by_this_worker("RUNNING", false)).await;
    force_terminal(&session, failed, "FAILED", "FAIL_RUNNING").await;
    let other_generation = generation() + chrono::Duration::seconds(1);

    let found = cancelled_runs(
        &session,
        &[
            (live, generation()),
            (cancelled, other_generation),
            (cancelled, generation()),
            (failed, generation()),
            (Uuid::new_v4(), generation()),
            (cancelled, generation()),
        ],
    )
    .await
    .unwrap_or_else(|error| panic!("read the cancelled runs: {}", describe(&error)));

    assert_eq!(found, vec![(cancelled, generation())]);
    assert_eq!(
        cancelled_runs(&session, &[])
            .await
            .unwrap_or_else(|error| panic!("read no pair: {}", describe(&error))),
        Vec::new()
    );
}

/// The read refuses arrays of different lengths, a NULL element and more
/// than its bound of pairs, with invalid_parameter_value.
#[tokio::test]
async fn the_cancelled_runs_read_refuses_malformed_input() {
    let session = covered().await;
    let one_id = vec![Uuid::new_v4()];
    let two_times = vec![generation(), generation()];
    let unequal = session
        .client()
        .query(CANCELLED_RUNS_SQL, &[&one_id, &two_times])
        .await;
    let null_time: Vec<Option<DateTime<Utc>>> = vec![None];
    let with_null = session
        .client()
        .query(CANCELLED_RUNS_SQL, &[&one_id, &null_time])
        .await;
    let over: Vec<(Uuid, DateTime<Utc>)> = (0..=CANCELLED_RUNS_MAX_PAIRS)
        .map(|_| (Uuid::new_v4(), generation()))
        .collect();
    let over_bound = cancelled_runs(&session, &over).await;

    for (case, outcome) in [
        ("arrays of different lengths", unequal.map(|_| ())),
        ("a NULL claim generation", with_null.map(|_| ())),
        ("more pairs than the bound", over_bound.map(|_| ())),
    ] {
        let error = outcome.expect_err(case);
        assert_eq!(
            error
                .as_db_error()
                .map(|database_error| database_error.code()),
            Some(&SqlState::INVALID_PARAMETER_VALUE),
            "{case}: {}",
            describe(&error)
        );
    }
}
