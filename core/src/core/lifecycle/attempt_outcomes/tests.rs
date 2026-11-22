//! The decoding of the failed-attempt and workflow-success outcomes: the
//! writes, the refusals, and the rows that fail closed. The column-shape
//! checks need a database row and are pinned by the writer tests.

use chrono::TimeZone;

use super::*;

fn task_id() -> Uuid {
    Uuid::parse_str("0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e01").expect("test task id")
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 11, 12, 0, 0)
        .single()
        .expect("test instant")
}

fn later() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 11, 12, 5, 0)
        .single()
        .expect("test instant")
}

fn observed() -> ObservedTaskState {
    ObservedTaskState {
        status: Some(TaskStatus::Running),
        worker_id: Some("w-1".to_owned()),
        claimed_at: Some(now()),
    }
}

/// No node facts: the columns of a plain task or a row that wrote nothing.
fn no_facts() -> NodeFactColumns {
    NodeFactColumns {
        node_has_dependents: None,
        downstream_subworkflow: None,
    }
}

/// The node facts that phase 1 returns.
fn facts(node_has_dependents: bool, downstream_subworkflow: Option<&str>) -> NodeFactColumns {
    NodeFactColumns {
        node_has_dependents: Some(node_has_dependents),
        downstream_subworkflow: downstream_subworkflow.map(str::to_owned),
    }
}

/// The shared columns of an APPLIED row of a failed attempt.
fn applied_raw() -> RawOutcomeRow {
    RawOutcomeRow {
        task_id: task_id(),
        ordinality: None,
        outcome: "APPLIED".to_owned(),
        terminal_at: Some(now()),
        terminalization_kind: Some("FAIL_RUNNING".to_owned()),
        observed_status: Some("RUNNING".to_owned()),
        observed_worker_id: Some("w-1".to_owned()),
        observed_claimed_at: Some(now()),
        guard_kind: None,
        observed_guard: None,
    }
}

/// The extra columns of an APPLIED row of a plain task.
fn applied_extra() -> FailedAttemptColumns {
    FailedAttemptColumns {
        retry_disposition: Some("NOT_REQUESTED".to_owned()),
        retry_count: None,
        next_retry_at: None,
        good_until: None,
        is_workflow_task: Some(false),
        queue_name: Some("q1".to_owned()),
        task_name: Some("task_a".to_owned()),
        facts: no_facts(),
    }
}

fn retry_raw() -> RawOutcomeRow {
    RawOutcomeRow {
        outcome: "RETRY_SCHEDULED".to_owned(),
        terminal_at: None,
        terminalization_kind: None,
        ..applied_raw()
    }
}

fn retry_extra() -> FailedAttemptColumns {
    FailedAttemptColumns {
        retry_disposition: None,
        retry_count: Some(2),
        next_retry_at: Some(later()),
        good_until: None,
        is_workflow_task: None,
        queue_name: Some("q1".to_owned()),
        task_name: None,
        facts: no_facts(),
    }
}

fn miss_raw(outcome: &str) -> RawOutcomeRow {
    RawOutcomeRow {
        outcome: outcome.to_owned(),
        terminal_at: None,
        terminalization_kind: None,
        ..applied_raw()
    }
}

fn miss_extra() -> FailedAttemptColumns {
    FailedAttemptColumns {
        retry_disposition: None,
        retry_count: None,
        next_retry_at: None,
        good_until: None,
        is_workflow_task: None,
        queue_name: None,
        task_name: None,
        facts: no_facts(),
    }
}

/// The outcome of a row that is not `RELEASED`.
fn decode_written(
    raw: &RawOutcomeRow,
    extra: FailedAttemptColumns,
) -> Result<FailedAttemptOutcome, OutcomeDecodeError> {
    match decode_failed_attempt(raw, extra)? {
        UnreportedAttemptOutcome::Failed(outcome) => Ok(outcome),
        UnreportedAttemptOutcome::Released(released) => {
            panic!("expected a written or refused attempt, got {released:?}")
        }
    }
}

/// The shared columns of a `RELEASED` row.
fn released_raw() -> RawOutcomeRow {
    RawOutcomeRow {
        outcome: "RELEASED".to_owned(),
        terminal_at: None,
        terminalization_kind: None,
        observed_status: Some("CLAIMED".to_owned()),
        ..applied_raw()
    }
}

/// The extra columns of a `RELEASED` row: the queue only.
fn released_extra() -> FailedAttemptColumns {
    FailedAttemptColumns {
        queue_name: Some("q1".to_owned()),
        ..miss_extra()
    }
}

// --- failed attempt: writes -------------------------------------------------

#[test]
fn a_plain_failure_is_applied_and_woken() {
    assert_eq!(
        decode_written(&applied_raw(), applied_extra()).unwrap(),
        FailedAttemptOutcome::FailureApplied {
            task_id: task_id(),
            terminal_at: now(),
            observed: observed(),
            retry: RetryRefusal::NotRequested,
            after: AfterFailure::CapacityWoken,
        }
    );
}

#[test]
fn a_workflow_failure_owes_phase_two() {
    let extra = FailedAttemptColumns {
        is_workflow_task: Some(true),
        facts: facts(true, Some("NONE")),
        ..applied_extra()
    };
    match decode_written(&applied_raw(), extra).unwrap() {
        FailedAttemptOutcome::FailureApplied { after, .. } => assert_eq!(
            after,
            AfterFailure::PhaseTwoOwed(PhaseTwoOwed {
                task_name: "task_a".to_owned(),
                queue_name: "q1".to_owned(),
                step: PhaseTwoStep::DependentsWithoutSubworkflow,
            })
        ),
        other => panic!("expected FailureApplied, got {other:?}"),
    }
}

#[test]
fn the_node_facts_give_the_phase_two_step() {
    let cases = [
        (facts(false, Some("NONE")), PhaseTwoStep::NoDependents),
        (facts(false, None), PhaseTwoStep::NoDependents),
        (
            facts(true, Some("NONE")),
            PhaseTwoStep::DependentsWithoutSubworkflow,
        ),
        (
            facts(true, Some("REACHABLE")),
            PhaseTwoStep::SubworkflowReachable,
        ),
    ];
    for (node_facts, step) in cases {
        let extra = FailedAttemptColumns {
            is_workflow_task: Some(true),
            facts: node_facts,
            ..applied_extra()
        };
        match decode_written(&applied_raw(), extra).unwrap() {
            FailedAttemptOutcome::FailureApplied {
                after: AfterFailure::PhaseTwoOwed(owed),
                ..
            } => assert_eq!(owed.step, step),
            other => panic!("expected a failure that owes phase 2, got {other:?}"),
        }
    }
}

#[test]
fn node_facts_that_disagree_or_are_unknown_fail_closed() {
    for node_facts in [
        no_facts(),
        facts(false, Some("REACHABLE")),
        facts(true, None),
        facts(true, Some("SOMETIMES")),
    ] {
        let extra = FailedAttemptColumns {
            is_workflow_task: Some(true),
            facts: node_facts,
            ..applied_extra()
        };
        assert!(decode_failed_attempt(&applied_raw(), extra).is_err());
    }
}

#[test]
fn node_facts_on_a_row_without_phase_two_fail_closed() {
    let cases = [
        (applied_raw(), applied_extra()),
        (retry_raw(), retry_extra()),
        (miss_raw("LOST_CLAIM"), miss_extra()),
        (released_raw(), released_extra()),
    ];
    for (raw, extra) in cases {
        let extra = FailedAttemptColumns {
            facts: facts(true, Some("NONE")),
            ..extra
        };
        assert!(decode_failed_attempt(&raw, extra).is_err());
    }
}

#[test]
fn a_retry_past_its_deadline_carries_both_instants() {
    let extra = FailedAttemptColumns {
        retry_disposition: Some("PAST_DEADLINE".to_owned()),
        next_retry_at: Some(later()),
        good_until: Some(now()),
        ..applied_extra()
    };
    match decode_written(&applied_raw(), extra).unwrap() {
        FailedAttemptOutcome::FailureApplied { retry, .. } => assert_eq!(
            retry,
            RetryRefusal::PastDeadline {
                next_retry_at: later(),
                good_until: now(),
            }
        ),
        other => panic!("expected FailureApplied, got {other:?}"),
    }
}

#[test]
fn a_scheduled_retry() {
    assert_eq!(
        decode_written(&retry_raw(), retry_extra()).unwrap(),
        FailedAttemptOutcome::RetryScheduled {
            task_id: task_id(),
            retry_count: 2,
            next_retry_at: later(),
            queue_name: "q1".to_owned(),
            observed: observed(),
        }
    );
}

// --- failed attempt: refusals -----------------------------------------------

#[test]
fn a_replay_is_already_applied() {
    let raw = RawOutcomeRow {
        terminal_at: Some(now()),
        terminalization_kind: Some("FAIL_RUNNING".to_owned()),
        observed_status: Some("FAILED".to_owned()),
        observed_worker_id: None,
        observed_claimed_at: None,
        ..miss_raw("ALREADY_APPLIED")
    };
    match decode_written(&raw, miss_extra()).unwrap() {
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { kind, .. }) => {
            assert_eq!(kind, TerminalizationKind::FailRunning);
        }
        other => panic!("expected AlreadyApplied, got {other:?}"),
    }
}

#[test]
fn a_lost_claim() {
    assert!(matches!(
        decode_written(&miss_raw("LOST_CLAIM"), miss_extra()).unwrap(),
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::LostClaim { .. })
    ));
}

#[test]
fn a_conflict() {
    let raw = RawOutcomeRow {
        observed_status: Some("CLAIMED".to_owned()),
        ..miss_raw("SOURCE_STATE_CONFLICT")
    };
    assert!(matches!(
        decode_written(&raw, miss_extra()).unwrap(),
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict { .. })
    ));
}

#[test]
fn an_absent_task() {
    let raw = RawOutcomeRow {
        observed_status: None,
        observed_worker_id: None,
        observed_claimed_at: None,
        ..miss_raw("TASK_ABSENT")
    };
    assert_eq!(
        decode_written(&raw, miss_extra()).unwrap(),
        FailedAttemptOutcome::Refused(AttemptWriteRefusal::TaskAbsent { task_id: task_id() })
    );
}

// --- failed attempt: the release of a claimed row --------------------------

#[test]
fn a_claimed_row_is_released() {
    assert_eq!(
        decode_failed_attempt(&released_raw(), released_extra()).unwrap(),
        UnreportedAttemptOutcome::Released(ReleasedClaim {
            task_id: task_id(),
            queue_name: "q1".to_owned(),
            observed: ObservedTaskState {
                status: Some(TaskStatus::Claimed),
                ..observed()
            },
        })
    );
}

// --- failed attempt: fails closed -------------------------------------------

#[test]
fn a_column_against_its_outcome_fails_closed() {
    let cases: Vec<(&str, RawOutcomeRow, FailedAttemptColumns)> = vec![
        (
            "applied_without_disposition",
            applied_raw(),
            FailedAttemptColumns {
                retry_disposition: None,
                ..applied_extra()
            },
        ),
        (
            "unknown_disposition",
            applied_raw(),
            FailedAttemptColumns {
                retry_disposition: Some("LATER".to_owned()),
                ..applied_extra()
            },
        ),
        (
            "past_deadline_without_instants",
            applied_raw(),
            FailedAttemptColumns {
                retry_disposition: Some("PAST_DEADLINE".to_owned()),
                ..applied_extra()
            },
        ),
        (
            "not_requested_with_a_retry_time",
            applied_raw(),
            FailedAttemptColumns {
                next_retry_at: Some(later()),
                ..applied_extra()
            },
        ),
        (
            "applied_with_a_retry_count",
            applied_raw(),
            FailedAttemptColumns {
                retry_count: Some(1),
                ..applied_extra()
            },
        ),
        (
            "applied_of_another_kind",
            RawOutcomeRow {
                terminalization_kind: Some("FAIL_STALE".to_owned()),
                ..applied_raw()
            },
            applied_extra(),
        ),
        (
            "applied_without_the_task_kind",
            applied_raw(),
            FailedAttemptColumns {
                is_workflow_task: None,
                ..applied_extra()
            },
        ),
        (
            "phase_two_without_a_task_name",
            applied_raw(),
            FailedAttemptColumns {
                is_workflow_task: Some(true),
                task_name: None,
                ..applied_extra()
            },
        ),
        (
            "retry_with_a_terminal_time",
            RawOutcomeRow {
                terminal_at: Some(now()),
                ..retry_raw()
            },
            retry_extra(),
        ),
        (
            "retry_without_a_count",
            retry_raw(),
            FailedAttemptColumns {
                retry_count: None,
                ..retry_extra()
            },
        ),
        (
            "retry_without_a_queue",
            retry_raw(),
            FailedAttemptColumns {
                queue_name: None,
                ..retry_extra()
            },
        ),
        (
            "retry_with_a_deadline",
            retry_raw(),
            FailedAttemptColumns {
                good_until: Some(now()),
                ..retry_extra()
            },
        ),
        (
            "refusal_with_a_retry_count",
            miss_raw("LOST_CLAIM"),
            FailedAttemptColumns {
                retry_count: Some(1),
                ..miss_extra()
            },
        ),
        (
            "refusal_with_a_queue",
            miss_raw("LOST_CLAIM"),
            FailedAttemptColumns {
                queue_name: Some("q1".to_owned()),
                ..miss_extra()
            },
        ),
        (
            "unknown_outcome",
            miss_raw("APPLIED_ELSEWHERE"),
            miss_extra(),
        ),
        (
            "workflow_failure_without_the_dependents_fact",
            applied_raw(),
            FailedAttemptColumns {
                is_workflow_task: Some(true),
                ..applied_extra()
            },
        ),
        (
            "plain_failure_with_the_dependents_fact",
            applied_raw(),
            FailedAttemptColumns {
                facts: facts(false, Some("NONE")),
                ..applied_extra()
            },
        ),
        (
            "retry_with_the_dependents_fact",
            retry_raw(),
            FailedAttemptColumns {
                facts: facts(false, Some("NONE")),
                ..retry_extra()
            },
        ),
        (
            "refusal_with_the_dependents_fact",
            miss_raw("LOST_CLAIM"),
            FailedAttemptColumns {
                facts: facts(true, Some("NONE")),
                ..miss_extra()
            },
        ),
        ("release_without_a_queue", released_raw(), miss_extra()),
        (
            "release_with_a_retry_count",
            released_raw(),
            FailedAttemptColumns {
                retry_count: Some(1),
                ..released_extra()
            },
        ),
        (
            "release_with_a_task_name",
            released_raw(),
            FailedAttemptColumns {
                task_name: Some("task_a".to_owned()),
                ..released_extra()
            },
        ),
        (
            "release_with_a_terminal_time",
            RawOutcomeRow {
                terminal_at: Some(now()),
                ..released_raw()
            },
            released_extra(),
        ),
        (
            "release_of_a_running_row",
            RawOutcomeRow {
                observed_status: Some("RUNNING".to_owned()),
                ..released_raw()
            },
            released_extra(),
        ),
    ];
    for (name, raw, extra) in cases {
        assert!(
            decode_failed_attempt(&raw, extra).is_err(),
            "{name} must fail closed"
        );
    }
}

// --- stale attempt ------------------------------------------------------------

/// The shared columns of an APPLIED row of a stale attempt.
fn stale_applied_raw() -> RawOutcomeRow {
    RawOutcomeRow {
        terminalization_kind: Some("FAIL_STALE".to_owned()),
        ..applied_raw()
    }
}

#[test]
fn a_stale_plain_failure_owes_nothing() {
    assert_eq!(
        decode_stale_attempt(&stale_applied_raw(), applied_extra()).expect("a stale failure"),
        StaleAttemptOutcome::FailureApplied {
            task_id: task_id(),
            terminal_at: now(),
            observed: observed(),
            retry: RetryRefusal::NotRequested,
            after: AfterStaleFailure::NothingOwed,
        }
    );
}

#[test]
fn a_stale_workflow_failure_owes_phase_two() {
    let extra = FailedAttemptColumns {
        is_workflow_task: Some(true),
        facts: facts(false, Some("NONE")),
        ..applied_extra()
    };
    match decode_stale_attempt(&stale_applied_raw(), extra).expect("a stale failure") {
        StaleAttemptOutcome::FailureApplied { after, .. } => assert_eq!(
            after,
            AfterStaleFailure::PhaseTwoOwed(PhaseTwoOwed {
                task_name: "task_a".to_owned(),
                queue_name: "q1".to_owned(),
                step: PhaseTwoStep::NoDependents,
            })
        ),
        other => panic!("expected FailureApplied, got {other:?}"),
    }
}

#[test]
fn a_stale_retry_past_its_deadline_is_a_failure() {
    let extra = FailedAttemptColumns {
        retry_disposition: Some("PAST_DEADLINE".to_owned()),
        next_retry_at: Some(later()),
        good_until: Some(now()),
        ..applied_extra()
    };
    match decode_stale_attempt(&stale_applied_raw(), extra).expect("a stale failure") {
        StaleAttemptOutcome::FailureApplied { retry, .. } => assert_eq!(
            retry,
            RetryRefusal::PastDeadline {
                next_retry_at: later(),
                good_until: now(),
            }
        ),
        other => panic!("expected FailureApplied, got {other:?}"),
    }
}

#[test]
fn a_stale_retry_is_scheduled() {
    assert_eq!(
        decode_stale_attempt(&retry_raw(), retry_extra()).expect("a stale retry"),
        StaleAttemptOutcome::RetryScheduled {
            task_id: task_id(),
            retry_count: 2,
            next_retry_at: later(),
            queue_name: "q1".to_owned(),
            observed: observed(),
        }
    );
}

#[test]
fn a_stale_retry_count_conflict_is_refused() {
    let raw = RawOutcomeRow {
        guard_kind: Some("RETRY_COUNT".to_owned()),
        observed_guard: Some(serde_json::json!({
            "expected_retry_count": 0,
            "retry_count": 1
        })),
        ..miss_raw("SOURCE_STATE_CONFLICT")
    };
    match decode_stale_attempt(&raw, miss_extra()).expect("a refusal") {
        StaleAttemptOutcome::Refused(AttemptWriteRefusal::SourceStateConflict {
            evidence: GuardEvidence::RetryCount(counts),
            ..
        }) => {
            assert_eq!((counts.expected_retry_count, counts.retry_count), (0, 1));
        }
        other => panic!("expected a retry-count conflict, got {other:?}"),
    }
}

#[test]
fn a_stale_row_against_its_outcome_fails_closed() {
    let cases: Vec<(&str, RawOutcomeRow, FailedAttemptColumns)> = vec![
        (
            "applied_of_the_running_kind",
            applied_raw(),
            applied_extra(),
        ),
        (
            "applied_with_a_retry_count",
            stale_applied_raw(),
            FailedAttemptColumns {
                retry_count: Some(1),
                ..applied_extra()
            },
        ),
        (
            "workflow_failure_without_dependents_fact",
            stale_applied_raw(),
            FailedAttemptColumns {
                is_workflow_task: Some(true),
                ..applied_extra()
            },
        ),
        (
            "retry_with_a_disposition",
            retry_raw(),
            FailedAttemptColumns {
                retry_disposition: Some("NOT_REQUESTED".to_owned()),
                ..retry_extra()
            },
        ),
        ("released", released_raw(), released_extra()),
        (
            "refusal_with_a_queue",
            miss_raw("LOST_CLAIM"),
            FailedAttemptColumns {
                queue_name: Some("q1".to_owned()),
                ..miss_extra()
            },
        ),
    ];
    for (name, raw, extra) in cases {
        assert!(
            decode_stale_attempt(&raw, extra).is_err(),
            "{name} must fail closed"
        );
    }
}

/// A node fact on the stale failure of a plain task fails closed and names
/// the column.
#[test]
fn a_stale_failure_of_a_plain_task_with_a_node_fact_fails_closed() {
    let extra = FailedAttemptColumns {
        facts: NodeFactColumns {
            node_has_dependents: None,
            downstream_subworkflow: Some("NONE".to_owned()),
        },
        ..applied_extra()
    };

    let refused = decode_stale_attempt(&stale_applied_raw(), extra)
        .expect_err("a node fact on a plain task is refused");

    assert!(
        refused.0.contains("does not set") && refused.0.contains("downstream_subworkflow"),
        "{refused}"
    );
}

// --- workflow success ---------------------------------------------------------

fn success_raw() -> RawOutcomeRow {
    RawOutcomeRow {
        terminalization_kind: Some("COMPLETE_LOCKED".to_owned()),
        ..applied_raw()
    }
}

fn success_columns(
    queue_name: Option<&str>,
    task_name: Option<&str>,
    facts: NodeFactColumns,
) -> WorkflowSuccessColumns {
    WorkflowSuccessColumns {
        queue_name: queue_name.map(str::to_owned),
        task_name: task_name.map(str::to_owned),
        facts,
    }
}

#[test]
fn an_applied_success_owes_phase_two() {
    assert_eq!(
        decode_workflow_success(
            &success_raw(),
            success_columns(Some("q1"), Some("task_a"), facts(true, Some("REACHABLE")))
        )
        .unwrap(),
        WorkflowSuccessOutcome::SuccessApplied {
            task_id: task_id(),
            terminal_at: now(),
            observed: observed(),
            phase_two: PhaseTwoOwed {
                task_name: "task_a".to_owned(),
                queue_name: "q1".to_owned(),
                step: PhaseTwoStep::SubworkflowReachable,
            },
        }
    );
}

#[test]
fn a_success_replay_is_already_applied() {
    let raw = RawOutcomeRow {
        terminal_at: Some(now()),
        terminalization_kind: Some("COMPLETE_FUSED".to_owned()),
        observed_status: Some("COMPLETED".to_owned()),
        observed_worker_id: None,
        observed_claimed_at: None,
        ..miss_raw("ALREADY_APPLIED")
    };
    assert!(matches!(
        decode_workflow_success(&raw, success_columns(None, None, no_facts())).unwrap(),
        WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::AlreadyApplied { .. })
    ));
}

#[test]
fn a_success_refusal_is_decoded() {
    assert!(matches!(
        decode_workflow_success(
            &miss_raw("LOST_CLAIM"),
            success_columns(None, None, no_facts())
        )
        .unwrap(),
        WorkflowSuccessOutcome::Refused(AttemptWriteRefusal::LostClaim { .. })
    ));
}

#[test]
fn a_success_against_its_outcome_fails_closed() {
    let cases: Vec<(&str, RawOutcomeRow, WorkflowSuccessColumns)> = vec![
        (
            "applied_of_another_kind",
            applied_raw(),
            success_columns(Some("q1"), Some("task_a"), facts(true, Some("NONE"))),
        ),
        (
            "applied_without_a_queue",
            success_raw(),
            success_columns(None, Some("task_a"), facts(true, Some("NONE"))),
        ),
        (
            "applied_without_a_task_name",
            success_raw(),
            success_columns(Some("q1"), None, facts(true, Some("NONE"))),
        ),
        (
            "applied_without_the_dependents_fact",
            success_raw(),
            success_columns(Some("q1"), Some("task_a"), no_facts()),
        ),
        (
            "applied_with_disagreeing_node_facts",
            success_raw(),
            success_columns(Some("q1"), Some("task_a"), facts(false, Some("REACHABLE"))),
        ),
        (
            "refusal_with_a_queue",
            miss_raw("LOST_CLAIM"),
            success_columns(Some("q1"), None, no_facts()),
        ),
        (
            "refusal_with_the_dependents_fact",
            miss_raw("LOST_CLAIM"),
            success_columns(None, None, facts(false, Some("NONE"))),
        ),
        (
            "unknown_outcome",
            miss_raw("APPLIED_ELSEWHERE"),
            success_columns(None, None, no_facts()),
        ),
    ];
    for (name, raw, columns) in cases {
        assert!(
            decode_workflow_success(&raw, columns).is_err(),
            "{name} must fail closed"
        );
    }
}
