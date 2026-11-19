//! Row contracts and argument rules of the claim, the start and the
//! release. Ports of Python `clients/python/tests/unit/test_worker_helpers.py`
//! `TestReadClaimRows` (the claimed rows, the short take, rows the claim
//! never returns) and of the encodings of `clients/python/tests/unit/test_claim_plan.py`
//! `TestEncodings`. The statements run in
//! `broker::lifecycle_tests::claim_start_release`.

use chrono::TimeZone;

use super::*;

fn instant() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 16, 9, 0, 0)
        .single()
        .expect("test instant")
}

fn task(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn claimed_wire(n: u128) -> ClaimWireRow {
    ClaimWireRow {
        row_kind: "CLAIMED".to_owned(),
        id: Some(task(n)),
        task_name: Some("task_a".to_owned()),
        queue_name: Some("q1".to_owned()),
        input: Some("{}".to_owned()),
        task_options: None,
        execution: Some("SHARED_ASYNC".to_owned()),
        is_workflow_task: Some(false),
        claimed_at: Some(instant()),
        retry_count: Some(1),
        max_retries: Some(3),
        short_by: None,
    }
}

fn short_wire(short_by: i32) -> ClaimWireRow {
    ClaimWireRow {
        row_kind: "SHORT".to_owned(),
        short_by: Some(short_by),
        ..ClaimWireRow::default()
    }
}

// --- claim pass rows ----------------------------------------------------------

#[test]
fn claimed_rows_come_in_claim_order_with_no_short_take() {
    let pass = decode_claim_pass(vec![claimed_wire(2), claimed_wire(1)]).unwrap();
    assert_eq!(pass.take, ClaimTake::Full);
    assert_eq!(
        pass.claimed.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![task(2), task(1)]
    );
    assert_eq!(
        pass.claimed[0],
        ClaimedRow {
            id: task(2),
            task_name: "task_a".to_owned(),
            queue_name: "q1".to_owned(),
            input: "{}".to_owned(),
            task_options: None,
            execution: Execution::SharedAsync,
            is_workflow_task: false,
            claimed_at: instant(),
            retry_count: 1,
            max_retries: 3,
        }
    );
}

#[test]
fn a_short_row_is_counted_and_is_not_a_task() {
    let pass = decode_claim_pass(vec![claimed_wire(1), short_wire(2)]).unwrap();
    assert_eq!(pass.claimed.len(), 1);
    assert_eq!(
        pass.take,
        ClaimTake::Short {
            short_by: NonZeroU32::new(2).expect("positive")
        }
    );
}

#[test]
fn an_empty_pass_is_full() {
    let pass = decode_claim_pass(Vec::new()).unwrap();
    assert!(pass.claimed.is_empty());
    assert_eq!(pass.take, ClaimTake::Full);
}

#[test]
fn rows_the_claim_never_returns_are_a_defect() {
    let cases: Vec<(&str, Vec<ClaimWireRow>)> = vec![
        ("second_short_row", vec![short_wire(1), short_wire(1)]),
        (
            "unknown_row_kind",
            vec![ClaimWireRow {
                row_kind: "TAKEN".to_owned(),
                ..claimed_wire(1)
            }],
        ),
        (
            "claimed_with_short_by",
            vec![ClaimWireRow {
                short_by: Some(1),
                ..claimed_wire(1)
            }],
        ),
        (
            "claimed_without_retry_count",
            vec![ClaimWireRow {
                retry_count: None,
                ..claimed_wire(1)
            }],
        ),
        (
            "claimed_without_generation",
            vec![ClaimWireRow {
                claimed_at: None,
                ..claimed_wire(1)
            }],
        ),
        (
            "claimed_of_an_unknown_class",
            vec![ClaimWireRow {
                execution: Some("SHARED".to_owned()),
                ..claimed_wire(1)
            }],
        ),
        (
            "short_with_a_task_column",
            vec![ClaimWireRow {
                id: Some(task(1)),
                ..short_wire(1)
            }],
        ),
        ("short_by_zero", vec![short_wire(0)]),
        ("short_by_negative", vec![short_wire(-1)]),
        (
            "short_without_a_count",
            vec![ClaimWireRow {
                short_by: None,
                ..short_wire(1)
            }],
        ),
    ];
    for (name, rows) in cases {
        assert!(decode_claim_pass(rows).is_err(), "{name} must fail closed");
    }
}

/// Python `test_claim_plan.py::TestRowClass::test_the_stored_class_parses`:
/// the stored `execution` of a claimed row reads as its class, for each of
/// the three values.
#[test]
fn the_stored_class_of_a_claimed_row_parses() {
    for (stored, class) in [
        ("ISOLATED", Execution::Isolated),
        ("SHARED_ASYNC", Execution::SharedAsync),
        ("SHARED_THREAD", Execution::SharedThread),
    ] {
        let pass = decode_claim_pass(vec![ClaimWireRow {
            execution: Some(stored.to_owned()),
            ..claimed_wire(1)
        }])
        .expect("a claimed row of a stored class reads");

        assert_eq!(pass.claimed.len(), 1, "{stored}");
        assert_eq!(pass.claimed[0].execution, class, "{stored}");
    }
}

/// Python `test_claim_plan.py::TestRowClass::test_a_value_outside_the_enum_is_a_defect`:
/// a claimed row whose `execution` is not an `Execution` value breaks the
/// row contract of the claim, and the refusal names the task and the value.
#[test]
fn a_claimed_row_of_a_class_outside_the_enum_is_refused_with_its_task() {
    let refused = decode_claim_pass(vec![ClaimWireRow {
        execution: Some("SHARED_GPU".to_owned()),
        ..claimed_wire(1)
    }])
    .expect_err("a class outside the enum breaks the row contract");

    assert!(
        refused.contains(&format!(
            "claimed task {} has execution \"SHARED_GPU\"",
            task(1)
        )),
        "{refused}"
    );
}

// --- claim pass arguments -----------------------------------------------------

#[test]
fn the_budgets_and_routes_encode_by_class_name() {
    let classes = ClaimClasses::new(
        HashMap::from([(Execution::Isolated, 2), (Execution::SharedAsync, 8)]),
        HashMap::from([
            (Execution::Isolated, Execution::Isolated),
            (Execution::SharedThread, Execution::Isolated),
            (Execution::SharedAsync, Execution::SharedAsync),
        ]),
    )
    .expect("every route has its budget");
    assert_eq!(
        classes.budgets_json(),
        serde_json::json!({"ISOLATED": 2, "SHARED_ASYNC": 8})
    );
    assert_eq!(
        classes.routes_json(),
        serde_json::json!({
            "ISOLATED": "ISOLATED",
            "SHARED_THREAD": "ISOLATED",
            "SHARED_ASYNC": "SHARED_ASYNC",
        })
    );
}

#[test]
fn a_worker_that_serves_nothing_sends_empty_maps() {
    let classes = ClaimClasses::new(HashMap::new(), HashMap::new()).expect("empty plan");
    assert_eq!(classes.budgets_json(), serde_json::json!({}));
    assert_eq!(classes.routes_json(), serde_json::json!({}));
}

#[test]
fn a_route_to_a_budget_the_pass_does_not_hold_is_refused() {
    assert_eq!(
        ClaimClasses::new(
            HashMap::from([(Execution::Isolated, 1)]),
            HashMap::from([(Execution::SharedThread, Execution::SharedThread)]),
        ),
        Err(UnheldBudget {
            row_class: Execution::SharedThread,
            budget_class: Execution::SharedThread,
        })
    );
}

#[test]
fn a_count_above_the_integer_range_is_refused() {
    assert!(matches!(
        claim_integer("max_claim_batch", u32::MAX),
        Err(BrokerError::InvalidClaimArgument(_))
    ));
    assert_eq!(
        claim_integer("max_claim_batch", 2_147_483_647).ok(),
        Some(i32::MAX)
    );
}

// --- start --------------------------------------------------------------------

#[test]
fn a_started_row_carries_its_start() {
    assert_eq!(
        decode_start("STARTED", None, Some(instant())),
        Ok(StartOutcome::Started {
            started_at: instant()
        })
    );
}

#[test]
fn each_miss_reason_decodes() {
    for (wire, miss) in [
        ("DEADLINE_PASSED", StartMiss::DeadlinePassed),
        ("NOT_HELD", StartMiss::NotHeld),
        ("LEASE_EXPIRED", StartMiss::LeaseExpired),
        ("WORKFLOW_PAUSED", StartMiss::WorkflowPaused),
        ("WORKFLOW_CANCELLED", StartMiss::WorkflowCancelled),
        ("NODE_NOT_LINKED", StartMiss::NodeNotLinked),
    ] {
        assert_eq!(
            decode_start("MISSED", Some(wire), None),
            Ok(StartOutcome::Missed(miss)),
            "{wire}"
        );
    }
}

#[test]
fn a_start_row_against_its_outcome_fails_closed() {
    let cases: [(&str, &str, Option<&str>, Option<DateTime<Utc>>); 5] = [
        ("started_without_a_start", "STARTED", None, None),
        (
            "started_with_a_reason",
            "STARTED",
            Some("NOT_HELD"),
            Some(instant()),
        ),
        (
            "missed_with_a_start",
            "MISSED",
            Some("NOT_HELD"),
            Some(instant()),
        ),
        (
            "missed_with_an_unknown_reason",
            "MISSED",
            Some("PAUSED"),
            None,
        ),
        ("unknown_outcome", "SKIPPED", None, None),
    ];
    for (name, outcome, reason, started_at) in cases {
        assert!(
            decode_start(outcome, reason, started_at).is_err(),
            "{name} must fail closed"
        );
    }
}

// --- release ------------------------------------------------------------------

#[test]
fn a_release_batch_takes_up_to_its_bound() {
    let claims: Vec<(Uuid, DateTime<Utc>)> = (0..MAX_RELEASED_CLAIMS as u128)
        .map(|n| (task(n), instant()))
        .collect();
    assert!(ReleaseBatch::new(&claims).is_ok());
    assert!(ReleaseBatch::new(&[]).is_ok());
}

#[test]
fn a_release_batch_over_its_bound_is_refused() {
    let claims: Vec<(Uuid, DateTime<Utc>)> = (0..=MAX_RELEASED_CLAIMS as u128)
        .map(|n| (task(n), instant()))
        .collect();
    assert_eq!(
        ReleaseBatch::new(&claims),
        Err(ReleaseBatchError::TooManyRows {
            count: MAX_RELEASED_CLAIMS + 1
        })
    );
}

#[test]
fn a_task_twice_in_a_release_is_refused() {
    assert_eq!(
        ReleaseBatch::new(&[(task(1), instant()), (task(1), instant())]),
        Err(ReleaseBatchError::RepeatedTask { task_id: task(1) })
    );
}

#[test]
fn release_outcomes_follow_the_input_order() {
    let input = [task(5), task(3)];
    let rows = vec![
        (2, task(3), "NOT_HELD".to_owned()),
        (1, task(5), "RELEASED".to_owned()),
    ];
    assert_eq!(
        decode_release(&input, rows),
        Ok(vec![
            (task(5), ReleaseOutcome::Released),
            (task(3), ReleaseOutcome::NotHeld),
        ])
    );
}

#[test]
fn release_rows_that_break_the_contract_fail_closed() {
    let input = [task(1), task(2)];
    let cases: Vec<(&str, Vec<(i64, Uuid, String)>)> = vec![
        ("missing_row", vec![(1, task(1), "RELEASED".to_owned())]),
        (
            "row_answered_twice",
            vec![
                (1, task(1), "RELEASED".to_owned()),
                (1, task(1), "RELEASED".to_owned()),
                (2, task(2), "RELEASED".to_owned()),
            ],
        ),
        (
            "wrong_task_at_its_position",
            vec![
                (1, task(2), "RELEASED".to_owned()),
                (2, task(1), "RELEASED".to_owned()),
            ],
        ),
        (
            "ordinality_out_of_range",
            vec![
                (0, task(1), "RELEASED".to_owned()),
                (2, task(2), "RELEASED".to_owned()),
            ],
        ),
        (
            "unknown_outcome",
            vec![
                (1, task(1), "KEPT".to_owned()),
                (2, task(2), "RELEASED".to_owned()),
            ],
        ),
    ];
    for (name, rows) in cases {
        assert!(
            decode_release(&input, rows).is_err(),
            "{name} must fail closed"
        );
    }
}

// ---------------------------------------------------------------------------
// Start check
// ---------------------------------------------------------------------------

fn queues(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

/// Python `test_count_is_running_plus_started_not_running`.
#[test]
fn the_count_of_a_queue_is_running_plus_started_not_running() {
    let starts = decode_queue_starts(
        &queues(&["a", "b"]),
        vec![("a".to_owned(), 2, 1), ("b".to_owned(), 0, 3)],
    )
    .expect("a valid count");
    assert_eq!(starts.of("a"), 3);
    assert_eq!(starts.of("b"), 3);
}

/// Python `test_no_row_is_no_count`.
#[test]
fn a_queue_with_no_row_counts_0() {
    let starts = decode_queue_starts(&queues(&["a"]), Vec::new()).expect("a valid count");
    assert_eq!(starts, QueueStarts::default());
    assert_eq!(starts.of("a"), 0);
}

/// Python `test_a_queue_counted_twice_is_a_defect`, and the other rows
/// the statement never returns.
#[test]
fn count_rows_that_break_the_contract_fail_closed() {
    let cases: Vec<(&str, Vec<(String, i64, i64)>)> = vec![
        (
            "queue_counted_twice",
            vec![("a".to_owned(), 1, 0), ("a".to_owned(), 1, 0)],
        ),
        ("queue_not_named", vec![("c".to_owned(), 1, 0)]),
        ("negative_running", vec![("a".to_owned(), -1, 0)]),
        ("negative_started", vec![("a".to_owned(), 0, -1)]),
    ];
    for (name, rows) in cases {
        assert!(
            decode_queue_starts(&queues(&["a", "b"]), rows).is_err(),
            "{name} must fail closed"
        );
    }
}

/// The start names its process by its pid: the process name
/// `worker-<pid>`.
#[test]
fn a_start_names_its_process_by_its_pid() {
    let start = StartAttempt::of_process(
        task(1),
        OwnedClaim {
            worker_id: "worker-a".to_owned(),
            claimed_at: instant(),
        },
        4242,
        "host-1".to_owned(),
    );
    assert_eq!(start.worker_pid, 4242);
    assert_eq!(start.worker_hostname, "host-1");
    assert_eq!(start.worker_process_name, "worker-4242");
}

/// Each miss reason reads back from its own text.
#[test]
fn each_miss_reason_reads_back_from_its_text() {
    for miss in [
        StartMiss::DeadlinePassed,
        StartMiss::NotHeld,
        StartMiss::LeaseExpired,
        StartMiss::WorkflowPaused,
        StartMiss::WorkflowCancelled,
        StartMiss::NodeNotLinked,
    ] {
        assert_eq!(StartMiss::from_wire(miss.as_str()), Some(miss));
    }
}
