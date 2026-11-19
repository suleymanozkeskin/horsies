//! Tests of the slots: routes, budgets, the writes of each end, the answer
//! of each task, and isolated slots driven by scripted children.

use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use uuid::Uuid;

use super::outcome::{
    crash_recovery_writes, killed_child_write, retry_request, task_error_write,
    timeout_recovery_writes, timeout_write, worker_failure_write, WriteStage,
};
use super::pool::Answer;
use super::scripted::{drop_bytes, exit_at_stop, take_init, ScriptCount};
use super::*;
use crate::broker::attempt_writes::{AttemptFailureKind, RetryPlacement, RetryRequest};
use crate::core::task::error::{OperationalErrorCode, OutcomeCode, TaskError};
use crate::worker::executor::placement::ChildCpus;
use crate::worker::executor::protocol::{
    encode_child_frame, encode_parent_frame, CancelGraceMs, ChildAfterFrame, ChildConnection,
    ChildFrame, ChildKind, ChildLogLevel, ChildPool, ChildProgram, DatabaseUrl, InitFrame,
    IsolatedChild, IsolatedWork, NoLeftWork, ParentFrame, PoolMaxSize, PoolMinSize, ProcessId,
    ReadyFrame, ResultEnvelopeText, ResultFrame, RssReading, RunEnd, RunFrame, SharedWork,
    StoredOptionsRefusal, TaskInputText, TaskSendPool, TaskTimeoutMs,
};
use crate::worker::executor::supervisor::{ChildCommand, Spawner, CHILD_KILL_WAIT};
use crate::worker::retry::{read_stored_options, StoredOptions};
use crate::worker::runner_beats::LiveDispatches;

const TASK: &str = "0190f5a2-7b1c-7d3e-8f00-0000000000aa";

fn options(text: &str) -> StoredOptions {
    read_stored_options(Some(text))
}

fn task(execution: ExecutionClass, stored: &str) -> ClaimedTask {
    ClaimedTask {
        task_id: Uuid::parse_str(TASK).unwrap(),
        task_name: "app.tasks.work".to_owned(),
        queue_name: "default".to_owned(),
        claimed_at: Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap(),
        execution,
        kind: TaskKind::Plain,
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 2,
        },
        options: options(stored),
        trace: crate::events::RunTrace::NoCarrier,
        trace_id: crate::telemetry::trace_carrier::StoredTraceId::NoTraceId,
    }
}

mod deadline_tests;
mod settle_tests;

const RETRY_ON_TIMEOUT: &str = r#"{"timeout_ms": 1000, "retry_policy": {"max_retries": 2,
    "intervals": [5, 5], "backoff_strategy": "fixed", "jitter": false,
    "max_delay_seconds": null, "auto_retry_for": ["TASK_TIMEOUT"]}}"#;

// ---------------------------------------------------------------------------
// Claim plan and routes
// ---------------------------------------------------------------------------

#[test]
fn a_worker_with_only_isolated_children_claims_every_class_into_isolated() {
    let plan = ClaimPlan::for_children(4, 0, 0);
    for class in ExecutionClass::ALL {
        assert_eq!(
            plan.route(class),
            ClaimRoute::ServedBy(ExecutionClass::Isolated)
        );
    }
    assert_eq!(plan.budgets(), vec![ExecutionClass::Isolated]);
    assert_eq!(
        plan.served_by(ExecutionClass::Isolated),
        ExecutionClass::ALL.to_vec()
    );
}

#[test]
fn a_shared_class_with_children_of_its_own_keeps_its_budget() {
    let plan = ClaimPlan::for_children(2, 1, 0);
    assert_eq!(
        plan.route(ExecutionClass::SharedAsync),
        ClaimRoute::ServedBy(ExecutionClass::SharedAsync)
    );
    assert_eq!(
        plan.route(ExecutionClass::SharedThread),
        ClaimRoute::ServedBy(ExecutionClass::Isolated)
    );
}

#[test]
fn a_worker_without_isolated_children_serves_no_other_class() {
    let plan = ClaimPlan::for_children(0, 1, 0);
    assert_eq!(plan.route(ExecutionClass::Isolated), ClaimRoute::NotServed);
    assert_eq!(
        plan.route(ExecutionClass::SharedThread),
        ClaimRoute::NotServed
    );
    assert_eq!(
        dispatch_route(
            ExecutionClass::SharedThread,
            plan,
            &options(r#"{"timeout_ms": 2000}"#)
        ),
        Err(UnservedClass {
            class: ExecutionClass::SharedThread
        })
    );
}

#[test]
fn an_isolated_row_runs_with_its_stored_deadline_or_none() {
    let plan = ClaimPlan::for_children(1, 0, 0);
    assert_eq!(
        dispatch_route(
            ExecutionClass::Isolated,
            plan,
            &options(r#"{"timeout_ms": 2000}"#)
        ),
        Ok(DispatchRoute::Isolated {
            work: IsolatedWork::Run,
            deadline: IsolatedDeadline::After(TaskTimeoutMs::new(2000).unwrap()),
        })
    );
    assert_eq!(
        dispatch_route(ExecutionClass::Isolated, plan, &options("{}")),
        Ok(DispatchRoute::Isolated {
            work: IsolatedWork::Run,
            deadline: IsolatedDeadline::NoDeadline,
        })
    );
    assert!(matches!(
        dispatch_route(ExecutionClass::Isolated, plan, &options("{")),
        Ok(DispatchRoute::Isolated {
            work: IsolatedWork::Refusal(StoredOptionsRefusal::OptionsUnreadable(_)),
            deadline: IsolatedDeadline::NoDeadline,
        })
    ));
    assert!(matches!(
        dispatch_route(
            ExecutionClass::Isolated,
            plan,
            &options(r#"{"timeout_ms": 5}"#)
        ),
        Ok(DispatchRoute::Isolated {
            work: IsolatedWork::Refusal(StoredOptionsRefusal::TimeoutValueInvalid(_)),
            ..
        })
    ));
}

#[test]
fn a_shared_async_row_needs_a_timeout_and_a_cancel_grace() {
    let plan = ClaimPlan::for_children(1, 1, 0);
    assert_eq!(
        dispatch_route(
            ExecutionClass::SharedAsync,
            plan,
            &options(r#"{"timeout_ms": 3000, "cancel_grace_ms": 500}"#)
        ),
        Ok(DispatchRoute::SharedAsync(SharedWork::AsyncTask {
            timeout_ms: TaskTimeoutMs::new(3000).unwrap(),
            cancel_grace_ms: CancelGraceMs::new(500).unwrap(),
        }))
    );
    match dispatch_route(
        ExecutionClass::SharedAsync,
        plan,
        &options(r#"{"timeout_ms": 3000}"#),
    ) {
        Ok(DispatchRoute::SharedAsync(SharedWork::Refusal(
            StoredOptionsRefusal::CancelGraceInvalid(reason),
        ))) => assert_eq!(
            reason.as_str(),
            "stored cancel_grace_ms is absent for execution SHARED_ASYNC"
        ),
        other => panic!("expected a grace refusal, got {other:?}"),
    }
    match dispatch_route(ExecutionClass::SharedAsync, plan, &options("{}")) {
        Ok(DispatchRoute::SharedAsync(SharedWork::Refusal(
            StoredOptionsRefusal::TimeoutValueInvalid(reason),
        ))) => assert_eq!(
            reason.as_str(),
            "stored timeout_ms is absent for execution SHARED_ASYNC"
        ),
        other => panic!("expected a timeout refusal, got {other:?}"),
    }
}

/// A shared row on the isolated budget gets the timeout or the refusal of
/// its seat work, so its outcome does not depend on the child kind.
#[test]
fn a_shared_row_on_the_isolated_budget_keeps_its_seat_rules() {
    let plan = ClaimPlan::for_children(1, 0, 0);
    assert_eq!(
        dispatch_route(
            ExecutionClass::SharedAsync,
            plan,
            &options(r#"{"timeout_ms": 3000, "cancel_grace_ms": 500}"#)
        ),
        Ok(DispatchRoute::Isolated {
            work: IsolatedWork::Run,
            deadline: IsolatedDeadline::After(TaskTimeoutMs::new(3000).unwrap()),
        })
    );
    assert!(matches!(
        dispatch_route(ExecutionClass::SharedThread, plan, &options("{}")),
        Ok(DispatchRoute::Isolated {
            work: IsolatedWork::Refusal(StoredOptionsRefusal::TimeoutValueInvalid(_)),
            deadline: IsolatedDeadline::NoDeadline,
        })
    ));
}

// ---------------------------------------------------------------------------
// Budgets
// ---------------------------------------------------------------------------

#[test]
fn capacity_counts_what_can_serve() {
    assert_eq!(
        isolated_capacity(&[
            SlotService::Booting,
            SlotService::InService,
            SlotService::Retiring,
            SlotService::Down,
        ]),
        3
    );
    let shared = |service, seats| SharedSlotService { service, seats };
    assert_eq!(
        shared_capacity(&[
            shared(SlotService::InService, 10),
            shared(SlotService::Booting, 10),
            shared(SlotService::Closing { runs: 3 }, 10),
            shared(SlotService::Down, 10),
        ]),
        23
    );
    let pool = PoolService {
        isolated: vec![SlotService::InService, SlotService::Down],
        shared_async: vec![shared(SlotService::InService, 50)],
        shared_thread: Vec::new(),
    };
    assert_eq!(
        pool.capacities(),
        vec![
            BudgetCapacity {
                budget: ExecutionClass::Isolated,
                in_service: 1
            },
            BudgetCapacity {
                budget: ExecutionClass::SharedAsync,
                in_service: 50
            },
        ]
    );
    assert_eq!(pool.total_capacity(), 51);
}

// ---------------------------------------------------------------------------
// The budgets and routes of a claim pass
// ---------------------------------------------------------------------------

fn claim_classes_of(
    budgets: &[(ExecutionClass, u32)],
    routes: &[(ExecutionClass, ExecutionClass)],
) -> crate::broker::claims::ClaimClasses {
    crate::broker::claims::ClaimClasses::new(
        budgets.iter().copied().collect(),
        routes.iter().copied().collect(),
    )
    .expect("every expected route names a held budget")
}

fn booting_shared(count: usize, seats: u32) -> Vec<SharedSlotService> {
    vec![
        SharedSlotService {
            service: SlotService::Booting,
            seats,
        };
        count
    ]
}

/// Python `test_claim_plan.py::TestEncodings::test_routes_name_only_served_classes`:
/// a worker of shared async children routes only its own class; a worker
/// of isolated children routes every class into the `ISOLATED` budget.
#[test]
fn the_routes_of_a_pass_name_only_the_served_classes() {
    let shared_async_only = PoolService {
        isolated: Vec::new(),
        shared_async: booting_shared(1, 10),
        shared_thread: Vec::new(),
    };
    assert_eq!(
        shared_async_only.claim_classes(),
        claim_classes_of(
            &[(ExecutionClass::SharedAsync, 10)],
            &[(ExecutionClass::SharedAsync, ExecutionClass::SharedAsync)],
        )
    );

    let isolated_only = PoolService {
        isolated: vec![SlotService::Booting; 2],
        shared_async: Vec::new(),
        shared_thread: Vec::new(),
    };
    assert_eq!(
        isolated_only.claim_classes(),
        claim_classes_of(
            &[(ExecutionClass::Isolated, 2)],
            &[
                (ExecutionClass::Isolated, ExecutionClass::Isolated),
                (ExecutionClass::SharedAsync, ExecutionClass::Isolated),
                (ExecutionClass::SharedThread, ExecutionClass::Isolated),
            ],
        )
    );
}

/// Python `test_claim_plan.py::TestEncodings::test_budgets_and_worker_state_capacity`
/// (the budgets): the budget of each class is its capacity in service.
#[test]
fn the_budgets_of_a_pass_are_the_capacity_in_service() {
    let pool = PoolService {
        isolated: vec![SlotService::InService; 3],
        shared_async: vec![SharedSlotService {
            service: SlotService::InService,
            seats: 40,
        }],
        shared_thread: Vec::new(),
    };
    assert_eq!(
        pool.claim_classes(),
        claim_classes_of(
            &[
                (ExecutionClass::Isolated, 3),
                (ExecutionClass::SharedAsync, 40)
            ],
            &[
                (ExecutionClass::Isolated, ExecutionClass::Isolated),
                (ExecutionClass::SharedAsync, ExecutionClass::SharedAsync),
                (ExecutionClass::SharedThread, ExecutionClass::Isolated),
            ],
        )
    );
}

/// A down isolated slot leaves the `ISOLATED` budget; before the boot every
/// seat counts. Every slot of a kind down leaves its budget at 0 and keeps
/// its route: the plan follows the children the worker runs, not their
/// state.
#[test]
fn a_down_slot_leaves_its_budget_and_keeps_the_routes() {
    let pool = PoolService {
        isolated: vec![SlotService::Down, SlotService::Booting],
        shared_async: Vec::new(),
        shared_thread: vec![SharedSlotService {
            service: SlotService::Down,
            seats: 4,
        }],
    };
    assert_eq!(
        pool.claim_classes(),
        claim_classes_of(
            &[
                (ExecutionClass::Isolated, 1),
                (ExecutionClass::SharedThread, 0)
            ],
            &[
                (ExecutionClass::Isolated, ExecutionClass::Isolated),
                (ExecutionClass::SharedAsync, ExecutionClass::Isolated),
                (ExecutionClass::SharedThread, ExecutionClass::SharedThread),
            ],
        )
    );
}

/// Python `test_claim_plan.py::TestRoutes::test_a_worker_without_children_serves_nothing`:
/// no budget and no route.
#[test]
fn a_worker_without_children_sends_no_budget_and_no_route() {
    assert_eq!(
        PoolService::default().claim_classes(),
        claim_classes_of(&[], &[])
    );
}

/// Python `test_claim_plan.py::TestEncodings::test_the_log_line_names_each_route`.
#[test]
fn the_log_line_names_each_route() {
    assert_eq!(
        describe_claim_plan(ClaimPlan::for_children(0, 1, 0)),
        "ISOLATED -> not served, SHARED_ASYNC -> SHARED_ASYNC budget, SHARED_THREAD -> not served"
    );
}

/// Python `test_claim_plan.py::TestStartLog::test_unserved_classes_get_one_line`
/// and `::test_a_worker_that_serves_every_class_logs_no_such_line` (the
/// lines; the core tests have no log capture, so the worker start that logs
/// them is checked by its source in `worker::worker::tests`).
#[test]
fn unserved_classes_get_one_line_and_a_full_plan_none() {
    assert_eq!(
        unserved_classes_line(ClaimPlan::for_children(0, 1, 0)).as_deref(),
        Some(
            "This worker claims no rows of class ISOLATED, SHARED_THREAD: it runs no children \
             that can run them"
        )
    );
    assert_eq!(
        unserved_classes_line(ClaimPlan::for_children(1, 0, 0)),
        None
    );
    assert!(describe_claim_plan(ClaimPlan::for_children(1, 0, 0))
        .starts_with("ISOLATED -> ISOLATED budget"));
}

// ---------------------------------------------------------------------------
// The writes of each end
// ---------------------------------------------------------------------------

#[test]
fn a_timeout_retries_when_the_policy_names_it() {
    let claimed = task(ExecutionClass::Isolated, RETRY_ON_TIMEOUT);
    match retry_request(&claimed, Some("TASK_TIMEOUT"), RetryPlacement::Isolated) {
        RetryRequest::RetryAfter {
            expected_retry_count,
            placement,
            delay_seconds,
        } => {
            assert_eq!(expected_retry_count, 0);
            assert_eq!(placement, RetryPlacement::Isolated);
            assert!(delay_seconds >= 1.0);
        }
        RetryRequest::NoRetry => panic!("the policy names TASK_TIMEOUT"),
    }
    assert_eq!(
        retry_request(&claimed, Some("WORKER_CRASHED"), RetryPlacement::SameClass),
        RetryRequest::NoRetry
    );
    let spent = ClaimedTask {
        retry: RetryFacts {
            retry_count: 2,
            max_retries: 2,
        },
        ..claimed
    };
    assert_eq!(
        retry_request(&spent, Some("TASK_TIMEOUT"), RetryPlacement::SameClass),
        RetryRequest::NoRetry
    );
}

/// One retry of `TASK_TIMEOUT` after 30 s.
const ONE_TIMEOUT_RETRY_AFTER_30_S: &str = r#"{"retry_policy": {"max_retries": 1,
    "intervals": [30], "backoff_strategy": "fixed", "jitter": false,
    "max_delay_seconds": null, "auto_retry_for": ["TASK_TIMEOUT"]}}"#;

/// The command of a retry after a timeout (the order is
/// `exit_not_seen_tests::a_deadline_kill_gives_no_end_before_the_exit_is_seen`):
/// the timeout write of a task whose policy lists `TASK_TIMEOUT` is a
/// `TASK_TIMEOUT` failure that asks for the retry: the retry count of the
/// claim, the delay of the policy, in the class of the end.
#[test]
fn a_timeout_write_asks_for_the_retry_of_its_policy() {
    let claimed = ClaimedTask {
        retry: RetryFacts {
            retry_count: 0,
            max_retries: 1,
        },
        ..task(ExecutionClass::Isolated, ONE_TIMEOUT_RETRY_AFTER_30_S)
    };

    let write = timeout_write(
        &claimed,
        "worker-1",
        TaskTimeoutMs::new(1_000).unwrap(),
        RetryPlacement::SameClass,
        TimeoutCancel::NoSignal,
    );

    assert_eq!(write.error_code.as_deref(), Some("TASK_TIMEOUT"));
    assert_eq!(
        write.retry,
        RetryRequest::RetryAfter {
            expected_retry_count: 0,
            delay_seconds: 30.0,
            placement: RetryPlacement::SameClass,
        }
    );
}

#[test]
fn a_killed_child_write_names_its_code() {
    let claimed = task(ExecutionClass::Isolated, RETRY_ON_TIMEOUT);
    let write = killed_child_write(
        &claimed,
        "worker-1",
        OutcomeCode::TaskTimeout.into(),
        "task exceeded timeout_ms=1000".to_owned(),
        RetryPlacement::SameClass,
    );
    assert_eq!(write.error_code.as_deref(), Some("TASK_TIMEOUT"));
    assert_eq!(write.attempt.kind, AttemptFailureKind::Failed);
    assert_eq!(write.fence.claimed_at, claimed.claimed_at);
    assert!(matches!(write.retry, RetryRequest::RetryAfter { .. }));
    let loop_write = killed_child_write(
        &claimed,
        "worker-1",
        LoopKillCode::NeighbourBlockedLoop.code().into(),
        "blocked".to_owned(),
        RetryPlacement::SameClass,
    );
    assert_eq!(
        loop_write.error_code.as_deref(),
        Some("NEIGHBOUR_BLOCKED_LOOP")
    );
    assert_eq!(loop_write.retry, RetryRequest::NoRetry);
}

/// Python `_recover_worker_future_failure`: the write of a child that the
/// worker killed or lost stores the task and the worker in the error
/// `data`, for each code it writes: a crash, each loop kill, and crash
/// recovery after the failed `TASK_TIMEOUT` writes.
#[test]
fn a_killed_child_write_stores_the_task_and_the_worker() {
    let claimed = task(ExecutionClass::SharedAsync, RETRY_ON_TIMEOUT);
    let expected = serde_json::json!({"task_id": TASK, "worker_id": "worker-1"});
    for code in [
        OperationalErrorCode::WorkerCrashed,
        LoopKillCode::TaskBlockedLoop.code(),
        LoopKillCode::NeighbourBlockedLoop.code(),
    ] {
        let code_text = code.to_string();
        let write = killed_child_write(
            &claimed,
            "worker-1",
            code.into(),
            "the child ended".to_owned(),
            RetryPlacement::SameClass,
        );
        assert_eq!(write.error_code.as_deref(), Some(code_text.as_str()));
        assert_eq!(
            stored_error_data(&write.result_json),
            expected,
            "{code_text}"
        );
    }
    let recovery = timeout_recovery_writes(
        &claimed,
        "worker-1",
        TaskTimeoutMs::new(1000).unwrap(),
        RetryPlacement::Isolated,
    )
    .first;
    assert_eq!(recovery.error_code.as_deref(), Some("TASK_TIMEOUT"));
    assert_eq!(stored_error_data(&recovery.result_json), expected);
}

/// Python `_max_retries_of`: the timeout write, crash recovery and phase 1
/// are each sent again at most 3 times.
#[test]
fn each_write_stage_keeps_the_retry_bound_of_python() {
    assert_eq!(WriteStage::PhaseOne.max_retries(), 3);
    assert_eq!(WriteStage::TimeoutWrite.max_retries(), 3);
    assert_eq!(WriteStage::CrashRecovery.max_retries(), 3);
}

/// The `data` of the error that a failed-attempt write stores.
fn stored_error_data(result_json: &str) -> serde_json::Value {
    let stored: serde_json::Value =
        serde_json::from_str(result_json).expect("a failed-attempt write stores a JSON envelope");
    stored["err"]["data"].clone()
}

/// Python `_settle_timed_out_row`: the data of a `TASK_TIMEOUT` error names
/// the task and its `timeout_ms`; a shared async run also names how it ended
/// after its cancel signal (`cancel_end`), also at a kill of its child. An
/// isolated or a shared thread run has no `cancel_end`.
#[test]
fn the_timeout_data_names_how_a_shared_async_run_ended() {
    let claimed = task(ExecutionClass::SharedAsync, RETRY_ON_TIMEOUT);
    let timeout = TaskTimeoutMs::new(1000).unwrap();
    let task_id = claimed.task_id.to_string();
    for (cancel, cancel_end) in [
        (
            TimeoutCancel::Ended(CancelEnd::ReturnedAfterSignal),
            Some("RETURNED_AFTER_SIGNAL"),
        ),
        (
            TimeoutCancel::Ended(CancelEnd::DroppedAfterGrace),
            Some("DROPPED_AFTER_GRACE"),
        ),
        (
            TimeoutCancel::Ended(CancelEnd::KilledWithChild),
            Some("KILLED_WITH_CHILD"),
        ),
        (TimeoutCancel::NoSignal, None),
    ] {
        let write = timeout_write(
            &claimed,
            "worker-1",
            timeout,
            RetryPlacement::SameClass,
            cancel,
        );
        let mut expected = serde_json::json!({"task_id": task_id, "timeout_ms": 1000});
        if let Some(end) = cancel_end {
            expected["cancel_end"] = serde_json::Value::from(end);
        }
        assert_eq!(
            stored_error_data(&write.result_json),
            expected,
            "{cancel:?}"
        );
        assert_eq!(
            write.error_code.as_deref(),
            Some("TASK_TIMEOUT"),
            "{cancel:?}"
        );
    }
}

/// Python `_recover_timed_out_row`: after the last retry of a TASK_TIMEOUT
/// write, crash recovery writes TASK_TIMEOUT with the kept placement, and
/// the policy decides the retry on that code.
#[test]
fn crash_recovery_of_a_timeout_keeps_its_code_and_placement() {
    let claimed = task(ExecutionClass::SharedAsync, RETRY_ON_TIMEOUT);
    let timeout = TaskTimeoutMs::new(1000).unwrap();
    let first = timeout_write(
        &claimed,
        "worker-1",
        timeout,
        RetryPlacement::Isolated,
        TimeoutCancel::Ended(CancelEnd::ReturnedAfterSignal),
    );
    let recovery =
        timeout_recovery_writes(&claimed, "worker-1", timeout, RetryPlacement::Isolated).first;
    for write in [&first, &recovery] {
        assert_eq!(write.error_code.as_deref(), Some("TASK_TIMEOUT"));
        assert_eq!(write.fence.claimed_at, claimed.claimed_at);
        match write.retry {
            RetryRequest::RetryAfter { placement, .. } => {
                assert_eq!(placement, RetryPlacement::Isolated);
            }
            RetryRequest::NoRetry => panic!("the policy names TASK_TIMEOUT"),
        }
    }
    assert_eq!(
        first.attempt.error_message.as_deref(),
        Some("Task exceeded timeout_ms=1000")
    );
    assert_eq!(
        recovery.attempt.error_message.as_deref(),
        Some("Task reached timeout_ms=1000, and the TASK_TIMEOUT write failed 4 times")
    );
    let spent = ClaimedTask {
        retry: RetryFacts {
            retry_count: 2,
            max_retries: 2,
        },
        ..claimed
    };
    assert_eq!(
        timeout_recovery_writes(&spent, "worker-1", timeout, RetryPlacement::Isolated)
            .first
            .retry,
        RetryRequest::NoRetry
    );
}

/// The `err.message` of the error envelope that a failed-attempt write
/// stores.
fn stored_error_message(result_json: &str) -> serde_json::Value {
    let stored: serde_json::Value =
        serde_json::from_str(result_json).expect("a failed-attempt write stores a JSON envelope");
    stored["err"]["message"].clone()
}

/// A crash-recovery write sent again after a database error stores
/// `Retry after future-stage finalize error: ` and the message of the first
/// send, as the error message and as the attempt's message and failed
/// reason. The code, the retry, the fence and the error `data` are those of
/// the first send. Each crash-recovery write has this form: a lost child, a
/// loop kill, and crash recovery after the failed `TASK_TIMEOUT` writes.
#[test]
fn a_crash_recovery_write_sent_again_names_the_retry() {
    let claimed = task(ExecutionClass::SharedAsync, RETRY_ON_TIMEOUT);
    let lost = "Worker child lost: child died: exit code 9";
    let crash = crash_recovery_writes(
        &claimed,
        "worker-1",
        OperationalErrorCode::WorkerCrashed.into(),
        lost.to_owned(),
        RetryPlacement::SameClass,
    );
    let loop_kill = crash_recovery_writes(
        &claimed,
        "worker-1",
        LoopKillCode::TaskBlockedLoop.code().into(),
        "blocked".to_owned(),
        RetryPlacement::Isolated,
    );
    let timeout = timeout_recovery_writes(
        &claimed,
        "worker-1",
        TaskTimeoutMs::new(1000).unwrap(),
        RetryPlacement::Isolated,
    );
    let expected = [
        (
            &crash,
            lost,
            "Retry after future-stage finalize error: Worker child lost: child died: exit code 9",
        ),
        (
            &loop_kill,
            "blocked",
            "Retry after future-stage finalize error: blocked",
        ),
        (
            &timeout,
            "Task reached timeout_ms=1000, and the TASK_TIMEOUT write failed 4 times",
            "Retry after future-stage finalize error: Task reached timeout_ms=1000, and the \
             TASK_TIMEOUT write failed 4 times",
        ),
    ];

    for (writes, first_text, retry_text) in expected {
        let (first, retry) = (&writes.first, &writes.retry);
        assert_eq!(first.attempt.error_message.as_deref(), Some(first_text));
        assert_eq!(stored_error_message(&first.result_json), first_text);
        assert_eq!(retry.attempt.error_message.as_deref(), Some(retry_text));
        assert_eq!(retry.attempt.failed_reason.as_deref(), Some(retry_text));
        assert_eq!(stored_error_message(&retry.result_json), retry_text);
        assert_eq!(retry.error_code, first.error_code, "{retry_text}");
        assert_eq!(retry.retry, first.retry, "{retry_text}");
        assert_eq!(retry.fence, first.fence, "{retry_text}");
        assert_eq!(retry.failed_reason, None, "{retry_text}");
        assert_eq!(
            stored_error_data(&retry.result_json),
            stored_error_data(&first.result_json),
            "{retry_text}"
        );
    }
}

#[test]
fn a_worker_failure_is_a_broker_error_that_never_retries() {
    let claimed = task(ExecutionClass::Isolated, RETRY_ON_TIMEOUT);
    let write = worker_failure_write(&claimed, "worker-1", "");
    assert_eq!(write.error_code.as_deref(), Some("BROKER_ERROR"));
    assert_eq!(write.retry, RetryRequest::NoRetry);
    assert_eq!(write.attempt.kind, AttemptFailureKind::WorkerFailure);
    assert_eq!(write.failed_reason.as_deref(), Some("Worker failure"));
}

#[test]
fn a_reported_task_error_keeps_its_envelope_and_retries_by_policy() {
    let claimed = task(ExecutionClass::Isolated, RETRY_ON_TIMEOUT);
    let error = TaskError::builtin(OperationalErrorCode::TaskException, "boom");
    let envelope = ResultEnvelopeText::new(
        serde_json::to_string(
            &crate::core::task::result::TaskResult::<serde_json::Value>::Err(error.clone()),
        )
        .unwrap(),
    );
    let write = task_error_write(&claimed, "worker-1", &error, &envelope);
    assert_eq!(write.result_json, envelope.as_str());
    assert_eq!(write.error_code.as_deref(), Some("TASK_EXCEPTION"));
    assert_eq!(write.retry, RetryRequest::NoRetry);
}

#[test]
fn every_task_gets_exactly_one_answer() {
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    drop(Answer::new(sender));
    assert_eq!(
        receiver.try_recv().unwrap(),
        AttemptEnd::NotDispatched(NotRunCause::PoolStopping)
    );
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    let mut answer = Answer::new(sender);
    answer.child_took_it(ProcessId::new(77).unwrap());
    drop(answer);
    assert_eq!(
        receiver.try_recv().unwrap(),
        AttemptEnd::EndUnknown {
            pid: ProcessId::new(77).unwrap()
        }
    );
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    Answer::new(sender).send(AttemptEnd::NotDispatched(NotRunCause::QueueFull));
    assert_eq!(
        receiver.try_recv().unwrap(),
        AttemptEnd::NotDispatched(NotRunCause::QueueFull)
    );
}

// ---------------------------------------------------------------------------
// Isolated slots with scripted children
// ---------------------------------------------------------------------------

fn init_frame() -> InitFrame {
    InitFrame {
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        connection: ChildConnection::default(),
        process_name: "horsies-test-child".to_owned(),
        child_kind: ChildKind::Isolated(IsolatedChild {
            pool: ChildPool {
                min_size: PoolMinSize::new(0).unwrap(),
                max_size: PoolMaxSize::new(1).unwrap(),
                check: true,
            },
            task_send_pool: TaskSendPool::new(1).unwrap(),
        }),
    }
}

/// `printf` text that writes `bytes`: one octal escape per byte.
fn printf_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("\\{byte:03o}")).collect()
}

fn run_frame_bytes(claimed: &ClaimedTask) -> Vec<u8> {
    encode_parent_frame(&ParentFrame::Run(RunFrame {
        task_id: claimed.task_id,
        task_name: claimed.task_name.clone(),
        worker_id: "worker-1".to_owned(),
        claimed_at: claimed.claimed_at,
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        work: IsolatedWork::Run,
        input: TaskInputText::new("{}".to_owned()),
        trace: crate::events::RunTrace::NoCarrier,
    }))
    .unwrap()
    .to_bytes()
}

/// A child script: take the init frame, send ready, take the run frame,
/// then do `after_run`.
fn scripted_child(claimed: &ClaimedTask, after_run: &str) -> ChildCommand {
    let ready = encode_child_frame(&ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    }))
    .unwrap()
    .to_bytes();
    let script = format!(
        r#"{init}printf '{ready}' >&"$0"; {run}{after_run}"#,
        init = take_init(),
        ready = printf_bytes(&ready),
        run = drop_bytes(ScriptCount::Known(run_frame_bytes(claimed).len())),
    );
    ChildCommand {
        program: crate::test_support::SCRIPT_SHELL.into(),
        args: vec!["-c".into(), script.into()],
    }
}

fn pool_config(command: ChildCommand) -> IsolatedPoolConfig {
    IsolatedPoolConfig {
        command,
        init: init_frame(),
        worker_id: "worker-1".to_owned(),
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        pending_limit: NonZeroUsize::new(4).unwrap(),
        tasks_per_child: TaskLimit::Off,
        memory_per_child: MemoryLimit::Off,
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
        child_cpus: ChildCpus::Inherited,
        kill_wait: CHILD_KILL_WAIT,
    }
}

async fn run_through_pool(after_run: &str, deadline: IsolatedDeadline) -> AttemptEnd {
    let claimed = task(ExecutionClass::Isolated, "{}");
    let spawner = Spawner::start().unwrap();
    let board = std::sync::Arc::new(PoolBoard::new(ChildLayout {
        isolated: 1,
        shared_async: 0,
        shared_thread: 0,
    }));
    let pool = IsolatedPool::start(
        pool_config(scripted_child(&claimed, after_run)),
        board,
        spawner.handle(),
        LiveDispatches::default(),
    );
    let receiver = match pool.submit(
        &claimed,
        TaskInputText::new("{}".to_owned()),
        IsolatedWork::Run,
        deadline,
    ) {
        Submitted::Queued(receiver) => receiver,
        Submitted::NotDispatched(cause) => panic!("not dispatched: {cause}"),
    };
    let end = tokio::time::timeout(Duration::from_secs(30), receiver)
        .await
        .unwrap()
        .unwrap();
    let _ = pool.stop(Duration::from_secs(5)).await;
    let _ = spawner.stop(Duration::from_secs(10));
    end
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_slot_reports_the_result_of_its_child() {
    let claimed = task(ExecutionClass::Isolated, "{}");
    let result = encode_child_frame(&ChildFrame::Result(ResultFrame {
        task_id: claimed.task_id,
        end: RunEnd::Result(ResultEnvelopeText::new(
            r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
        )),
        started_at: crate::events::AttemptStart::Reported {
            started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        },
        rss: RssReading::Unreadable,
        task_loop: NoLeftWork::NoTaskLoop,
        child: ChildAfterFrame::ServesOn,
    }))
    .unwrap()
    .to_bytes();
    let after_run = format!(
        r#"printf '{}' >&"$0"; {}"#,
        printf_bytes(&result),
        exit_at_stop(),
    );
    assert_eq!(
        run_through_pool(&after_run, IsolatedDeadline::NoDeadline).await,
        AttemptEnd::Reported(RunEnd::Result(ResultEnvelopeText::new(
            r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned()
        )))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_kills_the_child_and_ends_the_task_as_timed_out() {
    let timeout = TaskTimeoutMs::new(1000).unwrap();
    assert_eq!(
        run_through_pool("exec sleep 60", IsolatedDeadline::After(timeout)).await,
        AttemptEnd::TimedOut {
            timeout,
            retry: RetryPlacement::SameClass,
            cancel: TimeoutCancel::NoSignal,
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_dies_with_a_task_takes_the_crash_path() {
    match run_through_pool("exit 3", IsolatedDeadline::NoDeadline).await {
        AttemptEnd::Crashed { reason } => {
            assert!(reason.as_str().contains("exit code 3"), "{reason}");
        }
        other => panic!("expected the crash path, got {other:?}"),
    }
}

/// The free slots of an isolated pool, for the soft-cap start check: a ready child that waits for
/// a task is one free slot; a submitted task takes it at once, before the
/// slot takes the task from the queue; a stopping pool has none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_child_is_a_free_slot_until_a_task_takes_it() {
    let claimed = task(ExecutionClass::Isolated, "{}");
    let spawner = Spawner::start().unwrap();
    let board = std::sync::Arc::new(PoolBoard::new(ChildLayout {
        isolated: 1,
        shared_async: 0,
        shared_thread: 0,
    }));
    let live = LiveDispatches::default();
    let pool = IsolatedPool::start(
        pool_config(scripted_child(&claimed, "exec sleep 60")),
        std::sync::Arc::clone(&board),
        spawner.handle(),
        live.clone(),
    );
    super::scripted::until(
        "the child waits for a task",
        Duration::from_secs(30),
        || pool.free_slots() == 1,
    )
    .await;

    let submitted = pool.submit(
        &claimed,
        TaskInputText::new("{}".to_owned()),
        IsolatedWork::Run,
        IsolatedDeadline::NoDeadline,
    );
    assert!(matches!(submitted, Submitted::Queued(_)));
    assert_eq!(
        pool.free_slots(),
        0,
        "the submitted task holds the idle slot"
    );
    super::scripted::until("the child holds the task", Duration::from_secs(30), || {
        live.held_task_ids() == vec![claimed.task_id]
    })
    .await;
    assert_eq!(pool.free_slots(), 0, "the busy child is no free slot");

    board.begin_stop();
    assert_eq!(pool.free_slots(), 0, "a stopping pool has no free slot");
    let _ = pool.stop(Duration::from_secs(1)).await;
    let _ = spawner.stop(Duration::from_secs(10));
}
