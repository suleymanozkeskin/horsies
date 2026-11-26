//! The deadline of an isolated task and the run frame it gets, with
//! scripted children (Python `clients/python/tests/unit/test_worker_helpers.py`,
//! `TestAwaitSlotOutcome`, `TestDispatchOne`, `TestFinalizeAfterSlotOutcome`).
//!
//! - The deadline starts when the run frame is sent, not when the task is
//!   submitted: a task that waits for its slot does not use its time.
//! - A result inside the deadline is the end of the task.
//! - The run frame carries the claim fields of the task.
//! - A lost child gives the crash path, and its reason names the exit.

use super::*;
use crate::worker::executor::slots::scripted::read_bytes;

/// The child script: wait `before_boot`, take the init frame, send ready,
/// then do `after_ready` (which reads the run frame itself).
fn child_script(before_boot: &str, after_ready: &str) -> ChildCommand {
    let ready = encode_child_frame(&ChildFrame::Ready(ReadyFrame {
        pid: ProcessId::new(1).unwrap(),
        rss: RssReading::Unreadable,
    }))
    .unwrap()
    .to_bytes();
    let script = format!(
        r#"{before_boot} {init}printf '{ready}' >&"$0"; {after_ready}"#,
        init = take_init(),
        ready = printf_bytes(&ready),
    );
    ChildCommand {
        program: crate::test_support::SCRIPT_SHELL.into(),
        args: vec!["-c".into(), script.into()],
    }
}

/// The script text that sends an ok result for `claimed`, then waits for
/// the stop frame.
fn answer_ok(claimed: &ClaimedTask) -> String {
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
    format!(
        r#"printf '{}' >&"$0"; {}"#,
        printf_bytes(&result),
        exit_at_stop(),
    )
}

/// The end of [`answer_ok`]: its result, with the start of its frame.
fn ok_end() -> AttemptEnd {
    AttemptEnd::Result {
        envelope: ResultEnvelopeText::new(
            r#"{"__h_task_result__":true,"ok":3,"err":null}"#.to_owned(),
        ),
        start: crate::events::AttemptStart::Reported {
            started_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
        },
    }
}

/// Submit `claimed` to one isolated slot whose child runs `command`, and
/// return its end.
async fn end_of(
    claimed: &ClaimedTask,
    command: ChildCommand,
    deadline: IsolatedDeadline,
) -> AttemptEnd {
    let spawner = Spawner::start().unwrap();
    let board = std::sync::Arc::new(PoolBoard::new(ChildLayout {
        isolated: 1,
        shared_async: 0,
        shared_thread: 0,
    }));
    let pool = IsolatedPool::start(
        pool_config(command),
        board,
        spawner.handle(),
        LiveDispatches::default(),
    );
    let receiver = match pool.submit(
        claimed,
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

/// Python `TestAwaitSlotOutcome::test_timeout_does_not_run_while_the_task_waits_for_a_slot`:
/// the child takes 1.5 s to boot and the deadline is 1 s; the task waits
/// for its slot, then runs inside its deadline and reports its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_deadline_starts_when_the_child_takes_the_task() {
    let claimed = task(ExecutionClass::Isolated, "{}");
    let after_ready = format!(
        r#"{run}{answer}"#,
        run = drop_bytes(ScriptCount::Known(run_frame_bytes(&claimed).len())),
        answer = answer_ok(&claimed),
    );
    let deadline = IsolatedDeadline::After(TaskTimeoutMs::new(1_000).unwrap());
    assert_eq!(
        end_of(&claimed, child_script("sleep 1.5;", &after_ready), deadline).await,
        ok_end()
    );
}

/// Python `TestAwaitSlotOutcome::test_a_result_inside_the_limit_needs_no_timeout_handler`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_inside_the_deadline_is_the_end_of_the_task() {
    let claimed = task(ExecutionClass::Isolated, "{}");
    let deadline = IsolatedDeadline::After(TaskTimeoutMs::new(10_000).unwrap());
    assert_eq!(
        end_of(
            &claimed,
            scripted_child(&claimed, &answer_ok(&claimed)),
            deadline
        )
        .await,
        ok_end()
    );
}

/// Script text that reads the run frame, ends with exit code 9 when its
/// bytes are not `expected`, and answers ok when they are.
fn compare_run_frame(claimed: &ClaimedTask, expected: &[u8]) -> String {
    let hex: String = expected.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        r#"received=$({run} | od -An -v -tx1 | tr -d ' \n'); [ "$received" = "{hex}" ] || exit 9; {answer}"#,
        run = read_bytes(ScriptCount::Known(expected.len())),
        answer = answer_ok(claimed),
    )
}

/// Python `TestDispatchOne::test_frame_carries_the_claim_fields`: the child
/// gets the task id, name, worker id, kind, claim generation and input of
/// the claimed task. A frame built for another claim generation does not
/// match, so the comparison can fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_run_frame_carries_the_claim_fields() {
    let claimed = ClaimedTask {
        kind: TaskKind::WorkflowTask,
        ..task(ExecutionClass::Isolated, "{}")
    };
    let expected = run_frame_bytes(&claimed);
    assert_eq!(
        end_of(
            &claimed,
            child_script("", &compare_run_frame(&claimed, &expected)),
            IsolatedDeadline::NoDeadline,
        )
        .await,
        ok_end()
    );

    let other_generation = ClaimedTask {
        claimed_at: claimed.claimed_at + chrono::Duration::seconds(1),
        ..claimed.clone()
    };
    let not_sent = run_frame_bytes(&other_generation);
    match end_of(
        &claimed,
        child_script("", &compare_run_frame(&claimed, &not_sent)),
        IsolatedDeadline::NoDeadline,
    )
    .await
    {
        AttemptEnd::Crashed { reason } => {
            assert!(reason.as_str().contains("exit code 9"), "{reason}");
        }
        other => panic!("expected the frame of another generation to differ, got {other:?}"),
    }
}

/// Python `TestFinalizeAfterSlotOutcome::test_lost_child_reason_names_the_exit`
/// and `_finalize_child_loss`: a child that dies by `SIGKILL` with a task
/// gives the crash path, its reason starts with `Worker child lost: ` and
/// names the signal, and the write carries that reason as the attempt's
/// message and failed reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_child_names_its_exit_in_the_crash_reason() {
    let claimed = task(ExecutionClass::Isolated, "{}");
    let reason = match end_of(
        &claimed,
        scripted_child(&claimed, "kill -9 $$"),
        IsolatedDeadline::NoDeadline,
    )
    .await
    {
        AttemptEnd::Crashed { reason } => reason,
        other => panic!("expected the crash path, got {other:?}"),
    };
    assert!(reason.as_str().contains("signal 9 (SIGKILL)"), "{reason}");
    assert!(
        reason
            .as_str()
            .starts_with(crate::worker::executor::slots::dispatch::LOST_CHILD_REASON_PREFIX),
        "{reason}"
    );

    let write = killed_child_write(
        &claimed,
        "worker-1",
        OperationalCode::WorkerCrashed.into(),
        reason.as_str().to_owned(),
        RetryPlacement::SameClass,
        crate::core::lifecycle::AttemptPlacement::NotObserved,
    );
    assert_eq!(write.error_code.as_deref(), Some("WORKER_CRASHED"));
    assert_eq!(
        write.attempt.error_message.as_deref(),
        Some(reason.as_str())
    );
    assert_eq!(
        write.attempt.failed_reason.as_deref(),
        Some(reason.as_str())
    );
    assert!(write.result_json.contains("signal 9 (SIGKILL)"));
}
