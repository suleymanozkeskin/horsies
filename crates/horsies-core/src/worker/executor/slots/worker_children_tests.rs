//! The pools of a worker from its profile, with scripted children: the
//! budgets of a claim pass from the board, and the route of each claimed
//! row to the pool of its kind (Python
//! `tests/unit/test_claim_plan.py::TestClaimPassWithClasses::test_the_pass_sends_the_budgets_and_the_routes`).

use super::scripted::{
    act_on_runs, boot_any_kind, idle_child_of_any_kind, ok_envelope, result_frame, run_ref, send,
    shared_result, shell, task, ANSWER_WAIT, SPAWNER_STOP_WAIT, STATE_WAIT, WORKER_ID,
};
use super::*;
use crate::core::config::worker_profile::WorkerProfile;
use crate::worker::children::{ChildLaunch, PoolFacts, WorkerChildren};
use crate::worker::executor::placement::CpuPlacement;
use crate::worker::executor::protocol::{
    ChildConnection, ChildLogLevel, ChildProgram, DatabaseUrl, RunEnd, TaskInputText,
};
use crate::worker::executor::supervisor::{ChildCommand, Spawner};
use crate::worker::retry::read_stored_options;
use crate::worker::runner_beats::LiveDispatches;

fn launch(command: ChildCommand) -> ChildLaunch {
    ChildLaunch {
        command,
        preload: crate::worker::children::PreloadProgram::NoPreloadProgram,
        program: ChildProgram::Rust,
        log_level: ChildLogLevel::Info,
        database_url: DatabaseUrl::new("postgresql://u@h/db".to_owned()),
        connection: ChildConnection::default(),
    }
}

fn facts() -> PoolFacts {
    PoolFacts {
        worker_id: WORKER_ID.to_owned(),
        orphan_self_heal: crate::broker::OrphanSelfHeal::Off,
        pending_limit: std::num::NonZeroUsize::new(16).unwrap(),
        exit_not_seen: ExitNotSeen::LeaveToRecovery,
        placement: CpuPlacement::SharedCpus,
    }
}

fn profile(document: serde_json::Value) -> WorkerProfile {
    serde_json::from_value(document).expect("a valid test profile")
}

/// Python `TestClaimPassWithClasses::test_the_pass_sends_the_budgets_and_the_routes`,
/// through the board: a worker of one isolated child sends the budget
/// `{ISOLATED: 1}` and routes every class into it, before and after its
/// child is ready.
#[tokio::test]
async fn the_budgets_and_routes_of_a_pass_come_from_the_board() {
    let spawner = Spawner::start().unwrap();
    let profile = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 1}
    }));
    let children = WorkerChildren::start(
        &profile,
        &launch(idle_child_of_any_kind()),
        &facts(),
        &spawner.handle(),
        &LiveDispatches::default(),
    );
    let expected = crate::broker::claims::ClaimClasses::new(
        [(ExecutionClass::Isolated, 1)].into_iter().collect(),
        ExecutionClass::ALL
            .into_iter()
            .map(|class| (class, ExecutionClass::Isolated))
            .collect(),
    )
    .unwrap();

    assert_eq!(children.service().claim_classes(), expected);
    super::scripted::serving(children.board(), 0).await;
    assert_eq!(children.service().claim_classes(), expected);
    assert!(children.down_slots().is_empty());

    let _ = children.stop(STATE_WAIT).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// Each row goes to the pool of
/// the budget that claimed it, and its end comes back from that pool. One
/// child of each kind answers only the task of its kind.
#[tokio::test]
async fn each_route_goes_to_the_pool_of_its_kind() {
    let spawner = Spawner::start().unwrap();
    let profile = profile(serde_json::json!({
        "isolated": {"kind": "isolated_children", "count": 1},
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 2},
        "shared_thread": {"kind": "shared_thread_children", "count": 1, "seats": 2}
    }));
    let isolated = task(1, ExecutionClass::Isolated);
    let shared_async = ClaimedTask {
        options: read_stored_options(Some(r#"{"timeout_ms": 5000, "cancel_grace_ms": 500}"#)),
        ..task(2, ExecutionClass::SharedAsync)
    };
    let shared_thread = ClaimedTask {
        options: read_stored_options(Some(r#"{"timeout_ms": 5000}"#)),
        ..task(3, ExecutionClass::SharedThread)
    };
    let answers = [
        (&isolated, send(&[result_frame(&isolated, 1)])),
        (
            &shared_async,
            send(&[shared_result(run_ref(&shared_async, 1), 2)]),
        ),
        (
            &shared_thread,
            send(&[shared_result(run_ref(&shared_thread, 1), 3)]),
        ),
    ];
    let script = format!("{}{}", boot_any_kind(), act_on_runs(&answers, 1));
    let children = WorkerChildren::start(
        &profile,
        &launch(shell(&script)),
        &facts(),
        &spawner.handle(),
        &LiveDispatches::default(),
    );

    for (task, value) in [(&isolated, 1), (&shared_async, 2), (&shared_thread, 3)] {
        let submitted = children.submit(Claimed {
            task: task.clone(),
            input: TaskInputText::new("{}".to_owned()),
        });
        let end =
            tokio::time::timeout(ANSWER_WAIT, crate::worker::children::attempt_end(submitted))
                .await
                .expect("the end of the task comes within the bound");
        assert_eq!(
            end,
            AttemptEnd::Reported(RunEnd::Result(ok_envelope(value))),
            "{}",
            task.execution
        );
    }

    let _ = children.stop(STATE_WAIT).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}

/// A row of a class that the worker does not serve never reaches a child:
/// a worker of shared async children only takes no `ISOLATED` row.
#[tokio::test]
async fn a_row_of_an_unserved_class_never_reaches_a_child() {
    let spawner = Spawner::start().unwrap();
    let profile = profile(serde_json::json!({
        "shared_async": {"kind": "shared_async_children", "count": 1, "seats": 2}
    }));
    let children = WorkerChildren::start(
        &profile,
        &launch(idle_child_of_any_kind()),
        &facts(),
        &spawner.handle(),
        &LiveDispatches::default(),
    );

    let submitted = children.submit(Claimed {
        task: task(4, ExecutionClass::Isolated),
        input: TaskInputText::new("{}".to_owned()),
    });

    assert_eq!(
        crate::worker::children::attempt_end(submitted).await,
        AttemptEnd::NotDispatched(NotRunCause::NoChildOfItsKind)
    );
    let _ = children.stop(STATE_WAIT).await;
    let _ = spawner.stop(SPAWNER_STOP_WAIT);
}
