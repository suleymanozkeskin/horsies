//! The stack margin of the deepest workflow paths in a debug build.
//!
//! A libtest thread and a tokio worker thread have a 2 MiB stack. An `async
//! fn` holds each future that it awaits inline in its state, and in a debug
//! build its poll function also reserves stack for each future that it
//! builds, so the nested workflow steps need a large part of that stack. The
//! engine awaits its steps on the heap (module docs of
//! `workflow_engine::steps`).
//!
//! [`the_deepest_workflow_paths_run_on_a_stack_of_one_and_a_half_mib`] runs
//! the deepest paths on a thread of [`STACK_MARGIN_THREAD_BYTES`]: each path
//! keeps 512 KiB of the 2 MiB. A stack overflow aborts the whole process, so
//! the paths run in a child process of the test binary
//! ([`stack_margin_probe`]); an overflow fails the named test, and the other
//! tests of the binary go on. The probe writes the size of the future of each
//! operation and the operation that it starts to stderr; the test repeats
//! those lines.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    child_of, child_specs_of, err, node_status, run_node_task, seed_node, seed_workflow, spec,
    start, sub_workflow_node, task_node, worker_phase_one, workflow_status, IsolatedDatabase,
    NodeReport, NodeSeed, RunningNodeTask, WorkflowSeed,
};
use crate::broker::PostgresBroker;
use crate::core::WorkflowSpec;
use crate::monitoring::{
    cancel_workflow_action, pause_workflow_action, resume_workflow_action, ActionBody,
    ActionOutcome, ActionResponse, ResumeChildren,
};
use crate::test_support::child_specs::TestChildSpecs;
use crate::worker::execution::{run_phase2, Phase2Work};
use crate::workflow_engine::recovery::recover_stuck_workflows_observed_with;
use crate::workflow_engine::steps::ChildBuild;

/// The stack of the thread that runs the paths: 1.5 MiB, 512 KiB under the
/// 2 MiB of a libtest thread and of a tokio worker thread.
const STACK_MARGIN_THREAD_BYTES: usize = 1536 * 1024;

/// The variable that makes [`stack_margin_probe`] run the paths.
const PROBE_ENV: &str = "HORSIES_TEST_WORKFLOW_STACK_MARGIN_PROBE";

/// The libtest name of [`stack_margin_probe`].
const PROBE_TEST: &str = "workflow_engine::port_tests::stack_margin::stack_margin_probe";

/// The prefix of each line that the probe writes to stderr.
const PROBE_LINE: &str = "workflow stack margin probe:";

/// The line that the probe writes when every path ended.
const PROBE_DONE: &str = "every path ended";

/// How long the probe waits for all paths.
const PROBE_BOUND: Duration = Duration::from_secs(120);

/// `workflow_recovery_failure_limit` of the recovery pass.
const RECOVERY_FAILURE_LIMIT: u32 = 3;

/// The task of every task node that the paths start.
const TASK: &str = "stack_margin_task";

/// Runs [`stack_margin_probe`] in a child process of the test binary and
/// fails with its output when the probe did not end every path: an overflow
/// of the 1.5 MiB stack, a failed assertion, or the bound.
#[test]
fn the_deepest_workflow_paths_run_on_a_stack_of_one_and_a_half_mib() {
    let output = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args(["--exact", PROBE_TEST, "--nocapture", "--test-threads=1"])
        .env(PROBE_ENV, "1")
        .output()
        .expect("the probe process runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    // The future sizes, for the record of a build.
    for line in stderr.lines().filter(|line| line.starts_with(PROBE_LINE)) {
        eprintln!("{line}");
    }
    assert!(
        output.status.success(),
        "the probe on a stack of {STACK_MARGIN_THREAD_BYTES} bytes failed ({}).\n\
         stdout:\n{stdout}\nstderr:\n{stderr}",
        output.status,
    );
    assert!(
        stderr.contains(&format!("{PROBE_LINE} {PROBE_DONE}")),
        "the probe did not run the paths.\nstdout:\n{stdout}\nstderr:\n{stderr}",
    );
}

/// The body of the probe process: every path on a thread of
/// [`STACK_MARGIN_THREAD_BYTES`], on a current-thread runtime, as a
/// `#[tokio::test]` runs its body. Without [`PROBE_ENV`] it returns at once.
#[test]
fn stack_margin_probe() {
    let Some(_probe) = std::env::var_os(PROBE_ENV) else {
        return;
    };
    let paths = std::thread::Builder::new()
        .name("workflow-stack-margin".to_owned())
        .stack_size(STACK_MARGIN_THREAD_BYTES)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the probe runtime builds");
            runtime.block_on(Box::pin(tokio::time::timeout(
                PROBE_BOUND,
                Box::pin(every_path()),
            )))
        })
        .expect("the probe thread starts");
    let ended = paths.join().expect("the paths end without a panic");
    ended.expect("the paths end within the bound");
    eprintln!("{PROBE_LINE} {PROBE_DONE}");
}

/// `operation` on the heap. Writes the size of its future and its start to
/// stderr, so the last line before an overflow names it.
fn probe_step<F: Future>(name: &str, operation: F) -> Pin<Box<F>> {
    let operation = Box::pin(operation);
    eprintln!(
        "{PROBE_LINE} {name}: future of {} bytes, started",
        std::mem::size_of_val(&*operation),
    );
    operation
}

/// Every path, in one database of the probe.
async fn every_path() {
    let db = Box::pin(IsolatedDatabase::create()).await;
    Box::pin(recovery_pass_with_a_case_zero_candidate(&db.pool)).await;
    let child = spec("stack_margin_child", vec![task_node(0, TASK)]);
    let child_specs = child_specs_of(std::slice::from_ref(&child));
    let parent_id = Box::pin(start_with_a_child_and_a_late_child(
        &db.pool,
        &child,
        &child_specs,
    ))
    .await;
    Box::pin(phase_two_with_a_child_start(
        &db.pool,
        parent_id,
        &child_specs,
    ))
    .await;
    Box::pin(phase_two_with_a_parent_advance(
        &db.pool,
        parent_id,
        &child_specs,
    ))
    .await;
    Box::pin(monitoring_actions(&db.pool, parent_id, child_specs)).await;
    Box::pin(db.drop()).await;
}

/// The global recovery pass with a case-0 candidate: node 1 waits for the
/// FAILED node 0, so the step skips it and promotes and enqueues its three
/// dependents, which allow failed dependencies.
async fn recovery_pass_with_a_case_zero_candidate(pool: &PgPool) {
    let workflow_id = seed_workflow(pool, &WorkflowSeed::running()).await;
    let failure =
        serde_json::to_string(&err("STACK_MARGIN_FAIL", "forced")).expect("encode the failure");
    let _failed_node = seed_node(
        pool,
        &NodeSeed::task(workflow_id, 0, "FAILED").result(failure),
    )
    .await;
    let _waiting_node =
        seed_node(pool, &NodeSeed::task(workflow_id, 1, "PENDING").deps(&[0])).await;
    for index in [2, 3, 4] {
        let _dependent_node = seed_node(
            pool,
            &NodeSeed::task(workflow_id, index, "PENDING")
                .deps(&[1])
                .allow_failed_deps(),
        )
        .await;
    }
    let broker = PostgresBroker::from_pool(pool.clone());
    let child_specs = TestChildSpecs::empty();

    let report = probe_step(
        "recover_stuck_workflows",
        recover_stuck_workflows_observed_with(
            &broker,
            ChildBuild::Client(&child_specs),
            RECOVERY_FAILURE_LIMIT,
        ),
    )
    .await
    .expect("the recovery pass returns its report");

    assert_eq!(
        (report.case0_pending_reevaluated, report.errors),
        (1, 0),
        "the case-0 step applied"
    );
    assert_eq!(node_status(pool, workflow_id, 1).await, "SKIPPED");
    for index in [2, 3, 4] {
        assert_eq!(
            node_status(pool, workflow_id, index).await,
            "ENQUEUED",
            "node {index}"
        );
    }
}

/// The core start of a parent with a task root (node 0), a sub-workflow root
/// (node 1, its child built at the start) and a sub-workflow node that waits
/// for node 0 (node 2). Returns the parent.
async fn start_with_a_child_and_a_late_child(
    pool: &PgPool,
    child: &WorkflowSpec,
    child_specs: &TestChildSpecs,
) -> Uuid {
    let mut late_child = sub_workflow_node(2, child);
    late_child.dependencies = vec![0];
    let parent = spec(
        "stack_margin_parent",
        vec![task_node(0, TASK), sub_workflow_node(1, child), late_child],
    );

    let parent_id = probe_step("start_client_workflow", start(pool, &parent, child_specs)).await;

    let _started_child = child_of(pool, parent_id, 1).await;
    assert_eq!(node_status(pool, parent_id, 2).await, "PENDING");
    parent_id
}

/// The worker's phase 1, then its phase 2 ([`run_phase2`]) of `task`.
async fn finish_through_the_worker(
    pool: &PgPool,
    task: &RunningNodeTask,
    child_specs: &TestChildSpecs,
    name: &str,
) {
    let broker = PostgresBroker::from_pool(pool.clone());
    let done = worker_phase_one(&broker, task, NodeReport::Success).await;

    probe_step(
        name,
        run_phase2(
            &broker,
            ChildBuild::Client(child_specs),
            Phase2Work {
                task_id: task.task_id,
                queue_name: task.queue_name.clone(),
                is_workflow_task: true,
                capacity_notified: done.capacity_notified,
                phase_two: done.phase_two,
            },
        ),
    )
    .await;
}

/// Phase 2 of node 0 of the parent: the node finish in a transaction, which
/// promotes node 2 and starts its child.
async fn phase_two_with_a_child_start(
    pool: &PgPool,
    parent_id: Uuid,
    child_specs: &TestChildSpecs,
) {
    let root = run_node_task(pool, parent_id, 0).await;

    Box::pin(finish_through_the_worker(
        pool,
        &root,
        child_specs,
        "run_phase2 of a node that starts a child",
    ))
    .await;

    assert_eq!(node_status(pool, parent_id, 0).await, "COMPLETED");
    let _late_child = child_of(pool, parent_id, 2).await;
}

/// Phase 2 of the root of the child of node 1: the child ends, and the
/// parent advance that it owes completes node 1.
async fn phase_two_with_a_parent_advance(
    pool: &PgPool,
    parent_id: Uuid,
    child_specs: &TestChildSpecs,
) {
    let child_id = child_of(pool, parent_id, 1).await;
    let child_root = run_node_task(pool, child_id, 0).await;

    Box::pin(finish_through_the_worker(
        pool,
        &child_root,
        child_specs,
        "run_phase2 of a node that ends its child workflow",
    ))
    .await;

    assert_eq!(workflow_status(pool, child_id).await, "COMPLETED");
    assert_eq!(node_status(pool, parent_id, 1).await, "COMPLETED");
}

/// The monitoring pause, resume and cancel of the parent: the resume runs its
/// step, the parent advances and the recovery pass of the tree.
async fn monitoring_actions(pool: &PgPool, parent_id: Uuid, child_specs: TestChildSpecs) {
    let broker = PostgresBroker::from_pool(pool.clone());
    let children = ResumeChildren::Client(Arc::new(child_specs));

    let paused = probe_step(
        "pause_workflow_action",
        pause_workflow_action(&broker, parent_id),
    )
    .await;
    assert_success(&paused, "paused");
    assert_eq!(workflow_status(pool, parent_id).await, "PAUSED");

    let resumed = probe_step(
        "resume_workflow_action",
        resume_workflow_action(&broker, parent_id, &children),
    )
    .await;
    assert_success(&resumed, "resumed");
    assert_eq!(workflow_status(pool, parent_id).await, "RUNNING");

    let cancelled = probe_step(
        "cancel_workflow_action",
        cancel_workflow_action(&broker, parent_id),
    )
    .await;
    assert_success(&cancelled, "cancelled");
    assert_eq!(workflow_status(pool, parent_id).await, "CANCELLED");
}

/// `outcome` is a 200 with `word` and no warning.
fn assert_success(outcome: &ActionOutcome, word: &str) {
    assert_eq!(
        outcome.body(),
        &ActionBody::Success(ActionResponse {
            outcome: word.to_owned(),
            was_status: None,
            warning: None,
        }),
        "{word}"
    );
    assert_eq!(outcome.status_code(), 200, "{word}");
}
