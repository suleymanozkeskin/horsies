//! Join rules and composed DAG shapes, driven node by node through phase 2.
//! Python: `tests/integration/test_workflow_join.py`,
//! `test_workflow_composed_patterns.py`, `test_workflow_dag_patterns.py`.
//!
//! The nodes run on [`PORT_QUEUE`], which no worker test claims, so a live
//! task left on the shared database stays where it is.

use serial_test::serial;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    args_from_inner, broker, claim_once, claim_pass, code, err, error_of, finish_node, node,
    node_status, node_task_input, ok, required, run_node_task, shared_pool, spec, start, task_node,
    worker_phase_one, worker_phase_two, workflow_status, IsolatedDatabase, NodeReport,
    RunningNodeTask, PORT_QUEUE,
};
use crate::core::workflow::node::JoinType;
use crate::core::{AnyNode, OnError, TaskResult, WorkflowSpec};
use crate::test_support::child_specs::TestChildSpecs;

/// A task node at `index` that waits for `dependencies`.
fn after(index: usize, dependencies: &[usize]) -> AnyNode {
    let mut node = task_node(index, &format!("pattern_task_{index}"));
    node.dependencies = dependencies.to_vec();
    node
}

/// A node with `join` (and `min_success` for a quorum).
fn joined(
    index: usize,
    dependencies: &[usize],
    join: JoinType,
    min_success: Option<i32>,
) -> AnyNode {
    let mut node = after(index, dependencies);
    node.join = join;
    node.min_success = min_success;
    node
}

async fn run(pool: &PgPool, workflow_spec: &WorkflowSpec) -> Uuid {
    start(pool, workflow_spec, &TestChildSpecs::empty()).await
}

/// Finishes node `index` with success.
async fn succeed(pool: &PgPool, workflow_id: Uuid, index: i32) {
    let _task = finish_node(
        pool,
        workflow_id,
        index,
        &ok(serde_json::json!(index)),
        &TestChildSpecs::empty(),
    )
    .await;
}

/// Finishes node `index` with the failure `error_code`.
async fn fail(pool: &PgPool, workflow_id: Uuid, index: i32, error_code: &str) {
    let _task = finish_node(
        pool,
        workflow_id,
        index,
        &err(error_code, "failed"),
        &TestChildSpecs::empty(),
    )
    .await;
}

async fn assert_status(pool: &PgPool, workflow_id: Uuid, index: i32, expected: &str) {
    assert_eq!(
        node_status(pool, workflow_id, index).await,
        expected,
        "node {index}"
    );
}

/// `test_workflow_join.py::TestOrJoin.test_any_join_runs_when_first_dep_succeeds`
#[tokio::test]
#[serial]
async fn join_any_is_enqueued_on_the_first_success() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "test_any_join",
            vec![
                after(0, &[]),
                after(1, &[]),
                after(2, &[]),
                joined(3, &[0, 1, 2], JoinType::Any, None),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// `TestOrJoin.test_any_join_stays_pending_while_possible`
#[tokio::test]
#[serial]
async fn join_any_stays_pending_while_a_success_is_possible() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "test_any_join_pending",
            vec![
                after(0, &[]),
                after(1, &[]),
                joined(2, &[0, 1], JoinType::Any, None),
            ],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "FAIL").await;
    assert_status(&pool, workflow_id, 2, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;

    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
}

/// `TestOrJoin.test_any_join_single_dep_succeeds` and `.test_any_join_single_dep_fails`
#[tokio::test]
#[serial]
async fn join_any_with_one_dependency_follows_it() {
    let pool = shared_pool().await;
    let single = || {
        spec(
            "test_any_single",
            vec![after(0, &[]), joined(1, &[0], JoinType::Any, None)],
        )
    };
    let succeeded = run(&pool, &single()).await;
    let failed = run(&pool, &single()).await;

    succeed(&pool, succeeded, 0).await;
    fail(&pool, failed, 0, "FAIL").await;

    assert_status(&pool, succeeded, 1, "ENQUEUED").await;
    assert_status(&pool, failed, 1, "SKIPPED").await;
}

/// `TestOrJoin.test_any_join_cascade_skips_downstream`
#[tokio::test]
#[serial]
async fn a_skipped_join_any_node_skips_its_join_all_dependent() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "test_any_cascade",
            vec![
                after(0, &[]),
                joined(1, &[0], JoinType::Any, None),
                after(2, &[1]),
            ],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "FAIL").await;

    assert_status(&pool, workflow_id, 1, "SKIPPED").await;
    assert_status(&pool, workflow_id, 2, "SKIPPED").await;
}

/// `TestOrJoin.test_any_join_idempotent_after_enqueue`
#[tokio::test]
#[serial]
async fn another_dependency_finishing_leaves_an_enqueued_join_any_node() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "test_any_idempotent",
            vec![
                after(0, &[]),
                after(1, &[]),
                joined(2, &[0, 1], JoinType::Any, None),
            ],
        ),
    )
    .await;
    succeed(&pool, workflow_id, 0).await;
    let first_task = node(&pool, workflow_id, 2).await.task_id;

    succeed(&pool, workflow_id, 1).await;

    let joined_node = node(&pool, workflow_id, 2).await;
    assert_eq!(joined_node.status, "ENQUEUED");
    assert_eq!(joined_node.task_id, first_task, "one task for the node");
}

/// `TestOrJoin.test_any_join_out_of_order_completion`
#[tokio::test]
#[serial]
async fn join_any_is_enqueued_when_the_last_dependency_finishes_first() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "test_any_ooo",
            vec![
                after(0, &[]),
                after(1, &[]),
                after(2, &[]),
                joined(3, &[0, 1, 2], JoinType::Any, None),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 2).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// A join over nodes 0 and 1 whose context dependency is node 1: node 0
/// meets the join, then node 1 fails. Returns the aggregator's status.
async fn met_join_with_a_terminal_ctx_dependency(
    name: &str,
    join: JoinType,
    min_success: Option<i32>,
) -> String {
    let pool = shared_pool().await;
    let mut aggregator = joined(2, &[0, 1], join, min_success);
    aggregator.workflow_ctx_from = Some(vec!["node_1".to_owned()]);
    let workflow_id = run(
        &pool,
        &spec(name, vec![after(0, &[]), after(1, &[]), aggregator]),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    fail(&pool, workflow_id, 1, "FAIL").await;

    node_status(&pool, workflow_id, 2).await
}

/// `TestOrJoin.test_any_join_ctx_dep_fails`
#[tokio::test]
#[serial]
async fn join_any_proceeds_when_its_context_dependency_failed() {
    assert_eq!(
        met_join_with_a_terminal_ctx_dependency("test_any_ctx_fail", JoinType::Any, None).await,
        "ENQUEUED"
    );
}

/// `TestQuorumJoin.test_quorum_ctx_dep_fails`
#[tokio::test]
#[serial]
async fn a_met_quorum_proceeds_when_its_context_dependency_failed() {
    assert_eq!(
        met_join_with_a_terminal_ctx_dependency("test_quorum_ctx_fail", JoinType::Quorum, Some(1))
            .await,
        "ENQUEUED"
    );
}

/// `TestOrJoin.test_any_join_ctx_dep_skipped`
#[tokio::test]
#[serial]
async fn join_any_proceeds_when_its_context_dependency_was_skipped() {
    let pool = shared_pool().await;
    let mut aggregator = joined(3, &[1, 2], JoinType::Any, None);
    aggregator.workflow_ctx_from = Some(vec!["node_1".to_owned()]);
    let workflow_id = run(
        &pool,
        &spec(
            "test_any_ctx_skip",
            vec![after(0, &[]), after(1, &[0]), after(2, &[]), aggregator],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "FAIL").await;
    assert_status(&pool, workflow_id, 1, "SKIPPED").await;
    succeed(&pool, workflow_id, 2).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// Three roots and a quorum aggregator (3) with `min_success`.
fn quorum_of_three(name: &str, min_success: i32) -> WorkflowSpec {
    spec(
        name,
        vec![
            after(0, &[]),
            after(1, &[]),
            after(2, &[]),
            joined(3, &[0, 1, 2], JoinType::Quorum, Some(min_success)),
        ],
    )
}

/// `TestQuorumJoin.test_quorum_runs_when_threshold_met`
#[tokio::test]
#[serial]
async fn quorum_is_enqueued_when_min_success_succeed() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &quorum_of_three("test_quorum", 2)).await;

    succeed(&pool, workflow_id, 0).await;
    succeed(&pool, workflow_id, 1).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// `TestQuorumJoin.test_quorum_stays_pending_while_possible`
#[tokio::test]
#[serial]
async fn quorum_stays_pending_while_its_threshold_is_reachable() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &quorum_of_three("test_quorum_pending", 2)).await;

    fail(&pool, workflow_id, 0, "FAIL").await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// `TestQuorumJoin.test_quorum_min_success_equals_total_deps`
#[tokio::test]
#[serial]
async fn quorum_of_every_dependency_needs_all_to_succeed() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &quorum_of_three("test_quorum_all", 3)).await;

    succeed(&pool, workflow_id, 0).await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// `TestQuorumJoin.test_quorum_min_success_one`
#[tokio::test]
#[serial]
async fn quorum_of_one_is_enqueued_on_the_first_success() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &quorum_of_three("test_quorum_one", 1)).await;

    succeed(&pool, workflow_id, 0).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// `TestQuorumJoin.test_quorum_cascade_skips_downstream`
#[tokio::test]
#[serial]
async fn an_unreachable_quorum_skips_itself_and_its_join_all_dependent() {
    let pool = shared_pool().await;
    let mut workflow_spec = quorum_of_three("test_quorum_cascade", 2);
    workflow_spec.tasks.push(after(4, &[3]));
    let workflow_id = run(&pool, &workflow_spec).await;

    fail(&pool, workflow_id, 0, "FAIL").await;
    fail(&pool, workflow_id, 1, "FAIL").await;

    assert_status(&pool, workflow_id, 3, "SKIPPED").await;
    assert_status(&pool, workflow_id, 4, "SKIPPED").await;
}

/// `TestQuorumJoin.test_quorum_exact_threshold_boundary`
#[tokio::test]
#[serial]
async fn quorum_waits_at_one_success_and_one_failure_and_promotes_on_the_second_success() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &quorum_of_three("test_quorum_boundary", 2)).await;

    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    fail(&pool, workflow_id, 1, "FAIL").await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
}

/// A → [B, C] → D, with D built by `sink`.
fn diamond(name: &str, sink: AnyNode) -> WorkflowSpec {
    spec(
        name,
        vec![after(0, &[]), after(1, &[0]), after(2, &[0]), sink],
    )
}

/// `test_workflow_composed_patterns.py::TestComposedPatterns.test_diamond_join_any_on_sink`
#[tokio::test]
#[serial]
async fn a_join_any_sink_is_enqueued_when_the_first_arm_succeeds() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &diamond("diamond_join_any", joined(3, &[1, 2], JoinType::Any, None)),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    succeed(&pool, workflow_id, 3).await;
    succeed(&pool, workflow_id, 2).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestComposedPatterns.test_diamond_join_any_first_arm_fails`
#[tokio::test]
#[serial]
async fn a_join_any_sink_waits_after_a_failed_arm_and_runs_after_the_other() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &diamond(
            "diamond_join_any_fail",
            joined(3, &[1, 2], JoinType::Any, None),
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    fail(&pool, workflow_id, 1, "ARM_FAIL").await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    succeed(&pool, workflow_id, 3).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// `TestComposedPatterns.test_diamond_allow_failed_deps_on_sink`
#[tokio::test]
#[serial]
async fn an_allow_failed_deps_sink_gets_both_results_one_failed() {
    let pool = shared_pool().await;
    let mut sink = after(3, &[1, 2]);
    sink.args_from.insert("b_result".to_owned(), 1);
    sink.args_from.insert("c_result".to_owned(), 2);
    sink.allow_failed_deps = true;
    let workflow_id = run(&pool, &diamond("diamond_afd_args", sink)).await;

    succeed(&pool, workflow_id, 0).await;
    fail(&pool, workflow_id, 1, "B_FAILED").await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;

    let input = node_task_input(&pool, workflow_id, 3).await;
    assert_eq!(
        code(error_of(&args_from_inner(&input, "b_result"))).as_deref(),
        Some("B_FAILED")
    );
    assert!(matches!(
        args_from_inner(&input, "c_result"),
        TaskResult::Ok(_)
    ));
    succeed(&pool, workflow_id, 3).await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// `TestComposedPatterns.test_fan_out_with_success_policy`
#[tokio::test]
#[serial]
async fn a_policy_that_requires_one_child_completes_when_the_others_fail() {
    let pool = shared_pool().await;
    let mut workflow_spec = spec(
        "fan_out_success_policy",
        vec![
            after(0, &[]),
            after(1, &[0]),
            after(2, &[0]),
            after(3, &[0]),
        ],
    );
    workflow_spec.success_policy = Some(required(&[&[1]]));
    let workflow_id = run(&pool, &workflow_spec).await;

    succeed(&pool, workflow_id, 0).await;
    succeed(&pool, workflow_id, 1).await;
    fail(&pool, workflow_id, 2, "C_FAIL").await;
    fail(&pool, workflow_id, 3, "D_FAIL").await;

    assert_status(&pool, workflow_id, 2, "FAILED").await;
    assert_status(&pool, workflow_id, 3, "FAILED").await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestComposedPatterns.test_wide_fan_in_quorum_on_sink`
#[tokio::test]
#[serial]
async fn a_quorum_sink_is_enqueued_while_other_dependencies_still_run() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "wide_quorum",
            vec![
                after(0, &[]),
                after(1, &[0]),
                after(2, &[0]),
                after(3, &[0]),
                after(4, &[0]),
                joined(5, &[1, 2, 3, 4], JoinType::Quorum, Some(2)),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 5, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 5, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 4, "ENQUEUED").await;
    for index in [5, 3, 4] {
        succeed(&pool, workflow_id, index).await;
    }

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// A recovery node (`input_result` from `source`, `allow_failed_deps`).
fn recovery(index: usize, source: usize) -> AnyNode {
    let mut node = after(index, &[source]);
    node.args_from.insert("input_result".to_owned(), source);
    node.allow_failed_deps = true;
    node
}

/// `TestComposedPatterns.test_mixed_dag_failure_with_recovery_branch`
#[tokio::test]
#[serial]
async fn a_recovery_branch_runs_after_a_failure_and_the_workflow_still_fails() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "mixed_recovery",
            vec![
                after(0, &[]),
                after(1, &[0]),
                after(2, &[0]),
                recovery(3, 1),
                after(4, &[2, 3]),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    fail(&pool, workflow_id, 1, "B_FAILED").await;
    assert_status(&pool, workflow_id, 1, "FAILED").await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 4, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 4, "PENDING").await;
    succeed(&pool, workflow_id, 3).await;
    assert_status(&pool, workflow_id, 4, "ENQUEUED").await;
    succeed(&pool, workflow_id, 4).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// `TestComposedPatterns.test_join_any_with_args_from_partial_terminal`
#[tokio::test]
#[serial]
async fn join_any_leaves_out_an_args_from_dependency_that_did_not_finish() {
    let pool = shared_pool().await;
    let mut sink = joined(3, &[1, 2], JoinType::Any, None);
    sink.args_from.insert("b_result".to_owned(), 1);
    sink.args_from.insert("c_result".to_owned(), 2);
    let workflow_id = run(&pool, &diamond("join_any_args_partial", sink)).await;

    succeed(&pool, workflow_id, 0).await;
    succeed(&pool, workflow_id, 1).await;

    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    let input = node_task_input(&pool, workflow_id, 3).await;
    assert!(input.get("b_result").is_some(), "{input}");
    assert!(input.get("c_result").is_none(), "{input}");
    succeed(&pool, workflow_id, 3).await;
    succeed(&pool, workflow_id, 2).await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestComposedPatterns.test_recovery_branch_itself_fails`
#[tokio::test]
#[serial]
async fn a_failed_recovery_branch_skips_the_rest_of_the_chain() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "double_failure",
            vec![
                after(0, &[]),
                recovery(1, 0),
                after(2, &[1]),
                after(3, &[2]),
            ],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "A_FAIL").await;
    assert_status(&pool, workflow_id, 0, "FAILED").await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;
    fail(&pool, workflow_id, 1, "RECOVERY_ALSO_FAILED").await;

    assert_status(&pool, workflow_id, 1, "FAILED").await;
    assert_status(&pool, workflow_id, 2, "SKIPPED").await;
    assert_status(&pool, workflow_id, 3, "SKIPPED").await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// `TestComposedPatterns.test_success_policy_with_recovery_on_diamond`
#[tokio::test]
#[serial]
async fn a_policy_that_requires_the_recovery_node_completes_the_workflow() {
    let pool = shared_pool().await;
    let mut workflow_spec = spec(
        "success_policy_recovery",
        vec![
            after(0, &[]),
            after(1, &[0]),
            after(2, &[0]),
            recovery(3, 1),
        ],
    );
    workflow_spec.success_policy = Some(required(&[&[3]]));
    let workflow_id = run(&pool, &workflow_spec).await;

    succeed(&pool, workflow_id, 0).await;
    fail(&pool, workflow_id, 1, "B_FAIL").await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    fail(&pool, workflow_id, 2, "C_FAIL").await;
    succeed(&pool, workflow_id, 3).await;

    assert_status(&pool, workflow_id, 3, "COMPLETED").await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestComposedPatterns.test_join_any_on_intermediate_chains_downstream`
#[tokio::test]
#[serial]
async fn an_early_join_any_middle_node_leads_its_dependent() {
    let pool = shared_pool().await;
    let mut workflow_spec = diamond(
        "join_any_intermediate",
        joined(3, &[1, 2], JoinType::Any, None),
    );
    workflow_spec.tasks.push(after(4, &[3]));
    let workflow_id = run(&pool, &workflow_spec).await;

    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    assert_status(&pool, workflow_id, 4, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 4, "PENDING").await;
    succeed(&pool, workflow_id, 3).await;
    assert_status(&pool, workflow_id, 4, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    succeed(&pool, workflow_id, 4).await;
    succeed(&pool, workflow_id, 2).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestComposedPatterns.test_pause_fires_before_success_policy_evaluation`
#[tokio::test]
#[serial]
async fn on_error_pause_pauses_before_a_policy_that_could_still_succeed() {
    let pool = shared_pool().await;
    let mut workflow_spec = spec("pause_overrides_policy", vec![after(0, &[]), after(1, &[])]);
    workflow_spec.on_error = OnError::Pause;
    workflow_spec.success_policy = Some(required(&[&[1]]));
    let workflow_id = run(&pool, &workflow_spec).await;
    assert_status(&pool, workflow_id, 0, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;

    fail(&pool, workflow_id, 0, "A_FAIL").await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");
    succeed(&pool, workflow_id, 1).await;

    assert_status(&pool, workflow_id, 1, "COMPLETED").await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");
    super::support::end_workflows(&pool, &[workflow_id]).await;
}

/// `test_workflow_dag_patterns.py::TestDAGPatterns.test_ready_siblings_priority_order`:
/// promoted siblings carry their node priority, and the claim takes the
/// smaller number first.
#[tokio::test]
#[serial]
async fn promoted_siblings_are_claimed_in_priority_order() {
    let db = IsolatedDatabase::create().await;
    let mut urgent = after(1, &[0]);
    urgent.priority = Some(10);
    let mut relaxed = after(2, &[0]);
    relaxed.priority = Some(50);
    let workflow_id = run(
        &db.pool,
        &spec("dag_priority", vec![after(0, &[]), urgent, relaxed]),
    )
    .await;
    succeed(&db.pool, workflow_id, 0).await;
    let urgent_task = node(&db.pool, workflow_id, 1).await.task_id;
    let mut one = claim_pass(PORT_QUEUE, 1);
    one.max_claim_batch = 1;

    let claimed = claim_once(&db.pool, &one).await;

    let ids: Vec<Option<Uuid>> = claimed.iter().map(|row| Some(row.id)).collect();
    assert_eq!(ids, vec![urgent_task]);
    db.drop().await;
}

/// `TestDAGPatterns.test_mixed_pattern`
#[tokio::test]
#[serial]
async fn a_mixed_dag_runs_in_dependency_order_and_completes() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "mixed_dag",
            vec![
                after(0, &[]),
                after(1, &[0]),
                after(2, &[0]),
                after(3, &[1]),
                after(4, &[2, 3]),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 4, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 4, "PENDING").await;
    succeed(&pool, workflow_id, 3).await;
    assert_status(&pool, workflow_id, 4, "ENQUEUED").await;
    succeed(&pool, workflow_id, 4).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_wide_fan_out_fan_in`
#[tokio::test]
#[serial]
async fn ten_middle_nodes_fan_out_and_fan_in() {
    let pool = shared_pool().await;
    let mut tasks = vec![after(0, &[])];
    tasks.extend((1..=10).map(|index| after(index, &[0])));
    tasks.push(after(11, &(1..=10).collect::<Vec<_>>()));
    let workflow_id = run(&pool, &spec("wide_fan", tasks)).await;

    assert_status(&pool, workflow_id, 0, "ENQUEUED").await;
    for index in 1..=11 {
        assert_status(&pool, workflow_id, index, "PENDING").await;
    }
    succeed(&pool, workflow_id, 0).await;
    for index in 1..=10 {
        assert_status(&pool, workflow_id, index, "ENQUEUED").await;
    }
    assert_status(&pool, workflow_id, 11, "PENDING").await;
    for index in 1..=10 {
        succeed(&pool, workflow_id, index).await;
    }
    assert_status(&pool, workflow_id, 11, "ENQUEUED").await;
    succeed(&pool, workflow_id, 11).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_fan_in_reverse_completion_order`
#[tokio::test]
#[serial]
async fn a_join_node_runs_when_its_parents_finish_in_reverse_order() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "reverse_fan_in",
            vec![
                after(0, &[]),
                after(1, &[]),
                after(2, &[]),
                after(3, &[0, 1, 2]),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    succeed(&pool, workflow_id, 3).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_fan_out_root_fails`
#[tokio::test]
#[serial]
async fn a_failed_root_skips_every_child() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "fan_out_fail",
            vec![
                after(0, &[]),
                after(1, &[0]),
                after(2, &[0]),
                after(3, &[0]),
            ],
        ),
    )
    .await;

    fail(&pool, workflow_id, 0, "DELIBERATE_FAIL").await;

    assert_status(&pool, workflow_id, 0, "FAILED").await;
    for index in 1..=3 {
        assert_status(&pool, workflow_id, index, "SKIPPED").await;
    }
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// `TestDAGPatterns.test_mixed_pattern_mid_node_fails`
#[tokio::test]
#[serial]
async fn a_failed_middle_node_skips_across_levels() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "mid_fail",
            vec![
                after(0, &[]),
                after(1, &[0]),
                after(2, &[0]),
                after(3, &[1]),
                after(4, &[2, 3]),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    fail(&pool, workflow_id, 1, "DELIBERATE_FAIL").await;
    assert_status(&pool, workflow_id, 3, "SKIPPED").await;
    assert_status(&pool, workflow_id, 4, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;

    assert_status(&pool, workflow_id, 4, "SKIPPED").await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// `TestDAGPatterns.test_linear_chain`: A -> B -> C runs in order; each
/// node is enqueued when the one before it completes.
#[tokio::test]
#[serial]
async fn a_linear_chain_runs_in_order() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "linear",
            vec![after(0, &[]), after(1, &[0]), after(2, &[1])],
        ),
    )
    .await;

    assert_status(&pool, workflow_id, 0, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 1, "PENDING").await;
    assert_status(&pool, workflow_id, 2, "PENDING").await;
    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 0, "COMPLETED").await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 1, "COMPLETED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 2, "COMPLETED").await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_fan_in`: [A, B, C] -> D; D is enqueued only after
/// its last parent completes.
#[tokio::test]
#[serial]
async fn a_fan_in_node_runs_after_all_its_parents() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "fan_in",
            vec![
                after(0, &[]),
                after(1, &[]),
                after(2, &[]),
                after(3, &[0, 1, 2]),
            ],
        ),
    )
    .await;

    for index in 0..=2 {
        assert_status(&pool, workflow_id, index, "ENQUEUED").await;
    }
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    succeed(&pool, workflow_id, 3).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_diamond`: A -> [B, C] -> D.
#[tokio::test]
#[serial]
async fn a_diamond_runs_its_sink_after_both_arms() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "diamond",
            vec![
                after(0, &[]),
                after(1, &[0]),
                after(2, &[0]),
                after(3, &[1, 2]),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 3, "ENQUEUED").await;
    succeed(&pool, workflow_id, 3).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_deep_linear`: A -> B -> C -> D -> E; each node is
/// enqueued in turn and completes.
#[tokio::test]
#[serial]
async fn a_deep_chain_runs_each_node_in_turn() {
    let pool = shared_pool().await;
    let mut tasks = vec![after(0, &[])];
    tasks.extend((1..5).map(|index| after(index, &[index - 1])));
    let workflow_id = run(&pool, &spec("deep", tasks)).await;

    for index in 0..5 {
        assert_status(&pool, workflow_id, index, "ENQUEUED").await;
        succeed(&pool, workflow_id, index).await;
        assert_status(&pool, workflow_id, index, "COMPLETED").await;
    }

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_multiple_roots`: [A, B] -> C.
#[tokio::test]
#[serial]
async fn two_roots_start_together_and_lead_their_join() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "multi_root",
            vec![after(0, &[]), after(1, &[]), after(2, &[0, 1])],
        ),
    )
    .await;

    assert_status(&pool, workflow_id, 0, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "PENDING").await;
    succeed(&pool, workflow_id, 0).await;
    succeed(&pool, workflow_id, 1).await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    succeed(&pool, workflow_id, 2).await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_single_task_workflow`: one root with no
/// dependencies completes the workflow.
#[tokio::test]
#[serial]
async fn a_single_node_completes_its_workflow() {
    let pool = shared_pool().await;
    let workflow_id = run(&pool, &spec("single", vec![after(0, &[])])).await;

    assert_status(&pool, workflow_id, 0, "ENQUEUED").await;
    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 0, "COMPLETED").await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
}

/// `TestDAGPatterns.test_diamond_one_arm_fails`: A -> [B, C] -> D; B fails.
/// D waits until C is terminal too, then is SKIPPED (no
/// `allow_failed_deps`), and the workflow fails.
#[tokio::test]
#[serial]
async fn a_failed_diamond_arm_skips_the_sink_once_the_other_arm_ends() {
    let pool = shared_pool().await;
    let workflow_id = run(
        &pool,
        &spec(
            "diamond_fail",
            vec![
                after(0, &[]),
                after(1, &[0]),
                after(2, &[0]),
                after(3, &[1, 2]),
            ],
        ),
    )
    .await;

    succeed(&pool, workflow_id, 0).await;
    assert_status(&pool, workflow_id, 1, "ENQUEUED").await;
    assert_status(&pool, workflow_id, 2, "ENQUEUED").await;
    fail(&pool, workflow_id, 1, "DELIBERATE_FAIL").await;
    assert_status(&pool, workflow_id, 1, "FAILED").await;
    assert_status(&pool, workflow_id, 3, "PENDING").await;
    succeed(&pool, workflow_id, 2).await;
    assert_status(&pool, workflow_id, 3, "SKIPPED").await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
}

/// The roots of the wide fan-in.
const WIDE_FAN_IN_ROOTS: usize = 50;

/// Every task row (live or history) of the task names `task_names`, with the
/// index of the node of `workflow_id` that links it, if any.
async fn task_rows_and_links(
    pool: &PgPool,
    workflow_id: Uuid,
    task_names: &[&str],
) -> Vec<(String, Option<i32>)> {
    let names: Vec<String> = task_names.iter().map(|name| (*name).to_owned()).collect();
    sqlx::query_as(
        "SELECT r.task_name, n.task_index
         FROM (
             SELECT id AS task_id, task_name::text AS task_name
             FROM horsies_tasks WHERE task_name = ANY($2)
             UNION ALL
             SELECT task_id, task_name::text
             FROM horsies_task_history WHERE task_name = ANY($2)
         ) r
         LEFT JOIN horsies_workflow_tasks n
             ON n.task_id = r.task_id AND n.workflow_id = $1",
    )
    .bind(workflow_id)
    .bind(&names)
    .fetch_all(pool)
    .await
    .expect("read the task rows and their links")
}

/// `tests/e2e/test_queue_starvation_repros.py::test_wide_fanin_join_does_not_create_orphaned_join_tasks`:
/// 50 roots and one join node that waits for all of them. Every root
/// finishes at once through the worker's finalize (phase 1, then phase 2,
/// concurrently on one broker); then the join finishes. The workflow
/// completes with exactly one task row for the join node, which the node
/// links, and no task row that no node links.
#[tokio::test]
#[serial]
async fn a_wide_fan_in_mints_one_join_task() {
    const ROOT_TASK: &str = "wide_fan_in_root";
    const JOIN_TASK: &str = "wide_fan_in_join";
    let db = IsolatedDatabase::create().await;
    let child_specs = TestChildSpecs::empty();
    let mut tasks: Vec<AnyNode> = (0..WIDE_FAN_IN_ROOTS)
        .map(|index| task_node(index, ROOT_TASK))
        .collect();
    let mut join = task_node(WIDE_FAN_IN_ROOTS, JOIN_TASK);
    join.dependencies = (0..WIDE_FAN_IN_ROOTS).collect();
    tasks.push(join);
    let workflow_id = start(&db.pool, &spec("wide_fan_in", tasks), &child_specs).await;
    let join_index = i32::try_from(WIDE_FAN_IN_ROOTS).expect("the join index is an i32");
    let mut roots: Vec<RunningNodeTask> = Vec::with_capacity(WIDE_FAN_IN_ROOTS);
    for root_index in 0..join_index {
        roots.push(run_node_task(&db.pool, workflow_id, root_index).await);
    }
    let broker = broker(&db.pool);
    let (shared_broker, shared_child_specs) = (&broker, &child_specs);

    futures::future::join_all(roots.iter().map(|root| async move {
        let done = worker_phase_one(shared_broker, root, NodeReport::Success).await;
        worker_phase_two(shared_broker, shared_child_specs, root, done).await;
    }))
    .await;
    assert_status(&db.pool, workflow_id, join_index, "ENQUEUED").await;
    let join_task = run_node_task(&db.pool, workflow_id, join_index).await;
    let done = worker_phase_one(&broker, &join_task, NodeReport::Success).await;
    worker_phase_two(&broker, &child_specs, &join_task, done).await;

    assert_eq!(workflow_status(&db.pool, workflow_id).await, "COMPLETED");
    let rows = task_rows_and_links(&db.pool, workflow_id, &[ROOT_TASK, JOIN_TASK]).await;
    let join_rows: Vec<Option<i32>> = rows
        .iter()
        .filter(|(task_name, _)| task_name == JOIN_TASK)
        .map(|(_, task_index)| *task_index)
        .collect();
    assert_eq!(join_rows, vec![Some(join_index)]);
    assert_eq!(rows.len(), WIDE_FAN_IN_ROOTS + 1, "{rows:?}");
    assert!(
        rows.iter().all(|(_, task_index)| task_index.is_some()),
        "a task row that no node links: {rows:?}"
    );
    db.drop().await;
}
