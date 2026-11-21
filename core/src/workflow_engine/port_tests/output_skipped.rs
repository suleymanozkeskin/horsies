//! A COMPLETED workflow whose output node has no result, through the engine.
//!
//! A success case that does not require the output node lets a workflow be
//! COMPLETED while that node is SKIPPED or FAILED. A sub-workflow node is
//! COMPLETED only when its child's output is ok: an err output, or a stored
//! child result that does not parse, makes a FAILED node, so
//! `allow_failed_deps = false`, `join` and a success case hold by status.
//!
//! A test that leaves a live task row runs in its own database, so no worker
//! test of the shared database claims it.

use std::time::Duration;

use uuid::Uuid;

use super::support::{
    advance_parent_of, args_from_inner, child_of, child_specs_of, code, ctx_result, err, error_of,
    finish_node, handle, node, node_status, node_task_input, ok, required, seed_node,
    seed_workflow, shared_pool, spec, start, sub_workflow_node, task_node, workflow,
    workflow_error_code, workflow_status, IsolatedDatabase, NodeSeed, ParentLink, WorkflowSeed,
};
use crate::core::workflow::node::JoinType;
use crate::core::{OnError, SubWorkflowSummary, TaskResult, WorkflowSpec, WorkflowStatus};
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sql::TestPool;

/// The code of the failure that the driver gives a child node.
const FAILED_CODE: &str = "DELIBERATE_FAIL";

/// How long `get()` waits; every workflow here is terminal before the call.
const GET_WAIT: Duration = Duration::from_secs(2);

fn failed() -> TaskResult<serde_json::Value> {
    err(FAILED_CODE, "node failed")
}

/// `gate` (0) fails, so `out` (1) is SKIPPED; `fallback` (2) satisfies the
/// case. Python `SkippedOutputChild`.
fn skipped_output_child() -> WorkflowSpec {
    let gate = task_node(0, "output_skipped_gate");
    let mut out = task_node(1, "output_skipped_out");
    out.dependencies = vec![0];
    let fallback = task_node(2, "output_skipped_fallback");
    let mut child = spec("output_skipped_child", vec![gate, out, fallback]);
    child.output_index = Some(1);
    child.success_policy = Some(required(&[&[2]]));
    child
}

/// `out` (0) fails; `fallback` (1) satisfies the case. Python `FailedOutputChild`.
fn failed_output_child() -> WorkflowSpec {
    let out = task_node(0, "output_skipped_out");
    let fallback = task_node(1, "output_skipped_fallback");
    let mut child = spec("output_failed_child", vec![out, fallback]);
    child.output_index = Some(0);
    child.success_policy = Some(required(&[&[1]]));
    child
}

/// A workflow whose only node, and output, is the skipped-output child.
/// Python `MidOverSkippedChild`.
fn mid_over_skipped_child() -> WorkflowSpec {
    let mut mid = spec(
        "output_skipped_mid",
        vec![sub_workflow_node(0, &skipped_output_child())],
    );
    mid.output_index = Some(0);
    mid
}

/// The mid workflow's only case does not require the child node (0); `spare`
/// (1) satisfies it. Python `MidOverSkippedChildWithCase`.
fn mid_over_skipped_child_with_case() -> WorkflowSpec {
    let mut mid = spec(
        "output_skipped_mid_with_case",
        vec![
            sub_workflow_node(0, &skipped_output_child()),
            task_node(1, "output_skipped_mid_spare"),
        ],
    );
    mid.output_index = Some(0);
    mid.success_policy = Some(required(&[&[1]]));
    mid
}

/// Which child a parent runs.
#[derive(Debug, Clone, Copy)]
enum Child {
    SkippedOutput,
    FailedOutput,
}

impl Child {
    fn spec(self) -> WorkflowSpec {
        match self {
            Self::SkippedOutput => skipped_output_child(),
            Self::FailedOutput => failed_output_child(),
        }
    }

    /// The code that the parent node of this child holds.
    fn expected_code(self) -> &'static str {
        match self {
            Self::SkippedOutput => "WORKFLOW_OUTPUT_SKIPPED",
            Self::FailedOutput => FAILED_CODE,
        }
    }

    /// Finishes the child's tasks the way the worker does.
    async fn run(self, pool: &TestPool, child_id: Uuid, child_specs: &TestChildSpecs) {
        let (failing, passing) = match self {
            Self::SkippedOutput => (0, 2),
            Self::FailedOutput => (0, 1),
        };
        let _gate = finish_node(pool, child_id, failing, &failed(), child_specs).await;
        let _fallback = finish_node(
            pool,
            child_id,
            passing,
            &ok(serde_json::json!(7)),
            child_specs,
        )
        .await;
    }
}

fn output_children() -> TestChildSpecs {
    child_specs_of(&[
        skipped_output_child(),
        failed_output_child(),
        mid_over_skipped_child(),
        mid_over_skipped_child_with_case(),
    ])
}

async fn get_code(pool: &TestPool, workflow_id: Uuid) -> Option<String> {
    code(error_of(
        &handle(pool, workflow_id)
            .get(Some(GET_WAIT))
            .await
            .expect("the result read"),
    ))
}

/// `test_skipped_output_gives_the_named_outcome`
#[tokio::test]
async fn a_skipped_output_gives_the_named_outcome() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let workflow_id = start(&pool, &skipped_output_child(), &child_specs).await;

    Child::SkippedOutput
        .run(&pool, workflow_id, &child_specs)
        .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_eq!(node_status(&pool, workflow_id, 1).await, "SKIPPED");
    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");
    let error = error_of(&got);
    assert_eq!(code(error).as_deref(), Some("WORKFLOW_OUTPUT_SKIPPED"));
    assert_eq!(
        error.data_value(),
        Some(serde_json::json!({
            "workflow_id": workflow_id.to_string(),
            "output_node_id": "node_1",
            "output_task_index": 1,
            "output_task_name": "output_skipped_out",
        }))
    );
}

/// `test_failed_output_outside_the_required_set_keeps_its_error`
#[tokio::test]
async fn a_failed_output_outside_the_required_set_keeps_its_error() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let workflow_id = start(&pool, &failed_output_child(), &child_specs).await;

    Child::FailedOutput
        .run(&pool, workflow_id, &child_specs)
        .await;

    let row = workflow(&pool, workflow_id).await;
    let output = node(&pool, workflow_id, 0).await;
    assert_eq!(row.status, "COMPLETED");
    assert_eq!(output.status, "FAILED");
    assert_eq!(row.result, output.result);
    assert_eq!(
        get_code(&pool, workflow_id).await.as_deref(),
        Some(FAILED_CODE)
    );
}

/// `test_err_output_fails_the_node_and_skips_a_default_dependent`
/// (skipped output, failed output)
#[tokio::test]
async fn an_err_output_fails_the_node_and_skips_a_default_dependent() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    for child in [Child::SkippedOutput, Child::FailedOutput] {
        let mut dependent = task_node(1, "output_skipped_consume");
        dependent.dependencies = vec![0];
        dependent.args_from.insert("upstream".to_owned(), 0);
        let mut parent = spec(
            "output_skipped_parent",
            vec![sub_workflow_node(0, &child.spec()), dependent],
        );
        parent.output_index = Some(1);
        let workflow_id = start(&pool, &parent, &child_specs).await;

        child
            .run(&pool, child_of(&pool, workflow_id, 0).await, &child_specs)
            .await;

        let sub = node(&pool, workflow_id, 0).await;
        assert_eq!(sub.status, "FAILED", "{child:?}");
        let summary: SubWorkflowSummary = serde_json::from_str(
            sub.sub_workflow_summary
                .as_deref()
                .expect("the node stores its child's summary"),
        )
        .expect("the summary decodes");
        assert_eq!(summary.status, WorkflowStatus::Completed);
        assert_eq!(summary.output, None);
        let skipped = node(&pool, workflow_id, 1).await;
        assert_eq!(skipped.status, "SKIPPED");
        assert_eq!(skipped.task_id, None);
        let row = workflow(&pool, workflow_id).await;
        assert_eq!(row.status, "FAILED");
        assert_eq!(row.result, None);

        let expected = Some(child.expected_code());
        let workflow_handle = handle(&pool, workflow_id);
        assert_eq!(get_code(&pool, workflow_id).await.as_deref(), expected);
        assert_eq!(
            code(error_of(
                &workflow_handle
                    .result_for::<serde_json::Value>("node_0")
                    .await
                    .expect("the node result read")
            ))
            .as_deref(),
            expected
        );
        let results = workflow_handle.results().await.expect("results()");
        assert_eq!(code(error_of(&results["node_0"])).as_deref(), expected);
        let tasks = workflow_handle.tasks().await.expect("tasks()");
        let node_result = tasks[0]
            .result
            .as_ref()
            .expect("tasks() holds the node result");
        assert_eq!(code(error_of(node_result)).as_deref(), expected);
    }
}

/// `test_allow_failed_deps_dependent_receives_the_err` (skipped output, failed output)
#[tokio::test]
async fn an_allow_failed_deps_dependent_receives_the_err() {
    let db = IsolatedDatabase::create().await;
    let pool = db.pool.clone();
    let child_specs = output_children();
    for child in [Child::SkippedOutput, Child::FailedOutput] {
        let mut dependent = task_node(1, "output_skipped_recover");
        dependent.dependencies = vec![0];
        dependent.args_from.insert("upstream".to_owned(), 0);
        dependent.allow_failed_deps = true;
        let mut parent = spec(
            "output_skipped_recover_parent",
            vec![sub_workflow_node(0, &child.spec()), dependent],
        );
        parent.output_index = Some(1);
        let workflow_id = start(&pool, &parent, &child_specs).await;

        child
            .run(&pool, child_of(&pool, workflow_id, 0).await, &child_specs)
            .await;

        assert_eq!(node_status(&pool, workflow_id, 1).await, "ENQUEUED");
        let input = node_task_input(&pool, workflow_id, 1).await;
        assert_eq!(
            code(error_of(&args_from_inner(&input, "upstream"))).as_deref(),
            Some(child.expected_code()),
            "{child:?}"
        );
    }
    db.drop().await;
}

/// `test_workflow_context_holds_the_named_outcome`
#[tokio::test]
async fn the_workflow_context_holds_the_named_outcome() {
    let db = IsolatedDatabase::create().await;
    let pool = db.pool.clone();
    let child_specs = output_children();
    let mut dependent = task_node(1, "output_skipped_ctx_consume");
    dependent.dependencies = vec![0];
    dependent.workflow_ctx_from = Some(vec!["node_0".to_owned()]);
    dependent.allow_failed_deps = true;
    let mut parent = spec(
        "output_skipped_ctx_parent",
        vec![sub_workflow_node(0, &skipped_output_child()), dependent],
    );
    parent.output_index = Some(1);
    let workflow_id = start(&pool, &parent, &child_specs).await;

    Child::SkippedOutput
        .run(&pool, child_of(&pool, workflow_id, 0).await, &child_specs)
        .await;

    let input = node_task_input(&pool, workflow_id, 1).await;
    assert_eq!(
        code(error_of(&ctx_result(&input, "node_0"))).as_deref(),
        Some("WORKFLOW_OUTPUT_SKIPPED")
    );
    db.drop().await;
}

/// `test_join_any_does_not_count_the_failed_node`
#[tokio::test]
async fn join_any_does_not_count_the_failed_node() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut dependent = task_node(2, "output_skipped_after_any");
    dependent.dependencies = vec![0, 1];
    dependent.join = JoinType::Any;
    let mut parent = spec(
        "output_skipped_any_parent",
        vec![
            task_node(0, "output_skipped_sibling"),
            sub_workflow_node(1, &skipped_output_child()),
            dependent,
        ],
    );
    parent.output_index = Some(2);
    let workflow_id = start(&pool, &parent, &child_specs).await;

    let _sibling = finish_node(&pool, workflow_id, 0, &failed(), &child_specs).await;
    Child::SkippedOutput
        .run(&pool, child_of(&pool, workflow_id, 1).await, &child_specs)
        .await;

    assert_eq!(node_status(&pool, workflow_id, 2).await, "SKIPPED");
}

/// `test_join_quorum_does_not_count_the_failed_node`
#[tokio::test]
async fn join_quorum_does_not_count_the_failed_node() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut dependent = task_node(2, "output_skipped_after_quorum");
    dependent.dependencies = vec![0, 1];
    dependent.join = JoinType::Quorum;
    dependent.min_success = Some(2);
    let mut parent = spec(
        "output_skipped_quorum_parent",
        vec![
            task_node(0, "output_skipped_quorum_other"),
            sub_workflow_node(1, &skipped_output_child()),
            dependent,
        ],
    );
    parent.output_index = Some(2);
    let workflow_id = start(&pool, &parent, &child_specs).await;

    let _other = finish_node(
        &pool,
        workflow_id,
        0,
        &ok(serde_json::json!(7)),
        &child_specs,
    )
    .await;
    Child::SkippedOutput
        .run(&pool, child_of(&pool, workflow_id, 1).await, &child_specs)
        .await;

    assert_eq!(node_status(&pool, workflow_id, 2).await, "SKIPPED");
}

/// `test_parent_success_case_is_not_met_and_get_names_the_cause`
#[tokio::test]
async fn a_parent_success_case_is_not_met_and_get_names_the_cause() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut parent = spec(
        "output_skipped_policy_parent",
        vec![sub_workflow_node(0, &skipped_output_child())],
    );
    parent.output_index = Some(0);
    parent.success_policy = Some(required(&[&[0]]));
    let workflow_id = start(&pool, &parent, &child_specs).await;
    let child_id = child_of(&pool, workflow_id, 0).await;

    Child::SkippedOutput
        .run(&pool, child_id, &child_specs)
        .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");
    let error = error_of(&got);
    assert_eq!(code(error).as_deref(), Some("WORKFLOW_OUTPUT_SKIPPED"));
    assert_eq!(
        error.data_value().map(|data| data["workflow_id"].clone()),
        Some(serde_json::json!(child_id.to_string()))
    );
}

/// `test_policy_parent_with_the_node_in_no_case_keeps_its_error`: the node is
/// FAILED, `on_error` runs, and the case decides COMPLETED.
#[tokio::test]
async fn a_policy_parent_with_the_node_in_no_case_keeps_its_error() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut parent = spec(
        "output_skipped_no_case_parent",
        vec![
            sub_workflow_node(0, &skipped_output_child()),
            task_node(1, "output_skipped_spare"),
        ],
    );
    parent.output_index = Some(1);
    parent.success_policy = Some(required(&[&[1]]));
    let workflow_id = start(&pool, &parent, &child_specs).await;

    Child::SkippedOutput
        .run(&pool, child_of(&pool, workflow_id, 0).await, &child_specs)
        .await;
    assert_eq!(workflow_status(&pool, workflow_id).await, "RUNNING");
    let _spare = finish_node(
        &pool,
        workflow_id,
        1,
        &ok(serde_json::json!(7)),
        &child_specs,
    )
    .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "COMPLETED");
    assert_eq!(
        workflow_error_code(&pool, workflow_id).await.as_deref(),
        Some("WORKFLOW_OUTPUT_SKIPPED")
    );
    match handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read")
    {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(7)),
        TaskResult::Err(error) => panic!("get() of a COMPLETED workflow: {error:?}"),
    }
}

/// `test_pause_policy_pauses_the_parent_and_resume_finishes_it`
#[tokio::test]
async fn a_pause_policy_pauses_the_parent_and_resume_finishes_it() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut dependent = task_node(1, "output_skipped_after_pause");
    dependent.dependencies = vec![0];
    let mut parent = spec(
        "output_skipped_pause_parent",
        vec![sub_workflow_node(0, &skipped_output_child()), dependent],
    );
    parent.output_index = Some(1);
    parent.on_error = OnError::Pause;
    let workflow_id = start(&pool, &parent, &child_specs).await;

    Child::SkippedOutput
        .run(&pool, child_of(&pool, workflow_id, 0).await, &child_specs)
        .await;

    assert_eq!(workflow_status(&pool, workflow_id).await, "PAUSED");
    assert_eq!(node_status(&pool, workflow_id, 1).await, "PENDING");

    let resumed = handle(&pool, workflow_id)
        .resume(&TestChildSpecs::empty())
        .await
        .expect("resume()");
    assert!(resumed);

    assert_eq!(node_status(&pool, workflow_id, 1).await, "SKIPPED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        get_code(&pool, workflow_id).await.as_deref(),
        Some("WORKFLOW_OUTPUT_SKIPPED")
    );
}

/// `test_two_levels_with_default_policy_give_subworkflow_failed`
#[tokio::test]
async fn two_levels_with_the_default_policy_give_subworkflow_failed() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut grand = spec(
        "output_skipped_grand",
        vec![sub_workflow_node(0, &mid_over_skipped_child())],
    );
    grand.output_index = Some(0);
    let workflow_id = start(&pool, &grand, &child_specs).await;
    let mid_id = child_of(&pool, workflow_id, 0).await;
    let child_id = child_of(&pool, mid_id, 0).await;

    Child::SkippedOutput
        .run(&pool, child_id, &child_specs)
        .await;

    assert_eq!(node_status(&pool, mid_id, 0).await, "FAILED");
    assert_eq!(workflow_status(&pool, mid_id).await, "FAILED");
    assert_eq!(node_status(&pool, workflow_id, 0).await, "FAILED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    assert_eq!(
        get_code(&pool, workflow_id).await.as_deref(),
        Some("SUBWORKFLOW_FAILED")
    );
}

/// `test_two_levels_with_a_mid_case_keep_the_named_outcome`
#[tokio::test]
async fn two_levels_with_a_mid_case_keep_the_named_outcome() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut grand = spec(
        "output_skipped_grand_with_case",
        vec![sub_workflow_node(0, &mid_over_skipped_child_with_case())],
    );
    grand.output_index = Some(0);
    let workflow_id = start(&pool, &grand, &child_specs).await;
    let mid_id = child_of(&pool, workflow_id, 0).await;
    let child_id = child_of(&pool, mid_id, 0).await;

    let _spare = finish_node(&pool, mid_id, 1, &ok(serde_json::json!(7)), &child_specs).await;
    Child::SkippedOutput
        .run(&pool, child_id, &child_specs)
        .await;

    assert_eq!(node_status(&pool, mid_id, 0).await, "FAILED");
    assert_eq!(workflow_status(&pool, mid_id).await, "COMPLETED");
    assert_eq!(node_status(&pool, workflow_id, 0).await, "FAILED");
    assert_eq!(workflow_status(&pool, workflow_id).await, "FAILED");
    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");
    let error = error_of(&got);
    assert_eq!(code(error).as_deref(), Some("WORKFLOW_OUTPUT_SKIPPED"));
    assert_eq!(
        error.data_value().map(|data| data["workflow_id"].clone()),
        Some(serde_json::json!(child_id.to_string()))
    );
}

/// The entry of `node_id` in the ok map of an outputless workflow's `get()`.
fn outputless_entry(
    got: &TaskResult<serde_json::Value>,
    node_id: &str,
) -> TaskResult<serde_json::Value> {
    match got {
        TaskResult::Ok(map) => serde_json::from_value(map[node_id].clone())
            .unwrap_or_else(|error| panic!("entry '{node_id}' is not an envelope: {error}: {map}")),
        TaskResult::Err(error) => panic!("get() of a COMPLETED outputless workflow: {error:?}"),
    }
}

/// `test_outputless_parent_entry_holds_the_named_outcome`
#[tokio::test]
async fn an_outputless_parent_entry_holds_the_named_outcome() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut parent = spec(
        "output_skipped_outputless_parent",
        vec![
            sub_workflow_node(0, &skipped_output_child()),
            task_node(1, "output_skipped_outputless_spare"),
        ],
    );
    parent.success_policy = Some(required(&[&[1]]));
    let workflow_id = start(&pool, &parent, &child_specs).await;

    let _spare = finish_node(
        &pool,
        workflow_id,
        1,
        &ok(serde_json::json!(7)),
        &child_specs,
    )
    .await;
    Child::SkippedOutput
        .run(&pool, child_of(&pool, workflow_id, 0).await, &child_specs)
        .await;

    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");
    assert_eq!(
        code(error_of(&outputless_entry(&got, "node_0"))).as_deref(),
        Some("WORKFLOW_OUTPUT_SKIPPED")
    );
    match outputless_entry(&got, "node_1") {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(7)),
        TaskResult::Err(error) => panic!("entry node_1: {error:?}"),
    }
}

/// A RUNNING parent whose node 0 is linked to a COMPLETED skipped-output child
/// that stores `stored` as its result. Returns the parent and the child.
async fn seed_parent_of_a_completed_child(
    pool: &TestPool,
    parent: WorkflowSeed,
    stored: Option<&str>,
) -> (Uuid, Uuid) {
    let parent_id = seed_workflow(pool, &parent).await;
    let mut child = WorkflowSeed::child(ParentLink::of_root(parent_id, 0))
        .status("COMPLETED")
        .output(1)
        .policy(serde_json::to_value(required(&[&[2]])).expect("encode the policy"));
    child.result = stored.map(str::to_owned);
    let child_id = seed_workflow(pool, &child).await;
    seed_node(pool, &NodeSeed::task(child_id, 0, "FAILED")).await;
    seed_node(pool, &NodeSeed::task(child_id, 1, "SKIPPED").deps(&[0])).await;
    seed_node(pool, &NodeSeed::task(child_id, 2, "COMPLETED")).await;
    let key = skipped_output_child()
        .definition_key
        .expect("the child has a definition key");
    seed_node(
        pool,
        &NodeSeed::sub_workflow(parent_id, 0, "RUNNING", &key, Some(child_id)),
    )
    .await;
    (parent_id, child_id)
}

/// `test_child_text_that_does_not_parse_fails_the_node`: the node holds an
/// envelope that names the cause, and the raw text stays on the child row.
#[tokio::test]
async fn child_result_text_that_does_not_parse_fails_the_node() {
    let db = IsolatedDatabase::create().await;
    let pool = db.pool.clone();
    let child_specs = output_children();
    for stored in [
        Some("null"),
        Some("not{json"),
        Some(r#"{"ok": 99}"#),
        None,
        Some(""),
    ] {
        let (parent_id, child_id) =
            seed_parent_of_a_completed_child(&pool, WorkflowSeed::running(), stored).await;
        let mut dependent = NodeSeed::task(parent_id, 1, "PENDING")
            .deps(&[0])
            .args_from("upstream", 0)
            .allow_failed_deps();
        dependent.task_name = "output_skipped_stored_data_recover".to_owned();
        seed_node(&pool, &dependent).await;

        advance_parent_of(&pool, child_id, &child_specs).await;

        let sub = node(&pool, parent_id, 0).await;
        assert_eq!(sub.status, "FAILED", "{stored:?}");
        let node_result = sub.result.expect("the node stores an error envelope");
        assert_ne!(Some(node_result.as_str()), stored);
        assert!(node_result.contains(&child_id.to_string()), "{node_result}");
        assert_eq!(workflow(&pool, child_id).await.result.as_deref(), stored);
        assert_eq!(node_status(&pool, parent_id, 1).await, "ENQUEUED");
        let input = node_task_input(&pool, parent_id, 1).await;
        assert_eq!(
            code(error_of(&args_from_inner(&input, "upstream"))).as_deref(),
            Some("RESULT_DESERIALIZATION_ERROR"),
            "{stored:?}"
        );
        assert_eq!(
            code(error_of(
                &handle(&pool, parent_id)
                    .result_for::<serde_json::Value>("node_0")
                    .await
                    .expect("the node result read")
            ))
            .as_deref(),
            Some("RESULT_DESERIALIZATION_ERROR")
        );
    }
    db.drop().await;
}

/// `test_required_unreadable_node_reports_the_cause_at_get`
#[tokio::test]
async fn a_required_unreadable_node_reports_the_cause_at_get() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    for stored in [Some("null"), Some("not{json"), None] {
        let parent = WorkflowSeed::running()
            .output(0)
            .policy(serde_json::to_value(required(&[&[0]])).expect("encode the policy"));
        let (parent_id, child_id) = seed_parent_of_a_completed_child(&pool, parent, stored).await;

        advance_parent_of(&pool, child_id, &child_specs).await;

        assert_eq!(
            workflow_status(&pool, parent_id).await,
            "FAILED",
            "{stored:?}"
        );
        let got = handle(&pool, parent_id)
            .get(Some(GET_WAIT))
            .await
            .expect("the result read");
        let error = error_of(&got);
        assert_eq!(code(error).as_deref(), Some("RESULT_DESERIALIZATION_ERROR"));
        assert_eq!(
            error.data_value().map(|data| data["workflow_id"].clone()),
            Some(serde_json::json!(child_id.to_string()))
        );
    }
}

/// `test_outputless_skipped_terminal_holds_the_named_outcome`
#[tokio::test]
async fn an_outputless_skipped_terminal_holds_the_named_outcome() {
    let pool = shared_pool().await;
    let child_specs = output_children();
    let mut outputless = skipped_output_child();
    outputless.name = "output_skipped_outputless".to_owned();
    outputless.definition_key = Some("tests.port.output_skipped_outputless.v1".to_owned());
    outputless.output_index = None;
    let workflow_id = start(&pool, &outputless, &child_specs).await;

    Child::SkippedOutput
        .run(&pool, workflow_id, &child_specs)
        .await;

    let got = handle(&pool, workflow_id)
        .get(Some(GET_WAIT))
        .await
        .expect("the result read");
    assert_eq!(
        code(error_of(&outputless_entry(&got, "node_1"))).as_deref(),
        Some("WORKFLOW_OUTPUT_SKIPPED")
    );
    match outputless_entry(&got, "node_2") {
        TaskResult::Ok(value) => assert_eq!(value, serde_json::json!(7)),
        TaskResult::Err(error) => panic!("entry node_2: {error:?}"),
    }
}
