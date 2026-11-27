// The parent workflows of the sub-workflow layer (Rust `clients/rust/tests/worker/tests/layer7_subworkflows.rs`,
// Python `clients/python/tests/e2e/test_layer7_subworkflows.py`). Their children are the definitions of `children.ts`.
// The node ids are the ones of the Python tests.

import { defineWorkflow, h } from "../../../../horsies/src/index.ts";
import {
  CompletedChildProbe,
  FailedChildProbe,
  PetSeen,
  subExpiring,
  subFail,
  subFailedInput,
  subNestedFailMiddle,
  subNestedOkMiddle,
  subNestedSlowMiddle,
  subOk,
  subParam,
  subProbeCompletedChild,
  subProbeFailedChild,
  subRetry,
  subSlow,
  subTypedKwarg,
  UpstreamSeen,
} from "./children.ts";
import { fail, failWith, mark, produceInt, slowMark } from "../tasks/workflow-nodes.ts";
import { named } from "./naming.ts";
import { contextNode, node } from "./node-options.ts";

/** How long the slow node of a join, and the gate before a child, run. */
export const SLOW_NODE_MS = 1_000;

/** How long the gate of `cancel_before_child_start` runs. */
export const CANCEL_GATE_MS = 2_000;

/** How long the gate of `ready_child_recovery` runs: the test pauses the workflow while it runs. */
export const RECOVERY_GATE_MS = 1_500;

/** The value of the upstream node of `child_from_upstream`. */
export const UPSTREAM_VALUE = 37;

/** The children of `e2e_sub_slow_workflow` that the cancel and pause tests run. */
export const SLOW_CHILDREN = {
  cancel: { value: "cancel_me", delayMs: 2_000 },
  pause: { value: "pause_me", delayMs: 1_000 },
  resume: { value: "resume_me", delayMs: 500 },
  pauseThenCancel: { value: "pause_cancel_me", delayMs: 2_000 },
} as const;

/** A check case deadline: the builds of `check` read no clock. */
const CHECK_DEADLINE = Temporal.Instant.from("2030-01-01T00:00:00Z");

/** The name `e2e_layer7_<name>` and the definition key of the workflow `name` (Python `_workflow_spec`). Pure. */
function layer7(name: string): ReturnType<typeof named> {
  return named(`layer7_${name}`);
}

/** One child that completes. */
export const successSurface = defineWorkflow({ ...layer7("success_surface"), parameters: {}, outputType: h.int() }, (w) => ({
  output: w.subWorkflow(subOk, {}, node("e2e_sub_success_child")),
}));

/** One child that fails; a failure fails the workflow. */
export const failureFailPolicy = defineWorkflow(
  { ...layer7("failure_fail_policy"), parameters: {}, outputType: h.int(), onError: "fail" },
  (w) => ({ output: w.subWorkflow(subFail, {}, node("e2e_sub_failure_child")) }),
);

/** One child that fails; a failure pauses the workflow. */
export const failurePausePolicy = defineWorkflow(
  { ...layer7("failure_pause_policy"), parameters: {}, outputType: h.int(), onError: "pause" },
  (w) => ({ output: w.subWorkflow(subFail, {}, node("e2e_sub_pause_child")) }),
);

/** The node id of the completed child that `success_context_probe` reads. */
export const SUCCESS_CTX_CHILD = "e2e_sub_success_ctx_child";

/** A child that completes, then a probe that reads it from its context. */
export const successContextProbe = defineWorkflow({ ...layer7("success_context_probe"), parameters: {}, outputType: CompletedChildProbe }, (w) => {
  const child = w.subWorkflow(subOk, {}, node(SUCCESS_CTX_CHILD));

  return {
    output: w.task(subProbeCompletedChild, { childNode: SUCCESS_CTX_CHILD }, contextNode("e2e_sub_success_ctx_probe", [child], { after: [child] })),
  };
});

/** The node id of the failed child that `failure_args_context_probe` reads. */
export const FAILURE_PROBE_CHILD = "e2e_sub_failure_probe_child";

/** A child that fails, then a probe that gets it as a parameter and from its context; the policy requires the probe. */
export const failureArgsContextProbe = defineWorkflow(
  { ...layer7("failure_args_context_probe"), parameters: {}, outputType: FailedChildProbe },
  (w) => {
    const child = w.subWorkflow(subFail, {}, node(FAILURE_PROBE_CHILD));

    const probe = w.task(
      subProbeFailedChild,
      { childResult: child, childNode: FAILURE_PROBE_CHILD },
      contextNode("e2e_sub_failure_probe", [child], { runsAfterFailure: true }),
    );

    return { output: probe, successPolicy: { cases: [{ required: [probe] }], optional: [child] } };
  },
);

/** A required task and an optional child that fails. */
export const optionalFailedChild = defineWorkflow({ ...layer7("optional_failed_child"), parameters: {}, outputType: h.string() }, (w) => {
  const required = w.task(mark, { value: "required_ok" }, node("e2e_sub_outputless_ok"));
  const optional = w.subWorkflow(subFail, {}, node("e2e_sub_outputless_fail_child"));

  return { output: required, successPolicy: { cases: [{ required: [required] }], optional: [optional] } };
});

/** Two children side by side: one completes and is required, one fails and is optional. */
export const parallelChildren = defineWorkflow({ ...layer7("parallel_children"), parameters: {}, outputType: h.int() }, (w) => {
  const ok = w.subWorkflow(subOk, {}, node("e2e_sub_parallel_ok_child"));
  const failing = w.subWorkflow(subFail, {}, node("e2e_sub_parallel_fail_child"));

  return { output: ok, successPolicy: { cases: [{ required: [ok] }], optional: [failing] } };
});

/** A child whose own child completes. */
export const nestedSuccess = defineWorkflow({ ...layer7("nested_success"), parameters: {}, outputType: h.int() }, (w) => ({
  output: w.subWorkflow(subNestedOkMiddle, {}, node("e2e_sub_nested_success_child")),
}));

/** A child whose own child fails. */
export const nestedFailure = defineWorkflow({ ...layer7("nested_failure"), parameters: {}, outputType: h.int() }, (w) => ({
  output: w.subWorkflow(subNestedFailMiddle, {}, node("e2e_sub_nested_failure_child")),
}));

/** A task that gives `UPSTREAM_VALUE`, then a child whose build gets that result. */
export const childFromUpstream = defineWorkflow({ ...layer7("child_from_upstream"), parameters: {}, outputType: h.int() }, (w) => {
  const source = w.task(produceInt, { value: UPSTREAM_VALUE }, node("e2e_sub_param_source"));

  return { output: w.subWorkflow(subParam, { value: source }, node("e2e_sub_param_child")) };
});

/** A task that fails, then a child that allows failed dependencies and whose build gets the failure. */
export const childFromFailedUpstream = defineWorkflow(
  { ...layer7("child_from_failed_upstream"), parameters: {}, outputType: UpstreamSeen },
  (w) => {
    const source = w.task(failWith, { errorCode: "E2E_UPSTREAM_FAIL" }, node("e2e_sub_failed_input_source"));
    const child = w.subWorkflow(subFailedInput, { value: source }, node("e2e_sub_failed_input_child", { runsAfterFailure: true }));

    return { output: child, successPolicy: { cases: [{ required: [child] }], optional: [source] } };
  },
);

/** A child whose static param is a tagged union value: the dog Ada. */
export const childWithTypedParam = defineWorkflow({ ...layer7("child_with_typed_param"), parameters: {}, outputType: PetSeen }, (w) => ({
  output: w.subWorkflow(subTypedKwarg, { pet: { kind: "dog", name: "Ada", barkVolume: 7 } }, node("e2e_sub_typed_kwarg_child")),
}));

/** A fast and a slow task; the child joins on any of them. */
export const anyJoinFirstSuccess = defineWorkflow({ ...layer7("any_join_first_success"), parameters: {}, outputType: h.int() }, (w) => {
  const fast = w.task(mark, { value: "fast" }, node("l7_any_fast_success"));
  const slow = w.task(slowMark, { value: "slow", delayMs: SLOW_NODE_MS }, node("l7_any_slow_success"));

  return { output: w.subWorkflow(subOk, {}, node("l7_any_child", { after: [fast, slow], join: { kind: "any" } })) };
});

/** Two tasks that fail; the child joins on any of them. */
export const anyJoinAllFailChild = defineWorkflow({ ...layer7("any_join_all_fail_child"), parameters: {}, outputType: h.int() }, (w) => {
  const a = w.task(fail, {}, node("l7_any_fail_a"));
  const b = w.task(fail, {}, node("l7_any_fail_b"));

  return { output: w.subWorkflow(subOk, {}, node("l7_any_all_fail_child", { after: [a, b], join: { kind: "any" } })) };
});

/** Two tasks that complete and one that fails; the child needs two to succeed; the failure is optional. */
export const quorumMetChild = defineWorkflow({ ...layer7("quorum_met_child"), parameters: {}, outputType: h.int() }, (w) => {
  const a = w.task(mark, { value: "A" }, node("l7_quorum_met_a"));
  const b = w.task(mark, { value: "B" }, node("l7_quorum_met_b"));
  const c = w.task(failWith, { errorCode: "OPTIONAL_FAIL" }, node("l7_quorum_met_c_fail"));
  const child = w.subWorkflow(subOk, {}, node("l7_quorum_met_child", { after: [a, b, c], join: { kind: "quorum", minSuccess: 2 } }));

  return { output: child, successPolicy: { cases: [{ required: [child] }], optional: [c] } };
});

/** One task that completes and two that fail; the child needs two to succeed. */
export const quorumUnmetChild = defineWorkflow({ ...layer7("quorum_unmet_child"), parameters: {}, outputType: h.int() }, (w) => {
  const a = w.task(mark, { value: "A" }, node("l7_quorum_unmet_a"));
  const b = w.task(fail, {}, node("l7_quorum_unmet_b"));
  const c = w.task(fail, {}, node("l7_quorum_unmet_c"));

  return {
    output: w.subWorkflow(subOk, {}, node("l7_quorum_unmet_child", { after: [a, b, c], join: { kind: "quorum", minSuccess: 2 } })),
  };
});

/** Two tasks that fail and a slow one; the child needs two to succeed, which the failures make impossible. */
export const quorumImpossibleChild = defineWorkflow({ ...layer7("quorum_impossible_child"), parameters: {}, outputType: h.int() }, (w) => {
  const a = w.task(fail, {}, node("l7_quorum_impossible_a"));
  const b = w.task(fail, {}, node("l7_quorum_impossible_b"));
  const c = w.task(slowMark, { value: "slow_c", delayMs: SLOW_NODE_MS }, node("l7_quorum_impossible_slow_c"));
  const join = { kind: "quorum", minSuccess: 2 } as const;

  return { output: w.subWorkflow(subOk, {}, node("l7_quorum_impossible_child", { after: [a, b, c], join })) };
});

/** A task that fails, then a child after it. */
export const taskFailSkipsChild = defineWorkflow({ ...layer7("task_fail_skips_child"), parameters: {}, outputType: h.int() }, (w) => {
  const source = w.task(fail, {}, node("l7_skip_source_task"));

  return { output: w.subWorkflow(subOk, {}, node("l7_skip_sub_after_task_fail", { after: [source] })) };
});

/** A child that fails, then a task after it. */
export const childFailSkipsTask = defineWorkflow({ ...layer7("child_fail_skips_task"), parameters: {}, outputType: h.string() }, (w) => {
  const child = w.subWorkflow(subFail, {}, node("l7_skip_failed_sub"));

  return { output: w.task(mark, { value: "should_skip" }, node("l7_skip_task_after_failed_sub", { after: [child] })) };
});

/** A task that fails, then a child, a task and a child, each after the one before. */
export const skipChain = defineWorkflow({ ...layer7("skip_chain"), parameters: {}, outputType: h.int() }, (w) => {
  const source = w.task(fail, {}, node("l7_chain_source_fail"));
  const first = w.subWorkflow(subOk, {}, node("l7_chain_first_sub_skipped", { after: [source] }));
  const middle = w.task(mark, { value: "middle" }, node("l7_chain_middle_task_skipped", { after: [first] }));

  return { output: w.subWorkflow(subOk, {}, node("l7_chain_second_sub_skipped", { after: [middle] })) };
});

/** One slow child, for the cancel of a running child. */
export const cancelRunningChild = defineWorkflow({ ...layer7("cancel_running_child"), parameters: {}, outputType: h.string() }, (w) => ({
  output: w.subWorkflow(subSlow, SLOW_CHILDREN.cancel, node("l7_cancel_running_child")),
}));

/** A slow gate, then a child after it. */
export const cancelBeforeChildStart = defineWorkflow({ ...layer7("cancel_before_child_start"), parameters: {}, outputType: h.int() }, (w) => {
  const gate = w.task(slowMark, { value: "gate", delayMs: CANCEL_GATE_MS }, node("l7_cancel_before_start_gate"));

  return { output: w.subWorkflow(subOk, {}, node("l7_cancel_before_start_child", { after: [gate] })) };
});

/** A child whose own child is slow. */
export const cancelNestedChild = defineWorkflow({ ...layer7("cancel_nested_child"), parameters: {}, outputType: h.string() }, (w) => ({
  output: w.subWorkflow(subNestedSlowMiddle, {}, node("l7_cancel_nested_middle")),
}));

/** One slow child, for the pause of a running child. */
export const pauseRunningChild = defineWorkflow({ ...layer7("pause_running_child"), parameters: {}, outputType: h.string() }, (w) => ({
  output: w.subWorkflow(subSlow, SLOW_CHILDREN.pause, node("l7_pause_running_child")),
}));

/** One slow child, for a pause and a resume. */
export const pauseResumeChild = defineWorkflow({ ...layer7("pause_resume_child"), parameters: {}, outputType: h.string() }, (w) => ({
  output: w.subWorkflow(subSlow, SLOW_CHILDREN.resume, node("l7_pause_resume_child")),
}));

/** One slow child, for a pause and a cancel. */
export const pauseCancelChild = defineWorkflow({ ...layer7("pause_cancel_child"), parameters: {}, outputType: h.string() }, (w) => ({
  output: w.subWorkflow(subSlow, SLOW_CHILDREN.pauseThenCancel, node("l7_pause_then_cancel_child")),
}));

/** A gate of `RECOVERY_GATE_MS`, then a child after it. */
export const readyChildRecovery = defineWorkflow({ ...layer7("ready_child_recovery"), parameters: {}, outputType: h.int() }, (w) => {
  const gate = w.task(slowMark, { value: "gate", delayMs: RECOVERY_GATE_MS }, node("l7_recovery_ready_gate"));

  return { output: w.subWorkflow(subOk, {}, node("l7_recovery_ready_child", { after: [gate] })) };
});

/** One child that needs `succeedOnAttempt` attempts of `token`. */
export const childRetries = defineWorkflow(
  {
    ...layer7("child_retries"),
    parameters: { token: h.string(), succeedOnAttempt: h.int() },
    outputType: h.string(),
    checkCases: [{ token: "check", succeedOnAttempt: 1 }],
  },
  (w, params) => ({ output: w.subWorkflow(subRetry, params, node("l7_retry_child")) }),
);

/** One child whose node's task expires at `goodUntil`. */
export const expiringChild = defineWorkflow(
  { ...layer7("expiring_child"), parameters: { goodUntil: h.instant() }, outputType: h.string(), checkCases: [{ goodUntil: CHECK_DEADLINE }] },
  (w, params) => ({ output: w.subWorkflow(subExpiring, params, node("l7_good_until_expired_child")) }),
);

/** One child that fails, which the policy requires. */
export const requiredFailedChild = defineWorkflow({ ...layer7("required_failed_child"), parameters: {}, outputType: h.int() }, (w) => {
  const child = w.subWorkflow(subFail, {}, node("l7_policy_required_fail_child"));

  return { output: child, successPolicy: { cases: [{ required: [child] }] } };
});

/** The first case requires a child that fails; the second a child that completes. */
export const policyCaseChildren = defineWorkflow({ ...layer7("policy_case_children"), parameters: {}, outputType: h.int() }, (w) => {
  const failing = w.subWorkflow(subFail, {}, node("l7_policy_case_fail_sub"));
  const ok = w.subWorkflow(subOk, {}, node("l7_policy_case_ok_sub"));

  return { output: ok, successPolicy: { cases: [{ required: [failing] }, { required: [ok] }] } };
});

/** A required child and a required task; an optional child that fails. */
export const mixedRequiredOptional = defineWorkflow({ ...layer7("mixed_required_optional"), parameters: {}, outputType: h.string() }, (w) => {
  const requiredChild = w.subWorkflow(subOk, {}, node("l7_policy_mixed_required_sub"));
  const requiredTask = w.task(mark, { value: "task_ok" }, node("l7_policy_mixed_required_task"));
  const optionalChild = w.subWorkflow(subFail, {}, node("l7_policy_mixed_optional_sub"));

  return {
    output: requiredTask,
    successPolicy: { cases: [{ required: [requiredChild, requiredTask] }], optional: [optionalChild] },
  };
});

/** A required task, and an optional child whose own child fails. */
export const deepOptionalFailure = defineWorkflow({ ...layer7("deep_optional_failure"), parameters: {}, outputType: h.string() }, (w) => {
  const required = w.task(mark, { value: "required_ok" }, node("l7_deep_outputless_required"));
  const nested = w.subWorkflow(subNestedFailMiddle, {}, node("l7_deep_outputless_nested_fail"));

  return { output: required, successPolicy: { cases: [{ required: [required] }], optional: [nested] } };
});

/** Two required children that fail: one directly, one through its own child. */
export const deepRequiredFailures = defineWorkflow({ ...layer7("deep_required_failures"), parameters: {}, outputType: h.int() }, (w) => {
  const direct = w.subWorkflow(subFail, {}, node("l7_deep_all_failed_a"));
  const nested = w.subWorkflow(subNestedFailMiddle, {}, node("l7_deep_all_failed_b"));

  return { output: direct, successPolicy: { cases: [{ required: [direct, nested] }] } };
});

/**
 * A child that completes; a task that fails and a child after it that allows failed dependencies; an optional child
 * whose own child fails. The policy requires the completed child and the child after the failed task.
 */
export const deepMixedPaths = defineWorkflow({ ...layer7("deep_mixed_paths"), parameters: {}, outputType: h.int() }, (w) => {
  const ok = w.subWorkflow(subOk, {}, node("l7_deep_mixed_ok_sub"));
  const source = w.task(failWith, { errorCode: "E2E_UPSTREAM_FAIL" }, node("l7_deep_mixed_failed_source"));
  const afterFailure = w.subWorkflow(subFailedInput, { value: source }, node("l7_deep_mixed_allow_failed_sub", { runsAfterFailure: true }));
  const nested = w.subWorkflow(subNestedFailMiddle, {}, node("l7_deep_mixed_nested_optional_fail"));

  return {
    output: ok,
    successPolicy: { cases: [{ required: [ok, afterFailure] }], optional: [source, nested] },
  };
});

/** A required task and an optional child that fails; no output node, so `get` reads the outcome of each terminal node. */
export const outputlessOptionalFailedChild = defineWorkflow({ ...layer7("outputless_optional_failed_child"), parameters: {} }, (w) => {
  const required = w.task(mark, { value: "required_ok" }, node("e2e_sub_outputless_ok"));
  const optional = w.subWorkflow(subFail, {}, node("e2e_sub_outputless_fail_child"));

  return { successPolicy: { cases: [{ required: [required] }], optional: [optional] } };
});

/** A required task, and an optional child whose own child fails; no output node. */
export const outputlessDeepOptionalFailure = defineWorkflow({ ...layer7("outputless_deep_optional_failure"), parameters: {} }, (w) => {
  const required = w.task(mark, { value: "required_ok" }, node("l7_deep_outputless_required"));
  const nested = w.subWorkflow(subNestedFailMiddle, {}, node("l7_deep_outputless_nested_fail"));

  return { successPolicy: { cases: [{ required: [required] }], optional: [nested] } };
});

/** Two required children that fail: one directly, one through its own child; no output node. */
export const outputlessDeepRequiredFailures = defineWorkflow({ ...layer7("outputless_deep_required_failures"), parameters: {} }, (w) => {
  const direct = w.subWorkflow(subFail, {}, node("l7_deep_all_failed_a"));
  const nested = w.subWorkflow(subNestedFailMiddle, {}, node("l7_deep_all_failed_b"));

  return { successPolicy: { cases: [{ required: [direct, nested] }] } };
});

/** The paths of `deepMixedPaths`, with no output node: the terminal nodes are the two required children and the optional nested child. */
export const outputlessDeepMixedPaths = defineWorkflow({ ...layer7("outputless_deep_mixed_paths"), parameters: {} }, (w) => {
  const ok = w.subWorkflow(subOk, {}, node("l7_deep_mixed_ok_sub"));
  const source = w.task(failWith, { errorCode: "E2E_UPSTREAM_FAIL" }, node("l7_deep_mixed_failed_source"));
  const afterFailure = w.subWorkflow(subFailedInput, { value: source }, node("l7_deep_mixed_allow_failed_sub", { runsAfterFailure: true }));
  const nested = w.subWorkflow(subNestedFailMiddle, {}, node("l7_deep_mixed_nested_optional_fail"));

  return { successPolicy: { cases: [{ required: [ok, afterFailure] }], optional: [source, nested] } };
});

/** The workflows of this module. */
export const SUBWORKFLOW_PARENTS = [
  successSurface,
  failureFailPolicy,
  failurePausePolicy,
  successContextProbe,
  failureArgsContextProbe,
  optionalFailedChild,
  parallelChildren,
  nestedSuccess,
  nestedFailure,
  childFromUpstream,
  childFromFailedUpstream,
  childWithTypedParam,
  anyJoinFirstSuccess,
  anyJoinAllFailChild,
  quorumMetChild,
  quorumUnmetChild,
  quorumImpossibleChild,
  taskFailSkipsChild,
  childFailSkipsTask,
  skipChain,
  cancelRunningChild,
  cancelBeforeChildStart,
  cancelNestedChild,
  pauseRunningChild,
  pauseResumeChild,
  pauseCancelChild,
  readyChildRecovery,
  childRetries,
  expiringChild,
  requiredFailedChild,
  policyCaseChildren,
  mixedRequiredOptional,
  deepOptionalFailure,
  deepRequiredFailures,
  deepMixedPaths,
  outputlessOptionalFailedChild,
  outputlessDeepOptionalFailure,
  outputlessDeepRequiredFailures,
  outputlessDeepMixedPaths,
] as const;
