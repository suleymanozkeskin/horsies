// The workflows of the advanced layer (Rust `clients/rust/tests/worker/tests/layer6_workflow_advanced.rs`, Python
// `clients/python/tests/e2e/tasks/workflows.py`, the `spec_*` of layer 6): joins, pause and resume, the error rule
// `pause`, success policies, node retries, and the params of a sub-workflow node. The recovery tests use the
// workflows of the structure layer; the crash test uses the instance `recovery_queues`.

import { defineWorkflow, h } from "../../../../horsies/src/index.ts";
import { subLabel } from "./children.ts";
import { ctxReaderStr, fail, failWith, mark, produceInt, retryThenOk, slowStep, step } from "../tasks/workflow-nodes.ts";
import { named } from "./naming.ts";
import { contextNode, node } from "./node-options.ts";

/** How long the three dependencies of `quorum_context_gate` run: the context source `c` ends last. */
export const QUORUM_GATE_MS = { a: 100, b: 150, c: 300 } as const;

/** How long the nodes of `pausable` run, each after the one before. */
export const PAUSABLE_MS = { a: 100, b: 200, c: 200, d: 100 } as const;

/** The value that `subworkflow_mixed_input` gives its child as an upstream result. */
export const MIXED_INPUT_VALUE = 21;

/** a, b and c of `QUORUM_GATE_MS`; d needs two of them to succeed, and reads c, its context source. */
export const quorumContextGate = defineWorkflow({ ...named("quorum_context_gate"), params: {}, output: h.string() }, (w) => {
  const a = w.task(slowStep, { step: "a", delayMs: QUORUM_GATE_MS.a }, node("a"));
  const b = w.task(slowStep, { step: "b", delayMs: QUORUM_GATE_MS.b }, node("b"));
  const c = w.task(slowStep, { step: "c", delayMs: QUORUM_GATE_MS.c }, node("c"));
  const join = { kind: "quorum", minSuccess: 2 } as const;

  return { output: w.task(ctxReaderStr, { source: "c" }, contextNode("d", [c], { after: [a, b, c], join })) };
});

/** a and b fail, c completes; d needs all three to succeed. */
export const quorumImpossible = defineWorkflow({ ...named("quorum_impossible"), params: {}, output: h.string() }, (w) => {
  const a = w.task(fail, {}, node("a"));
  const b = w.task(fail, {}, node("b"));
  const c = w.task(step, { step: "c" }, node("c"));

  return { output: w.task(step, { step: "d" }, node("d", { after: [a, b, c], join: { kind: "quorum", minSuccess: 3 } })) };
});

/** a and b fail; c joins on any of them. */
export const anyJoinAllFail = defineWorkflow({ ...named("any_join_all_fail"), params: {}, output: h.string() }, (w) => {
  const a = w.task(fail, {}, node("a"));
  const b = w.task(fail, {}, node("b"));

  return { output: w.task(step, { step: "c" }, node("c", { after: [a, b], join: { kind: "any" } })) };
});

/** Python `spec_pausable`: a, b, c, d of `PAUSABLE_MS`, each after the one before. */
export const pausable = defineWorkflow({ ...named("pausable"), params: {}, output: h.string() }, (w) => {
  const a = w.task(slowStep, { step: "a", delayMs: PAUSABLE_MS.a }, node("a"));
  const b = w.task(slowStep, { step: "b", delayMs: PAUSABLE_MS.b }, node("b", { after: [a] }));
  const c = w.task(slowStep, { step: "c", delayMs: PAUSABLE_MS.c }, node("c", { after: [b] }));

  return { output: w.task(slowStep, { step: "d", delayMs: PAUSABLE_MS.d }, node("d", { after: [c] })) };
});

/** Python `spec_on_error_pause`: a, then b that fails, then c; a failure pauses the workflow. */
export const pauseOnError = defineWorkflow(
  { ...named("pause_on_error"), params: {}, output: h.string(), onError: "pause" },
  (w) => {
    const a = w.task(step, { step: "a" }, node("a"));
    const b = w.task(fail, {}, node("b", { after: [a] }));

    return { output: w.task(step, { step: "c" }, node("c", { after: [b] })) };
  },
);

/** a is required; b is optional and fails with `OPTIONAL_FAIL`. */
export const policySatisfied = defineWorkflow({ ...named("policy_satisfied"), params: {}, output: h.string() }, (w) => {
  const a = w.task(mark, { value: "required_A" }, node("a"));
  const b = w.task(failWith, { errorCode: "OPTIONAL_FAIL" }, node("b"));

  return { output: a, successPolicy: { cases: [{ required: [a] }], optional: [b] } };
});

/** a and b are required; b fails with `REQUIRED_FAIL`. */
export const policyNotMet = defineWorkflow({ ...named("policy_not_met"), params: {}, output: h.string() }, (w) => {
  const a = w.task(mark, { value: "required_A" }, node("a"));
  const b = w.task(failWith, { errorCode: "REQUIRED_FAIL" }, node("b"));

  return { output: a, successPolicy: { cases: [{ required: [a, b] }] } };
});

/** The first case requires a, which fails with `CASE1_FAIL`; the second requires b, which completes. */
export const policyCases = defineWorkflow({ ...named("policy_cases"), params: {}, output: h.string() }, (w) => {
  const a = w.task(failWith, { errorCode: "CASE1_FAIL" }, node("a"));
  const b = w.task(mark, { value: "case2_ok" }, node("b"));

  return { output: b, successPolicy: { cases: [{ required: [a] }, { required: [b] }] } };
});

/** Two nodes that fail: `first` (index 0) with `FIRST_ERROR`, `second` (index 1) with `SECOND_ERROR`. */
export const twoFailures = defineWorkflow({ ...named("two_failures"), params: {}, output: h.int() }, (w) => {
  const first = w.task(failWith, { errorCode: "FIRST_ERROR" }, node("first"));

  w.task(failWith, { errorCode: "SECOND_ERROR" }, node("second"));

  return { output: first };
});

/** One node that needs `succeedOnAttempt` attempts of `token`. The node names no options: the task's policy applies. */
export const retryingNode = defineWorkflow(
  {
    ...named("retrying_node"),
    params: { token: h.string(), succeedOnAttempt: h.int() },
    output: h.string(),
    checkCases: [{ token: "check", succeedOnAttempt: 1 }],
  },
  (w, params) => ({ output: w.task(retryThenOk, params, node("a")) }),
);

/** A sub-workflow node that gives its child the static param `label` and the result of `produce`. */
export const subworkflowMixedInput = defineWorkflow({ ...named("subworkflow_mixed_input"), params: {}, output: h.string() }, (w) => {
  const source = w.task(produceInt, { value: MIXED_INPUT_VALUE }, node("produce"));

  return { output: w.subWorkflow(subLabel, { label: "count", value: source }, node("child")) };
});

/** The workflows of this module. */
export const ADVANCED_WORKFLOWS = [
  quorumContextGate,
  quorumImpossible,
  anyJoinAllFail,
  pausable,
  pauseOnError,
  policySatisfied,
  policyNotMet,
  policyCases,
  twoFailures,
  retryingNode,
  subworkflowMixedInput,
] as const;
