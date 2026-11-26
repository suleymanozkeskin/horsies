// The workflows of the data flow layer (Rust `clients/rust/tests/worker/tests/layer5_workflow_dataflow.rs`, Python
// `clients/python/tests/e2e/tasks/workflows.py`, the `spec_*` of layer 5): results as parameters (`h.resultOf`),
// results through the workflow context (the context sources of a node), and how a failure or a skip reaches the nodes after it.

import { defineWorkflow, h } from "../../../../horsies/src/index.ts";
import {
  checkUpstream,
  ContextReport,
  contextReport,
  ctxReader,
  ctxReaderStr,
  ctxSum,
  DictResult,
  double,
  fail,
  mixedReader,
  produceDict,
  produceInt,
  receiveDict,
  slowStep,
  step,
  sumTwo,
} from "../tasks/workflow-nodes.ts";
import { named } from "./naming.ts";
import { contextNode, node } from "./node-options.ts";

/** How long the two dependencies of `any_join_context_gate` run: the context source `b` ends last. */
export const CONTEXT_GATE_MS = { a: 100, b: 300 } as const;

/** a gives 5; b doubles it. */
export const argsFromSingle = defineWorkflow({ ...named("args_from_single"), params: {}, output: h.int() }, (w) => {
  const a = w.task(produceInt, { value: 5 }, node("a"));

  return { output: w.task(double, { inputResult: a }, node("b")) };
});

/** a gives 3 and b gives 4; sum adds them, one parameter each. */
export const argsFromMultiple = defineWorkflow({ ...named("args_from_multiple"), params: {}, output: h.int() }, (w) => {
  const a = w.task(produceInt, { value: 3 }, node("a"));
  const b = w.task(produceInt, { value: 4 }, node("b"));

  return { output: w.task(sumTwo, { first: a, second: b }, node("sum")) };
});

/** a gives 3; b doubles it; c doubles b. */
export const argsFromChain = defineWorkflow({ ...named("args_from_chain"), params: {}, output: h.int() }, (w) => {
  const a = w.task(produceInt, { value: 3 }, node("a"));
  const b = w.task(double, { inputResult: a }, node("b"));

  return { output: w.task(double, { inputResult: b }, node("c")) };
});

/** a gives an object with a nested object; b returns it as it arrived. */
export const argsFromObject = defineWorkflow({ ...named("args_from_object"), params: {}, output: DictResult }, (w) => {
  const a = w.task(produceDict, { value: "test_value" }, node("a"));

  return { output: w.task(receiveDict, { inputResult: a }, node("b")) };
});

/** a fails; b allows failed dependencies, gets the failure and passes its code on. No success policy. */
export const failurePassedOn = defineWorkflow({ ...named("failure_passed_on"), params: {}, output: h.int() }, (w) => {
  const a = w.task(fail, {}, node("a"));

  return { output: w.task(double, { inputResult: a }, node("b", { runsAfterFailure: true })) };
});

/** a fails; b allows failed dependencies and reports what it got; the policy requires b only. */
export const failureReported = defineWorkflow({ ...named("failure_reported"), params: {}, output: h.string() }, (w) => {
  const a = w.task(fail, {}, node("a"));
  const b = w.task(checkUpstream, { inputResult: a }, node("b", { runsAfterFailure: true }));

  return { output: b, successPolicy: { cases: [{ required: [b] }], optional: [a] } };
});

/** a fails; b is skipped; c allows failed dependencies and reports what it got of b; the policy requires c only. */
export const skipReported = defineWorkflow({ ...named("skip_reported"), params: {}, output: h.string() }, (w) => {
  const a = w.task(fail, {}, node("a"));
  const b = w.task(double, { inputResult: a }, node("b"));
  const c = w.task(checkUpstream, { inputResult: b }, node("c", { runsAfterFailure: true }));

  return { output: c, successPolicy: { cases: [{ required: [c] }], optional: [a, b] } };
});

/** a fails; b, which does not allow failed dependencies, is skipped. */
export const failureSkips = defineWorkflow({ ...named("failure_skips"), params: {}, output: h.int() }, (w) => {
  const a = w.task(fail, {}, node("a"));

  return { output: w.task(double, { inputResult: a }, node("b")) };
});

/** a gives 7; reader reads it from its context. */
export const contextSingle = defineWorkflow({ ...named("context_single"), params: {}, output: h.int() }, (w) => {
  const a = w.task(produceInt, { value: 7 }, node("a"));

  return { output: w.task(ctxReader, { source: "a" }, contextNode("reader", [a], { after: [a] })) };
});

/** a gives 3 and b gives 4; sum reads both from its context. */
export const contextMultiple = defineWorkflow({ ...named("context_multiple"), params: {}, output: h.int() }, (w) => {
  const a = w.task(produceInt, { value: 3 }, node("a"));
  const b = w.task(produceInt, { value: 4 }, node("b"));

  return { output: w.task(ctxSum, { first: "a", second: "b" }, contextNode("sum", [a, b], { after: [a, b] })) };
});

/** a gives 7; b doubles it; c gives 3 after a; d gets b as a parameter and c from its context. */
export const mixedSources = defineWorkflow(
  { ...named("mixed_sources"), params: {}, output: h.object({ fromArgs: h.int(), fromCtx: h.int() }) },
  (w) => {
    const a = w.task(produceInt, { value: 7 }, node("a"));
    const b = w.task(double, { inputResult: a }, node("b"));
    const c = w.task(produceInt, { value: 3 }, node("c", { after: [a] }));

    return { output: w.task(mixedReader, { fromArgs: b, ctxSource: "c" }, contextNode("d", [c], { after: [c] })) };
  },
);

/** a gives 77; reader gets a as a parameter and from its context. */
export const sameSourceTwice = defineWorkflow(
  { ...named("same_source_twice"), params: {}, output: h.object({ fromArgs: h.int(), fromCtx: h.int() }) },
  (w) => {
    const a = w.task(produceInt, { value: 77 }, node("a"));

    return { output: w.task(mixedReader, { fromArgs: a, ctxSource: "a" }, contextNode("reader", [a])) };
  },
);

/** a and b of `CONTEXT_GATE_MS`; c joins on any of them and reads b, its context source, which ends last. */
export const anyJoinContextGate = defineWorkflow({ ...named("any_join_context_gate"), params: {}, output: h.string() }, (w) => {
  const a = w.task(slowStep, { step: "a", delayMs: CONTEXT_GATE_MS.a }, node("a"));
  const b = w.task(slowStep, { step: "b", delayMs: CONTEXT_GATE_MS.b }, node("b"));

  return {
    output: w.task(ctxReaderStr, { source: "b" }, contextNode("c", [b], { after: [a, b], join: { kind: "any" } })),
  };
});

/** a fails and b gives 9; reader joins on any of them and reads b from its context. */
export const anyJoinReadsSuccess = defineWorkflow({ ...named("any_join_reads_success"), params: {}, output: h.int() }, (w) => {
  const a = w.task(fail, {}, node("a"));
  const b = w.task(produceInt, { value: 9 }, node("b"));

  return {
    output: w.task(ctxReader, { source: "b" }, contextNode("reader", [a, b], { after: [a, b], join: { kind: "any" } })),
  };
});

/** The value of the context source of the context report workflows. */
export const REPORTED_SOURCE_VALUE = 21;

/** a gives `REPORTED_SOURCE_VALUE`; report, a node with context sources, reports its workflow context. */
export const contextOfNode = defineWorkflow({ ...named("context_of_node"), params: {}, output: ContextReport }, (w) => {
  const a = w.task(produceInt, { value: REPORTED_SOURCE_VALUE }, node("a"));

  return { output: w.task(contextReport, { source: "a" }, contextNode("report", [a], { after: [a] })) };
});

/** a gives `REPORTED_SOURCE_VALUE`; report waits for a and has no context sources; plain, a task with no context, runs too. */
export const contextOfNodeWithoutSources = defineWorkflow(
  { ...named("context_of_node_without_sources"), params: {}, output: ContextReport },
  (w) => {
    const a = w.task(produceInt, { value: REPORTED_SOURCE_VALUE }, node("a"));

    w.task(step, { step: "plain" }, node("plain", { after: [a] }));

    return { output: w.task(contextReport, { source: "a" }, node("report", { after: [a] })) };
  },
);

/** The workflows of this module. */
export const DATAFLOW_WORKFLOWS = [
  argsFromSingle,
  argsFromMultiple,
  argsFromChain,
  argsFromObject,
  failurePassedOn,
  failureReported,
  skipReported,
  failureSkips,
  contextSingle,
  contextMultiple,
  mixedSources,
  sameSourceTwice,
  anyJoinContextGate,
  anyJoinReadsSuccess,
  contextOfNode,
  contextOfNodeWithoutSources,
] as const;
