// The workflows of the structure layer (Rust `clients/rust/tests/worker/tests/layer4_workflow_structure.rs`, Python
// `clients/python/tests/e2e/tasks/workflows.py`, the `spec_*` of layer 4): chains, fan-out, fan-in, a diamond, a DAG of
// seven nodes, an output node, deadlines, failures and a cancel.

import { defineWorkflow, h } from "../../../../horsies/src/index.ts";
import { fail, FinalResult, finalResult, slowStep, step } from "../tasks/workflow-nodes.ts";
import { named } from "./naming.ts";
import { node } from "./node-options.ts";

/** How long each fan-out branch runs. */
export const FANOUT_BRANCH_MS = 1_000;

/** How long each source of the fan-in runs: staggered, so some end while others run. */
export const FANIN_SOURCE_MS = { a: 200, b: 400, c: 600 } as const;

/** How long the branches of the diamond run: `c` is the slower one. */
export const DIAMOND_BRANCH_MS = { b: 200, c: 400 } as const;

/** How long the nodes of `slow_linear` run. */
export const SLOW_LINEAR_MS = { a: 500, b: 3_000, c: 3_000 } as const;

/** How long the first node of `expiring_node` runs: past the deadline of the second node. */
export const EXPIRING_FIRST_MS = 3_000;

/** A check case deadline: the builds of `check` read no clock. */
const CHECK_DEADLINE = Temporal.Instant.from("2030-01-01T00:00:00Z");

/** Python `spec_linear`: a, b, c, each after the one before; the output is c. */
export const linear = defineWorkflow({ ...named("linear"), params: {}, output: h.string() }, (w) => {
  const a = w.task(step, { step: "a" }, node("a"));
  const b = w.task(step, { step: "b" }, node("b", { after: [a] }));

  return { output: w.task(step, { step: "c" }, node("c", { after: [b] })) };
});

/** A failing first node, then b and c: the skip of b skips c. */
export const linearFailFirst = defineWorkflow({ ...named("linear_fail_first"), params: {}, output: h.string() }, (w) => {
  const a = w.task(fail, {}, node("a"));
  const b = w.task(step, { step: "b" }, node("b", { after: [a] }));

  return { output: w.task(step, { step: "c" }, node("c", { after: [b] })) };
});

/** Python `spec_linear_fail_mid`: a, then b that fails, then c. */
export const linearFailMid = defineWorkflow({ ...named("linear_fail_mid"), params: {}, output: h.string() }, (w) => {
  const a = w.task(step, { step: "a" }, node("a"));
  const b = w.task(fail, {}, node("b", { after: [a] }));

  return { output: w.task(step, { step: "c" }, node("c", { after: [b] })) };
});

/** Python `spec_fanout`: a root, then the branches b, c, d of `FANOUT_BRANCH_MS` each. */
export const fanout = defineWorkflow({ ...named("fanout"), params: {}, output: h.string() }, (w) => {
  const root = w.task(step, { step: "root" }, node("root"));
  const b = w.task(slowStep, { step: "b", delayMs: FANOUT_BRANCH_MS }, node("b", { after: [root] }));

  w.task(slowStep, { step: "c", delayMs: FANOUT_BRANCH_MS }, node("c", { after: [root] }));
  w.task(slowStep, { step: "d", delayMs: FANOUT_BRANCH_MS }, node("d", { after: [root] }));

  return { output: b };
});

/** Python `spec_fanin`: the sources a, b, c of `FANIN_SOURCE_MS`, then the aggregate that waits for all three. */
export const fanin = defineWorkflow({ ...named("fanin"), params: {}, output: h.string() }, (w) => {
  const a = w.task(slowStep, { step: "a", delayMs: FANIN_SOURCE_MS.a }, node("a"));
  const b = w.task(slowStep, { step: "b", delayMs: FANIN_SOURCE_MS.b }, node("b"));
  const c = w.task(slowStep, { step: "c", delayMs: FANIN_SOURCE_MS.c }, node("c"));

  return { output: w.task(step, { step: "aggregate" }, node("aggregate", { after: [a, b, c] })) };
});

/** Python `spec_diamond`: a, then b and c of `DIAMOND_BRANCH_MS`, then d after both. */
export const diamond = defineWorkflow({ ...named("diamond"), params: {}, output: h.string() }, (w) => {
  const a = w.task(step, { step: "a" }, node("a"));
  const b = w.task(slowStep, { step: "b", delayMs: DIAMOND_BRANCH_MS.b }, node("b", { after: [a] }));
  const c = w.task(slowStep, { step: "c", delayMs: DIAMOND_BRANCH_MS.c }, node("c", { after: [a] }));

  return { output: w.task(step, { step: "d" }, node("d", { after: [b, c] })) };
});

/** Python `spec_complex`: a; b, c, d after a; e after b and c; f after c and d; g after e and f. */
export const complexDag = defineWorkflow({ ...named("complex_dag"), params: {}, output: h.string() }, (w) => {
  const a = w.task(step, { step: "a" }, node("a"));
  const b = w.task(step, { step: "b" }, node("b", { after: [a] }));
  const c = w.task(step, { step: "c" }, node("c", { after: [a] }));
  const d = w.task(step, { step: "d" }, node("d", { after: [a] }));
  const e = w.task(step, { step: "e" }, node("e", { after: [b, c] }));
  const f = w.task(step, { step: "f" }, node("f", { after: [c, d] }));

  return { output: w.task(step, { step: "g" }, node("g", { after: [e, f] })) };
});

/** Python `spec_output`: a step, then the node whose object is the workflow's result. */
export const explicitOutput = defineWorkflow({ ...named("explicit_output"), params: {}, output: FinalResult }, (w) => {
  const a = w.task(step, { step: "a" }, node("a"));

  return { output: w.task(finalResult, {}, node("final", { after: [a] })) };
});

/** Python `spec_single_node`: one node. */
export const singleNode = defineWorkflow({ ...named("single_node"), params: {}, output: h.string() }, (w) => ({
  output: w.task(step, { step: "only" }, node("only")),
}));

/** Python `spec_fanout_one_fail`: a root, then b that fails and the branches c and d. */
export const fanoutOneFail = defineWorkflow({ ...named("fanout_one_fail"), params: {}, output: h.string() }, (w) => {
  const root = w.task(step, { step: "root" }, node("root"));

  w.task(fail, {}, node("b", { after: [root] }));

  const c = w.task(step, { step: "c" }, node("c", { after: [root] }));

  w.task(step, { step: "d" }, node("d", { after: [root] }));

  return { output: c };
});

/** Python `spec_fanin_one_fail`: a that fails, b and c, then the aggregate that waits for all three. */
export const faninOneFail = defineWorkflow({ ...named("fanin_one_fail"), params: {}, output: h.string() }, (w) => {
  const a = w.task(fail, {}, node("a"));
  const b = w.task(step, { step: "b" }, node("b"));
  const c = w.task(step, { step: "c" }, node("c"));

  return { output: w.task(step, { step: "aggregate" }, node("aggregate", { after: [a, b, c] })) };
});

/** Python `spec_slow_linear`: a, b, c of `SLOW_LINEAR_MS`, each after the one before. */
export const slowLinear = defineWorkflow({ ...named("slow_linear"), params: {}, output: h.string() }, (w) => {
  const a = w.task(slowStep, { step: "a", delayMs: SLOW_LINEAR_MS.a }, node("a"));
  const b = w.task(slowStep, { step: "b", delayMs: SLOW_LINEAR_MS.b }, node("b", { after: [a] }));

  return { output: w.task(slowStep, { step: "c", delayMs: SLOW_LINEAR_MS.c }, node("c", { after: [b] })) };
});

/** a of `EXPIRING_FIRST_MS`, then b whose task expires at `goodUntil`. The build reads no clock. */
export const expiringNode = defineWorkflow(
  { ...named("expiring_node"), params: { goodUntil: h.instant() }, output: h.string(), checkCases: [{ goodUntil: CHECK_DEADLINE }] },
  (w, { goodUntil }) => {
    const a = w.task(slowStep, { step: "a", delayMs: EXPIRING_FIRST_MS }, node("a"));

    return { output: w.task(step, { step: "b" }, node("b", { after: [a], deadline: goodUntil })) };
  },
);

/** One node whose task expires at `goodUntil`. */
export const deadlineNode = defineWorkflow(
  { ...named("deadline_node"), params: { goodUntil: h.instant() }, output: h.string(), checkCases: [{ goodUntil: CHECK_DEADLINE }] },
  (w, { goodUntil }) => ({ output: w.task(step, { step: "deadline" }, node("deadline", { deadline: goodUntil })) }),
);

/** The workflows of this module. */
export const STRUCTURE_WORKFLOWS = [
  linear,
  linearFailFirst,
  linearFailMid,
  fanout,
  fanin,
  diamond,
  complexDag,
  explicitOutput,
  singleNode,
  fanoutOneFail,
  faninOneFail,
  slowLinear,
  expiringNode,
  deadlineNode,
] as const;
