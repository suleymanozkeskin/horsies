// The workflows of the upstream-code layer: a node whose parameter is the outcome of an upstream node that failed. The
// node `gate` holds the reader back, so a test changes the stored code of the upstream node before the reader's input is
// built. The core checks that stored code against the `errorCodes` that the task registry of the child holds.

import { defineWorkflow, h } from "#horsies";
import { failWith, gate, openFail, readFailWith, readOpenFail } from "../tasks/workflow-nodes.ts";
import { named } from "./naming.ts";
import { node } from "./node-options.ts";

/** `upstream` fails with a declared code; `reader` reads its outcome after `held` ends. The policy requires `reader` only. */
export const upstreamCodeDeclared = defineWorkflow({ ...named("upstream_code_declared"), parameters: {}, outputType: h.string() }, (w) => {
  const upstream = w.task(failWith, { errorCode: "REQUIRED_FAIL" }, node("upstream"));
  const held = w.task(gate, {}, node("held"));
  const reader = w.task(readFailWith, { upstream }, node("reader", { after: [held], runsAfterFailure: true }));

  return { output: reader, successPolicy: { cases: [{ required: [reader] }], optional: [upstream, held] } };
});

/** `upstream` declares no `errorCodes` and fails; `reader` reads its outcome after `held` ends. The policy requires `reader` only. */
export const upstreamCodeOpen = defineWorkflow({ ...named("upstream_code_open"), parameters: {}, outputType: h.string() }, (w) => {
  const upstream = w.task(openFail, {}, node("upstream"));
  const held = w.task(gate, {}, node("held"));
  const reader = w.task(readOpenFail, { upstream }, node("reader", { after: [held], runsAfterFailure: true }));

  return { output: reader, successPolicy: { cases: [{ required: [reader] }], optional: [upstream, held] } };
});

/** The workflows of this module. */
export const UPSTREAM_CODE_WORKFLOWS = [upstreamCodeDeclared, upstreamCodeOpen] as const;
