// The tasks of the workflow nodes (Rust `clients/rust/tests/worker/src/tasks/workflow.rs`, Python
// `clients/python/tests/e2e/tasks/workflows.py`). Each task is `ISOLATED` on the default queue, and each declares the
// codes `WF_CODES`, so a node reads the failure of any upstream node with its code and can pass it on.

import {
  defineTask,
  h,
  type Horsies,
  type RuntimeWorkflowContext,
  type TaskError,
  type TaskOutcome,
} from "../../../../horsies/src/index.ts";
import { err, ok, type Result } from "../../../../horsies/src/wire/result.ts";
import { errorCode } from "../../support/outcome.ts";
import { markerErrorText, nextAttempt } from "../markers.ts";
import { holdThread } from "./waits.ts";

/** The shortest run of a node: distinct timestamps between nodes (Python `time.sleep(0.05)`). */
export const NODE_RUN_MS = 50;

/** The codes of the node tasks. */
export const WF_CODES = [
  // `e2e_wf_fail`: the deliberate failure of a node.
  "DELIBERATE_FAILURE",
  // The codes that `e2e_wf_fail_with` and `e2e_sub_int_fail` take as input.
  "OPTIONAL_FAIL",
  "REQUIRED_FAIL",
  "CASE1_FAIL",
  "FIRST_ERROR",
  "SECOND_ERROR",
  "E2E_SUB_CHILD_FAIL",
  "E2E_UPSTREAM_FAIL",
  // A node got an upstream failure whose code is not one of these codes, so it does not pass the code on.
  "UPSTREAM_FAILED",
  // `e2e_wf_retry_then_ok` before its successful attempt.
  "RETRY_NEEDED",
  // A marker operation failed in the child.
  "MARKER_FAILED",
  // A context node ran with no workflow context.
  "NO_CONTEXT",
  // A context node did not find the node that it reads, or the stored value did not read.
  "NOT_IN_CONTEXT",
  // A probe node got an outcome of the other kind than the one that it reports.
  "UNEXPECTED_OUTCOME",
] as const;

/** A code of the node tasks. */
export type WfCode = (typeof WF_CODES)[number];

/** The error that a node task returns. */
export type NodeError = { readonly code: WfCode; readonly message: string };

/** The object of `e2e_wf_final_result` (Python `{"final": "result", "count": 42}`). */
export const FinalResult = h.object({ final: h.string(), count: h.int() });

/** The object of `e2e_wf_produce_dict`: members, a number and a nested object. */
export const DictResult = h.object({ key: h.string(), count: h.int(), nested: h.object({ inner: h.boolean() }) });

/** What `e2e_wf_context_report` saw in its runtime: the context of its node, or none. */
export const ContextReport = h.union("kind", [
  h.object({
    kind: h.literal("context"),
    workflowId: h.string(),
    taskIndex: h.int(),
    taskName: h.string(),
    sourceValue: h.int(),
  }),
  h.object({ kind: h.literal("noContext") }),
]);

/** Python `e2e_wf_step`: `completed_<step>`. */
export const step = defineTask({ name: "e2e_wf_step", input: { step: h.string() }, output: h.string(), errorCodes: WF_CODES });

/** Python `e2e_wf_slow_step`: `completed_<step>` after `delayMs`. */
export const slowStep = defineTask({
  name: "e2e_wf_slow_step",
  input: { step: h.string(), delayMs: h.int() },
  output: h.string(),
  errorCodes: WF_CODES,
});

export const finalResult = defineTask({ name: "e2e_wf_final_result", input: {}, output: FinalResult, errorCodes: WF_CODES });

/** Python `e2e_wf_fail`: always `DELIBERATE_FAILURE`. */
export const fail = defineTask({ name: "e2e_wf_fail", input: {}, output: h.int(), errorCodes: WF_CODES });

/** Python `e2e_wf_fail_with`: always the code `errorCode`. */
export const failWith = defineTask({
  name: "e2e_wf_fail_with",
  input: { errorCode: h.enum(WF_CODES) },
  output: h.int(),
  errorCodes: WF_CODES,
});

/** Python `e2e_wf_mark`: returns `value`. */
export const mark = defineTask({ name: "e2e_wf_mark", input: { value: h.string() }, output: h.string(), errorCodes: WF_CODES });

/** Python `e2e_wf_slow_mark`: returns `value` after `delayMs`. */
export const slowMark = defineTask({
  name: "e2e_wf_slow_mark",
  input: { value: h.string(), delayMs: h.int() },
  output: h.string(),
  errorCodes: WF_CODES,
});

/**
 * Python `e2e_wf_retry_then_ok`: `RETRY_NEEDED` until the attempt `succeedOnAttempt` of `token`, then
 * `succeeded_on_attempt_<n>`. Three retries 1 s, 2 s and 4 s apart. The attempts are counted with the markers
 * `<token>-attempt-<n>`, which every child of the worker sees.
 */
export const retryThenOk = defineTask({
  name: "e2e_wf_retry_then_ok",
  input: { token: h.string(), succeedOnAttempt: h.int() },
  output: h.string(),
  errorCodes: WF_CODES,
  retryPolicy: { kind: "exponential", baseSeconds: 1, maxRetries: 3, jitter: false, retryFor: ["RETRY_NEEDED"] },
});

/** Python `e2e_wf_produce_int`: returns `value`. */
export const produceInt = defineTask({ name: "e2e_wf_produce_int", input: { value: h.int() }, output: h.int(), errorCodes: WF_CODES });

/** Python `e2e_wf_double`: twice its upstream value, or the upstream error. */
export const double = defineTask({
  name: "e2e_wf_double",
  input: { inputResult: h.taskOutcome(produceInt) },
  output: h.int(),
  errorCodes: WF_CODES,
});

/** Python `e2e_wf_sum_two`: the sum of two upstream values, or the first upstream error. */
export const sumTwo = defineTask({
  name: "e2e_wf_sum_two",
  input: { first: h.taskOutcome(produceInt), second: h.taskOutcome(produceInt) },
  output: h.int(),
  errorCodes: WF_CODES,
});

/** Python `e2e_wf_check_upstream_skipped`: which outcome reached it, as a text. */
export const checkUpstream = defineTask({
  name: "e2e_wf_check_upstream_skipped",
  input: { inputResult: h.taskOutcome(fail) },
  output: h.string(),
  errorCodes: WF_CODES,
});

export const produceDict = defineTask({ name: "e2e_wf_produce_dict", input: { value: h.string() }, output: DictResult, errorCodes: WF_CODES });

/** Python `e2e_wf_receive_dict`: the upstream object as it arrived. */
export const receiveDict = defineTask({
  name: "e2e_wf_receive_dict",
  input: { inputResult: h.taskOutcome(produceDict) },
  output: DictResult,
  errorCodes: WF_CODES,
});

/** Python `e2e_wf_ctx_reader`: the integer result of the node `source`, read from the workflow context. */
export const ctxReader = defineTask({
  name: "e2e_wf_ctx_reader",
  input: { source: h.string() },
  output: h.int(),
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** Python `e2e_wf_ctx_reader_str`: the text result of the node `source`, read from the workflow context. */
export const ctxReaderStr = defineTask({
  name: "e2e_wf_ctx_reader_str",
  input: { source: h.string() },
  output: h.string(),
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** Python `e2e_wf_ctx_sum`: the sum of the nodes `first` and `second`, read from the workflow context. */
export const ctxSum = defineTask({
  name: "e2e_wf_ctx_sum",
  input: { first: h.string(), second: h.string() },
  output: h.int(),
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** Python `e2e_wf_mixed_reader` and `e2e_wf_dual_reader`: one value as a parameter, one from the context node `ctxSource`. */
export const mixedReader = defineTask({
  name: "e2e_wf_mixed_reader",
  input: { fromArgs: h.taskOutcome(produceInt), ctxSource: h.string() },
  output: h.object({ fromArgs: h.int(), fromCtx: h.int() }),
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** The workflow context of its runtime: its facts and the integer result of the node `source`, or `noContext`. */
export const contextReport = defineTask({
  name: "e2e_wf_context_report",
  input: { source: h.string() },
  output: ContextReport,
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** Every contract of this module. */
export const WORKFLOW_NODE_CONTRACTS = [
  step,
  slowStep,
  finalResult,
  fail,
  failWith,
  mark,
  slowMark,
  retryThenOk,
  produceInt,
  double,
  sumTwo,
  checkUpstream,
  produceDict,
  receiveDict,
  ctxReader,
  ctxReaderStr,
  ctxSum,
  mixedReader,
  contextReport,
] as const;

/** Whether `code` is one of `WF_CODES`. Pure. */
function isWfCode(code: string): code is WfCode {
  return WF_CODES.some((known) => known === code);
}

/** The error that a node returns for the upstream failure `error`: its own code when it is one of `WF_CODES`. Pure. */
export function passedOn(error: TaskError): NodeError {
  const code = errorCode(error);
  const message = `upstream ${code.source} code ${code.code}: ${error.message ?? "no message"}`;

  return { code: code.source === "custom" && isWfCode(code.code) ? code.code : "UPSTREAM_FAILED", message };
}

/** The value of `outcome`, or the error that passes its failure on. Pure. */
export function outcomeValue<T>(outcome: TaskOutcome<T>): Result<T, NodeError> {
  switch (outcome.kind) {
    case "succeeded":
      return ok(outcome.value);
    case "failed":
      return err(passedOn(outcome.error));
  }
}

/** The integer result of the context node `nodeId`, or the error that the reading node returns. Pure. */
export function contextInt(context: RuntimeWorkflowContext, nodeId: string): Result<number, NodeError> {
  switch (context.kind) {
    case "noContext":
      return err({ code: "NO_CONTEXT", message: "the run has no workflow context" });
    case "context": {
      const read = context.context.resultFor(nodeId, produceInt);

      return read.ok ? outcomeValue(read.value) : err({ code: "NOT_IN_CONTEXT", message: JSON.stringify(read.error) });
    }
  }
}

/** The text result of the context node `nodeId`, or the error that the reading node returns. Pure. */
function contextText(context: RuntimeWorkflowContext, nodeId: string): Result<string, NodeError> {
  switch (context.kind) {
    case "noContext":
      return err({ code: "NO_CONTEXT", message: "the run has no workflow context" });
    case "context": {
      const read = context.context.resultFor(nodeId, mark);

      return read.ok ? outcomeValue(read.value) : err({ code: "NOT_IN_CONTEXT", message: JSON.stringify(read.error) });
    }
  }
}

/** The text of the outcome that reached `e2e_wf_check_upstream_skipped`. Pure. */
function upstreamText(outcome: TaskOutcome<number>): string {
  switch (outcome.kind) {
    case "succeeded":
      return `received_value_${String(outcome.value)}`;
    case "failed": {
      const code = errorCode(outcome.error).code;

      return code === "UPSTREAM_SKIPPED" ? "received_upstream_skipped" : `received_error_${code}`;
    }
  }
}

/** The implementations of this module on `app`. */
export function implementWorkflowNodes(app: Horsies): void {
  implementSteps(app);
  implementDataNodes(app);
  implementContextNodes(app);
}

function implementSteps(app: Horsies): void {
  app.implement(step, ({ step: name }, { ok }) => {
    holdThread(NODE_RUN_MS);

    return ok(`completed_${name}`);
  });
  app.implement(slowStep, ({ step: name, delayMs }, { ok }) => {
    holdThread(delayMs);

    return ok(`completed_${name}`);
  });
  app.implement(finalResult, (_input, { ok }) => {
    holdThread(NODE_RUN_MS);

    return ok({ final: "result", count: 42 });
  });
  app.implement(fail, (_input, { err }) => {
    holdThread(NODE_RUN_MS);

    return err({ code: "DELIBERATE_FAILURE", message: "Failed: DELIBERATE_FAILURE" });
  });
  app.implement(failWith, ({ errorCode: code }, { err }) => {
    holdThread(NODE_RUN_MS);

    return err({ code, message: `Failed with ${code}` });
  });
  app.implement(mark, ({ value }, { ok }) => {
    holdThread(NODE_RUN_MS);

    return ok(value);
  });
  app.implement(slowMark, ({ value, delayMs }, { ok }) => {
    holdThread(delayMs);

    return ok(value);
  });
  app.implement(retryThenOk, ({ token, succeedOnAttempt }, { ok, err }) => {
    holdThread(NODE_RUN_MS);

    const attempt = nextAttempt(token);

    if (!attempt.ok) {
      return err({ code: "MARKER_FAILED", message: markerErrorText(attempt.error) });
    }

    return attempt.value < succeedOnAttempt
      ? err({ code: "RETRY_NEEDED", message: `attempt ${String(attempt.value)} of ${String(succeedOnAttempt)}` })
      : ok(`succeeded_on_attempt_${String(attempt.value)}`);
  });
}

function implementDataNodes(app: Horsies): void {
  app.implement(produceInt, ({ value }, { ok }) => {
    holdThread(NODE_RUN_MS);

    return ok(value);
  });
  app.implement(double, ({ inputResult }, { ok, err }) => {
    const value = outcomeValue(inputResult);

    return value.ok ? ok(value.value * 2) : err(value.error);
  });
  app.implement(sumTwo, ({ first, second }, { ok, err }) => {
    const firstValue = outcomeValue(first);
    const secondValue = outcomeValue(second);

    if (!firstValue.ok) {
      return err(firstValue.error);
    }

    return secondValue.ok ? ok(firstValue.value + secondValue.value) : err(secondValue.error);
  });
  app.implement(checkUpstream, ({ inputResult }, { ok }) => ok(upstreamText(inputResult)));
  app.implement(produceDict, ({ value }, { ok }) => ok({ key: value, count: 42, nested: { inner: true } }));
  app.implement(receiveDict, ({ inputResult }, { ok, err }) => {
    const value = outcomeValue(inputResult);

    return value.ok ? ok(value.value) : err(value.error);
  });
}

function implementContextNodes(app: Horsies): void {
  app.implement(ctxReader, ({ source }, { ok, err, workflowContext }) => {
    const value = contextInt(workflowContext, source);

    return value.ok ? ok(value.value) : err(value.error);
  });
  app.implement(ctxReaderStr, ({ source }, { ok, err, workflowContext }) => {
    const value = contextText(workflowContext, source);

    return value.ok ? ok(value.value) : err(value.error);
  });
  app.implement(ctxSum, ({ first, second }, { ok, err, workflowContext }) => {
    const firstValue = contextInt(workflowContext, first);
    const secondValue = contextInt(workflowContext, second);

    if (!firstValue.ok) {
      return err(firstValue.error);
    }

    return secondValue.ok ? ok(firstValue.value + secondValue.value) : err(secondValue.error);
  });
  app.implement(mixedReader, ({ fromArgs, ctxSource }, { ok, err, workflowContext }) => {
    holdThread(NODE_RUN_MS);

    const argsValue = outcomeValue(fromArgs);
    const ctxValue = contextInt(workflowContext, ctxSource);

    if (!argsValue.ok) {
      return err(argsValue.error);
    }

    return ctxValue.ok ? ok({ fromArgs: argsValue.value, fromCtx: ctxValue.value }) : err(ctxValue.error);
  });
  app.implement(contextReport, ({ source }, { ok, err, workflowContext }) => {
    const seen = workflowContext;

    switch (seen.kind) {
      case "noContext":
        return ok({ kind: "noContext" });
      case "context": {
        const value = contextInt(seen, source);
        const { workflowId, taskIndex, taskName } = seen.context;

        return value.ok
          ? ok({ kind: "context", workflowId, taskIndex, taskName, sourceValue: value.value })
          : err(value.error);
      }
    }
  });
}
