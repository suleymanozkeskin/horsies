// The child workflows of the sub-workflow layer and the tasks of their nodes (Rust
// `clients/rust/tests/worker/src/tasks/subworkflow.rs`, Python `clients/python/tests/e2e/tasks/workflows.py`, the
// `E2ESub*` definitions, and `dynamic_fanout.py`). Every process of an instance lists them, so the worker builds each
// child from its own app when the child's node is ready.
//
// A child param that takes an upstream result is a required `h.taskOutcome(contract)`. Its check cases give the
// outcome that the upstream node would give, one case per branch of the build, so `check` builds each branch (#2549).

import {
  defineTask,
  defineWorkflow,
  h,
  type Horsies,
  type TaskError,
  type TaskOutcome,
  type ValueOf,
} from "#horsies";
import { err, ok, type Result } from "../../../../horsies/src/wire/result.ts";
import { errorCode } from "../../support/outcome.ts";
import {
  contextInt,
  failWith,
  mark,
  type NodeError,
  NODE_RUN_MS,
  outcomeValue,
  produceInt,
  retryThenOk,
  slowMark,
  WF_CODES,
} from "../tasks/workflow-nodes.ts";
import { holdThread } from "../tasks/waits.ts";
import { contextNode, node } from "./node-options.ts";

/** The value of the node of `e2e_sub_ok_workflow` (Python `e2e_sub_int_ok` default). */
export const SUB_OK_VALUE = 11;

/** The code of the node of `e2e_sub_fail_workflow`. */
export const SUB_CHILD_FAIL = "E2E_SUB_CHILD_FAIL";

/** The node id of each child workflow's own node. */
export const CHILD_NODE = "child";

/** The tenant of the fan-out child and the suffixes of its shards (Python `dynamic_fanout.py`). */
export const FANOUT_TENANT_ID = "tenant-1";
const FANOUT_SHARD_SUFFIXES = ["a", "b", "c"] as const;

/** What a child build saw of an upstream param: its value, or its failure. */
export const UpstreamSeen = h.union("kind", [
  h.object({ kind: h.literal("succeeded"), value: h.int() }),
  h.object({ kind: h.literal("failed"), code: h.string() }),
]);

/** The upstream failure of a check case: a skipped upstream node, as the engine gives it. */
const CHECK_UPSTREAM_FAILURE: TaskError<never> = {
  kind: "builtIn",
  code: "UPSTREAM_SKIPPED",
  message: null,
  data: null,
  exception: null,
  subWorkflow: null,
};

/** A param whose variant the stored form keeps (Python `E2ESubPet`). */
export const Pet = h.union("kind", [
  h.object({ kind: h.literal("cat"), name: h.string() }),
  h.object({ kind: h.literal("dog"), name: h.string(), barkVolume: h.int() }),
]);

/** What the build of `e2e_sub_typed_kwarg_workflow` saw of its pet: the variant and its own member. */
export const PetSeen = h.union("kind", [
  h.object({ kind: h.literal("cat") }),
  h.object({ kind: h.literal("dog"), barkVolume: h.int() }),
]);

/** What a node after a failed child read of it, as a parameter and through its context. */
export const FailedChildProbe = h.object({
  argsCode: h.string(),
  argsChildId: h.string(),
  ctxChildId: h.string(),
  summaryStatus: h.string(),
  summaryFailedTasks: h.int(),
});

/** What a node after a completed child read of it through its context. */
export const CompletedChildProbe = h.object({
  resultValue: h.int(),
  summaryStatus: h.string(),
  summaryOutput: h.int(),
  summaryCompletedTasks: h.int(),
});

/** Python `e2e_sub_int_ok`: returns `value`. */
export const subIntOk = defineTask({ name: "e2e_sub_int_ok", input: { value: h.int() }, output: h.int(), errorCodes: WF_CODES });

/** Python `e2e_sub_int_fail`: always the code `errorCode`. */
export const subIntFail = defineTask({
  name: "e2e_sub_int_fail",
  input: { errorCode: h.enum(WF_CODES) },
  output: h.int(),
  errorCodes: WF_CODES,
});

/** Python `e2e_sub_report_input`: returns what the build saw of its upstream param. */
export const subReportInput = defineTask({
  name: "e2e_sub_report_input",
  input: { seen: UpstreamSeen },
  output: UpstreamSeen,
  errorCodes: WF_CODES,
});

/** Python `e2e_sub_pet_report`: returns what the build saw of its pet. */
export const subPetReport = defineTask({ name: "e2e_sub_pet_report", input: { seen: PetSeen }, output: PetSeen, errorCodes: WF_CODES });

/** Python `e2e_sub_probe_success_ctx`: what the node read of the completed child of the node `childNode`. */
export const subProbeCompletedChild = defineTask({
  name: "e2e_sub_probe_success_ctx",
  input: { childNode: h.string() },
  output: CompletedChildProbe,
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** Python `e2e_fanout_list_shards`: one shard name per suffix. */
export const listShards = defineTask({
  name: "e2e_fanout_list_shards",
  input: { tenantId: h.string() },
  output: h.array(h.string()),
  errorCodes: WF_CODES,
});

/** Python `e2e_fanout_count_shard_rows`: the length of the shard name. */
export const countShardRows = defineTask({ name: "e2e_fanout_count_shard_rows", input: { shard: h.string() }, output: h.int(), errorCodes: WF_CODES });

/** Python `e2e_fanout_sum_shard_rows`: the sum of the nodes `shard_<n>`, read from the workflow context. */
export const sumShardRows = defineTask({
  name: "e2e_fanout_sum_shard_rows",
  input: { shardCount: h.int() },
  output: h.int(),
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** Python `E2ESubOkWorkflow`: one node that returns `SUB_OK_VALUE`. */
export const subOk = defineWorkflow(
  { name: "e2e_sub_ok_workflow", definitionKey: "tests.e2e.sub.ok.v1", parameters: {}, outputType: h.int() },
  (w) => ({ output: w.task(subIntOk, { value: SUB_OK_VALUE }, node(CHILD_NODE)) }),
);

/** Python `E2ESubFailWorkflow`: one node that fails with `SUB_CHILD_FAIL`. */
export const subFail = defineWorkflow(
  { name: "e2e_sub_fail_workflow", definitionKey: "tests.e2e.sub.fail.v1", parameters: {}, outputType: h.int() },
  (w) => ({ output: w.task(subIntFail, { errorCode: SUB_CHILD_FAIL }, node(CHILD_NODE)) }),
);

/** Python `e2e_sub_probe_failed_child`: what the node read of the failed child of the node `childNode`. */
export const subProbeFailedChild = defineTask({
  name: "e2e_sub_probe_failed_child",
  input: { childResult: h.taskOutcome(subFail), childNode: h.string() },
  output: FailedChildProbe,
  errorCodes: WF_CODES,
  workflowContext: true,
});

/** Python `E2ESubNestedOkMiddleWorkflow`: one sub-workflow node of `e2e_sub_ok_workflow`. */
export const subNestedOkMiddle = defineWorkflow(
  { name: "e2e_sub_nested_ok_middle_workflow", definitionKey: "tests.e2e.sub.nested.ok.middle.v1", parameters: {}, outputType: h.int() },
  (w) => ({ output: w.subWorkflow(subOk, {}, node(CHILD_NODE)) }),
);

/** Python `E2ESubNestedFailMiddleWorkflow`: one sub-workflow node of `e2e_sub_fail_workflow`. */
export const subNestedFailMiddle = defineWorkflow(
  {
    name: "e2e_sub_nested_fail_middle_workflow",
    definitionKey: "tests.e2e.sub.nested.fail.middle.v1",
    parameters: {},
    outputType: h.int(),
  },
  (w) => ({ output: w.subWorkflow(subFail, {}, node(CHILD_NODE)) }),
);

/**
 * Python `E2ESubParamWorkflow`: one node that returns the upstream value. An upstream failure builds a node that
 * fails with `E2E_UPSTREAM_FAIL`.
 */
export const subParam = defineWorkflow(
  {
    name: "e2e_sub_param_workflow",
    definitionKey: "tests.e2e.sub.param.v1",
    parameters: { value: h.taskOutcome(produceInt) },
    outputType: h.int(),
    checkCases: [
      { value: { kind: "succeeded", value: 1 } },
      { value: { kind: "failed", error: CHECK_UPSTREAM_FAILURE } },
    ],
  },
  (w, { value }) => {
    const given = upstreamSeen(value);

    return {
      output:
        given.kind === "succeeded"
          ? w.task(subIntOk, { value: given.value }, node(CHILD_NODE))
          : w.task(subIntFail, { errorCode: "E2E_UPSTREAM_FAIL" }, node(CHILD_NODE)),
    };
  },
);

/** Python `E2ESubFailedInputWorkflow`: one node that reports the upstream param that the build saw. */
export const subFailedInput = defineWorkflow(
  {
    name: "e2e_sub_failed_input_workflow",
    definitionKey: "tests.e2e.sub.failed.input.v1",
    parameters: { value: h.taskOutcome(failWith) },
    outputType: UpstreamSeen,
    checkCases: [{ value: { kind: "failed", error: CHECK_UPSTREAM_FAILURE } }],
  },
  (w, { value }) => ({ output: w.task(subReportInput, { seen: upstreamSeen(value) }, node(CHILD_NODE)) }),
);

/** One node that returns `<label>=<value>`: the static param and the upstream param that the build saw. */
export const subLabel = defineWorkflow(
  {
    name: "e2e_sub_label_workflow",
    definitionKey: "tests.e2e.sub.label.v1",
    parameters: { label: h.string(), value: h.taskOutcome(produceInt) },
    outputType: h.string(),
    checkCases: [{ label: "check", value: { kind: "succeeded", value: 1 } }],
  },
  (w, { label, value }) => ({ output: w.task(mark, { value: `${label}=${seenText(upstreamSeen(value))}` }, node(CHILD_NODE)) }),
);

/** Python `E2ESubTypedKwargWorkflow`: one node that reports the variant of the pet that the build saw. */
export const subTypedKwarg = defineWorkflow(
  {
    name: "e2e_sub_typed_kwarg_workflow",
    definitionKey: "tests.e2e.sub.typed.kwarg.v1",
    parameters: { pet: Pet },
    outputType: PetSeen,
    checkCases: [{ pet: { kind: "cat", name: "Tom" } }],
  },
  (w, { pet }) => ({ output: w.task(subPetReport, { seen: petSeen(pet) }, node(CHILD_NODE)) }),
);

/** Python `E2ESubSlowWorkflow`: one node that returns `value` after `delayMs`. */
export const subSlow = defineWorkflow(
  {
    name: "e2e_sub_slow_workflow",
    definitionKey: "tests.e2e.sub.slow.v1",
    parameters: { value: h.string(), delayMs: h.int() },
    outputType: h.string(),
    checkCases: [{ value: "check", delayMs: 1 }],
  },
  (w, { value, delayMs }) => ({ output: w.task(slowMark, { value, delayMs }, node(CHILD_NODE)) }),
);

/** The value and the run of the leaf of `e2e_sub_nested_slow_middle_workflow`. */
export const NESTED_SLOW_LEAF = { value: "nested_slow_leaf", delayMs: 1_000 } as const;

/** Python `E2ESubNestedSlowMiddleWorkflow`: one sub-workflow node of `e2e_sub_slow_workflow`. */
export const subNestedSlowMiddle = defineWorkflow(
  {
    name: "e2e_sub_nested_slow_middle_workflow",
    definitionKey: "tests.e2e.sub.nested.slow.middle.v1",
    parameters: {},
    outputType: h.string(),
  },
  (w) => ({ output: w.subWorkflow(subSlow, NESTED_SLOW_LEAF, node(CHILD_NODE)) }),
);

/** Python `E2ESubRetryWorkflow`: one node that needs `succeedOnAttempt` attempts of `token`. */
export const subRetry = defineWorkflow(
  {
    name: "e2e_sub_retry_workflow",
    definitionKey: "tests.e2e.sub.retry.v1",
    parameters: { token: h.string(), succeedOnAttempt: h.int() },
    outputType: h.string(),
    checkCases: [{ token: "check", succeedOnAttempt: 1 }],
  },
  (w, params) => ({ output: w.task(retryThenOk, params, node(CHILD_NODE)) }),
);

/** Python `E2ESubExpiredWorkflow`: one node whose task expires at `goodUntil`. The build reads no clock. */
export const subExpiring = defineWorkflow(
  {
    name: "e2e_sub_expired_workflow",
    definitionKey: "tests.e2e.sub.expired.v1",
    parameters: { goodUntil: h.instant() },
    outputType: h.string(),
    checkCases: [{ goodUntil: Temporal.Instant.from("2030-01-01T00:00:00Z") }],
  },
  (w, { goodUntil }) => ({ output: w.task(mark, { value: "expired_child" }, node(CHILD_NODE, { deadline: goodUntil })) }),
);

/**
 * Python `ShardReport`: one count node `shard_<n>` per shard of the upstream list, and the node `total` that reads
 * each of them from its context. An upstream failure builds a node that fails with `E2E_UPSTREAM_FAIL`.
 */
export const shardReport = defineWorkflow(
  {
    name: "e2e_fanout_shard_report",
    definitionKey: "tests.e2e.dynamic_fanout.shard_report.v1",
    parameters: { shards: h.taskOutcome(listShards) },
    outputType: h.int(),
    checkCases: [
      { shards: { kind: "succeeded", value: ["shard-a", "shard-b"] } },
      { shards: { kind: "failed", error: CHECK_UPSTREAM_FAILURE } },
    ],
  },
  (w, { shards }) => {
    if (shards.kind === "failed") {
      return { output: w.task(subIntFail, { errorCode: "E2E_UPSTREAM_FAIL" }, node("total")) };
    }

    const names = shards.value;
    const counts = names.map((shard, position) => w.task(countShardRows, { shard }, node(`shard_${String(position)}`)));
    const total = w.task(sumShardRows, { shardCount: counts.length }, contextNode("total", counts, { after: counts }));

    return { output: total };
  },
);

/** Python `TenantReport`: list the shards, then build the child from the list. */
export const tenantReport = defineWorkflow(
  { name: "e2e_fanout_tenant_report", definitionKey: "tests.e2e.dynamic_fanout.tenant_report.v1", parameters: {}, outputType: h.int() },
  (w) => {
    const shards = w.task(listShards, { tenantId: FANOUT_TENANT_ID }, node("shards"));

    return { output: w.subWorkflow(shardReport, { shards }, node("report")) };
  },
);

/** The child workflows of this module. */
export const CHILD_WORKFLOWS = [
  subOk,
  subFail,
  subNestedOkMiddle,
  subNestedFailMiddle,
  subParam,
  subFailedInput,
  subLabel,
  subTypedKwarg,
  subSlow,
  subNestedSlowMiddle,
  subRetry,
  subExpiring,
  shardReport,
  tenantReport,
] as const;

/** The task contracts of this module. */
export const CHILD_NODE_CONTRACTS = [
  subIntOk,
  subIntFail,
  subReportInput,
  subPetReport,
  subProbeFailedChild,
  subProbeCompletedChild,
  listShards,
  countShardRows,
  sumShardRows,
] as const;

/** What a build saw of the upstream param `value`. Pure. */
function upstreamSeen(value: TaskOutcome<number>): ValueOf<typeof UpstreamSeen> {
  switch (value.kind) {
    case "succeeded":
      return { kind: "succeeded", value: value.value };
    case "failed":
      return { kind: "failed", code: errorCode(value.error).code };
  }
}

/** What a build saw of `pet`: its variant, and the member that only a dog has. Pure. */
function petSeen(pet: ValueOf<typeof Pet>): ValueOf<typeof PetSeen> {
  switch (pet.kind) {
    case "cat":
      return { kind: "cat" };
    case "dog":
      return { kind: "dog", barkVolume: pet.barkVolume };
  }
}

/** The text of what a build saw of an upstream param. Pure. */
function seenText(seen: ValueOf<typeof UpstreamSeen>): string {
  switch (seen.kind) {
    case "succeeded":
      return String(seen.value);
    case "failed":
      return seen.code;
  }
}

/** The code and the child workflow id of a failed child's outcome, or the error that the probe returns. Pure. */
function failedChildId(
  outcome: TaskOutcome<number>,
  surface: string,
): Result<{ readonly code: string; readonly childId: string }, NodeError> {
  switch (outcome.kind) {
    case "succeeded":
      return err({ code: "UNEXPECTED_OUTCOME", message: `${surface}: the child succeeded with ${String(outcome.value)}` });
    case "failed":
      return outcome.error.subWorkflow === null
        ? err({ code: "UNEXPECTED_OUTCOME", message: `${surface}: the error is not a sub-workflow failure` })
        : ok({ code: errorCode(outcome.error).code, childId: outcome.error.subWorkflow.workflowId });
  }
}

/** The implementations of this module on `app`. */
export function implementChildNodes(app: Horsies): void {
  app.implement(subIntOk, ({ value }, { ok }) => {
    holdThread(NODE_RUN_MS);

    return ok(value);
  });
  app.implement(subIntFail, ({ errorCode: code }, { err }) => {
    holdThread(NODE_RUN_MS);

    return err({ code, message: `Subworkflow failed: ${code}` });
  });
  app.implement(subReportInput, ({ seen }, { ok }) => ok(seen));
  app.implement(subPetReport, ({ seen }, { ok }) => ok(seen));
  app.implement(listShards, ({ tenantId }, { ok }) => ok(FANOUT_SHARD_SUFFIXES.map((suffix) => `${tenantId}-${suffix}`)));
  app.implement(countShardRows, ({ shard }, { ok }) => ok(shard.length));
  app.implement(sumShardRows, ({ shardCount }, { ok, err, workflowContext }) => {
    let total = 0;

    for (let position = 0; position < shardCount; position += 1) {
      const shard = contextInt(workflowContext, `shard_${String(position)}`);

      if (!shard.ok) {
        return err(shard.error);
      }

      total += shard.value;
    }

    return ok(total);
  });
  implementProbes(app);
}

function implementProbes(app: Horsies): void {
  app.implement(subProbeFailedChild, ({ childResult, childNode }, { ok, err, workflowContext }) => {
    const seen = workflowContext;

    if (seen.kind === "noContext") {
      return err({ code: "NO_CONTEXT", message: "the run has no workflow context" });
    }

    const read = seen.context.resultFor(childNode, subIntOk);
    const summary = seen.context.summaryFor(childNode);

    if (!read.ok || !summary.ok) {
      return err({ code: "NOT_IN_CONTEXT", message: JSON.stringify([read, summary]) });
    }

    const argsChild = failedChildId(childResult, "the parameter");

    if (!argsChild.ok) {
      return err(argsChild.error);
    }

    const ctxChild = failedChildId(read.value, "the context");

    if (!ctxChild.ok) {
      return err(ctxChild.error);
    }

    return ok({
      argsCode: argsChild.value.code,
      argsChildId: argsChild.value.childId,
      ctxChildId: ctxChild.value.childId,
      summaryStatus: summary.value.status,
      summaryFailedTasks: summary.value.failedTasks,
    });
  });
  app.implement(subProbeCompletedChild, ({ childNode }, { ok, err, workflowContext }) => {
    const seen = workflowContext;

    if (seen.kind === "noContext") {
      return err({ code: "NO_CONTEXT", message: "the run has no workflow context" });
    }

    const read = seen.context.resultFor(childNode, subIntOk);
    const summary = seen.context.summaryFor(childNode);
    const output = seen.context.outputFor(childNode, subOk);

    if (!read.ok || !summary.ok || !output.ok) {
      return err({ code: "NOT_IN_CONTEXT", message: JSON.stringify([read, summary, output]) });
    }

    const value = outcomeValue(read.value);

    if (!value.ok || output.value.kind === "notRecorded") {
      return err({ code: "UNEXPECTED_OUTCOME", message: JSON.stringify([read.value, output.value]) });
    }

    return ok({
      resultValue: value.value,
      summaryStatus: summary.value.status,
      summaryOutput: output.value.value,
      summaryCompletedTasks: summary.value.completedTasks,
    });
  });
}
