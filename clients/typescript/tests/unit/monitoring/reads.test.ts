// The monitoring reads with a fake addon: each read decodes the core's text, a refused query and a core error are
// values, an id or an index that the core cannot take is refused before the call, and the timing variants decode.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import type {
  MonitoringReadOutcome,
  MonitoringReadsAddon,
  TaskDetailOutcome,
} from "../../../horsies/src/native/facades/monitoring-reads.ts";
import {
  listTasks,
  type ReadTarget,
  schedules,
  taskBreakdown,
  taskDetail,
  taskFacets,
  taskStats,
  workflowNames,
  workflowNode,
  workflowRun,
  workflowRuns,
} from "../../../horsies/src/monitoring/reads.ts";
import type { WireJson } from "../../../horsies/src/_catalog/wire_json.ts";
import type { RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import type { RelayedAddon } from "../../../horsies/src/monitoring/seat-calls.ts";
import {
  coreText,
  fakeHandle,
  monitoringDbFailed,
  sample,
  taskIdOf,
  textSlot,
  timingExamples,
  valueEnvelope,
  withMember,
  workflowIdOf,
} from "./support/fakes.ts";

const ID_TEXT = "01890a5d-ac96-774b-bcce-b302099a8057";
const TASK_ID = taskIdOf(ID_TEXT);
const WORKFLOW_ID = workflowIdOf(ID_TEXT);

/** The arguments of one call of the fake addon, after the handle. */
type CallArguments = ReadonlyArray<object | string | number>;

/** A fake addon that answers every read with `outcome` and the detail read with `detail`, and logs each call. */
function fakeTarget(
  outcome: MonitoringReadOutcome,
  detail: TaskDetailOutcome = { kind: "json", text: coreText(sample("TaskDetail")) },
): { readonly target: ReadTarget; readonly calls: Map<string, CallArguments> } {
  const calls = new Map<string, CallArguments>();
  const handle = fakeHandle();

  const read = (name: string) => async (given: object, ...args: CallArguments): Promise<NativeEnvelope<MonitoringReadOutcome>> => {
    assert.equal(given, handle);
    calls.set(name, args);

    return valueEnvelope(outcome);
  };

  const addon: MonitoringReadsAddon = {
    readTaskStats: read("readTaskStats"),
    readTaskFacets: read("readTaskFacets"),
    readTaskBreakdown: read("readTaskBreakdown"),
    readTaskList: read("readTaskList"),
    readWorkflowNames: read("readWorkflowNames"),
    readWorkflowRuns: read("readWorkflowRuns"),
    readWorkflowRun: read("readWorkflowRun"),
    readWorkflowNode: read("readWorkflowNode"),
    readSchedules: read("readSchedules"),
    readTaskDetail: async (given, taskId) => {
      assert.equal(given, handle);
      calls.set("readTaskDetail", [taskId]);

      return valueEnvelope(detail);
    },
  };

  return { target: { addon, handle }, calls };
}

/** The outcome of the JSON text of `value`. */
function jsonOutcome(value: WireJson): MonitoringReadOutcome {
  return { kind: "json", text: coreText(value) };
}

describe("each read decodes the core's text", () => {
  test("taskStats gives the status counts", async () => {
    const { target } = fakeTarget(jsonOutcome([sample("StatusCount"), sample("StatusCount")]));
    const read = await taskStats(target, {});

    assert.ok(read.ok);
    assert.equal(read.value.length, 2);
    assert.ok(Object.isFrozen(read.value));
  });

  test("taskFacets, taskBreakdown and listTasks give their models", async () => {
    const facets = await taskFacets(fakeTarget(jsonOutcome(sample("Facets"))).target, {});
    const breakdown = await taskBreakdown(fakeTarget(jsonOutcome(sample("Breakdown"))).target, { groupBy: "queue" });
    const page = await listTasks(fakeTarget(jsonOutcome(sample("TaskListPage"))).target, {});

    assert.ok(facets.ok && breakdown.ok && page.ok);
    assert.ok(facets.value.errorCategoryTotals instanceof Map);
    assert.ok(Array.isArray(page.value.rows));
  });

  test("workflowNames gives the names, workflowRuns and schedules their models", async () => {
    const names = await workflowNames(fakeTarget(jsonOutcome(["nightly", "sync"])).target);
    const runs = await workflowRuns(fakeTarget(jsonOutcome([sample("WorkflowRunSummary")])).target, {});
    const states = await schedules(fakeTarget(jsonOutcome([sample("ScheduleStateInfo")])).target);

    assert.deepEqual(names, { ok: true, value: ["nightly", "sync"] });
    assert.ok(runs.ok && runs.value.length === 1);
    assert.ok(states.ok && states.value.length === 1);
  });

  test("the list read passes the handle and its argument to the addon", async () => {
    const { target, calls } = fakeTarget(jsonOutcome(sample("TaskListPage")));

    const read = await listTasks(target, { limit: 7 });

    assert.ok(read.ok);

    const [arg] = calls.get("readTaskList") ?? [];

    assert.ok(arg instanceof Object && "limit" in arg);
    assert.deepEqual(arg.limit, { kind: "set", value: 7 });
  });
});

describe("found and not found", () => {
  test("taskDetail found is the detail with its attempts", async () => {
    const read = await taskDetail(fakeTarget(jsonOutcome(null)).target, TASK_ID);

    assert.ok(read.ok && read.value.kind === "found");
    assert.ok(Array.isArray(read.value.taskDetail.attempts));
  });

  test("taskDetail of a task that no table holds is notFound with its reason", async () => {
    const absent: TaskDetailOutcome = { kind: "absent", taskId: TASK_ID, reason: "NOT_CLASSIFIED" };
    const read = await taskDetail(fakeTarget(jsonOutcome(null), absent).target, TASK_ID);

    assert.deepEqual(read, { ok: true, value: { kind: "notFound", taskId: TASK_ID, reason: "NOT_CLASSIFIED" } });
  });

  test("workflowRun found and not found", async () => {
    const found = await workflowRun(fakeTarget(jsonOutcome(sample("WorkflowRunDetail"))).target, WORKFLOW_ID);
    const missing = await workflowRun(fakeTarget(jsonOutcome(null)).target, WORKFLOW_ID);

    assert.ok(found.ok && found.value.kind === "found");
    assert.deepEqual(missing, { ok: true, value: { kind: "notFound", workflowId: WORKFLOW_ID } });
  });

  test("workflowNode found and not found", async () => {
    const found = await workflowNode(fakeTarget(jsonOutcome(sample("WorkflowTaskDetail"))).target, WORKFLOW_ID, 2);
    const missing = await workflowNode(fakeTarget(jsonOutcome(null)).target, WORKFLOW_ID, 2);

    assert.ok(found.ok && found.value.kind === "found");
    assert.deepEqual(missing, { ok: true, value: { kind: "notFound", workflowId: WORKFLOW_ID, nodeIndex: 2 } });
  });
});

describe("expected failures are values", () => {
  test("a refusal of the core is QUERY_REFUSED with its field and reason", async () => {
    const outcome: MonitoringReadOutcome = { kind: "refused", field: "window", reason: "the window exceeds the 30-day maximum" };
    const read = await listTasks(fakeTarget(outcome).target, {});

    assert.deepEqual(read, {
      ok: false,
      error: { code: "QUERY_REFUSED", retryable: false, field: "window", reason: "the window exceeds the 30-day maximum" },
    });
  });

  test("a failed read is DB_OPERATION_FAILED with the retry rule of its cause", async () => {
    const read = await listTasks(fakeTarget({ kind: "failed", error: monitoringDbFailed("task list query", true) }).target, {});

    assert.ok(!read.ok && read.error.code === "DB_OPERATION_FAILED");
    assert.equal(read.error.retryable, true);
    assert.equal(read.error.message, "task list query failed: connection refused");
  });

  test("a stored row outside the read contract is HISTORY_CONTRACT_VIOLATION, never retryable", async () => {
    const error: RawCatalogError = {
      class: "MonitoringQueryHistoryContractViolation",
      slots: [
        textSlot("message", "history contract violation: status DONE"),
        {
          name: "exception",
          value: { kind: "exception", exception: { kind: "generated", class: "HistoryContractError", text: "status DONE" } },
        },
      ],
    };

    const read = await listTasks(fakeTarget({ kind: "failed", error }).target, {});

    assert.ok(!read.ok && read.error.code === "HISTORY_CONTRACT_VIOLATION");
    assert.equal(read.error.retryable, false);
    assert.deepEqual(read.error.exception, { kind: "historyContract", message: "status DONE" });
  });

  test("a read with no query to refuse makes a refusal of the bridge a defect", async () => {
    const outcome: MonitoringReadOutcome = { kind: "refused", field: "limit", reason: "no limit" };

    await assert.rejects(schedules(fakeTarget(outcome).target), { name: "ClientDefect", subject: "schedules" });
  });

  test("a failed detail read is the core's error", async () => {
    const failed: TaskDetailOutcome = { kind: "failed", error: monitoringDbFailed("task detail query", false) };
    const read = await taskDetail(fakeTarget(jsonOutcome(null), failed).target, TASK_ID);

    assert.ok(!read.ok && read.error.code === "DB_OPERATION_FAILED" && !read.error.retryable);
  });
});

describe("refusals before the call", () => {
  test("a task index outside 32 bits or with a fraction is QUERY_REFUSED", async () => {
    for (const index of [2 ** 31, -(2 ** 31) - 1, 1.5]) {
      const { target, calls } = fakeTarget(jsonOutcome(null));
      const read = await workflowNode(target, WORKFLOW_ID, index);

      assert.ok(!read.ok && read.error.code === "QUERY_REFUSED" && read.error.field === "nodeIndex", `${index}`);
      assert.equal(calls.size, 0);
    }
  });

  test("a page that is not a whole number is QUERY_REFUSED and the addon is not called", async () => {
    const { target, calls } = fakeTarget(jsonOutcome(null));
    const read = await listTasks(target, { offset: -0.5 });

    assert.ok(!read.ok && read.error.code === "QUERY_REFUSED" && read.error.field === "page");
    assert.equal(calls.size, 0);
  });
});

describe("thrown and defect ends", () => {
  test("a thrown envelope throws its class", async () => {
    const { target } = fakeTarget(jsonOutcome(null));

    const thrown: RelayedAddon<MonitoringReadsAddon> = {
      ...target.addon,
      readSchedules: async () => ({
        kind: "thrown",
        thrown: { code: "CORE_STOPPED", subject: "s", cause: "c", stateAfter: "noChange" },
      }),
    };

    await assert.rejects(schedules({ ...target, addon: thrown }), { name: "CoreStopped" });
  });

  test("a text that does not decode is a defect", async () => {
    await assert.rejects(taskStats(fakeTarget(jsonOutcome([new Map([["status", "PENDING"]])])).target, {}), { name: "ClientDefect" });
  });
});

describe("the timing variants of the core decode through the reads", () => {
  test("every attempt phases variant of a task detail", async () => {
    const detail = sample("TaskDetail");
    const attempts = withMember(detail, "attempts", [sample("TaskAttemptInfo")]);

    for (const phases of timingExamples("attempt_phases")) {
      const text = coreText(withMember(attempts, "attempts", [withMember(sample("TaskAttemptInfo"), "phases", phases)]));
      const read = await taskDetail(fakeTarget(jsonOutcome(null), { kind: "json", text }).target, TASK_ID);

      assert.ok(read.ok && read.value.kind === "found");
      assert.equal(read.value.taskDetail.attempts.length, 1);
    }
  });

  test("every run timing and node timing variant of a workflow run", async () => {
    const run = sample("WorkflowRunDetail");

    for (const timing of timingExamples("run_timings")) {
      const read = await workflowRun(fakeTarget(jsonOutcome(withMember(run, "timing", timing))).target, WORKFLOW_ID);

      assert.ok(read.ok && read.value.kind === "found");
    }

    for (const timing of timingExamples("node_timings")) {
      const node = withMember(sample("WorkflowNodeInfo"), "timing", timing);
      const read = await workflowRun(fakeTarget(jsonOutcome(withMember(run, "nodes", [node]))).target, WORKFLOW_ID);

      assert.ok(read.ok && read.value.kind === "found" && read.value.run.nodes.length === 1);
    }
  });
});
