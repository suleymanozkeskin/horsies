// The monitoring area of the app: each member calls its addon function with the app's handle, and the defaults of the
// optional arguments.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createMonitoringArea, type MonitoringAddons } from "../../../horsies/src/monitoring/app-area.ts";
import type { CancelAddon } from "../../../horsies/src/native/facades/cancel.ts";
import type { HealthAddon } from "../../../horsies/src/native/facades/health.ts";
import type { MonitoringReadsAddon } from "../../../horsies/src/native/facades/monitoring-reads.ts";
import type { NativeCoreHandle } from "../../../horsies/src/native/handle/addon.ts";
import { AddonOutsideMainThread } from "../../../horsies/src/thrown-errors.ts";
import {
  coreText,
  fakeHandle,
  mainThreadAccess,
  sample,
  taskIdOf,
  valueEnvelope,
  workflowIdOf,
} from "./support/fakes.ts";

const ID_TEXT = "01890a5d-ac96-774b-bcce-b302099a8057";
const TASK_ID = taskIdOf(ID_TEXT);
const WORKFLOW_ID = workflowIdOf(ID_TEXT);

/** Fake addons that log the name of each called function, and check the handle. */
function fakeAddons(handle: NativeCoreHandle, called: string[]): MonitoringAddons {
  const json = (name: string, text: string) => async (given: NativeCoreHandle) => {
    assert.equal(given, handle);
    called.push(name);

    return valueEnvelope({ kind: "json" as const, text });
  };

  const done = <T>(name: string, record: T) => async (given: NativeCoreHandle) => {
    assert.equal(given, handle);
    called.push(name);

    return valueEnvelope({ kind: "done" as const, record });
  };

  const reads: MonitoringReadsAddon = {
    readTaskStats: json("readTaskStats", "[]"),
    readTaskFacets: json("readTaskFacets", coreText(sample("Facets"))),
    readTaskBreakdown: json("readTaskBreakdown", coreText(sample("Breakdown"))),
    readTaskList: json("readTaskList", coreText(sample("TaskListPage"))),
    readWorkflowNames: json("readWorkflowNames", "[]"),
    readWorkflowRuns: json("readWorkflowRuns", "[]"),
    readWorkflowRun: json("readWorkflowRun", "null"),
    readWorkflowNode: json("readWorkflowNode", "null"),
    readSchedules: json("readSchedules", "[]"),
    readTaskDetail: json("readTaskDetail", coreText(sample("TaskDetail"))),
  };

  const health: HealthAddon = {
    pingWorkers: done("pingWorkers", []),
    listWorkerStates: done("listWorkerStates", []),
    getWorkerState: done("getWorkerState", { kind: "notFound" as const }),
    getWorkerStateHistory: async (given, _workerId, limit) => {
      assert.deepEqual(limit, { kind: "allRetained" });

      return done("getWorkerStateHistory", [])(given);
    },
    getStaleTasks: async (given, thresholdMinutes) => {
      assert.equal(thresholdMinutes, 15);

      return done("getStaleTasks", { kind: "complete" as const, rows: [] })(given);
    },
    getExpiredTasks: done("getExpiredTasks", { kind: "complete" as const, rows: [] }),
    getClassCoverage: done("getClassCoverage", { checkedAtMicros: 0n, liveWorkers: 0n, streams: [] }),
    getClassReport: done("getClassReport", { checkedAtMicros: 0n, streams: [], workers: [] }),
  };

  const cancel: CancelAddon = {
    cancelTask: async (given, taskId) => done("cancelTask", { taskId, wasStatus: "CLAIMED" })(given),
  };

  return { reads: () => reads, health: () => health, cancel: () => cancel };
}

describe("createMonitoringArea", () => {
  test("each member calls its addon function with the app's handle", async () => {
    const called: string[] = [];
    const native: NativeCoreHandle = { ...fakeHandle(), pingDatabase: async () => valueEnvelope({ kind: "done" as const, record: { latencyMs: 1 } }) };
    const area = createMonitoringArea(mainThreadAccess(native), fakeAddons(native, called));
    const { monitoring } = area;

    const answers = [
      await monitoring.taskStats(),
      await monitoring.taskFacets(),
      await monitoring.taskBreakdown({ groupBy: "taskName" }),
      await monitoring.listTasks(),
      await monitoring.taskDetail(TASK_ID),
      await monitoring.workflowNames(),
      await monitoring.workflowRuns(),
      await monitoring.workflowRun(WORKFLOW_ID),
      await monitoring.workflowNode(WORKFLOW_ID, 0),
      await monitoring.schedules(),
      await monitoring.workers(),
      await monitoring.worker("w1"),
      await monitoring.workerHistory("w1"),
      await monitoring.staleTasks({ thresholdMinutes: 15 }),
      await monitoring.expiredTasks(),
      await monitoring.classCoverage(),
      await monitoring.classReport(),
      await monitoring.cancelTask(TASK_ID, { policy: "includeRunning" }),
      await area.pingWorkers(),
      await area.pingDatabase(),
    ];

    assert.ok(answers.every((answer) => answer.ok));
    assert.deepEqual(called, [
      "readTaskStats",
      "readTaskFacets",
      "readTaskBreakdown",
      "readTaskList",
      "readTaskDetail",
      "readWorkflowNames",
      "readWorkflowRuns",
      "readWorkflowRun",
      "readWorkflowNode",
      "readSchedules",
      "listWorkerStates",
      "getWorkerState",
      "getWorkerStateHistory",
      "getStaleTasks",
      "getExpiredTasks",
      "getClassCoverage",
      "getClassReport",
      "cancelTask",
      "pingWorkers",
    ]);
  });

  test("the area and its monitoring member are frozen", () => {
    const native = fakeHandle();
    const area = createMonitoringArea(mainThreadAccess(native), fakeAddons(native, []));

    assert.ok(Object.isFrozen(area) && Object.isFrozen(area.monitoring));
  });

  test("a thread with no addon throws AddonOutsideMainThread at the call, not at the build", async () => {
    const native = fakeHandle();
    const area = createMonitoringArea({ kind: "noCore" }, fakeAddons(native, []));

    await assert.rejects(area.monitoring.listTasks(), AddonOutsideMainThread);
    await assert.rejects(area.pingDatabase(), AddonOutsideMainThread);
  });
});
