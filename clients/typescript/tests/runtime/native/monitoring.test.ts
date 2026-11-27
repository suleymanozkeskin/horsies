// The monitoring reads, the health reads, the pings and the cancel with a built addon and a database. An installed
// empty database answers each read; an unreachable database gives each read's failure code; a value out of its
// bounds is refused before any connect. The cases that need a sent task or a running worker are end-to-end cases.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CoreAccess } from "../../../horsies/src/app/core-access.ts";
import { TaskId, WorkflowId } from "../../../horsies/src/contract/ids.ts";
import { createMonitoringArea, type MonitoringArea } from "../../../horsies/src/monitoring/app-area.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle, type CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { processCoreAddon } from "../../../horsies/src/native/process-core/addon.ts";
import { nativeValue } from "../../../horsies/src/native/call/envelope.ts";
import { noWorkflowCore } from "../../../horsies/src/workflow/core.ts";
import { manifestJson, manifestJsonWithSchema, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** A database URL whose connect is refused at once: nothing listens on port 1. */
const REFUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** A UUIDv4 text: the detail read cannot classify the absence of its task. */
const UUID_V4_TEXT = "4b6d3a2e-8c1f-4e7a-9b5d-2f0c6e8a1d3b";

/** The window of the pings of these tests, in milliseconds. */
const PING_WINDOW_MS = 200;

/** A parsed id, or a failed test. */
function parsed<T>(result: { readonly ok: true; readonly value: T } | { readonly ok: false }): T {
  assert.ok(result.ok, "the fixture id is a canonical UUID text");

  return result.value;
}

const UUID_V4 = parsed(TaskId.parse(UUID_V4_TEXT));
const WORKFLOW_V4 = parsed(WorkflowId.parse(UUID_V4_TEXT));

/** The core access of a main thread with `handle`; these tests make no send. */
function access(handle: CoreHandle): CoreAccess {
  const noSend = async (): Promise<never> => assert.fail("the monitoring tests make no send");

  return {
    kind: "addon",
    handle,
    producer: {
      prepareSend: noSend,
      enqueue: noSend,
      getResult: noSend,
      taskInfo: noSend,
      processRole: noSend,
      workflows: noWorkflowCore(),
    },
  };
}

/** The area of an app whose manifest names `databaseUrl`. */
function area(databaseUrl: string): MonitoringArea {
  return createMonitoringArea(access(buildCoreHandle(manifestJson(databaseUrl, false), "PRODUCER")));
}

/** The area of an installed test database. */
async function installedArea(): Promise<MonitoringArea> {
  const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");
  const installed = await handle.installSchema();

  assert.equal(installed.kind, "installed");

  return createMonitoringArea(access(handle));
}

describe("the reads of an installed database", () => {
  test("each monitoring read answers its model", async () => {
    const { monitoring } = await installedArea();
    const stats = await monitoring.taskStats();

    assert.ok(stats.ok && stats.value.length === 7);
    assert.ok((await monitoring.taskFacets()).ok);
    assert.ok((await monitoring.taskBreakdown({ groupBy: "queue" })).ok);

    const page = await monitoring.listTasks({ statuses: ["FAILED"], limit: 50 });

    assert.ok(page.ok && page.value.total === 0);
    assert.deepEqual(await monitoring.listWorkflowNames(), { ok: true, value: [] });
    assert.deepEqual(await monitoring.listWorkflowRuns(), { ok: true, value: [] });
    assert.deepEqual(await monitoring.listSchedules(), { ok: true, value: [] });
  });

  test("each health read answers its value", async () => {
    const { monitoring } = await installedArea();

    assert.deepEqual(await monitoring.listWorkerStates(), { ok: true, value: [] });
    assert.deepEqual(await monitoring.getWorkerState("no-such-worker"), {
      ok: true,
      value: { kind: "notFound", workerId: "no-such-worker" },
    });
    assert.deepEqual(await monitoring.getWorkerStateHistory("no-such-worker"), { ok: true, value: [] });
    assert.deepEqual(await monitoring.getStaleTasks({ thresholdMinutes: 5 }), { ok: true, value: { kind: "complete", rows: [] } });
    assert.deepEqual(await monitoring.getExpiredTasks(), { ok: true, value: { kind: "complete", rows: [] } });
    assert.ok((await monitoring.getExecutionClassCoverage()).ok);
    assert.ok((await monitoring.getExecutionClassReport()).ok);
  });

  test("taskDetail of an absent task names why: NOT_CLASSIFIED for a UUIDv4, WITHIN_RETAINED_HISTORY for a UUIDv7", async () => {
    const { monitoring } = await installedArea();
    const minted = nativeValue("mintTaskId", () => processCoreAddon().mintTaskId());

    assert.ok(minted.kind === "minted");
    assert.deepEqual(await monitoring.getTaskDetail(UUID_V4), {
      ok: true,
      value: { kind: "notFound", taskId: UUID_V4, reason: "NOT_CLASSIFIED" },
    });

    const read = await monitoring.getTaskDetail(parsed(TaskId.parse(minted.taskId)));

    assert.ok(read.ok && read.value.kind === "notFound" && read.value.reason === "WITHIN_RETAINED_HISTORY");
  });

  test("an unknown workflow run and node are notFound", async () => {
    const { monitoring } = await installedArea();

    assert.deepEqual(await monitoring.getWorkflowRun(WORKFLOW_V4), {
      ok: true,
      value: { kind: "notFound", workflowId: WORKFLOW_V4 },
    });
    assert.deepEqual(await monitoring.getWorkflowNode(WORKFLOW_V4, 0), {
      ok: true,
      value: { kind: "notFound", workflowId: WORKFLOW_V4, taskIndex: 0 },
    });
  });

  test("a cancel of an unknown id is TASK_NOT_FOUND", async () => {
    const { monitoring } = await installedArea();
    const answer = await monitoring.cancelTask(UUID_V4, { policy: "pendingOrClaimedOnly" });

    assert.ok(!answer.ok && answer.error.code === "TASK_NOT_FOUND");
  });

  test("the database ping answers its latency, and a ping of no worker answers no pong after its window", async () => {
    const answered = await installedArea();
    const ping = await answered.pingDatabase();

    assert.ok(ping.ok && ping.value.latencyMs >= 0);
    assert.deepEqual(await answered.pingWorkers({ waitMs: PING_WINDOW_MS }), { ok: true, value: [] });
  });
});

describe("an unreachable database", () => {
  test("each monitoring read is DATABASE_OPERATION_FAILED with the retry rule of the cause", async () => {
    const { monitoring } = area(REFUSED_DATABASE_URL);

    const reads = [
      ["task stats query", await monitoring.taskStats()],
      ["task list query", await monitoring.listTasks()],
      ["task detail query", await monitoring.getTaskDetail(UUID_V4)],
      ["workflow names query", await monitoring.listWorkflowNames()],
      ["schedule state query", await monitoring.listSchedules()],
    ] as const;

    for (const [subject, read] of reads) {
      assert.ok(!read.ok && read.error.code === "DATABASE_OPERATION_FAILED", subject);
      assert.ok(read.error.retryable, subject);
      assert.ok(read.error.message.startsWith(`${subject} failed:`), read.error.message);
    }
  });

  test("each health read, each ping and the cancel give their failure codes", async () => {
    const unreachable = area(REFUSED_DATABASE_URL);
    const workers = await unreachable.monitoring.listWorkerStates();
    const coverage = await unreachable.monitoring.getExecutionClassCoverage();
    const ping = await unreachable.pingDatabase();
    const pongs = await unreachable.pingWorkers({ waitMs: PING_WINDOW_MS });
    const cancel = await unreachable.monitoring.cancelTask(UUID_V4, { policy: "includeRunning" });

    assert.ok(!workers.ok && workers.error.code === "MONITORING_QUERY_FAILED");
    assert.ok(!coverage.ok && coverage.error.code === "MONITORING_QUERY_FAILED");
    assert.ok(!ping.ok && ping.error.code === "DATABASE_PING_FAILED");
    assert.ok(!pongs.ok && pongs.error.code === "WORKER_PING_FAILED");
    assert.ok(!cancel.ok && cancel.error.code === "DATABASE_OPERATION_FAILED" && cancel.error.retryable);
  });

  test("each query bound is invalidInput before the connect", async () => {
    const { monitoring } = area(REFUSED_DATABASE_URL);
    const until = Temporal.Now.instant();

    const refusals = [
      ["window", await monitoring.listTasks({ window: { since: until, until } })],
      ["window", await monitoring.taskStats({ window: { since: until.subtract({ hours: 31 * 24 }), until } })],
      ["page", await monitoring.listTasks({ offset: -1 })],
      ["page", await monitoring.listTasks({ limit: 0 })],
      ["page", await monitoring.listTasks({ limit: 201 })],
      ["page", await monitoring.listTasks({ offset: 400, limit: 101 })],
      ["limit", await monitoring.taskBreakdown({ groupBy: "worker", limit: 0 })],
      ["limit", await monitoring.taskBreakdown({ groupBy: "worker", limit: 501 })],
      ["limit", await monitoring.listWorkflowRuns({ limit: 0 })],
      ["limit", await monitoring.listWorkflowRuns({ limit: 201 })],
      ["taskIndex", await monitoring.getWorkflowNode(WORKFLOW_V4, 2 ** 31)],
    ] as const;

    for (const [field, read] of refusals) {
      assert.ok(!read.ok && read.error.kind === "invalidInput" && "field" in read.error && read.error.field === field, field);
    }
  });

  test("a count or a threshold out of its rule is INVALID_INPUT before the connect", async () => {
    const unreachable = area(REFUSED_DATABASE_URL);

    const answers = [
      await unreachable.pingWorkers({ waitMs: 0 }),
      await unreachable.monitoring.getStaleTasks({ thresholdMinutes: -1 }),
      await unreachable.monitoring.getStaleTasks({ thresholdMinutes: 525_601 }),
      await unreachable.monitoring.getWorkerStateHistory("w1", { kind: "newest", count: 0 }),
    ];

    for (const answer of answers) {
      assert.ok(!answer.ok && answer.error.code === "INVALID_INPUT");
    }
  });

  test("a ping on a handle with no listener is LISTENER_DISABLED before the connect", async () => {
    const handle = buildCoreHandle(manifestJsonWithSchema(REFUSED_DATABASE_URL, false, "ASSUME_INITIALIZED"), "PRODUCER");
    const answer = await createMonitoringArea(access(handle)).pingWorkers({ waitMs: PING_WINDOW_MS });

    assert.ok(!answer.ok && answer.error.code === "LISTENER_DISABLED");
  });
});
