// An app on a database, with sent tasks and started workflows, for the native tests that read or change what a send
// wrote. No worker runs: a sent task stays PENDING until a test cancels it.

import assert from "node:assert/strict";

import { appCoreHandle } from "../../../horsies/src/app/core-access.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import {
  defineTask,
  defineWorkflow,
  h,
  Horsies,
  type RerunInputDefault,
  TaskId,
  WorkflowId,
} from "../../../horsies/src/index.ts";
import type { RerunEnqueuePolicy } from "../../../horsies/src/rerun/types.ts";
import type { TaskSendOptions } from "../../../horsies/src/send/options.ts";

/** The task of the sent tasks of the native tests. */
export const sentTask = defineTask({ name: "sent_task_probe", input: { n: h.int() }, output: h.int() });

/** The task that the node of `sentWorkflow` runs. */
export const nodeTask = defineTask({ name: "sent_workflow_node_probe", input: { who: h.string() }, output: h.string() });

/** A workflow of one task node, `greet`. */
export const sentWorkflow = defineWorkflow(
  {
    name: "sent_workflow_probe",
    definitionKey: "tests.sent_workflow_probe.v1",
    outputType: h.string(),
    parameters: { who: h.string() },
    checkCases: [{ who: "a" }],
  },
  (workflow, { who }) => ({ output: workflow.task(nodeTask, { who }, { nodeId: "greet" }) }),
);

/** The policy of the new task of a rerun: kept forever, input kept, a key window of one day. */
export const KEEP_POLICY: RerunEnqueuePolicy = {
  retention: { kind: "forever" },
  retainRerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ days: 1 }),
};

/** The app of the database `databaseUrl` that stores the input of a send for a rerun as `retainInput` says. */
export function sentApp(databaseUrl: string, retainInput: RerunInputDefault): Horsies {
  const created = Horsies.create({
    database: { databaseUrl: { kind: "value", value: databaseUrl }, retainRerunInputDefault: retainInput },
    tasks: [sentTask, nodeTask],
    workflows: [sentWorkflow],
  });

  if (!created.ok) {
    assert.fail(`Horsies.create refused: ${created.error.refusals.map((refusal) => refusal.message).join("; ")}`);
  }

  return created.value;
}

/** Closes the pools of the app. */
export async function closeApp(app: Horsies): Promise<void> {
  await appCoreHandle(appInternals(app).core).close();
}

/** Sends `sentTask` with the input `n` and options `options`; the id of the new PENDING task. */
export async function sendPending(
  app: Horsies,
  n: number,
  options: TaskSendOptions = {},
): Promise<TaskId> {
  const sent = await app.send(sentTask, { n }, options);

  if (!sent.ok) {
    assert.fail(`the send failed: ${sent.error.code}: ${sent.error.message}`);
  }

  return sent.value.taskId;
}

/** Cancels the PENDING task `taskId`, so that it moves to history as CANCELLED. */
export async function cancelPending(app: Horsies, taskId: TaskId): Promise<void> {
  const cancelled = await app.monitoring.cancelTask(taskId, { policy: "pendingOrClaimedOnly" });

  assert.deepEqual(cancelled, { ok: true, value: { taskId, wasStatus: "PENDING" } });
}

/** A task sent and then cancelled: a terminal source for a rerun. */
export async function sendCancelled(app: Horsies, n: number): Promise<TaskId> {
  const taskId = await sendPending(app, n);

  await cancelPending(app, taskId);

  return taskId;
}

/** Starts `sentWorkflow`; its id. The root node task is PENDING. */
export async function startWorkflow(app: Horsies, who: string): Promise<WorkflowId> {
  const started = await app.start(sentWorkflow, { who });

  if (!started.ok) {
    assert.fail(`the start failed: ${started.error.code}: ${started.error.message}`);
  }

  return started.value.workflowId;
}

/** The id of the task that node 0 of the workflow `workflowId` runs. */
export async function nodeTaskId(app: Horsies, workflowId: WorkflowId): Promise<TaskId> {
  const read = await app.monitoring.getWorkflowNode(workflowId, 0);

  assert.ok(read.ok && read.value.kind === "found", `read node 0: ${read.ok ? read.value.kind : read.error.code}`);

  const row = read.value.node.taskRow;

  assert.ok(row !== null, "node 0 of a started workflow has a task row");

  const parsed = TaskId.parse(row.taskId);

  assert.ok(parsed.ok, "the stored task id is a canonical UUID");

  return parsed.value;
}

/** The pause between two probes of `pollUntil`, in milliseconds. */
const POLL_INTERVAL_MS = 100;

/** Probes `probe` every `POLL_INTERVAL_MS` until it gives a value, and gives that value. Fails the test with `subject` at `limitMs`. */
export async function pollUntil<T>(limitMs: number, subject: string, probe: () => Promise<T | undefined>): Promise<T> {
  const deadline = performance.now() + limitMs;

  for (;;) {
    const probed = await probe();

    if (probed !== undefined) {
      return probed;
    }

    if (performance.now() >= deadline) {
      assert.fail(`${subject}: not reached within ${String(limitMs)} ms`);
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}
