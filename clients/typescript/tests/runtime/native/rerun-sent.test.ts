// `app.rerunTask` on sent tasks, with a built addon and a database: each outcome and error that needs a source task.
// A source is a task that a test sent and cancelled, so it is in history; no worker runs. `COMPLETED_SOURCE` needs a
// completed task: it is an end-to-end case (`tests/e2e/layers`).
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, describe, test } from "node:test";

import type { Horsies, TaskId } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import type { RerunCommand, RerunOutcome } from "../../../horsies/src/rerun/types.ts";
import { testDatabaseUrl } from "./manifest.ts";
import {
  closeApp,
  KEEP_POLICY,
  nodeTaskId,
  sendCancelled,
  sendPending,
  sentApp,
  sentWorkflow,
  startWorkflow,
} from "./sent-support.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest key that the core accepts, in UTF-8 bytes. */
const KEY_MAX_BYTES = 255;

/** A key that no other test uses. */
function newKey(): string {
  return `rerun-sent-${randomUUID()}`;
}

/** The command that reruns `sourceTaskId`, with `idempotencyKey` when given. */
function commandFor(sourceTaskId: TaskId, idempotencyKey?: string): RerunCommand {
  return idempotencyKey === undefined
    ? { sourceTaskId, goodUntil: { kind: "none" } }
    : { sourceTaskId, goodUntil: { kind: "none" }, idempotencyKey };
}

/** The outcome of the rerun of `command` in the app's transaction, or the failed test with the error. */
async function outcomeOf(app: Horsies, command: RerunCommand): Promise<RerunOutcome> {
  const answer = await app.rerunTask(command, KEEP_POLICY);

  if (!answer.ok) {
    assert.fail(`the rerun failed: ${answer.error.code}: ${answer.error.message}`);
  }

  return answer.value;
}

/** The id of the new task of an `enqueued` outcome, or the failed test. */
function enqueuedId(outcome: RerunOutcome): TaskId {
  assert.equal(outcome.kind, "enqueued", JSON.stringify(outcome));

  return outcome.kind === "enqueued" ? outcome.newTaskId : assert.fail("unreachable");
}

describe("app.rerunTask on sent tasks", () => {
  let keeping: Horsies;
  let dropping: Horsies;

  before(() => {
    keeping = sentApp(testDatabaseUrl(), "keep");
    dropping = sentApp(testDatabaseUrl(), "drop");
  });

  after(async () => {
    await closeApp(keeping);
    await closeApp(dropping);
  });

  test("enqueued: a cancelled PENDING task whose input is kept gives a new PENDING task", async () => {
    const source = await sendCancelled(keeping, 10);
    const outcome = await outcomeOf(keeping, commandFor(source));
    const newTaskId = enqueuedId(outcome);

    assert.deepEqual(outcome, { kind: "enqueued", newTaskId, sourceTaskId: source, rerunRootTaskId: source });
    assert.notEqual(newTaskId, source);

    const detail = await keeping.monitoring.getTaskDetail(newTaskId);

    assert.ok(detail.ok && detail.value.kind === "found", JSON.stringify(detail));
    assert.equal(detail.value.taskDetail.taskRow.status, "PENDING");
  });

  test("sourceLive: a PENDING task", async () => {
    const source = await sendPending(keeping, 11);

    assert.deepEqual(await outcomeOf(keeping, commandFor(source)), { kind: "sourceLive", taskId: source });
  });

  test("notEligible with WORKFLOW_TASK: the task of a cancelled workflow", async () => {
    const workflowId = await startWorkflow(keeping, "rerun");
    const source = await nodeTaskId(keeping, workflowId);
    const cancelled = await keeping.workflowHandle(sentWorkflow, workflowId).cancel();

    assert.ok(cancelled.ok, "the workflow cancels");
    assert.deepEqual(await outcomeOf(keeping, commandFor(source)), { kind: "notEligible", taskId: source, reason: "WORKFLOW_TASK" });
  });

  test("inputUnavailable: a task sent with its input dropped", async () => {
    const source = await sendCancelled(dropping, 12);

    assert.deepEqual(await outcomeOf(dropping, commandFor(source)), {
      kind: "inputUnavailable",
      taskId: source,
      disposition: "DECLINED_BY_POLICY",
    });
  });

  test("keyConflict: two sources with one key; the second names the task that holds the key", async () => {
    const first = await sendCancelled(keeping, 13);
    const second = await sendCancelled(keeping, 14);
    const key = newKey();
    const owner = enqueuedId(await outcomeOf(keeping, commandFor(first, key)));

    assert.deepEqual(await outcomeOf(keeping, commandFor(second, key)), { kind: "keyConflict", taskId: second, ownerTaskId: owner });
  });

  test("keyReplay: the same keyed command twice gives the first new task", async () => {
    const source = await sendCancelled(keeping, 15);
    const key = newKey();
    const created = enqueuedId(await outcomeOf(keeping, commandFor(source, key)));

    assert.deepEqual(await outcomeOf(keeping, commandFor(source, key)), { kind: "keyReplay", existingTaskId: created });
  });

  test("IDEMPOTENCY_KEY_REFUSED: an empty key, and a key of 256 UTF-8 bytes, on an eligible source", async () => {
    const source = await sendCancelled(keeping, 16);

    for (const key of ["", "k".repeat(KEY_MAX_BYTES + 1)]) {
      const answer = await keeping.rerunTask(commandFor(source, key), KEEP_POLICY);

      assert.ok(!answer.ok && answer.error.code === "IDEMPOTENCY_KEY_REFUSED", `${String(key.length)}: ${JSON.stringify(answer)}`);
      assert.equal(answer.error.retryable, false);
    }
  });

  test("a key of 255 UTF-8 bytes is accepted", async () => {
    const source = await sendCancelled(keeping, 17);
    const key = newKey().padEnd(KEY_MAX_BYTES, "k");

    enqueuedId(await outcomeOf(keeping, commandFor(source, key)));
  });
});
