// The rerun in the app's transaction with a built addon and a database: the outcomes and the errors
// that need no sent task. A refused request answers before the connect.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { before, describe, test } from "node:test";

import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { TaskId } from "../../../horsies/src/contract/ids.ts";
import { mintTaskId } from "../../../horsies/src/native/process-core/process-core.ts";
import { NO_ACTIVE_CARRIER, createRerunArea } from "../../../horsies/src/rerun/app-area.ts";
import type { RerunArea } from "../../../horsies/src/rerun/app-area.ts";
import type { RerunCommand, RerunPolicy } from "../../../horsies/src/rerun/types.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

/** A database URL whose connect is refused at once: nothing listens on port 1. */
const REFUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** A retention period that no install of the tests registers. */
const UNREGISTERED_DAYS = 1777;

/** A policy that keeps the new task forever, with a key window of one day. */
const POLICY: RerunPolicy = {
  retention: { kind: "forever" },
  rerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ days: 1 }),
};

/** The rerun area of a handle of `databaseUrl`, with no active span. */
function areaOf(handle: CoreHandle): RerunArea {
  return createRerunArea({ coreHandle: () => handle, activeCarrier: () => NO_ACTIVE_CARRIER });
}

/** The task id of a canonical UUID text. */
function taskIdOf(text: string): TaskId {
  const parsed = TaskId.parse(text);

  assert.ok(parsed.ok, "a canonical UUID text");

  return parsed.value;
}

/** A UUIDv7 that no task has: minted now and never sent. */
function unsentTaskId(): TaskId {
  const minted = mintTaskId();

  assert.ok(minted.kind === "minted");

  return taskIdOf(minted.taskId);
}

/** An unkeyed command for `sourceTaskId` with no deadline. */
function commandFor(sourceTaskId: TaskId): RerunCommand {
  return { sourceTaskId, deadline: { kind: "none" } };
}

describe("app.rerun with a database", () => {
  let area: RerunArea;

  before(async () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal((await handle.installSchema()).kind, "installed");
    area = areaOf(handle);
  });

  test("an unsent UUIDv7 is sourceAbsent within the retained history", async () => {
    const taskId = unsentTaskId();

    assert.deepEqual(await area.rerun(commandFor(taskId), POLICY), {
      ok: true,
      value: { kind: "sourceAbsent", taskId, reason: "WITHIN_RETAINED_HISTORY" },
    });
  });

  test("a UUIDv4 is sourceAbsent and not classified", async () => {
    const taskId = taskIdOf(randomUUID());

    assert.deepEqual(await area.rerun(commandFor(taskId), POLICY), {
      ok: true,
      value: { kind: "sourceAbsent", taskId, reason: "NOT_CLASSIFIED" },
    });
  });

  test("a period with no class row is CLASS_NOT_REGISTERED, before the source is read", async () => {
    const answer = await area.rerun(commandFor(unsentTaskId()), {
      ...POLICY,
      retention: { kind: "days", days: UNREGISTERED_DAYS },
    });

    assert.ok(!answer.ok && answer.error.code === "CLASS_NOT_REGISTERED", JSON.stringify(answer));
    assert.equal(answer.error.classKey, `standard_${UNREGISTERED_DAYS}d`);
    assert.equal(answer.error.retryable, false);
  });
});

describe("app.rerun with a database that cannot be reached", () => {
  const area = areaOf(buildCoreHandle(manifestJson(REFUSED_DATABASE_URL, false), "PRODUCER"));

  test("CONNECT_FAILED is retryable, with the connection cause", async () => {
    const answer = await area.rerun(commandFor(taskIdOf(randomUUID())), POLICY);

    assert.ok(!answer.ok && answer.error.code === "CONNECT_FAILED", JSON.stringify(answer));
    assert.equal(answer.error.retryable, true);
    assert.equal(answer.error.exception.subject, "rerun_task");
  });

  test("a window of 0 and of 31 days is INVALID_POLICY, not the connect failure", async () => {
    for (const days of [0, 31]) {
      const answer = await area.rerun(commandFor(taskIdOf(randomUUID())), {
        ...POLICY,
        reservationWindow: Temporal.Duration.from({ days }),
      });

      assert.equal(!answer.ok && answer.error.code, "INVALID_POLICY", `${days}`);
    }
  });
});
