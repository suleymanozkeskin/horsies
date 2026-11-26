// A send and the task reads of task code in a task child go through the child host's producer handle (a pool of the
// child's `task_send_pool` connections, no overflow), not through the app's handle.
//
// What this file shows, and what it does not:
// - Only the core's child host fills the core's slot of the child producer, and a Node process cannot run a child
//   host before the child entry of the addon exists. So the probe exports of a `test-probes` build give the bridge's
//   choice (`ProducerHandle::chosen`) a child whose child host published a stand-in: a producer handle of the test
//   database with a pool of `TASK_SEND_POOL` connections and no overflow, built as the child host builds its handle.
// - The app's handle names a port where no server listens. The real `enqueue` on that handle fails. The same send
//   and both reads through the probes succeed, and the stand-in reports that an operation connected it. So in a task
//   child the operations of `enqueue`, `getResult` and `taskInfo` run on the child host's handle and open no
//   connection of the app's pools.
// - It does not count connections. The core's pool keeps the stand-in at `TASK_SEND_POOL` connections, and the core's
//   tests check that the child host sizes its handle from `task_send_pool` with no overflow. A count in a real Node
//   task child (`pg_stat_activity` by `application_name`) needs the child entry of the addon.
//
// Runs only where a `build:dev` addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { mintTaskId } from "../../../horsies/src/native/process-core/process-core.ts";
import type { EnqueueRequest } from "../../../horsies/src/native/sends/addon.ts";
import { enqueue } from "../../../horsies/src/native/sends/sends.ts";
import type { TraceArgument } from "../../../horsies/src/native/values/records.ts";
import { epochMicrosTowardPast } from "../../../horsies/src/send/options.ts";
import { childProducerProbeAddon } from "./child-producer-probe.ts";
import type { NativeProbeChildProducer, ProbeClose } from "./child-producer-probe.ts";
import { manifestJson, manifestJsonWithPool, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The `task_send_pool` of the stand-in: its pool size, with no overflow. */
const TASK_SEND_POOL = 2;

/** A database URL where no server listens: every operation on the app's handle fails to connect. */
const UNREACHABLE_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The task name of the sends of this file. No worker runs it. */
const TASK_NAME = "child_producer_probe";

/** How long the result read waits for the task, which stays `PENDING`. */
const RESULT_WAIT_MS = 100;

/** A send with no trace. */
const NO_CARRIER: TraceArgument = { kind: "noCarrier" };

/** The app's handle: its database cannot be reached. */
function unreachableAppHandle(): CoreHandle {
  return buildCoreHandle(manifestJson(UNREACHABLE_DATABASE_URL, false), "PRODUCER");
}

/** A new stand-in of the child host's handle: the test database, `TASK_SEND_POOL` connections, no overflow. */
function newStandIn(): NativeProbeChildProducer {
  const manifest = manifestJsonWithPool(testDatabaseUrl(), false, "MIGRATE_AND_VALIDATE", {
    size: TASK_SEND_POOL,
    maxOverflow: 0,
  });

  return envelopeValue(childProducerProbeAddon().probeChildProducer(manifest));
}

/** Run `body` with a new stand-in, then close the stand-in; how the close ended. */
async function withStandIn(body: (child: NativeProbeChildProducer) => Promise<void>): Promise<ProbeClose> {
  const child = newStandIn();

  try {
    await body(child);
  } catch (failure) {
    await child.close();

    throw failure;
  }

  return envelopeValue(await child.close());
}

/** A new task id. */
function newTaskId(): string {
  const minted = mintTaskId();

  if (minted.kind !== "minted") {
    assert.fail(`the core refused to mint a task id: ${minted.cause}`);
  }

  return minted.taskId;
}

/** The first send of a task with the input `{}` and no options. */
function sendRequest(taskId: string): EnqueueRequest {
  return {
    taskId,
    taskName: TASK_NAME,
    queueName: "default",
    priority: 100,
    inputJson: "{}",
    sentAtMicros: epochMicrosTowardPast(Temporal.Now.instant()),
    deadline: { kind: "noDeadline" },
    delay: { kind: "noDelay" },
    taskOptions: { kind: "none" },
    idempotencyKey: { kind: "none" },
    retention: { kind: "classKey", key: "standard_30d" },
  };
}

describe("the producer handle of a task child", () => {
  test("a send in a child is stored through the child host's handle; the app's handle cannot store it", async () => {
    const app = unreachableAppHandle();
    const taskId = newTaskId();

    const closed = await withStandIn(async (child) => {
      const sent = envelopeValue(
        await childProducerProbeAddon().probeEnqueueInChild(app.native, child, sendRequest(taskId), "FIRST", NO_CARRIER),
      );

      assert.deepEqual(sent, { kind: "enqueued", taskId });
    });

    assert.equal(closed, "CLOSED");

    const direct = await enqueue(app, sendRequest(newTaskId()), "FIRST", NO_CARRIER);

    assert.equal(direct.kind === "failed" ? direct.error.class : direct.kind, "TaskSendEnqueueFailed");
  });

  test("the result read and the info read in a child go through the child host's handle", async () => {
    const app = unreachableAppHandle();
    const taskId = newTaskId();

    const closed = await withStandIn(async (child) => {
      const probe = childProducerProbeAddon();

      const sent = envelopeValue(
        await probe.probeEnqueueInChild(app.native, child, sendRequest(taskId), "FIRST", NO_CARRIER),
      );

      assert.equal(sent.kind, "enqueued");

      const info = envelopeValue(await probe.probeTaskInfoInChild(app.native, child, taskId));

      assert.equal(info.kind === "found" ? info.record.status : info.kind, "PENDING");

      const result = envelopeValue(
        await probe.probeGetResultInChild(app.native, child, taskId, { kind: "upTo", milliseconds: RESULT_WAIT_MS }),
      );

      assert.deepEqual(result, {
        kind: "found",
        record: { taskId, taskName: TASK_NAME, envelope: { kind: "waitEnded", status: "PENDING" } },
      });
    });

    assert.equal(closed, "CLOSED");
  });
});
