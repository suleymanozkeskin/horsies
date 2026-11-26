// The relay of a seat isolate: the operation relay, the in-flight bound, the bound of the waiting operations and the
// transient failure past it, a refused core call, and an answer to an operation that does not wait. The cancel cells.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { SendsSeatOperation, SendsSeatReply } from "../../../horsies/src/app/core-access.ts";
import type { EnqueueRequest } from "../../../horsies/src/native/sends/addon.ts";
import { brokerReadError } from "../../../horsies/src/read/errors.ts";
import { enqueueFailed, taskInfoFailed } from "../../../horsies/src/seats/bound-answers.ts";
import {
  cancelStateOf,
  clearCancel,
  newCancelCells,
  requestCancel,
} from "../../../horsies/src/seats/cancel-cells.ts";
import { IsolateRelay, MAX_SEAT_OPS_IN_FLIGHT, MAX_SEAT_OPS_WAITING } from "../../../horsies/src/seats/relay.ts";
import type { SeatAnswer, SeatToMain } from "../../../horsies/src/seats/protocol.ts";
import { sendErrorOfRaw } from "../../../horsies/src/send/errors.ts";
import { ClientDefect, CoreStopped, TooManyPendingCalls } from "../../../horsies/src/thrown-errors.ts";
import { PREPARED, SENT_AT_MICROS, TASK_ID } from "../app/support.ts";
import { settled } from "./support.ts";

const TASK_INFO: SendsSeatOperation = { kind: "taskInfo", taskId: TASK_ID };

const REQUEST: EnqueueRequest = {
  taskId: TASK_ID,
  taskName: "crunch",
  queueName: "default",
  priority: 100,
  inputJson: '{"n":1}',
  sentAtMicros: SENT_AT_MICROS,
  deadline: { kind: "until", epochMicros: SENT_AT_MICROS + 60_000_000n },
  delay: { kind: "seconds", seconds: 30 },
  taskOptions: { kind: "text", text: '{"task_name":"crunch"}' },
  idempotencyKey: { kind: "none" },
  retention: { kind: "classKey", key: "days_30" },
};

const PREPARE: SendsSeatOperation = {
  kind: "prepareSend",
  taskName: "crunch",
  facts: { retention: { kind: "queueOrDefault" }, deadline: { kind: "noDeadline" }, inputBytes: 7 },
};

const ENQUEUE: SendsSeatOperation = { kind: "enqueue", request: REQUEST, sendKind: "FIRST", trace: { kind: "noCarrier" } };

function relayOnPosts(): { readonly relay: IsolateRelay; readonly posts: SeatToMain[] } {
  const posts: SeatToMain[] = [];

  return { relay: new IsolateRelay((message) => posts.push(message)), posts };
}

function sendsAnswer(reply: SendsSeatReply): SeatAnswer {
  return { kind: "reply", reply: { area: "sends", reply } };
}

function requestIds(posts: readonly SeatToMain[]): number[] {
  return posts.flatMap((post) => (post.kind === "op" ? [post.requestId] : []));
}

describe("the relay of a seat isolate", () => {
  test("an operation goes to the main thread as an op, and its reply is the answer", async () => {
    const { relay, posts } = relayOnPosts();
    const replied = relay.request("sends", PREPARE);

    assert.deepEqual(posts.map((post) => post.kind), ["op"]);
    relay.settle(1, sendsAnswer({ kind: "prepareSend", prepared: PREPARED }));
    assert.deepEqual(await replied, { kind: "prepareSend", prepared: PREPARED });
    assert.deepEqual(relay.size(), { inFlight: 0, waiting: 0 });
  });

  test("operations past the in-flight bound wait in order, and each answer sends the next", async () => {
    const { relay, posts } = relayOnPosts();
    const replies = Array.from({ length: MAX_SEAT_OPS_IN_FLIGHT + 2 }, async () => relay.request("sends", TASK_INFO));

    assert.deepEqual(requestIds(posts), [1, 2]);
    assert.deepEqual(relay.size(), { inFlight: 2, waiting: 2 });
    relay.settle(2, sendsAnswer({ kind: "taskInfo", read: { kind: "absent", reason: "NOT_CLASSIFIED" } }));
    assert.deepEqual(requestIds(posts), [1, 2, 3]);

    for (const id of [1, 3, 4]) {
      relay.settle(id, sendsAnswer({ kind: "taskInfo", read: { kind: "absent", reason: "NOT_CLASSIFIED" } }));
    }

    assert.equal((await Promise.all(replies)).length, MAX_SEAT_OPS_IN_FLIGHT + 2);
    assert.deepEqual(relay.size(), { inFlight: 0, waiting: 0 });
  });

  test("an operation past the bound of the waiting operations fails at once with its transient failure", async () => {
    const { relay, posts } = relayOnPosts();

    for (let index = 0; index < MAX_SEAT_OPS_IN_FLIGHT + MAX_SEAT_OPS_WAITING; index += 1) {
      void relay.request("sends", TASK_INFO);
    }

    const enqueued = await relay.request("sends", ENQUEUE);
    const read = await relay.request("sends", TASK_INFO);

    await assert.rejects(relay.request("sends", PREPARE), TooManyPendingCalls);
    assert.equal(posts.length, MAX_SEAT_OPS_IN_FLIGHT);
    assert.equal(enqueued.kind === "enqueue" && enqueued.outcome.kind === "failed" ? sendErrorOfRaw(enqueued.outcome.error).code : "", "ENQUEUE_FAILED");
    assert.equal(read.kind === "taskInfo" && read.read.kind === "failed" ? brokerReadError(read.read.error).kind : "", "database");
  });

  test("a refused core call rejects with the CoreError of its code", async () => {
    const { relay } = relayOnPosts();
    const replied = relay.request("sends", TASK_INFO);

    relay.settle(1, { kind: "refused", refusal: { code: "CORE_STOPPED", subject: "task_info", cause: "panic", stateAfter: "noChange" } });
    await assert.rejects(replied, CoreStopped);
  });

  test("an answer to an operation that does not wait is a defect", () => {
    const { relay } = relayOnPosts();

    assert.throws(() => {
      relay.settle(9, sendsAnswer({ kind: "taskInfo", read: { kind: "absent", reason: "NOT_CLASSIFIED" } }));
    }, ClientDefect);
  });
});

describe("the transient failures past the bound", () => {
  test("ENQUEUE_FAILED is retryable and holds the task id and the payload of the request", () => {
    const error = sendErrorOfRaw(enqueueFailed(REQUEST, "the bound"));

    assert.equal(error.code, "ENQUEUE_FAILED");
    assert.equal(error.code === "ENQUEUE_FAILED" ? error.retryable : false, true);
    assert.equal(error.code === "ENQUEUE_FAILED" ? error.taskId : "", TASK_ID);
    assert.deepEqual(error.code === "ENQUEUE_FAILED" ? { ...error.payload, sentAt: error.payload.sentAt.epochMilliseconds } : {}, {
      taskName: "crunch",
      queueName: "default",
      priority: 100,
      inputJson: '{"n":1}',
      sentAt: Number(SENT_AT_MICROS / 1000n),
      goodUntil: error.code === "ENQUEUE_FAILED" ? error.payload.goodUntil : null,
      enqueueDelaySeconds: 30,
      taskOptions: '{"task_name":"crunch"}',
      idempotencyKey: null,
      retentionClassKey: "days_30",
    });
  });

  test("a task read gets the retryable database failure of a task read", () => {
    assert.deepEqual(brokerReadError(taskInfoFailed("the bound")), {
      kind: "database",
      code: "TASK_INFO_QUERY_FAILED",
      retryable: true,
      message: "the bound",
    });
  });
});

describe("the cancel cells", () => {
  test("a cell reads not requested, then the first cause; a clear starts the next run with none", async () => {
    const cells = newCancelCells(2);

    assert.deepEqual(cancelStateOf(cells, 1), { kind: "notRequested" });
    requestCancel(cells, 1, "OPERATOR");
    requestCancel(cells, 1, "DEADLINE");
    assert.deepEqual(cancelStateOf(cells, 1), { kind: "requested", cause: "OPERATOR" });
    assert.deepEqual(cancelStateOf(cells, 0), { kind: "notRequested" });
    clearCancel(cells, 1);
    requestCancel(cells, 1, "DEADLINE");
    assert.deepEqual(cancelStateOf(cells, 1), { kind: "requested", cause: "DEADLINE" });
    await settled();
  });

  test("a value that no writer writes is a defect", () => {
    const cells = newCancelCells(1);

    Atomics.store(cells, 0, 9);
    assert.throws(() => cancelStateOf(cells, 0), ClientDefect);
  });
});
