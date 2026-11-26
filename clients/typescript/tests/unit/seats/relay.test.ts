// The relay of a seat isolate: the operation relay, the in-flight bound, an operation past the bound of the waiting
// operations and a prepare past its own bound (`seatRelayFull`, nothing sent), the send and the task info read at the
// bound (`SEAT_RELAY_FULL` through their own error unions), a post that throws takes no place, a refused core call, and
// an answer to an operation that does not wait. The cancel cells.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { SendsSeatOperation, SendsSeatReply } from "../../../horsies/src/app/core-access.ts";
import { taskIdOf } from "../monitoring/support/fakes.ts";
import type { EnqueueRequest } from "../../../horsies/src/native/sends/addon.ts";
import { readFormOf, taskHandleOf } from "../../../horsies/src/read/handle.ts";
import {
  cancelStateOf,
  clearCancel,
  newCancelCells,
  requestCancel,
} from "../../../horsies/src/seats/cancel-cells.ts";
import { IsolateRelay } from "../../../horsies/src/seats/relay.ts";
import type { SeatAnswer, SeatToMain } from "../../../horsies/src/seats/protocol.ts";
import { SeatRelayCode } from "../../../horsies/src/_catalog/task_send.ts";
import type { SeatRelayFull } from "../../../horsies/src/seats/relay-full.ts";
import { sendTask } from "../../../horsies/src/send/send.ts";
import { ClientDefect, CoreStopped } from "../../../horsies/src/thrown-errors.ts";
import { PREPARED, SENT_AT_MICROS, TASK_ID } from "../app/support.ts";
import { contextOf, crunch, seatApp, settled, TEST_RELAY_BOUNDS } from "./support.ts";

const { opsInFlight: IN_FLIGHT, opsWaiting: WAITING } = TEST_RELAY_BOUNDS;

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

  return { relay: new IsolateRelay((message) => posts.push(message), TEST_RELAY_BOUNDS), posts };
}

function sendsAnswer(reply: SendsSeatReply): SeatAnswer {
  return { kind: "reply", reply: { area: "sends", reply } };
}

function requestIds(posts: readonly SeatToMain[]): number[] {
  return posts.flatMap((post) => (post.kind === "op" ? [post.requestId] : []));
}

/** The operation kind of each `op` post. */
function operationKinds(posts: readonly SeatToMain[]): string[] {
  return posts.flatMap((post) => (post.kind === "op" && post.request.area === "sends" ? [post.request.operation.kind] : []));
}

describe("the relay of a seat isolate", () => {
  test("an operation goes to the main thread as an op, and its reply is the answer", async () => {
    const { relay, posts } = relayOnPosts();
    const replied = relay.request("sends", PREPARE);

    assert.deepEqual(posts.map((post) => post.kind), ["op"]);
    relay.settle(1, sendsAnswer({ kind: "prepareSend", prepared: PREPARED }));
    assert.deepEqual(await replied, { kind: "reply", reply: { kind: "prepareSend", prepared: PREPARED } });
    assert.deepEqual(relay.size(), { inFlight: 0, waiting: 0, sent: 0 });
  });

  test("operations past the in-flight bound wait in order, and each answer sends the next", async () => {
    const { relay, posts } = relayOnPosts();
    const replies = Array.from({ length: IN_FLIGHT + 2 }, async () => relay.request("sends", TASK_INFO));

    assert.deepEqual(requestIds(posts), [1, 2]);
    assert.deepEqual(relay.size(), { inFlight: 2, waiting: 2, sent: 2 });
    relay.settle(2, sendsAnswer({ kind: "taskInfo", read: { kind: "absent", reason: "NOT_CLASSIFIED" } }));
    assert.deepEqual(requestIds(posts), [1, 2, 3]);

    for (const id of [1, 3, 4]) {
      relay.settle(id, sendsAnswer({ kind: "taskInfo", read: { kind: "absent", reason: "NOT_CLASSIFIED" } }));
    }

    assert.equal((await Promise.all(replies)).length, IN_FLIGHT + 2);
    assert.deepEqual(relay.size(), { inFlight: 0, waiting: 0, sent: 0 });
  });

  test("a post that throws takes no place: a later operation is sent", async () => {
    let refuse = true;
    const posts: SeatToMain[] = [];

    const relay = new IsolateRelay((message) => {
      if (refuse) {
        throw new TypeError("DataCloneError: the value does not clone");
      }

      posts.push(message);
    }, TEST_RELAY_BOUNDS);

    for (let index = 0; index < IN_FLIGHT + 1; index += 1) {
      await assert.rejects(relay.request("sends", TASK_INFO), /does not clone/);
    }

    refuse = false;
    void relay.request("sends", TASK_INFO);
    assert.deepEqual(relay.size(), { inFlight: 1, waiting: 0, sent: 1 });
    assert.equal(posts.length, 1);
  });
});

/** A relay whose bound of the waiting operations is full: task reads that the main thread never answers. */
function fullRelay(): { readonly relay: IsolateRelay; readonly posts: SeatToMain[] } {
  const full = relayOnPosts();

  for (let index = 0; index < IN_FLIGHT + WAITING; index += 1) {
    void full.relay.request("sends", TASK_INFO);
  }

  assert.deepEqual(full.relay.size(), { inFlight: IN_FLIGHT, waiting: WAITING, sent: IN_FLIGHT });

  return full;
}

/** The text that names the relay bound and its value. */
const BOUND_TEXT = `its bound (MAX_RUN_WORK = ${String(WAITING)})`;

/** `SEAT_RELAY_FULL` with the bound of `waiting` in its text and in its cause, which is a retryable precondition. */
function assertRelayFull(error: SeatRelayFull, waiting: "operations" | "prepares"): void {
  assert.equal(error.code, SeatRelayCode.SEAT_RELAY_FULL);
  assert.equal(error.retryable, true);
  assert.ok(error.message.includes(`${String(WAITING)} ${waiting} that wait`), error.message);
  assert.ok(error.message.includes(BOUND_TEXT) && error.message.includes("nothing was sent; retry"), error.message);
  assert.deepEqual(error.exception, {
    kind: "coreFailure",
    failure: "preconditionFailed",
    subject: "seat relay",
    message: error.message,
    retryable: true,
  });
}

describe("the relay at its bounds", () => {
  test("an operation past the bound of the waiting operations gets seatRelayFull; nothing is sent", async () => {
    const { relay, posts } = fullRelay();
    const answer = await relay.request("sends", ENQUEUE);

    assert.equal(answer.kind, "seatRelayFull");
    assertRelayFull(answer.kind === "seatRelayFull" ? answer.error : assert.fail("answered"), "operations");
    assert.equal(posts.length, IN_FLIGHT);
  });

  test("a prepare does no I/O: it goes at once, takes no place of the in-flight bound, and gets its reply", async () => {
    const { relay, posts } = fullRelay();
    const replied = relay.request("sends", PREPARE);

    assert.equal(posts.length, IN_FLIGHT + 1);
    assert.deepEqual(relay.size(), { inFlight: IN_FLIGHT, waiting: WAITING, sent: IN_FLIGHT + 1 });
    relay.settle(IN_FLIGHT + 1, sendsAnswer({ kind: "prepareSend", prepared: PREPARED }));
    assert.deepEqual(await replied, { kind: "reply", reply: { kind: "prepareSend", prepared: PREPARED } });
  });

  test("a prepare past the bound of the waiting prepares gets seatRelayFull; nothing is sent", async () => {
    const { relay, posts } = relayOnPosts();

    for (let index = 0; index < WAITING; index += 1) {
      void relay.request("sends", PREPARE);
    }

    const answer = await relay.request("sends", PREPARE);

    assertRelayFull(answer.kind === "seatRelayFull" ? answer.error : assert.fail("answered"), "prepares");
    assert.deepEqual(operationKinds(posts), Array.from({ length: WAITING }, () => "prepareSend"));
  });
});

describe("each sends call kind past the bound gives SEAT_RELAY_FULL through its own error union", () => {
  test("a send whose prepare is past the bound", async () => {
    const { relay } = relayOnPosts();

    for (let index = 0; index < WAITING; index += 1) {
      void relay.request("sends", PREPARE);
    }

    const context = contextOf(seatApp());
    const sent = await sendTask({ ...context.send, core: { kind: "seatRelay", relay } }, crunch, { n: 1 }, {});
    const error = sent.ok ? assert.fail("the send past the bound was sent") : sent.error;

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error, "prepares");
  });

  test("a send whose enqueue is past the bound: no enqueue is sent", async () => {
    const posts: SeatToMain[] = [];

    const relay: IsolateRelay = new IsolateRelay((message) => {
      posts.push(message);

      // The main thread answers a prepare at once; it never answers a task read here.
      if (message.kind === "op" && message.request.area === "sends" && message.request.operation.kind === "prepareSend") {
        queueMicrotask(() => {
          relay.settle(message.requestId, sendsAnswer({ kind: "prepareSend", prepared: PREPARED }));
        });
      }
    }, TEST_RELAY_BOUNDS);

    for (let index = 0; index < IN_FLIGHT + WAITING; index += 1) {
      void relay.request("sends", TASK_INFO);
    }

    const context = contextOf(seatApp());
    const sent = await sendTask({ ...context.send, core: { kind: "seatRelay", relay } }, crunch, { n: 1 }, {});
    const error = sent.ok ? assert.fail("the send at the bound was sent") : sent.error;

    assert.ok(error.code === SeatRelayCode.SEAT_RELAY_FULL, error.code);
    assertRelayFull(error, "operations");
    assert.equal(operationKinds(posts).filter((kind) => kind === "enqueue").length, 0);
  });

  test("a task info read: the read error seatRelayFull", async () => {
    const { relay, posts } = fullRelay();
    const handle = taskHandleOf({ kind: "seatRelay", relay }, readFormOf(crunch), taskIdOf(TASK_ID));
    const read = await handle.info();
    const error = read.ok ? assert.fail("the read past the bound was answered") : read.error;

    assertRelayFull(error.kind === "seatRelayFull" ? error.error : assert.fail(`the read gave ${error.kind}`), "operations");
    assert.equal(posts.length, IN_FLIGHT);
  });
});

describe("the answers of the relay", () => {
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
