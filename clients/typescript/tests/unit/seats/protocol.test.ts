// The message protocol between a seat isolate and the main thread of its child: each message kind parses; a message of
// an unknown kind, of another protocol version, or with a field of another type is a defect; the seat marker.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { newCancelCells } from "../../../horsies/src/seats/cancel-cells.ts";
import {
  type MainToSeat,
  parseMainToSeat,
  parseSeatToMain,
  SEAT_PROTOCOL_VERSION,
  seatMarkerOf,
  seatWorkerData,
  type SeatToMain,
} from "../../../horsies/src/seats/protocol.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { PREPARED } from "../app/support.ts";
import { bodyEndedMessage, cancelMessage, replyMessage, runMessage, TEST_RELAY_BOUNDS } from "./support.ts";

const RECORDED_FACTS = {
  kind: "recorded",
  taskId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
  taskName: "crunch",
  queueName: "default",
  attempt: 1,
  workerId: "worker-1",
  execution: "SHARED_THREAD",
  workflow: { kind: "notInWorkflow" },
  carrier: { kind: "noCarrier" },
} as const;

/** A structured clone of `message`, as a port gives it. */
function cloned(message: object): object {
  return structuredClone(message);
}

describe("the messages of a seat isolate", () => {
  const messages: readonly SeatToMain[] = [
    { kind: "ready", protocol: SEAT_PROTOCOL_VERSION, seatIndex: 3 },
    { kind: "bootFailed", protocol: SEAT_PROTOCOL_VERSION, seatIndex: 0, hookName: "provide #1 openPool", cause: "Error: refused" },
    { kind: "result", protocol: SEAT_PROTOCOL_VERSION, runKey: 7n, outcome: { kind: "value", json: "42" } },
    {
      kind: "result",
      protocol: SEAT_PROTOCOL_VERSION,
      runKey: 8n,
      outcome: {
        kind: "error",
        parts: {
          code: { kind: "user", code: "TOO_BIG" },
          message: { kind: "none" },
          data: { kind: "noData" },
          exception: { kind: "noException" },
        },
      },
    },
    {
      kind: "op",
      protocol: SEAT_PROTOCOL_VERSION,
      requestId: 12,
      request: { area: "sends", operation: { kind: "taskInfo", taskId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b" } },
    },
    {
      kind: "op",
      protocol: SEAT_PROTOCOL_VERSION,
      requestId: 13,
      request: { area: "facades", operation: { part: "database", call: { method: "pingDatabase", args: [] } } },
    },
    { kind: "defect", protocol: SEAT_PROTOCOL_VERSION, code: "CLIENT_DEFECT", name: "ClientDefect", message: "seat run: a rule" },
    { kind: "log", protocol: SEAT_PROTOCOL_VERSION, line: "horsies child: a callback threw" },
    { kind: "flushed", protocol: SEAT_PROTOCOL_VERSION },
    { kind: "spanNotMade", protocol: SEAT_PROTOCOL_VERSION, span: { kind: "seatAttempt", taskId: "t-1" }, cause: "TypeError: x" },
    { kind: "spanNotMade", protocol: SEAT_PROTOCOL_VERSION, span: { kind: "workflowStart", workflowId: "w-1" }, cause: "Error: y" },
  ];

  for (const message of messages) {
    test(`${message.kind} parses`, () => {
      assert.deepEqual(parseSeatToMain(cloned(message)), message);
    });
  }

  test("an unknown kind is a defect", () => {
    assert.throws(() => parseSeatToMain({ kind: "hello", protocol: SEAT_PROTOCOL_VERSION }), ClientDefect);
  });

  test("another protocol version is a defect", () => {
    assert.throws(() => parseSeatToMain({ kind: "ready", protocol: 2, seatIndex: 0 }), /protocol version 2/);
  });

  test("a field of another type is a defect", () => {
    assert.throws(() => parseSeatToMain({ kind: "result", protocol: 1, runKey: 7, outcome: { kind: "value", json: "1" } }), /runKey/);
    assert.throws(() => parseSeatToMain({ kind: "result", protocol: 1, runKey: 7n, outcome: { kind: "maybe" } }), /outcome/);
    assert.throws(
      () => parseSeatToMain({ kind: "op", protocol: 1, requestId: 1, request: { area: "web", operation: {} } }),
      /unknown area web/,
    );
    assert.throws(() => parseSeatToMain({ kind: "spanNotMade", protocol: 1, span: { kind: "metric" }, cause: "c" }), /unknown kind metric/);
  });
});

describe("the messages of the main thread", () => {
  const messages: readonly MainToSeat[] = [
    runMessage(5n, "crunch", '{"n":1}'),
    runMessage(6n, "crunch", '{"n":1}', RECORDED_FACTS),
    cancelMessage(5n, "DEADLINE"),
    cancelMessage(5n, "OPERATOR"),
    bodyEndedMessage(5n, { kind: "returned" }),
    bodyEndedMessage(6n, { kind: "failed", code: "TOO_BIG" }),
    bodyEndedMessage(7n, { kind: "timedOut" }),
    replyMessage(3, {
      kind: "reply",
      reply: { area: "facades", reply: { envelope: { kind: "value", value: { kind: "json", text: "[]" } } } },
    }),
    replyMessage(1, { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } }),
    replyMessage(2, { kind: "refused", refusal: { code: "CORE_START_FAILED", cause: "no addon file" } }),
    { kind: "flush", protocol: SEAT_PROTOCOL_VERSION },
  ];

  for (const message of messages) {
    test(`${message.kind} parses`, () => {
      assert.deepEqual(parseMainToSeat(cloned(message)), message);
    });
  }

  test("an unknown kind is a defect", () => {
    assert.throws(() => parseMainToSeat({ kind: "stop", protocol: SEAT_PROTOCOL_VERSION }), ClientDefect);
  });

  test("another protocol version is a defect", () => {
    assert.throws(() => parseMainToSeat({ ...cancelMessage(1n, "DEADLINE"), protocol: 0 }), /protocol version 0/);
  });

  test("a verdict of an unknown kind and a defect of another code are defects", () => {
    assert.throws(() => parseMainToSeat({ ...bodyEndedMessage(1n, { kind: "returned" }), end: { kind: "maybe" } }), /verdict/);
    assert.throws(
      () => parseSeatToMain({ kind: "defect", protocol: SEAT_PROTOCOL_VERSION, code: "CORE_STOPPED", name: "x", message: "y" }),
      /no defect code/,
    );
  });

  test("a cancel cause outside the two causes and a defect code as a refusal are defects", () => {
    assert.throws(() => parseMainToSeat({ ...cancelMessage(1n, "DEADLINE"), cause: "LEFT_WORK" }), /LEFT_WORK/);

    const defectRefusal: object = {
      kind: "opReply",
      protocol: SEAT_PROTOCOL_VERSION,
      requestId: 1,
      answer: { kind: "refused", refusal: { code: "CLIENT_DEFECT", subject: "relay", rule: "a rule" } },
    };

    assert.throws(() => parseMainToSeat(defectRefusal), /no CoreError code/);
  });
});

describe("the seat marker", () => {
  test("workerData with the marker gives the marker; workerData without it is no seat", () => {
    const cancelCells = newCancelCells(4);

    const read = seatMarkerOf(
      structuredClone(seatWorkerData({ seatIndex: 2, protocol: SEAT_PROTOCOL_VERSION, cancelCells, relayBounds: TEST_RELAY_BOUNDS })),
    );

    assert.equal(read.kind, "seat");
    assert.equal(read.kind === "seat" ? read.marker.seatIndex : -1, 2);
    assert.deepEqual(read.kind === "seat" ? read.marker.relayBounds : undefined, TEST_RELAY_BOUNDS);
    // The clone shares the memory of the cells: a write on one side reads on the other.
    Atomics.store(cancelCells, 2, 1);
    assert.equal(read.kind === "seat" ? Atomics.load(read.marker.cancelCells, 2) : 0, 1);
    assert.deepEqual(seatMarkerOf({ other: true }), { kind: "notSeat" });
  });

  test("a marker with no shared cells, a seat past its cells, or a relay bound of 0 is a defect", () => {
    assert.throws(
      () => seatMarkerOf({ horsiesSeat: { seatIndex: 0, protocol: 1, cancelCells: new Int32Array(1), relayBounds: TEST_RELAY_BOUNDS } }),
      /SharedArrayBuffer/,
    );
    assert.throws(
      () => seatMarkerOf({ horsiesSeat: { seatIndex: 4, protocol: 1, cancelCells: newCancelCells(4), relayBounds: TEST_RELAY_BOUNDS } }),
      /no cancel cell/,
    );
    assert.throws(
      () =>
        seatMarkerOf({
          horsiesSeat: { seatIndex: 0, protocol: 1, cancelCells: newCancelCells(1), relayBounds: { opsInFlight: 0, opsWaiting: 3 } },
        }),
      /relay bound opsInFlight/,
    );
  });
});
