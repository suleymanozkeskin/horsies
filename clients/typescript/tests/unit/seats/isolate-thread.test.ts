// A seat isolate in a real `worker_threads` isolate: the app entry with the seat marker (and `configureLogging` at its
// top) boots and is ready; it loads no `.node` file (a spy on `process.dlopen`); a sync body that holds the isolate
// reads the cancel cell that this thread writes; a send goes through the relay as operations of this thread, and a
// result wait is refused in the isolate; an unawaited rejected Promise is logged and the run keeps its value; a package
// defect in a body goes to this thread as a `defect`; the database ping and the cancel of a body go through the relay;
// the isolate answers the flush of the child's stop. This thread plays the main thread of the child.

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { Worker } from "node:worker_threads";

import { clearCancel, newCancelCells, requestCancel } from "../../../horsies/src/seats/cancel-cells.ts";
import {
  type MainToSeat,
  parseSeatToMain,
  SEAT_PROTOCOL_VERSION,
  type SeatAnswer,
  type SeatRequest,
  type SeatToMain,
  seatWorkerData,
} from "../../../horsies/src/seats/protocol.ts";
import { ENQUEUED, PREPARED, TASK_ID } from "../app/support.ts";
import { bodyEndedMessage, cancelMessage, runMessage, TEST_RELAY_BOUNDS } from "./support.ts";

/** The longest wait for one message of the isolate. */
const MESSAGE_BOUND_MS = 15_000;

/**
 * `test` on Node; a skipped case on Bun. `SHARED_THREAD` runs on Node only (a Bun worker refuses it), and a worker
 * thread of Bun ends at a rejection or a throw that a listener of the thread handles.
 */
const nodeOnly = "bun" in process.versions ? test.skip : test;

/** The answer of this thread to one operation of the isolate. */
function answerOf(request: SeatRequest): SeatAnswer {
  switch (request.area) {
    case "sends":
      return request.operation.kind === "prepareSend"
        ? { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } }
        : { kind: "reply", reply: { area: "sends", reply: { kind: "enqueue", outcome: ENQUEUED } } };
    case "facades":
      return request.operation.part === "database"
        ? { kind: "reply", reply: { area: "facades", reply: { envelope: { kind: "value", value: { kind: "done", record: { latencyMs: 3 } } } } } }
        : {
            kind: "reply",
            reply: {
              area: "facades",
              reply: {
                envelope: {
                  kind: "value",
                  value: {
                    kind: "failed",
                    error: {
                      class: "TaskActionNotFound",
                      slots: [
                        { name: "message", value: { kind: "text", text: "no such task" } },
                        { name: "task_id", value: { kind: "uuid", text: TASK_ID } },
                      ],
                    },
                  },
                },
              },
            },
          };
    case "workflows":
      return assert.fail("the fixture entry sends no workflows operation");
    default:
      return assert.fail("an operation of an unknown area");
  }
}

/** The operation name of one request: `area part/kind`. */
function operationName(request: SeatRequest): string {
  switch (request.area) {
    case "sends":
      return `sends ${request.operation.kind}`;
    case "facades":
      return `facades ${request.operation.part} ${request.operation.call.method}`;
    case "workflows":
      return `workflows ${request.operation.kind}`;
    default:
      return assert.fail("an operation of an unknown area");
  }
}

/** The isolate of the fixture entry, and the messages it sent. */
class FixtureIsolate {
  readonly cells: Int32Array = newCancelCells(1);
  readonly operations: string[] = [];
  readonly errors: unknown[] = [];
  readonly #worker: Worker;
  readonly #messages: SeatToMain[] = [];

  constructor() {
    this.#worker = new Worker(new URL("fixtures/seat-entry.ts", import.meta.url), {
      workerData: seatWorkerData({ seatIndex: 0, protocol: SEAT_PROTOCOL_VERSION, cancelCells: this.cells, relayBounds: TEST_RELAY_BOUNDS }),
    });
    this.#worker.on("message", (message) => {
      this.#take(parseSeatToMain(Object(message)));
    });
    this.#worker.on("error", (cause) => {
      this.errors.push(cause);
    });
  }

  post(message: MainToSeat): void {
    this.#worker.postMessage(message);
  }

  /** The next message of `kind`, at most `MESSAGE_BOUND_MS` from now. */
  async next<K extends SeatToMain["kind"]>(kind: K): Promise<Extract<SeatToMain, { readonly kind: K }>> {
    const deadline = Date.now() + MESSAGE_BOUND_MS;

    while (Date.now() < deadline) {
      const index = this.#messages.findIndex((message) => message.kind === kind);
      const found = this.#messages[index];

      if (found !== undefined && found.kind === kind) {
        this.#messages.splice(index, 1);

        // SAFETY: the message has the kind `K`, checked on the line above.
        return found as Extract<SeatToMain, { readonly kind: K }>;
      }

      await new Promise<void>((resolve) => {
        setTimeout(resolve, 20);
      });
    }

    return assert.fail(`the isolate sent no ${kind} within ${String(MESSAGE_BOUND_MS)} ms; its errors: ${String(this.errors)}`);
  }

  /** Run `taskName` as the run `runKey`; give its result, and send the verdict as the main thread does. */
  async result(runKey: bigint, taskName: string): Promise<Extract<SeatToMain, { readonly kind: "result" }>> {
    this.post(runMessage(runKey, taskName, "{}"));

    const result = await this.next("result");

    this.post(bodyEndedMessage(runKey, { kind: "returned" }));

    return result;
  }

  async end(): Promise<void> {
    await this.#worker.terminate();
  }

  #take(message: SeatToMain): void {
    if (message.kind === "op") {
      this.operations.push(operationName(message.request));
      this.post({ kind: "opReply", protocol: SEAT_PROTOCOL_VERSION, requestId: message.requestId, answer: answerOf(message.request) });

      return;
    }

    this.#messages.push(message);
  }
}

describe("a seat isolate in a worker thread", () => {
  let isolate: FixtureIsolate;

  before(() => {
    isolate = new FixtureIsolate();
  });

  after(async () => {
    await isolate.end();
  });

  test("the entry with the seat marker and configureLogging at its top boots and is ready", async () => {
    assert.deepEqual(await isolate.next("ready"), { kind: "ready", protocol: 1, seatIndex: 0 });
  });

  test("an isolate loads no .node file", async () => {
    assert.deepEqual((await isolate.result(1n, "probe")).outcome, { kind: "value", json: "0" });
  });

  test("a sync body that holds its isolate reads the cancel cell that the main thread writes", async () => {
    clearCancel(isolate.cells, 0);
    isolate.post(runMessage(2n, "spin", "{}"));
    // The body holds its isolate in a loop of sync sleeps: the cell is the only way its cancel reaches it.
    await new Promise((resolve) => setTimeout(resolve, 100));
    requestCancel(isolate.cells, 0, "DEADLINE");
    isolate.post(cancelMessage(2n, "DEADLINE"));
    assert.deepEqual((await isolate.next("result")).outcome, { kind: "value", json: "1" });
    isolate.post(bodyEndedMessage(2n, { kind: "timedOut" }));
  });

  test("a send goes through the relay, and a result wait in the isolate is refused with no operation", async () => {
    const result = await isolate.result(3n, "send_and_wait");

    assert.deepEqual(isolate.operations.splice(0), ["sends prepareSend", "sends enqueue"]);
    assert.equal(
      result.outcome.kind === "value" ? JSON.parse(result.outcome.json) : "",
      `sent ${ENQUEUED.kind === "enqueued" ? ENQUEUED.taskId : ""}; read error waitInsideTask`,
    );
  });

  nodeOnly("an unawaited rejected Promise is logged, and the run keeps its value", async () => {
    assert.deepEqual((await isolate.result(4n, "reject_unawaited")).outcome, { kind: "value", json: "7" });
    assert.match((await isolate.next("log")).line, /a Promise that no body awaits rejected.*TypeError: not awaited/s);
  });

  nodeOnly("a detached throw in code of a run is logged, and the run keeps its value", async () => {
    assert.deepEqual((await isolate.result(5n, "throw_later")).outcome, { kind: "value", json: "5" });
    assert.match((await isolate.next("log")).line, /a callback threw in the run of task throw_later.*RangeError: a late throw/s);
  });

  test("a package defect in a body goes to the main thread as a defect", async () => {
    const result = await isolate.result(6n, "foreign_state");

    assert.match((await isolate.next("defect")).message, /belongs to another app/);
    assert.equal(result.outcome.kind === "error" ? result.outcome.parts.code.code : "", "UNHANDLED_EXCEPTION");
  });

  test("the database ping and the cancel of a body go through the relay and keep their Results", async () => {
    const result = await isolate.result(7n, "facades_probe");

    assert.deepEqual(isolate.operations.splice(0), ["facades database pingDatabase", "facades cancel cancelTask"]);
    assert.deepEqual(result.outcome, { kind: "value", json: JSON.stringify("ping 3; cancel TASK_NOT_FOUND") });
  });

  test("the flush of the child's stop: the isolate answers flushed", async () => {
    isolate.post({ kind: "flush", protocol: SEAT_PROTOCOL_VERSION });
    assert.deepEqual(await isolate.next("flushed"), { kind: "flushed", protocol: SEAT_PROTOCOL_VERSION });
  });
});
