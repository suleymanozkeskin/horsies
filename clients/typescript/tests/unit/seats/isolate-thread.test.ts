// A seat isolate in a real `worker_threads` isolate: the app entry with the seat marker boots and is ready; it loads no
// `.node` file (a spy on `process.dlopen`); a sync body that holds the isolate reads the cancel cell that this thread
// writes; a send goes through the relay as operations of this thread, and a result wait is refused in the isolate.
// This thread plays the main thread of the child.

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { Worker } from "node:worker_threads";

import { clearCancel, newCancelCells, requestCancel } from "../../../horsies/src/seats/cancel-cells.ts";
import {
  type MainToSeat,
  parseSeatToMain,
  SEAT_PROTOCOL_VERSION,
  type SeatAnswer,
  type SeatToMain,
  seatWorkerData,
} from "../../../horsies/src/seats/protocol.ts";
import { ENQUEUED, PREPARED } from "../app/support.ts";
import { cancelMessage, runMessage } from "./support.ts";

/** The longest wait for one message of the isolate. */
const MESSAGE_BOUND_MS = 15_000;

/** The isolate of the fixture entry, and the messages it sent. */
class FixtureIsolate {
  readonly cells: Int32Array = newCancelCells(1);
  readonly operations: string[] = [];
  readonly errors: unknown[] = [];
  readonly #worker: Worker;
  readonly #messages: SeatToMain[] = [];
  #wake: () => void = () => undefined;

  constructor() {
    this.#worker = new Worker(new URL("fixtures/seat-entry.ts", import.meta.url), {
      workerData: seatWorkerData({ seatIndex: 0, protocol: SEAT_PROTOCOL_VERSION, cancelCells: this.cells }),
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
        this.#wake = resolve;
        setTimeout(resolve, 50);
      });
    }

    return assert.fail(`the isolate sent no ${kind} within ${String(MESSAGE_BOUND_MS)} ms; its errors: ${String(this.errors)}`);
  }

  async end(): Promise<void> {
    await this.#worker.terminate();
  }

  #take(message: SeatToMain): void {
    if (message.kind === "op") {
      this.#answer(message.requestId, message.request.operation.kind);

      return;
    }

    this.#messages.push(message);
    this.#wake();
  }

  #answer(requestId: number, kind: string): void {
    this.operations.push(kind);

    const answer: SeatAnswer =
      kind === "prepareSend"
        ? { kind: "reply", reply: { area: "sends", reply: { kind: "prepareSend", prepared: PREPARED } } }
        : { kind: "reply", reply: { area: "sends", reply: { kind: "enqueue", outcome: ENQUEUED } } };

    this.post({ kind: "opReply", protocol: SEAT_PROTOCOL_VERSION, requestId, answer });
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

  test("the entry with the seat marker boots and is ready", async () => {
    assert.deepEqual(await isolate.next("ready"), { kind: "ready", protocol: 1, seatIndex: 0 });
  });

  test("an isolate loads no .node file", async () => {
    isolate.post(runMessage(1n, "probe", "{}"));
    assert.deepEqual((await isolate.next("result")).outcome, { kind: "value", json: "0" });
  });

  test("a sync body that holds its isolate reads the cancel cell that the main thread writes", async () => {
    clearCancel(isolate.cells, 0);
    isolate.post(runMessage(2n, "spin", "{}"));
    // The body holds its isolate in a loop of sync sleeps: the cell is the only way its cancel reaches it.
    await new Promise((resolve) => setTimeout(resolve, 100));
    requestCancel(isolate.cells, 0, "DEADLINE");
    isolate.post(cancelMessage(2n, "DEADLINE"));
    assert.deepEqual((await isolate.next("result")).outcome, { kind: "value", json: "1" });
  });

  test("a send goes through the relay, and a result wait in the isolate is refused with no operation", async () => {
    isolate.post(runMessage(3n, "send_and_wait", "{}"));

    const result = await isolate.next("result");

    assert.deepEqual(isolate.operations, ["prepareSend", "enqueue"]);
    assert.equal(result.outcome.kind === "value" ? JSON.parse(result.outcome.json) : "", `sent ${ENQUEUED.kind === "enqueued" ? ENQUEUED.taskId : ""}; read error waitInsideTask`);
  });
});
