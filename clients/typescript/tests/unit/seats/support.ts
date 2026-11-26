// Fakes of the seat tests: the tasks of a thread child, an app built with the fakes of the app hub, the port of a seat
// isolate, and the messages of the protocol.

import assert from "node:assert/strict";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import type { AppContext } from "../../../horsies/src/app/context.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { BodyFactsRecord, CancelCauseName } from "../../../horsies/src/native/child/addon.ts";
import { newCancelCells } from "../../../horsies/src/seats/cancel-cells.ts";
import { type IsolatePort, SeatChannel } from "../../../horsies/src/seats/channel.ts";
import type { SeatHost } from "../../../horsies/src/seats/pool.ts";
import {
  type MainToSeat,
  SEAT_PROTOCOL_VERSION,
  type SeatAnswer,
  type SeatToMain,
} from "../../../horsies/src/seats/protocol.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { DATABASE_URL, fakeEnvironment, TASK_ID } from "../app/support.ts";

/** A `SHARED_THREAD` task with a sync body in the tests. */
export const crunch = defineTask({
  name: "crunch",
  input: { n: h.int() },
  output: h.int(),
  execution: "SHARED_THREAD",
  timeoutMs: 5_000,
});

/** A `SHARED_THREAD` task with an async body in the tests. */
export const fetchPage = defineTask({
  name: "fetch_page",
  input: { url: h.string() },
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: 5_000,
});

/** The app of `crunch` and `fetchPage`, built with the fakes; a failed create fails the test. */
export function seatApp(): Horsies {
  const created = createHorsies({ broker: { databaseUrl: DATABASE_URL }, tasks: [crunch, fetchPage] }, fakeEnvironment());

  if (!created.ok) {
    assert.fail(`the seat app was refused: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
  }

  return created.value;
}

/** The context of `app`. */
export function contextOf(app: Horsies): AppContext {
  return appInternals(app);
}

/** A port of a seat isolate that records each post and delivers the messages that a test gives. */
export class FakeIsolatePort implements IsolatePort {
  readonly posts: SeatToMain[] = [];
  #listener: (message: object) => void = () => undefined;

  post(message: SeatToMain): void {
    this.posts.push(message);
  }

  onMessage(listener: (message: object) => void): void {
    this.#listener = listener;
  }

  /** Deliver `message` of the main thread to the isolate. */
  deliver(message: MainToSeat): void {
    this.#listener(message);
  }

  /** The posts of `kind`. */
  postsOf<K extends SeatToMain["kind"]>(kind: K): Extract<SeatToMain, { readonly kind: K }>[] {
    return this.posts.filter((post): post is Extract<SeatToMain, { readonly kind: K }> => post.kind === kind);
  }
}

/** The channel of seat `seatIndex` of a child of `seats` seats on a fake port. */
export function fakeChannel(seatIndex = 0, seats = 2): { readonly channel: SeatChannel; readonly port: FakeIsolatePort } {
  const port = new FakeIsolatePort();

  const channel = new SeatChannel(
    { seatIndex, protocol: SEAT_PROTOCOL_VERSION, cancelCells: newCancelCells(seats) },
    port,
  );

  return { channel, port };
}

const NOT_RECORDED: BodyFactsRecord = { kind: "notRecorded" };

/** A `run` message of `taskName` with the input text `inputJson`. */
export function runMessage(runKey: bigint, taskName: string, inputJson: string, facts: BodyFactsRecord = NOT_RECORDED): MainToSeat {
  return { kind: "run", protocol: SEAT_PROTOCOL_VERSION, runKey, taskName, inputJson, taskId: TASK_ID, attempt: 1, facts };
}

/** A `cancel` message of the run `runKey`. */
export function cancelMessage(runKey: bigint, cause: CancelCauseName): MainToSeat {
  return { kind: "cancel", protocol: SEAT_PROTOCOL_VERSION, runKey, cause };
}

/** An `opReply` message of the request `requestId`. */
export function replyMessage(requestId: number, answer: SeatAnswer): MainToSeat {
  return { kind: "opReply", protocol: SEAT_PROTOCOL_VERSION, requestId, answer };
}

/** Wait until the queued Promise jobs and the timers of 0 ms ran. */
export async function settled(): Promise<void> {
  for (let turn = 0; turn < 5; turn += 1) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}

/** The seat host of a child that starts no seat isolate: its start is a test failure. */
export function unusedSeatHost(): SeatHost {
  return {
    start: () => assert.fail("the child started a seat isolate"),
    clock: () => 0,
    timer: () => () => undefined,
  };
}
