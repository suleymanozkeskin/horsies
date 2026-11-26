// The seat pool on the main thread of a `SHARED_THREAD` child: one seat isolate per seat, the routing of each run to a
// free isolate, the result relay, the cancels, the operations of the isolates, and the replacement of an isolate that
// ended.
//
// - The boot: each seat starts its isolate; the boot ends when every isolate sent `ready`. A `bootFailed`, or an
//   isolate that ends before `ready`, fails the boot. The boot is one start hook of the child, so the core bounds it
//   (10 s) and ends the child with 173 when it fails.
// - A run (the core's `runBody` post of a seat thread) goes to a free isolate: the pool clears the seat's cancel cell
//   and posts `run`. The `result` settles the run, and the seat is free again. The core takes at most `seats` runs at
//   once and a seat thread ends only when its body returned, so a run finds a free isolate while no isolate ended.
// - A cancel writes the cause into the cell of the run's seat, then posts `cancel`. A cancel of a run that is not on a
//   seat and not waiting is left: the run ended and the cancel crossed it.
// - An isolate that ends during a run: the run ends `UNHANDLED_EXCEPTION` with the record of the error that ended the
//   isolate, and a new isolate takes the seat; it runs the providers before it takes a run. A run that comes while no
//   isolate is free waits for that isolate; the pool logs the length of each wait. A new isolate that fails its
//   providers, ends before `ready`, or is not ready within `REPLACEMENT_BOOT_BOUND_MS` ends the child with 173.
// - The pool never calls `worker.terminate()`: the normal stop is the cancel signal, then the end of the child.

import type { CancelCauseName, ChildPost } from "../native/child/addon.ts";
import { CHILD_HOOK_FAILED_EXIT_CODE } from "../_catalog/exit_codes.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { clearCancel, newCancelCells, requestCancel } from "./cancel-cells.ts";
import { type IsolateEnd, isolateEndOutcome, isolateEndRecord } from "./isolate-end.ts";
import {
  type MainToSeat,
  parseSeatToMain,
  SEAT_PROTOCOL_VERSION,
  type SeatAnswer,
  type SeatMarker,
  type SeatOutcome,
  type SeatRequest,
  type SeatToMain,
} from "./protocol.ts";

/** The longest boot of a new isolate: the start-hook bound of the core (`CHILD_START_HOOK_BOUND`, 10 s). */
export const REPLACEMENT_BOOT_BOUND_MS = 10_000;

/** One isolate, as the pool posts to it. */
export interface SeatIsolate {
  post(message: MainToSeat): void;
}

/** What an isolate reports to the pool. Each event of an isolate that a later isolate replaced is left. */
export interface IsolateEvents {
  /** A message of the isolate, not parsed yet. */
  message(message: object): void;
  /** The isolate threw an uncaught error, or reached its memory cap; its exit follows. */
  error(cause: unknown): void;
  exit(exitCode: number): void;
}

/** Start the isolate of `marker`, which reports to `events`. */
export type StartIsolate = (marker: SeatMarker, events: IsolateEvents) => SeatIsolate;

/** The `runBody` post of a core seat thread. */
export type SeatRunPost = Extract<ChildPost, { readonly kind: "runBody" }>;

/** What the seats of a child need from the process: the isolates, a monotonic clock, and a timer. */
export interface SeatHost {
  readonly start: StartIsolate;
  /** A monotonic clock, in milliseconds. */
  readonly clock: () => number;
  /** Call `callback` after `milliseconds`; the result cancels it. */
  readonly timer: (milliseconds: number, callback: () => void) => () => void;
}

/** What the pool does that has effects. */
export interface SeatPoolDeps extends SeatHost {
  /** Settle the run `runKey` of `taskName` with `outcome` (the settle call of the core's seat thread). */
  readonly settle: (runKey: bigint, taskName: string, outcome: SeatOutcome) => void;
  /** Run one operation of an isolate on the main thread. Rejects only with a defect of the package. */
  readonly serve: (request: SeatRequest) => Promise<SeatAnswer>;
  /** End the child with `code` through the exit rule. */
  readonly endChild: (code: number) => void;
  readonly writeLine: (line: string) => void;
  /** The defect rule: it never throws. */
  readonly reportDefect: (defect: CoreDefect) => void;
}

/** One isolate that the pool started, and the error that it reported before its exit. */
class StartedIsolate {
  #port: SeatIsolate | undefined;
  #error: { readonly kind: "noError" } | { readonly kind: "error"; readonly cause: unknown } = { kind: "noError" };

  attach(port: SeatIsolate): void {
    this.#port = port;
  }

  post(message: MainToSeat): void {
    if (this.#port === undefined) {
      throw new ClientDefect({ subject: "seat pool", rule: "a post to an isolate before its start returned" });
    }

    this.#port.post(message);
  }

  failed(cause: unknown): void {
    this.#error = { kind: "error", cause };
  }

  end(exitCode: number): IsolateEnd {
    switch (this.#error.kind) {
      case "noError":
        return { kind: "exit", exitCode };
      case "error":
        return { kind: "error", cause: this.#error.cause, exitCode };
      default:
        return unreachable(this.#error, "StartedIsolate.end");
    }
  }
}

/** How a seat boots: in the boot of the child, or as the new isolate of a seat whose isolate ended. */
type Boot = { readonly kind: "first" } | { readonly kind: "replacement"; readonly since: number; readonly stopTimer: () => void };

/** The stage of a seat: its isolate boots, is free, runs a run; or the seat has no isolate (the child ends). */
export type SeatStage = "booting" | "free" | "running" | "ended";

/** The state of one seat. */
type SeatState =
  | { readonly kind: "booting"; readonly isolate: StartedIsolate; readonly boot: Boot }
  | { readonly kind: "free"; readonly isolate: StartedIsolate }
  | { readonly kind: "running"; readonly isolate: StartedIsolate; readonly run: SeatRunPost }
  | { readonly kind: "ended" };

/** The cancel of a run that waits: none yet, or the first cause. */
type WaitingCancel = { readonly kind: "notRequested" } | { readonly kind: "requested"; readonly cause: CancelCauseName };

/** A run that waits for a free isolate, since `since` (the clock). */
type WaitingRun = { readonly post: SeatRunPost; readonly since: number; readonly cancel: WaitingCancel };

const NOT_REQUESTED: WaitingCancel = Object.freeze({ kind: "notRequested" });

/** The pool and the end of its boot. */
export type StartedPool = { readonly pool: SeatPool; readonly booted: Promise<void> };

/** Start the pool of `seats` isolates. `booted` resolves when every isolate is ready, and rejects at a failed boot. */
export function startSeatPool(seats: number, deps: SeatPoolDeps): StartedPool {
  const boot = Promise.withResolvers<void>();

  return { pool: new SeatPool(seats, deps, boot), booted: boot.promise };
}

/** The isolates of one child and the runs on them. */
export class SeatPool {
  readonly #deps: SeatPoolDeps;
  readonly #cells: Int32Array;
  readonly #seats: SeatState[];
  readonly #waiting: WaitingRun[] = [];
  readonly #boot: PromiseWithResolvers<void>;
  #booting: number;

  constructor(seats: number, deps: SeatPoolDeps, boot: PromiseWithResolvers<void>) {
    this.#deps = deps;
    this.#cells = newCancelCells(seats);
    this.#boot = boot;
    this.#booting = seats;
    this.#seats = Array.from({ length: seats }, (): SeatState => ({ kind: "ended" }));

    for (let seat = 0; seat < seats; seat += 1) {
      this.#seats[seat] = { kind: "booting", isolate: this.#startIsolate(seat), boot: { kind: "first" } };
    }
  }

  /** The run of `post` goes to a free isolate, or waits for the new isolate of a seat. */
  run(post: SeatRunPost): void {
    const seat = this.#seats.findIndex((state) => state.kind === "free");

    if (seat === -1) {
      this.#wait(post);

      return;
    }

    this.#startRun(seat, post, NOT_REQUESTED);
  }

  /** The cancel signal of the run `runKey` fired with `cause`. */
  cancel(runKey: bigint, cause: CancelCauseName): void {
    const seat = this.#seats.findIndex((state) => state.kind === "running" && state.run.runKey === runKey);
    const state = this.#seats[seat];

    if (state !== undefined && state.kind === "running") {
      requestCancel(this.#cells, seat, cause);
      state.isolate.post({ kind: "cancel", protocol: SEAT_PROTOCOL_VERSION, runKey, cause });

      return;
    }

    const waiting = this.#waiting.findIndex((run) => run.post.runKey === runKey);
    const run = this.#waiting[waiting];

    if (run !== undefined && run.cancel.kind === "notRequested") {
      this.#waiting[waiting] = { ...run, cancel: { kind: "requested", cause } };
    }
  }

  /** The stage of each seat, in seat order. */
  stages(): readonly SeatStage[] {
    return this.#seats.map((state) => state.kind);
  }

  /** The runs that wait for a free isolate. */
  waiting(): number {
    return this.#waiting.length;
  }

  #startIsolate(seat: number): StartedIsolate {
    const isolate = new StartedIsolate();

    const events: IsolateEvents = {
      message: (message) => {
        this.#guarded(() => {
          this.#onMessage(seat, isolate, parseSeatToMain(message));
        });
      },
      error: (cause) => {
        isolate.failed(cause);
      },
      exit: (exitCode) => {
        this.#guarded(() => {
          this.#onExit(seat, isolate, exitCode);
        });
      },
    };

    isolate.attach(this.#deps.start({ seatIndex: seat, protocol: SEAT_PROTOCOL_VERSION, cancelCells: this.#cells }, events));

    return isolate;
  }

  /** Run `step`; a defect goes to the defect rule, so an event handler never throws. */
  #guarded(step: () => void): void {
    try {
      step();
    } catch (cause) {
      this.#deps.reportDefect(cause instanceof CoreDefect ? cause : poolDefect(`an event of an isolate threw: ${String(cause)}`));
    }
  }

  #onMessage(seat: number, isolate: StartedIsolate, message: SeatToMain): void {
    const state = this.#seats[seat];

    if (state === undefined || !("isolate" in state) || state.isolate !== isolate) {
      return;
    }

    switch (message.kind) {
      case "ready":
        this.#onReady(seat, message.seatIndex, state);

        return;
      case "bootFailed":
        this.#onBootFailed(seat, state, `the provider ${message.hookName} failed in the seat isolate ${String(seat)}: ${message.cause}`);

        return;
      case "result":
        this.#onResult(seat, state, message.runKey, message.outcome);

        return;
      case "op":
        this.#serve(isolate, message.requestId, message.request);

        return;
      default:
        unreachable(message, "SeatPool.onMessage");
    }
  }

  #onReady(seat: number, seatIndex: number, state: SeatState): void {
    if (state.kind !== "booting" || seatIndex !== seat) {
      throw poolDefect(`the seat isolate ${String(seat)} sent ready for the seat ${String(seatIndex)} in the state ${state.kind}`);
    }

    this.#seats[seat] = { kind: "free", isolate: state.isolate };

    switch (state.boot.kind) {
      case "first":
        this.#booting -= 1;

        if (this.#booting === 0) {
          this.#boot.resolve();
        }

        return;
      case "replacement":
        state.boot.stopTimer();
        this.#nextWaiting(seat);

        return;
      default:
        unreachable(state.boot, "SeatPool.onReady");
    }
  }

  #onBootFailed(seat: number, state: SeatState, cause: string): void {
    if (state.kind !== "booting") {
      throw poolDefect(`the seat isolate ${String(seat)} sent bootFailed in the state ${state.kind}`);
    }

    this.#seats[seat] = { kind: "ended" };

    switch (state.boot.kind) {
      case "first":
        this.#boot.reject(new Error(cause));

        return;
      case "replacement":
        state.boot.stopTimer();
        this.#endChild(`${cause}; the child ends with ${String(CHILD_HOOK_FAILED_EXIT_CODE)}`);

        return;
      default:
        unreachable(state.boot, "SeatPool.onBootFailed");
    }
  }

  #onResult(seat: number, state: SeatState, runKey: bigint, outcome: SeatOutcome): void {
    if (state.kind !== "running" || state.run.runKey !== runKey) {
      throw poolDefect(`the seat isolate ${String(seat)} sent the result of the run ${String(runKey)}, which does not run there`);
    }

    this.#seats[seat] = { kind: "free", isolate: state.isolate };
    this.#deps.settle(runKey, state.run.taskName, outcome);
    this.#nextWaiting(seat);
  }

  #serve(isolate: StartedIsolate, requestId: number, request: SeatRequest): void {
    this.#deps.serve(request).then(
      (answer) => {
        // An answer to an isolate that ended reaches no one: its operations ended with it.
        if (this.#seats.some((state) => "isolate" in state && state.isolate === isolate)) {
          isolate.post({ kind: "opReply", protocol: SEAT_PROTOCOL_VERSION, requestId, answer });
        }
      },
      (cause: unknown) => {
        this.#deps.reportDefect(cause instanceof CoreDefect ? cause : poolDefect(`a seat operation failed: ${String(cause)}`));
      },
    );
  }

  #onExit(seat: number, isolate: StartedIsolate, exitCode: number): void {
    const state = this.#seats[seat];

    if (state === undefined || !("isolate" in state) || state.isolate !== isolate) {
      return;
    }

    const end = isolate.end(exitCode);
    const record = isolateEndRecord(end);
    const ended = `the seat isolate ${String(seat)} ended with exit code ${String(exitCode)}: ${record.type}: ${record.message}`;

    switch (state.kind) {
      case "booting":
        this.#onBootFailed(seat, state, `${ended}, before it was ready`);

        return;
      case "free":
        this.#deps.writeLine(`horsies child: ${ended}; a new isolate takes the seat`);
        this.#replace(seat);

        return;
      case "running":
        this.#deps.writeLine(
          `horsies child: ${ended}, during the run of task ${state.run.taskName} (task ${state.run.taskId}, attempt ${String(state.run.attempt)}); the run ends UNHANDLED_EXCEPTION and a new isolate takes the seat`,
        );
        this.#seats[seat] = { kind: "ended" };
        this.#deps.settle(state.run.runKey, state.run.taskName, isolateEndOutcome(state.run.taskName, end));
        this.#replace(seat);

        return;
      default:
        unreachable(state, "SeatPool.onExit");
    }
  }

  #replace(seat: number): void {
    const since = this.#deps.clock();
    const isolate = this.#startIsolate(seat);

    const stopTimer = this.#deps.timer(REPLACEMENT_BOOT_BOUND_MS, () => {
      this.#guarded(() => {
        this.#replacementTimedOut(seat, isolate);
      });
    });

    this.#seats[seat] = { kind: "booting", isolate, boot: { kind: "replacement", since, stopTimer } };
  }

  #replacementTimedOut(seat: number, isolate: StartedIsolate): void {
    const state = this.#seats[seat];

    if (state === undefined || state.kind !== "booting" || state.isolate !== isolate) {
      return;
    }

    this.#seats[seat] = { kind: "ended" };
    this.#endChild(
      `the new isolate of the seat ${String(seat)} was not ready within ${String(REPLACEMENT_BOOT_BOUND_MS)} ms; the child ends with ${String(CHILD_HOOK_FAILED_EXIT_CODE)}`,
    );
  }

  #endChild(cause: string): void {
    this.#deps.writeLine(`horsies child: ${cause}`);
    this.#deps.endChild(CHILD_HOOK_FAILED_EXIT_CODE);
  }

  /** A run waits for the new isolate of a seat. A wait with no new isolate on the way is a defect of the accounts. */
  #wait(post: SeatRunPost): void {
    const replacing = this.#seats.some((state) => state.kind === "booting" && state.boot.kind === "replacement");

    if (!replacing || this.#waiting.length >= this.#seats.length) {
      throw poolDefect(`the run of task ${post.taskName} found no free isolate, and no new isolate is on the way for it`);
    }

    this.#waiting.push({ post, since: this.#deps.clock(), cancel: NOT_REQUESTED });
  }

  #nextWaiting(seat: number): void {
    const run = this.#waiting.shift();

    if (run === undefined) {
      return;
    }

    const waited = Math.round(this.#deps.clock() - run.since);

    this.#deps.writeLine(
      `horsies child: the run of task ${run.post.taskName} (task ${run.post.taskId}, attempt ${String(run.post.attempt)}) waited ${String(waited)} ms for the new isolate of a seat`,
    );
    this.#startRun(seat, run.post, run.cancel);
  }

  #startRun(seat: number, post: SeatRunPost, cancel: WaitingCancel): void {
    const state = this.#seats[seat];

    if (state === undefined || state.kind !== "free") {
      throw poolDefect(`a run went to the seat ${String(seat)}, which is not free`);
    }

    clearCancel(this.#cells, seat);
    this.#seats[seat] = { kind: "running", isolate: state.isolate, run: post };

    const { runKey, taskName, inputJson, taskId, attempt, facts } = post;

    switch (cancel.kind) {
      case "notRequested":
        state.isolate.post({ kind: "run", protocol: SEAT_PROTOCOL_VERSION, runKey, taskName, inputJson, taskId, attempt, facts });

        return;
      case "requested":
        requestCancel(this.#cells, seat, cancel.cause);
        state.isolate.post({ kind: "run", protocol: SEAT_PROTOCOL_VERSION, runKey, taskName, inputJson, taskId, attempt, facts });
        state.isolate.post({ kind: "cancel", protocol: SEAT_PROTOCOL_VERSION, runKey, cause: cancel.cause });

        return;
      default:
        unreachable(cancel, "SeatPool.startRun");
    }
  }
}

function poolDefect(rule: string): ClientDefect {
  return new ClientDefect({ subject: "seat pool", rule });
}
