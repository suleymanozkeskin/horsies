// The seat pool on the main thread of a `SHARED_THREAD` child: one seat isolate per seat, the routing of each run to a
// free isolate, the result relay, the cancels, the verdicts, the operations, the defects and the log lines of the
// isolates, and the replacement of an isolate that ended.
//
// - The boot: each seat starts its isolate; the boot ends when every isolate sent `ready`. A `bootFailed`, an isolate
//   that ends before `ready`, or an isolate that does not start fails the boot. The boot is one start hook of the
//   child, so the core bounds it (10 s) and ends the child with 173 when it fails.
// - A run (the core's `runBody` post of a seat thread) goes to a free isolate: the pool clears the seat's cancel cell
//   and posts `run`. The `result` settles the run, then `bodyEnded` gives the isolate the verdict (from the cancel cause
//   that the run got, and from its outcome) and the time of the settle, and the seat is free again. The core takes at
//   most `seats` runs at once and a seat thread ends only when its body returned, so a run finds a free isolate while
//   no isolate ended.
// - A cancel writes the cause into the cell of the run's seat, then posts `cancel`. A cancel of a run that is not on a
//   seat and not waiting is left: the run ended and the cancel crossed it.
// - An isolate that ends during a run: the run ends `WORKER_CRASHED` (`isolate-end.ts`), the seat body observer of the
//   main thread gets the run's facts and times (`isolateEnded`: the isolate's own span never ends), and a new isolate
//   takes the seat; it runs the providers before it takes a run. A run that comes while no isolate is free waits for that
//   isolate; the pool logs the length of each wait. A new isolate that fails its providers, ends before `ready`, does
//   not start, or is not ready within the start-hook bound ends the child with 173.
// - Once the pool ends the child, it is `ending`: the child ends after its exit flush, so a run or a cancel that the
//   core posts in that time is left (the core gives such a run the crash path).
// - A `defect` of an isolate goes to the defect rule (the child aborts); a `log` line of an isolate goes to stderr; a
//   `spanNotMade` report of an isolate goes to the reports of the process.
// - At the child's stop the exit rule asks for `flush`: each isolate that took its runs (free or running) flushes its
//   seat body observer and answers `flushed`; the flush ends when each one answered or ended. A booting isolate ran no
//   body and is not asked. The exit rule bounds the wait, so a sync body that holds its isolate does not hold the end.
// - The pool never calls `worker.terminate()`: the normal stop is the cancel signal, then the end of the child.

import type { BodyEnd } from "../child/body-observer.ts";
import type { CancelCauseName, ChildPost } from "../native/child/addon.ts";
import type { NotMadeSpan } from "../native/logging/addon.ts";
import type { SeatBounds } from "../native/seats/addon.ts";
import { CHILD_HOOK_FAILED_EXIT_CODE } from "../_catalog/exit_codes.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { EndedSeatRun } from "./body-observer.ts";
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

/** One isolate, as the pool posts to it. */
export interface SeatIsolate {
  post(message: MainToSeat): void;
}

/** What an isolate reports to the pool. Each event of an isolate that a later isolate replaced is left. */
export interface IsolateEvents {
  /** A message of the isolate, not parsed yet. */
  message(message: object): void;
  /** A message of the isolate that does not deserialize: a defect of the package. */
  unreadable(cause: unknown): void;
  /** The isolate threw an uncaught error, or reached its memory cap; its exit follows. */
  error(cause: unknown): void;
  exit(exitCode: number): void;
}

/** The start of one isolate: the isolate, or why the runtime did not start it (for example a thread limit of the OS). */
export type IsolateStart = { readonly kind: "started"; readonly isolate: SeatIsolate } | { readonly kind: "notStarted"; readonly cause: string };

/** Start the isolate of `marker`, which reports to `events`. Never throws. */
export type StartIsolate = (marker: SeatMarker, events: IsolateEvents) => IsolateStart;

/** The `runBody` post of a core seat thread. */
export type SeatRunPost = Extract<ChildPost, { readonly kind: "runBody" }>;

/** What the seats of a child need from the process. */
export interface SeatHost {
  readonly start: StartIsolate;
  /** The bounds of the seats, from the core's constants (`seatBounds` of the addon). The pool reads them once. */
  readonly bounds: () => SeatBounds;
  /** A monotonic clock, in milliseconds. */
  readonly clock: () => number;
  /** The time now, for the `endedAt` of a verdict. */
  readonly now: () => Temporal.Instant;
  /** Call `callback` after `milliseconds`; the result cancels it. */
  readonly timer: (milliseconds: number, callback: () => void) => () => void;
}

/** What the pool does that has effects. */
export interface SeatPoolDeps extends SeatHost {
  /** Settle the run `runKey` of `taskName` with `outcome` (the settle call of the core's seat thread). */
  readonly settle: (runKey: bigint, taskName: string, outcome: SeatOutcome) => void;
  /** Report a run whose isolate ended before its result, for its span: the seat body observer's `isolateEnded`. It never throws. */
  readonly isolateEnded: (run: EndedSeatRun) => void;
  /** Run one operation of an isolate on the main thread. Rejects only with a defect of the package. */
  readonly serve: (request: SeatRequest) => Promise<SeatAnswer>;
  /** End the child with `code` through the exit rule. */
  readonly endChild: (code: number) => void;
  readonly writeLine: (line: string) => void;
  /** The report of a span that an isolate did not make, with the text of the cause: once per process and kind. */
  readonly spanNotMade: (span: NotMadeSpan, cause: string) => void;
  /** The defect rule: it never throws. */
  readonly reportDefect: (defect: CoreDefect) => void;
}

/** One isolate that the pool started, the error that it reported before its exit, and the flushes that wait for it. */
class StartedIsolate {
  #port: SeatIsolate | undefined;
  #error: { readonly kind: "noError" } | { readonly kind: "error"; readonly cause: unknown } = { kind: "noError" };
  /** The flushes that wait for the answer of the isolate: one per `flush` call, and the exit rule calls it once. */
  readonly #flushes: (() => void)[] = [];

  attach(port: SeatIsolate): void {
    this.#port = port;
  }

  post(message: MainToSeat): void {
    if (this.#port === undefined) {
      throw poolDefect("a post to an isolate before its start returned");
    }

    this.#port.post(message);
  }

  failed(cause: unknown): void {
    this.#error = { kind: "error", cause };
  }

  /** Post `flush`; the promise resolves at the isolate's `flushed`, or at its exit. */
  async flush(): Promise<void> {
    const answered = Promise.withResolvers<void>();

    this.#flushes.push(answered.resolve);
    this.post({ kind: "flush", protocol: SEAT_PROTOCOL_VERSION });

    return answered.promise;
  }

  /** The isolate answered `flushed`, or it ended: each flush that waits for it resolves. */
  flushesEnded(): void {
    for (const resolve of this.#flushes.splice(0)) {
      resolve();
    }
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
type Boot = { readonly kind: "first" } | { readonly kind: "replacement"; readonly stopTimer: () => void };

/** The cancel that a run got: none yet, or the first cause. */
type RunCancel = { readonly kind: "notRequested" } | { readonly kind: "requested"; readonly cause: CancelCauseName };

/** A run on a seat, with the cancel that it got. */
type SeatRun = { readonly post: SeatRunPost; readonly cancel: RunCancel };

/** The stage of a seat: its isolate boots, is free, runs a run; or the seat has no isolate (the child ends). */
export type SeatStage = "booting" | "free" | "running" | "ended";

/** The state of one seat. */
type SeatState =
  | { readonly kind: "booting"; readonly isolate: StartedIsolate; readonly boot: Boot }
  | { readonly kind: "free"; readonly isolate: StartedIsolate }
  | { readonly kind: "running"; readonly isolate: StartedIsolate; readonly run: SeatRun; readonly startedAt: Temporal.Instant }
  | { readonly kind: "ended" };

/** A run that waits for a free isolate, since `since` (the clock). */
type WaitingRun = { readonly run: SeatRun; readonly since: number };

/** The stage of the pool: it serves, or it ended the child and the child ends after its exit flush. */
export type PoolStage = "serving" | "ending";

const NOT_REQUESTED: RunCancel = Object.freeze({ kind: "notRequested" });

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
  readonly #bounds: SeatBounds;
  readonly #cells: Int32Array;
  readonly #seats: SeatState[];
  readonly #waiting: WaitingRun[] = [];
  readonly #boot: PromiseWithResolvers<void>;
  #booting: number;
  #stage: PoolStage = "serving";

  constructor(seats: number, deps: SeatPoolDeps, boot: PromiseWithResolvers<void>) {
    this.#deps = deps;
    this.#bounds = deps.bounds();
    this.#cells = newCancelCells(seats);
    this.#boot = boot;
    this.#booting = seats;
    this.#seats = Array.from({ length: seats }, (): SeatState => ({ kind: "ended" }));

    for (let seat = 0; seat < seats; seat += 1) {
      this.#bootSeat(seat, { kind: "first" });
    }
  }

  /** The run of `post` goes to a free isolate, or waits for the new isolate of a seat. Left while the child ends. */
  run(post: SeatRunPost): void {
    if (this.#stage === "ending") {
      return;
    }

    const seat = this.#seats.findIndex((state) => state.kind === "free");

    if (seat === -1) {
      this.#wait({ post, cancel: NOT_REQUESTED });

      return;
    }

    this.#startRun(seat, { post, cancel: NOT_REQUESTED });
  }

  /** The cancel signal of the run `runKey` fired with `cause`. Left while the child ends. */
  cancel(runKey: bigint, cause: CancelCauseName): void {
    if (this.#stage === "ending") {
      return;
    }

    const seat = this.#seats.findIndex((state) => state.kind === "running" && state.run.post.runKey === runKey);
    const state = this.#seats[seat];

    if (state !== undefined && state.kind === "running") {
      requestCancel(this.#cells, seat, cause);
      this.#seats[seat] = { ...state, run: { ...state.run, cancel: firstCancel(state.run.cancel, cause) } };
      state.isolate.post({ kind: "cancel", protocol: SEAT_PROTOCOL_VERSION, runKey, cause });

      return;
    }

    const waiting = this.#waiting.findIndex((entry) => entry.run.post.runKey === runKey);
    const entry = this.#waiting[waiting];

    if (entry !== undefined) {
      this.#waiting[waiting] = { ...entry, run: { ...entry.run, cancel: firstCancel(entry.run.cancel, cause) } };
    }
  }

  /** The stage of each seat, in seat order. */
  stages(): readonly SeatStage[] {
    return this.#seats.map((state) => state.kind);
  }

  /** The stage of the pool. */
  stage(): PoolStage {
    return this.#stage;
  }

  /** The runs that wait for a free isolate. */
  waiting(): number {
    return this.#waiting.length;
  }

  /**
   * Ask each isolate that took its runs (free or running) to flush its seat body observer. Resolves when each one
   * answered or ended; rejects only when a post to an isolate throws. The exit rule of the child bounds the wait.
   */
  async flush(): Promise<void> {
    const answers = this.#seats.flatMap((state): Promise<void>[] => {
      switch (state.kind) {
        case "free":
        case "running":
          return [state.isolate.flush()];
        case "booting":
        case "ended":
          return [];
        default:
          return unreachable(state, "SeatPool.flush");
      }
    });

    await Promise.all(answers);
  }

  /** Start the isolate of `seat` for `boot`; one that does not start fails the boot. */
  #bootSeat(seat: number, boot: Boot): void {
    const isolate = new StartedIsolate();

    const marker: SeatMarker = {
      seatIndex: seat,
      protocol: SEAT_PROTOCOL_VERSION,
      cancelCells: this.#cells,
      relayBounds: { opsInFlight: this.#bounds.opsInFlight, opsWaiting: this.#bounds.opsWaiting },
    };

    const started = this.#deps.start(marker, this.#events(seat, isolate));

    switch (started.kind) {
      case "started":
        isolate.attach(started.isolate);
        this.#seats[seat] = { kind: "booting", isolate, boot };

        return;
      case "notStarted":
        this.#seats[seat] = { kind: "ended" };
        this.#bootFailed(boot, `the seat isolate ${String(seat)} did not start: ${started.cause}`);

        return;
      default:
        unreachable(started, "SeatPool.bootSeat");
    }
  }

  #events(seat: number, isolate: StartedIsolate): IsolateEvents {
    return {
      message: (message) => {
        this.#guarded(() => {
          this.#onMessage(seat, isolate, parseSeatToMain(message));
        });
      },
      unreadable: (cause) => {
        this.#deps.reportDefect(poolDefect(`a message of the seat isolate ${String(seat)} does not deserialize: ${String(cause)}`));
      },
      error: (cause) => {
        isolate.failed(cause);
      },
      exit: (exitCode) => {
        isolate.flushesEnded();
        this.#guarded(() => {
          this.#onExit(seat, isolate, exitCode);
        });
      },
    };
  }

  /** Run `step`; a defect goes to the defect rule, so an event handler never throws. */
  #guarded(step: () => void): void {
    try {
      step();
    } catch (cause) {
      this.#deps.reportDefect(cause instanceof CoreDefect ? cause : poolDefect(`an event of an isolate threw: ${String(cause)}`));
    }
  }

  /** The state of `seat` when its isolate is `isolate`, or `undefined` for an event of a replaced isolate. */
  #currentState(seat: number, isolate: StartedIsolate): Exclude<SeatState, { readonly kind: "ended" }> | undefined {
    const state = this.#seats[seat];

    return state === undefined || state.kind === "ended" || state.isolate !== isolate ? undefined : state;
  }

  #onMessage(seat: number, isolate: StartedIsolate, message: SeatToMain): void {
    const state = this.#currentState(seat, isolate);

    if (state === undefined) {
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
      case "defect":
        this.#deps.reportDefect(
          new ClientDefect({ subject: `the seat isolate ${String(seat)}`, rule: `${message.name} (${message.code}): ${message.message}` }),
        );

        return;
      case "log":
        this.#deps.writeLine(message.line);

        return;
      case "flushed":
        state.isolate.flushesEnded();

        return;
      case "spanNotMade":
        this.#deps.spanNotMade(message.span, message.cause);

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
    this.#bootFailed(state.boot, cause);
  }

  /** A boot that failed: the first boot rejects (the hook fails); a replacement ends the child with 173. */
  #bootFailed(boot: Boot, cause: string): void {
    switch (boot.kind) {
      case "first":
        this.#boot.reject(new Error(cause));

        return;
      case "replacement":
        boot.stopTimer();
        this.#endChild(`${cause}; the child ends with ${String(CHILD_HOOK_FAILED_EXIT_CODE)}`);

        return;
      default:
        unreachable(boot, "SeatPool.bootFailed");
    }
  }

  #onResult(seat: number, state: SeatState, runKey: bigint, outcome: SeatOutcome): void {
    if (state.kind !== "running" || state.run.post.runKey !== runKey) {
      throw poolDefect(`the seat isolate ${String(seat)} sent the result of the run ${String(runKey)}, which does not run there`);
    }

    this.#seats[seat] = { kind: "free", isolate: state.isolate };
    this.#deps.settle(runKey, state.run.post.taskName, outcome);
    state.isolate.post({
      kind: "bodyEnded",
      protocol: SEAT_PROTOCOL_VERSION,
      runKey,
      end: verdictOf(state.run.cancel, outcome),
      endedAtEpochNanoseconds: this.#deps.now().epochNanoseconds,
    });
    this.#nextWaiting(seat);
  }

  #serve(isolate: StartedIsolate, requestId: number, request: SeatRequest): void {
    this.#deps.serve(request).then(
      (answer) => {
        // An answer to an isolate that ended reaches no one: its operations ended with it.
        if (this.#seats.some((state) => state.kind !== "ended" && state.isolate === isolate)) {
          isolate.post({ kind: "opReply", protocol: SEAT_PROTOCOL_VERSION, requestId, answer });
        }
      },
      (cause: unknown) => {
        this.#deps.reportDefect(cause instanceof CoreDefect ? cause : poolDefect(`a seat operation failed: ${String(cause)}`));
      },
    );
  }

  #onExit(seat: number, isolate: StartedIsolate, exitCode: number): void {
    const state = this.#currentState(seat, isolate);

    if (state === undefined) {
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
      case "running": {
        const post = state.run.post;

        this.#deps.writeLine(
          `horsies child: ${ended}, during the run of task ${post.taskName} (task ${post.taskId}, attempt ${String(post.attempt)}); the run ends WORKER_CRASHED and a new isolate takes the seat`,
        );
        this.#seats[seat] = { kind: "ended" };
        this.#deps.settle(post.runKey, post.taskName, isolateEndOutcome({ seat, taskId: post.taskId, end }));
        this.#deps.isolateEnded({ facts: post.facts, startedAt: state.startedAt, endedAt: this.#deps.now() });
        this.#replace(seat);

        return;
      }

      default:
        unreachable(state, "SeatPool.onExit");
    }
  }

  /** A new isolate for `seat`, bounded by the start-hook bound. Not while the child ends. */
  #replace(seat: number): void {
    if (this.#stage === "ending") {
      return;
    }

    const bound = this.#bounds.replacementBootBoundMs;
    const timer = { stop: (): void => undefined };
    const boot: Boot = { kind: "replacement", stopTimer: () => timer.stop() };

    timer.stop = this.#deps.timer(bound, () => {
      this.#guarded(() => {
        this.#replacementTimedOut(seat, boot, bound);
      });
    });
    this.#bootSeat(seat, boot);
  }

  /** The bound of the boot `boot` of `seat` passed: when that boot still runs, the child ends with 173. */
  #replacementTimedOut(seat: number, boot: Boot, bound: number): void {
    const state = this.#seats[seat];

    if (state === undefined || state.kind !== "booting" || state.boot !== boot) {
      return;
    }

    this.#seats[seat] = { kind: "ended" };
    this.#endChild(
      `the new isolate of the seat ${String(seat)} was not ready within ${String(bound)} ms; the child ends with ${String(CHILD_HOOK_FAILED_EXIT_CODE)}`,
    );
  }

  /** End the child with 173, once; the pool is `ending` from here on. */
  #endChild(cause: string): void {
    if (this.#stage === "ending") {
      return;
    }

    this.#stage = "ending";
    this.#deps.writeLine(`horsies child: ${cause}`);
    this.#deps.endChild(CHILD_HOOK_FAILED_EXIT_CODE);
  }

  /** A run waits for the new isolate of a seat. A wait with no new isolate on the way is a defect of the accounts. */
  #wait(run: SeatRun): void {
    const replacing = this.#seats.some((state) => state.kind === "booting" && state.boot.kind === "replacement");

    if (!replacing || this.#waiting.length >= this.#seats.length) {
      throw poolDefect(`the run of task ${run.post.taskName} found no free isolate, and no new isolate is on the way for it`);
    }

    this.#waiting.push({ run, since: this.#deps.clock() });
  }

  #nextWaiting(seat: number): void {
    const entry = this.#waiting.shift();

    if (entry === undefined) {
      return;
    }

    const post = entry.run.post;
    const waited = Math.round(this.#deps.clock() - entry.since);

    this.#deps.writeLine(
      `horsies child: the run of task ${post.taskName} (task ${post.taskId}, attempt ${String(post.attempt)}) waited ${String(waited)} ms for the new isolate of a seat`,
    );
    this.#startRun(seat, entry.run);
  }

  #startRun(seat: number, run: SeatRun): void {
    const state = this.#seats[seat];

    if (state === undefined || state.kind !== "free") {
      throw poolDefect(`a run went to the seat ${String(seat)}, which is not free`);
    }

    clearCancel(this.#cells, seat);
    this.#seats[seat] = { kind: "running", isolate: state.isolate, run, startedAt: this.#deps.now() };

    const { runKey, taskName, inputJson, taskId, attempt, maxRetries, facts } = run.post;

    state.isolate.post({ kind: "run", protocol: SEAT_PROTOCOL_VERSION, runKey, taskName, inputJson, taskId, attempt, maxRetries, facts });

    switch (run.cancel.kind) {
      case "notRequested":
        return;
      case "requested":
        requestCancel(this.#cells, seat, run.cancel.cause);
        state.isolate.post({ kind: "cancel", protocol: SEAT_PROTOCOL_VERSION, runKey, cause: run.cancel.cause });

        return;
      default:
        unreachable(run.cancel, "SeatPool.startRun");
    }
  }
}

/** The cancel of a run after a cancel with `cause`: the first cause stays. Pure. */
function firstCancel(cancel: RunCancel, cause: CancelCauseName): RunCancel {
  switch (cancel.kind) {
    case "notRequested":
      return { kind: "requested", cause };
    case "requested":
      return cancel;
    default:
      return unreachable(cancel, "firstCancel");
  }
}

/**
 * The verdict of a run: a deadline cancel is `timedOut` and an operator cancel `cancelled` (the core ends the run so,
 * whatever the body gives); else the outcome. The rule of the body adapter of the child (`bodyEnd`). Pure.
 */
export function verdictOf(cancel: RunCancel, outcome: SeatOutcome): BodyEnd {
  switch (cancel.kind) {
    case "requested":
      return cancelVerdict(cancel.cause);
    case "notRequested":
      return outcomeVerdict(outcome);
    default:
      return unreachable(cancel, "verdictOf");
  }
}

function cancelVerdict(cause: CancelCauseName): BodyEnd {
  switch (cause) {
    case "DEADLINE":
      return { kind: "timedOut" };
    case "OPERATOR":
      return { kind: "cancelled" };
    default:
      return unreachable(cause, "cancelVerdict");
  }
}

function outcomeVerdict(outcome: SeatOutcome): BodyEnd {
  switch (outcome.kind) {
    case "value":
      return { kind: "returned" };
    case "error":
      return { kind: "failed", code: outcome.parts.code.code };
    default:
      return unreachable(outcome, "outcomeVerdict");
  }
}

function poolDefect(rule: string): ClientDefect {
  return new ClientDefect({ subject: "seat pool", rule });
}
