// The run table of the JavaScript side of a child: one entry per run whose body runs now, and one per piece of
// spawned work that has not settled. Each entry holds the `AbortController` of its signal.
//
// - `enter` makes the controls of a run (`RunControls`); `leave` takes the run out when its body settled. Spawned work
//   stays in the table until its Promise settles.
// - `cancel` (the core's `cancelRun` post) aborts the run's signal and the signal of each piece of its work with
//   `{ cause }`; the first cause stays.
// - `stopWork` (the core's `stopWork` post) aborts the signal of one piece with its cause, `LEFT_WORK` included.
// - A post for a run or a piece that is not in the table is left: the run settled and the post crossed it.
// - The table is bounded by the core: at most the seats of the child, and at most `MAX_RUN_WORK` pieces per run.

import type { TaskId } from "../contract/ids.ts";
import type {
  CancelCauseName,
  RunCancelStateRecord,
  SpawnEnd,
  SpawnRegistration,
  WorkStopCauseName,
} from "../native/child/addon.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { AttemptNumber } from "./attempt.ts";
import { type RunMark, runStore } from "./run-store.ts";
import {
  type CancelState,
  NOT_REQUESTED,
  requestedState,
  type RunControls,
  type SpawnedWork,
  type SpawnRefused,
} from "./runtime.ts";
import type { StateKey, StateValues } from "./state.ts";

/** The calls of the addon that a run makes. */
export interface RunNative {
  runCancelStateOf(runKey: bigint): RunCancelStateRecord;
  registerSpawn(runKey: bigint, name: string): SpawnRegistration;
  endSpawn(workKey: bigint): SpawnEnd;
}

/** The run whose code the loop runs now, as the marks hold it. */
export type LoopHolder = { readonly kind: "noRun" } | { readonly kind: "run"; readonly taskId: string };

/**
 * The marks of the loop watch: the run whose synchronous code runs now. The core's mark has no nesting, so a caller
 * puts back the holder that its `hold` gave: a spawn in the synchronous part of a body keeps the body's mark.
 */
export interface LoopMarks {
  /** The loop runs code of the run of `taskId` now. Gives the holder that was in force. */
  hold(taskId: string): LoopHolder;
  /** Put back `holder`, which a `hold` gave. */
  restore(holder: LoopHolder): void;
}

/** The facts of one run, from the core's `runBody` post. */
export type RunFacts = {
  readonly runKey: bigint;
  readonly taskId: TaskId;
  readonly taskName: string;
  readonly attempt: AttemptNumber;
};

/** One run of the table. `seen` is the cancel state that the posts gave. */
type RunEntry = { readonly controller: AbortController; readonly works: Set<bigint>; seen: CancelState };

/** One piece of spawned work of the table. */
type WorkEntry = { readonly runKey: bigint; readonly controller: AbortController };

/** The run table of one child. */
export class RunTable {
  readonly #runs = new Map<bigint, RunEntry>();
  readonly #works = new Map<bigint, WorkEntry>();
  readonly #native: RunNative;
  readonly #marks: LoopMarks;
  readonly #states: StateValues;

  constructor(native: RunNative, marks: LoopMarks, states: StateValues) {
    this.#native = native;
    this.#marks = marks;
    this.#states = states;
  }

  /** Enter the run of `facts` and give its controls. A run key that is in the table is a defect of the core. */
  enter(facts: RunFacts): RunControls {
    if (this.#runs.has(facts.runKey)) {
      throw new ClientDefect({ subject: "runBody", rule: "the core posted a run key that runs already" });
    }

    const entry: RunEntry = { controller: new AbortController(), works: new Set(), seen: NOT_REQUESTED };

    this.#runs.set(facts.runKey, entry);

    return this.#controls(facts, entry);
  }

  /** The body of the run `runKey` settled: the run leaves the table. Its spawned work stays until it settles. */
  leave(runKey: bigint): void {
    this.#runs.delete(runKey);
  }

  /** The cancel signal of the run `runKey` fired with `cause`. */
  cancel(runKey: bigint, cause: CancelCauseName): void {
    const entry = this.#runs.get(runKey);

    if (entry === undefined) {
      return;
    }

    if (entry.seen.kind === "notRequested") {
      entry.seen = requestedState(cause);
    }

    abortWith(entry.controller, cause);

    for (const workKey of entry.works) {
      const work = this.#works.get(workKey);

      if (work !== undefined) {
        abortWith(work.controller, cause);
      }
    }
  }

  /** A cancel round asks the spawned work `workKey` to stop, for `cause`. */
  stopWork(workKey: bigint, cause: WorkStopCauseName): void {
    const work = this.#works.get(workKey);

    if (work !== undefined) {
      abortWith(work.controller, cause);
    }
  }

  /** The runs and the pieces of work in the table. */
  size(): { readonly runs: number; readonly works: number } {
    return { runs: this.#runs.size, works: this.#works.size };
  }

  #controls(facts: RunFacts, entry: RunEntry): RunControls {
    return Object.freeze({
      taskId: facts.taskId,
      attempt: facts.attempt,
      signal: entry.controller.signal,
      cancelState: (): CancelState => this.#cancelState(facts.runKey, entry),
      spawn: <T>(name: string, work: SpawnedWork<T>): Result<Promise<T>, SpawnRefused> =>
        this.#spawn(facts, entry, name, work),
      state: <T>(key: StateKey<T>): T => this.#states.value(key),
    });
  }

  /** The cancel state of a run: the addon's read while the run is in the core's table, else what the posts gave. */
  #cancelState(runKey: bigint, entry: RunEntry): CancelState {
    const read = this.#native.runCancelStateOf(runKey);

    switch (read.kind) {
      case "notRequested":
        return entry.seen;
      case "requested":
        return requestedState(read.cause);
      case "runEnded":
        return entry.seen;
      default:
        return unreachable(read, "cancelState");
    }
  }

  #spawn<T>(facts: RunFacts, entry: RunEntry, name: string, work: SpawnedWork<T>): Result<Promise<T>, SpawnRefused> {
    const registration = this.#native.registerSpawn(facts.runKey, name);

    switch (registration.kind) {
      case "registered":
        return ok(this.#startWork(facts, entry, registration.workKey, work));
      case "runEnded":
        return err(RUN_ENDED);
      case "tooMuchWork":
        return err(Object.freeze({ kind: "tooMuchWork", bound: registration.bound }));
      default:
        return unreachable(registration, "spawn");
    }
  }

  /** Start registered work: its signal, the synchronous part inside the run store and the loop marks, its end. */
  #startWork<T>(facts: RunFacts, entry: RunEntry, workKey: bigint, work: SpawnedWork<T>): Promise<T> {
    const controller = new AbortController();

    this.#works.set(workKey, { runKey: facts.runKey, controller });
    entry.works.add(workKey);

    if (entry.seen.kind === "requested") {
      abortWith(controller, entry.seen.cause);
    }

    const started = startedWork(facts, this.#marks, () => work(controller.signal));

    return started.finally(() => {
      this.#works.delete(workKey);
      entry.works.delete(workKey);
      this.#native.endSpawn(workKey);
    });
  }
}

/** The refusal of a spawn after the run ended. */
const RUN_ENDED: SpawnRefused = Object.freeze({ kind: "runEnded" });

/** Abort `controller` with `{ cause }`, unless it fired before: the first cause stays. */
function abortWith(controller: AbortController, cause: CancelCauseName | WorkStopCauseName): void {
  if (!controller.signal.aborted) {
    controller.abort(Object.freeze({ cause }));
  }
}

/**
 * Run the synchronous part of `start` inside the run store, with the run held in the loop marks; a sync throw is a
 * rejected Promise.
 */
function startedWork<T>(facts: RunFacts, marks: LoopMarks, start: () => Promise<T>): Promise<T> {
  const mark: RunMark = { taskId: facts.taskId, taskName: facts.taskName, attempt: facts.attempt };

  const before = marks.hold(facts.taskId);

  try {
    return Promise.resolve(runStore.run(mark, start));
  } catch (cause) {
    return Promise.reject(cause);
  } finally {
    marks.restore(before);
  }
}
