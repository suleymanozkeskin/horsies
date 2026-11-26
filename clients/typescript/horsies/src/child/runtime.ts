// The `runtime` object of a task body: the task id, the fencing token, the cancel signal and its state, tracked work
// (`spawn`), and the app state of this process.
//
// - `signal` fires at an operator cancel in every class, and at the deadline in `SHARED_ASYNC` (in `ISOLATED` the
//   worker kills the child at the deadline). Its reason is `{ cause: "DEADLINE" }` or `{ cause: "OPERATOR" }`; the
//   first cause stays.
// - `cancelState()` reads the cause with no turn of the event loop, so a sync body that holds the thread reads it.
// - `spawn(name, (signal) => work)` starts work that belongs to the run: the run's work set counts it until its
//   Promise settles. Its own signal fires at the deadline, at an operator cancel, and at the stop of left work
//   (`{ cause: "LEFT_WORK" }`). `work` is a function, so a refusal starts nothing. A plain Promise that the body does
//   not await is not tracked.
// - `state(key)` returns the value that the key's provider built in this process.
//
// A runtime asks its run for each of these (`RunControls`): the run table of a child, or the fixed answers of
// `TaskRuntime.forTest`.

import type { TaskId } from "../contract/ids.ts";
import type { Result } from "../wire/result.ts";
import type { AttemptNumber } from "./attempt.ts";
import type { StateKey } from "./state.ts";

/** Why the cancel signal of a run fired. */
export type CancelCause = "DEADLINE" | "OPERATOR";

/** The reason of `runtime.signal`. */
export type CancelReason = { readonly cause: CancelCause };

/** The reason of the signal of spawned work: the run's cause, or the stop of the work that the body left. */
export type WorkStopReason = { readonly cause: CancelCause | "LEFT_WORK" };

/** The cancel state of a run, for code that checks between steps. */
export type CancelState = { readonly kind: "notRequested" } | { readonly kind: "requested"; readonly cause: CancelCause };

/** The fencing token of a run: write it with your own side effects, so a write of an older attempt can be refused. */
export type FencingToken = { readonly taskId: TaskId; readonly value: AttemptNumber };

/** Why `spawn` started nothing: the run has ended, or it holds `bound` pieces of work already. */
export type SpawnRefused = { readonly kind: "runEnded" } | { readonly kind: "tooMuchWork"; readonly bound: number };

/** The work of `runtime.spawn`: it gets its own signal. */
export type SpawnedWork<T> = (signal: AbortSignal) => Promise<T>;

/** What a runtime asks of its run. */
export interface RunControls {
  readonly taskId: TaskId;
  readonly attempt: AttemptNumber;
  readonly signal: AbortSignal;
  cancelState(): CancelState;
  spawn<T>(name: string, work: SpawnedWork<T>): Result<Promise<T>, SpawnRefused>;
  state<T>(key: StateKey<T>): T;
}

/** The constructor of a runtime, for `taskRuntimeOf` in this module. */
let constructRuntime: (controls: RunControls) => TaskRuntime;

/** The run context of a task body. The child builds it; `TaskRuntime.forTest` builds one with no worker. */
export class TaskRuntime {
  /** The id of the task. */
  readonly taskId: TaskId;
  /** `{ taskId, value }`: `value` is the attempt number, greater for each later attempt of the task. */
  readonly fencingToken: FencingToken;
  /** Fires at an operator cancel, and at the deadline of a `SHARED_ASYNC` run. */
  readonly signal: AbortSignal;
  readonly #controls: RunControls;

  static {
    constructRuntime = (controls: RunControls): TaskRuntime => new TaskRuntime(controls);
  }

  private constructor(controls: RunControls) {
    this.taskId = controls.taskId;
    this.fencingToken = Object.freeze({ taskId: controls.taskId, value: controls.attempt });
    this.signal = controls.signal;
    this.#controls = controls;
    Object.freeze(this);
  }

  /** Whether a cancel was requested, and its first cause. Reads with no turn of the event loop. */
  cancelState(): CancelState {
    return this.#controls.cancelState();
  }

  /**
   * Start `work` as work of this run, named `name` in the reports. The returned Promise settles with the work. A
   * refusal starts nothing: `runEnded` after the body returned, `tooMuchWork` at the bound of 10,000 pieces.
   */
  spawn<T>(name: string, work: SpawnedWork<T>): Result<Promise<T>, SpawnRefused> {
    return this.#controls.spawn(name, work);
  }

  /** The value that the provider of `key` built in this process. A key of another app is a defect: `ClientDefect`. */
  state<T>(key: StateKey<T>): T {
    return this.#controls.state(key);
  }
}

/** The runtime of a `SHARED_THREAD` body: it has no `spawn`. */
export type ThreadTaskRuntime = Pick<TaskRuntime, "taskId" | "fencingToken" | "signal" | "cancelState" | "state">;

/** The runtime of `controls`. Not exported from an entry point. */
export function taskRuntimeOf(controls: RunControls): TaskRuntime {
  return constructRuntime(controls);
}

/** The cancel state of a run with no cancel. */
export const NOT_REQUESTED: CancelState = Object.freeze({ kind: "notRequested" });

/** The cancel state of a run whose signal fired with `cause`. Pure. */
export function requestedState(cause: CancelCause): CancelState {
  return Object.freeze({ kind: "requested", cause });
}
