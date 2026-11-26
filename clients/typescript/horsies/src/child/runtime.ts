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
// - `workflowContext`, in the runtime type of a task with `workflowContext: true` only (`WorkflowTaskRuntime`): the
//   context that the core wrote into the run's input, or `noContext`. Every runtime object holds the member; the
//   class type does not name it, so a body of a task that takes no context cannot read it.
//
// A runtime asks its run for each of these (`RunControls`): the run table of a child, or the fixed answers of
// `TaskRuntime.forTest`. The workflow context is a fact of the run's input, given when the runtime is built.

import type { TaskId } from "../contract/ids.ts";
import { testRunControls, type TestRuntimeOptions, testWorkflowContext } from "../testing/task-runtime.ts";
import type { Result } from "../wire/result.ts";
import type { RuntimeWorkflowContext } from "../workflow/context.ts";
import type { AttemptNumber } from "./attempt.ts";
import type { AnyStateKey, StateKey } from "./state.ts";

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
let constructRuntime: (controls: RunControls, context: RuntimeWorkflowContext) => WorkflowTaskRuntime;

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
    constructRuntime = (controls: RunControls, context: RuntimeWorkflowContext): WorkflowTaskRuntime =>
      // SAFETY: the constructor defines the `workflowContext` member, frozen, on every runtime object.
      new TaskRuntime(controls, context) as WorkflowTaskRuntime;
  }

  private constructor(controls: RunControls, context: RuntimeWorkflowContext) {
    this.taskId = controls.taskId;
    this.fencingToken = Object.freeze({ taskId: controls.taskId, value: controls.attempt });
    this.signal = controls.signal;
    this.#controls = controls;
    // The member of `WorkflowTaskRuntime`. The class type does not name it: a body of a task that takes no context has
    // the type `TaskRuntime`, with no `workflowContext`.
    Object.defineProperty(this, "workflowContext", { value: context, enumerable: true });
    Object.freeze(this);
  }

  /**
   * A runtime for a test of a task body, with no worker, no database and no addon: the task id and the attempt of
   * `options`; a signal that never fires; `spawn` that runs its work at once; `state` from the entries of `options`,
   * each a key that `app.provide` returned and a value of the key's type; `workflowContext` read from the stored form
   * of `options.workflowContext`, or `noContext` when `options` has none. The runtime fits the body of a task with or
   * without `workflowContext: true`. A stored form that does not read as a workflow context throws `TypeError`.
   */
  static forTest<const Keys extends readonly AnyStateKey[]>(options: TestRuntimeOptions<Keys>): WorkflowTaskRuntime {
    return constructRuntime(testRunControls(options), testWorkflowContext(options.workflowContext));
  }

  /** Whether a cancel was requested, and its first cause. Reads with no turn of the event loop. */
  cancelState(): CancelState {
    return this.#controls.cancelState();
  }

  /**
   * Start `work` as work of this run, named `name` in the reports. The returned Promise settles with the work. A
   * refusal starts nothing: `runEnded` once the run is over (its body returned and the work it left ended or was
   * stopped), `tooMuchWork` at the bound of 10,000 pieces. Work that the body spawned may spawn more work after the
   * body returned: it belongs to the run, as in the Rust and Python clients.
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

/** The runtime of an `ISOLATED` or `SHARED_ASYNC` body of a task with `workflowContext: true`. */
export type WorkflowTaskRuntime = TaskRuntime & {
  /** The context that the core wrote for the run's node (its `ctxFrom`), or `noContext`. */
  readonly workflowContext: RuntimeWorkflowContext;
};

/** The runtime of a `SHARED_THREAD` body of a task with `workflowContext: true`: it has no `spawn`. */
export type ThreadWorkflowTaskRuntime = ThreadTaskRuntime & Pick<WorkflowTaskRuntime, "workflowContext">;

/** The runtime of `controls` with the workflow context of the run's input. Not exported from an entry point. */
export function taskRuntimeOf(controls: RunControls, context: RuntimeWorkflowContext): WorkflowTaskRuntime {
  return constructRuntime(controls, context);
}

/** The cancel state of a run with no cancel. */
export const NOT_REQUESTED: CancelState = Object.freeze({ kind: "notRequested" });

/** The cancel state of a run whose signal fired with `cause`. Pure. */
export function requestedState(cause: CancelCause): CancelState {
  return Object.freeze({ kind: "requested", cause });
}
