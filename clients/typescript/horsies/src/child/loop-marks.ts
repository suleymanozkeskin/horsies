// The marks of the loop watch in a task child.
//
// - The turn timer calls `loopTurned()` every `LOOP_TURN_TICK_MS` (the core's `HOST_LOOP_TURN_TICK`, Python's
//   `LOOP_TURN_TICK_S`). The timer does not keep the process alive.
// - The body adapter and `runtime.spawn` hold the run around their synchronous parts (`LoopMarks`).
// - The culprit method after an `await`, on Node only: `v8.promiseHooks` keeps the run of each Promise that code of a
//   run creates, and marks that run before each continuation of the Promise and releases it after. Bun has no method
//   (no `v8.promiseHooks`; `async_hooks` does not call back), so a block after an `await` names no culprit there. The
//   child starts the method in a shared async child only: the other kinds watch no loop.

import * as v8 from "node:v8";

import type { LoopHolder, LoopMarks } from "./run-table.ts";
import { runStore } from "./run-store.ts";

/** How often the turn timer records a turn of the loop. */
export const LOOP_TURN_TICK_MS = 100;

/** The loop marks of the addon. */
export interface LoopNative {
  loopTurned(): void;
  loopHold(taskId: string): void;
  loopRelease(): void;
}

/** A timer that calls `tick` every `milliseconds` and does not keep the process alive; the result stops it. */
export type TurnTimer = (milliseconds: number, tick: () => void) => () => void;

/** The turn timer of this process. */
export const processTurnTimer: TurnTimer = (milliseconds, tick) => {
  const timer = setInterval(tick, milliseconds);

  timer.unref();

  return () => {
    clearInterval(timer);
  };
};

/** Start the turns of the loop; the result stops them. */
export function startTurns(native: LoopNative, timer: TurnTimer): () => void {
  native.loopTurned();

  return timer(LOOP_TURN_TICK_MS, () => {
    native.loopTurned();
  });
}

/** The holder of no run. */
const NO_RUN: LoopHolder = Object.freeze({ kind: "noRun" });

/**
 * The loop marks of the body adapter, of `runtime.spawn` and of the culprit method, on the addon. They keep the
 * holder in force, because the core's mark has no nesting: `restore` puts back the holder that a `hold` gave.
 */
export function loopMarksOf(native: LoopNative): LoopMarks {
  let current: LoopHolder = NO_RUN;

  return {
    hold: (taskId) => {
      const before = current;

      current = Object.freeze({ kind: "run", taskId });
      native.loopHold(taskId);

      return before;
    },
    restore: (holder) => {
      current = holder;

      switch (holder.kind) {
        case "noRun":
          native.loopRelease();

          return;
        case "run":
          native.loopHold(holder.taskId);

          return;
        default: {
          const unexpected: never = holder;

          throw new TypeError(`loopMarks.restore: a holder outside its type: ${String(unexpected)}`);
        }
      }
    },
  };
}

/** The callbacks of a Promise hook: a Promise is made, and a continuation of it runs. */
export interface PromiseHookCallbacks {
  init(promise: Promise<unknown>): void;
  before(promise: Promise<unknown>): void;
  after(promise: Promise<unknown>): void;
}

/** The culprit method of the runtime: the Promise hooks of Node, or none (Bun). */
export type CulpritMethod =
  | { readonly kind: "promiseHooks"; readonly createHook: (callbacks: PromiseHookCallbacks) => () => void }
  | { readonly kind: "none" };

/** The culprit method of this runtime. */
export function runtimeCulpritMethod(): CulpritMethod {
  // Bun's `node:v8` has no `promiseHooks` member: the namespace read gives `undefined` there, where a named import
  // would fail to link.
  const hooks = "bun" in process.versions ? undefined : v8.promiseHooks;

  if (hooks === undefined) {
    return { kind: "none" };
  }

  return {
    kind: "promiseHooks",
    createHook: (callbacks) => {
      const stop = hooks.createHook(callbacks);

      return () => {
        Reflect.apply(stop, undefined, []);
      };
    },
  };
}

/**
 * Start the culprit method: each Promise that code of a run makes marks that run at its continuations, and the
 * holder before the continuation comes back after it. Continuations nest only as calls nest, so the stack of the
 * holders to put back is as deep as the call stack at most.
 */
export function startCulpritMarks(method: CulpritMethod, marks: LoopMarks): () => void {
  switch (method.kind) {
    case "none":
      return () => undefined;
    case "promiseHooks": {
      const owners = new WeakMap<Promise<unknown>, string>();
      const before: LoopHolder[] = [];

      return method.createHook({
        init: (promise) => {
          const mark = runStore.getStore();

          if (mark !== undefined) {
            owners.set(promise, mark.taskId);
          }
        },
        before: (promise) => {
          const taskId = owners.get(promise);

          if (taskId !== undefined) {
            before.push(marks.hold(taskId));
          }
        },
        after: (promise) => {
          if (owners.has(promise)) {
            marks.restore(before.pop() ?? NO_RUN);
          }
        },
      });
    }

    default: {
      const unexpected: never = method;

      throw new TypeError(`startCulpritMarks: a method outside its type: ${String(unexpected)}`);
    }
  }
}
