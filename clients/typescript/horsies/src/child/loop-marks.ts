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

import type { LoopMarks } from "./run-table.ts";
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

/** The loop marks of the body adapter and of `runtime.spawn`, on the addon. */
export function loopMarksOf(native: LoopNative): LoopMarks {
  return {
    hold: (taskId) => {
      native.loopHold(taskId);
    },
    release: () => {
      native.loopRelease();
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

/** Start the culprit method: each Promise that code of a run makes marks that run at its continuations. */
export function startCulpritMarks(method: CulpritMethod, native: LoopNative): () => void {
  switch (method.kind) {
    case "none":
      return () => undefined;
    case "promiseHooks": {
      const owners = new WeakMap<Promise<unknown>, string>();

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
            native.loopHold(taskId);
          }
        },
        after: (promise) => {
          if (owners.has(promise)) {
            native.loopRelease();
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
