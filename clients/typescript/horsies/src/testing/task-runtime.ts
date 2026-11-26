// The run controls of `TaskRuntime.forTest`: a runtime for a test of a task body, with no worker, no database and no
// addon.
//
// - `taskId` and the attempt are the test's; `fencingToken` is `{ taskId, value: attempt }`.
// - `signal` never fires, and `cancelState()` is `notRequested`.
// - `spawn` runs the work at once with a signal of its own that never fires; it is never refused.
// - `state(key)` gives the value of the test's entry for `key`. Each entry pairs a key that `app.provide` returned
//   with a value of the key's type (typecheck); a key with no entry is a defect: `ClientDefect`.

import type { TaskId } from "../contract/ids.ts";
import type { AttemptNumber } from "../child/attempt.ts";
import type { RunControls } from "../child/runtime.ts";
import { NOT_REQUESTED } from "../child/runtime.ts";
import { type AnyStateKey, type StateKey, type StateValueOf, StateValues } from "../child/state.ts";
import { ok } from "../wire/result.ts";

/** One state entry of a test runtime: a key and a value of the key's type. */
export type TestStateEntry<K extends AnyStateKey> = readonly [key: K, value: StateValueOf<K>];

/** The state entries of a test runtime: entry `I` pairs key `I` with a value of that key's type. */
export type TestStateEntries<Keys extends readonly AnyStateKey[]> = { readonly [I in keyof Keys]: TestStateEntry<Keys[I]> };

/** The options of `TaskRuntime.forTest`. */
export interface TestRuntimeOptions<Keys extends readonly AnyStateKey[]> {
  readonly taskId: TaskId;
  readonly attempt: AttemptNumber;
  /** The app state of the test: `[[Db, fakePool]]`. */
  readonly state?: TestStateEntries<Keys>;
}

/** The run controls of a test runtime. */
export function testRunControls<Keys extends readonly AnyStateKey[]>(options: TestRuntimeOptions<Keys>): RunControls {
  const entries: readonly (readonly [AnyStateKey, StateValueOf<AnyStateKey>])[] = options.state ?? [];
  const states = new StateValues(entries.map(([key]) => key));

  for (const [key, value] of entries) {
    states.keep(key, value);
  }

  const signal = new AbortController().signal;

  return Object.freeze({
    taskId: options.taskId,
    attempt: options.attempt,
    signal,
    cancelState: () => NOT_REQUESTED,
    spawn: <T>(_name: string, work: (signal: AbortSignal) => Promise<T>) => ok(startedTestWork(work)),
    state: <T>(key: StateKey<T>): T => states.value(key),
  });
}

/** Start test work with a signal of its own; a sync throw of `work` is a rejected Promise, as in a child. */
function startedTestWork<T>(work: (signal: AbortSignal) => Promise<T>): Promise<T> {
  try {
    return Promise.resolve(work(new AbortController().signal));
  } catch (cause) {
    return Promise.reject(cause);
  }
}
