// The run controls of `TaskRuntime.forTest`: a runtime for a test of a task body, with no worker, no database and no
// addon.
//
// - `taskId` and the attempt are the test's; `fencingToken` is `{ taskId, value: attempt }`.
// - `signal` never fires, and `cancelState()` is `notRequested`.
// - `spawn` runs the work at once with a signal of its own that never fires; it is never refused.
// - `state(key)` gives the value of the test's entry for `key`. Each entry pairs a key that `app.provide` returned
//   with a value of the key's type (typecheck); a key with no entry is a defect: `ClientDefect`.
// - `workflowContext` is the context of the test's stored form, which the codec writer writes (strict JSON only) and
//   the context reader reads; `noContext` when the test gives none. A form that the writer or the reader refuses is a
//   mistake of the test: `TypeError`, with the member and the rule.

import { MAX_JSON_DEPTH } from "../codec/bounds.ts";
import { writtenJsonText } from "../codec/json-text.ts";
import { engineKeysKeptText, type JsonValue } from "../codec/json-value.ts";
import { refusalText, ROOT_PATH } from "../codec/refusal.ts";
import type { TaskId } from "../contract/ids.ts";
import type { AttemptNumber } from "../child/attempt.ts";
import type { RunControls } from "../child/runtime.ts";
import { NOT_REQUESTED } from "../child/runtime.ts";
import { type AnyStateKey, type StateKey, type StateValueOf, StateValues } from "../child/state.ts";
import { ok } from "../wire/result.ts";
import { NO_WORKFLOW_CONTEXT, type RuntimeWorkflowContext, workflowContextOf } from "../workflow/context.ts";

/** One state entry of a test runtime: a key and a value of the key's type. */
export type TestStateEntry<K extends AnyStateKey> = readonly [key: K, value: StateValueOf<K>];

/** The state entries of a test runtime: entry `I` pairs key `I` with a value of that key's type. */
export type TestStateEntries<Keys extends readonly AnyStateKey[]> = { readonly [I in keyof Keys]: TestStateEntry<Keys[I]> };

/**
 * The stored form of a workflow context: the `workflow_ctx` member that the core writes into the input of a task with
 * `workflowContext: true`.
 */
export interface StoredWorkflowContext {
  /** The workflow id, in the lower-case 8-4-4-4-12 form. */
  readonly workflow_id: string;
  /** The index of the task's node in its workflow: 0 or more. */
  readonly task_index: number;
  /** The task name of the node. */
  readonly task_name: string;
  /** The stored result of each context node, by node id: `{ __h_task_result__: true, ok: <value>, err: null }`. */
  readonly results_by_id?: { readonly [nodeId: string]: JsonValue };
  /** The stored summary of each context sub-workflow node, by node id. */
  readonly summaries_by_id?: { readonly [nodeId: string]: JsonValue };
}

/** The options of `TaskRuntime.forTest`. */
export interface TestRuntimeOptions<Keys extends readonly AnyStateKey[]> {
  readonly taskId: TaskId;
  readonly attempt: AttemptNumber;
  /** The app state of the test: `[[Db, fakePool]]`. */
  readonly state?: TestStateEntries<Keys>;
  /** The workflow context of the run, in its stored form. Left out: `noContext`, as a run outside a workflow. */
  readonly workflowContext?: StoredWorkflowContext;
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

/**
 * The workflow context of a test runtime: the context of `stored`, or `noContext` when the test gives none. A form
 * that the codec writer or the context reader refuses throws `TypeError` (a mistake of the test). Pure.
 */
export function testWorkflowContext(stored: StoredWorkflowContext | undefined): RuntimeWorkflowContext {
  if (stored === undefined) {
    return NO_WORKFLOW_CONTEXT;
  }

  const written = engineKeysKeptText(stored, ROOT_PATH, MAX_JSON_DEPTH);

  if (!written.ok) {
    throw new TypeError(`horsies: TaskRuntime.forTest: the workflowContext is not storable: ${refusalText(written.error)}`);
  }

  const read = workflowContextOf(writtenJsonText(written.value));

  if (!read.ok) {
    throw new TypeError(`horsies: TaskRuntime.forTest: workflowContext.${read.error.member}: ${read.error.detail}`);
  }

  return Object.freeze({ kind: "context", context: read.value });
}

/** Start test work with a signal of its own; a sync throw of `work` is a rejected Promise, as in a child. */
function startedTestWork<T>(work: (signal: AbortSignal) => Promise<T>): Promise<T> {
  try {
    return Promise.resolve(work(new AbortController().signal));
  } catch (cause) {
    return Promise.reject(cause);
  }
}
