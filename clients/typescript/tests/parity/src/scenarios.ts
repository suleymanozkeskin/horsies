// The scenarios of the parity runs: the copy of the TypeScript side.
//
// `crossclient/parity/scenarios.py` holds the same scenarios with the same names, sends, workflows and actions, and
// `clients/rust/tests/parity/src/lib.rs` holds the copy of the Rust side: each side runs its own copy, and the snapshot
// compare shows any difference between the copies too. The runner reads the profiles, the worker start and the
// scheduler of the Python copy. The send process of this side reads the rest from this copy, and the profiles for the
// count of the workers that a ping waits for.
//
// A scenario runs in this order: the sends, the reruns, the workflow starts, the actions on the started workflows, then
// the pings. Each scenario ends the same way on every run: no step depends on the timing of a worker.

import { CHAIN_NAME, ISOLATED, PARENT_NAME, type ProfileName, SHARED_ASYNC, SHARED_THREAD, type WorkflowName } from "./app.ts";

/** The input of the tasks that add two numbers. */
export type PairInput = { readonly a: number; readonly b: number };

/** The input of the tasks that take one number. */
export type ValueInput = { readonly value: number };

/** The input of the tasks that take no parameter. */
export type NoInput = { readonly [name: string]: never };

/** The tasks that take `PairInput`. */
export type PairTask = "parity.add" | "parity.add_async" | "parity.add_thread";

/** The tasks that take `ValueInput`. */
export type ValueTask =
  | "parity.refuse"
  | "parity.transient"
  | "parity.refuse_async"
  | "parity.transient_async"
  | "parity.refuse_thread"
  | "parity.transient_thread";

/** The tasks that take no parameter. */
export type NoInputTask = "parity.sleep_past_deadline" | "parity.async_past_deadline" | "parity.thread_past_deadline";

/** The task of a send and its input. */
export type TaskInput =
  | { readonly task: PairTask; readonly input: PairInput }
  | { readonly task: ValueTask; readonly input: ValueInput }
  | { readonly task: NoInputTask; readonly input: NoInput };

/** The idempotency key of a send: none, or a key. */
export type KeyChoice = { readonly kind: "noKey" } | { readonly kind: "key"; readonly key: string };

const NO_KEY: KeyChoice = { kind: "noKey" };

/**
 * One send, by how it is written:
 * - `direct`: one send through the parity app, with its key.
 * - `retryOfRefusedConnection`: a send of `parity.add` through a second app with the same contracts, whose broker
 *   refuses the connection, ends `ENQUEUE_FAILED` with its prepared payload; then two `retrySend` calls with that error
 *   on the parity app write one task under the prepared id. Python and Rust reach a send refused before any statement
 *   with a blocking send inside a running event loop (`ASYNC_CONTEXT`); the TypeScript send is async only, so it has
 *   no such refusal. The stored rows are the same.
 */
export type Send =
  | ({ readonly form: "direct"; readonly key: KeyChoice } & TaskInput)
  | { readonly form: "retryOfRefusedConnection"; readonly task: "parity.add"; readonly input: PairInput };

/** What an action does to a started workflow. */
export type ActionKind = "pause" | "resume" | "cancel";

/** An action on the started workflow at `index`. A pause and a resume must answer true. */
export type WorkflowAction = { readonly kind: ActionKind; readonly index: number };

/**
 * The rerun of the task of task line `source`: a send, or an earlier rerun. The send process waits for the end of that
 * task, then reruns it with a retention of 30 days, the input kept and a 24 h key reservation. The new task is a task
 * line after the lines of the sends, in the order of the reruns.
 */
export type Rerun = { readonly source: number };

/**
 * Whether the send process pings. `pinged`: after the actions, `pingDatabase`; `pingWorkers` until each worker of the
 * scenario answered, at most `PING_ATTEMPTS` pings of `PING_WINDOW_MS` (`send.ts`); then a ping of the first worker
 * that answered, by its id. The send process prints one ping line with the counts of the answers.
 */
export type Health = "notPinged" | "pinged";

/** One scenario: the parts that the send process of this side reads. */
export interface Scenario {
  readonly name: string;
  /** One worker per profile; the ping waits for this many workers. */
  readonly profiles: readonly ProfileName[];
  readonly sends: readonly Send[];
  readonly reruns: readonly Rerun[];
  readonly workflows: readonly WorkflowName[];
  readonly actions: readonly WorkflowAction[];
  readonly health: Health;
}

/** The fields of a scenario that `scenario` fills when they are left out: no step of that kind. */
type ScenarioFields = Pick<Scenario, "name" | "profiles"> & Partial<Omit<Scenario, "name" | "profiles">>;

/** A scenario with an empty list for each kind of step that `fields` leaves out, and no ping. Pure. */
function scenario(fields: ScenarioFields): Scenario {
  return { sends: [], reruns: [], workflows: [], actions: [], health: "notPinged", ...fields };
}

/** A direct send of `taskInput` with `key`. Pure. */
function direct(taskInput: TaskInput, key: KeyChoice = NO_KEY): Send {
  return { form: "direct", key, ...taskInput };
}

/** The scenarios of `crossclient/parity/scenarios.py`, in its order. */
export const SCENARIOS: readonly Scenario[] = [
  scenario({
    name: "isolated_outcomes",
    profiles: [ISOLATED],
    sends: [
      direct({ task: "parity.add", input: { a: 2, b: 3 } }),
      direct({ task: "parity.refuse", input: { value: 7 } }),
      direct({ task: "parity.transient", input: { value: 1 } }),
    ],
  }),
  scenario({
    name: "shared_async_outcomes",
    profiles: [SHARED_ASYNC],
    sends: [
      direct({ task: "parity.add_async", input: { a: 2, b: 3 } }),
      direct({ task: "parity.refuse_async", input: { value: 7 } }),
      direct({ task: "parity.transient_async", input: { value: 1 } }),
    ],
  }),
  scenario({
    name: "shared_thread_outcomes",
    profiles: [SHARED_THREAD],
    sends: [
      direct({ task: "parity.add_thread", input: { a: 2, b: 3 } }),
      direct({ task: "parity.refuse_thread", input: { value: 7 } }),
      direct({ task: "parity.transient_thread", input: { value: 1 } }),
    ],
  }),
  scenario({
    name: "isolated_timeout",
    profiles: [ISOLATED],
    sends: [direct({ task: "parity.sleep_past_deadline", input: {} })],
  }),
  scenario({
    name: "shared_async_timeout_cancel_grace",
    profiles: [SHARED_ASYNC],
    sends: [direct({ task: "parity.async_past_deadline", input: {} })],
  }),
  scenario({
    name: "shared_thread_timeout",
    profiles: [SHARED_THREAD],
    sends: [direct({ task: "parity.thread_past_deadline", input: {} })],
  }),
  scenario({
    name: "idempotency_key",
    profiles: [ISOLATED],
    sends: [
      direct({ task: "parity.add", input: { a: 1, b: 1 } }, { kind: "key", key: "parity-key" }),
      direct({ task: "parity.add", input: { a: 1, b: 1 } }, { kind: "key", key: "parity-key" }),
    ],
  }),
  scenario({ name: "workflow_chain", profiles: [ISOLATED], workflows: [CHAIN_NAME] }),
  // Layer 2: two workers, and each task has one possible worker. A worker with isolated children also serves a shared
  // class that it has no children for, through its ISOLATED budget; a worker with no isolated children serves only the
  // classes of its own children (the core's `ClaimPlan::route`). So neither worker here serves the class of the other.
  scenario({
    name: "several_workers",
    profiles: [SHARED_ASYNC, SHARED_THREAD],
    sends: [
      direct({ task: "parity.add_async", input: { a: 2, b: 3 } }),
      direct({ task: "parity.add_thread", input: { a: 2, b: 3 } }),
    ],
  }),
  scenario({
    name: "send_retry",
    profiles: [ISOLATED],
    sends: [{ form: "retryOfRefusedConnection", task: "parity.add", input: { a: 4, b: 5 } }],
  }),
  // Layer 6: the first chain is paused and resumed, the second cancelled, before any worker runs.
  scenario({
    name: "workflow_pause_resume_cancel",
    profiles: [ISOLATED],
    workflows: [CHAIN_NAME, CHAIN_NAME],
    actions: [
      { kind: "pause", index: 0 },
      { kind: "resume", index: 0 },
      { kind: "cancel", index: 1 },
    ],
  }),
  // Layer 7: a child workflow whose output a later node of the parent takes.
  scenario({ name: "sub_workflow", profiles: [ISOLATED], workflows: [PARENT_NAME] }),
  // Layer 3: the scheduler process and the move of the schedule are steps of the runner; the send process sends
  // nothing.
  scenario({ name: "schedule_slots", profiles: [ISOLATED] }),
  // Layer 8: the database and the two workers answer.
  scenario({ name: "worker_health", profiles: [ISOLATED, SHARED_ASYNC], health: "pinged" }),
  // A failed task, then its rerun: a new task with the source as its rerun parent and root.
  scenario({
    name: "rerun",
    profiles: [ISOLATED],
    sends: [direct({ task: "parity.refuse", input: { value: 7 } })],
    reruns: [{ source: 0 }],
  }),
];

/** The scenario of a name, or the name that no scenario has. */
export type ScenarioLookup = { readonly kind: "found"; readonly scenario: Scenario } | { readonly kind: "notFound"; readonly name: string };

/** The scenario named `name`. Pure. */
export function scenarioNamed(name: string): ScenarioLookup {
  const found = SCENARIOS.find((candidate) => candidate.name === name);

  return found === undefined ? { kind: "notFound", name } : { kind: "found", scenario: found };
}
