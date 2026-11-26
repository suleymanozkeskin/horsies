// The seat isolate side of a `SHARED_THREAD` child: `cli.run(app)` in a worker thread with the seat marker.
//
// 1. The members of the app close (a later `app.provide` is a defect); sends open (task code may send from here on).
// 2. Each provider runs, in registration order, and keeps its value for `runtime.state`. A provider that throws or
//    rejects: `bootFailed` with its name and the record of the throw, and the isolate takes no run; the child ends
//    with 173. The main thread bounds the boot of the isolates (10 s): a provider has no bound of its own here.
// 3. `ready`, then each `run`: the body adapter of the child (decode, the body, the read-back, the error mapping) with
//    a runtime of the run, then `result` with the stored outcome. One run at a time.
// 4. `cancel` of the run that runs: its signal aborts with `{ cause }`. The cancel cell of the seat already holds the
//    cause, so a sync body reads it with `runtime.cancelState()`. A `cancel` of another run is left: the run ended and
//    the cancel crossed it.
//
// The isolate never loads the addon: the runtime reads the cell, the relay sends the core operations, and the run's
// loop marks do nothing (no watch reads the loop of an isolate). The runtime has no `spawn` (`ThreadTaskRuntime`).

import type { Horsies } from "../app.ts";
import type { AppContext } from "../app/context.ts";
import { appInternals } from "../app/internals.ts";
import { type ProviderMember, servedMembers } from "../child/app-members.ts";
import { type AttemptNumber, attemptFromCore } from "../child/attempt.ts";
import type { BodyObservation } from "../child/body-observer.ts";
import { runBody, type StoredBodyOutcome } from "../child/body.ts";
import type { LoopHolder, LoopMarks } from "../child/run-table.ts";
import { type RunControls, taskRuntimeOf } from "../child/runtime.ts";
import { type StateKey, StateValues } from "../child/state.ts";
import { thrownRecord } from "../child/thrown-error.ts";
import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { compileContract } from "../contract/compile.ts";
import { type TaskId, taskIdFromCore } from "../contract/ids.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { bodyObservationOf, seatBodyObserverOf } from "./body-observer.ts";
import { cancelStateOf } from "./cancel-cells.ts";
import { type SeatChannel, type SeatRunHandler, threadChannel } from "./channel.ts";
import { type MainToSeat, SEAT_PROTOCOL_VERSION, type SeatOutcome } from "./protocol.ts";

/** What a seat isolate reads that has effects. */
export interface SeatIsolateDeps {
  /** The time now, for the `endedAt` of the seat body observer. */
  readonly now: () => Temporal.Instant;
}

/** The dependencies of a seat isolate in this thread. */
export function processSeatIsolateDeps(): SeatIsolateDeps {
  return { now: () => Temporal.Now.instant() };
}

/**
 * Serve `app` as the seat isolate of this thread. It never settles: the isolate serves until its process ends. An app
 * that this thread did not create, or a thread with no seat marker, is a defect: `ClientDefect`.
 */
export async function runSeatIsolate(app: Horsies, deps: SeatIsolateDeps): Promise<never> {
  const context = appInternals(app);
  const thread = threadChannel();

  switch (thread.kind) {
    case "notSeat":
      throw new ClientDefect({ subject: "cli.run", rule: "a seat isolate is a worker thread with the seat marker" });
    case "seat":
      if (context.core.kind !== "seatRelay" || context.core.relay !== thread.channel.relay) {
        throw new ClientDefect({ subject: "cli.run", rule: "the app of a seat isolate is created in its isolate" });
      }

      return serveSeat(context, thread.channel, deps);
    default:
      return unreachable(thread, "runSeatIsolate");
  }
}

/** How the providers of an isolate ended. */
type ProvidersEnd =
  | { readonly kind: "built" }
  | { readonly kind: "failed"; readonly hookName: string; readonly cause: string };

/** Steps 1 to 3 for `context` on `channel`. Never settles. */
export async function serveSeat(context: AppContext, channel: SeatChannel, deps: SeatIsolateDeps): Promise<never> {
  const members = servedMembers(context);
  const states = new StateValues(members.providers.map((provider) => provider.key));

  context.suppression.startServing();

  const built = await builtProviders(members.providers, states);
  const seatIndex = channel.marker.seatIndex;

  switch (built.kind) {
    case "failed":
      channel.post({ kind: "bootFailed", protocol: SEAT_PROTOCOL_VERSION, seatIndex, hookName: built.hookName, cause: built.cause });
      break;
    case "built":
      channel.serve(new SeatRuns(context, channel, states, deps));
      channel.post({ kind: "ready", protocol: SEAT_PROTOCOL_VERSION, seatIndex });
      break;
    default:
      unreachable(built, "serveSeat");
  }

  return new Promise<never>(() => undefined);
}

/** Run each provider in order and keep its value; the first throw or rejection ends the boot. */
async function builtProviders(providers: readonly ProviderMember[], states: StateValues): Promise<ProvidersEnd> {
  for (const provider of providers) {
    try {
      states.keep(provider.key, await provider.factory());
    } catch (cause) {
      const record = thrownRecord(cause);

      return { kind: "failed", hookName: provider.name, cause: `${record.type}: ${record.message}` };
    }
  }

  return { kind: "built" };
}

/** The run that a seat runs now, with the controller of its signal. */
type CurrentRun =
  | { readonly kind: "idle" }
  | {
      readonly kind: "running";
      readonly runKey: bigint;
      readonly controller: AbortController;
      readonly observation: BodyObservation;
    };

const NO_RUN: LoopHolder = Object.freeze({ kind: "noRun" });

/** The loop marks of an isolate: no watch reads the loop of an isolate, so they mark nothing. */
const NO_LOOP_MARKS: LoopMarks = Object.freeze({
  hold: (): LoopHolder => NO_RUN,
  restore: (): void => undefined,
});

/** The runs of one seat: at most one at a time. */
class SeatRuns implements SeatRunHandler {
  readonly #context: AppContext;
  readonly #channel: SeatChannel;
  readonly #states: StateValues;
  readonly #deps: SeatIsolateDeps;
  #current: CurrentRun = { kind: "idle" };

  constructor(context: AppContext, channel: SeatChannel, states: StateValues, deps: SeatIsolateDeps) {
    this.#context = context;
    this.#channel = channel;
    this.#states = states;
    this.#deps = deps;
  }

  /** Start the run of `message`; its `result` goes when the body ends. A second run at once is a defect. */
  run(message: Extract<MainToSeat, { readonly kind: "run" }>): void {
    if (this.#current.kind === "running") {
      throw new ClientDefect({ subject: "seat isolate", rule: "the main thread posted a run to a seat that runs one" });
    }

    const implementation = this.#context.implementations.get(message.taskName);

    if (implementation === undefined) {
      throw new ClientDefect({ subject: "seat run", rule: `the core posted the task ${message.taskName}, which has no implementation` });
    }

    const compiled = compileContract(implementation.contract);

    if (!compiled.ok) {
      throw new ClientDefect({ subject: "seat run", rule: `the contract of ${message.taskName} does not compile in the isolate` });
    }

    const taskId = taskIdFromCore(message.taskId, "seat run");
    const attempt = attemptFromCore(message.attempt, "seat run");
    const controller = new AbortController();
    const observation = bodyObservationOf(seatBodyObserverOf(this.#context).enter(message.facts), this.#deps.now);

    this.#current = { kind: "running", runKey: message.runKey, controller, observation };

    void runBody({
      // The core's child host reads each run input with its strict read rule (`build_task_input`) before it calls the
      // body; the main thread passes the text of the core's post to the isolate as it came.
      inputJson: jsonTextCheckedByCore(message.inputJson),
      implementation,
      compiled: compiled.value,
      runtime: taskRuntimeOf(this.#controls(taskId, attempt, controller)),
      mark: { taskId, taskName: message.taskName, attempt },
      marks: NO_LOOP_MARKS,
      observation,
    }).then((outcome) => {
      this.#current = { kind: "idle" };
      this.#channel.post({ kind: "result", protocol: SEAT_PROTOCOL_VERSION, runKey: message.runKey, outcome: seatOutcomeOf(outcome) });
    });
  }

  /** The cancel signal of the run `runKey` fired: abort its signal. A cancel of another run is left. */
  cancel(message: Extract<MainToSeat, { readonly kind: "cancel" }>): void {
    const current = this.#current;

    switch (current.kind) {
      case "idle":
        return;
      case "running":
        if (current.runKey === message.runKey && !current.controller.signal.aborted) {
          current.controller.abort(Object.freeze({ cause: message.cause }));
          current.observation.cancelled(message.cause);
        }

        return;
      default:
        unreachable(current, "SeatRuns.cancel");
    }
  }

  #controls(taskId: TaskId, attempt: AttemptNumber, controller: AbortController): RunControls {
    const marker = this.#channel.marker;

    return Object.freeze({
      taskId,
      attempt,
      signal: controller.signal,
      cancelState: () => cancelStateOf(marker.cancelCells, marker.seatIndex),
      spawn: (): never => {
        throw new ClientDefect({ subject: "runtime.spawn", rule: "the runtime of a SHARED_THREAD body has no spawn" });
      },
      state: <T>(key: StateKey<T>): T => this.#states.value(key),
    });
  }
}

/** The outcome that the isolate gives the main thread: the stored text of the value, or the parts of the error. Pure. */
function seatOutcomeOf(outcome: StoredBodyOutcome): SeatOutcome {
  switch (outcome.kind) {
    case "value":
      return { kind: "value", json: outcome.json };
    case "error":
      return { kind: "error", parts: outcome.parts };
    default:
      return unreachable(outcome, "seatOutcomeOf");
  }
}
