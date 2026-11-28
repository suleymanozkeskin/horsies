// The seat isolate side of a `SHARED_THREAD` child: `cli.run(app)` in a worker thread with the seat marker.
//
// 1. The process events of a child, in the isolate: a detached throw in code of a run (a callback that a body
//    scheduled) is logged with the run, and the run's outcome does not change; a throw outside every run is logged and
//    ends the isolate (the main thread replaces it); a rejection that no body awaits is logged. Each line goes to the
//    main thread (`log`). So an unawaited rejected Promise in a seat body has the outcome that it has in a
//    `SHARED_ASYNC` body.
// 2. The members of the app close (a later `app.provide` is a defect); sends open (task code may send from here on).
// 3. Each provider runs, in registration order, and keeps its value for `runtime.state`. A provider that throws or
//    rejects: `bootFailed` with its name and the record of the throw, and the isolate takes no run; the child ends
//    with 173. The main thread bounds the boot of the isolates (10 s): a provider has no bound of its own here.
// 4. `ready`, then each `run`: the body adapter of the child (decode, the body, the read-back, the error mapping) with
//    a runtime of the run, then `result` with the stored outcome. One run at a time.
// 5. `cancel` of the run that runs: its signal aborts with `{ cause }`. The cancel cell of the seat already holds the
//    cause, so a sync body reads it with `runtime.cancelState()`. A `cancel` of another run is left: the run ended and
//    the cancel crossed it.
// 6. `bodyEnded` of the run whose result went: the seat body observer ends the run's observation with the main
//    thread's verdict and time.
// 7. `flush` at the child's stop: the seat body observer flushes (the tracer provider of the isolate's SDK), then
//    `flushed` goes. A sync body that holds the isolate delays the answer; the exit rule of the child bounds the wait.
//
// The isolate never loads the addon: the runtime reads the cell, the relay sends the core operations, and the run's
// loop marks do nothing (no watch reads the loop of an isolate). The runtime has no `spawn` (`ThreadTaskRuntime`).
// A defect of the package goes to the main thread (the defect rule of a worker process), then the isolate throws.

import type { Horsies } from "../app.ts";
import type { AppContext } from "../app/context.ts";
import { appInternals } from "../app/internals.ts";
import { type ProviderMember, servedMembers } from "../child/app-members.ts";
import { type AttemptNumber, attemptFromCore } from "../child/attempt.ts";
import { type MaxRetries, maxRetriesFromCore } from "../child/retry-budget.ts";
import { runBody, type StoredBodyOutcome } from "../child/body.ts";
import { installProcessEvents, type ProcessEventHooks } from "../child/process-events.ts";
import type { LoopHolder, LoopMarks } from "../child/run-table.ts";
import { currentRun } from "../child/run-store.ts";
import type { RunControls } from "../child/runtime.ts";
import { type StateKey, StateValues } from "../child/state.ts";
import { thrownRecord } from "../child/thrown-error.ts";
import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { compileContract } from "../contract/compile.ts";
import { type TaskId, taskIdFromCore } from "../contract/ids.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { ClientDefect, CoreDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { bodyObservationOf, type SeatBodyObservation, seatBodyObserverOf } from "./body-observer.ts";
import { cancelStateOf } from "./cancel-cells.ts";
import { type SeatChannel, type SeatRunHandler, threadChannel } from "./channel.ts";
import { type MainToSeat, SEAT_PROTOCOL_VERSION, type SeatOutcome } from "./protocol.ts";

/** What a seat isolate does that has effects beyond its channel. */
export interface SeatIsolateDeps {
  /** The process events of the isolate's thread. */
  readonly events: ProcessEventHooks;
  /** End this isolate with `code` (`process.exit` in a worker thread ends that thread only). */
  readonly endIsolate: (code: number) => void;
}

/** The dependencies of a seat isolate in this thread. A worker thread gets no signals: its signal hook does nothing. */
export function processSeatIsolateDeps(): SeatIsolateDeps {
  return {
    events: {
      onSignal: () => undefined,
      onUncaught: (listener) => {
        process.on("uncaughtException", listener);
      },
      onRejection: (listener) => {
        process.on("unhandledRejection", listener);
      },
    },
    endIsolate: (code) => {
      process.exit(code);
    },
  };
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
        return raiseDefect(new ClientDefect({ subject: "cli.run", rule: "the app of a seat isolate is created in its isolate" }));
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

/** Steps 1 to 4 for `context` on `channel`. Never settles. */
export async function serveSeat(context: AppContext, channel: SeatChannel, deps: SeatIsolateDeps): Promise<never> {
  installProcessEvents({
    hooks: deps.events,
    writeLine: (line) => {
      channel.post({ kind: "log", protocol: SEAT_PROTOCOL_VERSION, line });
    },
    end: deps.endIsolate,
    currentRun,
  });

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
      channel.serve(new SeatRuns(context, channel, states));
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

/** The run that a seat runs now, with the controller of its signal and the observation of its body. */
type CurrentRun =
  | { readonly kind: "idle" }
  | {
      readonly kind: "running";
      readonly runKey: bigint;
      readonly controller: AbortController;
      readonly observation: SeatBodyObservation;
    };

/** The run whose result went and whose verdict (`bodyEnded`) has not come yet. */
type EndingRun = { readonly kind: "none" } | { readonly kind: "ending"; readonly runKey: bigint; readonly observation: SeatBodyObservation };

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
  #current: CurrentRun = { kind: "idle" };
  #ending: EndingRun = { kind: "none" };

  constructor(context: AppContext, channel: SeatChannel, states: StateValues) {
    this.#context = context;
    this.#channel = channel;
    this.#states = states;
  }

  /** Start the run of `message`; its `result` goes when the body ends. A second run at once is a defect. */
  run(message: Extract<MainToSeat, { readonly kind: "run" }>): void {
    if (this.#current.kind === "running" || this.#ending.kind === "ending") {
      this.#defect(new ClientDefect({ subject: "seat isolate", rule: "the main thread posted a run to a seat whose run has not ended" }));
    }

    const implementation = this.#context.implementations.get(message.taskName);

    if (implementation === undefined) {
      this.#defect(new ClientDefect({ subject: "seat run", rule: `the core posted the task ${message.taskName}, which has no implementation` }));
    }

    const compiled = compileContract(implementation.contract);

    if (!compiled.ok) {
      this.#defect(new ClientDefect({ subject: "seat run", rule: `the contract of ${message.taskName} does not compile in the isolate` }));
    }

    const taskId = taskIdFromCore(message.taskId, "seat run");
    const attempt = attemptFromCore(message.attempt, "seat run");
    const maxRetries = maxRetriesFromCore(message.maxRetries, "seat run");
    const controller = new AbortController();
    const observation = seatBodyObserverOf(this.#context).enter(message.facts);

    this.#current = { kind: "running", runKey: message.runKey, controller, observation };

    void runBody({
      start: {
        kind: "posted",
        // The core's child host reads each run input with its strict read rule (`build_task_input`) before it calls
        // the body; the main thread passes the text of the core's post to the isolate as it came, with the workflow
        // context member of a task that takes it.
        inputJson: jsonTextCheckedByCore(message.inputJson),
        controls: this.#controls(taskId, attempt, maxRetries, controller),
      },
      implementation,
      compiled: compiled.value,
      mark: { taskId, taskName: message.taskName, attempt },
      marks: NO_LOOP_MARKS,
      observation: bodyObservationOf(observation),
    }).then(
      (outcome) => {
        this.#ended(message.runKey, observation, outcome);
      },
      (cause: unknown) => {
        // The body adapter never rejects: a rejection is a defect of the package (for example a seat body observer
        // that threw). The main thread aborts the child; a throw here would only be a rejection that no one awaits.
        this.#channel.reportDefect(cause instanceof CoreDefect ? cause : runDefect(cause));
      },
    );
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

  /** The verdict of the run whose result went: the seat body observer ends its observation. Another run is a defect. */
  bodyEnded(message: Extract<MainToSeat, { readonly kind: "bodyEnded" }>): void {
    const ending = this.#ending;

    if (ending.kind !== "ending" || ending.runKey !== message.runKey) {
      this.#defect(new ClientDefect({ subject: "seat isolate", rule: `the main thread ended the run ${String(message.runKey)}, whose result did not go` }));
    }

    this.#ending = { kind: "none" };
    ending.observation.end(message.end, Temporal.Instant.fromEpochNanoseconds(message.endedAtEpochNanoseconds));
  }

  /** Flush the seat body observer, then answer `flushed`. The observer's flush never rejects: a rejection is a defect. */
  flush(): void {
    void seatBodyObserverOf(this.#context)
      .flush()
      .then(
        () => {
          this.#channel.post({ kind: "flushed", protocol: SEAT_PROTOCOL_VERSION });
        },
        (cause: unknown) => {
          this.#channel.reportDefect(flushDefect(cause));
        },
      );
  }

  /** The defect rule of the isolate: the defect goes to the main thread (the child aborts), then the throw. */
  #defect(defect: CoreDefect): never {
    this.#channel.reportDefect(defect);

    throw defect;
  }

  /** The body of the run `runKey` ended with `outcome`: its result goes, and its verdict is awaited. */
  #ended(runKey: bigint, observation: SeatBodyObservation, outcome: StoredBodyOutcome): void {
    this.#current = { kind: "idle" };
    this.#ending = { kind: "ending", runKey, observation };
    this.#channel.post({ kind: "result", protocol: SEAT_PROTOCOL_VERSION, runKey, outcome: seatOutcomeOf(outcome) });
  }

  #controls(taskId: TaskId, attempt: AttemptNumber, maxRetries: MaxRetries, controller: AbortController): RunControls {
    const marker = this.#channel.marker;

    return Object.freeze({
      taskId,
      attempt,
      maxRetries,
      signal: controller.signal,
      cancelState: () => cancelStateOf(marker.cancelCells, marker.seatIndex),
      spawn: (): never => this.#defect(new ClientDefect({ subject: "runtime.spawn", rule: "the runtime of a SHARED_THREAD body has no spawn" })),
      state: <T>(key: StateKey<T>): T => this.#states.value(key),
    });
  }
}

/** A flush of the seat body observer that rejected: a defect of the package. */
function flushDefect(cause: unknown): ClientDefect {
  const facts = thrownFacts(cause);

  return new ClientDefect({ subject: "seat flush", rule: `the flush of the seat body observer failed with ${facts.name}: ${facts.message}` });
}

/** A rejection of the body adapter that is not a defect class: a defect of the package. */
function runDefect(cause: unknown): ClientDefect {
  const facts = thrownFacts(cause);

  return new ClientDefect({ subject: "seat run", rule: `the body adapter rejected with ${facts.name}: ${facts.message}` });
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
