// The OpenTelemetry body observer of a child: one span of kind CONSUMER, `run <task_name>`, around each task body,
// active where the body runs, so a span that user code makes in the body nests under it, also after an `await`.
//
// - The parent comes from the carrier that the send stored, by the parentage: `childOfSend` gives the extracted
//   context as the parent; `linkedToSend` gives a root span with a link to the extracted span, in the extracted context with no span; a task with no
//   carrier gets a root span in both.
// - The body runs in `context.with` of a context that the package builds from the root context: a function that the
//   addon calls has an empty async context, so the package never takes the context of the bridge callback.
// - A thrown value is recorded on the span. The end sets the status: unset for a returned value (as the Python
//   client); ERROR with `error.type` for an error code, `TASK_TIMEOUT` (the run's deadline) and `TASK_CANCELLED` (an
//   operator cancel).
// - The end is the first of the body's end and, in a `SHARED_ASYNC` child, the core's decision at the run's cancel
//   signal: the core ends such a run at its deadline or at an operator cancel while the body may run on, so a body
//   that never settles still gets its span.
// - A body that does not start gets no span (the core posts no body for it).
// - A throw of the user's SDK never changes a run: no span at the open. A throw at any stage (the open, the record of
//   a thrown value, the end) is a span not made: one WARNING record per process on `horsies.telemetry`
//   (`not-made.ts`).
// - The seat isolates of a `SHARED_THREAD` child have their own observer (`seat-span.ts`), which opens, runs and ends
//   the same span with the functions of this module.

import { OutcomeCode } from "../_catalog/task_codes.ts";
import { thrownRecord } from "../child/thrown-error.ts";
import type { BodyEnd, BodyObservation, BodyObserver } from "../child/body-observer.ts";
import type {
  BodyCarrierRecord,
  BodyExecution,
  BodyFactsRecord,
  BodyWorkflowRecord,
  CancelCauseName,
} from "../native/child/addon.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  MESSAGING_DESTINATION_NAME_KEY,
  MESSAGING_MESSAGE_ID_KEY,
  MESSAGING_OPERATION_NAME_KEY,
  MESSAGING_OPERATION_TYPE_KEY,
  MESSAGING_SYSTEM,
  MESSAGING_SYSTEM_KEY,
  PROCESS_OPERATION,
  QUEUE_KEY,
  RUN_OPERATION,
  TASK_ATTEMPT_KEY,
  TASK_EXECUTION_KEY,
  TASK_ID_KEY,
  TASK_NAME_KEY,
  WORKER_ID_KEY,
  WORKFLOW_ID_KEY,
  WORKFLOW_NODE_KEY,
  WORKFLOW_ROOT_ID_KEY,
} from "./attributes.ts";
import { asciiLowerCase, extractedContextOf } from "./carrier.ts";
import { flushTracerProvider } from "./flush.ts";
import type { NoSpanIds, SpanIds } from "./log-context.ts";
import {
  INSTRUMENTATION_SCOPE,
  type OtelApi,
  type OtelAttributes,
  type OtelContext,
  type OtelHighResolutionTime,
  type OtelLink,
  type OtelSpan,
  type OtelSpanOptions,
} from "./otel-api.ts";
import { markError } from "./send-span.ts";
import { attemptSpanStore } from "./span-id-store.ts";
import type { AttemptParentage } from "./tracing-config.ts";
import type { SpansNotMade } from "./not-made.ts";
import type { TelemetryWarnings } from "./warnings.ts";

/** What the attempt-span observer reads. `warnings` takes a flush that fails; `notMade` a span not made. */
export type AttemptSpanDeps = {
  readonly api: OtelApi;
  readonly parentage: AttemptParentage;
  readonly warnings: TelemetryWarnings;
  readonly notMade: SpansNotMade;
};

/** The facts of a run that has its facts recorded. */
export type RecordedFacts = Extract<BodyFactsRecord, { readonly kind: "recorded" }>;

/** The time of the start or the end of an attempt span: the SDK's clock at the call, or a time that the caller gives. */
export type SpanTime = { readonly kind: "sdkClock" } | { readonly kind: "at"; readonly time: OtelHighResolutionTime };

/** The SDK's clock at the call. */
export const SDK_CLOCK: SpanTime = Object.freeze({ kind: "sdkClock" });

/** Where the attempt span takes its parent from. */
export type SpanPlace = { readonly parent: OtelContext; readonly links: OtelLink[] };

/** The facts of a run that every attempt span carries. `execution` is the class of the child that runs it. */
export type RunSpanFacts = {
  readonly taskId: string;
  readonly taskName: string;
  readonly queueName: string;
  readonly attempt: number;
  readonly workerId: string;
  readonly execution: BodyExecution;
};

const NO_SPAN_IDS: NoSpanIds = Object.freeze({ kind: "noSpanIds" });

const NO_OBSERVATION: BodyObservation = Object.freeze({
  run: <T>(call: () => T): T => call(),
  thrown: (): void => undefined,
  end: (): void => undefined,
  cancelled: (): void => undefined,
});

/** The body observer of a child of an app with OpenTelemetry tracing. */
export function otelBodyObserver(deps: AttemptSpanDeps): BodyObserver {
  return Object.freeze({
    enter: (facts: BodyFactsRecord): BodyObservation => {
      switch (facts.kind) {
        case "notRecorded":
          return NO_OBSERVATION;
        case "recorded":
          return openedObservation(deps, facts);
        default:
          return unreachable(facts, "otelBodyObserver.enter");
      }
    },
    flush: async (): Promise<void> => flushTracerProvider(deps),
  });
}

/** The observation of one body: its span, or none when the user's SDK threw at the open. */
function openedObservation(deps: AttemptSpanDeps, facts: RecordedFacts): BodyObservation {
  const notMade = (cause: unknown): void => {
    deps.notMade.report({ kind: "attempt", taskId: facts.taskId }, cause);
  };

  const opened = openedAttemptSpan(deps, facts, SDK_CLOCK, {}, notMade);

  switch (opened.kind) {
    case "notOpen":
      return NO_OBSERVATION;
    case "open":
      return new AttemptSpan(deps, opened.open, notMade);
    default:
      return unreachable(opened, "openedObservation");
  }
}

/** The span of one body and where it runs. */
export type OpenSpan = {
  readonly span: OtelSpan;
  readonly active: OtelContext;
  readonly ids: SpanIds | NoSpanIds;
  readonly execution: BodyExecution;
};

/** The attempt span of one body, or none when the user's SDK threw at the open. */
export type OpenedAttemptSpan = { readonly kind: "open"; readonly open: OpenSpan } | { readonly kind: "notOpen" };

/**
 * Open the attempt span of `facts` from `start`: `run <task_name>`, CONSUMER, the parent and the links by the
 * parentage, the attributes of the run and of its workflow node, and `extraAttributes`. A throw of the user's SDK goes to `openFailed` and
 * gives `notOpen`.
 */
export function openedAttemptSpan(
  deps: AttemptSpanDeps,
  facts: RecordedFacts,
  start: SpanTime,
  extraAttributes: OtelAttributes,
  openFailed: (cause: unknown) => void,
): OpenedAttemptSpan {
  const { api } = deps;

  try {
    const place = spanPlace(api, deps.parentage, facts.carrier);
    const attributes = { ...runAttributes(facts), ...workflowAttributes(facts.workflow), ...extraAttributes };
    const options = { kind: api.SpanKind.CONSUMER, attributes, links: place.links };

    const span = api.trace
      .getTracer(INSTRUMENTATION_SCOPE)
      .startSpan(`${RUN_OPERATION} ${facts.taskName}`, withTime(options, start), place.parent);

    const active = api.trace.setSpan(place.parent, span);

    return { kind: "open", open: { span, active, ids: spanIdsOf(api, span), execution: facts.execution } };
  } catch (cause) {
    openFailed(cause);

    return { kind: "notOpen" };
  }
}

/** `options` with the start time `start`: none for the SDK's clock. Pure. */
function withTime(options: OtelSpanOptions, start: SpanTime): OtelSpanOptions {
  switch (start.kind) {
    case "sdkClock":
      return options;
    case "at":
      return { ...options, startTime: start.time };
    default:
      return unreachable(start, "withTime");
  }
}

/**
 * The observation of one body with its span, open until its first end: the end of the body, or, in a `SHARED_ASYNC`
 * child, the core's decision at the run's cancel signal. A `thrown` or an `end` after the first end does nothing.
 */
class AttemptSpan implements BodyObservation {
  readonly #deps: AttemptSpanDeps;
  readonly #open: OpenSpan;
  readonly #notMade: (cause: unknown) => void;
  #state: "open" | "ended" = "open";

  constructor(deps: AttemptSpanDeps, open: OpenSpan, notMade: (cause: unknown) => void) {
    this.#deps = deps;
    this.#open = open;
    this.#notMade = notMade;
  }

  run<T>(call: () => T): T {
    return runInSpan(this.#deps.api, this.#open.active, this.#open.ids, call);
  }

  thrown(cause: unknown): void {
    if (this.#state === "open") {
      guarded(() => {
        recordThrown(this.#open.span, cause);
      }, this.#notMade);
    }
  }

  end(end: BodyEnd): void {
    this.#close(end);
  }

  /**
   * The run's cancel signal fired. In a `SHARED_ASYNC` child the core then ends the run (`TASK_TIMEOUT` at the
   * deadline, `TASK_CANCELLED` at an operator cancel) while the body may run on, so the span ends now. In an
   * `ISOLATED` child the span ends with the body: the body returns after the signal, or the worker kills the child and
   * the handler child makes the span of that end. `SHARED_THREAD` bodies run in seat isolates, which have their own
   * observer.
   */
  cancelled(cause: CancelCauseName): void {
    switch (this.#open.execution) {
      case "SHARED_ASYNC":
        this.#close(endAtSignal(cause));

        return;
      case "ISOLATED":
      case "SHARED_THREAD":
        return;
      default:
        unreachable(this.#open.execution, "AttemptSpan.cancelled");
    }
  }

  #close(end: BodyEnd): void {
    if (this.#state === "ended") {
      return;
    }

    this.#state = "ended";
    guarded(() => {
      endAttemptSpan(this.#deps.api, this.#open.span, end, SDK_CLOCK);
    }, this.#notMade);
  }
}

/** The end that the core decides at the run's cancel signal of `cause`. Pure. */
function endAtSignal(cause: CancelCauseName): BodyEnd {
  switch (cause) {
    case "DEADLINE":
      return { kind: "timedOut" };
    case "OPERATOR":
      return { kind: "cancelled" };
    default:
      return unreachable(cause, "endAtSignal");
  }
}

/** Run `call` with the attempt span active, and its ids in the store of `currentTask()`. */
export function runInSpan<T>(api: OtelApi, active: OtelContext, ids: SpanIds | NoSpanIds, call: () => T): T {
  const inContext = (): T => api.context.with(active, call);

  switch (ids.kind) {
    case "spanIds":
      return attemptSpanStore.run(ids, inContext);
    case "noSpanIds":
      return inContext();
    default:
      return unreachable(ids, "runInSpan");
  }
}

/** The parent and the links of an attempt span whose task has the stored carrier `carrier`. */
export function spanPlace(api: OtelApi, parentage: AttemptParentage, carrier: BodyCarrierRecord): SpanPlace {
  switch (carrier.kind) {
    case "noCarrier":
      return { parent: api.ROOT_CONTEXT, links: [] };
    case "carrier": {
      const entries = new Map(carrier.entries.map((entry) => [entry.key, entry.value] as const));

      return placeByParentage(api, parentage, extractedContextOf(api, entries));
    }

    default:
      return unreachable(carrier, "spanPlace");
  }
}

function placeByParentage(api: OtelApi, parentage: AttemptParentage, extracted: OtelContext): SpanPlace {
  switch (parentage) {
    case "childOfSend":
      return { parent: extracted, links: [] };
    case "linkedToSend": {
      const sendSpan = api.trace.getSpan(extracted);
      const links = sendSpan === undefined ? [] : [{ context: sendSpan.spanContext() }];

      // The extracted context with no span: the new trace keeps the other entries of the carrier (the baggage).
      return { parent: api.trace.deleteSpan(extracted), links };
    }

    default:
      return unreachable(parentage, "placeByParentage");
  }
}

/** The attributes of the attempt span of `facts` but the workflow ones: those of the Python and Rust clients. Pure. */
export function runAttributes(facts: RunSpanFacts): OtelAttributes {
  return {
    [TASK_ID_KEY]: facts.taskId,
    [TASK_NAME_KEY]: facts.taskName,
    [TASK_ATTEMPT_KEY]: facts.attempt,
    [TASK_EXECUTION_KEY]: facts.execution,
    [QUEUE_KEY]: facts.queueName,
    [WORKER_ID_KEY]: facts.workerId,
    [MESSAGING_SYSTEM_KEY]: MESSAGING_SYSTEM,
    [MESSAGING_OPERATION_TYPE_KEY]: PROCESS_OPERATION,
    [MESSAGING_OPERATION_NAME_KEY]: RUN_OPERATION,
    [MESSAGING_DESTINATION_NAME_KEY]: facts.queueName,
    [MESSAGING_MESSAGE_ID_KEY]: facts.taskId,
  };
}

/** `horsies.workflow.id`, `.node` and `.root_id` of a task that a workflow node links; none for another task. Pure. */
export function workflowAttributes(workflow: BodyWorkflowRecord): OtelAttributes {
  switch (workflow.kind) {
    case "notInWorkflow":
      return {};
    case "inWorkflow":
      return {
        [WORKFLOW_ID_KEY]: workflow.workflowId,
        [WORKFLOW_NODE_KEY]: workflow.nodeIndex,
        [WORKFLOW_ROOT_ID_KEY]: workflow.rootWorkflowId,
      };
    default:
      return unreachable(workflow, "workflowAttributes");
  }
}

/** The ids of `span` for the log context in lower-case hex, or none for a span that is not valid (no SDK). */
function spanIdsOf(api: OtelApi, span: OtelSpan): SpanIds | NoSpanIds {
  const spanContext = span.spanContext();

  return api.trace.isSpanContextValid(spanContext)
    ? Object.freeze({ kind: "spanIds", traceId: asciiLowerCase(spanContext.traceId), spanId: asciiLowerCase(spanContext.spanId) })
    : NO_SPAN_IDS;
}

/** Record the thrown value `cause` on `span`: an `Error` as it is, another value as its text. */
export function recordThrown(span: OtelSpan, cause: unknown): void {
  span.recordException(cause instanceof Error ? cause : thrownRecord(cause).message);
}

/**
 * Set the status of `span` by how its body ended, and end it at `at`. A returned value leaves the status unset, as the
 * Python client does and as the OpenTelemetry rule for instrumentation libraries says.
 */
export function endAttemptSpan(api: OtelApi, span: OtelSpan, end: BodyEnd, at: SpanTime): void {
  switch (end.kind) {
    case "returned":
      break;
    case "failed":
      markError(api, span, end.code);
      break;
    case "timedOut":
      markError(api, span, OutcomeCode.TASK_TIMEOUT);
      break;
    case "cancelled":
      markError(api, span, OutcomeCode.TASK_CANCELLED);
      break;
    default:
      unreachable(end, "endAttemptSpan");
  }

  switch (at.kind) {
    case "sdkClock":
      span.end();

      return;
    case "at":
      span.end(at.time);

      return;
    default:
      unreachable(at, "endAttemptSpan");
  }
}

/** Run `step` of a span; a throw of the user's SDK goes to `notMade`. */
export function guarded(step: () => void, notMade: (cause: unknown) => void): void {
  try {
    step();
  } catch (cause) {
    notMade(cause);
  }
}
