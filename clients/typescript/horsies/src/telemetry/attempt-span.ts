// The OpenTelemetry body observer of a child: one span of kind CONSUMER, `run <task_name>`, around each task body,
// active where the body runs, so a span that user code makes in the body nests under it, also after an `await`.
//
// - The parent comes from the carrier that the send stored, by the parentage: `childOfSend` gives the extracted
//   context as the parent; `linkedToSend` gives a root span with a link to the extracted span; a task with no
//   carrier gets a root span in both.
// - The body runs in `context.with` of a context that the package builds from the root context: a function that the
//   addon calls has an empty async context, so the package never takes the context of the bridge callback.
// - A thrown value is recorded on the span. The end sets the status: OK for a returned value; ERROR with
//   `error.type` for an error code, `TASK_TIMEOUT` (the run's deadline) and `TASK_CANCELLED` (an operator cancel).
// - A body that does not start gets no span (the core posts no body for it).
// - A throw of the user's SDK never changes a run: no span at the open, and one warning per process and kind.

import { OutcomeCode } from "../_catalog/task_codes.ts";
import { thrownRecord } from "../child/thrown-error.ts";
import type { BodyEnd, BodyObservation, BodyObserver } from "../child/body-observer.ts";
import type { BodyCarrierRecord, BodyExecution, BodyFactsRecord, BodyWorkflowRecord } from "../native/child/addon.ts";
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
import { extractedContextOf } from "./carrier.ts";
import { flushTracerProvider } from "./flush.ts";
import type { NoSpanIds, SpanIds } from "./log-context.ts";
import {
  INSTRUMENTATION_SCOPE,
  type OtelApi,
  type OtelAttributes,
  type OtelContext,
  type OtelLink,
  type OtelSpan,
} from "./otel-api.ts";
import { markError } from "./send-span.ts";
import { attemptSpanStore } from "./span-id-store.ts";
import type { AttemptParentage } from "./tracing-config.ts";
import type { TelemetryWarnings } from "./warnings.ts";

/** What the attempt-span observer reads. */
export type AttemptSpanDeps = {
  readonly api: OtelApi;
  readonly parentage: AttemptParentage;
  readonly warnings: TelemetryWarnings;
};

/** The facts of a run that has its facts recorded. */
type RecordedFacts = Extract<BodyFactsRecord, { readonly kind: "recorded" }>;

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
  const { api } = deps;
  let span: OtelSpan;
  let active: OtelContext;

  try {
    const place = spanPlace(api, deps.parentage, facts.carrier);
    const attributes = { ...runAttributes(facts), ...workflowAttributes(facts.workflow) };

    span = api.trace
      .getTracer(INSTRUMENTATION_SCOPE)
      .startSpan(`${RUN_OPERATION} ${facts.taskName}`, { kind: api.SpanKind.CONSUMER, attributes, links: place.links }, place.parent);
    active = api.trace.setSpan(place.parent, span);
  } catch (cause) {
    deps.warnings.warnOnce("attemptSpanOpen", cause);

    return NO_OBSERVATION;
  }

  const ids = spanIdsOf(api, span);

  return Object.freeze({
    run: <T>(call: () => T): T => runInSpan(api, active, ids, call),
    thrown: (cause: unknown): void => {
      guarded(deps, "attemptSpanEnd", () => {
        recordThrown(span, cause);
      });
    },
    end: (end: BodyEnd): void => {
      guarded(deps, "attemptSpanEnd", () => {
        endAttemptSpan(api, span, end);
      });
    },
  });
}

/** Run `call` with the attempt span active, and its ids in the store of `currentTask()`. */
function runInSpan<T>(api: OtelApi, active: OtelContext, ids: SpanIds | NoSpanIds, call: () => T): T {
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

      return { parent: api.ROOT_CONTEXT, links };
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

/** The ids of `span` for the log context, or none for a span that is not valid (no SDK). */
function spanIdsOf(api: OtelApi, span: OtelSpan): SpanIds | NoSpanIds {
  const spanContext = span.spanContext();

  return api.trace.isSpanContextValid(spanContext)
    ? Object.freeze({ kind: "spanIds", traceId: spanContext.traceId, spanId: spanContext.spanId })
    : NO_SPAN_IDS;
}

/** Record the thrown value `cause` on `span`: an `Error` as it is, another value as its text. */
function recordThrown(span: OtelSpan, cause: unknown): void {
  span.recordException(cause instanceof Error ? cause : thrownRecord(cause).message);
}

/** Set the status of `span` by how its body ended, and end it. */
function endAttemptSpan(api: OtelApi, span: OtelSpan, end: BodyEnd): void {
  switch (end.kind) {
    case "returned":
      span.setStatus({ code: api.SpanStatusCode.OK });
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

  span.end();
}

/** Run `step`; a throw of the user's SDK is written once per process. */
function guarded(deps: AttemptSpanDeps, kind: "attemptSpanEnd", step: () => void): void {
  try {
    step();
  } catch (cause) {
    deps.warnings.warnOnce(kind, cause);
  }
}
