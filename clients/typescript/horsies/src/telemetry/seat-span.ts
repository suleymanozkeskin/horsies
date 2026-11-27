// The OpenTelemetry seat body observer of a seat isolate of a `SHARED_THREAD` child: the attempt span of each seat run,
// the same span as in a Node child (`attempt-span.ts`): `run <task_name>`, kind CONSUMER, the parent from the stored
// carrier by the parentage, the attributes of the Python client, active where the body runs.
//
// - The SDK is the isolate's own: each isolate has its own globals, and the app entry that it evaluates sets them, so
//   an SDK set at the top of the entry (or in a provider, which runs in each isolate) reaches it.
// - The facts come in the `run` message. The end comes in the `bodyEnded` message: the main thread's verdict and the
//   main thread's time of the settle (`endedAt`), which is the end time of the span. The start time is this isolate's
//   wall clock at the open, so the start and the end come from one clock (the system clock of the child).
// - The run's cancel signal ends nothing: a seat run ends at its settle, with the verdict of its cancel cause. A sync
//   body that holds its isolate past its deadline ends its span at that settle, not at the deadline.
// - A run whose isolate ends has its span made by the main thread of the child (`isolateEnded`): the span that the
//   isolate opened is never exported. Same name, kind, parent and attributes, with `horsies.span.source` = `worker`
//   as the span that the handler child makes for an attempt that the worker ended; status ERROR with `error.type` =
//   `WORKER_CRASHED`; it starts at the main thread's time of the `run` post and ends at the isolate's exit.
// - `flush` calls `forceFlush()` of the isolate's tracer provider; the main thread asks for it at the child's stop.
// - A throw of the user's SDK never changes a run. A throw at any stage (the open, the record of a thrown value, the
//   end) is a span not made: one WARNING record per process on `horsies.telemetry` (`not-made.ts`; the isolate sends
//   it to the main thread of its child).

import { OperationalCode } from "../_catalog/task_codes.ts";
import type { BodyEnd } from "../child/body-observer.ts";
import type { BodyFactsRecord, CancelCauseName } from "../native/child/addon.ts";
import { type EndedSeatRun, NO_SEAT_OBSERVATION, type SeatBodyObservation, type SeatBodyObserver } from "../seats/body-observer.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  type AttemptSpanDeps,
  endAttemptSpan,
  guarded,
  type OpenSpan,
  openedAttemptSpan,
  type RecordedFacts,
  recordThrown,
  runInSpan,
} from "./attempt-span.ts";
import { SPAN_SOURCE_KEY, WORKER_SPAN_SOURCE } from "./attributes.ts";
import { flushTracerProvider } from "./flush.ts";
import type { OtelHighResolutionTime } from "./otel-api.ts";

const NANOSECONDS_PER_SECOND = 1_000_000_000n;

/** The high-resolution time of `instant`: seconds and nanoseconds since the epoch, with no loss. Pure. */
export function highResolutionTimeOfInstant(instant: Temporal.Instant): OtelHighResolutionTime {
  const nanoseconds = instant.epochNanoseconds;
  const remainder = ((nanoseconds % NANOSECONDS_PER_SECOND) + NANOSECONDS_PER_SECOND) % NANOSECONDS_PER_SECOND;

  return [Number((nanoseconds - remainder) / NANOSECONDS_PER_SECOND), Number(remainder)];
}

/** What the seat observer reads: the attempt span parts, and the isolate's wall clock for the start of a span. */
export type SeatSpanDeps = AttemptSpanDeps & { readonly now: () => Temporal.Instant };

/** The seat body observer of an app with OpenTelemetry tracing. */
export function otelSeatBodyObserver(deps: SeatSpanDeps): SeatBodyObserver {
  return Object.freeze({
    enter: (facts: BodyFactsRecord): SeatBodyObservation => {
      switch (facts.kind) {
        case "notRecorded":
          return NO_SEAT_OBSERVATION;
        case "recorded":
          return seatObservation(deps, facts);
        default:
          return unreachable(facts, "otelSeatBodyObserver.enter");
      }
    },
    flush: async (): Promise<void> => flushTracerProvider(deps),
    isolateEnded: (run: EndedSeatRun): void => {
      makeIsolateEndedSpan(deps, run);
    },
  });
}

/**
 * Make and end the span of a run whose isolate ended: `WORKER_CRASHED`, from the main thread's time of the `run` post
 * to the isolate's exit. Facts that are not recorded give no span. A throw of the user's SDK is a span not made.
 */
function makeIsolateEndedSpan(deps: SeatSpanDeps, run: EndedSeatRun): void {
  switch (run.facts.kind) {
    case "notRecorded":
      return;
    case "recorded": {
      const { facts } = run;

      const notMade = (cause: unknown): void => {
        deps.notMade.report({ kind: "seatAttempt", taskId: facts.taskId }, cause);
      };

      const opened = openedAttemptSpan(
        deps,
        facts,
        { kind: "at", time: highResolutionTimeOfInstant(run.startedAt) },
        { [SPAN_SOURCE_KEY]: WORKER_SPAN_SOURCE },
        notMade,
      );

      switch (opened.kind) {
        case "notOpen":
          return;
        case "open":
          guarded(() => {
            endAttemptSpan(
              deps.api,
              opened.open.span,
              { kind: "failed", code: OperationalCode.WORKER_CRASHED },
              { kind: "at", time: highResolutionTimeOfInstant(run.endedAt) },
            );
          }, notMade);

          return;
        default:
          return unreachable(opened, "makeIsolateEndedSpan");
      }
    }

    default:
      return unreachable(run.facts, "makeIsolateEndedSpan");
  }
}

/** The observation of one seat body: its span from the isolate's clock now, or none when the SDK threw at the open. */
function seatObservation(deps: SeatSpanDeps, facts: RecordedFacts): SeatBodyObservation {
  const notMade = (cause: unknown): void => {
    deps.notMade.report({ kind: "seatAttempt", taskId: facts.taskId }, cause);
  };

  const opened = openedAttemptSpan(deps, facts, { kind: "at", time: highResolutionTimeOfInstant(deps.now()) }, {}, notMade);

  switch (opened.kind) {
    case "notOpen":
      return NO_SEAT_OBSERVATION;
    case "open":
      return seatSpanObservation(deps, opened.open, notMade);
    default:
      return unreachable(opened, "seatObservation");
  }
}

/**
 * The observation of the open span `open`. The isolate ends it once, at the verdict of the run; a throw of the SDK at
 * any stage goes to `notMade`.
 */
function seatSpanObservation(deps: SeatSpanDeps, open: OpenSpan, notMade: (cause: unknown) => void): SeatBodyObservation {
  return Object.freeze({
    run: <T>(call: () => T): T => runInSpan(deps.api, open.active, open.ids, call),
    thrown: (cause: unknown): void => {
      guarded(() => {
        recordThrown(open.span, cause);
      }, notMade);
    },
    end: (end: BodyEnd, endedAt: Temporal.Instant): void => {
      guarded(() => {
        endAttemptSpan(deps.api, open.span, end, { kind: "at", time: highResolutionTimeOfInstant(endedAt) });
      }, notMade);
    },
    cancelled: (_cause: CancelCauseName): void => undefined,
  });
}
