// The tracing step of `Horsies.create`: the setting of the config, the load of the OpenTelemetry API for `otel`, and
// the observers that the send path, the workflow start path, the children and the seat isolates call. It runs in each
// process that runs the app entry, and in each seat isolate (each isolate evaluates the entry, so its `Horsies.create`
// loads the API there): the API is plain JavaScript and loads no addon.
//
// - `none`: the API is never loaded; every observer is the one that does nothing.
// - `otel`: the API is loaded once; a failed load is the refusal `CONFIG_OTEL_API_MISSING` (`HRS-220`).

import { ErrorCode } from "../_catalog/error_codes.ts";
import { NO_OP_OBSERVER, type SendObserver } from "../app/observers.ts";
import type { AppContext } from "../app/context.ts";
import { type BodyObserver, NO_BODY_OBSERVER, setBodyObserver } from "../child/body-observer.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import { NO_SEAT_BODY_OBSERVER, type SeatBodyObserver, setSeatBodyObserver } from "../seats/body-observer.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { NO_OP_START_OBSERVER, type StartObserver } from "../workflow/start.ts";
import { OTEL_API_MODULE, type OtelApi, type OtelApiLoader } from "./otel-api.ts";
import { type AttemptSpanDeps, otelBodyObserver } from "./attempt-span.ts";
import { type EndedSpans, NO_ENDED_SPANS, otelEndedSpans } from "./ended-span.ts";
import { otelSeatBodyObserver } from "./seat-span.ts";
import { otelSendObserver } from "./send-span.ts";
import { otelStartObserver } from "./start-span.ts";
import type { OtelTracingSetting, TracingSetting } from "./tracing-config.ts";
import { processWarnings, type TelemetryWarnings } from "./warnings.ts";

/** The tracing parts of an app: its setting and the observers that the package calls. */
export type TracingParts = {
  readonly setting: TracingSetting;
  /** Wraps each send of the app; gives the carrier of a rerun. */
  readonly sendObserver: SendObserver;
  /** Wraps the core's start of each `app.start` and `app.retryStart`; gives the carrier that the tree stores. */
  readonly startObserver: StartObserver;
  /** Wraps each task body in a child of the app. */
  readonly bodyObserver: BodyObserver;
  /** Wraps each task body in a seat isolate of a `SHARED_THREAD` child of the app. */
  readonly seatBodyObserver: SeatBodyObserver;
  /** Makes the span of each attempt that the worker ended, in the handler child. */
  readonly endedSpans: EndedSpans;
};

/** The parts of an app with no tracing. */
const NO_TRACING_PARTS: TracingParts = Object.freeze({
  setting: Object.freeze({ kind: "none" }),
  sendObserver: NO_OP_OBSERVER,
  startObserver: NO_OP_START_OBSERVER,
  bodyObserver: NO_BODY_OBSERVER,
  seatBodyObserver: NO_SEAT_BODY_OBSERVER,
  endedSpans: NO_ENDED_SPANS,
});

/** What the tracing step reads. A test gives its own. */
export type TracingEnvironment = {
  readonly loadApi: OtelApiLoader;
  readonly warnings: TelemetryWarnings;
  /** Milliseconds on the monotonic clock. */
  readonly clock: () => number;
  /** The wall clock: the start of the span of a seat run, whose end is the main thread's wall-clock time. */
  readonly now: () => Temporal.Instant;
};

/** The tracing environment of this process. */
export function processTracingEnvironment(loadApi: OtelApiLoader): TracingEnvironment {
  return { loadApi, warnings: processWarnings, clock: () => performance.now(), now: () => Temporal.Now.instant() };
}

/** The config path of the tracing setting. */
const TRACING_PATH = "tracing";

/**
 * The tracing parts of `setting`, or the refusal `CONFIG_OTEL_API_MISSING`. Effects: loads the API for `otel` only.
 */
export function tracingPartsOf(setting: TracingSetting, environment: TracingEnvironment): Result<TracingParts, Refusal> {
  switch (setting.kind) {
    case "none":
      return ok(NO_TRACING_PARTS);
    case "otel": {
      const loaded = environment.loadApi();

      switch (loaded.kind) {
        case "loaded":
          return ok(otelParts(setting, loaded.api, environment));
        case "otelApiMissing":
          return err(apiMissing(loaded.cause));
        default:
          return unreachable(loaded, "tracingPartsOf");
      }
    }

    default:
      return unreachable(setting, "tracingPartsOf");
  }
}

function otelParts(setting: OtelTracingSetting, api: OtelApi, environment: TracingEnvironment): TracingParts {
  const { warnings } = environment;
  const spanDeps = { api, parentage: setting.parentage, warnings };

  return Object.freeze({
    setting,
    sendObserver: otelSendObserver({ api, warnings, clock: environment.clock }),
    startObserver: otelStartObserver({ api, warnings }),
    bodyObserver: otelBodyObserver(spanDeps),
    seatBodyObserver: otelSeatBodyObserver({ ...spanDeps, now: environment.now }),
    endedSpans: endedSpansOf(setting, spanDeps),
  });
}

/** The span maker of the worker-ended spans by the setting's switch. */
function endedSpansOf(setting: OtelTracingSetting, deps: AttemptSpanDeps): EndedSpans {
  switch (setting.workerEndedSpans) {
    case "makeSpans":
      return otelEndedSpans(deps);
    case "noSpans":
      return NO_ENDED_SPANS;
    default:
      return unreachable(setting.workerEndedSpans, "endedSpansOf");
  }
}

const partsByApp = new WeakMap<AppContext, TracingParts>();

/** Keep the tracing parts of the app of `context`, and set its body observers. `Horsies.create` calls it. */
export function recordTracingParts(context: AppContext, parts: TracingParts): void {
  partsByApp.set(context, parts);
  setBodyObserver(context, parts.bodyObserver);
  setSeatBodyObserver(context, parts.seatBodyObserver);
}

/**
 * The tracing parts of the app of `context`. `Horsies.create` records the parts of each context it makes, so a
 * context with none is a defect of the package: `ClientDefect`.
 */
export function tracingPartsOfApp(context: AppContext): TracingParts {
  const recorded = partsByApp.get(context);

  if (recorded === undefined) {
    throw new ClientDefect({ subject: "tracingPartsOfApp", rule: "Horsies.create records the tracing parts of each app" });
  }

  return recorded;
}

/** The refusal of `otel` with no API: the cause of the failed load, and the two fixes. Pure. */
function apiMissing(cause: string): Refusal {
  return refusal(
    ErrorCode.CONFIG_OTEL_API_MISSING,
    TRACING_PATH,
    `{ kind: "otel" } needs the OpenTelemetry API: the module ${OTEL_API_MODULE} does not load (${cause})`,
    [`install the peer dependency ${OTEL_API_MODULE} (1.7 or later, before 2.0)`, 'or set tracing: { kind: "none" }'],
  );
}
