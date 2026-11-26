// The tracing step of `Horsies.create`: the setting of the config, the load of the OpenTelemetry API for `otel`, and
// the observers that the send path and the children call. It runs in each process that runs the app entry, seat
// isolates included: the API is plain JavaScript and loads no addon.
//
// - `none`: the API is never loaded; every observer is the one that does nothing.
// - `otel`: the API is loaded once; a failed load is the refusal `CONFIG_OTEL_API_MISSING` (`HRS-220`).

import { ErrorCode } from "../_catalog/error_codes.ts";
import { NO_OP_OBSERVER, type SendObserver } from "../app/observers.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { OTEL_API_MODULE, type OtelApi, type OtelApiLoader } from "./otel-api.ts";
import { otelSendObserver } from "./send-span.ts";
import type { TracingSetting } from "./tracing-config.ts";
import { processWarnings, type TelemetryWarnings } from "./warnings.ts";

/** The tracing parts of an app: its setting and the observers that the package calls. */
export type TracingParts = {
  readonly setting: TracingSetting;
  readonly sendObserver: SendObserver;
};

/** What the tracing step reads. A test gives its own. */
export type TracingEnvironment = {
  readonly loadApi: OtelApiLoader;
  readonly warnings: TelemetryWarnings;
  /** Milliseconds on the monotonic clock. */
  readonly clock: () => number;
};

/** The tracing environment of this process. */
export function processTracingEnvironment(loadApi: OtelApiLoader): TracingEnvironment {
  return { loadApi, warnings: processWarnings, clock: () => performance.now() };
}

/** The config path of the tracing setting. */
const TRACING_PATH = "tracing";

/**
 * The tracing parts of `setting`, or the refusal `CONFIG_OTEL_API_MISSING`. Effects: loads the API for `otel` only.
 */
export function tracingPartsOf(setting: TracingSetting, environment: TracingEnvironment): Result<TracingParts, Refusal> {
  switch (setting.kind) {
    case "none":
      return ok(Object.freeze({ setting, sendObserver: NO_OP_OBSERVER }));
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

function otelParts(setting: TracingSetting, api: OtelApi, environment: TracingEnvironment): TracingParts {
  return Object.freeze({
    setting,
    sendObserver: otelSendObserver({ api, warnings: environment.warnings, clock: environment.clock }),
  });
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
