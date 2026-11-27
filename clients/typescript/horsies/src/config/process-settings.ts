// The two settings of the app config that the core's `AppConfig` does not hold: the schema mode of the app's core
// handle, and the format of the stderr lines of the core log records. `Horsies.create` reads them; each has a default.

import type { SchemaMode } from "../_catalog/manifest.ts";
import { unreachable } from "../wire/unreachable.ts";

export type { SchemaMode } from "../_catalog/manifest.ts";

/** The schema mode of an app that leaves `schemaMode` out: the first operation that needs the schema migrates it. */
export const DEFAULT_SCHEMA_MODE: SchemaMode = "MIGRATE_AND_VALIDATE";

/**
 * The format of the stderr lines of the core log records in the worker, the scheduler, `check` and each worker child:
 * - `textLogs`: one line of text per record: the time, the level, the logger, the message and each field.
 * - `jsonLogs`: one JSON object per line, with the same keys in every record: `ts` (ISO 8601 in UTC, in
 *   microseconds), `level`, `logger`, `message`, `process` (the process id), `task_id`, `task_name`, `attempt`,
 *   `trace_id`, `span_id` (`null` when the record has no field of that name) and `exc` (`null`). The output is ASCII.
 */
export type LogFormat = { readonly kind: "textLogs" } | { readonly kind: "jsonLogs" };

/** The log format of an app that leaves `logFormat` out. */
export const TEXT_LOGS: LogFormat = Object.freeze({ kind: "textLogs" });

/** The log format of `jsonLogs`. */
const JSON_LOGS: LogFormat = Object.freeze({ kind: "jsonLogs" });

/** `mode` with the default filled. A value outside the union (a caller with no type check) is a `TypeError`. Pure. */
export function schemaModeOf(mode: SchemaMode | undefined): SchemaMode {
  switch (mode) {
    case undefined:
      return DEFAULT_SCHEMA_MODE;
    case "MIGRATE_AND_VALIDATE":
    case "OBSERVE_ONLY":
    case "ASSUME_INITIALIZED":
      return mode;
    default:
      return unreachable(mode, "schemaModeOf");
  }
}

/** `format` with the default filled. A `kind` outside the union (a caller with no type check) is a `TypeError`. Pure. */
export function logFormatOf(format: LogFormat | undefined): LogFormat {
  if (format === undefined) {
    return TEXT_LOGS;
  }

  switch (format.kind) {
    case "textLogs":
      return TEXT_LOGS;
    case "jsonLogs":
      return JSON_LOGS;
    default:
      return unreachable(format, "logFormatOf");
  }
}
