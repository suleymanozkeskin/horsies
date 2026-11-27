// The functions of the `logging` area of the addon, the records they carry, and the accessor.

import type { LoggerName, LogLevel, LogThreshold } from "../../logging.ts";
import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";

/** One core log record, as the addon hands it to the sink. */
export interface LogRecordOut {
  readonly logger: LoggerName;
  readonly level: LogLevel;
  readonly message: string;
  readonly fields: LogFieldOut[];
  /** When the event was written, in epoch microseconds. */
  readonly epochMicros: bigint;
  readonly thread: string;
  readonly source: LogSourceOut;
}

/** One field of a record other than its message. */
export interface LogFieldOut {
  readonly name: string;
  readonly value: LogFieldValueOut;
}

/** The value of one field, as the event recorded it. */
export type LogFieldValueOut =
  | { readonly kind: "flag"; readonly value: boolean }
  | { readonly kind: "integer"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "text"; readonly text: string };

/** Where in the source an event was written. */
export type LogSourceOut =
  | { readonly kind: "known"; readonly file: string; readonly line: number }
  | { readonly kind: "unknown" };

/** The thresholds that the package sets. */
export interface LogLevelsArg {
  readonly defaultLevel: LogThreshold;
  readonly loggers: LoggerLevelArg[];
}

/** The threshold of one logger. */
export interface LoggerLevelArg {
  readonly logger: LoggerName;
  readonly level: LogThreshold;
}

/**
 * A span that the package did not make or did not complete: of a send, of a workflow start, of an attempt in a Node
 * child or in a seat isolate, of an attempt that the worker ended. Ids are canonical UUID texts.
 */
export type NotMadeSpan =
  | { readonly kind: "send"; readonly taskId: string }
  | { readonly kind: "workflowStart"; readonly workflowId: string }
  | { readonly kind: "attempt"; readonly taskId: string }
  | { readonly kind: "seatAttempt"; readonly taskId: string }
  | { readonly kind: "workerEnded"; readonly taskId: string };

/**
 * The sink function that the addon calls with each batch: `true` when the sink took the batch,
 * `false` when it threw. It never throws.
 */
export type NativeLogSink = (records: LogRecordOut[]) => boolean;

/** The format of the stderr lines of the core log records: the `kind` of the app's `logFormat`. */
export type LogFormatKind = "textLogs" | "jsonLogs";

/** The functions of the `logging` area. None of them starts the core. */
export interface LoggingAddon {
  setLogLevels(levels: LogLevelsArg): NativeEnvelope<undefined>;
  /** Set the format of the stderr lines of later core log records. */
  setLogFormat(format: LogFormatKind): NativeEnvelope<undefined>;
  setLogSink(sink: NativeLogSink): NativeEnvelope<undefined>;
  resetLogSink(): NativeEnvelope<undefined>;
  /**
   * Write the WARNING record of `span`, not made for `cause`, on `horsies.telemetry`, once per process and kind of span.
   * Starts the log bridge; a forwarder that cannot start throws `RUNTIME_START_FAILED`.
   */
  logSpanNotMade(span: NotMadeSpan, cause: string): NativeEnvelope<undefined>;
}

/** The `logging` functions of the addon. Throws as `requireAddon` does. */
export function loggingAddon(): LoggingAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as LoggingAddon;
}
