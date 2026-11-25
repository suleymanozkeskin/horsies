// The functions of the `logging` area of the addon, the records they carry, and the accessor.

import type { LoggerName, LogLevel, LogThreshold } from "../../logging.ts";
import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";

/** One core log record, as the addon hands it to the sink. */
export interface LogRecordOut {
  readonly logger: LoggerName;
  readonly level: LogLevel;
  readonly message: string;
  readonly fields: readonly LogFieldOut[];
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
  readonly loggers: readonly LoggerLevelArg[];
}

/** The threshold of one logger. */
export interface LoggerLevelArg {
  readonly logger: LoggerName;
  readonly level: LogThreshold;
}

/**
 * The sink function that the addon calls with each batch: `true` when the sink took the batch,
 * `false` when it threw. It never throws.
 */
export type NativeLogSink = (records: LogRecordOut[]) => boolean;

/** The functions of the `logging` area. None of them starts the core. */
export interface LoggingAddon {
  setLogLevels(levels: LogLevelsArg): NativeEnvelope<undefined>;
  setLogSink(sink: NativeLogSink): NativeEnvelope<undefined>;
  resetLogSink(): NativeEnvelope<undefined>;
}

/** The `logging` functions of the addon. Throws as `requireAddon` does. */
export function loggingAddon(): LoggingAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as LoggingAddon;
}
