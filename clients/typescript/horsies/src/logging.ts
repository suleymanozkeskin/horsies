// The log output of the core in this process.
//
// The core writes its records to 18 named loggers of the `horsies` namespace. Each logger forwards
// the records at or above its threshold. The default threshold is WARNING, and the default sink
// writes one line per record to stderr. `configureLogging` sets the thresholds and the sink for the
// whole process; a change applies to later records.

import { envelopeValue } from "./native/call/envelope.ts";
import { loggingAddon } from "./native/logging/addon.ts";
import type { LogLevelsArg, LogRecordOut, NativeLogSink } from "./native/logging/addon.ts";

/** The loggers of the core, in the order of the core's list. */
export const LOGGER_NAMES = [
  "horsies.app",
  "horsies.bridge",
  "horsies.broker",
  "horsies.cli",
  "horsies.codec",
  "horsies.core",
  "horsies.db",
  "horsies.lifecycle",
  "horsies.listener",
  "horsies.monitoring",
  "horsies.scheduler",
  "horsies.scheduler.state",
  "horsies.sql",
  "horsies.web",
  "horsies.worker",
  "horsies.workflow.engine",
  "horsies.workflow.handle",
  "horsies.workflow.recovery",
] as const;

/** A logger of the core. */
export type LoggerName = (typeof LOGGER_NAMES)[number];

/** The level of one record. */
export type LogLevel = "TRACE" | "DEBUG" | "INFO" | "WARNING" | "ERROR";

/** The lowest level that a logger forwards. `CRITICAL` forwards no core record. */
export type LogThreshold = LogLevel | "CRITICAL";

/** The value of one field of a record, as the core wrote it. */
export type LogFieldValue =
  | { readonly kind: "flag"; readonly value: boolean }
  | { readonly kind: "integer"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "text"; readonly text: string };

/** One field of a record other than its message. */
export interface LogField {
  readonly name: string;
  readonly value: LogFieldValue;
}

/** Where in the core's source a record was written. */
export type LogSource =
  | { readonly kind: "known"; readonly file: string; readonly line: number }
  | { readonly kind: "unknown" };

/** One core log record. */
export interface LogRecord {
  readonly logger: LoggerName;
  readonly level: LogLevel;
  readonly message: string;
  readonly fields: readonly LogField[];
  readonly time: Temporal.Instant;
  /** The name of the core thread that wrote the record. */
  readonly thread: string;
  readonly source: LogSource;
}

/**
 * A sink of core log records. It runs on the JavaScript thread, with one batch at a time, and
 * returns when it took the batch. A batch that the sink throws on goes to stderr.
 */
export type LogSink = (records: readonly LogRecord[]) => void;

/** A default threshold for every logger, and thresholds for named loggers. */
export interface LogLevels {
  readonly default: LogThreshold;
  readonly loggers?: Readonly<Partial<Record<LoggerName, LogThreshold>>>;
}

/** What `configureLogging` changes. An absent member keeps its setting. */
export interface LoggingOptions {
  /** A sink function, or `"stderr"` for one line per record on stderr (the default). */
  readonly sink?: LogSink | "stderr";
  readonly levels?: LogLevels;
}

/**
 * Set the thresholds and the sink of the core log records of this process. It can run before the
 * first core call and at any later time; it starts no core.
 */
export function configureLogging(options: LoggingOptions): void {
  const addon = loggingAddon();

  if (options.levels !== undefined) {
    envelopeValue(addon.setLogLevels(levelsArgument(options.levels)));
  }

  switch (options.sink) {
    case undefined:
      return;
    case "stderr":
      envelopeValue(addon.resetLogSink());

      return;
    default:
      envelopeValue(addon.setLogSink(guardedSink(options.sink, writeStderr)));
  }
}

/** The thresholds as the addon takes them: the default, then each named logger. Pure. */
export function levelsArgument(levels: LogLevels): LogLevelsArg {
  const named = levels.loggers ?? {};

  return {
    defaultLevel: levels.default,
    loggers: LOGGER_NAMES.flatMap((logger) => {
      const level = named[logger];

      return level === undefined ? [] : [{ logger, level }];
    }),
  };
}

/** The public form of one record. Pure. */
export function publicRecord(record: LogRecordOut): LogRecord {
  return {
    logger: record.logger,
    level: record.level,
    message: record.message,
    fields: record.fields,
    time: Temporal.Instant.fromEpochNanoseconds(record.epochMicros * 1000n),
    thread: record.thread,
    source: record.source,
  };
}

/**
 * The sink function for the addon: it hands each batch to `sink` and never throws. A throw of
 * `sink` goes to `writeError` as one line, and the answer `false` sends the batch to stderr.
 */
export function guardedSink(sink: LogSink, writeError: (line: string) => void): NativeLogSink {
  return (records) => {
    try {
      sink(records.map(publicRecord));

      return true;
    } catch (cause) {
      reportSinkThrow(cause, writeError);

      return false;
    }
  };
}

function reportSinkThrow(cause: unknown, writeError: (line: string) => void): void {
  try {
    writeError(`horsies: the log sink threw: ${cause instanceof Error ? cause.message : String(cause)}\n`);
  } catch {
    // The wrapper must not throw into the addon: a throw there ends the process. The batch still
    // goes to stderr through the addon.
  }
}

function writeStderr(line: string): void {
  process.stderr.write(line);
}
