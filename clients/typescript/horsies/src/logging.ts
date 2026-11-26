// The log output of the core in this process.
//
// The core writes its records to 19 named loggers of the `horsies` namespace. Each logger forwards
// the records at or above its threshold. The default threshold is WARNING, and the default sink
// writes one line per record to stderr. `configureLogging` sets the thresholds and the sink for the
// whole process; a change applies to later records.
//
// Thresholds follow the dotted names, as Python's `logging` does: a threshold set for
// `horsies.workflow` applies to `horsies.workflow.engine` unless that logger has its own.

import { processStart } from "./app/suppression.ts";
import { nativeValue } from "./native/call/envelope.ts";
import { thrownFacts } from "./native/call/thrown-facts.ts";
import { loggingAddon } from "./native/logging/addon.ts";
import type { LogLevelsArg, LogRecordOut, NativeLogSink } from "./native/logging/addon.ts";
import { keepLogSink } from "./native/logging/sink-slot.ts";

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
  "horsies.telemetry",
  "horsies.web",
  "horsies.worker",
  "horsies.workflow.engine",
  "horsies.workflow.handle",
  "horsies.workflow.recovery",
] as const;

/** A logger of the core. */
export type LoggerName = (typeof LOGGER_NAMES)[number];

/** The dotted parents of the loggers that are no logger themselves. */
export const LOGGER_PARENTS = ["horsies", "horsies.workflow"] as const;

/** A key of the thresholds: a logger, or a dotted parent of loggers. */
export type LoggerKey = LoggerName | (typeof LOGGER_PARENTS)[number];

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
 * returns when it took the batch. It is sync: its type refuses an async function. A batch that the
 * sink throws on goes to stderr. A Promise that it returns anyway, and that rejects, is reported on
 * stderr with the count of rejected batches; its records are lost.
 */
export type LogSink = (records: readonly LogRecord[]) => undefined;

/**
 * A default threshold, and thresholds by key. A key applies to the logger of its name and to the
 * loggers below it in the dotted names; the longest key that applies wins; a logger that no key
 * applies to has the default.
 */
export interface LogLevels {
  readonly default: LogThreshold;
  readonly loggers?: Readonly<Partial<Record<LoggerKey, LogThreshold>>>;
}

/** Where the core log records go: one line per record on stderr (the default), or a sink function. */
export type LogSinkSetting = { readonly kind: "stderr" } | { readonly kind: "function"; readonly sink: LogSink };

/** What `configureLogging` changes. An absent member keeps its setting. */
export interface LoggingOptions {
  readonly sink?: LogSinkSetting;
  readonly levels?: LogLevels;
}

/**
 * Set the thresholds and the sink of the core log records of this process. It can run before the
 * first core call and at any later time; it starts no core. The thresholds apply first: if the sink
 * step then throws, the thresholds stay as set. In a seat isolate of a `SHARED_THREAD` child it does
 * nothing: no core runs there, and the child's main thread holds the core log of the process (the
 * entry runs there too).
 */
export function configureLogging(options: LoggingOptions): void {
  const thread = loggingThread();

  switch (thread) {
    case "seatIsolate":
      return;
    case "processCore":
      configureCoreLogging(options);

      return;
    default: {
      const unexpected: never = thread;

      throw new TypeError(`configureLogging: a thread outside its type: ${String(unexpected)}`);
    }
  }
}

/** Where `configureLogging` applies: the core log of the process, or nothing in a seat isolate. */
function loggingThread(): "processCore" | "seatIsolate" {
  return processStart().seatIsolate ? "seatIsolate" : "processCore";
}

/** The thresholds and the sink of the core log of this process. Loads the addon. */
function configureCoreLogging(options: LoggingOptions): void {
  const addon = loggingAddon();
  const levels = options.levels;

  if (levels !== undefined) {
    nativeValue("setLogLevels", () => addon.setLogLevels(levelsArgument(levels)));
  }

  const sink = options.sink;

  if (sink === undefined) {
    return;
  }

  switch (sink.kind) {
    case "stderr":
      nativeValue("resetLogSink", () => addon.resetLogSink());
      keepLogSink({ kind: "stderr" });

      return;
    case "function": {
      const guarded = guardedSink(sink.sink, writeStderr);

      nativeValue("setLogSink", () => addon.setLogSink(guarded));
      keepLogSink({ kind: "function", sink: guarded });

      return;
    }

    default: {
      const unexpected: never = sink;

      throw new TypeError(`configureLogging: a sink outside its type: ${String(unexpected)}`);
    }
  }
}

/** The thresholds as the addon takes them: the default, and each logger that a key applies to. Pure. */
export function levelsArgument(levels: LogLevels): LogLevelsArg {
  const keyed = levels.loggers ?? {};

  return {
    defaultLevel: levels.default,
    loggers: LOGGER_NAMES.flatMap((logger) => {
      const level = keyedThreshold(logger, keyed);

      return level === undefined ? [] : [{ logger, level }];
    }),
  };
}

/** The threshold of the longest key that applies to `logger`, or none. Pure. */
function keyedThreshold(
  logger: LoggerName,
  keyed: Readonly<Partial<Record<LoggerKey, LogThreshold>>>,
): LogThreshold | undefined {
  const keys: readonly LoggerKey[] = [...LOGGER_NAMES, ...LOGGER_PARENTS];

  const applying = keys
    .filter((key) => logger === key || logger.startsWith(`${key}.`))
    .filter((key) => keyed[key] !== undefined)
    .toSorted((left, right) => right.length - left.length);

  const longest = applying[0];

  return longest === undefined ? undefined : keyed[longest];
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
 * `sink` goes to `writeError` as one line, and the answer `false` sends the batch to stderr. A
 * Promise that `sink` returns gets a rejection handler: a rejection goes to `writeError` with the
 * count of rejected batches, and the process goes on. An empty batch (one that the addon abandoned
 * at its answer bound) does not reach `sink`.
 */
export function guardedSink(sink: LogSink, writeError: (line: string) => void): NativeLogSink {
  let rejectedBatches = 0;

  return (records) => {
    if (records.length === 0) {
      return true;
    }

    let returned: unknown;

    try {
      returned = sink(records.map(publicRecord));
    } catch (cause) {
      writeSafely(writeError, `horsies: the log sink threw: ${thrownFacts(cause).message}\n`);

      return false;
    }

    if (returned !== undefined) {
      const count = records.length;

      Promise.resolve(returned).then(undefined, (cause: unknown) => {
        rejectedBatches += 1;
        writeSafely(
          writeError,
          `horsies: the log sink rejected a batch of ${count} records (rejected batches: ${rejectedBatches}); the records are lost: ${thrownFacts(cause).message}\n`,
        );
      });
    }

    return true;
  };
}

/** Write one line; a throw of `writeError` is dropped: the wrapper must not throw into the addon. */
function writeSafely(writeError: (line: string) => void, line: string): void {
  try {
    writeError(line);
  } catch {
    // Nothing more can report it.
  }
}

function writeStderr(line: string): void {
  process.stderr.write(line);
}
