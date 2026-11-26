// The pure parts of the log configuration: the thresholds argument, the public record, and the
// sink wrapper that never throws.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { guardedSink, levelsArgument, publicRecord } from "../../../horsies/src/logging.ts";
import type { LogRecord, LogSink } from "../../../horsies/src/logging.ts";
import type { LogRecordOut } from "../../../horsies/src/native/logging/addon.ts";

const RAW_RECORD: LogRecordOut = {
  logger: "horsies.worker",
  level: "INFO",
  message: "worker ready",
  fields: [{ name: "bytes", value: { kind: "integer", value: 18_446_744_073_709_551_615n } }],
  epochMicros: 1_795_608_000_123_456n,
  thread: "horsies-core",
  source: { kind: "known", file: "core/src/worker/worker.rs", line: 12 },
};

describe("levelsArgument", () => {
  test("takes the default and only the named loggers", () => {
    assert.deepEqual(
      levelsArgument({ default: "WARNING", loggers: { "horsies.worker": "INFO", "horsies.sql": "CRITICAL" } }),
      {
        defaultLevel: "WARNING",
        loggers: [
          { logger: "horsies.sql", level: "CRITICAL" },
          { logger: "horsies.worker", level: "INFO" },
        ],
      },
    );
  });

  test("a dotted parent applies to every logger below it", () => {
    assert.deepEqual(levelsArgument({ default: "WARNING", loggers: { "horsies.workflow": "DEBUG" } }).loggers, [
      { logger: "horsies.workflow.engine", level: "DEBUG" },
      { logger: "horsies.workflow.handle", level: "DEBUG" },
      { logger: "horsies.workflow.recovery", level: "DEBUG" },
    ]);
  });

  test("a logger applies to the loggers below it", () => {
    assert.deepEqual(levelsArgument({ default: "WARNING", loggers: { "horsies.scheduler": "INFO" } }).loggers, [
      { logger: "horsies.scheduler", level: "INFO" },
      { logger: "horsies.scheduler.state", level: "INFO" },
    ]);
  });

  test("the longest key wins", () => {
    assert.deepEqual(
      levelsArgument({
        default: "WARNING",
        loggers: { "horsies.workflow": "DEBUG", "horsies.workflow.engine": "ERROR" },
      }).loggers,
      [
        { logger: "horsies.workflow.engine", level: "ERROR" },
        { logger: "horsies.workflow.handle", level: "DEBUG" },
        { logger: "horsies.workflow.recovery", level: "DEBUG" },
      ],
    );
  });

  test("the namespace key applies to every logger", () => {
    const argument = levelsArgument({ default: "WARNING", loggers: { horsies: "ERROR" } });

    assert.equal(argument.loggers.length, 18);
    assert.ok(argument.loggers.every((named) => named.level === "ERROR"));
  });

  test("gives no named logger when none is named", () => {
    assert.deepEqual(levelsArgument({ default: "DEBUG" }), { defaultLevel: "DEBUG", loggers: [] });
  });
});

describe("publicRecord", () => {
  test("keeps every fact and gives the time as an instant to the microsecond", () => {
    const { time, ...facts } = publicRecord(RAW_RECORD);

    assert.equal(time.toString(), "2026-11-25T12:00:00.123456Z");
    assert.deepEqual(facts, {
      logger: RAW_RECORD.logger,
      level: RAW_RECORD.level,
      message: RAW_RECORD.message,
      fields: RAW_RECORD.fields,
      thread: RAW_RECORD.thread,
      source: RAW_RECORD.source,
    });
  });
});

describe("guardedSink", () => {
  test("hands the batch to the sink and answers true", () => {
    const taken: LogRecord[] = [];

    const sink = guardedSink((records) => {
      taken.push(...records);
    }, () => {});

    assert.equal(sink([RAW_RECORD]), true);
    assert.equal(taken.length, 1);
    assert.equal(taken[0]?.message, "worker ready");
  });

  test("answers false and writes one line when the sink throws", () => {
    const lines: string[] = [];

    const sink = guardedSink(
      () => {
        throw new Error("disk full");
      },
      (line) => {
        lines.push(line);
      },
    );

    assert.equal(sink([RAW_RECORD]), false);
    assert.deepEqual(lines, ["horsies: the log sink threw: disk full\n"]);
  });

  test("an empty batch does not reach the sink", () => {
    let calls = 0;

    const sink = guardedSink(() => {
      calls += 1;
    }, () => {});

    assert.equal(sink([]), true);
    assert.equal(calls, 0);
  });

  test("a sink that rejects is reported with the count of rejected batches, and nothing throws", async () => {
    const lines: string[] = [];

    const rejecting = async (): Promise<undefined> => {
      throw new Error("collector down");
    };

    // SAFETY: a JavaScript caller can pass an async sink, which the types refuse; the test passes one.
    const sink = guardedSink(rejecting as unknown as LogSink, (line) => {
      lines.push(line);
    });

    assert.equal(sink([RAW_RECORD]), true);
    assert.equal(sink([RAW_RECORD, RAW_RECORD]), true);
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    assert.deepEqual(lines, [
      "horsies: the log sink rejected a batch of 1 records (rejected batches: 1); the records are lost: collector down\n",
      "horsies: the log sink rejected a batch of 2 records (rejected batches: 2); the records are lost: collector down\n",
    ]);
  });

  test("answers false and does not throw when the error line cannot be written", () => {
    const sink = guardedSink(
      () => {
        throw new Error("disk full");
      },
      () => {
        throw new Error("stderr closed");
      },
    );

    assert.equal(sink([RAW_RECORD]), false);
  });
});
