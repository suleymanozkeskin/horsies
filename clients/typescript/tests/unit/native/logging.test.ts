// The pure parts of the log configuration: the thresholds argument, the public record, and the
// sink wrapper that never throws.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { guardedSink, levelsArgument, publicRecord } from "../../../horsies/src/logging.ts";
import type { LogRecord } from "../../../horsies/src/logging.ts";
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
