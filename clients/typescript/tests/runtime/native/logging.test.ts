// The JavaScript log sink with a built addon and a database: the statement log of a ping reaches
// the sink at the thresholds that the package set; a sink that throws does not end the process; a
// set sink does not keep an idle process alive; a defect in a worker process reaches the sink
// before the abort; a record just before the exit rule of a child reaches the sink; the first payload warning of a
// process reaches the sink with no core started.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { configureLogging } from "../../../horsies/src/logging.ts";
import type { LogRecord } from "../../../horsies/src/logging.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { runAbortingChildCase, runChildCase } from "./child-process.ts";
import { EXIT_RECORD_CODE, EXIT_RECORD_MESSAGE } from "./logging-cases.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest time a child case may take. */
const CHILD_CASE_BOUND_MS = 30_000;

/** The longest time a test that runs one child case may take: the child bound, and the time to read its end. */
const CHILD_TEST_BOUND_MS = CHILD_CASE_BOUND_MS + 5_000;

/** The longest time a process with an empty loop may take to exit. */
const IDLE_EXIT_BOUND_MS = 10_000;

/** The keys of a JSON log line, in order: the keys of Python's `JsonLogsFormatter`. */
const JSON_LOG_KEYS = ["ts", "level", "logger", "message", "process", "task_id", "task_name", "attempt", "trace_id", "span_id", "exc"];

/** The levels that the default threshold WARNING lets through. */
const WARNING_AND_ABOVE: ReadonlySet<string> = new Set(["WARNING", "ERROR"]);

/** The longest wait for a record to reach the sink. */
const RECORD_WAIT_MS = 5_000;

/** The pause between two looks at the records that reached the sink. */
const RECORD_POLL_MS = 20;

/** Wait until `holds` is true, at most `RECORD_WAIT_MS`; whether it holds at the end. */
async function waitUntil(holds: () => boolean): Promise<boolean> {
  for (let waited = 0; waited < RECORD_WAIT_MS && !holds(); waited += RECORD_POLL_MS) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, RECORD_POLL_MS);
    });
  }

  return holds();
}

describe("the JavaScript log sink", () => {
  test("the statement log of a ping reaches the sink at INFO on horsies.sql", async () => {
    const records: LogRecord[] = [];

    configureLogging({
      levels: { default: "WARNING", loggers: { "horsies.sql": "INFO" } },
      sink: {
        kind: "function",
        sink: (batch) => {
          records.push(...batch);
        },
      },
    });

    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), true), "PRODUCER");

    await handle.pingDatabase();

    await waitUntil(() => records.some((record) => record.logger === "horsies.sql"));

    const statement = records.find((record) => record.logger === "horsies.sql");

    assert.ok(statement !== undefined, `records: ${records.map((record) => record.logger).join(", ")}`);
    assert.equal(statement.level, "INFO");
    assert.ok(statement.fields.some((field) => field.name === "statement"));
    assert.ok(
      records.every((record) => record.logger === "horsies.sql" || WARNING_AND_ABOVE.has(record.level)),
      records.map((record) => `${record.level} ${record.logger}`).join(", "),
    );
    await handle.close();
    configureLogging({ sink: { kind: "stderr" } });
  });

  test("a sink that throws sends the batch to stderr and the process goes on", () => {
    const child = runChildCase("logging-child.ts", "throwing-sink", CHILD_CASE_BOUND_MS);

    assert.equal(child.stdout, "pinged done; sink calls some", child.stderr);
    assert.match(child.stderr, /horsies: the log sink threw: planned sink failure/u);
    assert.match(child.stderr, /horsies: \d+ log records go to stderr: the JavaScript sink threw/u);
    assert.match(child.stderr, / INFO horsies\.sql /u);
  });

  test("a command of an app with jsonLogs writes each stderr record as one JSON line with the same keys", () => {
    const child = runChildCase("logging-child.ts", "command-json-lines", CHILD_CASE_BOUND_MS);

    assert.equal(child.stdout, "pinged done", child.stderr);

    const objects = child.stderr
      .split("\n")
      .filter((line) => line.startsWith("{"))
      .map((line): Record<string, unknown> => JSON.parse(line));

    const statement = objects.find((object) => object["logger"] === "horsies.sql");

    assert.ok(statement !== undefined, child.stderr);
    assert.equal(statement["level"], "INFO");
    assert.ok(Number.isSafeInteger(statement["process"]), child.stderr);
    assert.ok(objects.every((object) => Object.keys(object).join(",") === JSON_LOG_KEYS.join(",")), child.stderr);
    assert.doesNotMatch(child.stderr, / INFO horsies\.sql /u);
  });

  test("a process with a sink set and a started core exits when its loop is empty", () => {
    const child = runChildCase("logging-child.ts", "idle-exit-with-sink", IDLE_EXIT_BOUND_MS);

    assert.equal(child.signal, null, `killed at the bound: ${child.stderr}`);
    assert.equal(child.status, 0, child.stderr);
    assert.equal(child.stdout, "probed answered");
  });

  test("a defect in a worker process goes to stderr, then to the sink, then the process aborts", { timeout: CHILD_TEST_BOUND_MS }, () => {
    const child = runAbortingChildCase("logging-child.ts", "defect-record-to-sink", CHILD_CASE_BOUND_MS);

    assert.equal(child.signal, "SIGABRT", child.stderr);
    assert.match(child.stderr, /horsies: worker process \d+ aborts: defect CLIENT_DEFECT of the horsies package/u);
    assert.match(child.stdout, /^SINK ERROR horsies\.bridge the worker process aborts: defect CLIENT_DEFECT/u);
  });

  test("a core record just before the exit rule of a child reaches the sink before the process ends", () => {
    const child = runChildCase("logging-child.ts", "child-exit-after-record", CHILD_CASE_BOUND_MS);

    assert.equal(child.status, EXIT_RECORD_CODE, child.stderr);
    assert.equal(child.stdout, `SINK WARNING horsies.worker ${EXIT_RECORD_MESSAGE}`, child.stderr);
  });

  test("the first payload warning of a process reaches the sink, and the core does not start", () => {
    const child = runChildCase("logging-child.ts", "first-payload-warning", CHILD_CASE_BOUND_MS);

    assert.equal(child.status, 0, child.stderr);
    assert.match(child.stdout, /^prepared; core not_started; WARNING horsies\.app payload size guardrail/u);
  });
});
