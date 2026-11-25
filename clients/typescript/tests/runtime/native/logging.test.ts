// The JavaScript log sink with a built addon and a database: the statement log of a ping reaches
// the sink at the thresholds that the package set, and a sink that throws does not end the process.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { configureLogging } from "../../../horsies/src/logging.ts";
import type { LogRecord } from "../../../horsies/src/logging.ts";
import { envelopeValue } from "../../../horsies/src/native/call/envelope.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

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
      sink: (batch) => {
        records.push(...batch);
      },
    });

    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), true), "PRODUCER");

    envelopeValue(await handle.pingDatabase());

    await waitUntil(() => records.some((record) => record.logger === "horsies.sql"));

    const statement = records.find((record) => record.logger === "horsies.sql");

    assert.ok(statement !== undefined, `records: ${records.map((record) => record.logger).join(", ")}`);
    assert.equal(statement.level, "INFO");
    assert.ok(statement.fields.some((field) => field.name === "statement"));
    assert.ok(records.every((record) => record.logger === "horsies.sql" || record.level !== "INFO"));
    envelopeValue(await handle.close());
    configureLogging({ sink: "stderr" });
  });

  test("a sink that throws sends the batch to stderr and the process goes on", async () => {
    let calls = 0;

    configureLogging({
      levels: { default: "WARNING", loggers: { "horsies.sql": "INFO" } },
      sink: () => {
        calls += 1;

        throw new Error("planned sink failure");
      },
    });

    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), true), "PRODUCER");
    const ping = envelopeValue(await handle.pingDatabase());

    assert.equal(ping.kind, "done");
    assert.ok(await waitUntil(() => calls > 0));
    envelopeValue(await handle.close());
    configureLogging({ sink: "stderr" });
  });
});
