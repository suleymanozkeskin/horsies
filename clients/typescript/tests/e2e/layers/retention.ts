// The retention config through a worker of the CLI (Python `clients/python/tests/e2e/test_queue_retention_routing.py`,
// `test_declared_retention_assembly.py`).
//
// - A task sent on a queue that the config maps to a period lands, at its end, in a history partition of that period's
//   class. The partition comes from `tableoid`: the routing fact itself.
// - A worker registers the period that its config declares. The test process installs the schema with the default
//   instance, whose install registers no such period, so the class can only come from the worker.

import assert from "node:assert/strict";
import { describe } from "node:test";

import { loadAddon } from "#horsies";
import { DECLARED_CLASS_KEY, DECLARED_PERIOD_DAYS, MAPPED_CLASS_KEY } from "../src/instances.ts";
import * as basic from "../src/tasks/basic.ts";
import { sentHandle } from "../support/outcome.ts";
import { pollUntil, sqlText } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the end of a task and for the registration of a class. */
const WAIT_MS = 60_000;

/** The retention days of class `classKey`, or `absent`. */
async function registeredDays(suite: Suite, classKey: string): Promise<number | "absent"> {
  const read = await suite.rows.query(
    `read the class ${classKey}`,
    `SELECT retention_days::text FROM horsies_retention_classes WHERE class_key = ${sqlText(classKey)}`,
  );

  const value = read.rows[0]?.[0];

  return value === undefined || value === null ? "absent" : Number(value);
}

describe("retention", () => {
  e2eTest("a task on a mapped queue lands in a history partition of its period's class", "queue_retention", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 21 }));
    let partition = "";

    await pollUntil(WAIT_MS, "the task reaches history", async () => {
      const read = await suite.rows.query(
        "read the partition of the record",
        `SELECT tableoid::regclass::text FROM horsies_task_history WHERE task_id = ${sqlText(handle.taskId)}`,
      );

      partition = read.rows[0]?.[0] ?? "";

      return partition !== "";
    });
    assert.ok(partition.includes(MAPPED_CLASS_KEY), `the record landed in ${partition}, not in a leaf of ${MAPPED_CLASS_KEY}`);
  });

  e2eTest("a worker registers the retention class of the period that its config declares", "default", async (suite) => {
    assert.equal(await registeredDays(suite, DECLARED_CLASS_KEY), "absent", "the schema install registered the class");

    await suite.worker("isolated-1", "declared_retention");
    await pollUntil(WAIT_MS, `the class ${DECLARED_CLASS_KEY} is registered`, async () => (await registeredDays(suite, DECLARED_CLASS_KEY)) !== "absent");
    assert.equal(await registeredDays(suite, DECLARED_CLASS_KEY), DECLARED_PERIOD_DAYS);
  });
});
