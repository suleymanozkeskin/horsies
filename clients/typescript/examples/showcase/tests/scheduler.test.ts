// The schedules: the table of the app (offline), and a scheduler process that sends the due slots of a schedule of a
// 1 s interval (the database test, opt-in with `ACME_DATABASE_URL`). No worker runs: the slots stay `PENDING`.

import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { describe, test } from "node:test";

import { TaskId } from "horsies";

import { prewarmSearch } from "../src/contracts/promotions.ts";
import { ACME_SCHEDULES } from "../src/schedules.ts";
import { closeStore } from "../src/store/connection.ts";
import { startEntryAt, stopEntry, logTail } from "../scripts/entry-process.ts";
import { compressedScheduleName, SCHEDULE_MARKER_VARIABLE } from "./compressed-schedule-entry.ts";
import { testShowcase } from "./showcase.ts";
import { testDatabase, uniqueId } from "./support.ts";

/** The schedule counts of the Rust showcase, which this table ports. */
const SCHEDULE_COUNT = 32;
const ENABLED_SCHEDULE_COUNT = 29;

/** How long the scheduler of the database test runs before its stop. */
const SCHEDULER_RUN_MS = 6_000;

const COMPRESSED_ENTRY: string = fileURLToPath(new URL("./compressed-schedule-entry.ts", import.meta.url));

describe("the schedule table", () => {
  test("it has the 32 schedules of the Rust showcase, 29 enabled, each name once", () => {
    assert.equal(ACME_SCHEDULES.length, SCHEDULE_COUNT);
    assert.equal(ACME_SCHEDULES.filter((entry) => entry.enabled !== false).length, ENABLED_SCHEDULE_COUNT);
    assert.equal(new Set(ACME_SCHEDULES.map((entry) => entry.name)).size, SCHEDULE_COUNT);
  });
});

test("a scheduler process sends the due slots of a 1 s schedule", { timeout: 120_000 }, async () => {
  const database = await testDatabase();

  if (database.kind === "skipped") {
    return;
  }

  try {
    const showcase = testShowcase(database.settings);

    assert.ok((await showcase.app.pingDatabase()).ok, "the schema installs before the scheduler starts");

    const marker = uniqueId("schedule");

    const scheduler = startEntryAt(COMPRESSED_ENTRY, ["scheduler"], {
      ACME_DATABASE_URL: database.settings.url,
      [SCHEDULE_MARKER_VARIABLE]: marker,
    });

    await new Promise((resolve) => setTimeout(resolve, SCHEDULER_RUN_MS));
    assert.equal(await stopEntry(scheduler), "exited", logTail(scheduler, 30));
    assert.equal(scheduler.child.exitCode, 0, logTail(scheduler, 30));

    const states = await showcase.app.monitoring.listSchedules();

    assert.ok(states.ok, JSON.stringify(states));

    const state = states.value.find((each) => each.scheduleName === compressedScheduleName(marker));

    assert.ok(state !== undefined && state.runCount >= 1, `${JSON.stringify(state)}\n${logTail(scheduler, 30)}`);
    assert.ok(state.lastTaskId !== null);

    const lastTask = TaskId.parse(state.lastTaskId);

    assert.ok(lastTask.ok);

    const info = await showcase.app.taskHandle(prewarmSearch, lastTask.value).info();

    assert.ok(info.ok, JSON.stringify(info));
    assert.equal(info.value.taskName, "prewarm_search");
    assert.equal(info.value.status, "PENDING");
  } finally {
    await closeStore(database.store);
  }
});
