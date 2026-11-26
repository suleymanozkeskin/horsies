// The `scheduler` area of the addon with a built addon: the core's pattern check of the TypeScript pattern texts, the
// core's rules of a pattern, the config facts of the schedules, the core's manifest parser, and the run of a
// scheduler on a database until a stop request.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database. Run it under the memory watchdog (2 GiB, with a wall-time bound).

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { coreState } from "../../../horsies/src/native/process-core/process-core.ts";
import {
  checkSchedulePattern,
  checkScheduleTimezone,
  prepareScheduler,
  requestSchedulerStop,
  runScheduler,
  schedulerConfigFacts,
} from "../../../horsies/src/native/scheduler/scheduler.ts";
import { patternRefusalText, patternText, type SchedulePattern } from "../../../horsies/src/schedules/pattern.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import type { ScheduleStateInfo } from "../../../horsies/src/_catalog/monitoring_models.ts";
import { schedules } from "../../../horsies/src/monitoring/reads.ts";
import { monitoringReadsAddon } from "../../../horsies/src/native/facades/monitoring-reads.ts";
import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** A database URL whose connect is refused at once: nothing listens on port 1. */
const REFUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

/** The pattern texts that the core, the Python client and the TypeScript pattern tests share. */
const PATTERN_TEXTS_URL = new URL("../../../../../core/tests/fixtures/schedule_pattern/pattern_texts.json", import.meta.url);

const fixture: { readonly patterns: readonly { readonly name: string; readonly text: string }[] } = JSON.parse(
  readFileSync(PATTERN_TEXTS_URL, "utf8"),
);

/** The stored text of `pattern`; a refusal of the stored form fails the test. */
function textOf(pattern: SchedulePattern): string {
  const text = patternText(pattern);

  assert.ok(text.ok, text.ok ? "" : patternRefusalText(text.error));

  return text.value;
}

/** One enabled schedule of a manifest: `name` fires by `patternJson` in `timezone` and sends `ping` on the default queue. */
function enabledSchedule(name: string, patternJson: string, timezone = "UTC"): object {
  return {
    name,
    activation: {
      kind: "ENABLED",
      pattern_json: patternJson,
      timezone,
      catch_up_missed: false,
      max_catch_up_runs: 100,
      send: {
        task_name: "ping",
        queue_name: "default",
        priority: 100,
        input_json: "{}",
        task_options: { kind: "NO_OPTIONS" },
        retention_class_key: "standard_30d",
        retain_rerun_input: false,
      },
    },
  };
}

/** The text of a scheduler manifest of `schedules`, with the section defaults. */
function manifestText(schedules: readonly object[]): string {
  return JSON.stringify({ manifest_version: 1, enabled: true, check_interval_seconds: 1, schedules });
}

/** The most reads of the schedule state while a test waits for the start to write its row. */
const MAX_STATE_READS = 100;

/** The pause between two reads of the schedule state, in milliseconds. */
const STATE_READ_PAUSE_MS = 100;

/**
 * The state rows of the schedule `name`: at once (`now`), or after the first read that holds one (`written`), at most
 * `MAX_STATE_READS` reads apart by `STATE_READ_PAUSE_MS`. A read error fails the test.
 */
async function stateRowsOf(handle: CoreHandle, name: string, when: "now" | "written"): Promise<readonly ScheduleStateInfo[]> {
  for (let read = 1; read <= MAX_STATE_READS; read += 1) {
    const states = await schedules({ addon: monitoringReadsAddon(), handle: handle.native });

    assert.ok(states.ok, "the schedule state reads");

    const rows = states.value.filter((state) => state.scheduleName === name);

    if (when === "now" || rows.length > 0) {
      return rows;
    }

    await new Promise((resolve) => setTimeout(resolve, STATE_READ_PAUSE_MS));
  }

  return assert.fail(`no state row of ${name} after ${String(MAX_STATE_READS)} reads`);
}

/** The patterns that the stored form holds and that break a rule of the core, by the rule. */
const RULE_BREAKERS: readonly (readonly [string, SchedulePattern])[] = [
  ["an interval with no part", { kind: "interval", every: Temporal.Duration.from({ seconds: 0 }) }],
  ["an interval part above its bound", { kind: "interval", every: Temporal.Duration.from({ seconds: 86_401 }) }],
  ["a negative interval", { kind: "interval", every: Temporal.Duration.from({ minutes: -5 }) }],
  ["an hourly minute of 60", { kind: "hourly", minute: 60 }],
  ["a daily time with a fraction of a second", { kind: "daily", at: Temporal.PlainTime.from("09:30:00.5") }],
  ["a repeated weekly day", { kind: "weekly", days: ["monday", "monday"], at: Temporal.PlainTime.from("08:30") }],
  ["a monthly day of 32", { kind: "monthly", day: 32, at: Temporal.PlainTime.from("08:30") }],
  [
    "a cron minute value of 60",
    { kind: "cron", minute: [{ kind: "values", values: [60] }], hour: [{ kind: "every" }], month: [{ kind: "every" }], day: { kind: "everyDay" } },
  ],
  [
    "a cron range whose start comes after its end",
    { kind: "cron", minute: [{ kind: "every" }], hour: [{ kind: "range", start: 18, end: 8 }], month: [{ kind: "every" }], day: { kind: "everyDay" } },
  ],
  [
    "a cron month range that wraps around",
    {
      kind: "cron",
      minute: [{ kind: "every" }],
      hour: [{ kind: "every" }],
      month: [{ kind: "enumRange", start: "november", end: "february" }],
      day: { kind: "everyDay" },
    },
  ],
  [
    "a cron pattern that never fires",
    {
      kind: "cron",
      minute: [{ kind: "values", values: [0] }],
      hour: [{ kind: "values", values: [0] }],
      month: [{ kind: "enumValues", values: ["february"] }],
      day: { kind: "byMonthDay", dayOfMonth: [{ kind: "values", values: [30] }] },
    },
  ],
];

describe("the scheduler area of the addon", () => {
  test("each pattern text of the fixture passes the core's pattern check and the core's manifest parser", () => {
    for (const row of fixture.patterns) {
      assert.deepEqual(checkSchedulePattern(row.text), { kind: "accepted" }, row.name);
    }

    const schedules = fixture.patterns.map((row) => enabledSchedule(row.name, row.text));

    assert.equal(prepareScheduler(manifestText(schedules)).kind, "prepared");
    assert.equal(coreState(), "not_started");
  });

  for (const [rule, pattern] of RULE_BREAKERS) {
    test(`the core refuses ${rule}`, () => {
      const text = textOf(pattern);
      const checked = checkSchedulePattern(text);

      assert.equal(checked.kind, "refused", text);

      const prepared = prepareScheduler(manifestText([enabledSchedule("broken", text)]));

      assert.equal(prepared.kind, "manifestRefused", text);
    });
  }

  test("the core's zone data holds the four zones of the North American rules, on every runtime", () => {
    for (const name of ["EST5EDT", "CST6CDT", "MST7MDT", "PST8PDT", "UTC", "US/Eastern"]) {
      assert.equal(checkScheduleTimezone(name), "known", name);
    }

    for (const name of ["Nope/Zone", "utc", "+05:00"]) {
      assert.equal(checkScheduleTimezone(name), "unknown", name);
    }
  });

  test("a repeated name is refused by the core's parser at the second schedule", () => {
    const text = textOf({ kind: "hourly", minute: 5 });
    const prepared = prepareScheduler(manifestText([enabledSchedule("nightly", text), enabledSchedule("nightly", text)]));

    assert.equal(prepared.kind === "manifestRefused" ? prepared.fieldPath : prepared.kind, "schedules[1].name");
  });

  test("the config facts of the default queue mode and of custom queues", () => {
    const defaultMode = JSON.stringify({ broker: { database_url: "postgresql://127.0.0.1:55432/horsies" } });

    assert.deepEqual(schedulerConfigFacts(defaultMode, ["default"]), {
      rejectBound: { kind: "noBound" },
      retainRerunInput: false,
      queues: [{ queueName: "default", priority: { kind: "defaultQueue" }, retentionClassKey: "standard_30d" }],
    });

    const customMode = JSON.stringify({
      broker: { database_url: "postgresql://127.0.0.1:55432/horsies", retain_rerun_input_default: true },
      queue_mode: "custom",
      custom_queues: [{ name: "urgent", priority: 20 }],
      payload: { reject_bytes: 4096 },
      retention: { queue_retention: { urgent: null } },
    });

    assert.deepEqual(schedulerConfigFacts(customMode, ["urgent"]), {
      rejectBound: { kind: "bytes", bytes: 4096n },
      retainRerunInput: true,
      queues: [{ queueName: "urgent", priority: { kind: "configured", priority: 20 }, retentionClassKey: "forever" }],
    });
    assert.throws(() => schedulerConfigFacts(customMode, ["absent"]), ClientDefect);
    assert.equal(coreState(), "not_started");
  });

  test("a stop request before the run ends it at once; a second run throws ClientDefect", async () => {
    const handle = buildCoreHandle(manifestJson(REFUSED_DATABASE_URL, false), "PRODUCER");
    const prepared = prepareScheduler(manifestText([enabledSchedule("never-connects", textOf({ kind: "hourly", minute: 0 }))]));

    assert.equal(prepared.kind, "prepared");

    if (prepared.kind === "prepared") {
      requestSchedulerStop(prepared.scheduler);
      // The connect to the refused URL would end `startFailed`: the stop wins before it.
      assert.deepEqual(await runScheduler(prepared.scheduler, handle), { kind: "stopped" });
      await assert.rejects(runScheduler(prepared.scheduler, handle), ClientDefect);
    }

    await handle.close();
  });

  test("an enabled schedule starts active and writes its state row; a stop ends the run", async () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");
    const name = `native-active-${String(process.pid)}-${String(Date.now())}`;

    // The state reads below run while the run connects; on a new database they need the schema before the run
    // installs it.
    assert.equal((await handle.installSchema()).kind, "installed");
    const prepared = prepareScheduler(manifestText([enabledSchedule(name, textOf({ kind: "interval", every: Temporal.Duration.from({ seconds: 30 }) }))]));

    assert.equal(prepared.kind, "prepared");

    if (prepared.kind === "prepared") {
      const run = runScheduler(prepared.scheduler, handle);
      const rows = await stateRowsOf(handle, name, "written");

      requestSchedulerStop(prepared.scheduler);
      assert.deepEqual(await run, { kind: "stopped" });
      assert.equal(rows.length, 1);
    }

    await handle.close();
  });

  test("a zone that the core does not know passes the parser and is refused at the start, with no state row", async () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");
    const name = `native-zone-${String(process.pid)}-${String(Date.now())}`;
    const prepared = prepareScheduler(manifestText([enabledSchedule(name, textOf({ kind: "hourly", minute: 0 }), "Not/A_Zone")]));

    assert.equal(prepared.kind, "prepared");

    if (prepared.kind === "prepared") {
      const ended = await runScheduler(prepared.scheduler, handle);

      assert.equal(ended.kind, "startRefused");
      assert.equal(ended.kind === "startRefused" ? ended.schedule : "", name);
      assert.deepEqual(await stateRowsOf(handle, name, "now"), []);
    }

    await handle.close();
  });

  test("a run whose connect is refused is a start failure", async () => {
    const handle = buildCoreHandle(manifestJson(REFUSED_DATABASE_URL, false), "PRODUCER");
    const prepared = prepareScheduler(manifestText([]));

    assert.equal(prepared.kind, "prepared");

    if (prepared.kind === "prepared") {
      assert.equal((await runScheduler(prepared.scheduler, handle)).kind, "startFailed");
    }

    await handle.close();
  });
});
