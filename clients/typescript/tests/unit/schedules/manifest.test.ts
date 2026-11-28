// The scheduler manifest of a declaration: the send facts of an enabled schedule; the queue from the schedule, from
// the task, and the default queue; a disabled schedule holds its name only; the section; the defaults. The core's
// parser refuses a repeated name (`tests/runtime/native/scheduler.test.ts`; here the refusal of the fake parser
// reaches the list, `checks.test.ts`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { OPTIONS_TEXT } from "../app/support.ts";
import { customModeApp, declare, defaultModeApp, fakeCore, HOURLY, ping, report, sync, unlisted } from "./support.ts";

/** The one manifest text that the fake parser was given. */
function onlyManifest(manifests: readonly string[]): string {
  assert.equal(manifests.length, 1);

  return manifests[0] ?? assert.fail("no manifest");
}

/** The send member of an enabled schedule of the manifest with these facts. */
function sendOf(task: string, queue: string, priority: number, inputJson: string, retention: string, retainRerunInput: boolean): object {
  return {
    task_name: task,
    queue: queue,
    priority,
    input_json: inputJson,
    task_options: { kind: "ENCODED", json: OPTIONS_TEXT },
    retention_class_key: retention,
    retain_rerun_input: retainRerunInput,
  };
}

describe("the scheduler manifest of a declaration", () => {
  test("the send facts of an enabled schedule, in the members' order", () => {
    const core = fakeCore({ retainRerunInput: true });

    declare(
      defaultModeApp(),
      [
        {
          name: "daily-report",
          task: report,
          input: { format: "pdf" },
          pattern: { kind: "daily", time: Temporal.PlainTime.from("09:00") },
          timezone: "Europe/Berlin",
          catchUpMissed: true,
          maxCatchUpRuns: 5,
        },
      ],
      { kind: "core", calls: core },
    );

    const expected = {
      manifest_version: 1,
      enabled: true,
      check_interval_seconds: 1,
      schedules: [
        {
          name: "daily-report",
          activation: {
            kind: "ENABLED",
            pattern_json: '{"kind":"daily","time":"09:00:00"}',
            timezone: "Europe/Berlin",
            catch_up_missed: true,
            max_catch_up_runs: 5,
            send: sendOf("report", "default", 100, '{"format":"pdf"}', "standard_30d", true),
          },
        },
      ],
    };

    assert.equal(onlyManifest(core.manifests), JSON.stringify(expected));
  });

  test("the queue of each slot: the schedule's, the task's, and the default queue, each with its priority", () => {
    const custom = fakeCore();

    declare(
      customModeApp(),
      [
        { name: "on-task-queue", task: sync, input: { region: "eu" }, pattern: HOURLY },
        { name: "on-schedule-queue", task: sync, input: { region: "us" }, pattern: HOURLY, queue: "urgent" },
      ],
      { kind: "core", calls: custom },
    );

    const sends = JSON.parse(onlyManifest(custom.manifests)).schedules.map(
      (schedule: { readonly activation: { readonly send: object } }) => schedule.activation.send,
    );

    assert.deepEqual(sends, [
      sendOf("sync", "standard", 50, '{"region":"eu"}', "standard_30d", false),
      sendOf("sync", "urgent", 1, '{"region":"us"}', "forever", false),
    ]);

    const fallback = fakeCore();

    declare(defaultModeApp(), [{ name: "ping", task: ping, input: {}, pattern: HOURLY }], { kind: "core", calls: fallback });

    assert.deepEqual(JSON.parse(onlyManifest(fallback.manifests)).schedules[0].activation.send, sendOf("ping", "default", 100, "{}", "standard_30d", false));
  });

  test("a disabled schedule holds its name only", () => {
    const core = fakeCore();

    declare(defaultModeApp(), [{ name: "paused", task: unlisted, input: {}, pattern: HOURLY, enabled: false }], {
      kind: "core",
      calls: core,
    });

    assert.deepEqual(JSON.parse(onlyManifest(core.manifests)).schedules, [{ name: "paused", activation: { kind: "DISABLED" } }]);
  });

  test("the section: the master switch and the check interval", () => {
    const core = fakeCore();

    declare(defaultModeApp(), [], { kind: "core", calls: core }, { enabled: false, checkIntervalSeconds: 30 });

    assert.equal(onlyManifest(core.manifests), '{"manifest_version":1,"enabled":false,"check_interval_seconds":30,"schedules":[]}');
  });

  test("the defaults of an entry: the zone UTC, no catch-up, 100 catch-up runs", () => {
    const core = fakeCore();

    declare(defaultModeApp(), [{ name: "ping", task: ping, input: {}, pattern: HOURLY }], { kind: "core", calls: core });

    const activation = JSON.parse(onlyManifest(core.manifests)).schedules[0].activation;

    assert.deepEqual([activation.timezone, activation.catch_up_missed, activation.max_catch_up_runs], ["UTC", false, 100]);
  });
});
