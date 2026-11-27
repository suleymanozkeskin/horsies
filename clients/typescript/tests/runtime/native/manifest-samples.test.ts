// The core parses one sample of each generated manifest root through the addon: the text is the generated `toWire`
// tree, written by the package's codec writer (`manifestText`). The unit tests of the generated types compare these
// texts with Python's; only the addon shows that the core accepts them. A refusal fails the test with its field path.
//
// - `ProducerManifest`: `buildCoreHandle` parses it with the core's producer manifest parser.
// - `SchedulerManifest`: `prepareScheduler`, with one pattern of each kind.
// - `WorkflowSpecData`: `prepareClientSpec`.
// - `WorkerManifest`: the addon builds the config sections itself and parses the whole manifest; the start members
//   (profile, metrics endpoint, tasks, handlers, spans switch and child launch) are the part that the package
//   writes, so `prepareWorker` takes them.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`). It needs no database: nothing connects.
// Run it under the memory watchdog (2 GiB, with a wall-time bound).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { producerManifestToWire, type ProducerManifest } from "../../../horsies/src/_catalog/manifest.ts";
import {
  schedulePatternToWire,
  type SchedulePattern,
} from "../../../horsies/src/_catalog/schedule_pattern.ts";
import {
  schedulerManifestToWire,
  type SchedulerManifest,
} from "../../../horsies/src/_catalog/scheduler_manifest.ts";
import type { WireJson } from "../../../horsies/src/_catalog/wire_json.ts";
import { workflowSpecDataToWire, type WorkflowSpecData } from "../../../horsies/src/_catalog/workflow_spec.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { coreState } from "../../../horsies/src/native/process-core/process-core.ts";
import { prepareScheduler } from "../../../horsies/src/native/scheduler/scheduler.ts";
import { prepareClientSpec } from "../../../horsies/src/native/workflows/workflows.ts";
import { prepareWorker } from "../../../horsies/src/native/worker/worker.ts";
import { manifestText } from "../../../horsies/src/worker/manifest-text.ts";
import { startMembersText, type StartMembers } from "../../../horsies/src/worker/start-members.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The text of `wire`, as the package writes it for the core. */
function textOf(wire: ReadonlyMap<string, WireJson>): string {
  return manifestText(wire);
}

const PRODUCER_SAMPLE: ProducerManifest = {
  manifest_version: 1,
  database: {
    database_url: "postgresql://app@127.0.0.1:55432/app",
    session_database: { kind: "SEPARATE", url: "postgresql://app@127.0.0.1:55432/session" },
    pgbouncer_transaction_mode: false,
    pool_pre_ping: true,
    pool_size: 5,
    max_overflow: 10,
    pool_timeout_seconds: 30,
    pool_recycle: { kind: "AFTER_SECONDS", seconds: 3600 },
    echo: false,
    tcp_keepalives: { kind: "ON", idle_seconds: 60, interval_seconds: 10, probes: 5 },
    tcp_user_timeout: { kind: "AFTER", milliseconds: 30_000 },
  },
  schema: "OBSERVE_ONLY",
  send: { retain_rerun_input_default: true, idempotency_reservation_window: 2_592_000_000_000 },
};

/** One pattern of each kind of the generated pattern union. */
const PATTERNS: readonly SchedulePattern[] = [
  { type: "interval", seconds: null, minutes: 5, hours: null, days: null },
  { type: "hourly", minute: 15, second: 0 },
  { type: "daily", time: Temporal.PlainTime.from("09:00:00") },
  { type: "weekly", days: ["monday", "friday"], time: Temporal.PlainTime.from("18:30:00") },
  { type: "monthly", day: 28, time: Temporal.PlainTime.from("02:00:00") },
  {
    type: "cron",
    minute: [{ kind: "values", values: [0, 30] }],
    hour: [{ kind: "range", start: 9, end: 17, step: 2 }],
    month: [{ kind: "every" }],
    day: { kind: "every_day" },
  },
];

/** The scheduler sample: one enabled schedule for each pattern, and one disabled schedule. */
function schedulerSample(): SchedulerManifest {
  return {
    manifest_version: 1,
    enabled: true,
    check_interval_seconds: 1,
    schedules: [
      ...PATTERNS.map((pattern, position) => ({
        name: `${pattern.type}-schedule-${String(position)}`,
        activation: {
          kind: "ENABLED" as const,
          pattern_json: textOf(schedulePatternToWire(pattern)),
          timezone: "UTC",
          catch_up_missed: position % 2 === 0,
          max_catch_up_runs: 100,
          send: {
            task_name: "ping",
            queue_name: "default",
            priority: 100,
            input_json: "{}",
            task_options: position === 0 ? { kind: "ENCODED" as const, json: '{"timeout_ms":5000}' } : { kind: "NO_OPTIONS" as const },
            retention_class_key: "standard_30d",
            retain_rerun_input: false,
          },
        },
      })),
      { name: "paused-digest", activation: { kind: "DISABLED" as const } },
    ],
  };
}

const WORKFLOW_SAMPLE: WorkflowSpecData = {
  data_version: 1,
  name: "checkout",
  definition_key: { kind: "KEYED", key: "tests.checkout.v1" },
  on_error: "PAUSE",
  output: { kind: "NODE", index: 1 },
  success_policy: { kind: "CASES", cases: [{ required: [1] }], optional: [0] },
  queue_classes: new Map([["default", "standard_30d"]]),
  nodes: [
    {
      index: 0,
      node_id: "first",
      task_name: "app.charge",
      input_json: '{"order_id":7}',
      dependencies: [],
      args_from: new Map(),
      workflow_context_from: [],
      allow_failed_dependencies: false,
      join: { kind: "ALL" },
      body: { kind: "TASK", queue: "default", priority: 100, task_options: { kind: "NO_OPTIONS" } },
    },
    {
      index: 1,
      node_id: "second",
      task_name: "app.ship",
      input_json: '{"order_id":7}',
      dependencies: [0],
      args_from: new Map([["charge", 0]]),
      workflow_context_from: [],
      allow_failed_dependencies: false,
      join: { kind: "QUORUM", min_success: 1 },
      body: { kind: "TASK", queue: "default", priority: 3, task_options: { kind: "NO_OPTIONS" } },
    },
  ],
};

/** The config text of an app with the profile `io` of one isolated child. */
const WORKER_CONFIG = JSON.stringify({
  database: { database_url: "postgresql://127.0.0.1:55432/horsies" },
  worker_profiles: { io: { isolated: { kind: "isolated_children", count: 1 } } },
});

const WORKER_START_MEMBERS: StartMembers = {
  profile: { kind: "NAMED", name: "io" },
  metricsEndpoint: { kind: "LISTEN_ADDRESS", host: "127.0.0.1", port: 9464 },
  tasks: new Map([
    ["ping", { execution: "ISOLATED", error_codes: [] }],
    ["charge", { execution: "SHARED_ASYNC", error_codes: ["CARD_DECLINED", "BANK_DOWN"] }],
  ]),
  handlers: { kind: "HANDLERS", handlers: [{ name: "alerts", event_type: "AttemptEnded" }] },
  workerEndedSpans: "MAKE_SPANS",
  childLaunch: {
    command: { program: process.execPath, args: ["/app/main.js", "--horsies-child"] },
    preload: { kind: "NO_PRELOAD_PROGRAM" },
    program: { kind: "RUST" },
    log_level: "INFO",
  },
};

describe("the core parses the generated manifest samples", () => {
  test("the producer manifest", () => {
    const handle = buildCoreHandle(textOf(producerManifestToWire(PRODUCER_SAMPLE)), "PRODUCER");

    assert.equal(handle.manifestVersion(), 1);
    assert.equal(coreState(), "not_started");
  });

  test("the scheduler manifest, with one pattern of each kind", () => {
    const prepared = prepareScheduler(textOf(schedulerManifestToWire(schedulerSample())));

    assert.equal(
      prepared.kind === "manifestRefused" ? `refused at ${prepared.fieldPath}` : prepared.kind,
      "prepared",
    );
    assert.equal(coreState(), "not_started");
  });

  test("the workflow spec data", () => {
    const prepared = prepareClientSpec(textOf(workflowSpecDataToWire(WORKFLOW_SAMPLE)));

    assert.equal(prepared.kind === "refused" ? `refused at ${prepared.fieldPath}: ${prepared.cause}` : prepared.kind, "prepared");
  });

  test("the worker manifest: the start members that the package writes, with the config sections of the addon", () => {
    const prepared = prepareWorker(WORKER_CONFIG, startMembersText(WORKER_START_MEMBERS), "CAPPED");

    assert.equal(prepared.kind, "prepared");
    assert.equal(coreState(), "not_started");
  });
});
