// Apps and fakes for the tests of the schedules: apps built with the fakes of the app hub, and a fake of the core
// calls of a declaration that records each call. No addon loads.

import assert from "node:assert/strict";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import type { HorsiesConfig } from "../../../horsies/src/app/create.ts";
import type { AppContext } from "../../../horsies/src/app/context.ts";
import { appInternals } from "../../../horsies/src/app/internals.ts";
import type { AnyTaskContract } from "../../../horsies/src/contract/define-task.ts";
import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type {
  NativePreparedScheduler,
  PatternCheck,
  RejectBound,
  SchedulerConfigFacts,
  SchedulerPreparation,
} from "../../../horsies/src/native/scheduler/addon.ts";
import {
  declareSchedules,
  type ScheduleCoreAccess,
  type ScheduleCoreCalls,
} from "../../../horsies/src/schedules/app-member.ts";
import {
  declaredEntries,
  type ScheduleEntries,
  type ScheduleSection,
} from "../../../horsies/src/schedules/entry.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { DATABASE_URL, fakeEnvironment, OPTIONS_TEXT } from "../app/support.ts";

/** A task of the default queue. */
export const report = defineTask({ name: "report", input: { format: h.string() }, output: h.null() });

/** A task with no parameters. */
export const ping = defineTask({ name: "ping", input: {}, output: h.null() });

/** A task of the custom queue `standard`. */
export const sync = defineTask({ name: "sync", queue: "standard", input: { region: h.string() }, output: h.null() });

/** The upstream task of the parameter of `upstreamOnly`. */
const intSource = defineTask({ name: "int_source", input: {}, output: h.int() });

/** A task that only a workflow node can run. */
export const upstreamOnly = defineTask({ name: "upstream_only", input: { upstream: h.taskOutcome(intSource) }, output: h.null() });

/** A task that the apps do not list. */
export const unlisted = defineTask({ name: "unlisted", input: {}, output: h.null() });

/** A pattern of every minute at second 0. */
export const HOURLY = { kind: "hourly", minute: 0 } as const;

/** The app of `config`, built with the fakes; a refused create fails the test. */
function appOf(config: HorsiesConfig): Horsies {
  const created = createHorsies(config, fakeEnvironment());

  if (!created.ok) {
    assert.fail(`the test app was refused: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
  }

  return created.value;
}

/** The app of the default queue mode with `report`, `ping` and `upstreamOnly`. */
export function defaultModeApp(): Horsies {
  return appOf({ broker: { databaseUrl: DATABASE_URL }, tasks: [report, ping, upstreamOnly] });
}

/** The app of the custom queues `standard` and `urgent` with `sync`. */
export function customModeApp(): Horsies {
  return appOf({
    broker: { databaseUrl: DATABASE_URL },
    queueMode: "custom",
    customQueues: [
      { name: "standard", priority: 50 },
      { name: "urgent", priority: 1 },
    ],
    tasks: [sync],
  });
}

/** A prepared scheduler of the fakes: its stop request does nothing. */
export const FAKE_SCHEDULER: NativePreparedScheduler = Object.freeze({
  requestStop: (): NativeEnvelope<undefined> => ({ kind: "value", value: undefined }),
});

/**
 * The zones of the fake core's zone data: names of the core's chrono-tz table, among them the four zones that the
 * zone data of Bun 1.4 does not hold (`EST5EDT`, `CST6CDT`, `MST7MDT`, `PST8PDT`).
 */
export const FAKE_CORE_ZONES: readonly string[] = [
  "UTC",
  "Europe/Berlin",
  "Asia/Calcutta",
  "US/Eastern",
  "EST5EDT",
  "CST6CDT",
  "MST7MDT",
  "PST8PDT",
];

/** The scripted answers of the fake core calls. Each answer left out accepts; the zones are `FAKE_CORE_ZONES`. */
export type CoreScript = {
  readonly pattern?: PatternCheck;
  readonly zones?: readonly string[];
  readonly rejectBound?: RejectBound;
  readonly retainRerunInput?: boolean;
  readonly preparation?: SchedulerPreparation;
};

/** A fake of the core calls of a declaration, with the recorded arguments of each call. */
export type FakeCore = ScheduleCoreCalls & {
  readonly patterns: string[];
  readonly zones: string[];
  readonly queueNames: (readonly string[])[];
  readonly manifests: string[];
};

/** The fake core calls of `script`: the custom queues have the priority 50 and 1 by name, the default queue none. */
export function fakeCore(script: CoreScript = {}): FakeCore {
  const patterns: string[] = [];
  const zones: string[] = [];
  const queueNames: (readonly string[])[] = [];
  const manifests: string[] = [];
  const known = new Set(script.zones ?? FAKE_CORE_ZONES);

  return {
    patterns,
    zones,
    queueNames,
    manifests,
    checkTimezone: (name) => {
      zones.push(name);

      return known.has(name) ? "known" : "unknown";
    },
    checkPattern: (text) => {
      patterns.push(text);

      return script.pattern ?? { kind: "accepted" };
    },
    configFacts: (_json, names): SchedulerConfigFacts => {
      queueNames.push(names);

      return {
        rejectBound: script.rejectBound ?? { kind: "noBound" },
        retainRerunInput: script.retainRerunInput ?? false,
        queues: names.map((queueName) => ({
          queueName,
          priority: queueName === "default" ? { kind: "defaultQueue" } : { kind: "configured", priority: queueName === "urgent" ? 1 : 50 },
          retentionClassKey: queueName === "urgent" ? "forever" : "standard_30d",
        })),
      };
    },
    optionsText: () => OPTIONS_TEXT,
    prepare: (text) => {
      manifests.push(text);

      return script.preparation ?? { kind: "prepared", scheduler: FAKE_SCHEDULER };
    },
  };
}

/** Declare `entries` and `section` on `app` with the core access `access`; give the context. */
export function declare<const T extends readonly AnyTaskContract[]>(
  app: Horsies,
  entries: ScheduleEntries<T>,
  access: ScheduleCoreAccess,
  section: ScheduleSection = {},
): AppContext {
  const context = appInternals(app);

  declareSchedules(context, declaredEntries(entries), section, access);

  return context;
}

/** The code, the path and the message of each recorded refusal of the schedules area, in order. */
export function scheduleRefusals(context: AppContext): readonly (readonly [string, string, string])[] {
  return context.refusals
    .recorded()
    .filter((recorded) => recorded.area === "schedules")
    .map((recorded) => [recorded.refusal.code, recorded.refusal.path, recorded.refusal.message] as const);
}
