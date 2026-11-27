// `app.schedules`: the declaration of the schedules of an app. It records the list and returns nothing. Each refusal
// goes to the schedules area of the refusal sink, which `check` and the scheduler start read; the worker start skips
// it. A second call is a refusal, and the first declaration stays.
//
// The section's check interval and each schedule's catch-up count and name are checked here, at their own field, in
// every thread. On a main thread the declaration also runs the core's checks, with no I/O and with no throw: the rules
// of each pattern, the zone data, the payload bound of each input, and the core's manifest parser over the whole
// declaration (the backstop of the field checks), which gives the prepared scheduler that the `scheduler` command
// runs. In a worker thread only the checks that are TypeScript code run, and no addon loads: no scheduler and no
// `check` runs there.

import { DEFAULT_QUEUE_ROUTE } from "../app/contract-list.ts";
import type { AppContext } from "../app/context.ts";
import { taskOptionsFacts } from "../app/core-checks.ts";
import { refusal, type HorsiesError } from "../app/refusal.ts";
import { ErrorCode } from "../_catalog/error_codes.ts";
import { CHECK_INTERVAL_SECONDS_MAX, CHECK_INTERVAL_SECONDS_MIN } from "../_catalog/scheduler_manifest.ts";
import type { QueueSetup } from "../config/queues.ts";
import { raiseDefect } from "../native/call/defect.ts";
import type {
  NativePreparedScheduler,
  PatternCheck,
  SchedulerConfigFacts,
  SchedulerPreparation,
  TimezoneCheck,
} from "../native/scheduler/addon.ts";
import {
  checkSchedulePattern,
  checkScheduleTimezone,
  prepareScheduler,
  schedulerConfigFacts,
} from "../native/scheduler/scheduler.ts";
import { checkTaskOptions, storedOptionsText } from "../native/sends/sends.ts";
import type { ListedTask } from "../send/send.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { boundedCount, checkSchedule, type CoreRules, type NameUse, scheduleRefusal } from "./checks.ts";
import { DEFAULT_CHECK_INTERVAL_SECONDS, type DeclaredEntry, type ScheduleSection } from "./entry.ts";
import { type SectionSettings, schedulerManifestText } from "./manifest.ts";

/** The core calls of a declaration on a main thread. The addon gives them; a test gives fakes. Each is pure. */
export interface ScheduleCoreCalls {
  checkPattern(patternText: string): PatternCheck;
  checkTimezone(name: string): TimezoneCheck;
  configFacts(appConfigJson: string, queueNames: readonly string[]): SchedulerConfigFacts;
  /** The stored options text of a send of a listed task with no deadline. */
  optionsText(task: ListedTask): string;
  prepare(manifestText: string): SchedulerPreparation;
}

/** Where a declaration runs the core's checks: on a main thread with the core, or nowhere (a worker thread). */
export type ScheduleCoreAccess = { readonly kind: "core"; readonly calls: ScheduleCoreCalls } | { readonly kind: "noCore" };

/**
 * The scheduler of a declaration: prepared by the core on a main thread; refused (each refusal is in the sink); or
 * not prepared, in a worker thread.
 */
export type DeclaredScheduler =
  | { readonly kind: "prepared"; readonly scheduler: NativePreparedScheduler }
  | { readonly kind: "refused" }
  | { readonly kind: "notPrepared" };

/** The schedules of an app: none declared, or the declaration of the first `app.schedules` call. */
export type ScheduleDeclaration =
  | { readonly kind: "notDeclared" }
  | { readonly kind: "declared"; readonly section: SectionSettings; readonly scheduler: DeclaredScheduler };

const NOT_DECLARED: ScheduleDeclaration = Object.freeze({ kind: "notDeclared" });

const declarations = new WeakMap<AppContext, ScheduleDeclaration>();

/** The schedule declaration of `context`. */
export function scheduleDeclaration(context: AppContext): ScheduleDeclaration {
  return declarations.get(context) ?? NOT_DECLARED;
}

/** The core calls of the addon. */
export const NATIVE_SCHEDULE_CALLS: ScheduleCoreCalls = Object.freeze({
  checkPattern: checkSchedulePattern,
  checkTimezone: checkScheduleTimezone,
  configFacts: schedulerConfigFacts,
  optionsText: nativeOptionsText,
  prepare: prepareScheduler,
});

/** The core access of a declaration of `context`: the addon on a main thread, none in a worker thread. */
export function scheduleCoreAccess(context: AppContext): ScheduleCoreAccess {
  switch (context.core.kind) {
    case "addon":
      return { kind: "core", calls: NATIVE_SCHEDULE_CALLS };
    case "seatRelay":
    case "noCore":
      return { kind: "noCore" };
    default:
      return unreachable(context.core, "scheduleCoreAccess");
  }
}

/** Record the schedules of `entries` and `section` for `context`, and each refusal in the sink. Calls no I/O. */
export function declareSchedules(
  context: AppContext,
  entries: readonly DeclaredEntry[],
  section: ScheduleSection,
  access: ScheduleCoreAccess,
): void {
  if (declarations.has(context)) {
    context.refusals.record(
      "schedules",
      refusal(ErrorCode.CONFIG_INVALID_SCHEDULE, "app.schedules", "app.schedules is called a second time; the schedules of the first call stay", [
        "declare every schedule of the app in one app.schedules call",
      ]),
    );

    return;
  }

  const declared = declarationOf(context, entries, section, access);

  for (const refused of declared.refusals) {
    context.refusals.record("schedules", refused);
  }

  declarations.set(context, Object.freeze({ kind: "declared", section: declared.section, scheduler: declared.scheduler }));
}

/** A declaration and its refusals, in order: the section, then each schedule, then the manifest. */
type Declared = { readonly section: SectionSettings; readonly scheduler: DeclaredScheduler; readonly refusals: readonly HorsiesError[] };

function declarationOf(
  context: AppContext,
  entries: readonly DeclaredEntry[],
  section: ScheduleSection,
  access: ScheduleCoreAccess,
): Declared {
  const settings = sectionSettings(section);
  const core = declarationCore(context, access);
  const checks = { contracts: context.send.contracts, queues: context.config.queues };
  const rules = coreRules(core);
  const checked = entries.map((entry, index) => checkSchedule(checks, entry, { index, nameUse: nameUse(entries, entry.name, index) }, rules));
  const refusals = [...(settings.ok ? [] : [settings.error]), ...checked.flatMap((one) => (one.ok ? [] : [one.error]))];
  const kept = settings.ok ? settings.value : defaultSettings(section);

  if (refusals.length > 0) {
    return { section: kept, scheduler: { kind: "refused" }, refusals };
  }

  switch (core.kind) {
    case "noCore":
      return { section: kept, scheduler: { kind: "notPrepared" }, refusals };
    case "core": {
      const schedules = checked.flatMap((one) => (one.ok ? [one.value] : []));
      const text = schedulerManifestText(kept, schedules, { config: core.facts, optionsText: (task) => core.calls.optionsText(task) });

      return preparedDeclaration(kept, core.calls.prepare(text));
    }

    default:
      return unreachable(core, "declarationOf");
  }
}

/** The core of a declaration: its calls and the facts of the accepted config, or none in a worker thread. */
type DeclarationCore =
  | { readonly kind: "core"; readonly calls: ScheduleCoreCalls; readonly facts: SchedulerConfigFacts }
  | { readonly kind: "noCore" };

/** The core of a declaration of `context` through `access`. Effects: the core's pure facts call on a main thread. */
function declarationCore(context: AppContext, access: ScheduleCoreAccess): DeclarationCore {
  switch (access.kind) {
    case "core":
      return {
        kind: "core",
        calls: access.calls,
        facts: access.calls.configFacts(context.config.coreConfigJson, admittedQueues(context.config.queues)),
      };
    case "noCore":
      return access;
    default:
      return unreachable(access, "declarationCore");
  }
}

/** The core rules of the schedule checks. Pure. */
function coreRules(core: DeclarationCore): CoreRules {
  switch (core.kind) {
    case "core":
      return {
        kind: "core",
        checkPattern: (text) => core.calls.checkPattern(text),
        checkTimezone: (name) => core.calls.checkTimezone(name),
        rejectBound: core.facts.rejectBound,
      };
    case "noCore":
      return core;
    default:
      return unreachable(core, "coreRules");
  }
}

/** The declaration of a manifest that the core prepared or refused. Pure. */
function preparedDeclaration(section: SectionSettings, preparation: SchedulerPreparation): Declared {
  switch (preparation.kind) {
    case "prepared":
      return { section, scheduler: { kind: "prepared", scheduler: preparation.scheduler }, refusals: [] };
    case "manifestRefused":
      return {
        section,
        scheduler: { kind: "refused" },
        refusals: [
          scheduleRefusal(
            "schedules",
            `the core's scheduler manifest parser refuses ${preparation.fieldPath}: ${preparation.cause}`,
            "keep each value inside its bound, and give each schedule its own name",
          ),
        ],
      };
    default:
      return unreachable(preparation, "preparedDeclaration");
  }
}

/** The section with its defaults, or the refusal of a check interval outside its bound. Pure. */
function sectionSettings(section: ScheduleSection): Result<SectionSettings, HorsiesError> {
  const settings = defaultSettings(section);
  const interval = boundedCount(settings.checkIntervalSeconds, CHECK_INTERVAL_SECONDS_MIN, CHECK_INTERVAL_SECONDS_MAX);

  return interval.ok
    ? ok(settings)
    : err(scheduleRefusal("schedules section", `checkIntervalSeconds: ${interval.error.rule}`, interval.error.help));
}

/** Whether an entry before `index` has `name`, the name of the entry at `index`. Pure. */
function nameUse(entries: readonly DeclaredEntry[], name: string, index: number): NameUse {
  const firstIndex = entries.findIndex((entry) => entry.name === name);

  return firstIndex < index ? { kind: "repeats", firstIndex } : { kind: "firstUse" };
}

function defaultSettings(section: ScheduleSection): SectionSettings {
  return Object.freeze({
    enabled: section.enabled !== false,
    checkIntervalSeconds: section.checkIntervalSeconds ?? DEFAULT_CHECK_INTERVAL_SECONDS,
  });
}

/** Every queue that a schedule of the app can send on: the default queue, or each custom queue. Pure. */
function admittedQueues(queues: QueueSetup): readonly string[] {
  switch (queues.kind) {
    case "default":
      return [DEFAULT_QUEUE_ROUTE.queueName];
    case "custom":
      return [...queues.queues.keys()];
    default:
      return unreachable(queues, "admittedQueues");
  }
}

/** The stored options text of `task`, by the core's check that `Horsies.create` ran on the same facts. */
function nativeOptionsText(task: ListedTask): string {
  const checked = checkTaskOptions(taskOptionsFacts(task.compiled.record, task.route));

  switch (checked.kind) {
    case "accepted":
      return storedOptionsText(checked.options);
    case "refused":
      return raiseDefect(
        new ClientDefect({ subject: "app.schedules", rule: `the core accepts the options of ${task.compiled.record.name} again` }),
      );
    default:
      return unreachable(checked, "nativeOptionsText");
  }
}
