// The functions of the `scheduler` area of the addon through the call wrapper: a throw or a rejection of a call is a
// defect under the defect rule, and a thrown envelope is its error.

import { nativeValue, nativeValueAsync } from "../call/envelope.ts";
import type { CoreHandle } from "../handle/core-handle.ts";
import { schedulerAddon } from "./addon.ts";
import type {
  NativePreparedScheduler,
  PatternCheck,
  SchedulerConfigFacts,
  SchedulerPreparation,
  SchedulerRunEnded,
  TimezoneCheck,
} from "./addon.ts";

/** The core's check of the pattern text `patternJson`: the rule of the scheduler manifest's pattern. Pure. */
export function checkSchedulePattern(patternJson: string): PatternCheck {
  return nativeValue("checkSchedulePattern", () => schedulerAddon().checkSchedulePattern(patternJson));
}

/** Whether the core's zone data holds the zone `name`: the rule of the scheduler start. Pure. */
export function checkScheduleTimezone(name: string): TimezoneCheck {
  return nativeValue("checkScheduleTimezone", () => schedulerAddon().checkScheduleTimezone(name));
}

/**
 * The facts of the accepted app config text `appConfigJson` for the queues `queueNames`. Pure: starts no core.
 * Throws `ClientDefect` for a config text that the core does not read, and for a queue that the custom queue mode
 * does not hold.
 */
export function schedulerConfigFacts(appConfigJson: string, queueNames: readonly string[]): SchedulerConfigFacts {
  return nativeValue("schedulerConfigFacts", () => schedulerAddon().schedulerConfigFacts(appConfigJson, [...queueNames]));
}

/**
 * The scheduler of the scheduler manifest text `manifestJson`, or the refusal of the core's parser. Pure: starts no
 * core; the run refuses a forked or stopped core.
 */
export function prepareScheduler(manifestJson: string): SchedulerPreparation {
  return nativeValue("prepareScheduler", () => schedulerAddon().prepareScheduler(manifestJson));
}

/**
 * Run `scheduler` on the broker of `handle` until a stop request, or until its start is refused or fails. Starts the
 * core of the process. A second run of one scheduler throws `ClientDefect`.
 */
export async function runScheduler(scheduler: NativePreparedScheduler, handle: CoreHandle): Promise<SchedulerRunEnded> {
  return nativeValueAsync("runScheduler", () => schedulerAddon().runScheduler(scheduler, handle.native));
}

/** Ask the run of `scheduler` to stop. Idempotent; returns at once. */
export function requestSchedulerStop(scheduler: NativePreparedScheduler): void {
  nativeValue("requestStop", () => scheduler.requestStop());
}
