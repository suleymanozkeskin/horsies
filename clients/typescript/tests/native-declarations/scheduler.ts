// The generated declarations of the `scheduler` area match the hand-kept ones, function by function and method by
// method (arguments and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  NativePreparedScheduler,
  PatternCheck,
  QueueSendFacts,
  RejectBound,
  SchedulerAddon,
  SchedulerConfigFacts,
  SchedulerPreparation,
  SchedulerRunEnded,
  TimezoneCheck,
} from "../../horsies/src/native/scheduler/addon.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const checkSchedulePatternMatches: SameFunction<
  typeof Generated.checkSchedulePattern,
  SchedulerAddon["checkSchedulePattern"]
> = true;

export const checkScheduleTimezoneMatches: SameFunction<
  typeof Generated.checkScheduleTimezone,
  SchedulerAddon["checkScheduleTimezone"]
> = true;

export const schedulerConfigFactsMatches: SameFunction<
  typeof Generated.schedulerConfigFacts,
  SchedulerAddon["schedulerConfigFacts"]
> = true;

export const prepareSchedulerMatches: SameFunction<
  typeof Generated.prepareScheduler,
  SchedulerAddon["prepareScheduler"]
> = true;

export const runSchedulerMatches: SameFunction<typeof Generated.runScheduler, SchedulerAddon["runScheduler"]> = true;

// The class, method by method, and its member set: no method on one side only.
export const requestStopMatches: SameFunction<
  Generated.PreparedScheduler["requestStop"],
  NativePreparedScheduler["requestStop"]
> = true;

export const preparedSchedulerMatches: Mutual<Generated.PreparedScheduler, NativePreparedScheduler> = true;

export const patternCheckMatches: Mutual<Generated.PatternCheck, PatternCheck> = true;
export const timezoneCheckMatches: Mutual<Generated.TimezoneCheck, TimezoneCheck> = true;
export const rejectBoundMatches: Mutual<Generated.RejectBound, RejectBound> = true;
export const queueSendFactsMatches: Mutual<Generated.QueueSendFacts, QueueSendFacts> = true;
export const configFactsMatches: Mutual<Generated.SchedulerConfigFacts, SchedulerConfigFacts> = true;
export const preparationMatches: Mutual<Generated.SchedulerPreparation, SchedulerPreparation> = true;
export const runEndedMatches: Mutual<Generated.SchedulerRunEnded, SchedulerRunEnded> = true;
