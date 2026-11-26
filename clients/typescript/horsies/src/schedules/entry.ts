// The entries and the section of `app.schedules`, with the defaults of the fields that they leave out. A schedule
// refers to a task contract, so its input has the contract's input type at typecheck.

import type { Execution } from "../_catalog/monitoring_models.ts";
import type { AnyTaskContract, TaskContract } from "../contract/define-task.ts";
import type { OutputSchema, SendableInput } from "../contract/schema.ts";
import type { SchedulePattern } from "./pattern.ts";

/** The input of each slot of a schedule of the contract `C`: the input that `app.send` of `C` takes. */
export type ScheduleInput<C> = C extends TaskContract<infer P, OutputSchema, string, Execution> ? SendableInput<P> : never;

/** One schedule: each slot of `pattern` sends `task` with `input`. */
export interface TaskSchedule<C> {
  /** Unique among the schedules of the app: the key of the schedule's state and of its slots' task ids. */
  readonly name: string;
  /** A contract of the app's `tasks` list. The scheduler only sends: the entry needs no implementation. */
  readonly task: C;
  /** The input of every slot, checked against the contract's input types. */
  readonly input: NoInfer<ScheduleInput<C>>;
  /** When the slots of the schedule fire. */
  readonly pattern: SchedulePattern;
  /** The IANA name of the zone of the pattern's times. Left out: `UTC`. */
  readonly timezone?: string;
  /** Send each slot that passed while no scheduler ran, at most `maxCatchUpRuns` per check. Left out: `false`. */
  readonly catchUpMissed?: boolean;
  /** The most slots that one check sends when `catchUpMissed` is on, 1 to 10,000. Left out: 100. */
  readonly maxCatchUpRuns?: number;
  /** The queue of each slot: a custom queue of the app. Left out: the task's queue. */
  readonly queue?: string;
  /** Left out: `true`. A disabled schedule sends nothing; its name stays known to the scheduler. */
  readonly enabled?: boolean;
}

/** The entries of `app.schedules`: one entry per contract of `T`, in order. */
export type ScheduleEntries<T extends readonly AnyTaskContract[]> = { readonly [K in keyof T]: TaskSchedule<T[K]> };

/** One entry as the declaration reads it: a contract, and the input as the object that it is. */
export type DeclaredEntry = Omit<TaskSchedule<AnyTaskContract>, "task" | "input"> & {
  readonly task: AnyTaskContract;
  readonly input: object;
};

/** The entries of `app.schedules` as the declaration reads them, in order. Pure. */
export function declaredEntries<T extends readonly AnyTaskContract[]>(entries: ScheduleEntries<T>): readonly DeclaredEntry[] {
  return entries.map((entry: TaskSchedule<T[number]>): DeclaredEntry => ({ ...entry, task: entry.task, input: Object(entry.input) }));
}

/** The section of `app.schedules`: the switches of the scheduler. */
export interface ScheduleSection {
  /** The master switch: off, the scheduler command refuses to start. Left out: `true`. */
  readonly enabled?: boolean;
  /** The wait between two checks for due slots, in whole seconds, 1 to 60. Left out: 1. */
  readonly checkIntervalSeconds?: number;
}

// The defaults below are the core's (`core/src/core/config/schedule.rs`: `default_timezone`,
// `default_max_catch_up_runs`, `default_check_interval`), the same as Python's `TaskSchedule` and `ScheduleConfig`.
// The generated catalog holds no constant for them yet; issue #2494 moves them to the codegen output.

/** The zone of a schedule that names none. */
export const DEFAULT_SCHEDULE_TIMEZONE = "UTC";

/** The most slots per check of a catch-up schedule that names no count. */
export const DEFAULT_MAX_CATCH_UP_RUNS = 100;

/** The wait between two checks of a section that names none, in seconds. */
export const DEFAULT_CHECK_INTERVAL_SECONDS = 1;
