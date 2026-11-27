// The `schedule` section of the app config and its entries, with the defaults of the fields that they leave out. A
// schedule refers to a task contract, so `taskSchedule` checks its input against the contract's input type at
// typecheck: the entry type `TaskSchedule` comes only from `taskSchedule`.

import type { Execution } from "../_catalog/monitoring_models.ts";
import type { AnyTaskContract, TaskContract } from "../contract/define-task.ts";
import type { OutputSchema, SendableInput } from "../contract/schema.ts";
import type { SchedulePattern } from "./pattern.ts";

declare const taskScheduleBrand: unique symbol;

/** The input of each slot of a schedule of the contract `C`: the input that `app.send` of `C` takes. */
export type ScheduleInput<C> = C extends TaskContract<infer P, OutputSchema, string, Execution> ? SendableInput<P> : never;

/** The fields of one schedule of the contract `C`, which `taskSchedule` takes: each slot of `pattern` sends `task` with `input`. */
export interface TaskScheduleOptions<C> {
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

/**
 * One schedule of the `schedule` section: a contract, and the input as the object that it is. Only `taskSchedule`
 * makes it, so the input of each entry was checked against its own task contract at typecheck.
 */
export type TaskSchedule = Omit<TaskScheduleOptions<AnyTaskContract>, "task" | "input"> & {
  readonly task: AnyTaskContract;
  readonly input: object;
  readonly [taskScheduleBrand]: "TaskSchedule";
};

/** One entry as the declaration reads it: an entry with no brand, as a caller with no type check writes it. */
export type DeclaredEntry = Omit<TaskSchedule, typeof taskScheduleBrand>;

/**
 * The schedule of `options`, for the `schedules` list of the `schedule` section. The input type is the input type of
 * `options.task`; a wrong input fails typecheck at this call. `check` and the scheduler start check the entry again at
 * run time. Pure.
 */
export function taskSchedule<C extends AnyTaskContract>(options: TaskScheduleOptions<C>): TaskSchedule {
  const entry: DeclaredEntry = { ...options, task: options.task, input: Object(options.input) };

  // SAFETY: the brand is the fact that the input type of the entry was the input type of its task, checked by the
  // parameter type of this function.
  return Object.freeze(entry) as TaskSchedule;
}

/** The `schedule` section of the app config: the switches of the scheduler and its schedules. */
export interface ScheduleSection {
  /** The master switch: off, the scheduler command refuses to start. Left out: `true`. */
  readonly enabled?: boolean;
  /** The schedules of the app, in order; each one made by `taskSchedule`. */
  readonly schedules: readonly TaskSchedule[];
  /** The wait between two checks for due slots, in whole seconds, 1 to 60. Left out: 1. */
  readonly checkIntervalSeconds?: number;
}

// The defaults of a schedule and of its zone and catch-up bound are the core's, in `_catalog/schedule_defaults.ts`.
// The core exports no constant for the check interval; its default is the core's `default_check_interval`
// (`core/src/core/config/schedule.rs`), the same as Python's `ScheduleConfig`.

/** The wait between two checks of a section that names none, in seconds. */
export const DEFAULT_CHECK_INTERVAL_SECONDS = 1;
