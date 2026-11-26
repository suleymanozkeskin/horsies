// The tasks and the schedules of the scheduler instances (Rust `clients/rust/tests/worker/src/tasks/scheduled.rs` and
// `instance.rs` `schedules`, Python `clients/python/tests/e2e/tasks/scheduler.py`, `instance_scheduler.py` and
// `instance_scheduler_invalid.py`).
//
// - `scheduler`: five schedules, among them a disabled one, one with an input, one that catches up the missed slots,
//   and a cron schedule of every minute.
// - `scheduler_invalid_rules`: an interval with no time unit, and two schedules with one name. The scheduler start
//   refuses them.
// - `scheduler_invalid_task`: a schedule of a contract that the app does not list. The scheduler start refuses it.

import { defineTask, h, type Horsies, type SchedulePattern } from "../../../../horsies/src/index.ts";

/** The interval of the interval schedules of the `scheduler` instance that do not catch up, in seconds. */
export const SCHEDULE_INTERVAL_SECONDS = 2;

/** The input value of `SCHEDULE_WITH_ARGS`: its task returns twice the value. */
export const SCHEDULED_VALUE = 42;

/** The schedule names of the `scheduler` instance. */
export const SCHEDULE_SIMPLE_INTERVAL = "e2e_scheduled_simple_interval";
export const SCHEDULE_DISABLED = "e2e_disabled_schedule";
export const SCHEDULE_WITH_ARGS = "e2e_schedule_with_args";
export const SCHEDULE_CATCH_UP = "e2e_schedule_catch_up";
export const SCHEDULE_CRON_EVERY_MINUTE = "e2e_schedule_cron_every_minute";

/** The schedule names of the `scheduler_invalid_rules` instance. */
export const NO_TIME_UNIT_SCHEDULE = "e2e_interval_without_time_unit";
export const DUPLICATE_SCHEDULE = "duplicate_name";

/** The schedule name of the `scheduler_invalid_task` instance. */
export const UNLISTED_TASK_SCHEDULE = "e2e_schedule_of_an_unlisted_task";

/** The check interval of every schedule section of the suite, in seconds. */
const CHECK_INTERVAL_SECONDS = 1;

export const scheduledSimple = defineTask({ name: "e2e_scheduled_simple", input: {}, output: h.string() });

/** Twice its scheduled input. */
export const scheduledWithArgs = defineTask({ name: "e2e_scheduled_with_args", input: { value: h.int() }, output: h.int() });

export const catchUpTask = defineTask({ name: "e2e_catch_up_task", input: {}, output: h.string() });

export const cronTask = defineTask({ name: "e2e_cron_task", input: {}, output: h.string() });

/** A contract that no app of the suite lists (Python `this_task_does_not_exist`). */
export const unlistedTask = defineTask({ name: "this_task_does_not_exist", input: {}, output: h.string() });

/** Every contract of this module that the apps list. */
export const SCHEDULED_CONTRACTS = [scheduledSimple, scheduledWithArgs, catchUpTask, cronTask] as const;

/** The implementations of this module on `app`. */
export function implementScheduled(app: Horsies): void {
  app.implement(scheduledSimple, () => scheduledSimple.ok("scheduled"));
  app.implement(scheduledWithArgs, ({ value }) => scheduledWithArgs.ok(value * 2));
  app.implement(catchUpTask, () => catchUpTask.ok("caught_up"));
  app.implement(cronTask, () => cronTask.ok("cron"));
}

/** An interval of `seconds`. Pure. */
function everySeconds(seconds: number): SchedulePattern {
  return { kind: "interval", every: Temporal.Duration.from({ seconds }) };
}

/** Second 0 of every minute. */
const EVERY_MINUTE: SchedulePattern = {
  kind: "cron",
  minute: [{ kind: "every" }],
  hour: [{ kind: "every" }],
  month: [{ kind: "every" }],
  day: { kind: "everyDay" },
};

/** The schedules of the `scheduler` instance. */
export function declareSchedules(app: Horsies): void {
  app.schedules(
    [
      {
        name: SCHEDULE_SIMPLE_INTERVAL,
        task: scheduledSimple,
        input: {},
        pattern: everySeconds(SCHEDULE_INTERVAL_SECONDS),
        timezone: "UTC",
        catchUpMissed: false,
      },
      { name: SCHEDULE_DISABLED, task: scheduledSimple, input: {}, pattern: everySeconds(1), enabled: false },
      { name: SCHEDULE_WITH_ARGS, task: scheduledWithArgs, input: { value: SCHEDULED_VALUE }, pattern: everySeconds(SCHEDULE_INTERVAL_SECONDS) },
      { name: SCHEDULE_CATCH_UP, task: catchUpTask, input: {}, pattern: everySeconds(1), catchUpMissed: true },
      { name: SCHEDULE_CRON_EVERY_MINUTE, task: cronTask, input: {}, pattern: EVERY_MINUTE, timezone: "UTC", catchUpMissed: false },
    ],
    { checkIntervalSeconds: CHECK_INTERVAL_SECONDS },
  );
}

/** The schedules of the `scheduler_invalid_rules` instance: an interval of zero, and one name twice. */
export function declareInvalidRuleSchedules(app: Horsies): void {
  app.schedules(
    [
      { name: NO_TIME_UNIT_SCHEDULE, task: scheduledSimple, input: {}, pattern: everySeconds(0) },
      { name: DUPLICATE_SCHEDULE, task: scheduledSimple, input: {}, pattern: everySeconds(5) },
      { name: DUPLICATE_SCHEDULE, task: catchUpTask, input: {}, pattern: everySeconds(10) },
    ],
    { checkIntervalSeconds: CHECK_INTERVAL_SECONDS },
  );
}

/** The schedule of the `scheduler_invalid_task` instance: a contract that the app does not list. */
export function declareUnlistedTaskSchedule(app: Horsies): void {
  app.schedules([{ name: UNLISTED_TASK_SCHEDULE, task: unlistedTask, input: {}, pattern: everySeconds(5) }], {
    checkIntervalSeconds: CHECK_INTERVAL_SECONDS,
  });
}
