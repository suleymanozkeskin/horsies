---
title: Schedule Config
summary: Define the schedules of a TypeScript app in the schedule section of Horsies.create with taskSchedule and the typed patterns, read the defaults and the bounds, and run the scheduler command.
related: [../configuration/app-config, ../cli, ../../scheduling/scheduler-overview, ../../scheduling/schedule-patterns]
tags: [typescript, scheduling, schedule, taskSchedule, ScheduleSection, SchedulePattern, CronSchedule, DaySelector, scheduler]
---

# Schedule Config

The `schedule` section of `Horsies.create` holds the schedules that the scheduler sends. How the scheduler fires a slot, keeps its state rows, reads the database clock and catches up is on [Scheduler Overview](/horsies/scheduling/scheduler-overview/).

## Define Schedules

<!-- ts-check: alone -->
```ts
import { cli, defineTask, h, Horsies, taskSchedule } from "horsies";

const buildReport = defineTask({ name: "build_report", input: { region: h.string() }, output: h.string() });

const cleanCache = defineTask({ name: "clean_cache", input: {}, output: h.int() });

const created = Horsies.create({
  database: { databaseUrl: { kind: "fromEnv", name: "DATABASE_URL" } },
  tasks: [buildReport, cleanCache],
  schedule: {
    schedules: [
      taskSchedule({
        name: "nightly-eu-report",
        task: buildReport,
        input: { region: "eu" },
        pattern: { kind: "daily", time: Temporal.PlainTime.from("03:00") },
        timezone: "Europe/Berlin",
      }),
      taskSchedule({
        name: "cache-cleanup",
        task: cleanCache,
        input: {},
        pattern: { kind: "interval", every: Temporal.Duration.from({ minutes: 15 }) },
        catchUpMissed: true,
        maxCatchUpRuns: 4,
      }),
    ],
  },
});

if (!created.ok) {
  process.exit(1);
}

process.exitCode = await cli.run(created.value);
```

```bash
node main.ts scheduler
```

The `scheduler` command, its flag and its exit codes are on [CLI Reference](/horsies/typescript/cli/#scheduler).

## ScheduleSection

| Field | Type | Default | Rule |
|-------|------|---------|------|
| `schedules` | `readonly TaskSchedule[]` | Required | Each entry from `taskSchedule`. Each name once |
| `enabled` | `boolean` | `true` | `false`: the `scheduler` command stops at its start |
| `checkIntervalSeconds` | `number` | 1 | A whole number, 1 to 60: the wait between two checks for due slots |

The defaults are the core's constants: `DEFAULT_SCHEDULE_ENABLED` (`true`) and `DEFAULT_SCHEDULE_CHECK_INTERVAL_SECONDS` (1). The package holds no copy of them.

## taskSchedule

`taskSchedule(options)` makes one entry. It is pure. The input type of `options.input` is the input type of `options.task`, so an input that does not fit fails typecheck at this call. A plain object is not a `TaskSchedule`: it does not compile.

| Field of `TaskScheduleOptions` | Type | Default |
|-------------------------------|------|---------|
| `name` | `string`: the key of the state row and of the task ids of the slots | Required |
| `task` | A task contract of the app's `tasks` list | Required |
| `input` | The input that `app.send` of `task` takes | Required |
| `pattern` | `SchedulePattern` | Required |
| `timezone` | `string`: an IANA zone name | `"UTC"` (`DEFAULT_SCHEDULE_TIMEZONE`) |
| `catchUpMissed` | `boolean`: send the slots that passed while no scheduler ran | `false` |
| `maxCatchUpRuns` | `number`: 1 to 10,000, the most slots that one check sends | 100 (`DEFAULT_MAX_CATCH_UP_RUNS`) |
| `queue` | `string`: a custom queue of the app | The queue of the task |
| `enabled` | `boolean` | `true`. `false`: the entry keeps its name and sends nothing |

- Every slot sends the same input.
- The scheduler only sends. A process that runs the scheduler needs no implementation of the task.
- A time of day is in the `timezone` of the entry.
- The meaning of `catchUpMissed` and `maxCatchUpRuns` is on [Scheduler Overview](/horsies/scheduling/scheduler-overview/#catch-up-logic).

## SchedulePattern

A pattern is an object told apart by `kind`. No pattern is a cron text.

| `kind` | Type | Fields | Rule |
|--------|------|--------|------|
| `"interval"` | `IntervalSchedule` | `every: Temporal.Duration` | Days, hours, minutes and seconds only. In whole seconds, 1 second to 365 days (31,536,000 s) |
| `"hourly"` | `HourlySchedule` | `minute: number`, `second?: number` | Each 0 to 59. `second` left out: 0 |
| `"daily"` | `DailySchedule` | `time: Temporal.PlainTime` | A whole second |
| `"weekly"` | `WeeklySchedule` | `weekdays`: a non-empty array of `Weekday`, `time: Temporal.PlainTime` | Each day once; `time` a whole second |
| `"monthly"` | `MonthlySchedule` | `dayOfMonth: number`, `time: Temporal.PlainTime` | `dayOfMonth` 1 to 31. A month with no such day has no slot |
| `"cron"` | `CronSchedule` | `minute`, `hour`, `month`, `day` | See [Typed Cron](#typed-cron) |

- `Weekday` is `"monday"` to `"sunday"`. `Month` is `"january"` to `"december"`.
- An interval with years, months or weeks is refused: these parts have no fixed length. A part below a second is refused: slots fire at whole seconds.
- A number field must be a safe integer (a whole number from -(2^53 - 1) to 2^53 - 1). The bounds of each field are the core's rules.

<!-- ts-check: alone -->
```ts
import type { SchedulePattern } from "horsies";

export const everyNinetySeconds: SchedulePattern = { kind: "interval", every: Temporal.Duration.from({ seconds: 90 }) };

export const quarterPast: SchedulePattern = { kind: "hourly", minute: 15 };

export const mondayAndFriday: SchedulePattern = {
  kind: "weekly",
  weekdays: ["monday", "friday"],
  time: Temporal.PlainTime.from("09:00"),
};

export const monthStart: SchedulePattern = { kind: "monthly", dayOfMonth: 1, time: Temporal.PlainTime.from("05:00") };
```

### Typed Cron

`CronSchedule` holds what a `minute hour day-of-month month day-of-week` crontab line holds, in typed fields. It fires at second 0 of each matched minute.

<!-- ts-check: alone -->
```ts
import type { CronSchedule } from "horsies";

export const weekdaysAtNineThirty: CronSchedule = {
  kind: "cron",
  minute: [{ kind: "values", values: [30] }],
  hour: [{ kind: "values", values: [9] }],
  month: [{ kind: "every" }],
  day: { kind: "byWeekday", dayOfWeek: [{ kind: "enumRange", start: "monday", end: "friday" }] },
};

export const firstOfQuarterEverySixHours: CronSchedule = {
  kind: "cron",
  minute: [{ kind: "values", values: [0] }],
  hour: [{ kind: "step", step: 6 }],
  month: [{ kind: "enumValues", values: ["january", "april", "july", "october"] }],
  day: { kind: "byMonthDay", dayOfMonth: [{ kind: "values", values: [1] }] },
};
```

| Field | Type | Domain |
|-------|------|--------|
| `minute` | A non-empty array of `CronNumericTerm` | 0 to 59 |
| `hour` | A non-empty array of `CronNumericTerm` | 0 to 23 |
| `month` | A non-empty array of `CronMonthTerm` (`CronEnumTerm<Month>`) | `"january"` (1) to `"december"` (12) |
| `day` | `DaySelector` | Day of month 1 to 31; day of week `"monday"` (0) to `"sunday"` (6) |

The terms of one field match the union of their values.

| `CronNumericTerm` | Crontab form |
|-------------------|--------------|
| `{ kind: "every" }` | `*` |
| `{ kind: "step", step }` | `*/n`, from the start of the domain |
| `{ kind: "values", values }` | `a,b,c`: a non-empty array |
| `{ kind: "range", start, end, step? }` | `a-b` or `a-b/n`; `step` left out: 1 |

| `CronEnumTerm<T>` | Crontab form |
|-------------------|--------------|
| `{ kind: "every" }` | `*` |
| `{ kind: "enumValues", values }` | `a,b,c`: a non-empty array |
| `{ kind: "enumRange", start, end, step? }` | `a-b` or `a-b/n`, in calendar order, with no wrap-around; `step` left out: 1 |
| `{ kind: "enumStep", step }` | `*/n`, by ordinal |

`DaySelector` makes the choice between the day of the month and the day of the week explicit:

| `kind` | Fields | Fires on |
|--------|--------|----------|
| `"everyDay"` | none | Every day |
| `"byMonthDay"` | `dayOfMonth` | The days of the month |
| `"byWeekday"` | `dayOfWeek` | The days of the week |
| `"eitherDay"` | `dayOfMonth`, `dayOfWeek` | A day that matches one of the two |
| `"bothDays"` | `dayOfMonth`, `dayOfWeek` | A day that matches both |

The cron bounds:

| Rule | Bound |
|------|-------|
| A value, and the `start` and `end` of a range | Inside the domain of its field |
| A range | `start` at most `end` |
| A step of a numeric field | 1 to the span of its field: minute 59, hour 23, day of month 30 |
| A step of a named field | 1 to the span of its field: month 11, day of week 6 |
| `month` and `day` | A combination that no date has (`"february"` with day of month 30 only) is refused |

## Checks

`Horsies.create` records the section once. A refusal of the section or of an entry does not refuse `Horsies.create`. It goes to the report of `check` and of the scheduler start. The worker start skips it.

The check of each entry runs in config order. The first refusal of an entry is its report line:

| Check | Entries | Code |
|-------|---------|------|
| The pattern has a stored form and keeps the core's rules | Every entry | `HRS-205` |
| `maxCatchUpRuns` is a whole number from 1 to 10,000 | Every entry | `HRS-205` |
| No earlier entry has the name | Every entry | `HRS-205` |
| The core knows the time zone | Enabled entries | `HRS-205` |
| The contract is in the app's `tasks` list | Enabled entries | `HRS-300` |
| `queue` is a custom queue of the app. In the default queue mode, an entry names no queue | Enabled entries | `HRS-205` |
| The task takes no upstream result: only a workflow node runs such a task | Enabled entries | `HRS-205` |
| The input encodes and reads back with the input types of the task | Enabled entries | `HRS-205` |
| The input is at most the payload reject bound (`payload.rejectBytes`) | Enabled entries | `HRS-205` |

A `checkIntervalSeconds` outside 1 to 60 is refused with `HRS-205` at the path `schedule section`. The check needs no database.

```text
error[HRS-205]: schedules[1] cache-cleanup: name: schedules[0] has the name "cache-cleanup"
  = help: give each schedule its own name
```

## Run the Scheduler

`<entry> scheduler` runs the scheduler through `cli.run(app)`:

1. The static check of the app runs, with the schedules. A refusal writes the report on stderr and exits with code 1. Nothing starts.
2. The scheduler connects, installs the schema once, and starts the state of each schedule.
3. It checks the schedules at once, then every `checkIntervalSeconds`, until `SIGTERM` or `SIGINT`. A failed check is logged, and the next check runs.

The command exits with code 1 and writes the cause on stderr when:

| Cause | Nothing started |
|-------|-----------------|
| The config has no `schedule` section | Yes |
| `schedule.enabled` is `false` | Yes |
| The check refused the app or a schedule | Yes |
| The start refused a schedule before any state write: a time zone that the core does not know, a send that the rules refuse, or a pattern with no slot after the database time | No state was written |
| The connect, the schema install or the client family check failed | No state was written |
| The core of this process refused the call | Yes |

- Sends stay suppressed in this process until the scheduler starts to serve: a send at module level writes nothing.
- Several schedulers can run against one database. Each fire locks the state row of its schedule, so a second scheduler writes nothing for a slot that the first one fired.
- The state rows are on `app.monitoring.listSchedules()`. See [Monitoring Queries](/horsies/typescript/monitoring/monitoring-queries/).

## Things to Avoid

**Do not rename a task parameter that a schedule's `input` names.** The parameter name is the stored key. A slot that the scheduler already sent then fails with `WORKER_SERIALIZATION_ERROR`.

**Do not give an entry a UTC offset as its `timezone`.** The core's zone data holds IANA zone names. The check refuses `"+05:00"`: write `"Asia/Karachi"`, so the slots follow the rules of the zone.
