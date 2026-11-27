// The schedule patterns of the `schedule` section: typed patterns told apart by `kind`, with no cron strings (the
// model of `SchedulePattern`, `CronSchedule` and `DaySelector` of the Python and Rust clients), and their stored text.
//
// - The stored text is the core's pattern text, the form of Python's `pattern.model_dump(mode="json")`. The generated
//   `schedulePatternToWire` gives the wire value and the manifest text writer writes it.
// - This module refuses only what the stored form cannot hold: a duration part with no fixed length (years, months,
//   weeks), a duration part below a second, and a number that is not a safe integer (a whole number from
//   -(2^53 - 1) to 2^53 - 1, the integers that a JavaScript number holds exactly). Every other rule of a pattern
//   (the bounds, a repeated day, the cron domains, a cron pattern that never fires, a time of day with a fraction of
//   a second) is the core's rule (`SchedulePattern::validate`): the declaration asks the core on a main thread, and the
//   core's manifest parser applies it again at the scheduler start.

import {
  CRON_ENUM_TERM_MONTH_ENUM_RANGE_STEP_DEFAULT,
  CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_STEP_DEFAULT,
  CRON_NUMERIC_TERM_RANGE_STEP_DEFAULT,
  type CronNumericTerm as StoredNumericTerm,
  type DaySelector as StoredDaySelector,
  HOURLY_SCHEDULE_SECOND_DEFAULT,
  type Month,
  type SchedulePattern as StoredPattern,
  schedulePatternToWire,
  type Weekday,
} from "../_catalog/schedule_pattern.ts";
import type { NonEmpty } from "../wire/builders.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import { manifestText } from "../worker/manifest-text.ts";

/** Every `every`: a fixed interval. The sum of the duration's days, hours, minutes and seconds. */
export interface IntervalSchedule {
  readonly kind: "interval";
  /** Days, hours, minutes and seconds only: years, months, weeks and the parts below a second are refused. */
  readonly every: Temporal.Duration;
}

/** Every hour at `minute` and `second` (0 to 59 each). */
export interface HourlySchedule {
  readonly kind: "hourly";
  readonly minute: number;
  /** Left out: 0. */
  readonly second?: number;
}

/** Every day at `time`, a time of day in whole seconds in the schedule's time zone. */
export interface DailySchedule {
  readonly kind: "daily";
  readonly time: Temporal.PlainTime;
}

/** On each of `weekdays` (each day once) at `time`. */
export interface WeeklySchedule {
  readonly kind: "weekly";
  readonly weekdays: NonEmpty<Weekday>;
  readonly time: Temporal.PlainTime;
}

/** On `dayOfMonth` of each month (1 to 31) at `time`. A month with no such day has no run. */
export interface MonthlySchedule {
  readonly kind: "monthly";
  readonly dayOfMonth: number;
  readonly time: Temporal.PlainTime;
}

/**
 * One term of a numeric cron field: every value, every `step`th value of the field, the listed values, or the values
 * from `start` to `end` (both included) at `step` (left out: 1).
 */
export type CronNumericTerm =
  | { readonly kind: "every" }
  | { readonly kind: "step"; readonly step: number }
  | { readonly kind: "values"; readonly values: NonEmpty<number> }
  | { readonly kind: "range"; readonly start: number; readonly end: number; readonly step?: number };

/**
 * One term of a cron field of named values (`Month`, `Weekday`), in calendar order: every value, the listed values,
 * the values from `start` to `end` (both included, no wrap-around) at `step` (left out: 1), or every `step`th value.
 */
export type CronEnumTerm<T extends Month | Weekday> =
  | { readonly kind: "every" }
  | { readonly kind: "enumValues"; readonly values: NonEmpty<T> }
  | { readonly kind: "enumRange"; readonly start: T; readonly end: T; readonly step?: number }
  | { readonly kind: "enumStep"; readonly step: number };

/** One term of the month field. */
export type CronMonthTerm = CronEnumTerm<Month>;

/** One term of the day-of-week field. */
export type CronWeekdayTerm = CronEnumTerm<Weekday>;

/**
 * The days of a cron pattern: every day; by day of month only; by day of week only; a day that matches either
 * (cron's "the 13th or a Friday"); or a day that matches both. Cron's silent "or" of two restricted day fields has
 * no form here: the choice is explicit.
 */
export type DaySelector =
  | { readonly kind: "everyDay" }
  | { readonly kind: "byMonthDay"; readonly dayOfMonth: NonEmpty<CronNumericTerm> }
  | { readonly kind: "byWeekday"; readonly dayOfWeek: NonEmpty<CronWeekdayTerm> }
  | {
      readonly kind: "eitherDay";
      readonly dayOfMonth: NonEmpty<CronNumericTerm>;
      readonly dayOfWeek: NonEmpty<CronWeekdayTerm>;
    }
  | {
      readonly kind: "bothDays";
      readonly dayOfMonth: NonEmpty<CronNumericTerm>;
      readonly dayOfWeek: NonEmpty<CronWeekdayTerm>;
    };

/**
 * The typed form of a cron line, at second 0 of each matched minute: the terms of each field match the union of their
 * values (minute 0 to 59, hour 0 to 23).
 */
export interface CronSchedule {
  readonly kind: "cron";
  readonly minute: NonEmpty<CronNumericTerm>;
  readonly hour: NonEmpty<CronNumericTerm>;
  readonly month: NonEmpty<CronMonthTerm>;
  readonly day: DaySelector;
}

/** When a schedule fires. */
export type SchedulePattern =
  | IntervalSchedule
  | HourlySchedule
  | DailySchedule
  | WeeklySchedule
  | MonthlySchedule
  | CronSchedule;

/** The parts of a duration that have no fixed length. */
type UnitWithoutFixedLength = "years" | "months" | "weeks";

/** The parts of a duration below a second. */
type UnitBelowSecond = "milliseconds" | "microseconds" | "nanoseconds";

const UNITS_WITHOUT_FIXED_LENGTH: readonly UnitWithoutFixedLength[] = ["years", "months", "weeks"];

const UNITS_BELOW_SECOND: readonly UnitBelowSecond[] = ["milliseconds", "microseconds", "nanoseconds"];

/**
 * Why a pattern has no stored form. `field` is the place in the pattern (`minute[0].values[1]`).
 * - `unitWithoutFixedLength`: the interval has years, months or weeks.
 * - `unitBelowSecond`: the interval has a part below a second; slots are whole seconds.
 * - `notASafeInteger`: a number field holds a value that is not a whole number from -(2^53 - 1) to 2^53 - 1.
 */
export type PatternRefusal =
  | { readonly kind: "unitWithoutFixedLength"; readonly unit: UnitWithoutFixedLength }
  | { readonly kind: "unitBelowSecond"; readonly unit: UnitBelowSecond }
  | { readonly kind: "notASafeInteger"; readonly field: string; readonly value: number };

type Stored<T> = Result<T, PatternRefusal>;

/** The stored text of `pattern`, or why the stored form cannot hold it. Pure. */
export function patternText(pattern: SchedulePattern): Result<string, PatternRefusal> {
  const stored = storedPattern(pattern);

  return stored.ok ? ok(manifestText(schedulePatternToWire(stored.value))) : stored;
}

/** The text of a pattern refusal, with its field. Pure. */
export function patternRefusalText(refused: PatternRefusal): string {
  switch (refused.kind) {
    case "unitWithoutFixedLength":
      return `pattern.every: the duration has ${refused.unit}, which have no fixed length; give the interval in days, hours, minutes and seconds`;
    case "unitBelowSecond":
      return `pattern.every: the duration has ${refused.unit}; schedules fire at whole seconds`;
    case "notASafeInteger":
      return `pattern.${refused.field}: ${safeIntegerRule(refused.value)}`;
    default:
      return unreachable(refused, "patternRefusalText");
  }
}

/** The stored form of `pattern`, the generated pattern type. Pure. */
export function storedPattern(pattern: SchedulePattern): Stored<StoredPattern> {
  switch (pattern.kind) {
    case "interval":
      return storedInterval(pattern.every);
    case "hourly":
      return storedHourly(pattern);
    case "daily":
      return ok({ type: "daily", time: pattern.time });
    case "weekly":
      return ok({ type: "weekly", days: [...pattern.weekdays], time: pattern.time });
    case "monthly": {
      const day = wholeNumber(pattern.dayOfMonth, "dayOfMonth");

      return day.ok ? ok({ type: "monthly", day: day.value, time: pattern.time }) : day;
    }

    case "cron":
      return storedCron(pattern);
    default:
      return unreachable(pattern, "storedPattern");
  }
}

/** The stored interval: each part the same part of the duration, `null` when it is 0. Pure. */
function storedInterval(every: Temporal.Duration): Stored<StoredPattern> {
  const withoutFixedLength = UNITS_WITHOUT_FIXED_LENGTH.find((unit) => every[unit] !== 0);

  if (withoutFixedLength !== undefined) {
    return err({ kind: "unitWithoutFixedLength", unit: withoutFixedLength });
  }

  const belowSecond = UNITS_BELOW_SECOND.find((unit) => every[unit] !== 0);

  if (belowSecond !== undefined) {
    return err({ kind: "unitBelowSecond", unit: belowSecond });
  }

  return ok({
    type: "interval",
    seconds: partOrNull(every.seconds),
    minutes: partOrNull(every.minutes),
    hours: partOrNull(every.hours),
    days: partOrNull(every.days),
  });
}

/** A part of a duration as the stored interval holds it: `null` for 0. Pure. */
function partOrNull(part: number): number | null {
  return part === 0 ? null : part;
}

function storedHourly(pattern: HourlySchedule): Stored<StoredPattern> {
  const minute = wholeNumber(pattern.minute, "minute");

  if (!minute.ok) {
    return minute;
  }

  const second = wholeNumber(pattern.second ?? HOURLY_SCHEDULE_SECOND_DEFAULT, "second");

  return second.ok ? ok({ type: "hourly", minute: minute.value, second: second.value }) : second;
}

function storedCron(pattern: CronSchedule): Stored<StoredPattern> {
  const minute = numericTerms(pattern.minute, "minute");

  if (!minute.ok) {
    return minute;
  }

  const hour = numericTerms(pattern.hour, "hour");

  if (!hour.ok) {
    return hour;
  }

  const month = enumTerms(pattern.month, "month", CRON_ENUM_TERM_MONTH_ENUM_RANGE_STEP_DEFAULT);

  if (!month.ok) {
    return month;
  }

  const day = storedDay(pattern.day);

  return day.ok ? ok({ type: "cron", minute: minute.value, hour: hour.value, month: month.value, day: day.value }) : day;
}

function storedDay(day: DaySelector): Stored<StoredDaySelector> {
  switch (day.kind) {
    case "everyDay":
      return ok({ kind: "every_day" });
    case "byMonthDay": {
      const dayOfMonth = numericTerms(day.dayOfMonth, "day.dayOfMonth");

      return dayOfMonth.ok ? ok({ kind: "by_month_day", day_of_month: dayOfMonth.value }) : dayOfMonth;
    }

    case "byWeekday": {
      const dayOfWeek = enumTerms(day.dayOfWeek, "day.dayOfWeek", CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_STEP_DEFAULT);

      return dayOfWeek.ok ? ok({ kind: "by_weekday", day_of_week: dayOfWeek.value }) : dayOfWeek;
    }

    case "eitherDay":
    case "bothDays": {
      const dayOfMonth = numericTerms(day.dayOfMonth, "day.dayOfMonth");

      if (!dayOfMonth.ok) {
        return dayOfMonth;
      }

      const dayOfWeek = enumTerms(day.dayOfWeek, "day.dayOfWeek", CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_STEP_DEFAULT);

      if (!dayOfWeek.ok) {
        return dayOfWeek;
      }

      const kind = day.kind === "eitherDay" ? "either_day" : "both_days";

      return ok({ kind, day_of_month: dayOfMonth.value, day_of_week: dayOfWeek.value });
    }

    default:
      return unreachable(day, "storedDay");
  }
}

/** The stored terms of a numeric field at `field`, in order. Pure. */
function numericTerms(terms: readonly CronNumericTerm[], field: string): Stored<StoredNumericTerm[]> {
  return eachStored(terms, (term, index) => numericTerm(term, `${field}[${String(index)}]`));
}

function numericTerm(term: CronNumericTerm, field: string): Stored<StoredNumericTerm> {
  switch (term.kind) {
    case "every":
      return ok({ kind: "every" });
    case "step": {
      const step = wholeNumber(term.step, `${field}.step`);

      return step.ok ? ok({ kind: "step", step: step.value }) : step;
    }

    case "values": {
      const values = eachStored(term.values, (value, index) => wholeNumber(value, `${field}.values[${String(index)}]`));

      return values.ok ? ok({ kind: "values", values: values.value }) : values;
    }

    case "range":
      return numericRange(term.start, term.end, term.step ?? CRON_NUMERIC_TERM_RANGE_STEP_DEFAULT, field);
    default:
      return unreachable(term, "numericTerm");
  }
}

function numericRange(start: number, end: number, step: number, field: string): Stored<StoredNumericTerm> {
  const checked = eachStored(
    [
      [start, "start"],
      [end, "end"],
      [step, "step"],
    ] as const,
    ([value, member]) => wholeNumber(value, `${field}.${member}`),
  );

  return checked.ok ? ok({ kind: "range", start, end, step }) : checked;
}

/** The stored form of one term of a field of named values. */
type StoredEnumTerm<T> =
  | { readonly kind: "every" }
  | { readonly kind: "enum_values"; readonly values: readonly T[] }
  | { readonly kind: "enum_range"; readonly start: T; readonly end: T; readonly step: number }
  | { readonly kind: "enum_step"; readonly step: number };

/** The stored terms of a field of named values at `field`; a range with no step takes `rangeStep`. Pure. */
function enumTerms<T extends Month | Weekday>(
  terms: readonly CronEnumTerm<T>[],
  field: string,
  rangeStep: number,
): Stored<StoredEnumTerm<T>[]> {
  return eachStored(terms, (term, index) => enumTerm(term, `${field}[${String(index)}]`, rangeStep));
}

function enumTerm<T extends Month | Weekday>(term: CronEnumTerm<T>, field: string, rangeStep: number): Stored<StoredEnumTerm<T>> {
  switch (term.kind) {
    case "every":
      return ok({ kind: "every" });
    case "enumValues":
      return ok({ kind: "enum_values", values: [...term.values] });
    case "enumRange": {
      const step = wholeNumber(term.step ?? rangeStep, `${field}.step`);

      return step.ok ? ok({ kind: "enum_range", start: term.start, end: term.end, step: step.value }) : step;
    }

    case "enumStep": {
      const step = wholeNumber(term.step, `${field}.step`);

      return step.ok ? ok({ kind: "enum_step", step: step.value }) : step;
    }

    default:
      return unreachable(term, "enumTerm");
  }
}

/** `value` when it is a safe integer, else the refusal of `field`. Pure. */
function wholeNumber(value: number, field: string): Stored<number> {
  return Number.isSafeInteger(value) ? ok(value) : err({ kind: "notASafeInteger", field, value });
}

/** The rule that `value` breaks: a whole number from `Number.MIN_SAFE_INTEGER` to `Number.MAX_SAFE_INTEGER`. Pure. */
export function safeIntegerRule(value: number): string {
  return `${String(value)} is not a whole number from ${String(Number.MIN_SAFE_INTEGER)} to ${String(Number.MAX_SAFE_INTEGER)}`;
}

/** The stored form of each item, in order, or the refusal of the first item that has none. Pure. */
function eachStored<T, S>(items: readonly T[], store: (item: T, index: number) => Stored<S>): Stored<S[]> {
  const stored: S[] = [];

  for (const [index, item] of items.entries()) {
    const one = store(item, index);

    if (!one.ok) {
      return one;
    }

    stored.push(one.value);
  }

  return ok(stored);
}
