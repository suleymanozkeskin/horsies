import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  CRON_ENUM_TERM_MONTH_ENUM_RANGE_STEP_DEFAULT,
  CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_STEP_DEFAULT,
  CRON_NUMERIC_TERM_RANGE_STEP_DEFAULT,
  HOURLY_SCHEDULE_SECOND_DEFAULT,
  schedulePatternToWire,
} from "../../../horsies/src/_catalog/schedule_pattern.ts";
import type { SchedulePattern } from "../../../horsies/src/_catalog/schedule_pattern.ts";
import { wireText } from "../../unit/generated/support/wire-text.ts";

/** A pattern, by the name of its row in the pattern texts fixture. */
interface PatternCase {
  readonly name: string;
  readonly pattern: SchedulePattern;
}

/** One row of the fixture: a name and the text of Python's `pattern.model_dump(mode='json')`. */
interface PatternRow {
  readonly name: string;
  readonly text: string;
}

/** The fixture file. */
interface PatternTextsFile {
  readonly patterns: ReadonlyArray<PatternRow>;
}

/**
 * The schedule pattern texts that the core and the Python client test too: one per pattern kind
 * and one per day selector of the cron kind. Together they hold every variant of every tagged type.
 */
const PATTERN_TEXTS_URL = new URL(
  "../../../../../core/tests/fixtures/schedule_pattern/pattern_texts.json",
  import.meta.url,
);

const patternTexts: PatternTextsFile = JSON.parse(readFileSync(PATTERN_TEXTS_URL, "utf8"));

/** Python's text of the pattern `name` in the fixture. */
function fixtureText(name: string): string {
  const row = patternTexts.patterns.find((candidate) => candidate.name === name);

  assert(row !== undefined, `the fixture has the pattern ${name}`);

  return row.text;
}

const CASES: ReadonlyArray<PatternCase> = [
  {
    name: "interval",
    pattern: { type: "interval", seconds: 30, minutes: null, hours: null, days: null },
  },
  {
    name: "hourly",
    pattern: { type: "hourly", minute: 15, second: HOURLY_SCHEDULE_SECOND_DEFAULT },
  },
  {
    name: "daily",
    pattern: { type: "daily", time: Temporal.PlainTime.from("09:00") },
  },
  {
    name: "weekly",
    pattern: { type: "weekly", days: ["monday", "friday"], time: Temporal.PlainTime.from("09:30") },
  },
  {
    name: "monthly",
    pattern: { type: "monthly", day: 15, time: Temporal.PlainTime.from("08:00:00") },
  },
  {
    name: "cron_every_day",
    pattern: {
      type: "cron",
      minute: [{ kind: "every" }],
      hour: [{ kind: "values", values: [9, 17] }],
      month: [{ kind: "every" }],
      day: { kind: "every_day" },
    },
  },
  {
    name: "cron_by_month_day",
    pattern: {
      type: "cron",
      minute: [{ kind: "step", step: 15 }],
      hour: [{ kind: "range", start: 9, end: 17, step: 2 }],
      month: [{ kind: "enum_values", values: ["january", "july"] }],
      day: { kind: "by_month_day", day_of_month: [{ kind: "values", values: [1, 15] }] },
    },
  },
  {
    name: "cron_by_weekday",
    pattern: {
      type: "cron",
      minute: [{ kind: "values", values: [0] }],
      hour: [{ kind: "every" }],
      month: [
        {
          kind: "enum_range",
          start: "march",
          end: "june",
          step: CRON_ENUM_TERM_MONTH_ENUM_RANGE_STEP_DEFAULT,
        },
      ],
      day: {
        kind: "by_weekday",
        day_of_week: [
          { kind: "every" },
          {
            kind: "enum_range",
            start: "monday",
            end: "friday",
            step: CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_STEP_DEFAULT,
          },
        ],
      },
    },
  },
  {
    name: "cron_either_day",
    pattern: {
      type: "cron",
      minute: [{ kind: "values", values: [30] }],
      hour: [{ kind: "values", values: [12] }],
      month: [{ kind: "enum_step", step: 3 }],
      day: {
        kind: "either_day",
        day_of_month: [{ kind: "values", values: [13] }],
        day_of_week: [{ kind: "enum_values", values: ["friday"] }],
      },
    },
  },
  {
    name: "cron_both_days",
    pattern: {
      type: "cron",
      minute: [{ kind: "values", values: [0] }],
      hour: [{ kind: "values", values: [8] }],
      month: [{ kind: "every" }],
      day: {
        kind: "both_days",
        day_of_month: [
          { kind: "range", start: 1, end: 7, step: CRON_NUMERIC_TERM_RANGE_STEP_DEFAULT },
        ],
        day_of_week: [{ kind: "enum_step", step: 2 }],
      },
    },
  },
];

void describe("schedulePatternToWire", () => {
  void test("has a pattern for each row of the fixture, in its order", () => {
    assert.deepEqual(
      CASES.map((patternCase) => patternCase.name),
      patternTexts.patterns.map((row) => row.name),
    );
  });

  for (const patternCase of CASES) {
    void test(`gives Python's text of the ${patternCase.name} pattern`, () => {
      assert.equal(wireText(schedulePatternToWire(patternCase.pattern)), fixtureText(patternCase.name));
    });
  }

  void test("keeps the fraction of a second of a time of day, which the core refuses", () => {
    const pattern: SchedulePattern = {
      type: "daily",
      time: Temporal.PlainTime.from("09:30:00.5"),
    };

    assert.equal(
      wireText(schedulePatternToWire(pattern)),
      String.raw`{"type":"daily","time":"09:30:00.5"}`,
    );
  });

  void test("throws a defect for a number that is not a safe integer, with its field", () => {
    const pattern: SchedulePattern = {
      type: "interval",
      seconds: 1.5,
      minutes: null,
      hours: null,
      days: null,
    };

    assert.throws(() => schedulePatternToWire(pattern), {
      message: "integerToWire: IntervalSchedule.seconds is 1.5, not a safe integer",
    });
  });
});
