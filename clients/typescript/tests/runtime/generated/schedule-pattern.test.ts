import { describe, test } from "node:test";
import assert from "node:assert/strict";

import {
  CRON_ENUM_TERM_MONTH_ENUM_RANGE_STEP_DEFAULT,
  CRON_ENUM_TERM_WEEKDAY_ENUM_RANGE_STEP_DEFAULT,
  CRON_NUMERIC_TERM_RANGE_STEP_DEFAULT,
  HOURLY_SCHEDULE_SECOND_DEFAULT,
  schedulePatternToWire,
} from "../../../horsies/src/_catalog/schedule_pattern.ts";
import type { SchedulePattern } from "../../../horsies/src/_catalog/schedule_pattern.ts";
import { wireText } from "../../unit/generated/support/wire-text.ts";

/** A pattern and the text of Python's `pattern.model_dump(mode='json')` for it. */
interface PatternCase {
  readonly name: string;
  readonly pattern: SchedulePattern;
  readonly text: string;
}

// The texts are the ten texts of `PYTHON_PATTERN_TEXTS` in
// `core/src/manifest_schema.rs`: one per pattern kind and one per day selector
// of the cron kind. Together they hold every variant of every tagged type.
const CASES: ReadonlyArray<PatternCase> = [
  {
    name: "interval",
    pattern: { type: "interval", seconds: 30, minutes: null, hours: null, days: null },
    text: String.raw`{"type":"interval","seconds":30,"minutes":null,"hours":null,"days":null}`,
  },
  {
    name: "hourly",
    pattern: { type: "hourly", minute: 15, second: HOURLY_SCHEDULE_SECOND_DEFAULT },
    text: String.raw`{"type":"hourly","minute":15,"second":0}`,
  },
  {
    name: "daily",
    pattern: { type: "daily", time: Temporal.PlainTime.from("09:00") },
    text: String.raw`{"type":"daily","time":"09:00:00"}`,
  },
  {
    name: "weekly",
    pattern: { type: "weekly", days: ["monday", "friday"], time: Temporal.PlainTime.from("09:30") },
    text: String.raw`{"type":"weekly","days":["monday","friday"],"time":"09:30:00"}`,
  },
  {
    name: "monthly",
    pattern: { type: "monthly", day: 15, time: Temporal.PlainTime.from("08:00:00") },
    text: String.raw`{"type":"monthly","day":15,"time":"08:00:00"}`,
  },
  {
    name: "cron on every day",
    pattern: {
      type: "cron",
      minute: [{ kind: "every" }],
      hour: [{ kind: "values", values: [9, 17] }],
      month: [{ kind: "every" }],
      day: { kind: "every_day" },
    },
    text: [
      String.raw`{"type":"cron","minute":[{"kind":"every"}],`,
      String.raw`"hour":[{"kind":"values","values":[9,17]}],"month":[{"kind":"every"}],`,
      String.raw`"day":{"kind":"every_day"}}`,
    ].join(""),
  },
  {
    name: "cron by month day",
    pattern: {
      type: "cron",
      minute: [{ kind: "step", step: 15 }],
      hour: [{ kind: "range", start: 9, end: 17, step: 2 }],
      month: [{ kind: "enum_values", values: ["january", "july"] }],
      day: { kind: "by_month_day", day_of_month: [{ kind: "values", values: [1, 15] }] },
    },
    text: [
      String.raw`{"type":"cron","minute":[{"kind":"step","step":15}],`,
      String.raw`"hour":[{"kind":"range","start":9,"end":17,"step":2}],`,
      String.raw`"month":[{"kind":"enum_values","values":["january","july"]}],`,
      String.raw`"day":{"kind":"by_month_day","day_of_month":[{"kind":"values","values":[1,15]}]}}`,
    ].join(""),
  },
  {
    name: "cron by weekday",
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
    text: [
      String.raw`{"type":"cron","minute":[{"kind":"values","values":[0]}],"hour":[{"kind":"every"}],`,
      String.raw`"month":[{"kind":"enum_range","start":"march","end":"june","step":1}],`,
      String.raw`"day":{"kind":"by_weekday","day_of_week":[{"kind":"every"},`,
      String.raw`{"kind":"enum_range","start":"monday","end":"friday","step":1}]}}`,
    ].join(""),
  },
  {
    name: "cron on either day",
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
    text: [
      String.raw`{"type":"cron","minute":[{"kind":"values","values":[30]}],`,
      String.raw`"hour":[{"kind":"values","values":[12]}],"month":[{"kind":"enum_step","step":3}],`,
      String.raw`"day":{"kind":"either_day","day_of_month":[{"kind":"values","values":[13]}],`,
      String.raw`"day_of_week":[{"kind":"enum_values","values":["friday"]}]}}`,
    ].join(""),
  },
  {
    name: "cron on both days",
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
    text: [
      String.raw`{"type":"cron","minute":[{"kind":"values","values":[0]}],`,
      String.raw`"hour":[{"kind":"values","values":[8]}],"month":[{"kind":"every"}],`,
      String.raw`"day":{"kind":"both_days","day_of_month":[{"kind":"range","start":1,"end":7,"step":1}],`,
      String.raw`"day_of_week":[{"kind":"enum_step","step":2}]}}`,
    ].join(""),
  },
];

void describe("schedulePatternToWire", () => {
  for (const patternCase of CASES) {
    void test(`gives Python's text of the ${patternCase.name} pattern`, () => {
      assert.equal(wireText(schedulePatternToWire(patternCase.pattern)), patternCase.text);
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
