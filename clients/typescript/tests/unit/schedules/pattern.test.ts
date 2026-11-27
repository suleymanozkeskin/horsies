// The schedule patterns: each kind gives Python's stored text (the rows of the core's pattern texts fixture), the
// defaults of the left-out fields, and each refusal of a form that the stored text cannot hold.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import {
  type CronSchedule,
  type PatternRefusal,
  patternRefusalText,
  patternText,
  type SchedulePattern,
} from "../../../horsies/src/schedules/pattern.ts";

/** One row of the fixture: a name and the text of Python's `pattern.model_dump(mode='json')`. */
type PatternRow = { readonly name: string; readonly text: string };

/** The schedule pattern texts that the core, the Python client and the generated writer test too. */
const PATTERN_TEXTS_URL = new URL("../../../../../core/tests/fixtures/schedule_pattern/pattern_texts.json", import.meta.url);

const fixture: { readonly patterns: readonly PatternRow[] } = JSON.parse(readFileSync(PATTERN_TEXTS_URL, "utf8"));

/** The pattern of each row of the fixture, in the TypeScript form, with the defaulted fields left out. */
const CASES: readonly (readonly [string, SchedulePattern])[] = [
  ["interval", { kind: "interval", every: Temporal.Duration.from({ seconds: 30 }) }],
  ["interval_days_and_time", { kind: "interval", every: Temporal.Duration.from({ days: 1, hours: 2, minutes: 30, seconds: 15 }) }],
  ["interval_365_days", { kind: "interval", every: Temporal.Duration.from({ days: 365 }) }],
  ["hourly", { kind: "hourly", minute: 15 }],
  ["daily", { kind: "daily", time: Temporal.PlainTime.from("09:00") }],
  ["weekly", { kind: "weekly", weekdays: ["monday", "friday"], time: Temporal.PlainTime.from("09:30") }],
  ["monthly", { kind: "monthly", dayOfMonth: 15, time: Temporal.PlainTime.from("08:00:00") }],
  [
    "cron_every_day",
    {
      kind: "cron",
      minute: [{ kind: "every" }],
      hour: [{ kind: "values", values: [9, 17] }],
      month: [{ kind: "every" }],
      day: { kind: "everyDay" },
    },
  ],
  [
    "cron_by_month_day",
    {
      kind: "cron",
      minute: [{ kind: "step", step: 15 }],
      hour: [{ kind: "range", start: 9, end: 17, step: 2 }],
      month: [{ kind: "enumValues", values: ["january", "july"] }],
      day: { kind: "byMonthDay", dayOfMonth: [{ kind: "values", values: [1, 15] }] },
    },
  ],
  [
    "cron_by_weekday",
    {
      kind: "cron",
      minute: [{ kind: "values", values: [0] }],
      hour: [{ kind: "every" }],
      month: [{ kind: "enumRange", start: "march", end: "june" }],
      day: { kind: "byWeekday", dayOfWeek: [{ kind: "every" }, { kind: "enumRange", start: "monday", end: "friday" }] },
    },
  ],
  [
    "cron_either_day",
    {
      kind: "cron",
      minute: [{ kind: "values", values: [30] }],
      hour: [{ kind: "values", values: [12] }],
      month: [{ kind: "enumStep", step: 3 }],
      day: {
        kind: "eitherDay",
        dayOfMonth: [{ kind: "values", values: [13] }],
        dayOfWeek: [{ kind: "enumValues", values: ["friday"] }],
      },
    },
  ],
  [
    "cron_both_days",
    {
      kind: "cron",
      minute: [{ kind: "values", values: [0] }],
      hour: [{ kind: "values", values: [8] }],
      month: [{ kind: "every" }],
      day: {
        kind: "bothDays",
        dayOfMonth: [{ kind: "range", start: 1, end: 7 }],
        dayOfWeek: [{ kind: "enumStep", step: 2 }],
      },
    },
  ],
];

/** The stored text of `pattern`; a refusal fails the test. */
function textOf(pattern: SchedulePattern): string {
  const text = patternText(pattern);

  assert.ok(text.ok, `the pattern has a stored text: ${text.ok ? "" : patternRefusalText(text.error)}`);

  return text.value;
}

/** The refusal of `pattern`; a stored text fails the test. */
function refusalOf(pattern: SchedulePattern): PatternRefusal {
  const text = patternText(pattern);

  assert.ok(!text.ok, "the pattern is refused");

  return text.error;
}

/** A cron pattern with `minute` and every other field `every`. */
function cronWithMinute(minute: CronSchedule["minute"]): SchedulePattern {
  return { kind: "cron", minute, hour: [{ kind: "every" }], month: [{ kind: "every" }], day: { kind: "everyDay" } };
}

describe("patternText: each kind maps to Python's stored text", () => {
  test("has a case for each row of the fixture, in its order", () => {
    assert.deepEqual(
      CASES.map(([name]) => name),
      fixture.patterns.map((row) => row.name),
    );
  });

  for (const [name, pattern] of CASES) {
    test(`the ${name} pattern`, () => {
      const row = fixture.patterns.find((candidate) => candidate.name === name);

      assert.ok(row !== undefined);
      assert.equal(textOf(pattern), row.text);
    });
  }

  test("an interval is stored as its length, the largest unit first: 90 minutes are 1 hour 30 minutes", () => {
    assert.equal(
      textOf({ kind: "interval", every: Temporal.Duration.from({ days: 1, hours: 2, minutes: 90, seconds: 5 }) }),
      '{"kind":"interval","every":"P1DT3H30M5S"}',
    );
    assert.equal(textOf({ kind: "interval", every: Temporal.Duration.from({ seconds: 90 }) }), '{"kind":"interval","every":"PT1M30S"}');
  });

  test("a time of day with a fraction of a second keeps it in the text, which the core refuses", () => {
    assert.equal(textOf({ kind: "daily", time: Temporal.PlainTime.from("09:30:00.5") }), '{"kind":"daily","time":"09:30:00.5"}');
  });

  test("a negative interval keeps its sign, which the core refuses", () => {
    assert.equal(textOf({ kind: "interval", every: Temporal.Duration.from({ minutes: -5 }) }), '{"kind":"interval","every":"-PT5M"}');
  });
});

describe("patternText: each form that the stored text cannot hold is refused", () => {
  for (const unit of ["years", "months", "weeks"] as const) {
    test(`an interval with ${unit}`, () => {
      const refused = refusalOf({ kind: "interval", every: Temporal.Duration.from({ [unit]: 1, hours: 1 }) });

      assert.deepEqual(refused, { kind: "unitWithoutFixedLength", unit });
      assert.match(patternRefusalText(refused), new RegExp(`^pattern\\.every: the duration has ${unit}, which have no fixed length`));
    });
  }

  for (const unit of ["milliseconds", "microseconds", "nanoseconds"] as const) {
    test(`an interval with ${unit}`, () => {
      const refused = refusalOf({ kind: "interval", every: Temporal.Duration.from({ seconds: 1, [unit]: 1 }) });

      assert.deepEqual(refused, { kind: "unitBelowSecond", unit });
      assert.equal(patternRefusalText(refused), `pattern.every: the duration has ${unit}; schedules fire at whole seconds`);
    });
  }

  const notWhole: readonly (readonly [string, SchedulePattern])[] = [
    ["minute", { kind: "hourly", minute: 1.5 }],
    ["second", { kind: "hourly", minute: 1, second: Number.NaN }],
    ["dayOfMonth", { kind: "monthly", dayOfMonth: 2 ** 53, time: Temporal.PlainTime.from("08:00") }],
    ["minute[0].step", cronWithMinute([{ kind: "step", step: 0.5 }])],
    ["minute[1].values[1]", cronWithMinute([{ kind: "every" }, { kind: "values", values: [1, 2.5] }])],
    ["minute[0].start", cronWithMinute([{ kind: "range", start: 0.1, end: 5 }])],
    ["minute[0].end", cronWithMinute([{ kind: "range", start: 0, end: 5.1 }])],
    ["minute[0].step", cronWithMinute([{ kind: "range", start: 0, end: 5, step: 1.1 }])],
    [
      "hour[0].values[0]",
      { kind: "cron", minute: [{ kind: "every" }], hour: [{ kind: "values", values: [Infinity] }], month: [{ kind: "every" }], day: { kind: "everyDay" } },
    ],
    [
      "month[0].step",
      { kind: "cron", minute: [{ kind: "every" }], hour: [{ kind: "every" }], month: [{ kind: "enumStep", step: 1.5 }], day: { kind: "everyDay" } },
    ],
    [
      "day.dayOfMonth[0].step",
      {
        kind: "cron",
        minute: [{ kind: "every" }],
        hour: [{ kind: "every" }],
        month: [{ kind: "every" }],
        day: { kind: "byMonthDay", dayOfMonth: [{ kind: "step", step: 2.5 }] },
      },
    ],
    [
      "day.dayOfWeek[0].step",
      {
        kind: "cron",
        minute: [{ kind: "every" }],
        hour: [{ kind: "every" }],
        month: [{ kind: "every" }],
        day: {
          kind: "bothDays",
          dayOfMonth: [{ kind: "every" }],
          dayOfWeek: [{ kind: "enumRange", start: "monday", end: "friday", step: 0.5 }],
        },
      },
    ],
  ];

  for (const [field, pattern] of notWhole) {
    test(`a number that is not a safe integer at ${field}`, () => {
      const refused = refusalOf(pattern);

      assert.equal(refused.kind, "notASafeInteger");
      assert.ok(refused.kind === "notASafeInteger" && refused.field === field, `the refusal names ${field}`);
      assert.match(
        patternRefusalText(refused),
        new RegExp(`^pattern\\.${field.replaceAll(/[.[\]]/g, "\\$&")}: .+ is not a whole number from -9007199254740991 to 9007199254740991$`),
      );
    });
  }

  test("a whole number above 2^53 - 1 names the safe integer bound", () => {
    const refused = refusalOf({ kind: "monthly", dayOfMonth: 2 ** 53, time: Temporal.PlainTime.from("08:00") });

    assert.equal(patternRefusalText(refused), "pattern.dayOfMonth: 9007199254740992 is not a whole number from -9007199254740991 to 9007199254740991");
  });

  test("the first field that is not a whole number wins", () => {
    const refused = refusalOf(cronWithMinute([{ kind: "values", values: [0.5] }, { kind: "step", step: 1.5 }]));

    assert.deepEqual(refused, { kind: "notASafeInteger", field: "minute[0].values[0]", value: 0.5 });
  });
});
