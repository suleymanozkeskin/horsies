// One test per outcome of the date-time kinds, and the date-time corpus against Pydantic's text.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefused, EncodeRefused } from "../../../horsies/src/codec/refusal.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { corpusLines, runtimeLabel } from "./support/files.ts";
import { sameValue } from "./support/vectors.ts";

function written<V>(wire: AnyWire, value: V): string {
  const result = encodeValue(kindOf(wire), value);

  assert(result.ok, result.ok ? "" : `refused ${result.error.kind}`);

  return result.value;
}

function writeRefused<V>(wire: AnyWire, value: V): EncodeRefused {
  const result = encodeValue(kindOf(wire), value);

  assert(!result.ok, "encoded a refused value");

  return result.error;
}

function read(wire: AnyWire, text: string): unknown {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(JSON.stringify(text)), "worker");

  assert(result.ok, result.ok ? "" : `refused ${result.error.kind}`);

  return result.value;
}

function readRefused(wire: AnyWire, text: string): DecodeRefused {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(JSON.stringify(text)), "worker");

  assert(!result.ok, "decoded a refused text");

  return result.error;
}

const instant = Temporal.Instant.from("2026-08-12T09:10:12Z");

describe("date-time writer", () => {
  test("the fraction: none for a whole second, 6 digits for a microsecond, 9 below the microsecond", () => {
    assert.equal(written(h.instant(), instant), '"2026-08-12T09:10:12Z"');
    assert.equal(written(h.instant(), instant.add({ milliseconds: 500 })), '"2026-08-12T09:10:12.500000Z"');
    assert.equal(written(h.instant(), instant.add({ microseconds: 654_321 })), '"2026-08-12T09:10:12.654321Z"');
    assert.equal(written(h.instant(), instant.add({ nanoseconds: 123_456_789 })), '"2026-08-12T09:10:12.123456789Z"');
  });

  test("an offset date-time keeps its offset; offset 0 is Z", () => {
    const east = Temporal.ZonedDateTime.from("2026-08-12T09:10:12.654321+02:00[+02:00]");
    const west = Temporal.ZonedDateTime.from("2026-08-12T09:10:12-05:30[-05:30]");
    const zero = Temporal.ZonedDateTime.from("2026-08-12T09:10:12+00:00[+00:00]");

    assert.equal(written(h.offsetDateTime(), east), '"2026-08-12T09:10:12.654321+02:00"');
    assert.equal(written(h.offsetDateTime(), west), '"2026-08-12T09:10:12-05:30"');
    assert.equal(written(h.offsetDateTime(), zero), '"2026-08-12T09:10:12Z"');
  });

  test("a date-time without an offset, and a date", () => {
    const wall = Temporal.PlainDateTime.from("2026-08-12T09:10:12.5");

    assert.equal(written(h.plainDateTime(), wall), '"2026-08-12T09:10:12.500000"');
    assert.equal(written(h.plainDate(), Temporal.PlainDate.from("0001-01-01")), '"0001-01-01"');
    assert.equal(written(h.plainDate(), Temporal.PlainDate.from("9999-12-31")), '"9999-12-31"');
  });

  test("YearOutOfRange: the years 0 and 10,000", () => {
    const year0 = Temporal.PlainDateTime.from({ year: 0, month: 6, day: 1 });
    const year10000 = Temporal.PlainDateTime.from({ year: 10_000, month: 6, day: 1 });

    assert.equal(writeRefused(h.plainDate(), year0.toPlainDate()).kind, "YearOutOfRange");
    assert.equal(writeRefused(h.plainDate(), year10000.toPlainDate()).kind, "YearOutOfRange");
    assert.equal(writeRefused(h.plainDateTime(), year10000).kind, "YearOutOfRange");
    assert.equal(writeRefused(h.instant(), year0.toZonedDateTime("+00:00").toInstant()).kind, "YearOutOfRange");
    assert.equal(writeRefused(h.offsetDateTime(), year10000.toZonedDateTime("+02:00")).kind, "YearOutOfRange");
  });

  test("NamedTimeZone: a named zone, UTC too", () => {
    for (const zone of ["Europe/Berlin", "UTC"]) {
      assert.equal(writeRefused(h.offsetDateTime(), instant.toZonedDateTimeISO(zone)).kind, "NamedTimeZone");
    }
  });

  test("NotIsoCalendar: a calendar other than iso8601", () => {
    const japanese = Temporal.PlainDate.from("2026-08-12").withCalendar("japanese");

    assert.equal(writeRefused(h.plainDate(), japanese).kind, "NotIsoCalendar");
    assert.equal(writeRefused(h.plainDateTime(), japanese.toPlainDateTime()).kind, "NotIsoCalendar");
  });

  test("WrongType: a Date, a text and a Temporal value of another kind", () => {
    assert.equal(writeRefused(h.instant(), new Date(0)).kind, "WrongType");
    assert.equal(writeRefused(h.plainDate(), "2026-08-12").kind, "WrongType");
    assert.equal(writeRefused(h.offsetDateTime(), instant).kind, "WrongType");
    assert.equal(writeRefused(h.plainDateTime(), Temporal.PlainDate.from("2026-08-12")).kind, "WrongType");
  });
});

describe("date-time reader", () => {
  test("each text form: Z, an offset, and 1 to 9 fraction digits", () => {
    for (const text of [
      "2026-08-12T09:10:12Z",
      "2026-08-12T11:10:12+02:00",
      "2026-08-12T03:40:12-05:30",
      "2026-08-12T09:10:12.5Z",
      "2026-08-12T09:10:12.500Z",
      "2026-08-12T09:10:12.500000Z",
    ]) {
      assert(sameValue(Temporal.Instant.from(text), read(h.instant(), text)), text);
    }

    assert(sameValue(read(h.instant(), "2026-08-12T09:10:12.123456789Z"), instant.add({ nanoseconds: 123_456_789 })));
  });

  test("an offset date-time reads with a fixed-offset zone; Z reads as +00:00", () => {
    const west = Temporal.ZonedDateTime.from("2026-08-12T09:10:12-05:30[-05:30]");
    const zero = Temporal.ZonedDateTime.from("2026-08-12T09:10:12+00:00[+00:00]");

    assert(sameValue(read(h.offsetDateTime(), "2026-08-12T09:10:12-05:30"), west));
    assert(sameValue(read(h.offsetDateTime(), "2026-08-12T09:10:12Z"), zero));
  });

  test("a date-time without an offset, and a date", () => {
    const wall = Temporal.PlainDateTime.from("2026-08-12T09:10:12.654321");

    assert(sameValue(read(h.plainDateTime(), "2026-08-12T09:10:12.654321"), wall));
    assert(sameValue(read(h.plainDate(), "2026-08-12"), Temporal.PlainDate.from("2026-08-12")));
  });

  test("BadTemporalText: an offset with seconds", () => {
    assert.equal(readRefused(h.offsetDateTime(), "2026-08-12T09:10:12+05:30:15").kind, "BadTemporalText");
    assert.equal(readRefused(h.instant(), "2026-08-12T09:10:12+05:30:15").kind, "BadTemporalText");
  });

  test("BadTemporalText: an instant without an offset; a plain date-time with one", () => {
    assert.equal(readRefused(h.instant(), "2026-08-12T09:10:12").kind, "BadTemporalText");
    assert.equal(readRefused(h.plainDateTime(), "2026-08-12T09:10:12Z").kind, "BadTemporalText");
  });

  test("BadTemporalText: a field out of range, the year 0, and another form", () => {
    assert.equal(readRefused(h.plainDate(), "2026-02-30").kind, "BadTemporalText");
    assert.equal(readRefused(h.plainDate(), "0000-01-01").kind, "BadTemporalText");
    assert.equal(readRefused(h.plainDateTime(), "2026-08-12T24:00:00").kind, "BadTemporalText");
    assert.equal(readRefused(h.plainDate(), "2026-08-12T00:00:00").kind, "BadTemporalText");
    assert.equal(readRefused(h.instant(), "2026-08-12t09:10:12z").kind, "BadTemporalText");
  });

  test("WrongType: a stored value that is not a string", () => {
    const result = decodeValue(kindOf(h.instant()), jsonTextCheckedByCore("1"), "worker");

    assert(!result.ok);
    assert.equal(result.error.kind, "WrongType");
  });
});

type DateTimeRow = {
  readonly y: number;
  readonly mo: number;
  readonly d: number;
  readonly h: number;
  readonly mi: number;
  readonly s: number;
  readonly us: number;
  readonly tz: "naive" | "utc" | number;
  readonly text: string;
};

function offsetId(minutes: number): string {
  const sign = minutes < 0 ? "-" : "+";
  const size = Math.abs(minutes);

  return `${sign}${String(Math.floor(size / 60)).padStart(2, "0")}:${String(size % 60).padStart(2, "0")}`;
}

type CorpusCase = {
  readonly wire: AnyWire;
  readonly value: Temporal.Instant | Temporal.ZonedDateTime | Temporal.PlainDateTime;
};

function corpusCase(row: DateTimeRow): CorpusCase {
  const fields = {
    year: row.y,
    month: row.mo,
    day: row.d,
    hour: row.h,
    minute: row.mi,
    second: row.s,
    millisecond: Math.floor(row.us / 1000),
    microsecond: row.us % 1000,
  };

  switch (row.tz) {
    case "naive":
      return { wire: h.plainDateTime(), value: Temporal.PlainDateTime.from(fields, { overflow: "reject" }) };
    case "utc":
      return { wire: h.instant(), value: Temporal.ZonedDateTime.from({ ...fields, timeZone: "+00:00" }).toInstant() };
    default:
      return {
        wire: h.offsetDateTime(),
        value: Temporal.ZonedDateTime.from({ ...fields, timeZone: offsetId(row.tz) }),
      };
  }
}

describe("date-time corpus", () => {
  test("2,000 date-times against Pydantic's text, and each text reads back to the value", () => {
    const mismatches: string[] = [];
    const lines = corpusLines("datetimes.jsonl");

    assert.equal(lines.length, 2000);

    for (const line of lines) {
      const row: DateTimeRow = JSON.parse(line);
      const { wire, value } = corpusCase(row);
      const kind = kindOf(wire);
      const text = encodeValue(kind, value);
      const back = decodeValue(kind, jsonTextCheckedByCore(row.text), "worker");

      if (!text.ok || text.value !== row.text || !back.ok || !sameValue(back.value, value)) {
        mismatches.push(line);
      }
    }

    assert.deepEqual(mismatches, [], `${runtimeLabel()}: ${mismatches.length} mismatches`);
  });
});
