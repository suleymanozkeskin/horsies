// The committed corpus of Python's texts (`corpus/`, written by `corpus/make_corpus.py`): each value encodes to
// Python's text, and each text reads back to the value. 0 mismatches on each runtime.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { PYTHON_DECIMAL_TABLES } from "../../horsies/src/codec/decimal-text.ts";
import { pythonFloatText } from "../../horsies/src/codec/float-text.ts";
import { jsonTextCheckedByCore } from "../../horsies/src/codec/json-text.ts";
import { IntegralFloat, readTree } from "../../horsies/src/codec/number-text.ts";
import { decodeValue } from "../../horsies/src/codec/reader.ts";
import { encodeValue } from "../../horsies/src/codec/writer.ts";
import { h } from "../../horsies/src/wire/builders.ts";
import { Decimal } from "../../horsies/src/wire/decimal.ts";
import type { AnyWire } from "../../horsies/src/wire/infer.ts";
import { kindOf } from "../../horsies/src/wire/wire-type.ts";
import { sameValue } from "../support/values.ts";
import { CORPUS_DIR, corpusLines, runtimeLabel } from "./files.ts";

const FLOAT_CORPUS_SIZE = 10_000;
const DECIMAL_CORPUS_SIZE = 2000;
const DECIMAL_FUZZ_SIZE = 4000;
const DATETIME_CORPUS_SIZE = 2000;
const TIME_CORPUS_SIZE = 1000;

function assertNoMismatch(mismatches: readonly string[]): void {
  assert.deepEqual(mismatches, [], `${runtimeLabel()}: ${mismatches.length} mismatches`);
}

describe("float corpus", () => {
  test(`${FLOAT_CORPUS_SIZE} doubles against Python's repr, and each text reads back bit for bit`, () => {
    const bits = new DataView(new ArrayBuffer(8));
    const mismatches: string[] = [];
    const lines = corpusLines("floats.txt");

    assert.equal(lines.length, FLOAT_CORPUS_SIZE);

    for (const line of lines) {
      const space = line.indexOf(" ");

      bits.setBigUint64(0, BigInt(`0x${line.slice(0, space)}`));
      const value = bits.getFloat64(0);
      const python = line.slice(space + 1);
      const read = readTree(jsonTextCheckedByCore(python));
      const readValue = read instanceof IntegralFloat ? read.value : read;

      if (pythonFloatText(value) !== python || !Object.is(readValue, value)) {
        mismatches.push(line);
      }
    }

    assertNoMismatch(mismatches);
  });
});

describe("decimal corpus", () => {
  test(`${DECIMAL_CORPUS_SIZE} decimal texts against Python's str(Decimal(text)), written and read back`, () => {
    const mismatches: string[] = [];
    const lines = corpusLines("decimals.jsonl");

    assert.equal(lines.length, DECIMAL_CORPUS_SIZE);

    for (const line of lines) {
      const [input, python]: [string, string] = JSON.parse(line);
      const parsed = Decimal.parse(input);
      const written = parsed.ok ? encodeValue(kindOf(h.decimal()), parsed.value) : parsed;
      const read = written.ok ? decodeValue(kindOf(h.decimal()), written.value, "worker") : written;

      if (!written.ok || written.value !== JSON.stringify(python) || !read.ok || String(read.value) !== python) {
        mismatches.push(line);
      }
    }

    assertNoMismatch(mismatches);
  });

  test("Python's whitespace and digit tables are the tables of the parser", () => {
    const tables: { readonly spaces: readonly number[]; readonly digit_zeros: readonly number[] } = JSON.parse(
      readFileSync(`${CORPUS_DIR}decimal-tables.json`, "utf8"),
    );

    assert.deepEqual([...PYTHON_DECIMAL_TABLES.spaces], tables.spaces);
    assert.deepEqual([...PYTHON_DECIMAL_TABLES.digitZeros], tables.digit_zeros);
  });

  test(`fuzz: ${DECIMAL_FUZZ_SIZE} texts accepted with the Python client's stored text, or refused as it refuses them`, () => {
    const mismatches: string[] = [];
    const lines = corpusLines("decimal-fuzz.jsonl");

    assert.equal(lines.length, DECIMAL_FUZZ_SIZE);

    for (const line of lines) {
      const [text, stored]: [string, string | null] = JSON.parse(line);
      const parsed = Decimal.parse(text);
      const outcome = parsed.ok ? parsed.value.toString() : null;

      if (outcome !== stored) {
        mismatches.push(line);
      }
    }

    assertNoMismatch(mismatches);
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

type TimeRow = {
  readonly h: number;
  readonly mi: number;
  readonly s: number;
  readonly us: number;
  readonly text: string;
};

function offsetId(minutes: number): string {
  const sign = minutes < 0 ? "-" : "+";
  const size = Math.abs(minutes);

  return `${sign}${String(Math.floor(size / 60)).padStart(2, "0")}:${String(size % 60).padStart(2, "0")}`;
}

type DateTimeCase = {
  readonly wire: AnyWire;
  readonly value: Temporal.Instant | Temporal.ZonedDateTime | Temporal.PlainDateTime;
};

function dateTimeCase(row: DateTimeRow): DateTimeCase {
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

/** The value encodes to `text`, and `text` reads back to the value. */
function writesAndReads<V>(wire: AnyWire, value: V, text: string): boolean {
  const kind = kindOf(wire);
  const written = encodeValue(kind, value);
  const back = decodeValue(kind, jsonTextCheckedByCore(text), "worker");

  return written.ok && written.value === text && back.ok && sameValue(back.value, value);
}

describe("date-time corpus", () => {
  test(`${DATETIME_CORPUS_SIZE} date-times against Pydantic's text, and each text reads back to the value`, () => {
    const lines = corpusLines("datetimes.jsonl");

    assert.equal(lines.length, DATETIME_CORPUS_SIZE);

    const mismatches = lines.filter((line) => {
      const row: DateTimeRow = JSON.parse(line);
      const { wire, value } = dateTimeCase(row);

      return !writesAndReads(wire, value, row.text);
    });

    assertNoMismatch(mismatches);
  });

  test(`${TIME_CORPUS_SIZE} times of day against Pydantic's text, and each text reads back to the value`, () => {
    const lines = corpusLines("times.jsonl");

    assert.equal(lines.length, TIME_CORPUS_SIZE);

    const mismatches = lines.filter((line) => {
      const row: TimeRow = JSON.parse(line);

      const value = Temporal.PlainTime.from({
        hour: row.h,
        minute: row.mi,
        second: row.s,
        millisecond: Math.floor(row.us / 1000),
        microsecond: row.us % 1000,
      });

      return !writesAndReads(h.plainTime(), value, row.text);
    });

    assertNoMismatch(mismatches);
  });
});
