// The hz.* codecs and hz.wire(): the stored text of a zod schema equals the text of its h.* kind, and zod's own
// decode and encode of each codec use the same text forms.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { h } from "../../../horsies/src/wire/builders.ts";
import { Decimal } from "../../../horsies/src/wire/decimal.ts";
import type { RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { codecKindOf, hz, wireKindOf } from "../../../horsies/src/zod/codecs.ts";
import type { ZodSchema } from "../../../horsies/src/zod/definition.ts";
import { acceptedKind, storedText, workerValue } from "./support.ts";

type CodecRow = {
  readonly name: string;
  readonly schema: ZodSchema;
  readonly wire: RequiredWire;
  readonly value: object;
  readonly text: string;
};

function decimal(text: string): Decimal {
  const parsed = Decimal.parse(text);

  assert.ok(parsed.ok);

  return parsed.value;
}

const ROWS: readonly CodecRow[] = [
  {
    name: "instant",
    schema: hz.instant(),
    wire: h.instant(),
    value: Temporal.Instant.from("2026-08-12T09:10:12.654321Z"),
    text: '"2026-08-12T09:10:12.654321Z"',
  },
  {
    name: "offsetDateTime",
    schema: hz.offsetDateTime(),
    wire: h.offsetDateTime(),
    value: Temporal.ZonedDateTime.from("2026-08-12T09:10:12+02:00[+02:00]"),
    text: '"2026-08-12T09:10:12+02:00"',
  },
  {
    name: "plainDateTime",
    schema: hz.plainDateTime(),
    wire: h.plainDateTime(),
    value: Temporal.PlainDateTime.from("2026-08-12T09:10:12.5"),
    text: '"2026-08-12T09:10:12.500000"',
  },
  {
    name: "plainDate",
    schema: hz.plainDate(),
    wire: h.plainDate(),
    value: Temporal.PlainDate.from("2026-08-12"),
    text: '"2026-08-12"',
  },
  {
    name: "plainTime",
    schema: hz.plainTime(),
    wire: h.plainTime(),
    value: Temporal.PlainTime.from("09:10:12"),
    text: '"09:10:12"',
  },
  { name: "decimal", schema: hz.decimal(), wire: h.decimal(), value: decimal("1e3"), text: '"1E+3"' },
];

describe("hz.* codecs", () => {
  for (const row of ROWS) {
    test(`hz.${row.name}() maps to the kind of h.${row.name}() and stores its text`, () => {
      const kind = acceptedKind(row.schema);

      assert.deepEqual(kind, kindOf(row.wire));
      assert.equal(storedText(kind, row.value), row.text);
      assert.equal(storedText(kindOf(row.wire), row.value), row.text);
    });

    test(`zod's own encode of hz.${row.name}() gives the stored text, and its decode reads it back`, () => {
      const text = JSON.parse(row.text);

      assert.equal(z.encode(row.schema, row.value), text);
      assert.deepEqual(z.decode(row.schema, text), workerValue(kindOf(row.wire), row.text));
    });
  }

  test("zod's decode of a text that is not the stored form gives a zod issue, not a throw of another kind", () => {
    const result = z.safeDecode(hz.instant(), "2026-08-12T09:10:12");

    assert.equal(result.success, false);
    assert.equal(result.error?.issues[0]?.code, "custom");
  });

  test("zod's encode of a value that the text form refuses gives a zod issue", () => {
    const named = Temporal.ZonedDateTime.from("2026-08-12T09:10:12+02:00[Europe/Berlin]");
    const result = z.safeEncode(hz.offsetDateTime(), named);

    assert.equal(result.success, false);
  });

  test("a codec that is not one of hz.* has no hz kind", () => {
    assert.equal(codecKindOf(Number).found, "none");
  });
});

describe("hz.wire()", () => {
  test("zod's own parse accepts the values that the wire type writes, and refuses others", () => {
    const order = hz.wire(h.object({ id: h.uuid() }));

    assert.equal(order.safeParse({ id: "00000000-0000-4000-8000-000000000000" }).success, true);
    assert.equal(order.safeParse({ id: "00000000-0000-4000-8000-00000000000A" }).success, false);
  });

  test("a wire type that may be left out, or an upstream outcome, is a defect", () => {
    // SAFETY: the static types refuse both arguments (tests/types/fail/zod-wire-*.ts); this test checks the run time.
    const optional = h.string().optional() as unknown as RequiredWire;
    // SAFETY: as above.
    const upstream = h.resultOf(h.int()) as unknown as RequiredWire;

    assert.throws(() => hz.wire(optional), TypeError);
    assert.throws(() => hz.wire(upstream), TypeError);
  });

  test("a test function that is not one of hz.wire() has no hz kind", () => {
    assert.equal(wireKindOf(() => true).found, "none");
  });
});
