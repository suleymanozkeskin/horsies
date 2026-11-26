// One test per outcome of Decimal.parse, h.decimal() and h.uuid(). The decimal corpus is in tests/stored-forms/.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefused, EncodeRefused } from "../../../horsies/src/codec/refusal.ts";
import { isObjectLike } from "../../../horsies/src/codec/values.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import { Decimal } from "../../../horsies/src/wire/decimal.ts";
import type { AnyWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

function decimal(text: string): Decimal {
  const parsed = Decimal.parse(text);

  assert(parsed.ok, `${text} parses`);

  return parsed.value;
}

function writeRefused<V>(wire: AnyWire, value: V): EncodeRefused {
  const result = encodeValue(kindOf(wire), value);

  assert(!result.ok, "encoded a refused value");

  return result.error;
}

function readRefused(wire: AnyWire, text: string): DecodeRefused {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(text), "worker");

  assert(!result.ok, "decoded a refused text");

  return result.error;
}

describe("Decimal.parse", () => {
  test("normalises to Python's str(Decimal(text))", () => {
    for (const [text, python] of [
      ["1.10", "1.10"],
      ["1e3", "1E+3"],
      ["0.0000001", "1E-7"],
      ["0.000001", "0.000001"],
      ["-0", "-0"],
      ["+5", "5"],
      [".5", "0.5"],
      ["5.", "5"],
      ["007.50", "7.50"],
      ["0E+5", "0E+5"],
      ["10E+1", "1.0E+2"],
      ["-0e-3", "-0.000"],
      ["123.456e5", "1.23456E+7"],
    ] as const) {
      assert.equal(decimal(text).toString(), python, text);
    }
  });

  test("NotFinite: an infinity and a NaN", () => {
    for (const text of ["NaN", "-Infinity", "inf", "sNaN", "nan12"]) {
      const parsed = Decimal.parse(text);

      assert(!parsed.ok && parsed.error.kind === "NotFinite", text);
    }
  });

  test("NotADecimal: any other text", () => {
    const texts = ["", "_", "1.2.3", "1e", "e5", "abc", "1 0", "0x10", "1\u0000", "1\u00a02", "infinit", "1\ud800"];

    for (const text of texts) {
      const parsed = Decimal.parse(text);

      assert(!parsed.ok && parsed.error.kind === "NotADecimal", JSON.stringify(text));
    }
  });

  test("Python's forms: whitespace at both ends, any underscore, Unicode decimal digits", () => {
    for (const [text, python] of [
      [" 1 ", "1"],
      ["\u20281\u3000", "1"],
      ["1_000", "1000"],
      ["_1_", "1"],
      ["1_0.0_1e1_0", "1.001E+11"],
      ["\u0661\u0662", "12"],
      ["1e\u0665", "1E+5"],
      ["\uff11", "1"],
      ["\ud835\udfd2", "4"],
    ] as const) {
      assert.equal(decimal(text).toString(), python, JSON.stringify(text));
    }
  });

  test("the exponent bounds of Python's decimal", () => {
    assert.equal(decimal("12E+999999999999999998").toString(), "1.2E+999999999999999999");
    assert.equal(decimal("1E-1999999999999999997").toString(), "1E-1999999999999999997");
    assert.equal(decimal("0.10E+1000000000000000000").toString(), "1.0E+999999999999999999");
    assert.equal(decimal("1E+0000000000000000000000001").toString(), "1E+1");

    for (const text of [
      "12E+999999999999999999",
      "1E+1000000000000000000",
      "0E+1000000000000000000",
      "1E-1999999999999999998",
      "10E-1999999999999999998",
      "1E+10000000000000000000000000",
    ]) {
      const parsed = Decimal.parse(text);

      assert(!parsed.ok && parsed.error.kind === "NotADecimal", text);
    }
  });

  test("equals compares the text; a Decimal is frozen and branded", () => {
    assert(decimal("1.10").equals(decimal("1.10")));
    assert(!decimal("1.10").equals(decimal("1.1")));
    assert(Object.isFrozen(decimal("1")));
    assert(Decimal.isDecimal(decimal("1")));
    assert(!Decimal.isDecimal({ toString: () => "1" }));
  });
});

describe("h.decimal()", () => {
  test("writes the text as a JSON string and reads it back", () => {
    const written = encodeValue(kindOf(h.decimal()), decimal("1E+3"));

    assert(written.ok);
    assert.equal(written.value, '"1E+3"');

    const read = decodeValue(kindOf(h.decimal()), written.value, "worker");

    assert(read.ok && isObjectLike(read.value) && Decimal.isDecimal(read.value));
    assert(read.value.equals(decimal("1e3")));
  });

  test("WrongType: a decimal text that is not a Decimal", () => {
    assert.equal(writeRefused(h.decimal(), "1.10").kind, "WrongType");
    assert.equal(readRefused(h.decimal(), "1.1").kind, "WrongType");
  });

  test("BadDecimalText: a stored text that is not a finite decimal", () => {
    assert.equal(readRefused(h.decimal(), '"abc"').kind, "BadDecimalText");
    assert.equal(readRefused(h.decimal(), '"NaN"').kind, "BadDecimalText");
  });
});

describe("h.uuid()", () => {
  const id = "0192f3b4-5c6d-7e8f-9a0b-1c2d3e4f5a6b";

  test("writes and reads the lower-case 8-4-4-4-12 form as it is", () => {
    const written = encodeValue(kindOf(h.uuid()), id);

    assert(written.ok);
    assert.equal(written.value, `"${id}"`);
    assert.deepEqual(decodeValue(kindOf(h.uuid()), written.value, "worker"), { ok: true, value: id });
  });

  test("a random UUID of the runtime has that form", () => {
    assert(encodeValue(kindOf(h.uuid()), crypto.randomUUID()).ok);
  });

  test("NotCanonicalUuid: upper case, no hyphens, braces", () => {
    for (const text of [id.toUpperCase(), id.replaceAll("-", ""), `{${id}}`]) {
      assert.equal(writeRefused(h.uuid(), text).kind, "NotCanonicalUuid", text);
    }
  });

  test("WrongType and BadUuidText", () => {
    assert.equal(writeRefused(h.uuid(), 1).kind, "WrongType");
    assert.equal(readRefused(h.uuid(), "1").kind, "WrongType");
    assert.equal(readRefused(h.uuid(), JSON.stringify(id.toUpperCase())).kind, "BadUuidText");
  });
});
