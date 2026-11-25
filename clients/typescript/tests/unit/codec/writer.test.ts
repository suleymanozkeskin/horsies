// One test per outcome of the writer.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { EncodeRefusalKind } from "../../../horsies/src/codec/refusal.ts";
import { encodeValue, encodeWire } from "../../../horsies/src/codec/writer.ts";
import { h, kindOf } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire } from "../../../horsies/src/wire/infer.ts";

function written<V>(wire: AnyWire, value: V): string {
  const result = encodeValue(kindOf(wire), value);

  assert(result.ok, result.ok ? "" : `refused ${result.error.kind}`);

  return result.value;
}

function refused<V>(wire: AnyWire, value: V): EncodeRefusalKind {
  const result = encodeValue(kindOf(wire), value);

  assert(!result.ok, "encoded a refused value");
  assert.equal(result.error.side, "encode");
  assert.equal(result.error.path, "$");

  return result.error.kind;
}

describe("writer: each scalar kind to its text", () => {
  test("null, boolean, string", () => {
    assert.equal(written(h.null(), null), "null");
    assert.equal(written(h.boolean(), true), "true");
    assert.equal(written(h.boolean(), false), "false");
    assert.equal(written(h.string(), 'a"b\\c\n\u0001é🐎'), JSON.stringify('a"b\\c\n\u0001é🐎'));
  });

  test("the escapes of Python's text: U+2028, U+2029 and / are not escaped", () => {
    assert.equal(written(h.string(), "\u2028\u2029/"), '"\u2028\u2029/"');
  });

  test("h.int() writes decimal digits; -0 is 0", () => {
    assert.equal(written(h.int(), 7), "7");
    assert.equal(written(h.int(), -Number.MAX_SAFE_INTEGER), "-9007199254740991");
    assert.equal(written(h.int(), -0), "0");
  });

  test("h.bigint() writes every digit, up to 4,300", () => {
    assert.equal(written(h.bigint(), -(2n ** 127n)), "-170141183460469231731687303715884105728");
    assert.equal(written(h.bigint(), 10n ** 4299n).length, 4300);
    assert.equal(written(h.bigint(), -(10n ** 4299n)).length, 4301);
  });

  test("h.float() writes Python's float text: 7 is 7.0, -0 is -0.0", () => {
    assert.equal(written(h.float(), 7), "7.0");
    assert.equal(written(h.float(), -0), "-0.0");
    assert.equal(written(h.float(), 1e-5), "1e-05");
  });

  test("encodeWire takes the wire type", () => {
    const result = encodeWire(h.float(), 0.5);

    assert(result.ok);
    assert.equal(result.value, "0.5");
  });
});

describe("writer: each refusal", () => {
  test("ExplicitUndefined", () => {
    for (const wire of [h.null(), h.boolean(), h.string(), h.int(), h.bigint(), h.float()]) {
      assert.equal(refused(wire, undefined), "ExplicitUndefined");
    }
  });

  test("WrongType: a value of another JavaScript type", () => {
    assert.equal(refused(h.null(), 0), "WrongType");
    assert.equal(refused(h.boolean(), "true"), "WrongType");
    assert.equal(refused(h.string(), 1), "WrongType");
    assert.equal(refused(h.int(), 7n), "WrongType");
    assert.equal(refused(h.bigint(), 7), "WrongType");
    assert.equal(refused(h.float(), 7n), "WrongType");
  });

  test("NotSafeInteger: a fraction, 2^53 and NaN under h.int()", () => {
    assert.equal(refused(h.int(), 1.5), "NotSafeInteger");
    assert.equal(refused(h.int(), 2 ** 53), "NotSafeInteger");
    assert.equal(refused(h.int(), Number.NaN), "NotSafeInteger");
  });

  test("IntegerTooLong: an integer of 4,301 digits", () => {
    assert.equal(refused(h.bigint(), 10n ** 4300n), "IntegerTooLong");
  });

  test("NonFiniteFloat: NaN and the infinities", () => {
    for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      assert.equal(refused(h.float(), value), "NonFiniteFloat");
    }
  });

  test("LoneSurrogate: a string that is not well formed", () => {
    assert.equal(refused(h.string(), "a\ud800b"), "LoneSurrogate");
    assert.equal(refused(h.string(), "\udc00"), "LoneSurrogate");
  });
});
