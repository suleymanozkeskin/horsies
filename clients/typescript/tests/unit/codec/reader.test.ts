// One test per outcome of the reader.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeValue, decodeWire } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefusalKind } from "../../../horsies/src/codec/refusal.ts";
import { h, kindOf } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire } from "../../../horsies/src/wire/infer.ts";

function read(wire: AnyWire, text: string): unknown {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(text));

  assert(result.ok, result.ok ? "" : `refused ${result.error.kind}`);

  return result.value;
}

function refused(wire: AnyWire, text: string): DecodeRefusalKind {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(text));

  assert(!result.ok, "decoded a refused text");
  assert.equal(result.error.side, "decode");
  assert.equal(result.error.path, "$");

  return result.error.kind;
}

describe("reader: each scalar kind from its text", () => {
  test("null, boolean, string", () => {
    assert.equal(read(h.null(), "null"), null);
    assert.equal(read(h.boolean(), "false"), false);
    assert.equal(read(h.string(), '"\\u00e9\\n"'), "é\n");
  });

  test("h.int() reads an integer text in the safe range", () => {
    assert.equal(read(h.int(), "-9007199254740991"), -Number.MAX_SAFE_INTEGER);
  });

  test("h.bigint() reads every digit, and a safe integer text as a bigint", () => {
    assert.equal(read(h.bigint(), "340282366920938463463374607431768211456"), 2n ** 128n);
    assert.equal(read(h.bigint(), "7"), 7n);
  });

  test("h.float() reads a float text, an integer text, and -0.0", () => {
    assert.equal(read(h.float(), "0.1"), 0.1);
    assert.equal(read(h.float(), "123.0"), 123);
    assert.equal(read(h.float(), "7"), 7);
    assert.equal(read(h.float(), "9223372036854775807"), 2 ** 63);
    assert(Object.is(read(h.float(), "-0.0"), -0));
  });

  test("decodeWire takes the wire type", () => {
    const result = decodeWire(h.bigint(), jsonTextCheckedByCore("12"));

    assert(result.ok);
    assert.equal(result.value, 12n);
  });
});

describe("reader: each refusal", () => {
  test("WrongType: a stored value of another JSON type", () => {
    assert.equal(refused(h.null(), "0"), "WrongType");
    assert.equal(refused(h.boolean(), '"true"'), "WrongType");
    assert.equal(refused(h.string(), "1"), "WrongType");
    assert.equal(refused(h.int(), '"7"'), "WrongType");
    assert.equal(refused(h.bigint(), "[]"), "WrongType");
    assert.equal(refused(h.float(), "null"), "WrongType");
  });

  test("WrongType: an integer text outside the finite float range under h.float()", () => {
    assert.equal(refused(h.float(), "1".repeat(400)), "WrongType");
  });

  test("FloatForInteger: 7.0 and 0.5 under h.int() and h.bigint()", () => {
    for (const wire of [h.int(), h.bigint()]) {
      assert.equal(refused(wire, "7.0"), "FloatForInteger");
      assert.equal(refused(wire, "0.5"), "FloatForInteger");
      assert.equal(refused(wire, "1e+16"), "FloatForInteger");
    }
  });

  test("IntegerOutOfRange: an integer text outside the safe range under h.int()", () => {
    assert.equal(refused(h.int(), "9007199254740992"), "IntegerOutOfRange");
  });
});
