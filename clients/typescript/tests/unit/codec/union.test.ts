// One test per outcome of literals, enums and unions.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { EncodeRefused, ValueRefused } from "../../../horsies/src/codec/refusal.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

function written<V>(wire: AnyWire, value: V): string {
  const result = encodeValue(kindOf(wire), value);

  assert(result.ok, result.ok ? "" : `refused ${JSON.stringify(result.error)}`);

  return result.value;
}

function writeRefused<V>(wire: AnyWire, value: V): EncodeRefused {
  const result = encodeValue(kindOf(wire), value);

  assert(!result.ok, "encoded a refused value");

  return result.error;
}

function read(wire: AnyWire, text: string): unknown {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(text), "worker");

  assert(result.ok, result.ok ? "" : `refused ${JSON.stringify(result.error)}`);

  return result.value;
}

function readRefused(wire: AnyWire, text: string): ValueRefused {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(text), "worker");

  assert(!result.ok, "decoded a refused text");
  assert(result.error.kind === "decodeRefused", `a value refusal: ${JSON.stringify(result.error)}`);

  return result.error;
}

const payment = h.union("method", [
  h.object({ method: h.literal("card"), last4: h.string() }),
  h.object({ iban: h.string(), method: h.enum(["bank", "sepa"]) }),
]);

describe("literals and enums", () => {
  test("each member type is stored as its JSON text", () => {
    const level = h.literal("low", 3, true, null);

    assert.equal(written(level, "low"), '"low"');
    assert.equal(written(level, 3), "3");
    assert.equal(written(level, true), "true");
    assert.equal(written(level, null), "null");
    assert.equal(read(level, "3"), 3);
    assert.equal(written(h.enum(["red", "green"]), "green"), '"green"');
  });

  test("NotAMember: a value outside the members, at encode and at decode", () => {
    assert.equal(writeRefused(h.enum(["red", "green"]), "blue").refusal, "NotAMember");
    assert.equal(writeRefused(h.literal(1, 3), 2).refusal, "NotAMember");
    assert.equal(readRefused(h.enum(["red", "green"]), '"blue"').refusal, "NotAMember");
    assert.equal(readRefused(h.literal(1, 3), "12345678901234567890").refusal, "NotAMember");
  });

  test("WrongType: a value that no literal can be", () => {
    assert.equal(writeRefused(h.literal(1), [1]).refusal, "WrongType");
    assert.equal(readRefused(h.literal(1), "[1]").refusal, "WrongType");
  });

  test("FloatForInteger: a float text for a number member", () => {
    assert.equal(readRefused(h.literal(1, 3), "1.0").refusal, "FloatForInteger");
  });
});

describe("primitive unions", () => {
  const amount = h.union([h.string(), h.int(), h.null()]);

  test("the first member that takes the value writes it", () => {
    assert.equal(written(amount, "7"), '"7"');
    assert.equal(written(amount, 7), "7");
    assert.equal(written(amount, null), "null");
    assert.equal(read(amount, '"7"'), "7");
    assert.equal(read(amount, "7"), 7);
  });

  test("NotAMember: a value of a type that no member has", () => {
    assert.equal(writeRefused(amount, true).refusal, "NotAMember");
    assert.equal(readRefused(amount, "true").refusal, "NotAMember");
  });

  test("a member of the value's type that refuses it gives its own refusal", () => {
    assert.equal(writeRefused(amount, "\ud800").refusal, "LoneSurrogate");
    assert.equal(writeRefused(amount, 1.5).refusal, "NotSafeInteger");
    assert.equal(readRefused(amount, "7.0").refusal, "FloatForInteger");
  });
});

describe("tagged unions", () => {
  test("the member object that the tag names, in its declaration order", () => {
    assert.equal(written(payment, { last4: "4242", method: "card" }), '{"method":"card","last4":"4242"}');
    assert.equal(written(payment, { method: "sepa", iban: "DE00" }), '{"iban":"DE00","method":"sepa"}');
    assert.deepEqual(read(payment, '{"iban":"DE00","method":"bank"}'), { iban: "DE00", method: "bank" });
  });

  test("NotAMember: a tag that names no member, and a missing tag", () => {
    const unknown = writeRefused(payment, { method: "cash" });

    assert.deepEqual([unknown.refusal, unknown.path], ["NotAMember", "$.method"]);
    assert.equal(writeRefused(payment, { last4: "4242" }).refusal, "NotAMember");
    assert.equal(readRefused(payment, '{"method":"cash"}').refusal, "NotAMember");
    assert.equal(readRefused(payment, '{"method":1}').refusal, "NotAMember");
  });

  test("the member's own rules: an undeclared key, and a value that is not an object", () => {
    assert.equal(writeRefused(payment, { method: "card", last4: "1", extra: 1 }).refusal, "UnknownMember");
    assert.equal(readRefused(payment, '{"method":"card","last4":"1","extra":1}').refusal, "UnknownMember");
    assert.equal(writeRefused(payment, "card").refusal, "WrongType");
    assert.equal(readRefused(payment, '"card"').refusal, "WrongType");
  });
});
