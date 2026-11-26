// One test per outcome of the reader.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { type DecodeForm, decodeParameters, decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { ValueRefused } from "../../../horsies/src/codec/refusal.ts";
import { decodeWire } from "../../../horsies/src/codec/typed.ts";
import { isPlainObject } from "../../../horsies/src/codec/values.ts";
import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire, RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

function read(wire: AnyWire, text: string, form: DecodeForm = "worker"): unknown {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(text), form);

  assert(result.ok, result.ok ? "" : `refused ${JSON.stringify(result.error)}`);

  return result.value;
}

function readObject(wire: AnyWire, text: string, form: DecodeForm = "worker"): Readonly<Record<string, unknown>> {
  const value = read(wire, text, form);

  assert(isPlainObject(value), "an object");

  return value;
}

function refused(wire: AnyWire, text: string): ValueRefused {
  const result = decodeValue(kindOf(wire), jsonTextCheckedByCore(text), "worker");

  assert(!result.ok, "decoded a refused text");
  assert(result.error.kind === "decodeRefused", `a value refusal: ${JSON.stringify(result.error)}`);

  return result.error;
}

function nestedArrays(levels: number): RequiredWire {
  let wire: RequiredWire = h.int();

  for (let level = 0; level < levels; level += 1) {
    wire = h.array(wire);
  }

  return wire;
}

function nestedText(levels: number): string {
  return `${"[".repeat(levels)}1${"]".repeat(levels)}`;
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
    const result = decodeWire(h.bigint(), jsonTextCheckedByCore("12"), "worker");

    assert(result.ok);
    assert.equal(result.value, 12n);
  });
});

describe("reader: containers", () => {
  test("an object: members by name; an absent optional member has no own key; null for a nullable member", () => {
    const note = h.object({ a: h.int().optional(), b: h.int().nullable() });
    const value = readObject(note, '{"b":null}');

    assert.equal(Object.hasOwn(value, "a"), false);
    assert.equal(value["b"], null);
    assert.deepEqual(readObject(note, '{"b":2,"a":1}'), { a: 1, b: 2 });
  });

  test("a default is filled in the worker form, with a new value each time", () => {
    const retry = h.object({ tags: h.array(h.string()).default(["a"]) });
    const first = readObject(retry, "{}");
    const second = readObject(retry, "{}");

    assert.deepEqual(first, { tags: ["a"] });
    assert.notEqual(first["tags"], second["tags"]);
  });

  test("a default left out stays absent in the stored form", () => {
    const retry = h.object({ count: h.int().default(3) });

    assert.equal(Object.hasOwn(readObject(retry, "{}", "stored"), "count"), false);
  });

  test("arrays, tuples and records", () => {
    assert.deepEqual(read(h.array(h.int()), "[1,2]"), [1, 2]);
    assert.deepEqual(read(h.tuple([h.string(), h.float()]), '["a",1.0]'), ["a", 1]);
    assert.deepEqual(read(h.record(h.int()), '{"b":1,"a":2}'), { b: 1, a: 2 });
  });

  test("a stored record key __proto__ is an own data property; the prototype stays", () => {
    const value = readObject(h.record(h.int()), '{"__proto__":1,"a":2}');

    assert.equal(Object.getPrototypeOf(value), Object.prototype);
    assert.deepEqual(Object.keys(value), ["__proto__", "a"]);
  });

  test(`${MAX_VALUE_JSON_DEPTH} levels are read`, () => {
    assert(Array.isArray(read(nestedArrays(MAX_VALUE_JSON_DEPTH), nestedText(MAX_VALUE_JSON_DEPTH))));
  });

  test("a task input: the parameters object is the engine's level", () => {
    const parameters = parametersKind({ value: nestedArrays(MAX_VALUE_JSON_DEPTH), note: h.string().optional() });

    const decoded = decodeParameters(
      parameters,
      jsonTextCheckedByCore(`{"value":${nestedText(MAX_VALUE_JSON_DEPTH)}}`),
      "worker",
    );

    assert(decoded.ok);
    assert(isPlainObject(decoded.value) && !Object.hasOwn(decoded.value, "note"));
  });
});

describe("reader: each refusal", () => {
  test("WrongType: a stored value of another JSON type", () => {
    assert.equal(refused(h.null(), "0").refusal, "WrongType");
    assert.equal(refused(h.boolean(), '"true"').refusal, "WrongType");
    assert.equal(refused(h.string(), "1").refusal, "WrongType");
    assert.equal(refused(h.int(), '"7"').refusal, "WrongType");
    assert.equal(refused(h.bigint(), "[]").refusal, "WrongType");
    assert.equal(refused(h.float(), "null").refusal, "WrongType");
    assert.equal(refused(h.object({}), "[]").refusal, "WrongType");
    assert.equal(refused(h.array(h.int()), "{}").refusal, "WrongType");
    assert.equal(refused(h.record(h.int()), "[]").refusal, "WrongType");
  });

  test("WrongType: an integer text outside the finite float range under h.float()", () => {
    assert.equal(refused(h.float(), "1".repeat(400)).refusal, "WrongType");
  });

  test("WrongType: a tuple of another length", () => {
    assert.equal(refused(h.tuple([h.int(), h.int()]), "[1]").detail, "expected an array of 2 items");
  });

  test("FloatForInteger: 7.0, 0.5 and 1e+16 under h.int() and h.bigint()", () => {
    for (const wire of [h.int(), h.bigint()]) {
      assert.equal(refused(wire, "7.0").refusal, "FloatForInteger");
      assert.equal(refused(wire, "0.5").refusal, "FloatForInteger");
      assert.equal(refused(wire, "1e+16").refusal, "FloatForInteger");
    }
  });

  test("IntegerOutOfRange: an integer text outside the safe range under h.int()", () => {
    assert.equal(refused(h.int(), "9007199254740992").refusal, "IntegerOutOfRange");
  });

  test("UnknownMember: an undeclared key at every depth; the key is not in the path", () => {
    const nested = refused(h.object({ inner: h.object({ a: h.int() }) }), '{"inner":{"a":1,"secret":2}}');

    assert.deepEqual([nested.refusal, nested.path], ["UnknownMember", "$.inner.<input>"]);
  });

  test("MissingMember: a required member, and a member with no default", () => {
    const error = refused(h.object({ a: h.int(), b: h.int().optional() }), "{}");

    assert.deepEqual([error.refusal, error.path], ["MissingMember", "$.a"]);
  });

  test("EngineKey: a stored record key that only the engine writes", () => {
    const error = refused(h.record(h.boolean()), '{"__h_task_result__":true}');

    assert.deepEqual([error.refusal, error.path], ["EngineKey", "$.<input>"]);
  });

  test(`TooDeep: more than ${MAX_VALUE_JSON_DEPTH} levels`, () => {
    const levels = MAX_VALUE_JSON_DEPTH + 1;

    assert.equal(refused(nestedArrays(levels), nestedText(levels)).refusal, "TooDeep");
  });
});
