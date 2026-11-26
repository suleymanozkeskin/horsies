// One test per outcome of the writer.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { encodeWithReadBack } from "../../../horsies/src/codec/read-back.ts";
import { decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { EncodeRefused } from "../../../horsies/src/codec/refusal.ts";
import { encodeWire } from "../../../horsies/src/codec/typed.ts";
import { encodeParameters, encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire, RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { INT_KIND, objectKind, REQUIRED } from "../../../horsies/src/wire/kinds.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

function written<V>(wire: AnyWire, value: V): string {
  const result = encodeValue(kindOf(wire), value);

  assert(result.ok, result.ok ? "" : `refused ${result.error.kind} at ${result.error.path}`);

  return result.value;
}

function refused<V>(wire: AnyWire, value: V): EncodeRefused {
  const result = encodeValue(kindOf(wire), value);

  assert(!result.ok, "encoded a refused value");
  assert.equal(result.error.side, "encode");

  return result.error;
}

function nestedArrays(levels: number): RequiredWire {
  let wire: RequiredWire = h.int();

  for (let level = 0; level < levels; level += 1) {
    wire = h.array(wire);
  }

  return wire;
}

function nestedValue(levels: number): readonly unknown[] | number {
  let value: readonly unknown[] | number = 1;

  for (let level = 0; level < levels; level += 1) {
    value = [value];
  }

  return value;
}

class Point {
  readonly x = 1;
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

describe("writer: containers", () => {
  test("object members in declaration order, not in the value's key order", () => {
    const order = h.object({ id: h.int(), amount: h.float() });

    assert.equal(written(order, { amount: 12.5, id: 7 }), '{"id":7,"amount":12.5}');
  });

  test("an omitted optional member is left out; null is written for a nullable value", () => {
    const note = h.object({ a: h.int().optional(), b: h.int().nullable() });

    assert.equal(written(note, { b: null }), '{"b":null}');
    assert.equal(written(note, { a: 1, b: 2 }), '{"a":1,"b":2}');
  });

  test("the sender never writes a default", () => {
    const retry = h.object({ count: h.int().default(3), name: h.string() });

    assert.equal(written(retry, { name: "x" }), '{"name":"x"}');
    assert.equal(written(retry, { count: 5, name: "x" }), '{"count":5,"name":"x"}');
  });

  test("arrays, tuples and an empty object", () => {
    assert.equal(written(h.array(h.int()), [1, 2, 3]), "[1,2,3]");
    assert.equal(written(h.array(h.int()), []), "[]");
    assert.equal(written(h.tuple([h.string(), h.float()]), ["a", 1]), '["a",1.0]');
    assert.equal(written(h.object({}), {}), "{}");
  });

  test("a record keeps the value's own key order, integer-like keys first", () => {
    assert.equal(written(h.record(h.int()), { b: 1, a: 2 }), '{"b":1,"a":2}');
    assert.equal(written(h.record(h.int()), { b: 1, "10": 2, "2": 3 }), '{"2":3,"10":2,"b":1}');
  });

  test("a record key __proto__ is written as data", () => {
    const value: Record<string, number> = JSON.parse('{"__proto__":1,"a":2}');

    assert.equal(written(h.record(h.int()), value), '{"__proto__":1,"a":2}');
  });

  test(`${MAX_VALUE_JSON_DEPTH} levels are written`, () => {
    assert.equal(written(nestedArrays(MAX_VALUE_JSON_DEPTH), nestedValue(MAX_VALUE_JSON_DEPTH)).length, 401);
  });

  test("a task input: one member per given parameter, in declaration order; no parameters is {}", () => {
    const parameters = parametersKind({ b: h.int(), a: h.string(), c: h.int().optional() });
    const result = encodeParameters(parameters, { a: "x", b: 1 });

    assert(result.ok);
    assert.equal(result.value, '{"b":1,"a":"x"}');

    const empty = encodeParameters(parametersKind({}), {});

    assert(empty.ok);
    assert.equal(empty.value, "{}");
  });

  test("each parameter starts at the top: a parameter of 200 levels is written", () => {
    const parameters = parametersKind({ value: nestedArrays(MAX_VALUE_JSON_DEPTH) });

    assert(encodeParameters(parameters, { value: nestedValue(MAX_VALUE_JSON_DEPTH) }).ok);
  });
});

describe("writer: each refusal", () => {
  test("ExplicitUndefined: a value, a member and an array hole", () => {
    for (const wire of [h.null(), h.boolean(), h.string(), h.int(), h.bigint(), h.float()]) {
      assert.equal(refused(wire, undefined).kind, "ExplicitUndefined");
    }

    const member = refused(h.object({ a: h.int().optional() }), { a: undefined });

    assert.deepEqual([member.kind, member.path], ["ExplicitUndefined", "$.a"]);

    const holes: number[] = [1];

    holes[2] = 3;

    assert.equal(refused(h.array(h.int()), holes).path, "$[1]");
  });

  test("WrongType: a value of another JavaScript type", () => {
    assert.equal(refused(h.null(), 0).kind, "WrongType");
    assert.equal(refused(h.boolean(), "true").kind, "WrongType");
    assert.equal(refused(h.string(), 1).kind, "WrongType");
    assert.equal(refused(h.int(), 7n).kind, "WrongType");
    assert.equal(refused(h.bigint(), 7).kind, "WrongType");
    assert.equal(refused(h.float(), 7n).kind, "WrongType");
    assert.equal(refused(h.object({}), []).kind, "WrongType");
    assert.equal(refused(h.array(h.int()), {}).kind, "WrongType");
    assert.equal(refused(h.record(h.int()), "a").kind, "WrongType");
  });

  test("WrongType: a tuple of another length", () => {
    const error = refused(h.tuple([h.int(), h.int()]), [1]);

    assert.deepEqual([error.kind, error.detail], ["WrongType", "expected an array of 2 items"]);
  });

  test("NotSafeInteger: a fraction, 2^53 and NaN under h.int()", () => {
    assert.equal(refused(h.int(), 1.5).kind, "NotSafeInteger");
    assert.equal(refused(h.int(), 2 ** 53).kind, "NotSafeInteger");
    assert.equal(refused(h.int(), Number.NaN).kind, "NotSafeInteger");
  });

  test("IntegerTooLong: an integer of 4,301 digits", () => {
    assert.equal(refused(h.bigint(), 10n ** 4300n).kind, "IntegerTooLong");
  });

  test("NonFiniteFloat: NaN and the infinities", () => {
    for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      assert.equal(refused(h.float(), value).kind, "NonFiniteFloat");
    }
  });

  test("LoneSurrogate: a string and a record key that are not well formed", () => {
    assert.equal(refused(h.string(), "a\ud800b").kind, "LoneSurrogate");
    assert.equal(refused(h.string(), "\udc00").kind, "LoneSurrogate");
    assert.equal(refused(h.record(h.int()), { "\ud800": 1 }).kind, "LoneSurrogate");
  });

  test("UnknownMember: a key that the type does not declare, at every depth; the key is not in the path", () => {
    const top = refused(h.object({ a: h.int() }), { a: 1, b: 2 });

    assert.deepEqual([top.kind, top.path], ["UnknownMember", "$.<input>"]);

    const nested = refused(h.object({ inner: h.object({ a: h.int() }) }), { inner: { a: 1, secret: 2 } });

    assert.deepEqual([nested.kind, nested.path], ["UnknownMember", "$.inner.<input>"]);
  });

  test("MissingMember: a required member that the value leaves out", () => {
    const error = refused(h.object({ a: h.int(), b: h.int() }), { a: 1 });

    assert.deepEqual([error.kind, error.path], ["MissingMember", "$.b"]);
  });

  test("EngineKey: a record key that only the engine writes, at any depth", () => {
    for (const key of ["__h_task_result__", "__horsies_x", "__builtin_task_code__"]) {
      assert.equal(refused(h.record(h.int()), { [key]: 1 }).kind, "EngineKey");
    }

    assert.equal(refused(h.array(h.record(h.int())), [{ __h_x: 1 }]).path, "$[0].<input>");
  });

  test(`TooDeep: more than ${MAX_VALUE_JSON_DEPTH} levels`, () => {
    const levels = MAX_VALUE_JSON_DEPTH + 1;

    assert.equal(refused(nestedArrays(levels), nestedValue(levels)).kind, "TooDeep");
  });

  test("NotJsonNative: a Date, a Map, a class instance, a function and a symbol key", () => {
    assert.equal(refused(h.object({}), new Date(0)).kind, "NotJsonNative");
    assert.equal(refused(h.record(h.int()), new Map()).kind, "NotJsonNative");
    assert.equal(refused(h.object({ x: h.int() }), new Point()).kind, "NotJsonNative");
    assert.equal(refused(h.object({}), () => 1).kind, "NotJsonNative");
    assert.equal(refused(h.record(h.int()), { [Symbol("k")]: 1 }).kind, "NotJsonNative");
  });
});

describe("writer: the check in front of the reader", () => {
  test("the writer makes no JsonText for a value whose text breaks the strict read rule", () => {
    const deep = MAX_VALUE_JSON_DEPTH + 1;

    const cases: readonly (readonly [AnyWire, unknown])[] = [
      [h.float(), Number.NaN],
      [h.float(), Number.POSITIVE_INFINITY],
      [h.json(), { a: Number.NEGATIVE_INFINITY }],
      [h.string(), "\ud800"],
      [h.record(h.int()), { "\udc00": 1 }],
      [h.bigint(), 10n ** 4300n],
      [h.json(), 10n ** 4300n],
      [nestedArrays(deep), nestedValue(deep)],
      [h.json(), nestedValue(deep)],
    ];

    for (const [wire, value] of cases) {
      const result = encodeValue(kindOf(wire), value);

      assert(!result.ok, "the writer gave a text");
    }
  });
});

describe("writer: the declared member list", () => {
  // A kind with the members `b`, then `10`, in this order. An object literal cannot hold that order: JavaScript puts
  // the integer-like key `10` first in `{ b: ..., "10": ... }`.
  const declared = objectKind([
    { name: "b", kind: INT_KIND, presence: REQUIRED },
    { name: "10", kind: INT_KIND, presence: REQUIRED },
  ]);

  test("members are written in the declared order, never in the value's key order, integer-like names too", () => {
    const value = { "10": 2, b: 1 };

    assert.deepEqual(Object.keys(value), ["10", "b"]);
    assert.deepEqual(encodeValue(declared, value), { ok: true, value: '{"b":1,"10":2}' });
  });

  test("the read-back keeps the values and the text of integer-like members", () => {
    const written = encodeWithReadBack(declared, { b: 1, "10": 2 });

    assert.deepEqual(written, { ok: true, value: '{"b":1,"10":2}' });

    const read = decodeValue(declared, jsonTextCheckedByCore('{"b":1,"10":2}'), "worker");

    assert.deepEqual(read, { ok: true, value: { b: 1, "10": 2 } });
  });
});
