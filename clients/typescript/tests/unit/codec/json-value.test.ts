// One test per outcome of h.json().

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/_catalog/codec_bounds.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { EncodeRefused, ValueRefused } from "../../../horsies/src/codec/refusal.ts";
import { isPlainObject } from "../../../horsies/src/codec/values.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

const JSON_KIND = kindOf(h.json());

function written<V>(value: V): string {
  const result = encodeValue(JSON_KIND, value);

  assert(result.ok, result.ok ? "" : `refused ${JSON.stringify(result.error)}`);

  return result.value;
}

function writeRefused<V>(value: V): EncodeRefused {
  const result = encodeValue(JSON_KIND, value);

  assert(!result.ok, "encoded a refused value");

  return result.error;
}

function read(text: string): unknown {
  const result = decodeValue(JSON_KIND, jsonTextCheckedByCore(text), "worker");

  assert(result.ok, result.ok ? "" : `refused ${JSON.stringify(result.error)}`);

  return result.value;
}

function readRefused(text: string): ValueRefused {
  const result = decodeValue(JSON_KIND, jsonTextCheckedByCore(text), "worker");

  assert(!result.ok, "decoded a refused text");
  assert(result.error.kind === "decodeRefused", `a value refusal: ${JSON.stringify(result.error)}`);

  return result.error;
}

function nested(levels: number): unknown {
  let value: unknown = 1;

  for (let level = 0; level < levels; level += 1) {
    value = [value];
  }

  return value;
}

class Point {
  readonly x = 1;
}

describe("h.json() writer", () => {
  test("a bigint and a safe integer are integers; every other number is Python's float text", () => {
    assert.equal(written(7), "7");
    assert.equal(written(2n ** 64n), "18446744073709551616");
    assert.equal(written(7.5), "7.5");
    assert.equal(written(1e16), "1e+16");
    assert.equal(written(1e-5), "1e-05");
    assert.equal(written(-0), "-0.0");
    assert.equal(written(2 ** 53), "9007199254740992.0");
  });

  test("objects in the value's own key order, arrays, strings, booleans and null", () => {
    const value = { z: [1, { y: null }], a: { b: "x", c: true } };

    assert.equal(written(value), '{"z":[1,{"y":null}],"a":{"b":"x","c":true}}');
    assert.equal(written(Object.create(null)), "{}");
  });

  test("EngineKey: a key that only the engine writes, at any depth", () => {
    const error = writeRefused({ a: [{ __h_task_result__: true }] });

    assert.deepEqual([error.refusal, error.path], ["EngineKey", "$.<input>[0].<input>"]);
  });

  test("NotJsonNative: a Date, a Map, a class instance, a function, a symbol, a symbol key", () => {
    for (const value of [new Date(0), new Map(), new Point(), () => 1, Symbol("s"), { [Symbol("k")]: 1 }]) {
      assert.equal(writeRefused(value).refusal, "NotJsonNative");
    }
  });

  test("ExplicitUndefined, NonFiniteFloat, LoneSurrogate, IntegerTooLong", () => {
    assert.equal(writeRefused({ a: undefined }).refusal, "ExplicitUndefined");
    assert.equal(writeRefused([1, undefined]).refusal, "ExplicitUndefined");
    assert.equal(writeRefused({ a: Number.NaN }).refusal, "NonFiniteFloat");
    assert.equal(writeRefused({ "\ud800": 1 }).refusal, "LoneSurrogate");
    assert.equal(writeRefused(10n ** 4300n).refusal, "IntegerTooLong");
  });

  test(`TooDeep: more than ${MAX_VALUE_JSON_DEPTH} levels; ${MAX_VALUE_JSON_DEPTH} are written`, () => {
    assert.equal(written(nested(MAX_VALUE_JSON_DEPTH)).length, 401);
    assert.equal(writeRefused(nested(MAX_VALUE_JSON_DEPTH + 1)).refusal, "TooDeep");
  });
});

describe("h.json() reader", () => {
  test("an integer text in the safe range is a number, outside it a bigint; a float text is a number", () => {
    assert.deepEqual(read("[7,18446744073709551616,0.5,7.0,1e+16]"), [7, 2n ** 64n, 0.5, 7, 1e16]);
    assert(Object.is(read("-0.0"), -0));
  });

  test("a stored key __proto__ is an own data property", () => {
    const value = read('{"__proto__":{"a":1}}');

    assert(isPlainObject(value));
    assert.equal(Object.getPrototypeOf(value), Object.prototype);
    assert.deepEqual(Object.keys(value), ["__proto__"]);
  });

  test("EngineKey and TooDeep", () => {
    assert.equal(readRefused('{"a":{"__horsies_x":1}}').refusal, "EngineKey");
    const levels = MAX_VALUE_JSON_DEPTH + 1;

    assert.equal(readRefused(`${"[".repeat(levels)}${"]".repeat(levels)}`).refusal, "TooDeep");
  });
});
