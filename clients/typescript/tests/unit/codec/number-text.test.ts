import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_JSON_DEPTH } from "../../../horsies/src/_catalog/codec_bounds.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import {
  IntegralFloat,
  isTreeArray,
  isTreeObject,
  readTree,
  type TreeObject,
  type TreeValue,
} from "../../../horsies/src/codec/number-text.ts";

function read(text: string): TreeValue {
  return readTree(jsonTextCheckedByCore(text));
}

function readObject(text: string): TreeObject {
  const node = read(text);

  assert(isTreeObject(node), "an object");

  return node;
}

function nested(levels: number): string {
  return "[".repeat(levels) + "]".repeat(levels);
}

describe("number text", () => {
  test("an integer text in the safe range is a number", () => {
    assert.equal(read("7"), 7);
    assert.equal(read("-42"), -42);
    assert.equal(read("9007199254740991"), Number.MAX_SAFE_INTEGER);
  });

  test("the integer text -0 is the number 0, as Python's int", () => {
    assert(Object.is(read("-0"), 0));
  });

  test("an integer text outside the safe range is a bigint with every digit", () => {
    assert.equal(read("9007199254740992"), 9007199254740992n);
    assert.equal(read("-9223372036854775809"), -9223372036854775809n);
    assert.equal(read("340282366920938463463374607431768211456"), 340282366920938463463374607431768211456n);
  });

  test("a float text with a non-integral value is a number", () => {
    assert.equal(read("0.1"), 0.1);
    assert.equal(read("1e-05"), 1e-5);
    assert.equal(read("-2.5E-3"), -0.0025);
  });

  test("a float text with an integral value is an IntegralFloat", () => {
    for (const [text, value] of [
      ["7.0", 7],
      ["1e+16", 1e16],
      ["-2.5E+3", -2500],
      ["123.0", 123],
    ] as const) {
      const node = read(text);

      assert(node instanceof IntegralFloat, text);
      assert.equal(node.value, value);
    }

    const negativeZero = read("-0.0");

    assert(negativeZero instanceof IntegralFloat);
    assert(Object.is(negativeZero.value, -0));
  });
});

describe("tree of a JSON text", () => {
  test("words, strings, arrays and objects", () => {
    const list = read(' [true, false, null, "a", {"b": [1]}] ');

    assert(isTreeArray(list));
    assert.deepEqual(list.slice(0, 4), [true, false, null, "a"]);
    assert.equal(JSON.stringify(list[4]), '{"b":[1]}');
  });

  test("each escape of a string", () => {
    assert.equal(read(String.raw`"\"\\\/\b\f\n\r\t\u00e9\ud83d\udc0e"`), '"\\/\b\f\n\r\té🐎');
  });

  test("an object has no prototype, so __proto__ is an own member", () => {
    const node = readObject('{"__proto__":1,"a":2}');

    assert.equal(Object.getPrototypeOf(node), null);
    assert.deepEqual(Object.keys(node), ["__proto__", "a"]);
  });

  test("a repeated key keeps the last value: the core refuses repeated keys first", () => {
    assert.deepEqual(Object.entries(readObject('{"a":1,"a":2}')), [["a", 2]]);
  });

  test(`${MAX_JSON_DEPTH} levels read`, () => {
    assert(isTreeArray(read(nested(MAX_JSON_DEPTH))));
  });

  test("a text that is not JSON is a defect of the path that made the JsonText: TypeError", () => {
    for (const text of [
      "",
      "NaN",
      '{"a":1} {}',
      '"open',
      '"tab\tinside"',
      String.raw`"\x"`,
      String.raw`"\u12"`,
      '{"a" 1}',
      '{"a":1 "b":2}',
      "[1 2]",
      "{1:2}",
      "tru",
      nested(MAX_JSON_DEPTH + 1),
    ]) {
      assert.throws(() => read(text), TypeError, JSON.stringify(text.slice(0, 20)));
    }
  });
});
