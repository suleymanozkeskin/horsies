// Integer-like record keys: a JavaScript object puts integer-like keys first, in ascending order. This client keeps
// the JavaScript order; the typescript_drift row map_integer_like_keys of task_input.json pins both texts
// (tests/stored-forms/).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeValue } from "../../../horsies/src/codec/reader.ts";
import { isPlainObject } from "../../../horsies/src/codec/values.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

const PYTHON_TEXT = '{"b":1,"10":2,"2":3}';
const JAVASCRIPT_TEXT = '{"2":3,"10":2,"b":1}';

describe("integer-like record keys", () => {
  for (const [label, wire] of [
    ["record", h.record(h.int())],
    ["json", h.json()],
  ] as const) {
    test(`${label}: written in the JavaScript order; the same value always writes the same text`, () => {
      const value: Record<string, number> = { b: 1, "10": 2, "2": 3 };
      const written = encodeValue(kindOf(wire), value);

      assert.deepEqual(written, { ok: true, value: JAVASCRIPT_TEXT });
    });

    test(`${label}: Python's text reads, and the value writes the JavaScript order`, () => {
      const read = decodeValue(kindOf(wire), jsonTextCheckedByCore(PYTHON_TEXT), "worker");

      assert(read.ok && isPlainObject(read.value));
      assert.deepEqual(Object.keys(read.value), ["2", "10", "b"]);
      assert.deepEqual(encodeValue(kindOf(wire), read.value), { ok: true, value: JAVASCRIPT_TEXT });
    });
  }
});
