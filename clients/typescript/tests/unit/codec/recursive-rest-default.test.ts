// One test per outcome of the writer, the reader, the read-back and the check view for a lazy kind (a recursive
// type), a tuple with a rest item, and a computed default (a default function).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { checkView } from "../../../horsies/src/codec/check-view.ts";
import type { CheckFailure } from "../../../horsies/src/codec/conversion.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { encodeWithReadBack } from "../../../horsies/src/codec/read-back.ts";
import { type DecodeForm, decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefused, EncodeRefused } from "../../../horsies/src/codec/refusal.ts";
import { isNumber, isPlainObject } from "../../../horsies/src/codec/values.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire, RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { checkedKind } from "../../../horsies/src/wire/kinds.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import { kindOf, type Wire } from "../../../horsies/src/wire/wire-type.ts";

type TreeNode = { name: string; children: TreeNode[] };

const TREE: Wire<TreeNode> = h.object({ name: h.string(), children: h.array(h.lazy(() => TREE)) });

const TOO_LARGE: CheckFailure = { issues: [{ path: "$.id", code: "too_big" }] };

function written<V>(wire: AnyWire, value: V): Result<string, EncodeRefused> {
  return encodeValue(kindOf(wire), value);
}

function decoded(wire: AnyWire, text: string, form: DecodeForm): Result<unknown, DecodeRefused> {
  return decodeValue(kindOf(wire), jsonTextCheckedByCore(text), form);
}

function refusalOf(outcome: Result<unknown, EncodeRefused | DecodeRefused>): readonly [string, string] {
  assert(!outcome.ok, "expected a refusal");
  assert(outcome.error.kind !== "envelopeRefused");

  return [outcome.error.refusal, outcome.error.path];
}

/** A check of an object value: its `id` is a number of at most 1. */
function atMostOne<V>(value: V): Result<true, CheckFailure> {
  const id = isPlainObject(value) ? value["id"] : undefined;

  return isNumber(id) && id <= 1 ? ok<true>(true) : err(TOO_LARGE);
}

/** A tree of `count` nodes, each in the children array of the one above it: 2 * `count` levels. */
function chainOf(count: number): TreeNode {
  let node: TreeNode = { name: "leaf", children: [] };

  for (let index = 1; index < count; index += 1) {
    node = { name: "node", children: [node] };
  }

  return node;
}

describe("a lazy kind", () => {
  test("a tree encodes in declaration order and decodes to the same value", () => {
    const value = { children: [{ name: "b", children: [] }], name: "a" };
    const text = '{"name":"a","children":[{"name":"b","children":[]}]}';

    assert.deepEqual(written(TREE, value), ok(text));
    assert.deepEqual(decoded(TREE, text, "worker"), ok({ name: "a", children: [{ name: "b", children: [] }] }));
  });

  test("200 levels pass; a value of 201 levels is TooDeep at encode and at decode", () => {
    const deepest = chainOf(MAX_VALUE_JSON_DEPTH / 2);
    const tooDeep = chainOf(MAX_VALUE_JSON_DEPTH / 2 + 1);

    assert(written(TREE, deepest).ok);
    assert.equal(refusalOf(written(TREE, tooDeep))[0], "TooDeep");
    assert.equal(refusalOf(decoded(TREE, JSON.stringify(tooDeep), "worker"))[0], "TooDeep");
  });

  test("a refusal inside the cycle names its path", () => {
    assert.deepEqual(refusalOf(written(TREE, { name: "a", children: [{ name: 1, children: [] }] })), [
      "WrongType",
      "$.children[0].name",
    ]);
  });

  test("a lazy kind with no level is a defect of a walk that checkKind did not refuse: TypeError, no stack overflow", () => {
    const loop: RequiredWire = h.lazy(() => loop.nullable());

    assert.throws(() => written(loop, 1), TypeError);
    assert.throws(() => decoded(loop, "1", "worker"), TypeError);
    assert.throws(() => checkView(kindOf(loop), 1, "$", 0), TypeError);
  });
});

describe("a tuple with a rest item", () => {
  const PAIR_THEN_FLAGS = h.tuple([h.int(), h.string()], h.boolean());

  test("0, 1 and many rest items encode and decode", () => {
    for (const value of [[1, "a"], [1, "a", true], [1, "a", true, false, true]]) {
      assert.deepEqual(written(PAIR_THEN_FLAGS, value), ok(JSON.stringify(value)));
      assert.deepEqual(decoded(PAIR_THEN_FLAGS, JSON.stringify(value), "worker"), ok(value));
    }
  });

  test("a wrong rest item is WrongType at its index", () => {
    assert.deepEqual(refusalOf(written(PAIR_THEN_FLAGS, [1, "a", true, 0])), ["WrongType", "$[3]"]);
    assert.deepEqual(refusalOf(decoded(PAIR_THEN_FLAGS, '[1,"a",true,0]', "worker")), ["WrongType", "$[3]"]);
  });

  test("fewer items than the fixed items is WrongType at the tuple; a fixed tuple refuses an extra item", () => {
    assert.deepEqual(refusalOf(written(PAIR_THEN_FLAGS, [1])), ["WrongType", "$"]);
    assert.deepEqual(refusalOf(decoded(PAIR_THEN_FLAGS, "[1]", "worker")), ["WrongType", "$"]);
    assert.deepEqual(refusalOf(written(h.tuple([h.int()]), [1, 2])), ["WrongType", "$"]);
  });

  test("the rest items of an empty fixed list: Python's tuple[T, ...]", () => {
    assert.deepEqual(written(h.tuple([], h.int()), []), ok("[]"));
    assert.deepEqual(decoded(h.tuple([], h.int()), "[1,2]", "worker"), ok([1, 2]));
  });
});

describe("a computed default", () => {
  function counter(): () => number {
    let calls = 0;

    return () => (calls += 1);
  }

  test("each worker-form decode gets a new value; the stored form leaves the member out", () => {
    const wire = h.object({ id: h.int().default(counter()), name: h.string() });

    assert.deepEqual(decoded(wire, '{"name":"a"}', "worker"), ok({ id: 1, name: "a" }));
    assert.deepEqual(decoded(wire, '{"name":"a"}', "worker"), ok({ id: 2, name: "a" }));
    assert.deepEqual(decoded(wire, '{"name":"a"}', "stored"), ok({ name: "a" }));
    assert.deepEqual(decoded(wire, '{"id":9,"name":"a"}', "worker"), ok({ id: 9, name: "a" }));
  });

  test("the read-back of a sent value that leaves it out does not change the text", () => {
    const wire = h.object({ id: h.uuid().default(() => crypto.randomUUID()), name: h.string() });

    assert.deepEqual(encodeWithReadBack(kindOf(wire), { name: "a" }), ok('{"name":"a"}'));
  });

  test("DefaultRefused: a function value that does not encode, at a worker decode", () => {
    const wire = h.object({ ratio: h.float().default(() => Number.NaN) });

    assert.deepEqual(refusalOf(decoded(wire, "{}", "worker")), ["DefaultRefused", "$.ratio"]);
  });

  test("the check view fills it once for a check; DefaultRefused at encode when its value does not encode", () => {
    const counted = checkedKind(kindOf(h.object({ id: h.int().default(counter()) })), atMostOne);

    assert.deepEqual(encodeValue(counted, {}), ok("{}"));
    assert.equal(refusalOf(encodeValue(counted, {}))[0], "CheckFailed");

    const refusedDefault = checkedKind(kindOf(h.object({ ratio: h.float().default(() => Number.NaN) })), atMostOne);

    assert.deepEqual(refusalOf(encodeValue(refusedDefault, {})), ["DefaultRefused", "$.ratio"]);
  });
});
