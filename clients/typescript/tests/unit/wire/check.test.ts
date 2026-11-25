import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import { checkKind, checkParameters } from "../../../horsies/src/wire/check.ts";
import { objectKind } from "../../../horsies/src/wire/kinds.ts";
import type { RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";

function nestedArrays(levels: number): RequiredWire {
  let wire: RequiredWire = h.int();

  for (let level = 0; level < levels; level += 1) {
    wire = h.array(wire);
  }

  return wire;
}

describe("checkKind", () => {
  test("a valid kind has no refusal", () => {
    const order = h.object({
      id: h.int(),
      tags: h.array(h.string()).default([]),
      note: h.string().optional().nullable(),
      lines: h.record(h.tuple([h.int(), h.float()])),
    });

    assert.deepEqual(checkKind(kindOf(order)), []);
    assert.deepEqual(checkKind(kindOf(nestedArrays(MAX_VALUE_JSON_DEPTH))), []);
  });

  test("DefaultNotStored: a default that the writer refuses, with the writer's refusal kind", () => {
    const refusals = checkKind(kindOf(h.object({ inner: h.object({ ratio: h.float().default(Number.NaN) }) })));

    assert.deepEqual(refusals, [
      { rule: "DefaultNotStored", path: "$.inner.ratio", detail: "the default is refused: NonFiniteFloat" },
    ]);
  });

  test("EngineKeyMember: a member name that only the engine writes", () => {
    const refusals = checkKind(kindOf(h.array(h.object({ __h_task_result__: h.boolean() }))));

    assert.deepEqual(
      refusals.map((refusal) => [refusal.rule, refusal.path]),
      [["EngineKeyMember", "$[*].__h_task_result__"]],
    );
  });

  test("AmbiguousUnion: an int-float union is refused at create, and a string with an instant", () => {
    const intFloat = checkKind(kindOf(h.union([h.int(), h.float()])));

    assert.deepEqual(intFloat, [{ rule: "AmbiguousUnion", path: "$", detail: "2 members store a JSON number" }]);
    assert.equal(checkKind(kindOf(h.union([h.string(), h.instant()])))[0]?.rule, "AmbiguousUnion");
    assert.equal(checkKind(kindOf(h.union([h.int().nullable(), h.null()])))[0]?.rule, "AmbiguousUnion");
  });

  test("a union of literals and of distinct stored types has no refusal", () => {
    assert.deepEqual(checkKind(kindOf(h.union([h.literal("a"), h.literal("b")]))), []);
    assert.deepEqual(checkKind(kindOf(h.union([h.literal("auto"), h.int(), h.string(), h.null()]))), []);
  });

  test("UnionMemberNotPrimitive: an object member of h.union([...])", () => {
    assert.equal(checkKind(kindOf(h.union([h.object({}), h.int()])))[0]?.rule, "UnionMemberNotPrimitive");
    assert.equal(checkKind(kindOf(h.union([h.json(), h.int()])))[0]?.rule, "UnionMemberNotPrimitive");
  });

  test("LiteralNotSafeInteger: a float member and 2^53", () => {
    assert.equal(checkKind(kindOf(h.literal(1.5)))[0]?.rule, "LiteralNotSafeInteger");
    assert.equal(checkKind(kindOf(h.literal(2 ** 53)))[0]?.rule, "LiteralNotSafeInteger");
  });

  test("UnionTagInvalid: no tag, an optional tag, a tag that is not a string literal, a repeated tag value", () => {
    const refusals = checkKind(
      kindOf(
        h.union("kind", [
          h.object({ kind: h.literal("a") }),
          h.object({ name: h.string() }),
          h.object({ kind: h.literal("c").optional() }),
          h.object({ kind: h.literal(4) }),
          h.object({ kind: h.enum(["a", "e"]) }),
        ]),
      ),
    );

    assert.deepEqual(
      refusals.map((refusal) => [refusal.rule, refusal.path]),
      [
        ["UnionTagInvalid", "$[1]"],
        ["UnionTagInvalid", "$[2]"],
        ["UnionTagInvalid", "$[3]"],
        ["UnionTagInvalid", "$[4]"],
      ],
    );
  });

  test("a task input may have an upstream outcome as a whole parameter, optional or nullable", () => {
    const upstream = h.resultOf(h.object({ id: h.int() }));
    const parameters = parametersKind({ a: upstream, b: upstream.optional(), c: upstream.nullable(), d: h.int() });

    assert.deepEqual(checkParameters(parameters), []);
  });

  test("ResultOfPlacement: an upstream outcome as a value, or below a parameter", () => {
    const upstream = kindOf(h.resultOf(h.int()));

    assert.equal(checkKind(upstream)[0]?.rule, "ResultOfPlacement");

    const nested = objectKind([
      { name: "list", kind: { node: "array", item: upstream }, presence: { mode: "required" } },
    ]);

    assert.deepEqual(
      checkParameters(nested).map((refusal) => [refusal.rule, refusal.path]),
      [["ResultOfPlacement", "$.list[*]"]],
    );
  });

  test("checkParameters checks the upstream output kind and the parameter names", () => {
    const parameters = parametersKind({
      __h_x: h.int(),
      order: h.resultOf(h.union([h.int(), h.float()])),
    });

    assert.deepEqual(
      checkParameters(parameters).map((refusal) => [refusal.rule, refusal.path]),
      [
        ["EngineKeyMember", "$.__h_x"],
        ["AmbiguousUnion", "$.order"],
      ],
    );
  });

  test("KindTooDeep: more arrays and objects than a stored value may have", () => {
    const refusals = checkKind(kindOf(nestedArrays(MAX_VALUE_JSON_DEPTH + 1)));

    assert.equal(refusals.length, 1);
    assert.equal(refusals[0]?.rule, "KindTooDeep");
  });
});
