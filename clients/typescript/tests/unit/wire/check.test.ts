import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { defineTask } from "../../../horsies/src/contract/define-task.ts";
import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import { checkKind, checkParameters } from "../../../horsies/src/wire/check.ts";
import { checkedKind, conversionKind, INT_KIND, type Kind, objectKind } from "../../../horsies/src/wire/kinds.ts";
import { MAX_KIND_WALK_DEPTH } from "../../../horsies/src/wire/lazy.ts";
import { ok } from "../../../horsies/src/wire/result.ts";
import type { RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf, type Wire } from "../../../horsies/src/wire/wire-type.ts";

/** An `h.taskOutcome` parameter of an upstream task of output `output`. */
function outcomeOf(output: RequiredWire): Wire<unknown, never, "required", "taskOutcome"> {
  return h.taskOutcome(defineTask({ name: "upstream", input: {}, output }));
}

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

  test("DefaultNotStored: the first value of a default function, which the check calls once", () => {
    let calls = 0;
    const kind = kindOf(h.object({ ratio: h.float().default(() => ((calls += 1), Number.NaN)) }));

    assert.deepEqual(checkKind(kind), [
      { rule: "DefaultNotStored", path: "$.ratio", detail: "the default function gives a refused value: NonFiniteFloat" },
    ]);
    assert.equal(calls, 1);
    assert.deepEqual(checkKind(kindOf(h.object({ id: h.uuid().default(() => crypto.randomUUID()) }))), []);
  });

  test("a recursive kind (h.lazy through an array or an object) has no refusal; its kind is walked once", () => {
    type TreeNode = { name: string; children: TreeNode[]; next?: TreeNode | null };

    const tree: Wire<TreeNode> = h.object({
      name: h.string(),
      children: h.array(h.lazy(() => tree)),
      next: h.lazy(() => tree).nullable().optional(),
    });

    assert.deepEqual(checkKind(kindOf(tree)), []);
  });

  test("RecursiveWithoutLevel: a lazy kind that reaches itself through a nullable or a union", () => {
    const nullable: RequiredWire = h.lazy(() => nullable.nullable());
    const union: RequiredWire = h.union([h.string(), h.lazy(() => union)]);

    assert.deepEqual(
      checkKind(kindOf(h.object({ a: nullable }))).map((refusal) => [refusal.rule, refusal.path]),
      [["RecursiveWithoutLevel", "$.a"]],
    );
    assert.deepEqual(
      checkKind(kindOf(union)).map((refusal) => refusal.rule),
      ["RecursiveWithoutLevel"],
    );
  });

  test("RecursiveWithoutLevel: a chain of new lazy kinds with no level fails closed at MAX_KIND_WALK_DEPTH", () => {
    const chain = (): RequiredWire => h.lazy(() => chain().nullable());
    const refusals = checkKind(kindOf(chain()));

    assert.deepEqual(refusals, [
      {
        rule: "RecursiveWithoutLevel",
        path: "$",
        detail: `the lazy kind reaches more than ${MAX_KIND_WALK_DEPTH} kinds through no array or object`,
      },
    ]);
  });

  test("KindNodesTooDeep: a kind nested deeper than MAX_KIND_WALK_DEPTH kinds", () => {
    let kind: Kind = INT_KIND;

    for (let depth = 0; depth <= MAX_KIND_WALK_DEPTH; depth += 1) {
      kind = checkedKind(kind, () => ok(true));
    }

    assert.deepEqual(
      checkKind(kind).map((refusal) => [refusal.rule, refusal.path]),
      [["KindNodesTooDeep", "$"]],
    );
  });

  test("a tuple with a rest item: the rest item kind is checked at [*]", () => {
    const refusals = checkKind(kindOf(h.tuple([h.int()], h.literal(1.5))));

    assert.deepEqual(
      refusals.map((refusal) => [refusal.rule, refusal.path]),
      [["LiteralNotSafeInteger", "$[*]"]],
    );
  });

  test("EngineKeyMember: a member name that only the engine writes", () => {
    const refusals = checkKind(kindOf(h.array(h.object({ __h_task_result__: h.boolean() }))));

    assert.deepEqual(
      refusals.map((refusal) => [refusal.rule, refusal.path]),
      [["EngineKeyMember", "$[*].__h_task_result__"]],
    );
  });

  test("IntegerLikeMember: an integer-like member name of h.object, at every depth", () => {
    const refusals = checkKind(kindOf(h.array(h.object({ b: h.int(), "10": h.int() }))));

    assert.deepEqual(
      refusals.map((refusal) => [refusal.rule, refusal.path]),
      [["IntegerLikeMember", "$[*].10"]],
    );
  });

  test("IntegerLikeMember: exactly the names that JavaScript puts first in an object", () => {
    for (const [name, integerLike] of [
      ["0", true],
      ["01", false],
      ["4294967294", true],
      ["4294967295", false],
      ["-1", false],
      ["1.0", false],
      ["-0", false],
      ["+1", false],
      ["1e3", false],
    ] as const) {
      const rules = checkKind(kindOf(h.object({ b: h.int(), [name]: h.int() }))).map((refusal) => refusal.rule);

      assert.deepEqual(rules, integerLike ? ["IntegerLikeMember"] : [], name);
      assert.equal(Object.keys({ b: 1, [name]: 2 })[0] === name, integerLike, `the runtime's key order of ${name}`);
    }
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
    const upstream = outcomeOf(h.object({ id: h.int() }));
    const parameters = parametersKind({ a: upstream, b: upstream.optional(), c: upstream.nullable(), d: h.int() });

    assert.deepEqual(checkParameters(parameters), []);
  });

  test("checkParameters looks inside a nullable upstream outcome and refuses one below a conversion", () => {
    const upstream = kindOf(outcomeOf(h.union([h.int(), h.float()])).nullable());
    const parameters = objectKind([{ name: "order", kind: upstream, presence: { mode: "required" } }]);

    assert.deepEqual(
      checkParameters(parameters).map((refusal) => [refusal.rule, refusal.path]),
      [["AmbiguousUnion", "$.order"]],
    );

    const converted = objectKind([
      {
        name: "order",
        kind: conversionKind(kindOf(outcomeOf(h.int())), ok, ok),
        presence: { mode: "required" },
      },
    ]);

    assert.equal(checkParameters(converted)[0]?.rule, "TaskOutcomePlacement");
  });

  test("TaskOutcomePlacement: an upstream outcome as a value, or below a parameter", () => {
    const upstream = kindOf(outcomeOf(h.int()));

    assert.equal(checkKind(upstream)[0]?.rule, "TaskOutcomePlacement");

    const nested = objectKind([
      { name: "list", kind: { node: "array", item: upstream }, presence: { mode: "required" } },
    ]);

    assert.deepEqual(
      checkParameters(nested).map((refusal) => [refusal.rule, refusal.path]),
      [["TaskOutcomePlacement", "$.list[*]"]],
    );
  });

  test("checkParameters checks the upstream output kind and the parameter names", () => {
    const parameters = parametersKind({
      __h_x: h.int(),
      order: outcomeOf(h.union([h.int(), h.float()])),
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
