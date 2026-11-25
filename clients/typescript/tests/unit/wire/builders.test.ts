import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { h, parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire, RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { kindOf, presenceOf } from "../../../horsies/src/wire/wire-type.ts";

describe("h.* builders", () => {
  test("each scalar builder holds its kind node", () => {
    assert.equal(kindOf(h.null()).node, "null");
    assert.equal(kindOf(h.boolean()).node, "boolean");
    assert.equal(kindOf(h.string()).node, "string");
    assert.equal(kindOf(h.int()).node, "int");
    assert.equal(kindOf(h.bigint()).node, "bigint");
    assert.equal(kindOf(h.float()).node, "float");
  });

  test("h.object keeps the declaration order and the presence of each member", () => {
    const kind = kindOf(h.object({ b: h.int(), a: h.string().optional(), c: h.float().default(1) }));

    assert.equal(kind.node, "object");
    assert(kind.node === "object");
    assert.deepEqual(
      kind.members.map((member) => [member.name, member.presence.mode]),
      [
        ["b", "required"],
        ["a", "optional"],
        ["c", "defaulted"],
      ],
    );
  });

  test("array, tuple and record hold their item kinds", () => {
    assert.deepEqual(kindOf(h.array(h.int())), { node: "array", item: { node: "int" } });
    assert.deepEqual(kindOf(h.tuple([h.int(), h.string()])), {
      node: "tuple",
      items: [{ node: "int" }, { node: "string" }],
    });
    assert.deepEqual(kindOf(h.record(h.float())), { node: "record", value: { node: "float" } });
  });

  test("modifiers: optional and default set the presence; nullable wraps once and keeps the presence", () => {
    assert.equal(presenceOf(h.int().optional()).mode, "optional");
    assert.equal(presenceOf(h.int().default(1)).mode, "defaulted");
    assert.deepEqual(kindOf(h.int().nullable().nullable()), { node: "nullable", inner: { node: "int" } });
    assert.equal(presenceOf(h.int().optional().nullable()).mode, "optional");
  });

  test("a default holds its stored text, or the writer's refusal", () => {
    const stored = presenceOf(h.array(h.int()).default([1, 2]));
    const refused = presenceOf(h.float().default(Number.NaN));

    assert.deepEqual(stored, { mode: "defaulted", fallback: { state: "stored", text: "[1,2]" } });
    assert(refused.mode === "defaulted" && refused.fallback.state === "refused");
    assert.equal(refused.fallback.refusal.kind, "NonFiniteFloat");
  });

  test("parametersKind is the object kind of a task input", () => {
    assert.deepEqual(
      kindOf(h.object({ a: h.int() })),
      parametersKind({ a: h.int() }),
    );
  });

  test("a wire type, its kind and h are frozen", () => {
    const wire = h.object({ a: h.int() });

    assert(Object.isFrozen(wire));
    assert(Object.isFrozen(kindOf(wire)));
    assert(Object.isFrozen(h));
  });

  test("an item that may be absent is a defect that the static types refuse: TypeError", () => {
    const optional: AnyWire = h.int().optional();
    // SAFETY: the static types refuse this call; the test checks the run-time guard behind them.
    const item = optional as RequiredWire;

    assert.throws(() => h.array(item), TypeError);
    assert.throws(() => h.tuple([item]), TypeError);
    assert.throws(() => h.record(item), TypeError);
  });

  test("a value that h.* did not build is a defect: TypeError", () => {
    const foreign: AnyWire = JSON.parse("{}");

    assert.throws(() => kindOf(foreign), TypeError);
    assert.throws(() => h.object({ a: foreign }), TypeError);
  });
});
