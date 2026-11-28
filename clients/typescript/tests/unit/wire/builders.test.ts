import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { defineTask } from "../../../horsies/src/contract/define-task.ts";
import { h, type ObjectWire, parametersKind } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire, RequiredWire } from "../../../horsies/src/wire/infer.ts";
import { ok } from "../../../horsies/src/wire/result.ts";
import { kindOf, presenceOf } from "../../../horsies/src/wire/wire-type.ts";

describe("h.* builders", () => {
  test("each scalar builder holds its kind node", () => {
    assert.equal(kindOf(h.null()).node, "null");
    assert.equal(kindOf(h.boolean()).node, "boolean");
    assert.equal(kindOf(h.string()).node, "string");
    assert.equal(kindOf(h.int()).node, "int");
    assert.equal(kindOf(h.bigint()).node, "bigint");
    assert.equal(kindOf(h.float()).node, "float");
    assert.equal(kindOf(h.uuid()).node, "uuid");
    assert.equal(kindOf(h.decimal()).node, "decimal");
    assert.equal(kindOf(h.json()).node, "json");
    assert.equal(kindOf(h.instant()).node, "instant");
    assert.equal(kindOf(h.offsetDateTime()).node, "offsetDateTime");
    assert.equal(kindOf(h.plainDateTime()).node, "plainDateTime");
    assert.equal(kindOf(h.plainDate()).node, "plainDate");
    assert.equal(kindOf(h.plainTime()).node, "plainTime");
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
      rest: { state: "none" },
    });
    assert.deepEqual(kindOf(h.record(h.float())), { node: "record", value: { node: "float" } });
  });

  test("a tuple with a rest item holds the rest item kind", () => {
    assert.deepEqual(kindOf(h.tuple([h.int()], h.boolean())), {
      node: "tuple",
      items: [{ node: "int" }],
      rest: { state: "items", item: { node: "boolean" } },
    });
    assert.deepEqual(kindOf(h.tuple([], h.string())), {
      node: "tuple",
      items: [],
      rest: { state: "items", item: { node: "string" } },
    });
  });

  test("h.lazy calls its getter at the first use only, and gives the kind of the wire type that it returns", () => {
    let calls = 0;

    const lazy = kindOf(
      h.lazy(() => {
        calls += 1;

        return h.int();
      }),
    );

    assert(lazy.node === "lazy");
    assert.equal(calls, 0);
    assert.deepEqual([lazy.target(), lazy.target(), calls], [{ node: "int" }, { node: "int" }, 1]);
    assert.deepEqual(lazy.level(), { state: "passesLevel" });
  });

  test("h.lazy of a wire type that may be absent is a defect that the static types refuse: TypeError at the first use", () => {
    // SAFETY: the static types refuse this call; the test checks the run-time guard behind them.
    const optional = h.int().optional() as AnyWire as RequiredWire;
    const lazy = kindOf(h.lazy(() => optional));

    assert(lazy.node === "lazy");
    assert.throws(() => lazy.target(), TypeError);
  });

  test("a function default is computed: the function runs at each call of compute, not at the build", () => {
    let calls = 0;
    const factory = (): number => (calls += 1);
    const presence = presenceOf(h.int().default(factory));

    assert(presence.mode === "defaulted" && presence.fallback.state === "computed");
    assert.equal(calls, 0);
    assert.equal(presence.fallback.source, factory);
    assert.deepEqual([presence.fallback.compute(), presence.fallback.compute()], [ok("1"), ok("2")]);
  });

  test("literal, enum and the two union forms", () => {
    assert.deepEqual(kindOf(h.literal("a", 1)), { node: "literal", members: ["a", 1] });
    assert.deepEqual(kindOf(h.enum(["red"])), { node: "literal", members: ["red"] });
    assert.deepEqual(kindOf(h.union([h.int(), h.string()])), {
      node: "union",
      members: [{ node: "int" }, { node: "string" }],
    });

    const variants = [h.object({ kind: h.enum(["a", "b"]) }), h.object({ kind: h.literal("c") })] as const;
    const tagged = kindOf(h.union("kind", variants));

    assert(tagged.node === "taggedUnion");
    assert.deepEqual([...tagged.byTag.keys()], ["a", "b", "c"]);
  });

  test("a tagged union member that is not an object is a defect that the static types refuse: TypeError", () => {
    // SAFETY: the static types refuse this call; the test checks the run-time guard behind them.
    const member = h.int() as AnyWire as ObjectWire;

    assert.throws(() => h.union("kind", [member]), TypeError);
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
    assert.equal(refused.fallback.refusal.refusal, "NonFiniteFloat");
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

  test("h.taskOutcome holds the output kind of its upstream contract; it is a parameter only", () => {
    const order = defineTask({ name: "order", input: {}, output: h.object({ id: h.int() }) });
    const upstream = h.taskOutcome(order);

    assert.deepEqual(kindOf(upstream), {
      node: "taskOutcome",
      output: { state: "compiled", kind: kindOf(h.object({ id: h.int() })) },
    });
    assert.equal(parametersKind({ order: upstream }).members[0]?.kind.node, "taskOutcome");

    // SAFETY: the static types refuse these calls; the test checks the run-time guard behind them.
    const asValue = upstream as AnyWire as RequiredWire;

    assert.throws(() => h.array(asValue), TypeError);
    assert.throws(() => h.object({ order: asValue }), TypeError);
    // SAFETY: the static types refuse this call; the test checks the run-time guard behind them.
    const nullableAsValue = h.taskOutcome(order).nullable() as AnyWire as RequiredWire;

    assert.throws(() => h.object({ order: nullableAsValue }), TypeError);
  });

  test("a value that h.* did not build is a defect: TypeError", () => {
    const foreign: RequiredWire = JSON.parse("{}");

    assert.throws(() => kindOf(foreign), TypeError);
    assert.throws(() => h.object({ a: foreign }), TypeError);
  });
});
