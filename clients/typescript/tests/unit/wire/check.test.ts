import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { MAX_VALUE_JSON_DEPTH } from "../../../horsies/src/codec/bounds.ts";
import { h } from "../../../horsies/src/wire/builders.ts";
import { checkKind } from "../../../horsies/src/wire/check.ts";
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

  test("KindTooDeep: more arrays and objects than a stored value may have", () => {
    const refusals = checkKind(kindOf(nestedArrays(MAX_VALUE_JSON_DEPTH + 1)));

    assert.equal(refusals.length, 1);
    assert.equal(refusals[0]?.rule, "KindTooDeep");
  });
});
