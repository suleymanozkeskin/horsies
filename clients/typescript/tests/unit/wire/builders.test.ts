import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { h, kindOf } from "../../../horsies/src/wire/builders.ts";
import type { AnyWire } from "../../../horsies/src/wire/infer.ts";

describe("h.* builders", () => {
  test("each scalar builder holds its kind node", () => {
    assert.equal(kindOf(h.null()).node, "null");
    assert.equal(kindOf(h.boolean()).node, "boolean");
    assert.equal(kindOf(h.string()).node, "string");
    assert.equal(kindOf(h.int()).node, "int");
    assert.equal(kindOf(h.bigint()).node, "bigint");
    assert.equal(kindOf(h.float()).node, "float");
  });

  test("a wire type, its kind and h are frozen", () => {
    const wire = h.int();

    assert(Object.isFrozen(wire));
    assert(Object.isFrozen(kindOf(wire)));
    assert(Object.isFrozen(h));
  });

  test("a value that h.* did not build is a defect: TypeError", () => {
    const foreign: AnyWire = JSON.parse("{}");

    assert.throws(() => kindOf(foreign), TypeError);
  });
});
