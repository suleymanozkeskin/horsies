// One public name for the built-in task codes: the public `BuiltInTaskCode` is the generated `BuiltInTaskCode`, which
// the event types name. The codec reads the generated list, so no second list exists to compare.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { BuiltInTaskCode as GeneratedBuiltInTaskCode } from "../../../horsies/src/_catalog/task_code.ts";
import type { BuiltInTaskCode } from "../../../horsies/src/wire/task-error.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

/** Compiles only when `A` and `B` are the same type. */
const sameType = <A, B>(same: Equal<A, B>): Equal<A, B> => same;

describe("the name of the built-in task codes", () => {
  test("BuiltInTaskCode is the generated set", () => {
    assert.equal(sameType<BuiltInTaskCode, GeneratedBuiltInTaskCode>(true), true);
  });
});
