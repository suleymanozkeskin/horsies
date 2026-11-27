// One public name for the built-in task codes: the public `BuiltInTaskCode` is the generated `BuiltInTaskCode`, which
// the event types name, and the codec's list holds the same codes in the same order as the generated list until the
// codec reads the generated list (#2372).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  BUILT_IN_TASK_CODES as GENERATED_BUILT_IN_TASK_CODES,
  type BuiltInTaskCode as GeneratedBuiltInTaskCode,
} from "../../../horsies/src/_catalog/task_code.ts";
import { BUILT_IN_TASK_CODES } from "../../../horsies/src/_catalog/task_code.ts";
import type { BuiltInTaskCode } from "../../../horsies/src/wire/task-error.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

/** Compiles only when `A` and `B` are the same type. */
const sameType = <A, B>(same: Equal<A, B>): Equal<A, B> => same;

describe("the name of the built-in task codes", () => {
  test("BuiltInTaskCode is the generated set, and the codec's list is the same set", () => {
    assert.equal(sameType<BuiltInTaskCode, GeneratedBuiltInTaskCode>(true), true);
    assert.equal(sameType<(typeof BUILT_IN_TASK_CODES)[number], BuiltInTaskCode>(true), true);
    assert.deepEqual([...BUILT_IN_TASK_CODES], [...GENERATED_BUILT_IN_TASK_CODES]);
  });
});
