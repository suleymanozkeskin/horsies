// One public name for the library's task codes: `LibraryTaskCode` is the generated `BuiltInTaskCode`, which the
// event types name, and the codec's list holds the same codes in the same order until the codec reads the generated
// list (#2372).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { BUILT_IN_TASK_CODES, type BuiltInTaskCode } from "../../../horsies/src/_catalog/task_code.ts";
import { LIBRARY_TASK_CODES } from "../../../horsies/src/codec/bounds.ts";
import type { LibraryTaskCode } from "../../../horsies/src/wire/task-error.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

/** Compiles only when `A` and `B` are the same type. */
const sameType = <A, B>(same: Equal<A, B>): Equal<A, B> => same;

describe("the name of the library's task codes", () => {
  test("LibraryTaskCode is the generated set, and the codec's list is the same set", () => {
    assert.equal(sameType<LibraryTaskCode, BuiltInTaskCode>(true), true);
    assert.equal(sameType<(typeof LIBRARY_TASK_CODES)[number], LibraryTaskCode>(true), true);
    assert.deepEqual([...LIBRARY_TASK_CODES], [...BUILT_IN_TASK_CODES]);
  });
});
