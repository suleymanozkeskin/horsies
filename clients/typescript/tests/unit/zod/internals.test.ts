// The internals types of the adapter (`src/zod/internals.ts`), which import no zod, against the declarations of the
// installed zod: each zod schema fits `ZodSchema`, each check fits `ZodCheck`, each definition of `$ZodTypes` fits the
// member of `ZodDefinitionData` with its type, and both list the same definition types. The checks are types, so
// `bun check` and the compiler lanes fail on a difference: a zod release that changes a field that the reader reads,
// or adds a definition type, fails the type check of this file until the reader reads it.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type * as core from "zod/v4/core";

import type { ZodCheck, ZodDefinitionData, ZodSchema } from "../../../horsies/src/zod/internals.ts";

/** The definitions of the schema types of the installed zod. */
type ZodDefinitions = core.$ZodTypes["_zod"]["def"];

/** `true` when the zod definition `D` fits the member of `ZodDefinitionData` with its type, for each member of `D`. */
type Fits<D> = D extends { readonly type: infer T }
  ? [D] extends [Extract<ZodDefinitionData, { readonly type: T }>]
    ? true
    : false
  : false;

/** `true` when every member of `T` is `true`. */
type Every<T> = [T] extends [true] ? true : false;

/** `true` when `A` and `B` hold the same members. */
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

const schemasFit: [core.$ZodType] extends [ZodSchema] ? true : false = true;

const checksFit: [core.$ZodCheck<never>] extends [ZodCheck] ? true : false = true;

const definitionsFit: Every<Fits<ZodDefinitions>> = true;

const sameDefinitionTypes: Same<ZodDefinitions["type"], ZodDefinitionData["type"]> = true;

describe("the zod internals types", () => {
  test("fit the declarations of the installed zod (the checks above are types)", () => {
    assert.deepEqual([schemasFit, checksFit, definitionsFit, sameDefinitionTypes], [true, true, true, true]);
  });
});
