// One schema per refusal rule of the zod adapter: the rule, the path in the schema and zod's definition type.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { h } from "../../../horsies/src/wire/builders.ts";
import { checkKind } from "../../../horsies/src/wire/check.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { MAX_SCHEMA_DEPTH, compileMember, compileValue } from "../../../horsies/src/zod/adapter.ts";
import type { ZodSchema } from "../../../horsies/src/zod/definition.ts";
import { zodValueWire } from "../../../horsies/src/zod/kind.ts";
import { type ZodSchemaRefused, zodRefusalText } from "../../../horsies/src/zod/rules.ts";
import { acceptedKind, withDefinition } from "./support.ts";

/** The rule, path and zod type of each refusal of a schema as a whole value. */
function refusals(schema: ZodSchema): readonly (readonly [string, string, string])[] {
  const compiled = compileValue(schema);

  assert.ok(!compiled.ok, "expected a refusal");

  return compiled.error.map((refused: ZodSchemaRefused) => [refused.rule, refused.path, refused.zodType] as const);
}

/** The rule and path of each `checkKind` refusal of a schema as a whole value, through the entry of the contract compile. */
function kindRefusals(schema: z.ZodType): readonly (readonly [string, string])[] {
  const outcome = zodValueWire(schema);

  assert.ok(!outcome.ok && outcome.error.kind === "KindRefused", "expected a KindRefused");

  return outcome.error.refusals.map((refused) => [refused.rule, refused.path] as const);
}

/** The help of the first `checkKind` refusal of a schema. */
function kindHelp(schema: z.ZodType): readonly string[] {
  const outcome = zodValueWire(schema);

  assert.ok(!outcome.ok && outcome.error.kind === "KindRefused", "expected a KindRefused");

  return outcome.error.refusals[0]?.help ?? [];
}

describe("zod adapter: refusal rules", () => {
  test("UNTYPED: z.any(), z.unknown()", () => {
    assert.deepEqual(refusals(z.object({ a: z.any(), b: z.unknown() })), [
      ["UNTYPED", "$.a", "any"],
      ["UNTYPED", "$.b", "unknown"],
    ]);
  });

  test("TRANSFORM: .transform(), z.preprocess(), .pipe() without a codec, z.success(), a check that replaces", () => {
    assert.deepEqual(refusals(z.string().transform((text) => text.length)), [["TRANSFORM", "$", "pipe"]]);
    assert.deepEqual(refusals(z.preprocess((value) => value, z.string())), [["TRANSFORM", "$", "pipe"]]);
    assert.deepEqual(refusals(z.string().pipe(z.email())), [["TRANSFORM", "$", "pipe"]]);
    assert.deepEqual(refusals(z.success(z.string())), [["TRANSFORM", "$", "success"]]);
    assert.deepEqual(refusals(z.object({ sku: z.string().trim() })), [["TRANSFORM", "$.sku", "string"]]);
    assert.deepEqual(refusals(z.string().toLowerCase()), [["TRANSFORM", "$", "string"]]);
  });

  test("CATCH: .catch()", () => {
    assert.deepEqual(refusals(z.int().catch(0)), [["CATCH", "$", "catch"]]);
  });

  test("DATE: z.date()", () => {
    assert.deepEqual(refusals(z.object({ at: z.date() })), [["DATE", "$.at", "date"]]);
  });

  test("LOOSE_OBJECT: z.looseObject(), .passthrough(), .catchall()", () => {
    assert.deepEqual(refusals(z.looseObject({ a: z.int() })), [["LOOSE_OBJECT", "$", "object"]]);
    assert.deepEqual(refusals(z.object({}).passthrough()), [["LOOSE_OBJECT", "$", "object"]]);
    assert.deepEqual(refusals(z.object({}).catchall(z.string())), [["LOOSE_OBJECT", "$", "object"]]);
  });

  test("NOT_DATA: maps, sets, symbols, functions, promises, files, instances, NaN, never, void, undefined", () => {
    const schemas: readonly ZodSchema[] = [
      z.map(z.string(), z.int()),
      z.set(z.int()),
      z.symbol(),
      z.function(),
      z.promise(z.int()),
      z.file(),
      z.instanceof(Uint8Array),
      z.custom<string>(),
      z.nan(),
      z.never(),
      z.void(),
      z.undefined(),
    ];

    for (const schema of schemas) {
      assert.equal(refusals(schema)[0]?.[0], "NOT_DATA");
    }
  });

  test("UNION_FALLBACK: a discriminated union with unionFallback", () => {
    const fallback = z.discriminatedUnion("kind", [z.object({ kind: z.literal("a") })], { unionFallback: true });

    assert.deepEqual(refusals(fallback), [["UNION_FALLBACK", "$", "union"]]);
  });

  test("a union with an object, array or record member compiles; checkKind refuses it with UnionMemberNotPrimitive and the zod fix", () => {
    for (const schema of [
      z.union([z.object({ a: z.int() }), z.string()]),
      z.union([z.array(z.int()), z.int()]),
      z.union([z.record(z.string(), z.int()), z.json()]),
    ]) {
      assert.deepEqual(kindRefusals(schema), [["UnionMemberNotPrimitive", "$"]]);
      assert.deepEqual(kindHelp(schema), ["use z.discriminatedUnion() of objects for a union with an object, array or record member"]);
    }
  });

  test("a union with two members that store the same JSON type compiles; checkKind refuses it with AmbiguousUnion and the zod fix", () => {
    for (const schema of [z.union([z.int(), z.number()]), z.union([z.bigint(), z.number().nullable()])]) {
      assert.deepEqual(kindRefusals(schema), [["AmbiguousUnion", "$"]]);
      assert.deepEqual(kindHelp(schema), ["use union members that store different JSON types: one number kind (z.int() or z.number()), not two"]);
    }
  });

  test("RECORD_KEY: a key schema that is not a string, a string format, a string enum or a string literal", () => {
    assert.deepEqual(refusals(z.record(z.int(), z.string())), [["RECORD_KEY", "$", "record"]]);
    assert.deepEqual(refusals(z.record(z.enum({ One: 1 }), z.string())), [["RECORD_KEY", "$", "record"]]);
  });

  test("ENGINE_KEY: an object member whose name only the engine writes", () => {
    assert.deepEqual(refusals(z.object({ __horsies_x: z.int(), __builtin_task_code__: z.string() })), [
      ["ENGINE_KEY", "$.__horsies_x", "object"],
      ["ENGINE_KEY", "$.__builtin_task_code__", "object"],
    ]);
  });

  test("UNKNOWN_DEFINITION: a definition type that this version does not know", () => {
    const known = z.string();
    const future = withDefinition(known, { ...known._zod.def, type: "future" });

    assert.deepEqual(refusals(future), [["UNKNOWN_DEFINITION", "$", "future"]]);
  });

  test("INTERSECTION: z.intersection(), .and()", () => {
    assert.deepEqual(refusals(z.object({ a: z.int() }).and(z.object({ b: z.int() }))), [
      ["INTERSECTION", "$", "intersection"],
    ]);
  });

  test("OPTIONAL_NOT_A_MEMBER: .optional(), .default() outside an object member, also at the top of a value", () => {
    assert.deepEqual(refusals(z.array(z.string().optional())), [["OPTIONAL_NOT_A_MEMBER", "$[*]", "optional"]]);
    assert.deepEqual(refusals(z.tuple([z.int().default(1)])), [["OPTIONAL_NOT_A_MEMBER", "$[0]", "default"]]);
    assert.deepEqual(refusals(z.record(z.string(), z.int().nullish())), [["OPTIONAL_NOT_A_MEMBER", "$.*", "optional"]]);
    assert.deepEqual(refusals(z.union([z.string().optional(), z.int()])), [
      ["OPTIONAL_NOT_A_MEMBER", "$[0]", "optional"],
    ]);
    assert.deepEqual(refusals(z.string().optional()), [["OPTIONAL_NOT_A_MEMBER", "$", "optional"]]);
    assert.equal(compileMember(z.string().optional()).ok, true);
  });

  test("CHANGING_DEFAULT: a function default whose value changes at each read", () => {
    assert.deepEqual(refusals(z.object({ id: z.uuid().default(() => crypto.randomUUID()) })), [
      ["CHANGING_DEFAULT", "$.id", "default"],
    ]);
    assert.equal(compileValue(z.object({ tags: z.array(z.string()).default(() => []) })).ok, true);
  });

  test("RECURSIVE: a schema that contains itself, through z.lazy() and through a getter", () => {
    const tree: z.ZodType = z.lazy(() => z.object({ children: z.array(tree) }));

    const category = z.object({
      name: z.string(),
      get children(): z.ZodArray<typeof category> {
        return z.array(category);
      },
    });

    assert.deepEqual(refusals(tree), [["RECURSIVE", "$.children[*]", "lazy"]]);
    assert.deepEqual(refusals(category), [["RECURSIVE", "$.children[*]", "object"]]);
  });

  test("TUPLE_REST: z.tuple([...], rest)", () => {
    assert.deepEqual(refusals(z.tuple([z.string()], z.int())), [["TUPLE_REST", "$", "tuple"]]);
  });

  test("LITERAL_MEMBER: a bigint or an undefined literal member", () => {
    assert.deepEqual(refusals(z.literal(1n)), [["LITERAL_MEMBER", "$", "literal"]]);
    assert.deepEqual(refusals(z.literal(undefined)), [["LITERAL_MEMBER", "$", "literal"]]);
  });

  test("UNION_OPTION: an option of a discriminated union that is not an object", () => {
    const optionA = z.object({ kind: z.literal("a") });
    const converted = z.codec(optionA, optionA, { decode: (value) => value, encode: (value) => value });
    const otherTag = z.discriminatedUnion("type", [z.object({ type: z.literal("x"), kind: z.literal("b") })]);

    assert.deepEqual(refusals(z.discriminatedUnion("kind", [converted, otherTag])), [
      ["UNION_OPTION", "$[0]", "pipe"],
      ["UNION_OPTION", "$[1]", "union"],
    ]);
  });

  test("TOO_DEEP: a schema nested deeper than the adapter walks", () => {
    let schema: z.ZodType = z.string();

    for (let level = 0; level < MAX_SCHEMA_DEPTH; level += 1) {
      schema = z.nullable(schema);
    }

    assert.deepEqual(refusals(schema)[0]?.slice(0, 2), ["TOO_DEEP", "$"]);
  });

  test("every refusal of a schema is reported, in walk order, with its fix", () => {
    const compiled = compileValue(z.looseObject({ at: z.date(), any: z.any() }));

    assert.ok(!compiled.ok);
    assert.deepEqual(compiled.error.map(zodRefusalText), [
      "$: zod type object is refused [LOOSE_OBJECT]; use z.object() or z.strictObject(): undeclared keys are refused",
      "$.at: zod type date is refused [DATE]; use hz.instant(): a Date holds milliseconds only",
      "$.any: zod type any is refused [UNTYPED]; use h.json() or z.json() for untyped JSON",
    ]);
  });
});

describe("zod adapter: kinds that checkKind refuses with the rules of h.*", () => {
  test("IntegerLikeMember: an integer-like member name of z.object, at the same path as for h.object", () => {
    const schema = z.object({ lines: z.array(z.object({ sku: z.string(), 10: z.int() })) });
    const wire = h.object({ lines: h.array(h.object({ sku: h.string(), 10: h.int() })) });

    assert.deepEqual(
      checkKind(acceptedKind(schema)).map((refused) => [refused.rule, refused.path]),
      [["IntegerLikeMember", "$.lines[*].10"]],
    );
    assert.deepEqual(
      checkKind(kindOf(wire)).map((refused) => [refused.rule, refused.path]),
      [["IntegerLikeMember", "$.lines[*].10"]],
    );
  });

  test("AmbiguousUnion, LiteralNotSafeInteger, DefaultNotStored: as for the h.* kinds", () => {
    assert.equal(checkKind(acceptedKind(z.union([z.string(), z.uuid()])))[0]?.rule, "AmbiguousUnion");
    assert.equal(checkKind(acceptedKind(z.literal(1.5)))[0]?.rule, "LiteralNotSafeInteger");
    const ratio = acceptedKind(z.object({ ratio: z.number().default(Number.NaN) }));

    assert.equal(checkKind(ratio)[0]?.rule, "DefaultNotStored");
  });
});
