// The self-check of the zod adapter on the installed zod (this file runs in each zod test lane), and the entries
// that run the release check, the adapter and `checkKind` in order.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";
import * as z3 from "zod/v3";

import { kindOf, presenceOf } from "../../../horsies/src/wire/wire-type.ts";
import { hz } from "../../../horsies/src/zod/codecs.ts";
import { zodMemberWire, zodValueWire } from "../../../horsies/src/zod/kind.ts";
import { knownCases, runSelfCheck, type SelfCheckCase, type SelfCheckFailure, selfCheck } from "./self-check-cases.ts";
import { withDefinition } from "./support.ts";

/** The outcome of the self-check on one case, as its failures. */
function failuresOf(known: SelfCheckCase): readonly SelfCheckFailure[] {
  const outcome = runSelfCheck([known]);

  return outcome.ok ? [] : outcome.error;
}

describe("self-check", () => {
  test("each known schema compiles to its expected kind or refusal on the installed zod", () => {
    assert.equal(runSelfCheck(knownCases()).ok, true);
  });

  test("covers each refusal rule that a zod schema of the installed release reaches", () => {
    const rules = new Set<string>(
      knownCases().flatMap((known) => (known.expected.outcome === "refused" ? [known.expected.rule] : [])),
    );

    for (const rule of ["UNTYPED", "TRANSFORM", "CATCH", "DATE", "LOOSE_OBJECT", "NOT_DATA"]) {
      assert.ok(rules.has(rule), rule);
    }

    for (const rule of ["UNION_FALLBACK", "RECORD_KEY", "ENGINE_KEY"]) {
      assert.ok(rules.has(rule), rule);
    }

    for (const rule of ["INTERSECTION", "OPTIONAL_NOT_A_MEMBER", "CHANGING_DEFAULT", "RECURSIVE", "TUPLE_REST"]) {
      assert.ok(rules.has(rule), rule);
    }

    assert.ok(rules.has("LITERAL_MEMBER") && rules.has("UNION_OPTION"));
  });

  test("finds a renamed optional field: a loose object whose catchall field has another name", () => {
    const loose = z.looseObject({ a: z.string() });
    const { catchall, ...others } = loose._zod.def;
    const renamed = withDefinition(loose, { ...others, rest: catchall });

    assert.deepEqual(
      failuresOf({
        name: "loose",
        schema: renamed,
        expected: { outcome: "refused", rule: "LOOSE_OBJECT" },
        probes: [],
      }),
      [{ name: "loose", expected: "LOOSE_OBJECT", found: "UNKNOWN_DEFINITION_FIELD" }],
    );
  });

  test("finds a check that stopped running: a probe that the kind writes but should refuse", () => {
    const record = z.record(z.enum(["a"]), z.boolean());
    const stripped = withDefinition(record, { ...record._zod.def, keyType: z.string() });

    const known: SelfCheckCase = {
      name: "enum record",
      schema: stripped,
      expected: { outcome: "kind", text: "record(boolean)" },
      probes: [{ value: {}, written: "refused" }],
    };

    assert.deepEqual(failuresOf(known), [{ name: "enum record", expected: "{} refused", found: "stored" }]);
  });

  test("a renamed required field is an unknown field: the reader reads no field that it does not know", () => {
    const lazy = z.lazy(() => z.string());
    const renamed = withDefinition(lazy, { type: "lazy", get: lazy._zod.def.getter });

    const known: SelfCheckCase = {
      name: "renamed",
      schema: renamed,
      expected: { outcome: "kind", text: "string" },
      probes: [],
    };

    assert.deepEqual(failuresOf(known), [
      { name: "renamed", expected: "string", found: "UNKNOWN_DEFINITION_FIELD" },
    ]);
  });

  test("a removed required field gives a failure, not a throw: getter, shape, innerType", () => {
    const lazy = z.lazy(() => z.string());
    const object = z.object({ a: z.string() });
    const nullable = z.string().nullable();

    const stubs = [
      withDefinition(lazy, { type: "lazy" }),
      withDefinition(object, { type: "object" }),
      withDefinition(nullable, { type: "nullable" }),
    ];

    for (const schema of stubs) {
      const expected: SelfCheckCase["expected"] = { outcome: "kind", text: "string" };
      const failures = failuresOf({ name: "removed", schema, expected, probes: [] });

      assert.equal(failures.length, 1);
      assert.match(failures[0]?.found ?? "", /^throws TypeError: /);
    }
  });

  test("a throw while the known schemas are built is one failure", () => {
    const outcome = runSelfCheck({ name: "known schemas", expected: "built", found: "throws TypeError: no codec" });

    assert.deepEqual(outcome, {
      ok: false,
      error: [{ name: "known schemas", expected: "built", found: "throws TypeError: no codec" }],
    });
  });

  test("passes on the installed zod, with the cases built by its own builders", () => {
    assert.deepEqual(selfCheck(), { ok: true, value: "passed" });
  });
});

describe("zodMemberWire and zodValueWire", () => {
  test("give the wire type of an accepted schema: its kind and its presence", () => {
    const member = zodMemberWire(z.string().optional());
    const value = zodValueWire(z.string());

    assert.equal(member.ok && presenceOf(member.value).mode, "optional");
    assert.equal(value.ok && kindOf(value.value).node, "string");
  });

  test("give SchemaRefused with every refusal of a refused schema", () => {
    const refused = zodValueWire(z.object({ at: z.date(), any: z.any() }));

    assert.equal(refused.ok ? "" : refused.error.kind, "SchemaRefused");
    assert.equal(refused.ok || refused.error.kind !== "SchemaRefused" ? 0 : refused.error.refusals.length, 2);
  });

  test("give KindRefused for each form that only checkKind refuses", () => {
    const forms = [
      z.object({ 10: z.int() }),
      z.union([z.string(), hz.instant()]),
      z.union([z.int(), z.bigint()]),
      z.literal(1.5),
      z.discriminatedUnion("kind", [z.object({ kind: z.literal(1) })]),
      z.object({ ratio: z.number().default(Number.NaN) }),
    ];

    const rules = forms.map((schema) => {
      const outcome = zodValueWire(schema);

      return outcome.ok || outcome.error.kind !== "KindRefused" ? "" : outcome.error.refusals[0]?.rule;
    });

    assert.deepEqual(rules, [
      "IntegerLikeMember",
      "AmbiguousUnion",
      "AmbiguousUnion",
      "LiteralNotSafeInteger",
      "UnionTagInvalid",
      "DefaultNotStored",
    ]);
  });

  test("zodMemberWire checks the member's own default", () => {
    const outcome = zodMemberWire(z.number().default(Number.NaN));

    const rule = outcome.ok || outcome.error.kind !== "KindRefused" ? "" : outcome.error.refusals[0]?.rule;

    assert.equal(rule, "DefaultNotStored");
  });

  test("give Zod3Schema for a zod 3 schema, which has the same Standard Schema marker", () => {
    const schema = z3.z.object({ a: z3.z.string() });

    assert.equal(schema["~standard"].vendor, "zod");
    assert.deepEqual(zodMemberWire(schema), {
      ok: false,
      error: {
        kind: "Zod3Schema",
        fix: 'build the schema with zod 4: import { z } from "zod", not from "zod/v3" or a zod 3 install',
      },
    });
  });
});
