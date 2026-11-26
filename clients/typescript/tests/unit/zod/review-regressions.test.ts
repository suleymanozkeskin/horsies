// Regression tests: each failing input of the review of the zod adapter, with zod's own answer next to horsies'.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { encodeWithReadBack } from "../../../horsies/src/codec/read-back.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { compileValue } from "../../../horsies/src/zod/adapter.ts";
import { hz } from "../../../horsies/src/zod/codecs.ts";
import type { ZodSchema } from "../../../horsies/src/zod/definition.ts";
import { zodRefusalText } from "../../../horsies/src/zod/rules.ts";
import { acceptedKind, encodeRefusal, passedSelfCheck, storedText, withDefinition, workerValue } from "./support.ts";

/** The rule texts of a refused schema. */
function refusalTexts(schema: ZodSchema): readonly string[] {
  const compiled = compileValue(passedSelfCheck(), schema);

  return compiled.ok ? [] : compiled.error.map(zodRefusalText);
}

describe("Z1: the writer checks the structure before a refine runs", () => {
  test("a refine of a union member does not run on a value of another member's structure", () => {
    const schema = z.union([z.string().refine((text) => text.trim().length > 0), z.int()]);

    assert.equal(schema.safeParse(5).success, true);
    assert.equal(storedText(acceptedKind(schema), 5), "5");
    assert.equal(encodeRefusal(acceptedKind(schema), " ").kind, "CheckFailed");
  });

  test("an object refine does not run on a value that is not of the object's structure", () => {
    const schema = z.object({ name: z.string() }).refine((value) => value.name.trim().length > 0);
    const kind = acceptedKind(schema);

    assert.equal(schema.safeParse(null).success, false);
    assert.equal(encodeRefusal(kind, null).kind, "WrongType");
    assert.deepEqual(
      [encodeRefusal(kind, { name: 1 }).kind, encodeRefusal(kind, { name: 1 }).path],
      ["WrongType", "$.name"],
    );

    const written = encodeWithReadBack(kind, null);

    assert.equal(written.ok ? "" : written.error.kind, "WrongType");
  });

  test("a refine of a discriminated-union option runs after its members are written", () => {
    const schema = z.discriminatedUnion("kind", [
      z.object({ kind: z.literal("a"), name: z.string() }).refine((value) => value.name.trim().length > 0),
      z.object({ kind: z.literal("b") }),
    ]);

    const missing = encodeRefusal(acceptedKind(schema), { kind: "a" });

    assert.deepEqual([missing.kind, missing.path], ["MissingMember", "$.name"]);
  });
});

describe("Z2: at encode a refine sees the defaults filled; the sender stores only the given keys", () => {
  test("an object refine that reads a defaulted member", () => {
    const schema = z
      .object({ start: hz.plainDate(), days: z.int().default(1) })
      .refine((range) => range.start.year > 2000 && range.days > 0);

    const kind = acceptedKind(schema);

    assert.equal(storedText(kind, { start: Temporal.PlainDate.from("2026-08-12") }), '{"start":"2026-08-12"}');
    assert.equal(encodeRefusal(kind, { start: Temporal.PlainDate.from("1999-01-01") }).kind, "CheckFailed");
  });

  test("an object refine that reads a defaulted array, and the read-back of that send", () => {
    const schema = z.object({ tags: z.array(z.string()).default([]) }).refine((value) => value.tags.length < 5);
    const kind = acceptedKind(schema);

    assert.equal(schema.safeParse({}).success, true);
    assert.equal(storedText(kind, {}), "{}");
    assert.deepEqual(encodeWithReadBack(kind, {}), { ok: true, value: "{}" });
  });
});

describe("Z3: no mapping is looser than zod", () => {
  test("an exclusive union (z.xor, inclusive: false without a discriminator) is refused", () => {
    const union = z.union([z.literal("a"), z.string()]);
    const exclusive = withDefinition(union, { ...union._zod.def, inclusive: false });

    assert.deepEqual(refusalTexts(z.object({ code: exclusive })), [
      "$.code: zod type union is refused [EXCLUSIVE_UNION]; " +
        "use z.union() of members that store different JSON types, or z.discriminatedUnion()",
    ]);

    const xor: unknown = Reflect.get(z, "xor");

    if (xor instanceof Function) {
      // SAFETY: zod 4.2 and later: `z.xor` builds a zod schema.
      const built = Reflect.apply(xor, z, [[z.literal("a"), z.string()]]) as ZodSchema;

      assert.equal(refusalTexts(built)[0]?.includes("[EXCLUSIVE_UNION]"), true);
    }
  });

  test("a number format that the reader does not know is refused, never read as a float", () => {
    const uint = z.uint32();
    const renamed = withDefinition(uint, { ...uint._zod.def, format: "u32" });

    assert.deepEqual(refusalTexts(renamed), [
      "$: zod type number is refused [UNKNOWN_FORMAT u32]; " +
        "use a zod release that this horsies supports, or h.* for this value",
    ]);
  });
});

describe("Z4: z.json() reads an integer outside the safe range as a bigint", () => {
  test("the value is a bigint, as for h.json(); the static type has bigint (tests/types/pass/zod-value.ts)", () => {
    const read = workerValue(acceptedKind(z.object({ data: z.json() })), '{"data":{"n":12345678901234567890}}');

    assert.deepEqual(read, { data: { n: 12_345_678_901_234_567_890n } });
  });
});

describe("the writer runs a refine once", () => {
  test("a refine of a template literal runs once at encode", () => {
    let runs = 0;

    const schema = z.templateLiteral(["order-", z.int()]).refine(() => {
      runs += 1;

      return true;
    });

    assert.equal(encodeValue(acceptedKind(schema), "order-1").ok, true);
    assert.equal(runs, 1);
  });
});
