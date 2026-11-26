// The definition reader fails closed: a field, a format or a check that it does not know is refused, with its name.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { z } from "zod";

import { compileMember, compileValue } from "../../../horsies/src/zod/adapter.ts";
import type { ZodSchema } from "../../../horsies/src/zod/definition.ts";
import { isSupported, SUPPORTED_ZOD, zodSchemaOf } from "../../../horsies/src/zod/version.ts";
import { passedSelfCheck, withDefinition } from "./support.ts";

/** The rule, path, zod type and name of each refusal of a schema as a whole value. */
function refusals(schema: ZodSchema): readonly (readonly string[])[] {
  const compiled = compileValue(passedSelfCheck(), schema);

  assert.ok(!compiled.ok, "expected a refusal");

  return compiled.error.map((refused) =>
    "name" in refused
      ? [refused.rule, refused.path, refused.zodType, refused.name]
      : [refused.rule, refused.path, refused.zodType],
  );
}

describe("fail closed: unknown definition parts", () => {
  test("UNKNOWN_DEFINITION_FIELD: a field that the reader does not list for the type", () => {
    const string = z.string();

    assert.deepEqual(refusals(withDefinition(string, { ...string._zod.def, future: true })), [
      ["UNKNOWN_DEFINITION_FIELD", "$", "string", "future"],
    ]);
  });

  test("UNKNOWN_DEFINITION_FIELD: a field value that the reader does not know: mode, partial, inclusive", () => {
    const record = z.record(z.string(), z.int());
    const union = z.union([z.string(), z.int()]);

    assert.deepEqual(refusals(withDefinition(record, { ...record._zod.def, mode: "lenient" })), [
      ["UNKNOWN_DEFINITION_FIELD", "$", "record", "mode"],
    ]);
    assert.deepEqual(refusals(withDefinition(record, { ...record._zod.def, partial: "yes" })), [
      ["UNKNOWN_DEFINITION_FIELD", "$", "record", "partial"],
    ]);
    assert.deepEqual(refusals(withDefinition(union, { ...union._zod.def, inclusive: "some" })), [
      ["UNKNOWN_DEFINITION_FIELD", "$", "union", "inclusive"],
    ]);
  });

  test("UNKNOWN_FORMAT: a number, a bigint or a string format that the reader does not list", () => {
    const float = z.float32();
    const big = z.int64();
    const email = z.email();

    assert.deepEqual(refusals(withDefinition(float, { ...float._zod.def, format: "float16" })), [
      ["UNKNOWN_FORMAT", "$", "number", "float16"],
    ]);
    assert.deepEqual(refusals(withDefinition(big, { ...big._zod.def, format: "int128" })), [
      ["UNKNOWN_FORMAT", "$", "bigint", "int128"],
    ]);
    assert.deepEqual(refusals(withDefinition(email, { ...email._zod.def, format: "email2" })), [
      ["UNKNOWN_FORMAT", "$", "string", "email2"],
    ]);
  });

  test("a custom string format has its own name and its own test: it maps to a checked string", () => {
    const sku = z.stringFormat("sku", /^[A-Z]{3}-\d+$/);
    const compiled = compileValue(passedSelfCheck(), z.object({ sku, digest: z.hex() }));

    assert.equal(compiled.ok, true);
  });

  test("UNKNOWN_CHECK: a check name that the reader does not list", () => {
    const string = z.string().min(1);
    const [check] = string._zod.def.checks ?? [];

    assert.ok(check !== undefined);

    const def = { ...check._zod.def, check: "min_graphemes" };
    // SAFETY: a stub of a check of a newer zod: the check `min_length` with another check name.
    const future = { ...check, _zod: { ...check._zod, def } } as typeof check;

    assert.deepEqual(refusals(withDefinition(string, { ...string._zod.def, checks: [future] })), [
      ["UNKNOWN_CHECK", "$", "string", "min_graphemes"],
    ]);
  });

  test("TRANSFORM: z.coerce.*() converts its input before its check", () => {
    for (const schema of [z.coerce.number(), z.coerce.string(), z.coerce.bigint(), z.coerce.boolean()]) {
      assert.equal(refusals(schema)[0]?.[0], "TRANSFORM");
    }
  });

  test("a .prefault() value that zod does not decode is a refused default", () => {
    const prefault = z.int().prefault(7);
    const notDecoded = withDefinition(prefault, { ...prefault._zod.def, defaultValue: "seven" });
    const member = compileMember(passedSelfCheck(), notDecoded);

    assert.ok(member.ok);
    assert.equal(member.value.presence.mode === "defaulted" && member.value.presence.fallback.state, "refused");
  });
});

describe("the supported zod releases", () => {
  test("the peer range of the package is SUPPORTED_ZOD.range", () => {
    const manifest: unknown = JSON.parse(
      readFileSync(new URL("../../../horsies/package.json", import.meta.url), "utf8"),
    );

    assert.equal(Reflect.get(Reflect.get(Object(manifest), "peerDependencies") ?? {}, "zod"), SUPPORTED_ZOD.range);
  });

  test("isSupported: from 4.1.13 up to, not including, 4.7.0", () => {
    const at = (major: number, minor: number, patch: number): boolean => isSupported({ major, minor, patch });

    assert.deepEqual(
      [at(4, 1, 12), at(4, 1, 13), at(4, 6, 99), at(4, 7, 0), at(5, 0, 0), at(3, 25, 76)],
      [false, true, true, false, false, false],
    );
  });

  test("ZodVersionNotSupported: a zod 4 release outside the range", () => {
    const string = z.string();
    const version = { major: 4, minor: 0, patch: 17 };
    // SAFETY: a stub of a zod 4.0 schema: the internals of `string` with the release 4.0.17.
    const older = { ...string, _zod: { ...string._zod, version } } as unknown as typeof string;

    assert.deepEqual(zodSchemaOf(older), {
      ok: false,
      error: { kind: "ZodVersionNotSupported", found: { major: 4, minor: 0, patch: 17 }, supported: ">=4.1.13 <4.7.0" },
    });
  });

  test("OtherZodCopy: a release inside the range from another copy of zod", () => {
    const string = z.string();
    // SAFETY: a stub of a schema of another zod copy: an equal release number in another version object.
    const other = { ...string, _zod: { ...string._zod, version: { ...string._zod.version } } } as typeof string;
    const outcome = zodSchemaOf(other);

    assert.equal(outcome.ok ? "" : outcome.error.kind, "OtherZodCopy");
  });

  test("Zod3Schema: a value with the zod marker and no zod 4 internals", () => {
    const outcome = zodSchemaOf({ "~standard": { vendor: "zod", version: 1 } });

    assert.equal(outcome.ok ? "" : outcome.error.kind, "Zod3Schema");
  });

  test("the zod schema of the copy that horsies loads, inside the range", () => {
    const string = z.string();

    assert.deepEqual(zodSchemaOf(string), { ok: true, value: string });
  });
});
