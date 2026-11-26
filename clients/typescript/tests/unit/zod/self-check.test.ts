// The self-check of the zod adapter, and the entries that run it before the adapter.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { compileMember } from "../../../horsies/src/zod/adapter.ts";
import { afterSelfCheck, zodMemberKind, zodValueKind } from "../../../horsies/src/zod/kind.ts";
import {
  knownCases,
  runSelfCheck,
  type SelfCheckFailure,
  selfCheckOnce,
} from "../../../horsies/src/zod/self-check.ts";
import { withDefinition } from "./support.ts";

describe("self-check", () => {
  test("each known schema compiles to its expected kind or refusal on the installed zod", () => {
    assert.deepEqual(runSelfCheck(knownCases()), { ok: true, value: true });
  });

  test("covers each refusal rule that a zod schema reaches", () => {
    const rules = new Set(
      knownCases().flatMap((known) => (known.expected.outcome === "refused" ? [known.expected.rule] : [])),
    );

    assert.deepEqual([...rules].toSorted(), [
      "CATCH",
      "CHANGING_DEFAULT",
      "DATE",
      "ENGINE_KEY",
      "INTERSECTION",
      "INT_FLOAT_UNION",
      "LITERAL_MEMBER",
      "LOOSE_OBJECT",
      "MIXED_UNION",
      "NOT_DATA",
      "OPTIONAL_NOT_A_MEMBER",
      "RECORD_KEY",
      "RECURSIVE",
      "TRANSFORM",
      "TUPLE_REST",
      "UNDISCRIMINATED_UNION",
      "UNION_OPTION",
      "UNTYPED",
    ]);
  });

  test("finds a renamed definition field: a loose object whose catchall field has another name", () => {
    const loose = z.looseObject({ a: z.string() });
    const { catchall, ...others } = loose._zod.def;
    const renamed = withDefinition(loose, { ...others, rest: catchall });

    const outcome = runSelfCheck([
      { name: "loose object", schema: renamed, expected: { outcome: "refused", rule: "LOOSE_OBJECT" }, probes: [] },
    ]);

    assert.deepEqual(outcome, {
      ok: false,
      error: [{ name: "loose object", expected: "LOOSE_OBJECT", found: "object(a:string)" }],
    });
  });

  test("finds a check that stopped running: a probe that the kind writes but should refuse", () => {
    const record = z.record(z.enum(["a"]), z.boolean());
    const stripped = withDefinition(record, { ...record._zod.def, keyType: z.string() });

    const outcome = runSelfCheck([
      {
        name: "enum record",
        schema: stripped,
        expected: { outcome: "kind", text: "record(boolean)" },
        probes: [{ value: {}, written: "refused" }],
      },
    ]);

    assert.deepEqual(outcome, { ok: false, error: [{ name: "enum record", expected: "{} refused", found: "stored" }] });
  });

  test("runs once per process: a later call gives the first outcome", () => {
    assert.equal(selfCheckOnce(), selfCheckOnce());
  });
});

describe("zodMemberKind and zodValueKind", () => {
  const failure: SelfCheckFailure = { name: "loose object", expected: "LOOSE_OBJECT", found: "object()" };

  test("give the kind of an accepted schema", () => {
    const member = zodMemberKind(z.string().optional());
    const value = zodValueKind(z.string());

    assert.equal(member.ok && member.value.presence.mode, "optional");
    assert.equal(value.ok && value.value.node, "string");
  });

  test("give SchemaRefused with every refusal of a refused schema", () => {
    const refused = zodValueKind(z.object({ at: z.date(), any: z.any() }));

    assert.equal(refused.ok ? "" : refused.error.kind, "SchemaRefused");
    assert.equal(refused.ok || refused.error.kind !== "SchemaRefused" ? 0 : refused.error.refusals.length, 2);
  });

  test("give SelfCheckFailed after a failed self-check, and do not compile", () => {
    let compiles = 0;

    const outcome = afterSelfCheck({ ok: false, error: [failure] }, () => {
      compiles += 1;

      return compileMember(z.string());
    });

    assert.deepEqual(outcome, { ok: false, error: { kind: "SelfCheckFailed", failures: [failure] } });
    assert.equal(compiles, 0);
  });
});
