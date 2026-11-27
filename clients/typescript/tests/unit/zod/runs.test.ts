// One test per outcome of the zod runs that the checked and conversion nodes call.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { encodeWithReadBack } from "../../../horsies/src/codec/read-back.ts";
import {
  ASYNC_ISSUE_CODE,
  checksRun,
  decodeRun,
  MISSING_KEY_ISSUE_CODE,
  ONE_WAY_ISSUE_CODE,
  parseRun,
  recordKeysRun,
  ZOD_ASYNC_MESSAGE,
  ZOD_ENCODE_ERROR_NAME,
} from "../../../horsies/src/zod/runs.ts";
import { acceptedKind, runsOfCodec } from "./support.ts";

const positive = z.int().positive();
const positiveChecks = positive._zod.def.checks ?? [];

describe("checksRun", () => {
  test("accepts a value that each check accepts", () => {
    assert.deepEqual(checksRun(positiveChecks)(3), { ok: true, value: true });
  });

  test("gives the path and the code of each issue, with no value", () => {
    const refined = z.object({}).superRefine((_value, context) => {
      context.addIssue({ code: "custom", path: ["lines", 2], message: "line 2 is empty" });
    });

    assert.deepEqual(checksRun(refined._zod.def.checks ?? [])({}), {
      ok: false,
      error: { issues: [{ kind: "zodIssue", path: "$.<input>[2]", code: "custom" }] },
    });
  });

  test("a check that returns a promise is the issue async_check", () => {
    const later = z.string().refine(async () => true);

    assert.deepEqual(checksRun(later._zod.def.checks ?? [])("x"), {
      ok: false,
      error: { issues: [{ kind: "horsiesIssue", path: "$", code: ASYNC_ISSUE_CODE }] },
    });
  });

  test("after an issue that does not continue, a check without a when rule does not run, a length check runs", () => {
    const schema = z
      .string()
      .superRefine((value, context) => {
        context.issues.push({ code: "custom", input: value, message: "an issue without continue" });
      })
      .refine(() => false)
      .max(1);

    assert.deepEqual(checksRun(schema._zod.def.checks ?? [])("ab"), {
      ok: false,
      error: {
        issues: [
          { kind: "zodIssue", path: "$", code: "custom" },
          { kind: "zodIssue", path: "$", code: "too_big" },
        ],
      },
    });
  });

  test("after an issue of a check with abort, no check runs, also a length check", () => {
    const schema = z
      .string()
      .refine(() => false, { abort: true })
      .max(1)
      .refine(() => false);

    assert.deepEqual(checksRun(schema._zod.def.checks ?? [])("ab"), {
      ok: false,
      error: { issues: [{ kind: "zodIssue", path: "$", code: "custom" }] },
    });
  });
});

describe("the marks of zod's errors", () => {
  test("the installed zod throws its async error with ZOD_ASYNC_MESSAGE and its encode error with its name", () => {
    assert.throws(() => z.string().refine(async () => true).parse("x"), { message: ZOD_ASYNC_MESSAGE });
    assert.throws(() => z.encode(z.string().transform((text) => text), "x"), { name: ZOD_ENCODE_ERROR_NAME });
  });
});

describe("parseRun", () => {
  test("accepts a value that the whole parse accepts, and gives the issues of one that it refuses", () => {
    const run = parseRun(z.templateLiteral(["v", z.int()]));

    assert.deepEqual(run("v2"), { ok: true, value: true });
    assert.deepEqual(run("w2"), { ok: false, error: { issues: [{ kind: "zodIssue", path: "$", code: "invalid_format" }] } });
  });
});

describe("codecRuns", () => {
  const percent = z.codec(z.int(), z.number().max(1), {
    decode: (stored, payload) => {
      if (stored > 100) {
        payload.issues.push({ code: "custom", message: "above 100", input: stored });
      }

      return stored / 100;
    },
    encode: (value) => Math.round(value * 100),
  });

  const runs = runsOfCodec(percent);

  test("decode runs the codec's decode, then the output schema", () => {
    assert.deepEqual(runs.decode(25), { ok: true, value: 0.25 });
  });

  test("decode gives the issues that the codec's decode adds", () => {
    assert.deepEqual(runs.decode(150), { ok: false, error: { issues: [{ kind: "zodIssue", path: "$", code: "custom" }] } });
  });

  test("encode runs the output schema backward, then the codec's encode", () => {
    assert.deepEqual(runs.encode(0.5), { ok: true, value: 50 });
    assert.deepEqual(runs.encode(2), { ok: false, error: { issues: [{ kind: "zodIssue", path: "$", code: "too_big" }] } });
  });

  test("encode gives the issues that the codec's encode adds", () => {
    const signed = z.codec(z.int(), z.number(), {
      decode: (stored) => stored,
      encode: (value, payload) => {
        if (value < 0) {
          payload.issues.push({ code: "custom", message: "below 0", input: value });
        }

        return value;
      },
    });

    assert.deepEqual(runsOfCodec(signed).encode(-1), { ok: false, error: { issues: [{ kind: "zodIssue", path: "$", code: "custom" }] } });
  });

  test("a codec decode or encode that returns a promise is the issue async_check", () => {
    const later = runsOfCodec(
      z.codec(z.string(), z.string(), { decode: async (text) => text, encode: async (text) => text }),
    );

    const asyncIssue = { ok: false, error: { issues: [{ kind: "horsiesIssue", path: "$", code: ASYNC_ISSUE_CODE }] } };

    assert.deepEqual([later.decode("x"), later.encode("x")], [asyncIssue, asyncIssue]);
  });

  test("a one-way transform in the output schema is the issue one_way_transform at encode", () => {
    const lossy = z.codec(z.string(), z.string().transform((text) => text), {
      decode: (text) => text,
      encode: (text) => text,
    });

    assert.deepEqual(runsOfCodec(lossy).encode("x"), {
      ok: false,
      error: { issues: [{ kind: "horsiesIssue", path: "$", code: ONE_WAY_ISSUE_CODE }] },
    });
  });

  test("a codec whose encode and decode do not invert fails the read-back", () => {
    const halving = z.codec(z.int(), z.number(), { decode: (stored) => stored / 2, encode: (value) => value });
    const written = encodeWithReadBack(acceptedKind(halving), 4);

    assert.equal(written.ok ? "" : written.error.kind, "readBackChanged");
  });
});

describe("decodeRun", () => {
  test("decodes an input value as zod does, or gives its issues", () => {
    const run = decodeRun(z.codec(z.string(), z.number(), { decode: Number, encode: String }));

    assert.deepEqual(run("7"), { ok: true, value: 7 });
    assert.deepEqual(run(7), { ok: false, error: { issues: [{ kind: "zodIssue", path: "$", code: "invalid_type" }] } });
  });
});

describe("recordKeysRun", () => {
  const run = recordKeysRun(z.enum(["a", "b"]), ["a", "b"]);

  test("accepts a record with each required key and keys that the key schema accepts", () => {
    assert.deepEqual(run({ a: 1, b: 2 }), { ok: true, value: true });
  });

  test("a missing key is the issue missing_key at its declared name", () => {
    assert.deepEqual(run({ a: 1 }), { ok: false, error: { issues: [{ kind: "horsiesIssue", path: "$.b", code: MISSING_KEY_ISSUE_CODE }] } });
  });

  test("a key that the key schema refuses is written <input>: the key is part of the value", () => {
    assert.deepEqual(run({ a: 1, b: 2, secret: 3 }), {
      ok: false,
      error: { issues: [{ kind: "zodIssue", path: "$.<input>", code: "invalid_value" }] },
    });
  });

  test("a value that is not a plain object passes: the writer refuses it", () => {
    assert.deepEqual(run([1]), { ok: true, value: true });
  });
});
