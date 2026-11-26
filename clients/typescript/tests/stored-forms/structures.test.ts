// Python's stored texts of a recursive model, of tuples with a variadic item and of a model with a `default_factory`
// member (`corpus/structures.json`, written by `corpus/make_corpus.py`): the `h.*` wire type and the zod schema of
// each case write Python's text, and read it back to the same value.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, test } from "node:test";

import { z } from "zod";

import { jsonTextCheckedByCore } from "../../horsies/src/codec/json-text.ts";
import { type DecodeForm, decodeValue } from "../../horsies/src/codec/reader.ts";
import { encodeValue } from "../../horsies/src/codec/writer.ts";
import { h } from "../../horsies/src/wire/builders.ts";
import type { RequiredWire } from "../../horsies/src/wire/infer.ts";
import type { Kind } from "../../horsies/src/wire/kinds.ts";
import { kindOf, type Wire } from "../../horsies/src/wire/wire-type.ts";
import { compileValue } from "../../horsies/src/zod/adapter.ts";
import type { ZodSchema } from "../../horsies/src/zod/definition.ts";
import { CORPUS_DIR } from "./files.ts";

type StructureCase = "recursive_model" | "variadic_tuple" | "tuple_with_rest" | "default_factory";

type StructureText = { readonly case: StructureCase; readonly text: string };

type StructureRefused = { readonly case: StructureCase; readonly text: string; readonly loc: readonly number[] };

type StructuresFile = {
  readonly texts: readonly StructureText[];
  readonly refused: readonly StructureRefused[];
  readonly default_factory_left_out: string;
};

const STRUCTURE_TEXT_COUNT = 8;

const STRUCTURES: StructuresFile = JSON.parse(readFileSync(`${CORPUS_DIR}structures.json`, "utf8"));

type TreeNode = { name: string; children: TreeNode[] };

/** Python's `class Tree(BaseModel): name: str; children: list[Tree]`. */
const TREE: Wire<TreeNode> = h.object({ name: h.string(), children: h.array(h.lazy(() => TREE)) });

const ZOD_TREE: z.ZodType<TreeNode> = z.lazy(() => z.object({ name: z.string(), children: z.array(ZOD_TREE) }));

/** The `h.*` wire type of each case: the type that the Python model of the case declares. */
function wireOf(name: StructureCase): RequiredWire {
  switch (name) {
    case "recursive_model":
      return TREE;
    case "variadic_tuple":
      return h.tuple([], h.boolean());
    case "tuple_with_rest":
      return h.tuple([h.int(), h.string()], h.boolean());
    case "default_factory":
      return h.object({ id: h.uuid().default(() => crypto.randomUUID()), name: h.string() });
    default:
      return assert.fail(`no wire type for ${String(name)}`);
  }
}

/** The zod schema of each case. */
function zodOf(name: StructureCase): ZodSchema {
  switch (name) {
    case "recursive_model":
      return ZOD_TREE;
    case "variadic_tuple":
      // zod types no tuple of zero fixed items with a rest item; z.array() stores the same JSON array.
      return z.array(z.boolean());
    case "tuple_with_rest":
      return z.tuple([z.int(), z.string()], z.boolean());
    case "default_factory":
      return z.object({ id: z.uuid().default(() => crypto.randomUUID()), name: z.string() });
    default:
      return assert.fail(`no zod schema for ${String(name)}`);
  }
}

function zodKind(name: StructureCase): Kind {
  const compiled = compileValue(zodOf(name));

  assert.ok(compiled.ok, `the zod schema of ${name} compiles`);

  return compiled.value;
}

function frontEnds(name: StructureCase): readonly (readonly [string, Kind])[] {
  return [
    ["h", kindOf(wireOf(name))],
    ["zod", zodKind(name)],
  ];
}

function read(kind: Kind, text: string, form: DecodeForm): unknown {
  const decoded = decodeValue(kind, jsonTextCheckedByCore(text), form);

  assert.ok(decoded.ok, decoded.ok ? "" : JSON.stringify(decoded.error));

  return decoded.value;
}

describe("structures corpus", () => {
  test(`${STRUCTURE_TEXT_COUNT} values write Python's text, and each text reads back to the value`, () => {
    assert.equal(STRUCTURES.texts.length, STRUCTURE_TEXT_COUNT);

    for (const row of STRUCTURES.texts) {
      const value: unknown = JSON.parse(row.text);

      for (const [frontEnd, kind] of frontEnds(row.case)) {
        assert.deepEqual(encodeValue(kind, value), { ok: true, value: row.text }, `${frontEnd} ${row.text}`);
        assert.deepEqual(read(kind, row.text, "worker"), value, `${frontEnd} ${row.text}`);
      }
    }
  });

  test("a rest item that Python's strict read refuses is WrongType at the same index", () => {
    assert.equal(STRUCTURES.refused.length, 1);

    for (const row of STRUCTURES.refused) {
      for (const [frontEnd, kind] of frontEnds(row.case)) {
        const decoded = decodeValue(kind, jsonTextCheckedByCore(row.text), "worker");

        assert.ok(!decoded.ok && decoded.error.kind === "decodeRefused", frontEnd);
        assert.deepEqual([decoded.error.refusal, decoded.error.path], ["WrongType", `$[${row.loc.join("][")}]`]);
      }
    }
  });

  test("a sender leaves the default_factory member out; each worker read fills a new value, as Python's read does", () => {
    const leftOut = STRUCTURES.default_factory_left_out;

    for (const [frontEnd, kind] of frontEnds("default_factory")) {
      const first = read(kind, leftOut, "worker");
      const second = read(kind, leftOut, "worker");

      assert.deepEqual(encodeValue(kind, { name: "a" }), { ok: true, value: leftOut }, frontEnd);
      assert.deepEqual(read(kind, leftOut, "stored"), { name: "a" }, frontEnd);
      assert.notDeepEqual(first, second, frontEnd);
    }
  });
});
