// Helpers of the zod adapter tests: the kind of a schema that the adapter accepts, and its texts.

import assert from "node:assert/strict";

import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { type DecodeForm, decodeValue } from "../../../horsies/src/codec/reader.ts";
import type { DecodeRefused, EncodeRefused } from "../../../horsies/src/codec/refusal.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import type { Kind } from "../../../horsies/src/wire/kinds.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { compileMember, compileValue, type ZodMemberKind } from "../../../horsies/src/zod/adapter.ts";
import { readDefinition, type ZodSchema } from "../../../horsies/src/zod/definition.ts";
import { type CodecRuns, codecRuns } from "../../../horsies/src/zod/runs.ts";
import { zodRefusalText } from "../../../horsies/src/zod/rules.ts";

/** The kind of a schema that the adapter accepts as a whole value; a refusal fails the test with its texts. */
export function acceptedKind(schema: ZodSchema): Kind {
  const compiled = compileValue(schema);

  assert.ok(compiled.ok, compiled.ok ? "" : compiled.error.map(zodRefusalText).join("\n"));

  return compiled.value;
}

/** The kind and presence of a schema that the adapter accepts as a member. */
export function acceptedMember(schema: ZodSchema): ZodMemberKind {
  const compiled = compileMember(schema);

  assert.ok(compiled.ok, compiled.ok ? "" : compiled.error.map(zodRefusalText).join("\n"));

  return compiled.value;
}

/** The stored text of a value under a kind; a refusal fails the test. */
export function storedText<V>(kind: Kind, value: V): string {
  const written = encodeValue(kind, value);

  assert.ok(written.ok, written.ok ? "" : `${written.error.kind} at ${written.error.path}: ${written.error.detail}`);

  return written.value;
}

/** The refusal of a value under a kind, which the test expects. */
export function encodeRefusal<V>(kind: Kind, value: V): EncodeRefused {
  const written = encodeValue(kind, value);

  assert.ok(!written.ok, `expected a refusal, got ${written.ok ? written.value : ""}`);

  return written.error;
}

/** The decode of a stored text that the core's strict rule accepts. */
export function decoded(kind: Kind, text: string, form: DecodeForm): Result<unknown, DecodeRefused> {
  return decodeValue(kind, jsonTextCheckedByCore(text), form);
}

/** The value of a stored text in the worker form; a refusal fails the test. */
export function workerValue(kind: Kind, text: string): unknown {
  const read = decoded(kind, text, "worker");

  assert.ok(read.ok, read.ok ? "" : `${read.error.kind} at ${read.error.path}: ${read.error.detail}`);

  return read.value;
}

/** The refusal of a stored text in a form, which the test expects. */
export function decodeRefusal(kind: Kind, text: string, form: DecodeForm): DecodeRefused {
  const read = decoded(kind, text, form);

  assert.ok(!read.ok, "expected a refusal");

  return read.error;
}

/**
 * A stub of a schema with another definition: the schema that a zod with renamed or new definition fields would
 * build. The adapter reads `_zod.def`, `_zod.traits` and `_zod.values` only.
 */
export function withDefinition(schema: ZodSchema, def: object): ZodSchema {
  // SAFETY: a test stub: the schema's own `_zod` fields, with the given definition in place of its own.
  return { ...schema, _zod: { ...schema._zod, def } } as unknown as ZodSchema;
}

/** The runs of a `z.codec()`, from its definition; another schema fails the test. */
export function runsOfCodec(schema: ZodSchema): CodecRuns {
  const definition = readDefinition(schema);

  assert.equal(definition.form, "codec");

  return definition.form === "codec"
    ? codecRuns(definition.output, definition.decode, definition.encode)
    : { decode: () => assert.fail("not a codec"), encode: () => assert.fail("not a codec") };
}
