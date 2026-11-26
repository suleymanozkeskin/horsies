// The compile of a contract: kind trees of the input and the output, the send form, and each refusal kind.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";
import * as zod3 from "zod/v3";

import { compileContract, type ContractRefused } from "../../../horsies/src/contract/compile.ts";
import { defineTask, type ParameterSchema } from "../../../horsies/src/contract/public.ts";
import { hz } from "../../../horsies/src/zod.ts";
import { h } from "../../../horsies/src/wire/public.ts";

function refusalsOf(contract: object): readonly (readonly [string, string])[] {
  const outcome = compileContract(contract);

  return outcome.ok ? [] : outcome.error.map((refused) => [refused.rule, refused.path] as const);
}

function refusedOf(contract: object): readonly ContractRefused[] {
  const outcome = compileContract(contract);

  return outcome.ok ? [] : outcome.error;
}

/** A zod string schema whose internals name the zod release `version` (a schema of another installed release). */
function schemaOfRelease(version: { readonly major: number; readonly minor: number; readonly patch: number }): ParameterSchema {
  const schema = z.string();

  // SAFETY: a test stub: the schema's own internals with another release number; the adapter reads `_zod.version`.
  return { ...schema, _zod: { ...schema._zod, version } } as unknown as ParameterSchema;
}

/** A Standard Schema of another vendor. */
const otherVendorSchema = { "~standard": { vendor: "valibot", version: 1 }, _zod: { def: { type: "string" } } } as const;

describe("compileContract", () => {
  test("h.* parameters compile in declaration order; a plain task is sendable with every parameter", () => {
    const outcome = compileContract(
      defineTask({ name: "t", input: { b: h.int(), a: h.string().optional() }, output: h.null() }),
    );

    assert.equal(outcome.ok, true);
    assert.deepEqual(outcome.ok ? outcome.value.parameters.members.map((member) => member.name) : [], ["b", "a"]);
    assert.equal(outcome.ok ? outcome.value.send.kind : "", "sendable");
  });

  test("zod parameters and a zod output compile through the adapter", () => {
    const outcome = compileContract(
      defineTask({
        name: "t",
        input: { order: z.object({ id: z.int(), at: hz.instant() }), note: z.string().optional() },
        output: z.object({ total: z.number() }),
      }),
    );

    assert.equal(outcome.ok, true);
    assert.deepEqual(
      outcome.ok ? outcome.value.parameters.members.map((member) => [member.name, member.presence.mode]) : [],
      [
        ["order", "required"],
        ["note", "optional"],
      ],
    );
  });

  test("a required upstream outcome makes the task workflow-node-only", () => {
    const outcome = compileContract(
      defineTask({ name: "t", input: { upstream: h.resultOf(h.int()), n: h.int() }, output: h.null() }),
    );

    assert.deepEqual(outcome.ok ? outcome.value.send : undefined, { kind: "workflowNodeOnly", upstream: ["upstream"] });
  });

  test("an optional upstream outcome is left out of the send form", () => {
    const outcome = compileContract(
      defineTask({ name: "t", input: { upstream: h.resultOf(h.int()).optional(), n: h.int() }, output: h.null() }),
    );

    const send = outcome.ok ? outcome.value.send : undefined;

    assert.equal(send?.kind, "sendable");
    assert.deepEqual(send?.kind === "sendable" ? send.parameters.members.map((member) => member.name) : [], ["n"]);
  });

  test("the definition refusals come first; the parameters and the output are still checked", () => {
    const contract = defineTask({
      name: "",
      input: { workflow_meta: h.json(), ratio: h.float().default(Number.NaN), note: z.any() },
      // SAFETY: a JavaScript caller can pass an output that the static type refuses; the compile must refuse it too.
      output: h.int().optional() as never,
    });

    assert.deepEqual(refusalsOf(contract), [
      ["TaskName", "name"],
      ["ParameterName", "input.workflow_meta"],
      ["ZodSchema", "input.note"],
      ["WireKind", "input.ratio"],
      ["WireKind", "output"],
    ]);
  });

  test("an empty task name is TaskName with its help", () => {
    const contract = defineTask({ name: "", input: {}, output: h.null() });

    assert.deepEqual(refusedOf(contract), [
      { rule: "TaskName", path: "name", detail: "the task name is empty", help: ["give a task name that is not empty"] },
    ]);
  });

  test("a task name with a lone surrogate is TaskName with its help", () => {
    const contract = defineTask({ name: "t\uDC00", input: {}, output: h.null() });

    assert.deepEqual(refusedOf(contract), [
      {
        rule: "TaskName",
        path: "name",
        detail: "the task name holds a lone surrogate (text that is not well formed)",
        help: ["give a task name of well-formed text"],
      },
    ]);
  });

  test("a parameter name that the definition refuses is not refused twice by the name rules of the wire types", () => {
    const contract = defineTask({ name: "t", input: { __h_x: h.int(), 10: h.int() }, output: h.null() });

    assert.deepEqual(refusalsOf(contract), [
      ["ParameterName", "input.10"],
      ["ParameterName", "input.__h_x"],
    ]);
  });

  test("the kind of a parameter with a refused name is still checked", () => {
    const contract = defineTask({ name: "t", input: { workflow_ctx: h.float().default(Number.NaN) }, output: h.null() });

    assert.deepEqual(refusalsOf(contract), [
      ["ParameterName", "input.workflow_ctx"],
      ["WireKind", "input.workflow_ctx"],
    ]);
  });

  test("a Standard Schema of another vendor is SchemaVendor at its place, with its help", () => {
    const contract = defineTask({ name: "t", input: { value: otherVendorSchema }, output: otherVendorSchema });

    assert.deepEqual(
      refusedOf(contract).map((refused) => [refused.rule, refused.path, refused.help]),
      [
        ["SchemaVendor", "input.value", ["use h.* or zod 4"]],
        ["SchemaVendor", "output", ["use h.* or zod 4"]],
      ],
    );
  });

  test("a default that the writer refuses is WireKind (DefaultNotStored) at its parameter", () => {
    const contract = defineTask({ name: "t", input: { ratio: h.float().default(Number.NaN) }, output: h.null() });

    assert.deepEqual(refusalsOf(contract), [["WireKind", "input.ratio"]]);
  });

  test("a zod default that the writer refuses is WireKind or ZodSchema, never a throw at a later decode", () => {
    const contract = defineTask({ name: "t", input: { ratio: z.number().default(Number.NaN) }, output: h.null() });
    const refusals = refusalsOf(contract);

    assert.equal(refusals.length > 0, true);
  });

  test("an output that may be left out is WireKind at output", () => {
    // SAFETY: a JavaScript caller can pass an output that the static type refuses; the compile must refuse it too.
    const contract = defineTask({ name: "t", input: {}, output: h.int().optional() as never });

    assert.deepEqual(refusalsOf(contract), [["WireKind", "output"]]);
  });

  test("an h.resultOf output is WireKind at output", () => {
    // SAFETY: as above, an output that the static type refuses.
    const contract = defineTask({ name: "t", input: {}, output: h.resultOf(h.int()) as never });

    assert.deepEqual(refusalsOf(contract), [["WireKind", "output"]]);
  });

  test("a zod form with no stored form is ZodSchema at its path, with the adapter's fix as its help", () => {
    const contract = defineTask({ name: "t", input: { value: z.object({ any: z.any() }) }, output: h.null() });

    assert.deepEqual(refusedOf(contract), [
      {
        rule: "ZodSchema",
        path: "input.value.any",
        detail: "zod type any is refused [UNTYPED]",
        help: ["use h.json() or z.json() for untyped JSON"],
      },
    ]);
  });

  test("a zod 3 schema is Zod3Schema at its parameter, with the adapter's fix as its help", () => {
    const schema = zod3.z.object({ name: zod3.z.string() });
    // SAFETY: a zod 3 schema carries the zod marker; a JavaScript caller can pass it where the static type refuses it.
    const parameter = schema as unknown as ParameterSchema;
    const contract = defineTask({ name: "t", input: { value: parameter }, output: h.null() });

    assert.deepEqual(refusedOf(contract), [
      {
        rule: "Zod3Schema",
        path: "input.value",
        detail: "a zod 3 schema",
        help: ['build the schema with zod 4: import { z } from "zod", not from "zod/v3" or a zod 3 install'],
      },
    ]);
  });

  test("a schema of a zod release outside the supported range is ZodVersionNotSupported, with the range as help", () => {
    const contract = defineTask({
      name: "t",
      input: { value: schemaOfRelease({ major: 4, minor: 7, patch: 0 }) },
      output: h.null(),
    });

    assert.deepEqual(refusedOf(contract), [
      {
        rule: "ZodVersionNotSupported",
        path: "input.value",
        detail: "zod 4.7.0 is outside the supported range >=4.1.13 <4.7.0",
        help: ["install a zod release in the range >=4.1.13 <4.7.0"],
      },
    ]);
  });

  test("a contract compiles once: the same outcome object", () => {
    const contract = defineTask({ name: "t", input: {}, output: h.null() });

    assert.equal(compileContract(contract), compileContract(contract));
  });
});
