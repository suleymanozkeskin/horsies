// The compile of a contract: kind trees of the input and the output, the send form, and each refusal kind.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";
import * as zod3 from "zod/v3";

import { compileContract } from "../../../horsies/src/contract/compile.ts";
import { defineTask, type ParameterSchema } from "../../../horsies/src/contract/public.ts";
import { hz } from "../../../horsies/src/zod.ts";
import { h } from "../../../horsies/src/wire/public.ts";

function refusalsOf(contract: object): readonly (readonly [string, string])[] {
  const outcome = compileContract(contract);

  return outcome.ok ? [] : outcome.error.map((refused) => [refused.rule, refused.path] as const);
}

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

  test("the definition refusals come first and stop the compile", () => {
    const contract = defineTask({ name: "t", input: { workflow_meta: h.json() }, output: h.null() });

    assert.deepEqual(refusalsOf(contract), [["ParameterName", "input.workflow_meta"]]);
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

  test("a zod form with no stored form is ZodSchema at its path", () => {
    const contract = defineTask({ name: "t", input: { value: z.object({ any: z.any() }) }, output: h.null() });

    assert.deepEqual(refusalsOf(contract), [["ZodSchema", "input.value.any"]]);
  });

  test("a zod 3 schema is ZodRelease at its parameter", () => {
    const schema = zod3.z.object({ name: zod3.z.string() });
    // SAFETY: a zod 3 schema carries the zod marker; a JavaScript caller can pass it where the static type refuses it.
    const parameter = schema as unknown as ParameterSchema;
    const contract = defineTask({ name: "t", input: { value: parameter }, output: h.null() });

    assert.deepEqual(refusalsOf(contract), [["ZodRelease", "input.value"]]);
  });

  test("a contract compiles once: the same outcome object", () => {
    const contract = defineTask({ name: "t", input: {}, output: h.null() });

    assert.equal(compileContract(contract), compileContract(contract));
  });
});
