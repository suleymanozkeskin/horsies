// `defineTask`: the frozen contract, its kept facts, the result constructors, and the refusals that the definition
// alone shows.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { z } from "zod";

import { defineTask, type UserFailure } from "../../../horsies/src/contract/public.ts";
import { contractRecord } from "../../../horsies/src/contract/definition.ts";
import { h } from "../../../horsies/src/wire/public.ts";

/** A Standard Schema of another vendor. */
const otherVendorSchema = {
  "~standard": { vendor: "valibot", version: 1 },
  _zod: { def: { type: "string" }, output: "text" },
} as const;

describe("defineTask", () => {
  test("the contract is frozen and holds the kind and the name", () => {
    const contract = defineTask({ name: "ping", input: {}, output: h.string() });

    assert.equal(contract.kind, "task");
    assert.equal(contract.name, "ping");
    assert.equal(Object.isFrozen(contract), true);
  });

  test("the kept facts: no queue, ISOLATED, no timeout, open codes, no retry", () => {
    const record = contractRecord(defineTask({ name: "ping", input: { n: h.int() }, output: h.string() }));

    assert.deepEqual(record.queue, { kind: "noQueue" });
    assert.equal(record.execution, "ISOLATED");
    assert.deepEqual(record.timeout, { kind: "notSet" });
    assert.deepEqual(record.cancelGrace, { kind: "notSet" });
    assert.deepEqual(record.codes, { kind: "open" });
    assert.deepEqual(record.retry, { kind: "noRetry" });
    assert.deepEqual(
      record.parameters.map((parameter) => [parameter.name, parameter.schema.kind]),
      [["n", "wire"]],
    );
    assert.deepEqual(record.refusals, []);
  });

  test("the kept facts of a full definition; jitter is true when left out", () => {
    const record = contractRecord(
      defineTask({
        name: "charge",
        queue: "billing",
        input: { order: z.object({ id: z.int() }) },
        output: h.null(),
        errors: ["PROVIDER_DOWN"],
        execution: "SHARED_ASYNC",
        timeoutMs: 30_000,
        cancelGraceMs: 2_000,
        retry: { kind: "exponential", baseSeconds: 30, maxRetries: 5, autoRetryFor: ["PROVIDER_DOWN"] },
      }),
    );

    assert.deepEqual(record.queue, { kind: "named", name: "billing" });
    assert.deepEqual(record.codes, { kind: "declared", codes: ["PROVIDER_DOWN"] });
    assert.deepEqual(record.timeout, { kind: "set", milliseconds: 30_000 });
    assert.deepEqual(record.cancelGrace, { kind: "set", milliseconds: 2_000 });
    assert.deepEqual(record.retry, {
      kind: "exponential",
      baseSeconds: 30,
      maxRetries: 5,
      jitter: true,
      maxDelay: { kind: "noCap" },
      autoRetryFor: ["PROVIDER_DOWN"],
    });
    assert.equal(record.parameters[0]?.schema.kind, "zod");
  });

  test("a value that defineTask did not build has no record: TypeError", () => {
    assert.throws(() => contractRecord({ kind: "task", name: "ping" }), TypeError);
  });
});

describe("result constructors", () => {
  const contract = defineTask({
    name: "validate_order",
    input: {},
    output: h.object({ id: h.int() }),
    errors: ["EMPTY_ORDER"],
  });

  test("ok gives a frozen success", () => {
    const value = contract.ok({ id: 1 });

    assert.deepEqual(value, { ok: true, value: { id: 1 } });
    assert.equal(Object.isFrozen(value), true);
  });

  test("err gives a frozen error with the code, the message and the data", () => {
    const value = contract.err({ code: "EMPTY_ORDER", message: "no lines", data: { lines: 0 } });

    assert.deepEqual(value, { ok: false, error: { code: "EMPTY_ORDER", message: "no lines", data: { lines: 0 } } });
    assert.equal(Object.isFrozen(value), true);
    assert.equal(Object.isFrozen(value.error), true);
  });

  test("err with data: undefined gives an error with no data member, as a left-out data does", () => {
    // SAFETY: a project without exactOptionalPropertyTypes, or a JavaScript caller, can pass `data: undefined`.
    const failure = { code: "EMPTY_ORDER", message: "no lines", data: undefined } as unknown as UserFailure<"EMPTY_ORDER">;
    const value = contract.err(failure);

    assert.deepEqual(value, { ok: false, error: { code: "EMPTY_ORDER", message: "no lines" } });
    assert.equal(value.ok ? true : Object.hasOwn(value.error, "data"), false);
    assert.equal(Object.isFrozen(value.ok ? value : value.error), true);
  });

  test("err with a built-in name throws TypeError", () => {
    const open = defineTask({ name: "ping", input: {}, output: h.null() });
    // SAFETY: a JavaScript caller, or a code read at run time, can pass a built-in name that the type refuses.
    const code = "TASK_TIMEOUT" as string;

    assert.throws(() => open.err({ code }), TypeError);
  });

  test("err with the reserved name WORKFLOW_STOPPED throws TypeError", () => {
    const open = defineTask({ name: "ping", input: {}, output: h.null() });
    // SAFETY: as above, a code that the type refuses.
    const code = "WORKFLOW_STOPPED" as string;

    assert.throws(() => open.err({ code }), TypeError);
  });
});

describe("definition refusals", () => {
  test("a Standard Schema of another vendor is kept with its vendor; the compile refuses it", () => {
    const record = contractRecord(defineTask({ name: "t", input: { value: otherVendorSchema }, output: h.null() }));

    assert.deepEqual(record.parameters[0]?.schema, { kind: "otherVendor", vendor: "valibot" });
    assert.deepEqual(record.refusals, []);
  });

  test("an empty task name is a TaskName refusal (empty)", () => {
    const record = contractRecord(defineTask({ name: "", input: {}, output: h.null() }));

    assert.deepEqual(record.refusals, [{ rule: "TaskName", path: "name", fault: "empty" }]);
  });

  test("a task name with a lone surrogate is a TaskName refusal (loneSurrogate)", () => {
    const record = contractRecord(defineTask({ name: "send\uD800", input: {}, output: h.null() }));

    assert.deepEqual(record.refusals, [{ rule: "TaskName", path: "name", fault: "loneSurrogate" }]);
  });

  test("a well-formed task name outside ASCII is no refusal", () => {
    const record = contractRecord(defineTask({ name: "sipariş_gönder_📦", input: {}, output: h.null() }));

    assert.deepEqual(record.refusals, []);
  });

  test("a parameter name that is not an identifier, that the engine fills, or that only the engine writes", () => {
    const record = contractRecord(
      defineTask({
        name: "t",
        input: { "first name": h.string(), workflow_ctx: h.json(), __h_x: h.int() },
        output: h.null(),
      }),
    );

    assert.deepEqual(
      record.refusals.map((refused) => [refused.rule, refused.path]),
      [
        ["ParameterName", "input.first name"],
        ["ParameterName", "input.workflow_ctx"],
        ["ParameterName", "input.__h_x"],
      ],
    );
  });

  test("an output of another vendor is kept with its vendor", () => {
    const record = contractRecord(defineTask({ name: "t", input: {}, output: otherVendorSchema }));

    assert.deepEqual(record.output, { kind: "otherVendor", vendor: "valibot" });
  });
});
