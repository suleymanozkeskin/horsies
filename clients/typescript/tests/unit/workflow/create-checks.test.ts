// The create checks of the workflow list and the checks of the `check` command: each code, and the create check on a
// worker thread with no addon.

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Worker } from "node:worker_threads";

import { h } from "../../../horsies/src/wire/public.ts";
import { checkWorkflows } from "../../../horsies/src/workflow/check-cases.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";
import { checkedAtCreate, duplicateIds, ping, plain } from "./create-workflows.ts";

function createCodes(workflows: readonly object[]): readonly string[] {
  return checkedAtCreate(workflows).refusals.map((refused) => refused.code);
}

function checkCodes(workflows: readonly object[]): readonly string[] {
  const checked = checkedAtCreate(workflows);

  assert.deepEqual(checked.refusals, []);

  return checkWorkflows(checked.registry).map((refused) => refused.code);
}

describe("checkWorkflowsAtCreate", () => {
  test("a valid list gives no refusal and a registry of every workflow", () => {
    const checked = checkedAtCreate([plain]);

    assert.deepEqual(checked.refusals, []);
    assert.equal(checked.registry.byKey("tests.plain.v1").kind, "listed");
  });

  test("an empty name: HRS-001; an empty definition key: HRS-016", () => {
    const noName = defineWorkflow({ name: "", definitionKey: "tests.a.v1", params: {} }, (w) => ({
      output: w.task(ping, {}, { id: "a" }),
    }));

    const noKey = defineWorkflow({ name: "b", definitionKey: " ", params: {} }, (w) => ({
      output: w.task(ping, {}, { id: "a" }),
    }));

    assert.deepEqual(createCodes([noName]), ["HRS-001"]);
    assert.deepEqual(createCodes([noKey]), ["HRS-016"]);
  });

  test("the workflow name is not the task name of the params contract: a lone surrogate in it gives no refusal here", () => {
    // The spec text writer refuses the name at a start (LoneSurrogate at $.name).
    const loneSurrogate = defineWorkflow({ name: "a\uD800", definitionKey: "tests.a.v1", params: {} }, (w) => ({
      output: w.task(ping, {}, { id: "a" }),
    }));

    assert.deepEqual(createCodes([loneSurrogate]), []);
  });

  test("a key of another workflow, and a workflow listed twice: HRS-017", () => {
    const sameKey = defineWorkflow({ name: "other", definitionKey: "tests.plain.v1", params: {} }, (w) => ({
      output: w.task(ping, {}, { id: "a" }),
    }));

    assert.deepEqual(createCodes([plain, sameKey]), ["HRS-017"]);
    assert.deepEqual(createCodes([plain, plain]), ["HRS-017"]);
  });

  test("params with a key and no check cases: HRS-027", () => {
    // SAFETY: the types require check cases for params with a key; the create check refuses them at run time too.
    const noCases = defineWorkflow(
      { name: "c", definitionKey: "tests.c.v1", params: { n: h.int() } } as never,
      (w) => ({
        output: w.task(ping, {}, { id: "a" }),
      }),
    );

    assert.deepEqual(createCodes([noCases]), ["HRS-027"]);
  });

  test("an error rule outside fail and pause: HRS-033; params that do not compile: HRS-105", () => {
    // SAFETY: the types refuse this error rule; the create check refuses it at run time too.
    const badRule = defineWorkflow(
      { name: "d", definitionKey: "tests.d.v1", params: {}, onError: "stop" as never },
      (w) => ({
        output: w.task(ping, {}, { id: "a" }),
      }),
    );

    const badParams = defineWorkflow(
      { name: "e", definitionKey: "tests.e.v1", params: { workflow_ctx: h.int() }, checkCases: [{ workflow_ctx: 1 }] },
      (w) => ({ output: w.task(ping, {}, { id: "a" }) }),
    );

    assert.deepEqual(createCodes([badRule]), ["HRS-033"]);
    assert.deepEqual(createCodes([badParams]), ["HRS-105"]);
  });

  test("a workflow with no params is built once with every spec check", () => {
    assert.deepEqual(createCodes([duplicateIds]), ["HRS-004"]);
  });

  test("the create check runs on a worker thread with no addon", async () => {
    const worker = new Worker(new URL("create-in-worker.ts", import.meta.url));

    const answer = new Promise<readonly string[]>((resolve, reject) => {
      worker.once("message", (codes: readonly string[]) => {
        resolve(codes);
      });
      worker.once("error", reject);
    });

    const exited = new Promise<number>((resolve) => {
      worker.once("exit", resolve);
    });

    assert.deepEqual(await answer, ["HRS-004"]);
    assert.equal(await exited, 0);
  });
});

describe("checkWorkflows", () => {
  test("a check case that does not encode: HRS-028; a case whose build throws: HRS-029", () => {
    const typed = defineWorkflow(
      // SAFETY: the types refuse a check case of another type; the check refuses it at run time too.
      { name: "f", definitionKey: "tests.f.v1", params: { n: h.int() }, checkCases: [{ n: "one" } as never] },
      (w) => ({ output: w.task(ping, {}, { id: "a" }) }),
    );

    const throws = defineWorkflow(
      { name: "g", definitionKey: "tests.g.v1", params: { n: h.int() }, checkCases: [{ n: 1 }] },
      () => {
        throw new RangeError("no");
      },
    );

    assert.deepEqual(checkCodes([typed]), ["HRS-028"]);
    assert.deepEqual(checkCodes([throws]), ["HRS-029"]);
  });

  test("a check case whose build fails a spec check gives the finding", () => {
    const finding = defineWorkflow(
      { name: "i", definitionKey: "tests.i.v1", params: { n: h.int() }, checkCases: [{ n: 1 }] },
      (w) => ({
        output: w.task(ping, {}, { id: "bad id" }),
      }),
    );

    assert.deepEqual(checkCodes([finding]), ["HRS-003"]);
  });

  test("a child that includes itself with the same params and no wait: HRS-037", () => {
    const loop: object = defineWorkflow({ name: "loop", definitionKey: "tests.loop.v1", params: {} }, (w) => {
      // SAFETY: the test nests the workflow in itself; the contract of `loop` is complete when the build runs.
      const again = w.subWorkflow(loop as never, {}, { id: "again" });

      return { output: again };
    });

    assert.deepEqual(checkCodes([loop]), ["HRS-037"]);
  });

  test("a nesting past depth 32 with no wait: HRS-036", () => {
    const deep: object = defineWorkflow(
      { name: "deep", definitionKey: "tests.deep.v1", params: { level: h.int() }, checkCases: [{ level: 0 }] },
      // SAFETY: the test nests the workflow in itself; the contract of `deep` is complete when the build runs.
      (w, { level }) => ({ output: w.subWorkflow(deep as never, { level: level + 1 }, { id: "next" }) }),
    );

    assert.deepEqual(checkCodes([deep]), ["HRS-036"]);
  });
});
