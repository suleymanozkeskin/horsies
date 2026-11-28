// The create checks of the workflow list and the checks of the `check` command: each code, and the create check on a
// worker thread with no addon.

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Worker } from "node:worker_threads";

import { z } from "zod";

import { MAX_CHECK_SUBWORKFLOW_BUILDS } from "../../../horsies/src/_catalog/workflow_bounds.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { checkWorkflows } from "../../../horsies/src/workflow/check-cases.ts";
import { MAX_DEFINITION_KEY_CHARS, MAX_WORKFLOW_NAME_CHARS } from "../../../horsies/src/workflow/create-checks.ts";
import { defineWorkflow, type NodeRef, type WorkflowBuilder } from "../../../horsies/src/workflow/public.ts";
import { checkedAtCreate, duplicateIds, ping, plain } from "./create-workflows.ts";

function createCodes(workflows: readonly object[]): readonly string[] {
  return checkedAtCreate(workflows).refusals.map((refused) => refused.code);
}

/** The code and the place of each refusal at create. */
function createRefusals(workflows: readonly object[]): readonly { readonly code: string; readonly path: string }[] {
  return checkedAtCreate(workflows).refusals.map((refused) => ({ code: refused.code, path: refused.path }));
}

/** A valid workflow with no params, named `name`. */
function namedWorkflow(name: string): object {
  return defineWorkflow({ name, definitionKey: "tests.named.v1", outputType: h.int(), parameters: {} }, (w) => ({
    output: w.task(ping, {}, { nodeId: "a" }),
  }));
}

/** A valid workflow with no params, named `keyed`, with the definition key `definitionKey`. */
function keyedWorkflow(definitionKey: string): object {
  return defineWorkflow({ name: "keyed", definitionKey, outputType: h.int(), parameters: {} }, (w) => ({
    output: w.task(ping, {}, { nodeId: "a" }),
  }));
}

function checkCodes(workflows: readonly object[]): readonly string[] {
  const checked = checkedAtCreate(workflows);

  assert.deepEqual(checked.refusals, []);

  return checkWorkflows(checked.registry).map((refused) => refused.code);
}

/** A leaf workflow with no params; `builds.count` counts its builds. */
function countedLeaf(builds: { count: number }): object {
  return defineWorkflow({ name: "leaf", definitionKey: "tests.leaf.v1", outputType: h.int(), parameters: {} }, (w) => {
    builds.count += 1;

    return { output: w.task(ping, {}, { nodeId: "a" }) };
  });
}

/** A root named `name` with `children` sub-workflow nodes of `leaf`, each a node of its own. */
function fanRoot(name: string, leaf: object, children: number): object {
  return defineWorkflow({ name, definitionKey: `tests.${name}.v1`, outputType: h.int(), parameters: {} }, (w) => {
    const nodes = Array.from({ length: children }, (_, position) =>
      // SAFETY: `leaf` is a workflow with no params and the output `h.int()`.
      w.subWorkflow(leaf as never, {}, { nodeId: `n${position}` }),
    );

    // SAFETY: the first node is a child whose declared output is `h.int()`, with the node id `n0`.
    return { output: nodes[0] as NodeRef<number, string, "n0"> };
  });
}

describe("checkWorkflowsAtCreate", () => {
  test("a valid list gives no refusal and a registry of every workflow", () => {
    const checked = checkedAtCreate([plain]);

    assert.deepEqual(checked.refusals, []);
    assert.equal(checked.registry.byKey("tests.plain.v1").kind, "listed");
  });

  test("an empty name: HRS-001, once, at the place of the entry", () => {
    assert.deepEqual(createRefusals([namedWorkflow(" ")]), [{ code: "HRS-001", path: "workflows[0]" }]);
  });

  test("an empty definition key: HRS-016, and no key check of another entry finds it", () => {
    assert.deepEqual(createRefusals([keyedWorkflow(" "), keyedWorkflow(" ")]), [
      { code: "HRS-016", path: "workflows[0] keyed" },
      { code: "HRS-016", path: "workflows[1] keyed" },
    ]);
  });

  test("a name with a lone surrogate: HRS-033 at Horsies.create, and the refusal does not carry the name", () => {
    assert.deepEqual(createRefusals([namedWorkflow("a\uD800")]), [{ code: "HRS-033", path: "workflows[0]" }]);
    assert.deepEqual(createRefusals([namedWorkflow("\uDC00a")]), [{ code: "HRS-033", path: "workflows[0]" }]);
  });

  test(`a name of more than ${MAX_WORKFLOW_NAME_CHARS} characters: HRS-033; the bound counts code points`, () => {
    const longest = "n".repeat(MAX_WORKFLOW_NAME_CHARS);
    const longestAstral = "🐎".repeat(MAX_WORKFLOW_NAME_CHARS);

    assert.deepEqual(createRefusals([namedWorkflow(longest)]), []);
    assert.deepEqual(createRefusals([namedWorkflow(longestAstral)]), []);
    assert.deepEqual(createRefusals([namedWorkflow(`${longest}n`)]), [{ code: "HRS-033", path: "workflows[0]" }]);

    const [refused] = checkedAtCreate([namedWorkflow(`${longestAstral}🐎`)]).refusals;

    assert.equal(refused?.code, "HRS-033");
    assert.equal(refused?.message, "workflows[0]: the workflow name has 256 characters; the most is 255");
  });

  test("a definition key with a lone surrogate: HRS-033, and the refusal does not carry the key", () => {
    const refused = checkedAtCreate([keyedWorkflow("tests.\uD800"), keyedWorkflow("\uDC00.v1")]).refusals;

    assert.deepEqual(
      refused.map((one) => ({ code: one.code, message: one.message })),
      [
        { code: "HRS-033", message: "workflows[0] keyed: the definition key holds a lone surrogate" },
        { code: "HRS-033", message: "workflows[1] keyed: the definition key holds a lone surrogate" },
      ],
    );
  });

  test(`a definition key of more than ${MAX_DEFINITION_KEY_CHARS} characters: HRS-033; the bound counts code points`, () => {
    const longest = "k".repeat(MAX_DEFINITION_KEY_CHARS);
    const longestAstral = "🐎".repeat(MAX_DEFINITION_KEY_CHARS);

    assert.deepEqual(createRefusals([keyedWorkflow(longest)]), []);
    assert.deepEqual(createRefusals([keyedWorkflow(longestAstral)]), []);
    assert.deepEqual(createRefusals([keyedWorkflow(`${longest}k`)]), [{ code: "HRS-033", path: "workflows[0] keyed" }]);

    const [refused] = checkedAtCreate([keyedWorkflow(`${longestAstral}🐎`)]).refusals;

    assert.equal(refused?.code, "HRS-033");
    assert.equal(refused?.message, "workflows[0] keyed: the definition key has 256 characters; the most is 255");
  });

  test("a key of another workflow, and a workflow listed twice: HRS-017", () => {
    const sameKey = defineWorkflow(
      { name: "other", definitionKey: "tests.plain.v1", outputType: h.int(), parameters: {} },
      (w) => ({
        output: w.task(ping, {}, { nodeId: "a" }),
      }),
    );

    assert.deepEqual(createCodes([plain, sameKey]), ["HRS-017"]);
    assert.deepEqual(createCodes([plain, plain]), ["HRS-017"]);
  });

  test("params with a key and no check cases: HRS-027", () => {
    const int = h.int();

    const noCases = defineWorkflow<{ readonly n: typeof int }, typeof int>(
      // SAFETY: the types require check cases for params with a key; the create check refuses them at run time too.
      { name: "c", definitionKey: "tests.c.v1", outputType: int, parameters: { n: int } } as never,
      (w) => ({
        output: w.task(ping, {}, { nodeId: "a" }),
      }),
    );

    assert.deepEqual(createCodes([noCases]), ["HRS-027"]);
  });

  test("an error rule outside fail and pause: HRS-033; params that do not compile: HRS-105", () => {
    // SAFETY: the types refuse this error rule; the create check refuses it at run time too.
    const badRule = defineWorkflow(
      { name: "d", definitionKey: "tests.d.v1", outputType: h.int(), parameters: {}, onError: "stop" as never },
      (w) => ({
        output: w.task(ping, {}, { nodeId: "a" }),
      }),
    );

    const badParams = defineWorkflow(
      {
        name: "e",
        definitionKey: "tests.e.v1",
        outputType: h.int(),
        parameters: { workflow_context: h.int() },
        checkCases: [{ workflow_context: 1 }],
      },
      (w) => ({ output: w.task(ping, {}, { nodeId: "a" }) }),
    );

    assert.deepEqual(createCodes([badRule]), ["HRS-033"]);
    assert.deepEqual(createCodes([badParams]), ["HRS-105"]);
  });

  test("a refusal of the parameters has a parameters path; a refusal of the declared output has an outputType path", () => {
    const badParams = defineWorkflow(
      {
        name: "params",
        definitionKey: "tests.params.v1",
        outputType: h.int(),
        parameters: { workflow_context: h.int() },
        checkCases: [{ workflow_context: 1 }],
      },
      (w) => ({ output: w.task(ping, {}, { nodeId: "a" }) }),
    );
    // SAFETY: a JavaScript caller can give an output that the types refuse; the create check refuses it.

    const badOutput = defineWorkflow(
      { name: "out", definitionKey: "tests.out.v1", outputType: h.int().optional() as never, parameters: {} },
      ((w: WorkflowBuilder) => ({ output: w.task(ping, {}, { nodeId: "a" }) })) as never,
    );

    const [params] = checkedAtCreate([badParams]).refusals;
    const [output] = checkedAtCreate([badOutput]).refusals;

    assert.match(params?.message ?? "", /^workflows\[0\] params: parameters\.workflow_context: /u);
    assert.match(output?.message ?? "", /^workflows\[0\] out: outputType: .*\[WireKind\]$/u);
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
      {
        name: "f",
        definitionKey: "tests.f.v1",
        outputType: h.int(),
        parameters: { n: h.int() },
        checkCases: [{ n: "one" } as never],
      },
      (w) => ({ output: w.task(ping, {}, { nodeId: "a" }) }),
    );

    const throws = defineWorkflow(
      { name: "g", definitionKey: "tests.g.v1", outputType: h.int(), parameters: { n: h.int() }, checkCases: [{ n: 1 }] },
      () => {
        throw new RangeError("no");
      },
    );

    assert.deepEqual(checkCodes([typed]), ["HRS-028"]);
    assert.deepEqual(checkCodes([throws]), ["HRS-029"]);
  });

  test("a check case whose build fails a spec check gives the finding", () => {
    const finding = defineWorkflow(
      { name: "i", definitionKey: "tests.i.v1", outputType: h.int(), parameters: { n: h.int() }, checkCases: [{ n: 1 }] },
      (w) => ({
        output: w.task(ping, {}, { nodeId: "bad id" }),
      }),
    );

    assert.deepEqual(checkCodes([finding]), ["HRS-003"]);
  });

  test("a child that includes itself with the same params and no wait: HRS-037", () => {
    const loop: object = defineWorkflow(
      { name: "loop", definitionKey: "tests.loop.v1", outputType: h.int(), parameters: {} },
      (w) => {
        // SAFETY: the test nests the workflow in itself; the contract of `loop` is complete when the build runs.
        const again = w.subWorkflow(loop as never, {}, { nodeId: "again" });

        // SAFETY: the child's declared output is `h.int()`; the self reference leaves its type unknown here.
        return { output: again as NodeRef<number, string, "again"> };
      },
    );

    assert.deepEqual(checkCodes([loop]), ["HRS-037"]);
  });

  test("child params that read back at the node checks and then do not decode in the walk: HRS-031", () => {
    // A check of the params that is not pure: it passes up to `passes` calls, then refuses. The first run counts the
    // calls of a walk that passes; the second refuses only the last one, the decode of the walk.
    let calls = 0;
    let passes = Number.POSITIVE_INFINITY;

    const child = defineWorkflow(
      {
        name: "child",
        definitionKey: "tests.impure_child.v1",
        outputType: h.int(),
        parameters: {
          n: z
            .number()
            .int()
            .refine(() => (calls += 1) <= passes),
        },
        checkCases: [{ n: 1 }],
      },
      (w) => ({ output: w.task(ping, {}, { nodeId: "a" }) }),
    );

    const parent = defineWorkflow(
      { name: "parent", definitionKey: "tests.impure_parent.v1", outputType: h.int(), parameters: {} },
      (w) => ({ output: w.subWorkflow(child, { n: 2 }, { nodeId: "child" }) }),
    );

    assert.deepEqual(checkCodes([child, parent]), []);

    passes = calls - 1;
    calls = 0;

    assert.deepEqual(checkCodes([child, parent]), ["HRS-031"]);
  });

  test("a nesting past depth 32 with no wait: HRS-036", () => {
    const deep: object = defineWorkflow(
      {
        name: "deep",
        definitionKey: "tests.deep.v1",
        outputType: h.int(),
        parameters: { level: h.int() },
        checkCases: [{ level: 0 }],
      },
      // SAFETY: the test nests the workflow in itself; the contract of `deep` is complete when the build runs.
      (w, { level }) => ({
        // SAFETY: as above; the child's declared output is `h.int()`.
        output: w.subWorkflow(deep as never, { level: level + 1 }, { nodeId: "next" }) as NodeRef<number, string, "next">,
      }),
    );

    assert.deepEqual(checkCodes([deep]), ["HRS-036"]);
  });

  test("a root with as many child builds as the bound: no finding, and every child is built", () => {
    const builds = { count: 0 };
    const leaf = countedLeaf(builds);
    const checked = checkedAtCreate([leaf, fanRoot("fan", leaf, MAX_CHECK_SUBWORKFLOW_BUILDS)]);
    const buildsBefore = builds.count;

    assert.deepEqual(checked.refusals, []);
    assert.deepEqual(checkWorkflows(checked.registry), []);
    // The leaf is a root of its own (one build), and the fan root builds it once per child.
    assert.equal(builds.count - buildsBefore, MAX_CHECK_SUBWORKFLOW_BUILDS + 1);
  });

  test("a root with more child builds than the bound: one HRS-039 at the root, and the walk builds no child past the bound", () => {
    const builds = { count: 0 };
    const leaf = countedLeaf(builds);
    const checked = checkedAtCreate([leaf, fanRoot("fan", leaf, MAX_CHECK_SUBWORKFLOW_BUILDS + 5)]);

    assert.deepEqual(checked.refusals, []);

    const buildsBefore = builds.count;
    const found = checkWorkflows(checked.registry);

    assert.deepEqual(
      found.map((refused) => [refused.code, refused.path]),
      [["HRS-039", "workflow fan"]],
    );
    assert.match(found[0]?.message ?? "", /the subworkflow nesting walk reached its bound of builds/);
    // The builds of the leaf: its own root build, and one per granted child build.
    assert.equal(builds.count - buildsBefore, MAX_CHECK_SUBWORKFLOW_BUILDS + 1);
  });

  test("the bound counts per root: a second root over the bound gets its own HRS-039, a root under it none", () => {
    const leaf = countedLeaf({ count: 0 });
    const over = MAX_CHECK_SUBWORKFLOW_BUILDS + 1;

    const found = checkWorkflows(
      checkedAtCreate([leaf, fanRoot("fanA", leaf, over), fanRoot("small", leaf, 3), fanRoot("fanB", leaf, over)]).registry,
    );

    assert.deepEqual(
      found.map((refused) => [refused.code, refused.path]),
      [
        ["HRS-039", "workflow fanA"],
        ["HRS-039", "workflow fanB"],
      ],
    );
  });

  test("a nesting that doubles its builds at each level: one HRS-039, the last finding of the root", () => {
    const fan: object = defineWorkflow(
      {
        name: "doubling",
        definitionKey: "tests.doubling.v1",
        outputType: h.int(),
        parameters: { level: h.int() },
        checkCases: [{ level: 0 }],
      },
      // SAFETY: the test nests the workflow in itself; the contract of `fan` is complete when the build runs.
      (w, { level }) => ({
        output: [0, 1].map((side) =>
          // SAFETY: as above; the child's declared output is `h.int()`.
          w.subWorkflow(fan as never, { level: level + 1 }, { nodeId: `side${side}` }),
        )[0] as NodeRef<number, string, "side0">,
      }),
    );

    const codes = checkCodes([fan]);

    assert.equal(codes.filter((code) => code === "HRS-039").length, 1);
    assert.equal(codes.at(-1), "HRS-039");
  });
});
