// A workflow with no `outputType` (Python `output=None`, Rust `NoWorkflowOutput`): the checks of its builds, the spec
// data, `get` of its plain envelope (`NoWorkflowOutput` or the stored error), the read of a sub-workflow node of such a
// child, and an `h.taskOutcome` parameter of it.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { checkContractList, contractLookup } from "../../../horsies/src/app/contract-list.ts";
import { jsonTextCheckedByCore } from "../../../horsies/src/codec/json-text.ts";
import { decodeParameters } from "../../../horsies/src/codec/reader.ts";
import { encodeValue } from "../../../horsies/src/codec/writer.ts";
import { compileContract } from "../../../horsies/src/contract/compile.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { WorkflowId } from "../../../horsies/src/contract/ids.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { kindOf } from "../../../horsies/src/wire/wire-type.ts";
import { workflowHandleByIdOf, workflowHandleOf } from "../../../horsies/src/workflow/handle.ts";
import type { BuildReturn, BuildReturnWithoutOutput } from "../../../horsies/src/workflow/node.ts";
import { NO_WORKFLOW_OUTPUT, NO_WORKFLOW_OUTPUT_WIRE } from "../../../horsies/src/workflow/no-output.ts";
import { defineWorkflow, type WorkflowBuilder } from "../../../horsies/src/workflow/public.ts";
import { listedWorkflowOf, type WorkflowRegistry, workflowRegistry } from "../../../horsies/src/workflow/registry.ts";
import { specDataTextOf } from "../../../horsies/src/workflow/spec-data.ts";
import { customErrorText, fakeCore, okEnvelope, WORKFLOW_ID } from "./core-support.ts";
import { findingCodes, passedSpec, viewOf } from "./support.ts";

const count = defineTask({ name: "count", input: {}, output: h.int() });
const word = defineTask({ name: "word", input: {}, output: h.string(), errorCodes: ["NO_WORD"] });

/** Python `spec_fanout` with no output: a root, then the terminal nodes b and c. */
const fanout = defineWorkflow({ name: "fanout", definitionKey: "tests.fanout.v1", parameters: {} }, (w) => {
  const root = w.task(count, {}, { nodeId: "root" });

  w.task(count, {}, { nodeId: "b", waitsFor: [root] });
  w.task(word, {}, { nodeId: "c", waitsFor: [root] });

  return {};
});

const afterChild = defineTask({ name: "after_child", input: { done: h.taskOutcome(fanout) }, output: h.int() });

const parent = defineWorkflow(
  { name: "parent", definitionKey: "tests.parent.v1", outputType: h.int(), parameters: {} },
  (w) => {
    const child = w.subWorkflow(fanout, {}, { nodeId: "child" });

    return { output: w.task(afterChild, { done: child }, { nodeId: "after" }) };
  },
);

const tasks = [count, word, afterChild];
const view = viewOf(tasks, [fanout, parent]);

function registry(): WorkflowRegistry {
  const listed = [fanout, parent].map((workflow) => {
    const entry = listedWorkflowOf(workflow);

    assert(entry.ok);

    return entry.value;
  });

  return workflowRegistry(listed, contractLookup(checkContractList(tasks, { kind: "default" }).entries), {
    kind: "default",
  });
}

function workflowId(): WorkflowId {
  const parsed = WorkflowId.parse(WORKFLOW_ID);

  assert(parsed.ok);

  return parsed.value;
}

/** The plain envelope that the core stores for a COMPLETED workflow with no output node. */
const PLAIN_ENVELOPE = '{"__h_task_result__":true,"ok":null,"err":null}';

/** The envelope form that the core stored before the plain one: a marker key and a map of the terminal nodes in `ok`. */
const OLD_TERMINAL_MAP_ENVELOPE =
  '{"__h_task_result__":true,"__h_outputless_terminals__":true,' +
  '"ok":{"results_by_id":{},"task_name_by_id":{},"definition_key_by_id":{}},"err":null}';

/** `get` of `workflow` when the wait finds the COMPLETED stored result `text`; `R` is the build return of `workflow`. */
async function getOf<R extends BuildReturn = BuildReturnWithoutOutput>(workflow: object, text: string) {
  const core = fakeCore({ wait: { kind: "completed", result: { kind: "checked", text } } });

  return workflowHandleOf<R>({
    core,
    registry: registry(),
    workflow,
    workflowId: workflowId(),
    targets: new Map(),
  }).get();
}

describe("the checks of a workflow with no outputType", () => {
  test("a build with no output node passes: the spec has no output, and the spec data writes NO_OUTPUT", () => {
    const spec = passedSpec(fanout, {}, view);

    assert.deepEqual(spec.output, { kind: "noOutput" });

    const text = specDataTextOf(spec, {
      taskOptionsText: (taskName) => `{"task":"${taskName}"}`,
      queueClass: (queueName) => `class_${queueName}`,
      queuePriority: () => 100,
    });

    assert(text.ok);
    assert.match(text.value, /"output":\{"kind":"NO_OUTPUT"\},/);
  });

  test("a build that returns an output node: HRS-025", () => {
    // SAFETY: the types refuse an output node when the workflow declares no outputType; the check refuses it too.
    const returnsNode = defineWorkflow(
      { name: "extra", definitionKey: "tests.extra.v1", parameters: {} },
      ((w: WorkflowBuilder) => ({ output: w.task(count, {}, { nodeId: "a" }) })) as never,
    );

    assert.deepEqual(findingCodes(returnsNode, {}, view), ["HRS-025"]);
  });

  test("a declared outputType and a build with no output node: HRS-025", () => {
    // SAFETY: the types require an output node for a declared outputType; the check refuses its absence too.
    const noNode = defineWorkflow(
      { name: "missing", definitionKey: "tests.missing.v1", outputType: h.int(), parameters: {} },
      ((w: WorkflowBuilder) => {
        w.task(count, {}, { nodeId: "a" });

        return {};
      }) as never,
    );

    assert.deepEqual(findingCodes(noNode, {}, view), ["HRS-025"]);
  });

  test("a build that returns a value that is not an object: HRS-011", () => {
    // SAFETY: the types require an object; the check refuses another value too.
    const notObject = defineWorkflow(
      { name: "scalar", definitionKey: "tests.scalar.v1", parameters: {} },
      ((w: WorkflowBuilder) => {
        w.task(count, {}, { nodeId: "a" });

        return 7;
      }) as never,
    );

    assert.deepEqual(findingCodes(notObject, {}, view), ["HRS-011"]);
  });

  test("a sub-workflow node of the child feeds an h.taskOutcome parameter of the child: no finding", () => {
    const spec = passedSpec(parent, {}, view);

    assert.deepEqual(spec.output, { kind: "node", index: 1 });
    assert.deepEqual([...(spec.nodes[1]?.argsFrom ?? [])], [["done", 0]]);
  });
});

describe("the success value of a sub-workflow node of a child with no output", () => {
  test("an h.taskOutcome parameter of the child reads the stored null as NoWorkflowOutput", () => {
    const compiled = compileContract(afterChild);

    assert(compiled.ok);

    const decoded = decodeParameters(
      compiled.value.parameters,
      jsonTextCheckedByCore('{"done":{"__h_task_result__":true,"ok":null,"err":null}}'),
      "worker",
    );

    assert.deepEqual(decoded, { ok: true, value: { done: { kind: "succeeded", value: NO_WORKFLOW_OUTPUT } } });
  });

  test("the wire type writes NoWorkflowOutput as null, and refuses another value", () => {
    const kind = kindOf(NO_WORKFLOW_OUTPUT_WIRE);

    assert.deepEqual(encodeValue(kind, NO_WORKFLOW_OUTPUT), { ok: true, value: "null" });

    const refused = encodeValue(kind, { kind: "other" });

    assert(!refused.ok);
    assert.equal(refused.error.refusal, "CheckFailed");
    assert.equal(refused.error.detail, 'the conversion refused the value: kind expected "noWorkflowOutput"');
  });

  test("resultFor of the node, by the started handle and by the handle of the id, gives NoWorkflowOutput", async () => {
    const core = fakeCore({ nodeResult: { kind: "stored", result: { kind: "checked", text: okEnvelope("null") } } });
    const facts = { core, registry: registry(), workflow: parent, workflowId: workflowId() };

    const started = workflowHandleOf<BuildReturn>({
      ...facts,
      targets: new Map([["child", { kind: "subWorkflow", workflow: fanout } as const]]),
    });

    const byId = workflowHandleByIdOf<BuildReturn>({ ...facts, targets: new Map() });
    const expected = { ok: true, value: { kind: "succeeded", value: NO_WORKFLOW_OUTPUT } };

    assert.deepEqual(await started.resultFor("child"), expected);
    assert.deepEqual(await byId.resultFor("child", fanout), expected);
  });
});

describe("get of a workflow with no outputType", () => {
  test("a COMPLETED workflow stores the plain envelope: success with NoWorkflowOutput", async () => {
    const read = await getOf(fanout, PLAIN_ENVELOPE);

    assert.deepEqual(read, { ok: true, value: { kind: "succeeded", value: NO_WORKFLOW_OUTPUT } });
  });

  test("a stored error: the failed outcome with that error", async () => {
    const text = `{"__h_task_result__":true,"ok":null,"err":${customErrorText("ANY")}}`;
    const read = await getOf(fanout, text);

    assert(read.ok && read.value.kind === "failed");
    assert.deepEqual([read.value.error.kind, read.value.error.code], ["custom", "ANY"]);
  });

  const refused: readonly (readonly [string, string])[] = [
    ["an error that does not read", '{"__h_task_result__":true,"ok":null,"err":7}'],
    ["no envelope marker", '{"ok":null,"err":null}'],
    ["a typed envelope with a value in ok", okEnvelope("{}")],
    ["the old terminal-map envelope", OLD_TERMINAL_MAP_ENVELOPE],
  ];

  for (const [title, text] of refused) {
    test(`${title}: storedResultUnreadable`, async () => {
      const read = await getOf(fanout, text);

      assert(!read.ok && read.error.kind === "storedResultUnreadable");
    });
  }

  test("the plain envelope read by a workflow with an outputType: storedResultUnreadable", async () => {
    const read = await getOf<BuildReturn>(parent, PLAIN_ENVELOPE);

    assert(!read.ok && read.error.kind === "storedResultUnreadable");
  });
});
