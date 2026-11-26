// The workflow context in a child: the `workflow_ctx` member that the core writes into the input of a task with
// `workflowContext: true` (a node with `ctxFrom`) is the runtime's `context`; an input with no member (a direct send, a
// node with no `ctxFrom`) gives `noContext`; a member that does not read is `WORKER_SERIALIZATION_ERROR`; the input of
// a task that takes no context decodes as it is; the plan gives the context use of each task to the child host.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { RunTable } from "../../../horsies/src/child/run-table.ts";
import { postDispatcher, taskContextUseOf } from "../../../horsies/src/child/serve.ts";
import { StateValues } from "../../../horsies/src/child/state.ts";
import { NO_BODY_OBSERVER } from "../../../horsies/src/child/body-observer.ts";
import { implementationOf, implementationParts, type ImplementationParts } from "../../../horsies/src/contract/implementation.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { ChildPost } from "../../../horsies/src/native/child/addon.ts";
import type { CoreDefect } from "../../../horsies/src/thrown-errors.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { RuntimeWorkflowContext } from "../../../horsies/src/workflow/context.ts";
import { FakeChildNative, settled } from "./child-native.ts";
import { RecordingMarks, RUN_TASK_ID } from "./support.ts";

const WORKFLOW_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5d";

/** The task of a context node. */
const produce = defineTask({ name: "produce", input: {}, output: h.int() });

/** A task that takes the workflow context. */
const audit = defineTask({ name: "audit", input: { n: h.int() }, output: h.null(), workflowContext: true });

/** A task that takes no context. */
const plain = defineTask({ name: "plain", input: { n: h.int() }, output: h.null() });

/** The `workflow_ctx` member that the core writes for a node whose `ctxFrom` names the node `produce`. */
const CONTEXT_MEMBER =
  `{"workflow_id":"${WORKFLOW_ID}","task_index":2,"task_name":"audit",` +
  `"results_by_id":{"produce":{"__h_task_result__":true,"ok":10,"err":null}},"summaries_by_id":{}}`;

/** What a body saw: its input and the workflow context of its runtime. */
type Seen = { readonly input: object; readonly context: RuntimeWorkflowContext };

function served(implementations: readonly ImplementationParts[]): {
  readonly post: (post: ChildPost) => void;
  readonly native: FakeChildNative;
  readonly defects: CoreDefect[];
} {
  const native = new FakeChildNative();
  const defects: CoreDefect[] = [];
  const marks = new RecordingMarks();

  const post = postDispatcher({
    implementations: new Map(implementations.map((parts) => [String(Reflect.get(parts.contract, "name")), parts])),
    table: new RunTable(native, marks, new StateValues([])),
    observer: NO_BODY_OBSERVER,
    native,
    marks,
    reportDefect: (defect) => defects.push(defect),
  });

  return { post, native, defects };
}

function runBody(runKey: bigint, taskName: string, inputJson: string): ChildPost {
  return { kind: "runBody", runKey, taskName, inputJson, taskId: RUN_TASK_ID, attempt: 1, facts: { kind: "notRecorded" } };
}

/** A child of `audit` whose body records what it saw. */
function auditChild(seen: Seen[]): ReturnType<typeof served> {
  return served([
    implementationParts(
      implementationOf(
        audit,
        (input, runtime) => {
          seen.push({ input, context: runtime.workflowContext });

          return runtime.ok(null);
        },
        [],
      ),
    ),
  ]);
}

describe("the workflow context in a child", () => {
  test("a node with ctxFrom: the workflow_ctx member is the runtime's context, and the parameters decode without it", async () => {
    const seen: Seen[] = [];
    const child = auditChild(seen);

    child.post(runBody(3n, "audit", `{"n":2,"workflow_ctx":${CONTEXT_MEMBER}}`));
    await settled();

    const first = seen[0] ?? assert.fail("the body did not run");

    assert.deepEqual(first.input, { n: 2 });

    const context = first.context.kind === "context" ? first.context.context : assert.fail("the runtime holds no context");

    assert.deepEqual([context.workflowId, context.taskIndex, context.taskName], [WORKFLOW_ID, 2, "audit"]);
    assert.deepEqual(context.resultFor("produce", produce), { ok: true, value: { kind: "succeeded", value: 10 } });
    assert.deepEqual(child.native.values, [[3n, "null"]]);
  });

  test("a direct send and a node with no ctxFrom: an input with no workflow_ctx member gives noContext", async () => {
    const seen: Seen[] = [];
    const child = auditChild(seen);

    child.post(runBody(4n, "audit", '{"n":2}'));
    await settled();
    assert.deepEqual(seen, [{ input: { n: 2 }, context: { kind: "noContext" } }]);
    assert.deepEqual(child.native.values, [[4n, "null"]]);
  });

  test("a workflow_ctx member that does not read is WORKER_SERIALIZATION_ERROR, and the body does not run", async () => {
    const seen: Seen[] = [];
    const child = auditChild(seen);

    child.post(runBody(5n, "audit", '{"n":2,"workflow_ctx":{"workflow_id":"7","task_index":0,"task_name":"audit"}}'));
    await settled();
    assert.deepEqual(seen, []);
    assert.deepEqual(child.native.errors[0]?.[1].code, { kind: "library", code: "WORKER_SERIALIZATION_ERROR" });
    assert.deepEqual(child.native.errors[0]?.[1].message, {
      kind: "text",
      text: "task input does not decode as audit.input: the workflow context workflow_id: not a UUID in the lower-case 8-4-4-4-12 form",
    });
    assert.deepEqual(child.defects, []);
  });

  test("a task that takes no context: the input decodes as it is, so a workflow_ctx member is an undeclared key", async () => {
    const seen: object[] = [];

    const child = served([
      implementationParts(
        implementationOf(
          plain,
          (input, { ok }) => {
            seen.push(input);

            return ok(null);
          },
          [],
        ),
      ),
    ]);

    child.post(runBody(6n, "plain", '{"n":2}'));
    child.post(runBody(7n, "plain", `{"n":2,"workflow_ctx":${CONTEXT_MEMBER}}`));
    await settled();
    assert.deepEqual(seen, [{ n: 2 }]);
    assert.deepEqual(child.native.values, [[6n, "null"]]);
    assert.deepEqual(child.native.errors[0]?.[0], 7n);
    assert.deepEqual(child.native.errors[0]?.[1].code, { kind: "library", code: "WORKER_SERIALIZATION_ERROR" });
  });

  test("the child host registers a task with workflowContext: true as TAKES_CONTEXT, any other task as NO_CONTEXT", () => {
    assert.deepEqual([taskContextUseOf(audit), taskContextUseOf(plain)], ["TAKES_CONTEXT", "NO_CONTEXT"]);
  });
});
