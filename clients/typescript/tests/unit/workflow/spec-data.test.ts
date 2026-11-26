// The conversion of a checked spec to the core's spec data text: one build per node kind, join, policy, deadline and
// sub-workflow form, the order of the queue classes, and the refusal of a string that has no text.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { WireJson } from "../../../horsies/src/_catalog/wire_json.ts";
import { defineTask } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { NodeDeadline } from "../../../horsies/src/workflow/findings.ts";
import type { BuildReturn } from "../../../horsies/src/workflow/node.ts";
import { defineWorkflow, type WorkflowBuilder } from "../../../horsies/src/workflow/public.ts";
import { type ConversionFacts, specDataTextOf } from "../../../horsies/src/workflow/spec-data.ts";
import { specDataText } from "../../../horsies/src/workflow/spec-text.ts";
import { passedSpec, viewOf } from "./support.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.int() });
const add = defineTask({ name: "add", input: { a: h.int(), b: h.resultOf(h.int()) }, output: h.int() });
const maybe = defineTask({ name: "maybe", input: { first: h.resultOf(h.int()).optional() }, output: h.int() });

const leaf = defineWorkflow({ name: "leaf", definitionKey: "tests.leaf.v1", params: {} }, (w) => ({
  output: w.task(ping, {}, { id: "ping" }),
}));

const region = defineWorkflow(
  {
    name: "region",
    definitionKey: "tests.region.v1",
    params: { name: h.string(), count: h.resultOf(h.int()).optional() },
    checkCases: [{ name: "eu" }],
  },
  (w) => ({ output: w.task(ping, {}, { id: "ping" }) }),
);

const view = viewOf([ping, add, maybe], [leaf, region]);

/** The deadlines that the fake facts were asked for, by task name. */
type Asked = { readonly taskName: string; readonly deadline: NodeDeadline }[];

/** Facts that write a short options text, give the class `class_<queue>`, and the priority 100 of each queue. */
function fakeFacts(asked: Asked = []): ConversionFacts {
  return {
    taskOptionsText: (taskName, deadline) => {
      asked.push({ taskName, deadline });

      return `{"task":"${taskName}"}`;
    },
    queueClass: (queueName) => `class_${queueName}`,
    queuePriority: () => 100,
  };
}

function textOf(build: (w: WorkflowBuilder) => BuildReturn, facts: ConversionFacts = fakeFacts()): string {
  const workflow = defineWorkflow(
    { name: "flow", definitionKey: "tests.flow.v1", params: {}, onError: "pause" },
    build,
  );

  const text = specDataTextOf(passedSpec(workflow, {}, view), facts);

  assert(text.ok, text.ok ? "" : text.error.detail);

  return text.value;
}

/** The text of a node of the default queue with the fake options text. */
function taskNode(index: number, id: string, rest: string): string {
  return (
    `{"index":${index},"node_id":"${id}",${rest},"body":{"kind":"TASK","queue":"default","priority":100,` +
    `"task_options":{"kind":"ENCODED","json":"{\\"task\\":\\"${id === "add" ? "add" : "ping"}\\"}"}}}`
  );
}

describe("specDataTextOf", () => {
  test("a task node with an ALL join and no policy: the whole document", () => {
    const text = textOf((w) => ({ output: w.task(ping, {}, { id: "ping" }) }));

    assert.equal(
      text,
      '{"data_version":1,"name":"flow","definition_key":{"kind":"KEYED","key":"tests.flow.v1"},"on_error":"PAUSE",' +
        '"output":{"kind":"NODE","index":0},"success_policy":{"kind":"NO_POLICY"},' +
        '"queue_classes":{"default":"class_default"},"nodes":[' +
        taskNode(
          0,
          "ping",
          '"task_name":"ping","input_json":"{}","dependencies":[],"args_from":{},"workflow_ctx_from":[],' +
            '"allow_failed_deps":false,"join":{"kind":"ALL"}',
        ) +
        "]}",
    );
  });

  test("a node parameter: args_from, the dependency, and the static input without the member", () => {
    const text = textOf((w) => {
      const source = w.task(ping, {}, { id: "source" });

      return { output: w.task(add, { a: 2, b: source }, { id: "add", allowFailedDeps: true }) };
    });

    assert.match(
      text,
      /"node_id":"add","task_name":"add","input_json":"\{\\"a\\":2\}","dependencies":\[0\],"args_from":\{"b":0\},"workflow_ctx_from":\[\],"allow_failed_deps":true/,
    );
  });

  test("ANY and QUORUM joins, and a CASES policy with optional nodes", () => {
    const text = textOf((w) => {
      const first = w.task(ping, {}, { id: "first" });
      const second = w.task(ping, {}, { id: "second" });
      const any = w.task(maybe, { first }, { id: "any", waitsFor: [second], join: { kind: "any" } });

      const quorum = w.task(
        ping,
        {},
        { id: "quorum", waitsFor: [first, second], join: { kind: "quorum", minSuccess: 1 } },
      );

      return {
        output: quorum,
        successPolicy: { cases: [{ required: [quorum] }, { required: [any] }], optional: [first] },
      };
    });

    assert.match(text, /"node_id":"any".*"join":\{"kind":"ANY"\}/);
    assert.match(text, /"node_id":"quorum".*"join":\{"kind":"QUORUM","min_success":1\}/);
    assert.match(
      text,
      /"success_policy":\{"kind":"CASES","cases":\[\{"required":\[3\]\},\{"required":\[2\]\}\],"optional":\[0\]\}/,
    );
  });

  test("a node's own priority, and the priority of its queue from the core's facts", () => {
    const facts = { ...fakeFacts(), queuePriority: () => 37 };

    const text = textOf((w) => {
      w.task(ping, {}, { id: "own", priority: 5 });

      return { output: w.task(ping, {}, { id: "queue" }) };
    }, facts);

    assert.match(text, /"node_id":"own".*?"priority":5,/);
    assert.match(text, /"node_id":"queue".*?"priority":37,/);
  });

  test("a node deadline: the core's facts give the options text with it", () => {
    const asked: Asked = [];

    textOf(
      (w) => ({
        output: w.task(ping, {}, { id: "ping", goodUntil: Temporal.Instant.from("2026-08-12T09:10:12.654321Z") }),
      }),
      fakeFacts(asked),
    );

    assert.deepEqual(asked, [{ taskName: "ping", deadline: { kind: "until", epochMicros: 1_786_525_812_654_321n } }]);
  });

  test("sub-workflow nodes: NOT_GIVEN with {}, GIVEN with the static params and the args_from of a node", () => {
    const text = textOf((w) => {
      const counted = w.task(ping, {}, { id: "count" });

      w.subWorkflow(leaf, {}, { id: "leaf" });

      return { output: w.subWorkflow(region, { name: "eu", count: counted }, { id: "region" }) };
    });

    assert.match(
      text,
      /"node_id":"leaf","task_name":"leaf","input_json":"\{\}".*"body":\{"kind":"SUB_WORKFLOW","definition_key":"tests.leaf.v1","params":"NOT_GIVEN"\}/,
    );
    assert.match(
      text,
      /"node_id":"region","task_name":"region","input_json":"\{\\"name\\":\\"eu\\"\}","dependencies":\[0\],"args_from":\{"count":0\}.*"params":"GIVEN"\}/,
    );
    assert.match(text, /"queue_classes":\{"default":"class_default"\}/);
  });

  test("the queue classes follow the nodes that first name them; an integer-like queue name keeps its place", () => {
    const urgent = defineTask({ name: "urgent", queue: "b", input: {}, output: h.int() });
    const numbered = defineTask({ name: "numbered", queue: "10", input: {}, output: h.int() });

    const queues = new Map([
      ["b", { name: "b", maxConcurrency: { kind: "coreDefault" as const } }],
      ["10", { name: "10", maxConcurrency: { kind: "coreDefault" as const } }],
    ]);

    const custom = viewOf([urgent, numbered], [], { kind: "custom", queues });

    const workflow = defineWorkflow({ name: "queues", definitionKey: "tests.queues.v1", params: {} }, (w) => {
      w.task(urgent, {}, { id: "first" });
      w.task(numbered, {}, { id: "second" });

      return { output: w.task(urgent, {}, { id: "third" }) };
    });

    const text = specDataTextOf(passedSpec(workflow, {}, custom), fakeFacts());

    assert(text.ok);
    assert.match(text.value, /"queue_classes":\{"b":"class_b","10":"class_10"\}/);
  });

  test("a workflow name with a lone surrogate has no text: LoneSurrogate at its path", () => {
    const workflow = defineWorkflow({ name: "bad\uD800", definitionKey: "tests.bad.v1", params: {} }, (w) => ({
      output: w.task(ping, {}, { id: "ping" }),
    }));

    const text = specDataTextOf(passedSpec(workflow, {}, view), fakeFacts());

    assert.deepEqual(text, {
      ok: false,
      error: { kind: "LoneSurrogate", path: "$.name", detail: "a string holds a lone surrogate" },
    });
  });
});

describe("specDataText", () => {
  test("writes bigint digits, map members in order, and strings as the codec writes them", () => {
    const text = specDataText(
      new Map<string, WireJson>([
        ["z", 10n],
        ["a", [true, null, "é\n"]],
      ]),
    );

    assert.deepEqual(text, { ok: true, value: '{"z":10,"a":[true,null,"é\\n"]}' });
  });

  test("a number is a defect of the conversion: TypeError", () => {
    assert.throws(() => specDataText(new Map([["index", 1]])), TypeError);
  });
});
