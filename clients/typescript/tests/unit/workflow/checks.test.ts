// The builder and the spec checks: the dependency order, node parameters, each node option with a valid and a refused
// value, each check code, and the two gates.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { defineTask, type OutputSchema } from "../../../horsies/src/contract/public.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { AnyNode, BuildReturn } from "../../../horsies/src/workflow/node.ts";
import { defineWorkflow, type WorkflowBuilder } from "../../../horsies/src/workflow/public.ts";
import { DEFAULT_QUEUES, findingCodes, passedSpec, viewOf } from "./support.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.int() });
const add = defineTask({ name: "add", input: { a: h.int(), b: h.resultOf(h.int()) }, output: h.int() });
const pair = defineTask({ name: "pair", input: { x: h.resultOf(h.int()), y: h.resultOf(h.int()) }, output: h.int() });
const say = defineTask({ name: "say", input: { text: h.string() }, output: h.string() });
const audit = defineTask({ name: "audit", input: {}, output: h.null(), workflowContext: true });
const maybe = defineTask({ name: "maybe", input: { first: h.resultOf(h.int()).optional() }, output: h.int() });
const tasks = [ping, add, pair, say, audit, maybe];

const leaf = defineWorkflow({ name: "leaf", definitionKey: "tests.leaf.v1", output: h.int(), params: {} }, (w) => ({
  output: w.task(ping, {}, { id: "ping" }),
}));

const region = defineWorkflow(
  {
    name: "region",
    definitionKey: "tests.region.v1",
    output: h.int(),
    params: { name: h.string().optional() },
    checkCases: [{}],
  },
  (w) => ({ output: w.task(ping, {}, { id: "ping" }) }),
);

const view = viewOf(tasks, [leaf, region]);

/** A workflow whose build is `build`, with no params. */
function flow(build: (w: WorkflowBuilder) => BuildReturn, name = "flow", output: OutputSchema = h.int()): object {
  // SAFETY: the builds of these tests return nodes of several types; the checks compare the output node with the
  // declared output at run time (HRS-025), and each test declares the output of its output node.
  return defineWorkflow({ name, definitionKey: `tests.${name}.v1`, output, params: {} }, build as never);
}

describe("the builder", () => {
  test("the dependencies: the parameter sources in declaration order, then waitsFor, each node once", () => {
    const spec = passedSpec(
      flow((w) => {
        const first = w.task(ping, {}, { id: "first" });
        const second = w.task(ping, {}, { id: "second" });
        const third = w.task(ping, {}, { id: "third" });

        return { output: w.task(pair, { y: second, x: first }, { id: "pair", waitsFor: [third, first] }) };
      }),
      {},
      view,
    );

    assert.deepEqual(spec.nodes[3]?.dependencies, [0, 1, 2]);
    assert.deepEqual(
      [...(spec.nodes[3]?.argsFrom ?? [])],
      [
        ["x", 0],
        ["y", 1],
      ],
    );
  });

  test("a node as a parameter adds a dependency and an args_from member; the static input leaves it out", () => {
    const spec = passedSpec(
      flow((w) => {
        const source = w.task(ping, {}, { id: "source" });

        return { output: w.task(add, { a: 1, b: source }, { id: "add" }) };
      }),
      {},
      view,
    );

    assert.equal(spec.nodes[1]?.staticInput, '{"a":1}');
    assert.deepEqual(spec.nodes[1]?.dependencies, [0]);
    assert.deepEqual([...(spec.nodes[1]?.argsFrom ?? [])], [["b", 0]]);
  });

  test("a child with params: {} is NOT_GIVEN with {}; a child with params is GIVEN, also with no member", () => {
    const spec = passedSpec(
      flow((w) => {
        w.subWorkflow(leaf, {}, { id: "leaf" });

        return { output: w.subWorkflow(region, {}, { id: "region" }) };
      }),
      {},
      view,
    );

    assert.deepEqual(spec.nodes[0]?.body, { kind: "subWorkflow", definitionKey: "tests.leaf.v1", params: "NOT_GIVEN" });
    assert.deepEqual(spec.nodes[1]?.body, { kind: "subWorkflow", definitionKey: "tests.region.v1", params: "GIVEN" });
    assert.equal(spec.nodes[1]?.staticInput, "{}");
  });

  test("valid node options: join, allowFailedDeps, ctxFrom, queue, priority and goodUntil", () => {
    const urgentPing = defineTask({ name: "urgent_ping", queue: "urgent", input: {}, output: h.int() });

    const urgentAudit = defineTask({
      name: "urgent_audit",
      queue: "urgent",
      input: {},
      output: h.null(),
      workflowContext: true,
    });

    const custom = viewOf([urgentPing, urgentAudit], [], {
      kind: "custom",
      queues: new Map([
        ["urgent", { name: "urgent", priority: 3, maxConcurrency: { kind: "coreDefault" } }],
        ["slow", { name: "slow", priority: 9, maxConcurrency: { kind: "coreDefault" } }],
      ]),
    });

    const at = Temporal.Instant.from("2026-08-12T09:10:12.654321789Z");

    const spec = passedSpec(
      flow(
        (w) => {
          const one = w.task(urgentPing, {}, { id: "one" });
          const two = w.task(urgentPing, {}, { id: "two", queue: "slow", priority: 7, goodUntil: at });

          return {
            output: w.task(
              urgentAudit,
              {},
              {
                id: "audit",
                waitsFor: [one, two],
                ctxFrom: [two],
                allowFailedDeps: true,
                join: { kind: "quorum", minSuccess: 2 },
              },
            ),
          };
        },
        "flow",
        h.null(),
      ),
      {},
      custom,
    );

    assert.deepEqual(spec.nodes[0]?.body, {
      kind: "task",
      queue: "urgent",
      priority: 3,
      deadline: { kind: "noDeadline" },
    });
    assert.deepEqual(spec.nodes[1]?.body, {
      kind: "task",
      queue: "slow",
      priority: 7,
      deadline: { kind: "until", epochMicros: 1_786_525_812_654_321n },
    });
    assert.deepEqual(spec.nodes[2]?.join, { kind: "quorum", minSuccess: 2 });
    assert.deepEqual(spec.nodes[2]?.ctxFrom, ["two"]);
    assert.equal(spec.nodes[2]?.allowFailedDeps, true);
  });
});

/** The codes of a flow whose one node is `ping` with the options `options`, after a source node. */
function optionCodes(options: object): readonly string[] {
  return findingCodes(
    flow(
      (w) => {
        const source = w.task(ping, {}, { id: "source" });

        // SAFETY: the test gives option values that the types refuse or that only the checks see.
        return { output: w.task(audit, {}, { id: "node", waitsFor: [source], ...options } as never) };
      },
      "flow",
      h.null(),
    ),
    {},
    view,
  );
}

describe("refused node options", () => {
  const cases: readonly (readonly [string, object, readonly string[]])[] = [
    ["an id with a space", { id: "bad id" }, ["HRS-003"]],
    ["an empty id", { id: "" }, ["HRS-003"]],
    ["an id of 129 characters", { id: "n".repeat(129) }, ["HRS-003"]],
    ["the id of another node", { id: "source" }, ["HRS-004"]],
    ["a quorum of 0", { join: { kind: "quorum", minSuccess: 0 } }, ["HRS-013"]],
    ["a quorum above the dependency count", { join: { kind: "quorum", minSuccess: 2 } }, ["HRS-013"]],
    ["a join of another kind", { join: { kind: "most" } }, ["HRS-013"]],
    ["a priority of 0", { priority: 0 }, ["HRS-034"]],
    ["a priority of 101", { priority: 101 }, ["HRS-034"]],
    ["a priority that is no integer", { priority: 1.5 }, ["HRS-034"]],
    ["a queue that the app does not admit", { queue: "urgent" }, ["HRS-014"]],
    ["a deadline in the year 10000", { goodUntil: Temporal.Instant.from("+010000-01-01T00:00:00Z") }, ["FIELD_BOUND"]],
  ];

  for (const [name, options, codes] of cases) {
    test(`${name}: ${codes.join(", ")}`, () => {
      assert.deepEqual(optionCodes(options), codes);
    });
  }

  test("a context source that is not a dependency: HRS-009", () => {
    const codes = findingCodes(
      flow(
        (w) => {
          const source = w.task(ping, {}, { id: "source" });

          return { output: w.task(audit, {}, { id: "audit", ctxFrom: [source] }) };
        },
        "flow",
        h.null(),
      ),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-009"]);
  });

  test("ctxFrom on a task that takes no workflow context: HRS-010", () => {
    const codes = findingCodes(
      flow((w) => {
        const source = w.task(ping, {}, { id: "source" });

        // SAFETY: the types refuse ctxFrom here; the check refuses it at run time too.
        return { output: w.task(ping, {}, { id: "ping", waitsFor: [source], ctxFrom: [source] } as never) };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-010"]);
  });

  test("a node of another build in waitsFor: HRS-006", () => {
    let foreign: AnyNode | undefined;

    passedSpec(
      flow((w) => {
        foreign = w.task(ping, {}, { id: "foreign" });

        return { output: foreign };
      }),
      {},
      view,
    );

    const other = foreign;

    assert(other !== undefined);
    assert.deepEqual(optionCodes({ waitsFor: [other] }), ["HRS-006"]);
  });
});

describe("the checks", () => {
  test("an empty name: HRS-001", () => {
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(ping, {}, { id: "a" }) }), " "),
        {},
        view,
      ),
      ["HRS-001"],
    );
  });

  test("a build with no node: HRS-002", () => {
    // SAFETY: a build with no node cannot return a node; the check refuses it before the output.
    assert.deepEqual(
      findingCodes(
        flow(() => ({ output: {} as AnyNode })),
        {},
        view,
      ),
      ["HRS-002", "HRS-011"],
    );
  });

  test("a build with 10,001 nodes: HRS-038", () => {
    const codes = findingCodes(
      flow((w) => {
        let last: AnyNode = w.task(ping, {}, { id: "n0" });

        for (let index = 1; index <= 10_000; index += 1) {
          last = w.task(ping, {}, { id: `n${index}` });
        }

        return { output: last };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-038"]);
  });

  test("an output that is no node of the build: HRS-011", () => {
    // SAFETY: the types refuse an output that is no node; the check refuses it at run time too.
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(ping, {}, { id: "a" }) && ({ kind: "node", id: "a" } as AnyNode) })),
        {},
        view,
      ),
      ["HRS-011"],
    );
  });

  test("a success policy with no case, or a case with no required node: HRS-012", () => {
    const codes = findingCodes(
      flow((w) => {
        const node = w.task(ping, {}, { id: "a" });

        return { output: node, successPolicy: { cases: [{ required: [node] }, { required: [] }] } };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-012"]);
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(ping, {}, { id: "a" }), successPolicy: { cases: [] } })),
        {},
        view,
      ),
      ["HRS-012"],
    );
  });

  test("a task that the app does not list: HRS-302", () => {
    const other = defineTask({ name: "other", input: {}, output: h.null() });

    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(other, {}, { id: "a" }) })),
        {},
        view,
      ),
      ["HRS-302"],
    );
  });

  test("a child workflow that the app does not list: HRS-020", () => {
    const unlisted = defineWorkflow(
      { name: "unlisted", definitionKey: "tests.unlisted.v1", output: h.int(), params: {} },
      (w) => ({
        output: w.task(ping, {}, { id: "a" }),
      }),
    );

    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.subWorkflow(unlisted, {}, { id: "a" }) })),
        {},
        view,
      ),
      ["HRS-020"],
    );
  });

  test("params for a child that takes none: HRS-022", () => {
    // SAFETY: the types refuse a member for a child with params: {}; the check refuses it at run time too.
    const codes = findingCodes(
      flow((w) => ({ output: w.subWorkflow(leaf, { name: "eu" } as never, { id: "a" }) })),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-022"]);
  });

  test("node inputs: unknown member HRS-019, missing parameter HRS-020, a value of another type HRS-031", () => {
    // SAFETY: each input breaks a rule that the types also refuse; the check refuses it at run time too.
    const unknown = { text: "a", extra: 1 } as never;
    // SAFETY: as above.
    const missing = {} as never;
    // SAFETY: as above.
    const wrong = { text: 7 } as never;

    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(say, unknown, { id: "a" }) }), "flow", h.string()),
        {},
        view,
      ),
      ["HRS-019"],
    );
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(say, missing, { id: "a" }) }), "flow", h.string()),
        {},
        view,
      ),
      ["HRS-020"],
    );
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(say, wrong, { id: "a" }) }), "flow", h.string()),
        {},
        view,
      ),
      ["HRS-031"],
    );
  });

  test("a node for a plain parameter, a value for h.resultOf, a source of another output type: HRS-024", () => {
    const codes = findingCodes(
      flow((w) => {
        const number = w.task(ping, {}, { id: "number" });
        const text = w.task(say, { text: "a" }, { id: "text" });

        // SAFETY: each input breaks a rule that the types also refuse; the check refuses it at run time too.
        w.task(say, { text: number } as never, { id: "plain" });
        // SAFETY: as above.
        w.task(add, { a: 1, b: 2 } as never, { id: "value" });

        // SAFETY: as above.
        return { output: w.task(add, { a: 1, b: text } as never, { id: "other" }) };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-024", "HRS-024", "HRS-024"]);
  });

  test("a required node-fed parameter under an any join: HRS-013; an optional one passes", () => {
    const required = findingCodes(
      flow((w) => {
        const source = w.task(ping, {}, { id: "source" });

        // SAFETY: the types refuse a required node-fed parameter under an any join; the check refuses it too.
        return { output: w.task(add, { a: 1, b: source } as never, { id: "add", join: { kind: "any" } }) };
      }),
      {},
      view,
    );

    assert.deepEqual(required, ["HRS-013"]);
    passedSpec(
      flow((w) => ({
        output: w.task(maybe, { first: w.task(ping, {}, { id: "s" }) }, { id: "m", join: { kind: "any" } }),
      })),
      {},
      view,
    );
  });

  test("an output node of another type than the declared output: HRS-025", () => {
    const codes = findingCodes(
      flow((w) => ({ output: w.task(say, { text: "a" }, { id: "say" }) }), "flow", h.int()),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-025"]);
  });

  test("a sub-workflow node gives its child's declared output: an h.resultOf parameter of another type is HRS-024", () => {
    const takesText = defineTask({ name: "takes_text", input: { text: h.resultOf(h.string()) }, output: h.int() });

    const codes = findingCodes(
      flow((w) => {
        const child = w.subWorkflow(leaf, {}, { id: "leaf" });

        // SAFETY: the types refuse a source of another output type; the check refuses it at run time too.
        return { output: w.task(takesText, { text: child } as never, { id: "take" }) };
      }),
      {},
      viewOf([...tasks, takesText], [leaf, region]),
    );

    assert.deepEqual(codes, ["HRS-024"]);
  });

  test("gate 1 stops before gate 2", () => {
    const codes = findingCodes(
      flow((w) => {
        w.task(ping, {}, { id: "same", priority: 0 });

        return { output: w.task(ping, {}, { id: "same" }) };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-004"]);
  });

  test("gate 2 reports every finding together", () => {
    const codes = findingCodes(
      flow((w) => {
        w.task(ping, {}, { id: "a", priority: 0 });

        return { output: w.task(ping, {}, { id: "b", queue: "urgent" }) };
      }),
      {},
      viewOf(tasks, [], DEFAULT_QUEUES),
    );

    assert.deepEqual(codes, ["HRS-034", "HRS-014"]);
  });
});
