// The builder and the spec checks: the dependency order, node parameters, each node option with a valid and a refused
// value, each check code, and the two gates.

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { runInNewContext } from "node:vm";

import { defineTask, type OutputSchema } from "../../../horsies/src/contract/public.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { h, type RequiredWire } from "../../../horsies/src/wire/public.ts";
import { MAX_WORKFLOW_NODES, runBuild } from "../../../horsies/src/workflow/builder.ts";
import type { AnyNode, BuildReturn } from "../../../horsies/src/workflow/node.ts";
import { defineWorkflow, type WorkflowBuilder } from "../../../horsies/src/workflow/public.ts";
import { checked, DEFAULT_QUEUES, findingCodes, passedSpec, viewOf } from "./support.ts";

const ping = defineTask({ name: "ping", input: {}, output: h.int() });
const add = defineTask({ name: "add", input: { a: h.int(), b: h.taskOutcome(ping) }, output: h.int() });
const pair = defineTask({ name: "pair", input: { x: h.taskOutcome(ping), y: h.taskOutcome(ping) }, output: h.int() });
const say = defineTask({ name: "say", input: { text: h.string() }, output: h.string() });
const audit = defineTask({ name: "audit", input: {}, output: h.null(), workflowContext: true });
const maybe = defineTask({ name: "maybe", input: { first: h.taskOutcome(ping).optional() }, output: h.int() });
const tasks = [ping, add, pair, say, audit, maybe];

const leaf = defineWorkflow({ name: "leaf", definitionKey: "tests.leaf.v1", outputType: h.int(), parameters: {} }, (w) => ({
  output: w.task(ping, {}, { nodeId: "ping" }),
}));

const region = defineWorkflow(
  {
    name: "region",
    definitionKey: "tests.region.v1",
    outputType: h.int(),
    parameters: { name: h.string().optional() },
    checkCases: [{}],
  },
  (w) => ({ output: w.task(ping, {}, { nodeId: "ping" }) }),
);

const view = viewOf(tasks, [leaf, region]);

/** A workflow whose build is `build`, with no params. */
function flow(build: (w: WorkflowBuilder) => BuildReturn, name = "flow", output: OutputSchema = h.int()): object {
  // SAFETY: the builds of these tests return nodes of several types; the checks compare the output node with the
  // declared output at run time (HRS-025), and each test declares the output of its output node.
  return defineWorkflow({ name, definitionKey: `tests.${name}.v1`, outputType: output, parameters: {} }, build as never);
}

describe("the builder", () => {
  test("a build of a record whose error rule create refuses is a defect", () => {
    // SAFETY: the types refuse this error rule; Horsies.create refuses it before any build (HRS-033).
    const refused = defineWorkflow(
      { name: "rule", definitionKey: "tests.rule.v1", outputType: h.int(), parameters: {}, onError: "stop" as never },
      (w) => ({ output: w.task(ping, {}, { nodeId: "ping" }) }),
    );

    assert.throws(() => checked(refused, {}, view), ClientDefect);
  });

  test("a build that returns a thenable of another realm is a thenable", () => {
    const thenable: unknown = runInNewContext("({ then(resolve) { resolve(1); } })");
    const run = runBuild(() => thenable, {});

    assert.equal(run.kind, "thenable");
  });

  test("the dependencies: the parameter sources in declaration order, then waitsFor, each node once", () => {
    const spec = passedSpec(
      flow((w) => {
        const first = w.task(ping, {}, { nodeId: "first" });
        const second = w.task(ping, {}, { nodeId: "second" });
        const third = w.task(ping, {}, { nodeId: "third" });

        return { output: w.task(pair, { y: second, x: first }, { nodeId: "pair", waitsFor: [third, first] }) };
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
        const source = w.task(ping, {}, { nodeId: "source" });

        return { output: w.task(add, { a: 1, b: source }, { nodeId: "add" }) };
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
        w.subWorkflow(leaf, {}, { nodeId: "leaf" });

        return { output: w.subWorkflow(region, {}, { nodeId: "region" }) };
      }),
      {},
      view,
    );

    assert.deepEqual(spec.nodes[0]?.body, { kind: "subWorkflow", definitionKey: "tests.leaf.v1", params: "NOT_GIVEN" });
    assert.deepEqual(spec.nodes[1]?.body, { kind: "subWorkflow", definitionKey: "tests.region.v1", params: "GIVEN" });
    assert.equal(spec.nodes[1]?.staticInput, "{}");
  });

  test("valid node options: join, allowFailedDependencies, workflowContextFrom, queue, priority and goodUntil", () => {
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
        ["urgent", { name: "urgent", maxConcurrency: { kind: "coreDefault" } }],
        ["slow", { name: "slow", maxConcurrency: { kind: "coreDefault" } }],
      ]),
    });

    const at = Temporal.Instant.from("2026-08-12T09:10:12.654321789Z");

    const spec = passedSpec(
      flow(
        (w) => {
          const one = w.task(urgentPing, {}, { nodeId: "one" });
          const two = w.task(urgentPing, {}, { nodeId: "two", queue: "slow", priority: 7, goodUntil: at });

          return {
            output: w.task(
              urgentAudit,
              {},
              {
                nodeId: "audit",
                waitsFor: [one, two],
                workflowContextFrom: [two],
                allowFailedDependencies: true,
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
      priority: { kind: "queue" },
      deadline: { kind: "noDeadline" },
    });
    assert.deepEqual(spec.nodes[1]?.body, {
      kind: "task",
      queue: "slow",
      priority: { kind: "given", priority: 7 },
      deadline: { kind: "until", epochMicros: 1_786_525_812_654_321n },
    });
    assert.deepEqual(spec.nodes[2]?.join, { kind: "quorum", minSuccess: 2 });
    assert.deepEqual(spec.nodes[2]?.workflowContextFrom, ["two"]);
    assert.equal(spec.nodes[2]?.allowFailedDependencies, true);
  });
});

/** The codes of a flow whose one node is `ping` with the options `options`, after a source node. */
function optionCodes(options: object): readonly string[] {
  return findingCodes(
    flow(
      (w) => {
        const source = w.task(ping, {}, { nodeId: "source" });

        // SAFETY: the test gives option values that the types refuse or that only the checks see.
        return { output: w.task(audit, {}, { nodeId: "node", waitsFor: [source], ...options } as never) };
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
    ["an id with a space", { nodeId: "bad id" }, ["HRS-003"]],
    ["an empty id", { nodeId: "" }, ["HRS-003"]],
    ["no id", { nodeId: undefined }, ["HRS-003"]],
    ["an id that is a number", { nodeId: 7 }, ["HRS-003"]],
    ["an id of 129 characters", { nodeId: "n".repeat(129) }, ["HRS-003"]],
    ["the id of another node", { nodeId: "source" }, ["HRS-004"]],
    ["a quorum of 0", { join: { kind: "quorum", minSuccess: 0 } }, ["HRS-013"]],
    ["a quorum above the dependency count", { join: { kind: "quorum", minSuccess: 2 } }, ["HRS-013"]],
    ["a join of another kind", { join: { kind: "most" } }, ["HRS-013"]],
    ["a priority of 0", { priority: 0 }, ["HRS-034"]],
    ["a priority of 101", { priority: 101 }, ["HRS-034"]],
    ["a priority that is no integer", { priority: 1.5 }, ["HRS-034"]],
    ["a queue that the app does not admit", { queue: "urgent" }, ["HRS-014"]],
    ["a deadline in the year 10000", { goodUntil: Temporal.Instant.from("+010000-01-01T00:00:00Z") }, ["HRS-033"]],
  ];

  for (const [name, options, codes] of cases) {
    test(`${name}: ${codes.join(", ")}`, () => {
      assert.deepEqual(optionCodes(options), codes);
    });
  }

  test("an id that is not a string: HRS-003 at the node's index, with no id in the place, and no throw", () => {
    for (const id of [undefined, 7]) {
      const outcome = checked(
        flow((w) => {
          w.task(ping, {}, { nodeId: "source" });

          // SAFETY: untyped code can give an id that is not a string; the check refuses it.
          return { output: w.task(ping, {}, { nodeId: id } as never) };
        }),
        {},
        view,
      );

      assert(!outcome.ok);
      assert.deepEqual(
        outcome.error.map((found) => [found.code, found.place, found.detail]),
        [["HRS-003", "nodes[1]", "invalid node id that is not a string"]],
      );
    }
  });

  test("a context source that is not a dependency: HRS-009", () => {
    const codes = findingCodes(
      flow(
        (w) => {
          const source = w.task(ping, {}, { nodeId: "source" });

          return { output: w.task(audit, {}, { nodeId: "audit", workflowContextFrom: [source] }) };
        },
        "flow",
        h.null(),
      ),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-009"]);
  });

  test("workflowContextFrom on a task that takes no workflow context: HRS-010", () => {
    const codes = findingCodes(
      flow((w) => {
        const source = w.task(ping, {}, { nodeId: "source" });

        // SAFETY: the types refuse workflowContextFrom here; the check refuses it at run time too.
        return { output: w.task(ping, {}, { nodeId: "ping", waitsFor: [source], workflowContextFrom: [source] } as never) };
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
        foreign = w.task(ping, {}, { nodeId: "foreign" });

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
        flow((w) => ({ output: w.task(ping, {}, { nodeId: "a" }) }), " "),
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
        let last: AnyNode = w.task(ping, {}, { nodeId: "n0" });

        for (let index = 1; index <= 10_000; index += 1) {
          last = w.task(ping, {}, { nodeId: `n${index}` });
        }

        return { output: last };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-038"]);
  });

  test("a build past the node bound: HRS-038 with the made count, and the session keeps one node past the bound", () => {
    const made = MAX_WORKFLOW_NODES + 50;

    const run = runBuild((w: WorkflowBuilder) => {
      let last: AnyNode = w.task(ping, {}, { nodeId: "n0" });

      for (let index = 1; index < made; index += 1) {
        last = w.task(ping, {}, { nodeId: `n${index}` });
      }

      return { output: last };
    }, {});

    assert(run.kind === "returned");
    assert.equal(run.session.nodes().length, MAX_WORKFLOW_NODES + 1);
    assert.equal(run.session.madeCount(), made);

    const outcome = checked(
      flow((w) => {
        let last: AnyNode = w.task(ping, {}, { nodeId: "n0" });

        for (let index = 1; index < made; index += 1) {
          last = w.task(ping, {}, { nodeId: `n${index}` });
        }

        return { output: last };
      }),
      {},
      view,
    );

    assert(!outcome.ok);
    assert.deepEqual(
      outcome.error.map((found) => [found.code, found.detail]),
      [["HRS-038", `the build made ${made} nodes, more than the limit of ${MAX_WORKFLOW_NODES}`]],
    );
  });

  test("an output that is no node of the build: HRS-011", () => {
    // SAFETY: the types refuse an output that is no node; the check refuses it at run time too.
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(ping, {}, { nodeId: "a" }) && ({ kind: "node", nodeId: "a" } as AnyNode) })),
        {},
        view,
      ),
      ["HRS-011"],
    );
  });

  test("a success policy with no case, or a case with no required node: HRS-012", () => {
    const codes = findingCodes(
      flow((w) => {
        const node = w.task(ping, {}, { nodeId: "a" });

        return { output: node, successPolicy: { cases: [{ required: [node] }, { required: [] }] } };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-012"]);
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(ping, {}, { nodeId: "a" }), successPolicy: { cases: [] } })),
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
        flow((w) => ({ output: w.task(other, {}, { nodeId: "a" }) })),
        {},
        view,
      ),
      ["HRS-302"],
    );
  });

  test("a child workflow that the app does not list: HRS-020", () => {
    const unlisted = defineWorkflow(
      { name: "unlisted", definitionKey: "tests.unlisted.v1", outputType: h.int(), parameters: {} },
      (w) => ({
        output: w.task(ping, {}, { nodeId: "a" }),
      }),
    );

    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.subWorkflow(unlisted, {}, { nodeId: "a" }) })),
        {},
        view,
      ),
      ["HRS-020"],
    );
  });

  test("params for a child that takes none: HRS-022", () => {
    // SAFETY: the types refuse a member for a child with params: {}; the check refuses it at run time too.
    const codes = findingCodes(
      flow((w) => ({ output: w.subWorkflow(leaf, { name: "eu" } as never, { nodeId: "a" }) })),
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
        flow((w) => ({ output: w.task(say, unknown, { nodeId: "a" }) }), "flow", h.string()),
        {},
        view,
      ),
      ["HRS-019"],
    );
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(say, missing, { nodeId: "a" }) }), "flow", h.string()),
        {},
        view,
      ),
      ["HRS-020"],
    );
    assert.deepEqual(
      findingCodes(
        flow((w) => ({ output: w.task(say, wrong, { nodeId: "a" }) }), "flow", h.string()),
        {},
        view,
      ),
      ["HRS-031"],
    );
  });

  test("a node for a plain parameter, a value for h.taskOutcome, a source of another output type: HRS-024", () => {
    const codes = findingCodes(
      flow((w) => {
        const number = w.task(ping, {}, { nodeId: "number" });
        const text = w.task(say, { text: "a" }, { nodeId: "text" });

        // SAFETY: each input breaks a rule that the types also refuse; the check refuses it at run time too.
        w.task(say, { text: number } as never, { nodeId: "plain" });
        // SAFETY: as above.
        w.task(add, { a: 1, b: 2 } as never, { nodeId: "value" });

        // SAFETY: as above.
        return { output: w.task(add, { a: 1, b: text } as never, { nodeId: "other" }) };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-024", "HRS-024", "HRS-024"]);
  });

  test("a required node-fed parameter under an any join: HRS-013; an optional one passes", () => {
    const required = findingCodes(
      flow((w) => {
        const source = w.task(ping, {}, { nodeId: "source" });

        // SAFETY: the types refuse a required node-fed parameter under an any join; the check refuses it too.
        return { output: w.task(add, { a: 1, b: source } as never, { nodeId: "add", join: { kind: "any" } }) };
      }),
      {},
      view,
    );

    assert.deepEqual(required, ["HRS-013"]);
    passedSpec(
      flow((w) => ({
        output: w.task(maybe, { first: w.task(ping, {}, { nodeId: "s" }) }, { nodeId: "m", join: { kind: "any" } }),
      })),
      {},
      view,
    );
  });

  test("an output node of another type than the declared output: HRS-025", () => {
    const codes = findingCodes(
      flow((w) => ({ output: w.task(say, { text: "a" }, { nodeId: "say" }) }), "flow", h.int()),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-025"]);
  });

  test("a sub-workflow node gives its child's declared output: an h.taskOutcome parameter of another type is HRS-024", () => {
    const takesText = defineTask({ name: "takes_text", input: { text: h.taskOutcome(say) }, output: h.int() });

    const codes = findingCodes(
      flow((w) => {
        const child = w.subWorkflow(leaf, {}, { nodeId: "leaf" });

        // SAFETY: the types refuse a source of another output type; the check refuses it at run time too.
        return { output: w.task(takesText, { text: child } as never, { nodeId: "take" }) };
      }),
      {},
      viewOf([...tasks, takesText], [leaf, region]),
    );

    assert.deepEqual(codes, ["HRS-024"]);
  });

  test("gate 1 stops before gate 2", () => {
    const codes = findingCodes(
      flow((w) => {
        w.task(ping, {}, { nodeId: "same", priority: 0 });

        return { output: w.task(ping, {}, { nodeId: "same" }) };
      }),
      {},
      view,
    );

    assert.deepEqual(codes, ["HRS-004"]);
  });

  test("gate 2 reports every finding together", () => {
    const codes = findingCodes(
      flow((w) => {
        w.task(ping, {}, { nodeId: "a", priority: 0 });

        return { output: w.task(ping, {}, { nodeId: "b", queue: "urgent" }) };
      }),
      {},
      viewOf(tasks, [], DEFAULT_QUEUES),
    );

    assert.deepEqual(codes, ["HRS-034", "HRS-014"]);
  });
});

/**
 * The codes of a build whose task of output `source` feeds the `h.taskOutcome` parameter of the output node, declared
 * with a contract of output `target`.
 */
function fitCodes(source: OutputSchema, target: RequiredWire): readonly string[] {
  const from = defineTask({ name: "from", input: {}, output: source });
  const declared = defineTask({ name: "declared", input: {}, output: target });
  const into = defineTask({ name: "into", input: { value: h.taskOutcome(declared) }, output: h.int() });

  const workflow = flow((w) => {
    const value = w.task(from, {}, { nodeId: "from" });

    // SAFETY: the target's type is not inferred here; the check compares the stored forms at run time.
    return { output: w.task(into, { value } as never, { nodeId: "into" }) };
  });

  const outcome = checked(workflow, {}, viewOf([from, into], []));

  return outcome.ok ? [] : outcome.error.map((found) => found.code);
}

/** The codes of a build whose output node is a task of output `source`, in a workflow that declares `declared`. */
function outputCodes(source: OutputSchema, declared: OutputSchema): readonly string[] {
  const from = defineTask({ name: "from", input: {}, output: source });
  const workflow = flow((w) => ({ output: w.task(from, {}, { nodeId: "from" }) }), "flow", declared);
  const outcome = checked(workflow, {}, viewOf([from], []));

  return outcome.ok ? [] : outcome.error.map((found) => found.code);
}

describe("the stored-form fit of a source (HRS-024, HRS-025)", () => {
  test("a string source fits an h.taskOutcome of a nullable string", () => {
    assert.deepEqual(fitCodes(h.string(), h.string().nullable()), []);
  });

  test("a literal source fits its base scalar", () => {
    assert.deepEqual(fitCodes(h.literal("a", "b"), h.string()), []);
    assert.deepEqual(fitCodes(h.literal(1, 2), h.int()), []);
  });

  test("an int source fits a float parameter", () => {
    assert.deepEqual(fitCodes(h.int(), h.float()), []);
  });

  test("a union source fits a wider union", () => {
    assert.deepEqual(fitCodes(h.union([h.string(), h.int()]), h.union([h.boolean(), h.int(), h.string()])), []);
  });

  test("a string output node fits a declared nullable string", () => {
    assert.deepEqual(outputCodes(h.string(), h.string().nullable()), []);
  });

  test("a source of another scalar is HRS-024", () => {
    assert.deepEqual(fitCodes(h.string(), h.int()), ["HRS-024"]);
  });

  test("a float source for an int parameter is HRS-024: not every float text reads as an int", () => {
    assert.deepEqual(fitCodes(h.float(), h.int()), ["HRS-024"]);
  });

  test("a nullable source for a parameter that is not nullable is HRS-024", () => {
    assert.deepEqual(fitCodes(h.string().nullable(), h.string()), ["HRS-024"]);
  });

  test("a literal source outside the parameter's literals or base scalar is HRS-024", () => {
    assert.deepEqual(fitCodes(h.literal("a"), h.literal("b")), ["HRS-024"]);
    assert.deepEqual(fitCodes(h.literal("a"), h.int()), ["HRS-024"]);
  });

  test("a wider union source for a narrower union is HRS-024", () => {
    assert.deepEqual(fitCodes(h.union([h.string(), h.int()]), h.union([h.string()])), ["HRS-024"]);
  });

  test("an output node of another scalar than the declared output is HRS-025", () => {
    assert.deepEqual(outputCodes(h.float(), h.int()), ["HRS-025"]);
  });

  test("a nullable output node for a declared output that is not nullable is HRS-025", () => {
    assert.deepEqual(outputCodes(h.string().nullable(), h.string()), ["HRS-025"]);
  });
});
