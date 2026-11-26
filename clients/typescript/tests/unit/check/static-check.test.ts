// The static check: a refusal of a registration area, the workflow check cases and the nesting check, a missing
// implementation, an implementation of a contract that is not in the list, a refused exception code, the first phase
// that refuses, the scopes of each area, and a passing app.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createHorsies, type Horsies } from "../../../horsies/src/app.ts";
import type { CheckOutcome, CheckPhaseName, CheckReport } from "../../../horsies/src/check/report.ts";
import { areaScopes, type CheckScope, staticCheck } from "../../../horsies/src/check/static-check.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import type { AnyWorkflowContract } from "../../../horsies/src/workflow/define.ts";
import { defineWorkflow, type NodeRef } from "../../../horsies/src/workflow/public.ts";
import { catalogRefusal, DATABASE_URL, fakeChecks, fakeEnvironment } from "../app/support.ts";
import { charge, contextOf, implementedApp, ping, testApp, unlisted } from "../worker/support.ts";

class RateLimited extends Error {}

function report(outcome: CheckOutcome): CheckReport {
  if (outcome.kind === "passed") {
    assert.fail("the check passed");
  }

  return outcome.report;
}

/** A workflow with params whose one check case throws in its build: `HRS-029` at check, nothing at create. */
const throwingCase = defineWorkflow(
  { name: "throwing", definitionKey: "tests.throwing.v1", output: h.int(), params: { n: h.int() }, checkCases: [{ n: 1 }] },
  () => {
    throw new RangeError("no build");
  },
);

/** A workflow that includes itself with the same params and no wait between: `HRS-037` at check, nothing at create. */
const selfNesting: object = defineWorkflow(
  { name: "loop", definitionKey: "tests.loop.v1", output: h.int(), params: {} },
  (w) => {
    // SAFETY: the workflow nests itself; its contract is complete when the build runs.
    const again = w.subWorkflow(selfNesting as never, {}, { nodeId: "again" });

    // SAFETY: the child's declared output is `h.int()`; the self reference leaves its type unknown here.
    return { output: again as NodeRef<number, string, "again"> };
  },
);

/** The app of `ping` and `charge` with the two workflows, built with the fakes; a failed create fails the test. */
function workflowApp(): Horsies {
  const created = createHorsies(
    {
      broker: { databaseUrl: DATABASE_URL },
      tasks: [ping, charge],
      // SAFETY: `selfNesting` is the workflow contract of `defineWorkflow`, typed `object` for its self reference.
      workflows: [throwingCase, selfNesting as AnyWorkflowContract],
    },
    fakeEnvironment(),
  );

  if (!created.ok) {
    assert.fail(`the workflow app was refused: ${created.error.refusals.map((refused) => refused.code).join(", ")}`);
  }

  return created.value;
}

/** The scope and the report name of each check: `check`, the worker start, the scheduler start. */
const EACH_CHECK: readonly (readonly [CheckScope, CheckPhaseName])[] = [
  ["withSchedules", "check"],
  ["withoutSchedules", "worker-startup"],
  ["withSchedules", "scheduler-startup"],
];

describe("the static check", () => {
  test("a passing app: the listed tasks and the declared codes by task name", () => {
    assert.deepEqual(staticCheck(contextOf(implementedApp()), fakeChecks(), "withSchedules", "check"), {
      kind: "passed",
      passed: { taskCount: 2, declaredErrorCodes: new Map([["charge", ["CARD_DECLINED", "BANK_DOWN"]]]) },
    });
  });

  test("an implementation of a contract that is not in the list is refused (a registration refusal)", () => {
    const app = implementedApp();

    app.implement(unlisted, (_input, { ok }) => ok(null));

    const refused = report(staticCheck(contextOf(app), fakeChecks(), "withoutSchedules", "worker-startup"));

    assert.equal(refused.phase, "worker-startup");
    assert.deepEqual(
      refused.refusals.map((refusal) => [refusal.code, refusal.path]),
      [["HRS-300", "implement unlisted"]],
    );
  });

  test("a listed contract with no implementation is refused with its place in the list", () => {
    const app = testApp();

    app.implement(ping, ({ n }, { ok }) => ok(n));

    const refused = report(staticCheck(contextOf(app), fakeChecks(), "withoutSchedules", "worker-startup"));

    assert.deepEqual(
      refused.refusals.map((refusal) => [refusal.code, refusal.path]),
      [["HRS-300", "tasks[1] charge"]],
    );
  });

  test("an exception code that the core's code rules refuse is refused", () => {
    const app = testApp();

    app.implement(ping, ({ n }, { ok }) => ok(n), { exceptionMapper: [[RateLimited, "rate limited"]] });
    app.implement(charge, async (_input, { ok }) => ok(null));

    const checks = fakeChecks({ codes: [catalogRefusal("HRS-112", "'rate limited' is invalid")] });
    const refused = report(staticCheck(contextOf(app), checks, "withSchedules", "check"));

    assert.deepEqual(
      refused.refusals.map((refusal) => [refusal.code, refusal.path]),
      [["HRS-112", "implement ping"]],
    );
  });

  test("the check stops at the first phase that refuses", () => {
    const app = testApp();

    app.implement(unlisted, (_input, { ok }) => ok(null));

    const refused = report(staticCheck(contextOf(app), fakeChecks(), "withSchedules", "check"));

    assert.deepEqual(
      refused.refusals.map((refusal) => refusal.path),
      ["implement unlisted"],
    );
  });

  test("each check runs the check cases and the nesting check of the listed workflows, in list order", () => {
    for (const [scope, phase] of EACH_CHECK) {
      const app = workflowApp();

      app.implement(ping, ({ n }, { ok }) => ok(n));
      app.implement(charge, async (_input, { ok }) => ok(null));

      const refused = report(staticCheck(contextOf(app), fakeChecks(), scope, phase));

      assert.equal(refused.phase, phase);
      assert.deepEqual(
        refused.refusals.map((refusal) => [refusal.code, refusal.path]),
        [
          ["HRS-029", "workflow throwing checkCases[0]"],
          ["HRS-037", "workflow loop > again > again"],
        ],
      );
    }
  });

  test("the workflows phase comes after the registrations and before the implementations", () => {
    const registered = workflowApp();

    registered.implement(unlisted, (_input, { ok }) => ok(null));

    const first = report(staticCheck(contextOf(registered), fakeChecks(), "withSchedules", "check"));
    const second = report(staticCheck(contextOf(workflowApp()), fakeChecks(), "withoutSchedules", "worker-startup"));

    // The registration refusal stops the check before the workflows; the workflow refusals stop it before the
    // implementation rule, which would refuse `tasks[0] ping` and `tasks[1] charge`.
    assert.deepEqual(
      first.refusals.map((refusal) => refusal.path),
      ["implement unlisted"],
    );
    assert.deepEqual(
      second.refusals.map((refusal) => refusal.path),
      ["workflow throwing checkCases[0]", "workflow loop > again > again"],
    );
  });

  test("the refusals of the contracts area count in both scopes", () => {
    assert.deepEqual(areaScopes("contracts"), ["withSchedules", "withoutSchedules"]);
  });
});
