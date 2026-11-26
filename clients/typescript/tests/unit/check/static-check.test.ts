// The static check: a refusal of a registration area, a missing implementation, an implementation of a contract
// that is not in the list, a refused exception code, the first phase that refuses, the scopes of each area, and a
// passing app.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CheckOutcome, CheckReport } from "../../../horsies/src/check/report.ts";
import { areaScopes, staticCheck } from "../../../horsies/src/check/static-check.ts";
import { catalogRefusal, fakeChecks } from "../app/support.ts";
import { charge, contextOf, implementedApp, ping, testApp, unlisted } from "../worker/support.ts";

class RateLimited extends Error {}

function report(outcome: CheckOutcome): CheckReport {
  if (outcome.kind === "passed") {
    assert.fail("the check passed");
  }

  return outcome.report;
}

describe("the static check", () => {
  test("a passing app: the listed tasks and the declared codes by task name", () => {
    assert.deepEqual(staticCheck(contextOf(implementedApp()), fakeChecks(), "withSchedules", "check"), {
      kind: "passed",
      passed: { taskCount: 2, declaredErrorCodes: new Map([["charge", ["CARD_DECLINED", "BANK_DOWN"]]]) },
    });
  });

  test("an implementation of a contract that is not in the list is refused (a registration refusal)", () => {
    const app = implementedApp();

    app.implement(unlisted, () => unlisted.ok(null));

    const refused = report(staticCheck(contextOf(app), fakeChecks(), "withoutSchedules", "worker-startup"));

    assert.equal(refused.phase, "worker-startup");
    assert.deepEqual(
      refused.refusals.map((refusal) => [refusal.code, refusal.path]),
      [["HRS-300", "implement unlisted"]],
    );
  });

  test("a listed contract with no implementation is refused with its place in the list", () => {
    const app = testApp();

    app.implement(ping, ({ n }) => ping.ok(n));

    const refused = report(staticCheck(contextOf(app), fakeChecks(), "withoutSchedules", "worker-startup"));

    assert.deepEqual(
      refused.refusals.map((refusal) => [refusal.code, refusal.path]),
      [["HRS-300", "tasks[1] charge"]],
    );
  });

  test("an exception code that the core's code rules refuse is refused", () => {
    const app = testApp();

    app.implement(ping, ({ n }) => ping.ok(n), { exceptions: [[RateLimited, "rate limited"]] });
    app.implement(charge, async () => charge.ok(null));

    const checks = fakeChecks({ codes: [catalogRefusal("HRS-112", "'rate limited' is invalid")] });
    const refused = report(staticCheck(contextOf(app), checks, "withSchedules", "check"));

    assert.deepEqual(
      refused.refusals.map((refusal) => [refusal.code, refusal.path]),
      [["HRS-112", "implement ping"]],
    );
  });

  test("the check stops at the first phase that refuses", () => {
    const app = testApp();

    app.implement(unlisted, () => unlisted.ok(null));

    const refused = report(staticCheck(contextOf(app), fakeChecks(), "withSchedules", "check"));

    assert.deepEqual(
      refused.refusals.map((refusal) => refusal.path),
      ["implement unlisted"],
    );
  });

  test("the refusals of the contracts area count in both scopes", () => {
    assert.deepEqual(areaScopes("contracts"), ["withSchedules", "withoutSchedules"]);
  });
});
