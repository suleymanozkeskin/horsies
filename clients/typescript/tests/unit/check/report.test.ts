// The text of a check report and of a passing check, and the outcome of the live steps.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { refusal } from "../../../horsies/src/app/refusal.ts";
import { liveCheckOutcome } from "../../../horsies/src/check/live-check.ts";
import { type CheckPassed, checkReport, passedText, reportText } from "../../../horsies/src/check/report.ts";
import { catalogRefusal } from "../app/support.ts";

const PASSED: CheckPassed = { taskCount: 2, declaredErrorCodes: new Map() };

describe("the check report", () => {
  test("each refusal with its catalog code and help lines, then the count", () => {
    const report = checkReport(
      "check",
      refusal("HRS-300", "tasks[1] charge", "the contract has no implementation in this entry", ["call app.implement"]),
      [refusal("HRS-227", "payload.warnBytes.bytes", "below 1", [])],
    );

    assert.equal(
      reportText(report),
      [
        "error[HRS-300]: tasks[1] charge: the contract has no implementation in this entry",
        "  = help: call app.implement",
        "error[HRS-227]: payload.warnBytes.bytes: below 1",
        "error: aborting due to 2 previous errors",
      ].join("\n"),
    );
  });

  test("a passing check with no declared code", () => {
    assert.equal(passedText(PASSED), "ok: all validations passed\n  2 task(s) registered\n  declared error codes: none");
  });

  test("a passing check lists the declared codes of each task", () => {
    const passed: CheckPassed = { taskCount: 1, declaredErrorCodes: new Map([["charge", ["CARD_DECLINED", "BANK_DOWN"]]]) };

    assert.equal(
      passedText(passed),
      "ok: all validations passed\n  1 task(s) registered\n  declared error codes:\n    charge: CARD_DECLINED, BANK_DOWN",
    );
  });
});

describe("the live steps", () => {
  test("every step passed keeps the static summary", () => {
    assert.deepEqual(liveCheckOutcome(PASSED, { kind: "passed" }), { kind: "passed", passed: PASSED });
  });

  test("a refused connect or SELECT 1 is a connectivity refusal with the core's cause", () => {
    const outcome = liveCheckOutcome(PASSED, { kind: "connectivityRefused", step: "MAIN_URL", message: "connection refused" });

    assert.equal(outcome.kind, "refused");
    assert.deepEqual(
      outcome.kind === "refused" ? outcome.report.refusals.map((refused) => [refused.code, refused.message]) : [],
      [["HRS-203", "broker: broker connectivity check failed; connection refused"]],
    );
  });

  test("a LISTEN probe that got no NOTIFY is the listener refusal", () => {
    const outcome = liveCheckOutcome(PASSED, { kind: "connectivityRefused", step: "LISTENER", message: "timed out" });

    assert.ok(outcome.kind === "refused" && outcome.report.refusals[0].message.includes("listener capability check failed"));
  });

  test("each profile that does not fit the connection limit is one refusal with HRS-219", () => {
    const outcome = liveCheckOutcome(PASSED, {
      kind: "connectionsRefused",
      refusals: [catalogRefusal("HRS-219", "profile io needs 41 connections"), catalogRefusal("HRS-219", "profile cpu needs 90")],
    });

    assert.deepEqual(
      outcome.kind === "refused" ? outcome.report.refusals.map((refused) => refused.code) : [],
      ["HRS-219", "HRS-219"],
    );
  });

  test("a connections refusal with no refusal is a defect", () => {
    assert.throws(() => liveCheckOutcome(PASSED, { kind: "connectionsRefused", refusals: [] }), TypeError);
  });
});
