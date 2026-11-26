// The rerun in the app's transaction with a fake bridge: each outcome and each failure of the addon
// gives its public value; a refused request calls nothing.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { AppRerunFailure, RerunOutcomeRecord } from "../../../horsies/src/native/facades/rerun.ts";
import type { CauseRecord } from "../../../horsies/src/native/values/records.ts";
import { rerunAreaOn } from "../../../horsies/src/rerun/app-area.ts";
import type { RerunHost } from "../../../horsies/src/rerun/app-area.ts";
import type { RerunError } from "../../../horsies/src/rerun/errors.ts";
import type { RerunOutcome } from "../../../horsies/src/rerun/types.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { CARRIER, COMMAND, ENQUEUED_ANSWERS, NEW_TASK_ID, POLICY, SOURCE_TASK_ID, fakeBridge, unusedCoreHandle } from "./support.ts";

const HANDLE = unusedCoreHandle();

/** A host with the test handle and an active span. */
const HOST: RerunHost = { coreHandle: () => HANDLE, activeCarrier: () => CARRIER };

/** The answer of `app.rerun` when the addon answers `rerunTask`. */
async function rerunAnswering(rerunTask: Parameters<typeof fakeBridge>[0]["rerunTask"]): Promise<Result<RerunOutcome, RerunError>> {
  const { bridge } = fakeBridge({ ...ENQUEUED_ANSWERS, rerunTask });

  return rerunAreaOn(HOST, bridge).rerun(COMMAND, POLICY);
}

/** The error of `app.rerun` when the addon fails with `error`. */
async function errorOf(error: AppRerunFailure): Promise<RerunError> {
  const answer = await rerunAnswering({ kind: "failed", error });

  assert.ok(!answer.ok, "the rerun failed");

  return answer.error;
}

const CONNECTION_CAUSE: CauseRecord = { kind: "connection", subject: "rerun_task", message: "connection refused", retryable: true };

describe("app.rerun", () => {
  test("passes the parsed request, the app's handle and the active carrier to the addon", async () => {
    const { bridge, calls } = fakeBridge(ENQUEUED_ANSWERS);

    const answer = await rerunAreaOn(HOST, bridge).rerun({ ...COMMAND, callerKey: "order-7" }, POLICY);

    assert.ok(answer.ok);
    assert.deepEqual(calls, [
      {
        call: "rerunTask",
        handle: HANDLE,
        request: {
          command: { sourceTaskId: SOURCE_TASK_ID, deadline: { kind: "noDeadline" }, callerKey: { kind: "keyed", key: "order-7" } },
          policy: { retention: { kind: "days", days: 7 }, rerunInput: "keep", reservationWindowMicros: 86_400_000_000n },
        },
        trace: CARRIER,
      },
    ]);
  });

  test("refuses a policy before any call of the addon", async () => {
    const { bridge, calls } = fakeBridge(ENQUEUED_ANSWERS);
    const answer = await rerunAreaOn(HOST, bridge).rerun(COMMAND, { ...POLICY, reservationWindow: Temporal.Duration.from({ days: 31 }) });

    assert.equal(!answer.ok && answer.error.code, "INVALID_POLICY");
    assert.deepEqual(calls, []);
  });

  test("gives each outcome of the addon as it is", async () => {
    const outcomes: RerunOutcomeRecord[] = [
      { kind: "enqueued", newTaskId: NEW_TASK_ID, sourceTaskId: SOURCE_TASK_ID, rerunRootTaskId: SOURCE_TASK_ID },
      { kind: "sourceLive", taskId: SOURCE_TASK_ID },
      { kind: "sourceAbsent", taskId: SOURCE_TASK_ID, reason: "WITHIN_RETAINED_HISTORY" },
      { kind: "sourceCorrupt", taskId: SOURCE_TASK_ID, detail: "digest" },
      { kind: "notEligible", taskId: SOURCE_TASK_ID, reason: "WORKFLOW_TASK" },
      { kind: "inputUnavailable", taskId: SOURCE_TASK_ID, disposition: "DECLINED_BY_POLICY" },
      { kind: "inputCorrupt", taskId: SOURCE_TASK_ID, detail: "envelope" },
      { kind: "keyConflict", taskId: SOURCE_TASK_ID, reservedByTaskId: NEW_TASK_ID },
      { kind: "keyReplay", existingTaskId: NEW_TASK_ID },
    ];

    for (const outcome of outcomes) {
      assert.deepEqual(await rerunAnswering({ kind: "done", outcome }), { ok: true, value: outcome }, outcome.kind);
    }
  });
});

describe("app.rerun errors", () => {
  test("CONNECT_FAILED keeps the retry rule and the cause", async () => {
    assert.deepEqual(await errorOf({ code: "CONNECT_FAILED", message: "the rerun could not connect: refused", cause: CONNECTION_CAUSE }), {
      kind: "connectFailed",
      code: "CONNECT_FAILED",
      retryable: true,
      message: "the rerun could not connect: refused",
      cause: { kind: "coreFailure", failure: "connection", subject: "rerun_task", message: "connection refused", retryable: true },
    });
  });

  test("STATEMENT_FAILED keeps its outcome and the server's SQLSTATE", async () => {
    const cause: CauseRecord = { kind: "server", subject: "rerun_task", sqlstate: "40001", message: "could not serialize access", retryable: true };

    assert.deepEqual(await errorOf({ code: "STATEMENT_FAILED", message: "database error: refused", cause, outcome: "unknown" }), {
      kind: "statementFailed",
      code: "STATEMENT_FAILED",
      retryable: true,
      message: "database error: refused",
      outcome: "unknown",
      cause: { kind: "coreFailure", failure: "server", subject: "rerun_task", message: "could not serialize access", retryable: true, sqlstate: "40001" },
    });
  });

  test("FAMILY_CHECK_FAILED with a precondition cause keeps its retry rule", async () => {
    const cause: CauseRecord = { kind: "preconditionFailed", subject: "rerun_task", message: "pools not opened", retryable: false };

    assert.deepEqual(await errorOf({ code: "FAMILY_CHECK_FAILED", message: "rerun refused: pools", cause }), {
      kind: "familyCheckFailed",
      code: "FAMILY_CHECK_FAILED",
      retryable: false,
      message: "rerun refused: pools",
      cause: { kind: "coreFailure", failure: "preconditionFailed", subject: "rerun_task", message: "pools not opened", retryable: false },
    });
  });

  test("a cause that is a contract violation is not retryable", async () => {
    const cause: CauseRecord = { kind: "contractViolation", subject: "rerun_task", message: "family row" };
    const error = await errorOf({ code: "FAMILY_CHECK_FAILED", message: "rerun refused: row", cause });

    assert.equal(error.retryable, false);
    assert.deepEqual("cause" in error && error.cause, { kind: "coreFailure", failure: "contractViolation", subject: "rerun_task", message: "family row" });
  });

  test("INVALID_POLICY of the core", async () => {
    assert.deepEqual(await errorOf({ code: "INVALID_POLICY", message: "invalid rerun policy: x" }), {
      kind: "invalidPolicy",
      code: "INVALID_POLICY",
      retryable: false,
      message: "invalid rerun policy: x",
    });
  });

  test("each refusal of the rerun steps is not retryable and keeps its fields", async () => {
    const refusals: [AppRerunFailure, RerunError][] = [
      [
        { code: "CLASS_NOT_REGISTERED", message: "unknown retention class", classKey: "standard_8d" },
        { kind: "classNotRegistered", code: "CLASS_NOT_REGISTERED", retryable: false, message: "unknown retention class", classKey: "standard_8d" },
      ],
      [
        { code: "CALLER_KEY_REFUSED", message: "rerun idempotency key failed" },
        { kind: "callerKeyRefused", code: "CALLER_KEY_REFUSED", retryable: false, message: "rerun idempotency key failed" },
      ],
      [
        { code: "ID_MINT_REFUSED", message: "rerun identity mint failed" },
        { kind: "idMintRefused", code: "ID_MINT_REFUSED", retryable: false, message: "rerun identity mint failed" },
      ],
      [
        { code: "CLIENT_FAMILY_MISMATCH", message: "rerun refused", stored: { kind: "recorded", family: "PYTHON" } },
        { kind: "clientFamilyMismatch", code: "CLIENT_FAMILY_MISMATCH", retryable: false, message: "rerun refused", stored: { kind: "recorded", family: "PYTHON" } },
      ],
      [
        { code: "UNKNOWN_STORED_FAMILY", message: "rerun refused: corrupt", stored: "GO" },
        { kind: "unknownStoredFamily", code: "UNKNOWN_STORED_FAMILY", retryable: false, message: "rerun refused: corrupt", stored: "GO" },
      ],
      [
        { code: "CONTRACT_VIOLATION", message: "statement returned no row" },
        { kind: "contractViolation", code: "CONTRACT_VIOLATION", retryable: false, message: "statement returned no row" },
      ],
    ];

    for (const [failure, expected] of refusals) {
      assert.deepEqual(await errorOf(failure), expected, failure.code);
    }
  });
});
