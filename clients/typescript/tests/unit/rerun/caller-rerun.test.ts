// The rerun in the caller's transaction with a fake bridge and a fake transaction: the statements run
// in order on the caller's transaction, nothing commits, and each answer gives its public value.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CallerStepRefusal } from "../../../horsies/src/native/facades/rerun.ts";
import { NO_ACTIVE_CARRIER, rerunAreaOn } from "../../../horsies/src/rerun/app-area.ts";
import type { RerunHost } from "../../../horsies/src/rerun/app-area.ts";
import type { CallerConnectionState } from "../../../horsies/src/rerun/caller.ts";
import type { CallerRerunError } from "../../../horsies/src/rerun/errors.ts";
import type { RerunOutcome } from "../../../horsies/src/rerun/types.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import {
  CARRIER,
  COMMAND,
  ENQUEUED_ANSWERS,
  NEW_TASK_ID,
  POLICY,
  SOURCE_TASK_ID,
  fakeBridge,
  fakeTransaction,
  rowOf,
  unusedCoreHandle,
} from "./support.ts";
import type { BridgeAnswers, TransactionAnswer } from "./support.ts";

/** A host with an active span. */
const HOST: RerunHost = { coreHandle: unusedCoreHandle, activeCarrier: () => CARRIER };

/** The answer of `app.rerunIn` and the names of the statements that ran, with these answers. */
async function rerunIn(
  answers: BridgeAnswers,
  transactionAnswers: readonly TransactionAnswer[],
): Promise<{ readonly answer: Result<RerunOutcome, CallerRerunError>; readonly ran: string[] }> {
  const { bridge } = fakeBridge(answers);
  const { transaction, statements } = fakeTransaction(transactionAnswers);
  const answer = await rerunAreaOn(HOST, bridge).rerunIn(transaction, COMMAND, POLICY);

  return { answer, ran: statements.map((statement) => statement.name) };
}

const ENQUEUED: RerunOutcome = {
  kind: "enqueued",
  newTaskId: NEW_TASK_ID,
  sourceTaskId: SOURCE_TASK_ID,
  rerunRootTaskId: SOURCE_TASK_ID,
};

describe("app.rerunIn", () => {
  test("runs the read, then the write, on the caller's transaction and commits nothing", async () => {
    const { bridge, calls } = fakeBridge(ENQUEUED_ANSWERS);
    const { transaction, statements } = fakeTransaction([rowOf("t", null), rowOf("INSERTED", NEW_TASK_ID)]);
    const answer = await rerunAreaOn(HOST, bridge).rerunIn(transaction, COMMAND, POLICY);

    assert.deepEqual(answer, { ok: true, value: ENQUEUED });
    assert.deepEqual(statements, [
      {
        name: "rerun_source_read",
        sql: "SELECT class_registered FROM horsies_rerun_source_read($1::uuid, $2::text)",
        parameters: [SOURCE_TASK_ID, "standard_7d"],
        columns: [{ name: "class_registered", type: "bool" }],
      },
      {
        name: "rerun_enqueue",
        sql: "SELECT outcome, task_id FROM horsies_enqueue_task($1::uuid, $2::bytea)",
        parameters: [NEW_TASK_ID, null],
        columns: [
          { name: "outcome", type: "text" },
          { name: "task_id", type: "uuid" },
        ],
      },
    ]);
    assert.deepEqual(calls.slice(1), [
      {
        call: "prepareWrite",
        trace: CARRIER,
        readRow: { cells: [{ kind: "text", text: "t" }, { kind: "null" }] },
      },
      { call: "decodeRow", row: { cells: [{ kind: "text", text: "INSERTED" }, { kind: "text", text: NEW_TASK_ID }] } },
    ]);
  });

  test("passes no carrier when the host has no active span", async () => {
    const { bridge, calls } = fakeBridge(ENQUEUED_ANSWERS);
    const { transaction } = fakeTransaction([rowOf("t"), rowOf("INSERTED", NEW_TASK_ID)]);

    const answer = await rerunAreaOn(
      { coreHandle: unusedCoreHandle, activeCarrier: () => NO_ACTIVE_CARRIER },
      bridge,
    ).rerunIn(transaction, COMMAND, POLICY);

    assert.ok(answer.ok);
    assert.deepEqual(calls[1]?.call === "prepareWrite" && calls[1].trace, { kind: "noCarrier" });
  });

  test("ends with the outcome of the read and runs no write", async () => {
    const outcome: RerunOutcome = { kind: "sourceAbsent", taskId: SOURCE_TASK_ID, reason: "NOT_CLASSIFIED" };
    const { answer, ran } = await rerunIn({ ...ENQUEUED_ANSWERS, preparation: { kind: "answered", outcome } }, [rowOf("t")]);

    assert.deepEqual(answer, { ok: true, value: outcome });
    assert.deepEqual(ran, ["rerun_source_read"]);
  });

  test("gives the outcome of the write: keyReplay", async () => {
    const outcome: RerunOutcome = { kind: "keyReplay", existingTaskId: NEW_TASK_ID };

    const { answer } = await rerunIn({ ...ENQUEUED_ANSWERS, decoded: { kind: "decoded", outcome } }, [
      rowOf("t"),
      rowOf("KEY_REPLAY", NEW_TASK_ID),
    ]);

    assert.deepEqual(answer, { ok: true, value: outcome });
  });

  test("refuses the request before any statement", async () => {
    const { bridge, calls } = fakeBridge(ENQUEUED_ANSWERS);
    const { transaction, statements } = fakeTransaction([]);
    const lateDeadline = { kind: "until", at: Temporal.Instant.from("+010000-01-01T00:00:00Z") } as const;
    const answer = await rerunAreaOn(HOST, bridge).rerunIn(transaction, { ...COMMAND, deadline: lateDeadline }, POLICY);

    assert.equal(!answer.ok && answer.error.code, "INVALID_COMMAND");
    assert.deepEqual(calls, []);
    assert.deepEqual(statements, []);
  });
});

describe("app.rerunIn errors", () => {
  test("each refusal of the steps writes nothing and keeps its fields", async () => {
    const refusals: CallerStepRefusal[] = [
      { code: "CLASS_NOT_REGISTERED", message: "unknown retention class", classKey: "standard_8d" },
      { code: "CALLER_KEY_REFUSED", message: "rerun idempotency key failed" },
      { code: "ID_MINT_REFUSED", message: "rerun identity mint failed" },
      { code: "CLIENT_FAMILY_MISMATCH", message: "rerun refused", stored: { kind: "notRecorded" } },
      { code: "UNKNOWN_STORED_FAMILY", message: "rerun refused: corrupt", stored: "GO" },
      { code: "CONTRACT_VIOLATION", message: "statement returned NULL" },
    ];

    for (const refusal of refusals) {
      const { answer, ran } = await rerunIn({ ...ENQUEUED_ANSWERS, preparation: { kind: "refused", error: refusal } }, [rowOf("t")]);

      assert.deepEqual(answer, { ok: false, error: { ...refusal, retryable: false } }, refusal.code);
      assert.deepEqual(ran, ["rerun_source_read"], refusal.code);
    }
  });

  test("DRIVER_FAILED carries the driver's error and the statement", async () => {
    const driverError = Object.assign(new Error("could not serialize access"), { code: "40001" });

    const { answer } = await rerunIn(ENQUEUED_ANSWERS, [
      rowOf("t"),
      { kind: "result", result: { ok: false, error: { kind: "driverFailed", driverError } } },
    ]);

    assert.deepEqual(answer, {
      ok: false,
      error: {
        code: "DRIVER_FAILED",
        message: "statement rerun_enqueue failed in the caller's driver: could not serialize access",
        statement: "rerun_enqueue",
        driverError,
      },
    });
  });

  test("a transaction that throws is DRIVER_FAILED, not a throw of the rerun", async () => {
    const thrown = new Error("Client was closed and is not queryable");
    const { answer, ran } = await rerunIn(ENQUEUED_ANSWERS, [{ kind: "throw", thrown }]);

    assert.equal(!answer.ok && answer.error.code === "DRIVER_FAILED" && answer.error.driverError, thrown);
    assert.deepEqual(ran, ["rerun_source_read"]);
  });

  test("NO_OPEN_TRANSACTION names the statement that did not run and the state, and runs no write", async () => {
    const { answer, ran } = await rerunIn(ENQUEUED_ANSWERS, [
      { kind: "result", result: { ok: false, error: { kind: "noOpenTransaction", state: { kind: "idle" } } } },
    ]);

    assert.deepEqual(answer, {
      ok: false,
      error: {
        code: "NO_OPEN_TRANSACTION",
        retryable: false,
        message: "statement rerun_source_read did not run: the caller's connection has no open transaction; run BEGIN first",
        statement: "rerun_source_read",
        state: { kind: "idle" },
      },
    });
    assert.deepEqual(ran, ["rerun_source_read"]);
  });

  test("NO_OPEN_TRANSACTION tells the caller what to do in each state", async () => {
    const causes: [CallerConnectionState, string][] = [
      [{ kind: "failedTransaction" }, "a statement of the caller's transaction failed; roll it back"],
      [{ kind: "notConnected" }, "the caller's client is not connected"],
      [{ kind: "unrecognized", status: "X" }, `the caller's driver gives the transaction status "X", not an open transaction`],
    ];

    for (const [state, cause] of causes) {
      const { answer } = await rerunIn(ENQUEUED_ANSWERS, [
        rowOf("t"),
        { kind: "result", result: { ok: false, error: { kind: "noOpenTransaction", state } } },
      ]);

      assert.ok(!answer.ok && answer.error.code === "NO_OPEN_TRANSACTION", state.kind);
      assert.equal(answer.error.message, `statement rerun_enqueue did not run: ${cause}`);
      assert.deepEqual(answer.error.state, state);
    }
  });

  test("RESULT_NOT_TEXT names the statement, the column and its format", async () => {
    const { answer, ran } = await rerunIn(ENQUEUED_ANSWERS, [
      { kind: "result", result: { ok: false, error: { kind: "resultNotText", column: "class_registered", format: "binary" } } },
    ]);

    assert.deepEqual(answer, {
      ok: false,
      error: {
        code: "RESULT_NOT_TEXT",
        retryable: false,
        message:
          "statement rerun_source_read gave the column class_registered in binary format; the rerun reads text format " +
          "(a pg client with binary: true gives binary format)",
        statement: "rerun_source_read",
        column: "class_registered",
        format: "binary",
      },
    });
    assert.deepEqual(ran, ["rerun_source_read"]);
  });

  test("a row that the transaction refuses is CONTRACT_VIOLATION", async () => {
    const { answer } = await rerunIn(ENQUEUED_ANSWERS, [
      { kind: "result", result: { ok: false, error: { kind: "rowRefused", detail: "statement rerun_source_read gave 0 rows; it gives one row" } } },
    ]);

    assert.deepEqual(answer, {
      ok: false,
      error: { code: "CONTRACT_VIOLATION", retryable: false, message: "statement rerun_source_read gave 0 rows; it gives one row" },
    });
  });

  test("a write row that does not fit is CONTRACT_VIOLATION", async () => {
    const { answer } = await rerunIn({ ...ENQUEUED_ANSWERS, decoded: { kind: "rowRefused", message: "statement `rerun_enqueue` declares 2 result columns; the row has 1 cells" } }, [
      rowOf("t"),
      rowOf("INSERTED"),
    ]);

    assert.deepEqual(answer, {
      ok: false,
      error: { code: "CONTRACT_VIOLATION", retryable: false, message: "statement `rerun_enqueue` declares 2 result columns; the row has 1 cells" },
    });
  });
});
