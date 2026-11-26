// The info read of a task handle: the info of a found task, field by field, and the read errors of `info()`.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { defineTask } from "../../../horsies/src/contract/public.ts";
import type { TaskAttemptRecord, TaskInfoRecord } from "../../../horsies/src/native/sends/addon.ts";
import { readFormOf, taskHandleOf } from "../../../horsies/src/read/handle.ts";
import { instantOfMicros, taskInfoOf } from "../../../horsies/src/read/info.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import { addonAccess, countingRelay, fakeProducer, operationFailedRaw, TASK_ID } from "./support.ts";

/** 2023-11-14T22:13:20.123456Z in epoch microseconds. */
const AT = 1_700_000_000_123_456n;

const contract = defineTask({ name: "validate_order", input: {}, output: h.null() });

function attempt(number: number, recorded: boolean): TaskAttemptRecord {
  const text = (value: string): TaskAttemptRecord["errorCode"] =>
    recorded ? { kind: "recorded", text: value } : { kind: "notRecorded" };

  return {
    attempt: number,
    outcome: recorded ? "FAILED" : "COMPLETED",
    willRetry: recorded,
    startedAtMicros: AT + BigInt(number),
    finishedAtMicros: AT + BigInt(number) + 1n,
    errorCode: text("CARD_DECLINED"),
    errorMessage: text("declined"),
    failedReason: text("the card was declined"),
    workerId: text("worker-1"),
    workerHostname: text("host-a"),
    workerPid: recorded ? { kind: "recorded", value: 4242 } : { kind: "notRecorded" },
    workerProcessName: text("child-1"),
  };
}

/** A record with every optional fact recorded (`recorded`), or none. */
function record(recorded: boolean, execution: TaskInfoRecord["execution"]): TaskInfoRecord {
  const instant = (offset: bigint): TaskInfoRecord["sentAt"] =>
    recorded ? { kind: "recorded", epochMicros: AT + offset } : { kind: "notRecorded" };

  const text = (value: string): TaskInfoRecord["errorCode"] =>
    recorded ? { kind: "recorded", text: value } : { kind: "notRecorded" };

  return {
    taskId: TASK_ID,
    taskName: "validate_order",
    status: recorded ? "FAILED" : "PENDING",
    queueName: "default",
    priority: 100,
    retryCount: recorded ? 2 : 0,
    maxRetries: 3,
    nextRetryAt: instant(10n),
    sentAt: instant(1n),
    enqueuedAtMicros: AT + 2n,
    claimedAt: instant(3n),
    startedAt: instant(4n),
    completedAt: instant(5n),
    failedAt: instant(6n),
    workerHostname: text("host-a"),
    workerPid: recorded ? { kind: "recorded", value: 4242 } : { kind: "notRecorded" },
    workerProcessName: text("child-1"),
    errorCode: text("CARD_DECLINED"),
    failedReason: text("the card was declined"),
    attempts: recorded ? [attempt(2, true), attempt(1, true)] : [],
    execution,
  };
}

describe("taskInfoOf", () => {
  test("every recorded fact: instants of epoch microseconds, texts, counts, attempts in the core's order", () => {
    const info = taskInfoOf(record(true, "SHARED_ASYNC"));

    assert.equal(info.taskId, TASK_ID);
    assert.equal(info.taskName, "validate_order");
    assert.equal(info.status, "FAILED");
    assert.equal(info.queueName, "default");
    assert.equal(info.priority, 100);
    assert.equal(info.retryCount, 2);
    assert.equal(info.maxRetries, 3);
    assert.equal(info.enqueuedAt.toString(), "2023-11-14T22:13:20.123458Z");
    assert.deepEqual(info.sentAt, { kind: "recorded", value: instantOfMicros(AT + 1n) });
    assert.deepEqual(info.claimedAt, { kind: "recorded", value: instantOfMicros(AT + 3n) });
    assert.deepEqual(info.startedAt, { kind: "recorded", value: instantOfMicros(AT + 4n) });
    assert.deepEqual(info.completedAt, { kind: "recorded", value: instantOfMicros(AT + 5n) });
    assert.deepEqual(info.failedAt, { kind: "recorded", value: instantOfMicros(AT + 6n) });
    assert.deepEqual(info.nextRetryAt, { kind: "recorded", value: instantOfMicros(AT + 10n) });
    assert.deepEqual(info.workerHostname, { kind: "recorded", value: "host-a" });
    assert.deepEqual(info.workerPid, { kind: "recorded", value: 4242 });
    assert.deepEqual(info.workerProcessName, { kind: "recorded", value: "child-1" });
    assert.deepEqual(info.errorCode, { kind: "recorded", value: "CARD_DECLINED" });
    assert.deepEqual(info.failedReason, { kind: "recorded", value: "the card was declined" });
    assert.deepEqual(info.execution, { kind: "class", execution: "SHARED_ASYNC" });
    assert.deepEqual(
      info.attempts.map((each) => [each.attempt, each.outcome, each.willRetry, each.startedAt.toString()]),
      [
        [2, "FAILED", true, "2023-11-14T22:13:20.123458Z"],
        [1, "FAILED", true, "2023-11-14T22:13:20.123457Z"],
      ],
    );
    assert.deepEqual(info.attempts[0]?.workerId, { kind: "recorded", value: "worker-1" });
    assert.deepEqual(info.attempts[0]?.workerPid, { kind: "recorded", value: 4242 });
    assert.equal(info.attempts[0]?.finishedAt.toString(), "2023-11-14T22:13:20.123459Z");
    assert.equal(Object.isFrozen(info), true);
    assert.equal(Object.isFrozen(info.attempts), true);
  });

  test("a fact that the row does not hold is notRecorded; a task read from history keeps no class", () => {
    const info = taskInfoOf(record(false, "NOT_KEPT_IN_HISTORY"));

    for (const fact of [
      info.sentAt,
      info.claimedAt,
      info.startedAt,
      info.completedAt,
      info.failedAt,
      info.nextRetryAt,
      info.workerHostname,
      info.workerPid,
      info.workerProcessName,
      info.errorCode,
      info.failedReason,
    ]) {
      assert.deepEqual(fact, { kind: "notRecorded" });
    }

    assert.deepEqual(info.attempts, []);
    assert.deepEqual(info.execution, { kind: "notKeptInHistory" });
  });

  test("each execution class of a live task", () => {
    for (const execution of ["ISOLATED", "SHARED_ASYNC", "SHARED_THREAD"] as const) {
      assert.deepEqual(taskInfoOf(record(true, execution)).execution, { kind: "class", execution });
    }
  });

  test("a task id from the core that is not canonical is a defect: TypeError", () => {
    assert.throws(() => taskInfoOf({ ...record(true, "ISOLATED"), taskId: "NOT-A-UUID" }), TypeError);
  });
});

describe("TaskHandle.info", () => {
  test("a found task gives its info, and info is not kept", async () => {
    const fake = fakeProducer({ infos: [{ kind: "found", record: record(true, "ISOLATED") }] });
    const handle = taskHandleOf(addonAccess(fake), readFormOf(contract), TASK_ID);
    const first = await handle.info();

    assert.equal(first.ok && first.value.status, "FAILED");
    void (await handle.info());
    assert.equal(fake.calls.taskInfo, 2);
  });

  test("an absent task is taskNotFound with its absence", async () => {
    const fake = fakeProducer({ infos: [{ kind: "absent", reason: "PREDATES_RETAINED_HISTORY" }] });
    const read = await taskHandleOf(addonAccess(fake), readFormOf(contract), TASK_ID).info();

    assert.deepEqual(read, { ok: false, error: { kind: "taskNotFound", absence: "predatesRetainedHistory" } });
  });

  test("a failed read is database", async () => {
    const fake = fakeProducer({ infos: [{ kind: "failed", error: operationFailedRaw("TASK_INFO_QUERY_FAILED", false) }] });
    const read = await taskHandleOf(addonAccess(fake), readFormOf(contract), TASK_ID).info();

    assert.deepEqual(read, {
      ok: false,
      error: { kind: "database", code: "TASK_INFO_QUERY_FAILED", retryable: false, message: "connection failed: refused" },
    });
  });

  test("in a seat isolate, info goes through the relay", async () => {
    const relay = countingRelay({ kind: "found", record: record(false, "SHARED_THREAD") });
    const read = await taskHandleOf(relay.access, readFormOf(contract), TASK_ID).info();

    assert.equal(read.ok && read.value.status, "PENDING");
    assert.equal(relay.requests(), 1);
  });

  test("a relay reply of another kind is a defect: ClientDefect", async () => {
    const handle = taskHandleOf(
      {
        kind: "seatRelay",
        relay: {
          request: async () => ({
            kind: "prepareSend",
            prepared: { kind: "refused", error: operationFailedRaw("TASK_INFO_QUERY_FAILED", false) },
          }),
        },
      },
      readFormOf(contract),
      TASK_ID,
    );

    await assert.rejects(handle.info(), ClientDefect);
  });
});
