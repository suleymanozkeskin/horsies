// The result read of a task handle: one test per read error kind, the outcome, and the kept first final read. The core
// is a fake producer at the boundary (`support.ts`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { defineTask } from "../../../horsies/src/contract/public.ts";
import { type ReadError, readErrorCode } from "../../../horsies/src/read/errors.ts";
import { readFormOf, taskHandleOf, waitBound } from "../../../horsies/src/read/handle.ts";
import { MAX_WAIT_LIMIT_MS } from "../../../horsies/src/send/bounds.ts";
import { AddonOutsideMainThread } from "../../../horsies/src/thrown-errors.ts";
import { h } from "../../../horsies/src/wire/public.ts";
import {
  addonAccess,
  countingRelay,
  fakeProducer,
  invalidJsonPayloadRaw,
  libraryErrorEnvelope,
  okEnvelope,
  operationFailedRaw,
  storedRead,
  TASK_ID,
  unfinishedRead,
  userErrorEnvelope,
} from "./support.ts";

const declared = defineTask({
  name: "validate_order",
  input: {},
  output: h.object({ id: h.int() }),
  errors: ["EMPTY_ORDER"],
});

const open = defineTask({ name: "validate_order", input: {}, output: h.object({ id: h.int() }) });

/** A contract that `Horsies.create` refuses: a default that the writer refuses. */
const refused = defineTask({ name: "refused", input: { ratio: h.float().default(Number.NaN) }, output: h.int() });

type Script = Parameters<typeof fakeProducer>[0];

/** A handle of the declared contract over a fake producer, and its calls. */
function declaredHandle(script: Script): {
  readonly handle: ReturnType<typeof taskHandleOf<{ id: number }, "EMPTY_ORDER">>;
  readonly calls: ReturnType<typeof fakeProducer>["calls"];
} {
  const fake = fakeProducer(script);

  return { handle: taskHandleOf(addonAccess(fake), readFormOf(declared), TASK_ID), calls: fake.calls };
}

/** The error of a read; a read that succeeded fails the test. */
async function readError(read: Promise<{ readonly ok: boolean; readonly error?: ReadError }>): Promise<ReadError> {
  const outcome = await read;

  if (outcome.ok || outcome.error === undefined) {
    assert.fail("the read succeeded");
  }

  return outcome.error;
}

describe("TaskHandle.get", () => {
  test("a stored ok value reads as the succeeded outcome", async () => {
    const { handle } = declaredHandle({ results: [storedRead("COMPLETED", okEnvelope('{"id":7}'))] });

    assert.deepEqual(await handle.get(), { ok: true, value: { kind: "succeeded", value: { id: 7 } } });
  });

  test("a task that has not finished when the wait ends is waitTimedOut with its status", async () => {
    for (const status of ["PENDING", "CLAIMED", "RUNNING"] as const) {
      const { handle } = declaredHandle({ results: [unfinishedRead(status)] });

      assert.deepEqual(await readError(handle.get({ timeoutMs: 0 })), { kind: "waitTimedOut", status });
    }
  });

  test("a task that neither table holds is taskNotFound with its absence", async () => {
    const absences = [
      ["PREDATES_RETAINED_HISTORY", "predatesRetainedHistory"],
      ["WITHIN_RETAINED_HISTORY", "withinRetainedHistory"],
      ["NOT_CLASSIFIED", "notClassified"],
    ] as const;

    for (const [reason, absence] of absences) {
      const { handle } = declaredHandle({ results: [{ kind: "absent", reason }] });

      assert.deepEqual(await readError(handle.get()), { kind: "taskNotFound", absence });
    }
  });

  test("a finished task that stores no result is resultNotAvailable with its status", async () => {
    for (const status of ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"] as const) {
      const { handle } = declaredHandle({ results: [unfinishedRead(status)] });

      assert.deepEqual(await readError(handle.get()), { kind: "resultNotAvailable", status });
    }
  });

  test("a stored value that does not fit the output kind is storedResultUnreadable with no value in the cause", async () => {
    const { handle } = declaredHandle({ results: [storedRead("COMPLETED", okEnvelope('{"id":"seven-secret"}'))] });
    const error = await readError(handle.get());

    assert.equal(error.kind, "storedResultUnreadable");
    assert.equal(error.kind === "storedResultUnreadable" && error.cause.includes("seven-secret"), false);
  });

  test("a contract that is refused reads as storedResultUnreadable, and its decode never runs", async () => {
    const fake = fakeProducer({ results: [storedRead("COMPLETED", okEnvelope("7"))] });
    const form = readFormOf(refused);
    const error = await readError(taskHandleOf(addonAccess(fake), form, TASK_ID).get());

    assert.equal(form.kind, "unreadable");
    assert.equal(error.kind, "storedResultUnreadable");
    assert.equal(error.kind === "storedResultUnreadable" && error.cause.startsWith("the contract is refused"), true);
  });

  test("a user code that the declared list does not hold is unknownUserCode", async () => {
    const { handle } = declaredHandle({ results: [storedRead("FAILED", userErrorEnvelope("CARD_DECLINED"))] });

    assert.deepEqual(await readError(handle.get()), { kind: "unknownUserCode", code: "CARD_DECLINED" });
  });

  test("a declared user code reads as the failed outcome with the user code", async () => {
    const { handle } = declaredHandle({ results: [storedRead("FAILED", userErrorEnvelope("EMPTY_ORDER"))] });
    const read = await handle.get();

    assert.equal(read.ok && read.value.kind === "failed" ? read.value.error.code.code : undefined, "EMPTY_ORDER");
    assert.equal(read.ok && read.value.kind === "failed" ? read.value.error.code.kind : undefined, "user");
  });

  test("with open codes, any user code reads", async () => {
    const fake = fakeProducer({ results: [storedRead("FAILED", userErrorEnvelope("CARD_DECLINED"))] });
    const read = await taskHandleOf(addonAccess(fake), readFormOf(open), TASK_ID).get();

    assert.deepEqual(read.ok && read.value.kind === "failed" ? read.value.error.code : undefined, {
      kind: "user",
      code: "CARD_DECLINED",
    });
  });

  test("a library code reads with a declared list", async () => {
    const { handle } = declaredHandle({ results: [storedRead("FAILED", libraryErrorEnvelope("BROKER_ERROR"))] });
    const read = await handle.get();

    assert.deepEqual(read.ok && read.value.kind === "failed" ? read.value.error.code : undefined, {
      kind: "library",
      code: "BROKER_ERROR",
    });
  });

  test("a wait in a process that runs task bodies is waitInsideTask, before any read", async () => {
    const { handle, calls } = declaredHandle({ role: "RUNS_TASK_BODIES", results: [unfinishedRead("PENDING")] });

    assert.deepEqual(await readError(handle.get({ timeoutMs: 1000 })), { kind: "waitInsideTask" });
    assert.equal(calls.processRole, 1);
    assert.equal(calls.getResult, 0);
  });

  test("a wait in a seat isolate is waitInsideTask with no relay request", async () => {
    const relay = countingRelay({ kind: "absent", reason: "NOT_CLASSIFIED" });
    const handle = taskHandleOf(relay.access, readFormOf(declared), TASK_ID);

    assert.deepEqual(await readError(handle.get()), { kind: "waitInsideTask" });
    assert.equal(relay.requests(), 0);
  });

  test("a failed database read is database with the broker code and the retryable flag", async () => {
    const { handle } = declaredHandle({ results: [{ kind: "failed", error: operationFailedRaw("TASK_INFO_QUERY_FAILED", true) }] });

    assert.deepEqual(await readError(handle.get()), {
      kind: "database",
      code: "TASK_INFO_QUERY_FAILED",
      retryable: true,
      message: "connection failed: refused",
    });
  });

  test("a stored result text that the core's strict check refuses ends as the core's refusal and never reaches the reader", async () => {
    const { handle, calls } = declaredHandle({ results: [{ kind: "failed", error: invalidJsonPayloadRaw() }] });
    const error = await readError(handle.get());

    assert.deepEqual(error, {
      kind: "database",
      code: "INVALID_JSON_PAYLOAD",
      retryable: false,
      message: "result JSON is refused: duplicate key",
    });
    // Not kept: a later read asks the core again.
    await handle.get();
    assert.equal(calls.getResult, 2);
  });

  test("a timeoutMs that is not whole milliseconds from 0 to the limit is invalidWait, before any core call", async () => {
    for (const timeoutMs of [-1, 1.5, MAX_WAIT_LIMIT_MS + 1, Number.NaN]) {
      const { handle, calls } = declaredHandle({ results: [unfinishedRead("PENDING")] });

      assert.deepEqual(await readError(handle.get({ timeoutMs })), { kind: "invalidWait", maxMs: MAX_WAIT_LIMIT_MS });
      assert.equal(calls.processRole + calls.getResult, 0);
    }
  });

  test("the wait bound: none waits until the end; 0 and the limit wait up to that bound", async () => {
    const { handle, calls } = declaredHandle({ results: [unfinishedRead("PENDING")] });

    await handle.get();
    await handle.get({ timeoutMs: 0 });
    await handle.get({ timeoutMs: MAX_WAIT_LIMIT_MS });

    assert.deepEqual(calls.waits, [
      { kind: "untilEnd" },
      { kind: "upTo", milliseconds: 0 },
      { kind: "upTo", milliseconds: MAX_WAIT_LIMIT_MS },
    ]);
    assert.deepEqual(waitBound(200), { ok: true, value: { kind: "upTo", milliseconds: 200 } });
  });

  test("the first final read is kept: a later get makes no call", async () => {
    const { handle, calls } = declaredHandle({ results: [storedRead("COMPLETED", okEnvelope('{"id":7}'))] });
    const first = await handle.get();
    const second = await handle.get({ timeoutMs: 5 });

    assert.equal(second, first);
    assert.equal(calls.getResult, 1);
  });

  test("a kept storedResultUnreadable is final too", async () => {
    const { handle, calls } = declaredHandle({ results: [storedRead("COMPLETED", okEnvelope("null"))] });

    await handle.get();
    await handle.get();

    assert.equal(calls.getResult, 1);
  });

  test("a waitTimedOut read is not kept: the next get reads again and can find the outcome", async () => {
    const { handle, calls } = declaredHandle({
      results: [unfinishedRead("RUNNING"), storedRead("COMPLETED", okEnvelope('{"id":8}'))],
    });

    assert.deepEqual(await readError(handle.get({ timeoutMs: 0 })), { kind: "waitTimedOut", status: "RUNNING" });
    assert.deepEqual(await handle.get(), { ok: true, value: { kind: "succeeded", value: { id: 8 } } });
    assert.equal(calls.getResult, 2);
  });

  test("a read in a worker thread with no core is a defect: AddonOutsideMainThread", async () => {
    const handle = taskHandleOf({ kind: "noCore" }, readFormOf(declared), TASK_ID);

    await assert.rejects(handle.get(), AddonOutsideMainThread);
    await assert.rejects(handle.info(), AddonOutsideMainThread);
  });
});

describe("readFormOf", () => {
  test("a contract that compiles is readable with its codes", () => {
    const form = readFormOf(declared);

    assert.equal(form.kind, "readable");
    assert.deepEqual(form.kind === "readable" ? form.codes : undefined, { kind: "declared", codes: ["EMPTY_ORDER"] });
  });

  test("a refused contract is unreadable, and the cause names the refused place", () => {
    const form = readFormOf(refused);

    assert.equal(form.kind === "unreadable" && form.cause.includes("input.ratio"), true);
  });
});

describe("readErrorCode", () => {
  test("each read error kind has the library code of the Python and Rust clients", () => {
    const cases: readonly (readonly [ReadError, string])[] = [
      [{ kind: "waitTimedOut", status: "PENDING" }, "WAIT_TIMEOUT"],
      [{ kind: "taskNotFound", absence: "notClassified" }, "TASK_NOT_FOUND"],
      [{ kind: "resultNotAvailable", status: "CANCELLED" }, "RESULT_NOT_AVAILABLE"],
      [{ kind: "storedResultUnreadable", cause: "c" }, "RESULT_DESERIALIZATION_ERROR"],
      [{ kind: "unknownUserCode", code: "X" }, "RESULT_DESERIALIZATION_ERROR"],
      [{ kind: "waitInsideTask" }, "WAIT_INSIDE_TASK"],
      [{ kind: "database", code: "TASK_INFO_QUERY_FAILED", retryable: true, message: "m" }, "BROKER_ERROR"],
      [{ kind: "invalidWait", maxMs: MAX_WAIT_LIMIT_MS }, "INVALID_INPUT"],
    ];

    for (const [error, code] of cases) {
      assert.equal(readErrorCode(error), code, error.kind);
    }
  });
});
