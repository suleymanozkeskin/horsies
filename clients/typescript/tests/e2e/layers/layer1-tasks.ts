// Layer 1: task fundamentals on the child model (Rust `clients/rust/tests/worker/tests/layer1_tasks.rs`, Python
// `clients/python/tests/e2e/test_layer1_tasks.py`).
//
// Each test has its own database, builds the app of its instance in this process, and starts the worker entry as its
// worker; every task body runs in a child of that worker. Run it with `run.ts` (see that file).

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import { defineTask, h, TaskId } from "#horsies";
import { loadAddon } from "#horsies";
import * as basic from "../src/tasks/basic.ts";
import * as custom from "../src/tasks/custom.ts";
import * as fencing from "../src/tasks/fencing.ts";
import * as retry from "../src/tasks/retry.ts";
import {
  dataMembers,
  errorCode,
  failed,
  failedCode,
  infoOf,
  outcomeOf,
  pollUntilInfo,
  sentHandle,
  succeeded,
} from "../support/outcome.ts";
import { maxDuring, neverDuring, setTime } from "../support/rows.ts";
import { pause } from "../support/process.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 15_000;

const MILLISECONDS_PER_SECOND = 1_000;

/** The delay before retry `retryNumber` (counted from 1) of the exponential policy of `retry.exponential`. */
function exponentialDelayMs(retryNumber: number): number {
  return retry.EXPONENTIAL_BASE_SECONDS * 2 ** (retryNumber - 1) * MILLISECONDS_PER_SECOND;
}

/** The result wait of a task that a cancel moved to history. */
const CANCELLED_READ_MS = 1_000;

/** A task that only this test process lists: no worker knows its name. */
const nonexistent = defineTask({ name: "nonexistent_task_xyz", input: {}, output: h.string() });

/** `e2e_simple` with an input type that the worker's `e2e_simple` does not decode. */
const simpleAsText = defineTask({ name: "e2e_simple", input: { x: h.string() }, output: h.int() });

/** The instant `seconds` from now. */
function inSeconds(seconds: number): Temporal.Instant {
  return Temporal.Now.instant().add({ milliseconds: Math.round(seconds * 1_000) });
}

describe("layer 1: tasks", () => {
  e2eTest("a simple task runs and gives its value", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), 10);
  });

  e2eTest("a completed task records its claim, start and completion", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }));

    await succeeded(handle, RESULT_WAIT_MS);

    const info = await infoOf(handle);

    assert.equal(info.status, "COMPLETED");
    assert.equal(info.claimedAt.kind, "recorded");
    assert.equal(info.startedAt.kind, "recorded");
    assert.equal(info.completedAt.kind, "recorded");
  });

  e2eTest("members that a send leaves out take the body's defaults", "default", async (suite) => {
    await suite.worker("isolated-1");

    const defaults = await sentHandle("with defaults", suite.app.send(basic.kwargs, { required: 5 }));
    const given = await sentHandle("with values", suite.app.send(basic.kwargs, { required: 5, optional: "custom", multiplier: 2 }));

    assert.equal(await succeeded(defaults, RESULT_WAIT_MS), "5_default");
    assert.equal(await succeeded(given, RESULT_WAIT_MS), "10_custom");
  });

  e2eTest("a returned error keeps its code, message and data", "default", async (suite) => {
    await suite.worker("isolated-1");

    const error = await failed(await sentHandle("e2e_error", suite.app.send(basic.deliberateError, {})), RESULT_WAIT_MS);

    assert.deepEqual(errorCode(error), { source: "custom", code: "DELIBERATE_ERROR" });
    assert.equal(error.message, "This is intentional");
    assert.deepEqual(error.data, { key: "value" });
  });

  e2eTest("a code that the policy does not name is not retried", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_retry_other_code", suite.app.send(retry.retryOtherCode, {}));

    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "RETRYABLE_ERROR");

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["FAILED", 0]);
  });

  e2eTest("an isolated body past its deadline fails with TASK_TIMEOUT and the next task runs", "default", async (suite) => {
    await suite.worker("isolated-1");

    const blocking = await sentHandle("e2e_timeout_sleeper", suite.app.send(basic.timeoutSleeper, { durationMs: 30_000 }));

    assert.equal(await failedCode(blocking, RESULT_WAIT_MS), "TASK_TIMEOUT");

    const asyncBody = await sentHandle("e2e_async_timeout_sleeper", suite.app.send(basic.asyncTimeoutSleeper, { durationMs: 30_000 }));
    const error = await failed(asyncBody, RESULT_WAIT_MS);
    const data = dataMembers(error);

    assert.equal(errorCode(error).code, "TASK_TIMEOUT");
    assert.equal(data["timeout_ms"], basic.SLEEPER_TIMEOUT_MS);
    assert.equal("cancel_end" in data, false, "only a shared async timeout stores cancel_end");

    const after = await sentHandle("after the timeouts", suite.app.send(basic.simple, { x: 1 }));

    assert.equal(await succeeded(after, RESULT_WAIT_MS), 2);
  });

  e2eTest("TASK_TIMEOUT in the retry policy retries the timed-out task", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_timeout_retry_sleeper", suite.app.send(basic.timeoutRetrySleeper, { durationMs: 30_000 }));

    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "TASK_TIMEOUT");

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["FAILED", 1]);
    assert.deepEqual(
      info.attempts.map((attempt) => attempt.errorCode),
      [
        { kind: "recorded", value: "TASK_TIMEOUT" },
        { kind: "recorded", value: "TASK_TIMEOUT" },
      ],
    );
  });

  e2eTest("a slow task completes", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_slow", suite.app.send(basic.slow, { durationMs: 1_500 }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), "slept_1500");
  });

  e2eTest("two isolated children run two tasks at once", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handles = [];

    for (let index = 0; index < 4; index += 1) {
      handles.push(await sentHandle("e2e_slow", suite.app.send(basic.slow, { durationMs: 800 })));
    }

    const mostRunning = await maxDuring(3_000, 50, async () => suite.rows.liveCount("RUNNING", "anyQueue"));

    for (const handle of handles) {
      await succeeded(handle, RESULT_WAIT_MS);
    }

    assert.ok(mostRunning >= 2, `at most ${String(mostRunning)} tasks ran at once`);
  });

  e2eTest("four children run four bodies at once: every body started before any ended", "default", async (suite) => {
    await suite.worker("isolated-4");

    const handles = [];

    for (let index = 0; index < 4; index += 1) {
      handles.push(await sentHandle("e2e_stamp", suite.app.send(basic.stamp, { durationMs: 1_500 })));
    }

    const stamps = [];

    for (const handle of handles) {
      stamps.push(stampParts(await succeeded(handle, RESULT_WAIT_MS)));
    }

    assert.equal(new Set(stamps.map((part) => part.pid)).size, 4, "each body ran in its own child");
    assert.ok(Math.max(...stamps.map((part) => part.start)) < Math.min(...stamps.map((part) => part.end)), "every body started before the first ended");
  });

  e2eTest("the readiness task runs", "default", async (suite) => {
    await suite.worker("isolated-1");

    assert.equal(await succeeded(await sentHandle("e2e_healthcheck", suite.app.send(basic.healthcheck, {})), RESULT_WAIT_MS), "ready");
  });

  e2eTest("a task that always fails retries until its policy ends", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_retry_exhausted", suite.app.send(retry.retryExhausted, {}));

    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "TRANSIENT");

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["FAILED", 3]);
  });

  e2eTest("a task that fails twice succeeds on its third attempt", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_retry_success", suite.app.send(retry.retrySuccess, { token: randomUUID() }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), "succeeded_on_attempt_3");
  });

  e2eTest("a task whose goodUntil passed before the worker started does not run", "default", async (suite) => {
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }, { goodUntil: inSeconds(-10) }));

    await suite.worker("isolated-1");
    await neverRuns(suite, handle.taskId);
  });

  e2eTest("a task within its goodUntil completes", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }, { goodUntil: inSeconds(60) }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), 10);
  });

  e2eTest("a retry that would start after goodUntil does not run", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_retry_long_interval", suite.app.send(retry.retryLongInterval, {}, { goodUntil: inSeconds(10) }));

    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "TRANSIENT");

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["FAILED", 0]);
  });

  e2eTest("retries within goodUntil run until the task succeeds", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_retry_success", suite.app.send(retry.retrySuccess, { token: randomUUID() }, { goodUntil: inSeconds(60) }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), "succeeded_on_attempt_3");

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["COMPLETED", 2]);
  });

  e2eTest("a task that the worker does not implement fails with WORKER_RESOLUTION_ERROR", "default", async (suite) => {
    const other = suite.otherApp([nonexistent]);

    await suite.worker("isolated-1");

    const handle = await sentHandle("nonexistent_task_xyz", other.send(nonexistent, {}));

    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "WORKER_RESOLUTION_ERROR");
  });

  e2eTest("a stored input that the worker's types refuse fails with WORKER_SERIALIZATION_ERROR", "default", async (suite) => {
    const other = suite.otherApp([simpleAsText]);

    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_simple as text", other.send(simpleAsText, { x: "not_a_number" }));

    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "WORKER_SERIALIZATION_ERROR");
  });

  e2eTest("a cancelled PENDING task reads as TASK_CANCELLED, with its task id in the data", "default", async (suite) => {
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }));

    await cancelPending(suite, handle.taskId);

    const error = await failed(handle, CANCELLED_READ_MS);

    assert.deepEqual(errorCode(error), { source: "builtIn", code: "TASK_CANCELLED" });
    assert.equal(dataMembers(error)["task_id"], handle.taskId);
  });

  e2eTest("a cancelled PENDING task read by its id gives TASK_CANCELLED", "default", async (suite) => {
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }));

    await cancelPending(suite, handle.taskId);

    const byId = suite.app.taskHandle(basic.simple, handle.taskId);

    assert.equal(await failedCode(byId, CANCELLED_READ_MS), "TASK_CANCELLED");
  });

  e2eTest("the info of a completed task names it and holds one attempt", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 21 }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), 42);

    const info = await infoOf(handle);

    assert.equal(info.taskId, handle.taskId);
    assert.equal(info.taskName, "e2e_simple");
    assert.equal(info.queue, "default");
    assert.equal(info.status, "COMPLETED");
    assert.equal(info.attempts.length, 1);
  });

  e2eTest("an exponential policy schedules its next retry", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_exponential", suite.app.send(retry.exponential, {}));
    const info = await pollUntilInfo(handle, 5_000, "a retry is scheduled", (read) => read.retryCount >= 1 && read.nextRetryAt.kind === "recorded");

    assert.equal(info.maxRetries, retry.EXPONENTIAL_MAX_RETRIES);
  });

  e2eTest("exponential retries wait longer each time; sentAt does not change", "default", async (suite) => {
    const handle = await sentHandle("e2e_exponential", suite.app.send(retry.exponential, {}));
    const first = await infoOf(handle);
    const due = [first.dueAt];

    await suite.worker("isolated-1");
    await pollUntilInfo(handle, 20_000, "the third retry", (info) => {
      if (info.retryCount === due.length) {
        due.push(info.dueAt);
      }

      assert.deepEqual(info.sentAt, first.sentAt, "sentAt changed on a retry");

      return due.length > retry.EXPONENTIAL_MAX_RETRIES;
    });

    // The interval from the send to the first retry holds the start of the worker, so it is not measured.
    const [, firstRetry, secondRetry, thirdRetry] = due.map((instant) => instant.epochMilliseconds);

    assert.ok(firstRetry !== undefined && secondRetry !== undefined && thirdRetry !== undefined);
    const secondDelay = secondRetry - firstRetry;
    const thirdDelay = thirdRetry - secondRetry;
    const policySecondDelayMs = exponentialDelayMs(2);
    const policyThirdDelayMs = exponentialDelayMs(3);

    assert.ok(secondDelay >= policySecondDelayMs, `the second delay is ${String(secondDelay)} ms, the policy gives ${String(policySecondDelayMs)} ms`);
    assert.ok(thirdDelay >= policyThirdDelayMs, `the third delay is ${String(thirdDelay)} ms, the policy gives ${String(policyThirdDelayMs)} ms`);
    assert.ok(thirdDelay > secondDelay, `delays ${String(secondDelay)} then ${String(thirdDelay)} ms`);
  });

  e2eTest("a code that the policy names is retried", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_retry_error_code", suite.app.send(retry.retryErrorCode, {}));

    assert.equal(await failedCode(handle, RESULT_WAIT_MS), "RETRYABLE_ERROR");

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["FAILED", 1]);
  });

  e2eTest("a task that expires before any claim does not run", "default", async (suite) => {
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }, { goodUntil: inSeconds(0.5) }));

    await pause(1_000);
    await suite.worker("isolated-1");
    await neverRuns(suite, handle.taskId);
  });

  e2eTest("a prefetched claim whose goodUntil passes before its start expires", "soft_cap", async (suite) => {
    await suite.worker("isolated-1");

    const blocker = await sentHandle("the blocker", suite.app.send(basic.slow, { durationMs: 4_000 }));

    await suite.rows.waitForStatus(blocker.taskId, "RUNNING", RESULT_WAIT_MS);

    const expiring = await sentHandle("the expiring task", suite.app.send(basic.simple, { x: 5 }, { goodUntil: inSeconds(1) }));

    await suite.rows.waitForStatus(expiring.taskId, "EXPIRED", RESULT_WAIT_MS);
    assert.equal(await succeeded(blocker, RESULT_WAIT_MS), "slept_4000");
  });

  e2eTest("a task of a queue with a higher priority starts first", "custom_queues", async (suite) => {
    const low = await sentHandle("e2e_low", suite.app.send(custom.low, {}));
    const high = await sentHandle("e2e_high", suite.app.send(custom.high, {}));

    await suite.worker("isolated-1");
    await succeeded(low, RESULT_WAIT_MS);
    await succeeded(high, RESULT_WAIT_MS);

    const lowStart = setTime((await suite.rows.taskRow(low.taskId)).startedAt, "the start of e2e_low");
    const highStart = setTime((await suite.rows.taskRow(high.taskId)).startedAt, "the start of e2e_high");

    assert.ok(highStart < lowStart, `high started at ${String(highStart)}, low at ${String(lowStart)}`);
  });

  e2eTest("two workers run each of 20 tasks once", "default", async (suite) => {
    await suite.worker("isolated-2");
    await suite.worker("isolated-2");
    await idempotentRuns(suite, "task_", 20);
  });

  e2eTest("one claim pass takes at most one task per queue", "default", async (suite) => {
    await suite.worker("claim-batch-1");

    const handles = [];

    for (let index = 0; index < 6; index += 1) {
      handles.push(await sentHandle("e2e_slow", suite.app.send(basic.slow, { durationMs: 300 })));
    }

    const mostClaimed = await maxDuring(4_000, 50, async () => suite.rows.liveCount("CLAIMED", "anyQueue"));

    for (const handle of handles) {
      await succeeded(handle, RESULT_WAIT_MS);
    }

    assert.ok(mostClaimed <= 1, `${String(mostClaimed)} tasks were CLAIMED at once`);
  });

  e2eTest("a read of an id that no task has is taskNotFound", "default", async (suite) => {
    const taskId = TaskId.parse(randomUUID());

    assert.ok(taskId.ok);

    const read = await suite.app.taskHandle(basic.simple, taskId.value).get({ waitMs: 1_000 });

    assert.equal(read.ok ? "read" : read.error.kind, "taskNotFound");
  });

  e2eTest("a bounded wait for a task that no worker runs is waitTimeout", "default", async (suite) => {
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 5 }));
    const read = await handle.get({ waitMs: 500 });

    assert.deepEqual(read, { ok: false, error: { kind: "waitTimeout", code: "WAIT_TIMEOUT", status: "PENDING" } });
  });

  e2eTest("a body that returns a built-in code is stored as UNHANDLED_EXCEPTION with the TypeError", "default", async (suite) => {
    await suite.worker("isolated-1");

    const error = await failed(await sentHandle("e2e_builtin_code", suite.app.send(basic.builtInCode, {})), RESULT_WAIT_MS);

    assert.deepEqual(errorCode(error), { source: "builtIn", code: "UNHANDLED_EXCEPTION" });
    assert.equal(error.exception?.type, "TypeError");
    assert.match(error.message ?? "", /^Unhandled exception in task e2e_builtin_code: TypeError: .*"TASK_EXCEPTION" is a built-in error code/);
  });

  e2eTest("a thrown error is stored as UNHANDLED_EXCEPTION with its name, message and stack; the child lives", "default", async (suite) => {
    await suite.worker("isolated-1");

    const before = await succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 })), RESULT_WAIT_MS);
    const error = await failed(await sentHandle("e2e_throws", suite.app.send(basic.throws, { text: "x" })), RESULT_WAIT_MS);

    assert.deepEqual(errorCode(error), { source: "builtIn", code: "UNHANDLED_EXCEPTION" });
    assert.equal(error.message, "Unhandled exception in task e2e_throws: TypeError: e2e boom: x");
    assert.deepEqual(error.data, { exception_type: "TypeError" });
    assert.equal(error.exception?.type, "TypeError");
    assert.equal(error.exception.message, "e2e boom: x");
    assert.match(error.exception.traceback, /TypeError: e2e boom: x\n\s+at /);

    const after = await succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 })), RESULT_WAIT_MS);

    assert.equal(after, before, "the throw did not end the child");
  });

  e2eTest("an object output keeps its members and nested lists", "default", async (suite) => {
    await suite.worker("isolated-1");

    const value = await succeeded(await sentHandle("e2e_complex_result", suite.app.send(basic.complexResult, {})), RESULT_WAIT_MS);

    assert.deepEqual(value, { value: 42, nested: { a: [1, 2, 3], b: [4, 5] } });
  });

  e2eTest("each task of a custom queue runs", "custom_queues", async (suite) => {
    await suite.worker("isolated-1");

    const high = await sentHandle("e2e_high", suite.app.send(custom.high, {}));
    const normal = await sentHandle("e2e_normal", suite.app.send(custom.normal, {}));
    const low = await sentHandle("e2e_low", suite.app.send(custom.low, {}));

    assert.equal(await succeeded(high, RESULT_WAIT_MS), "high");
    assert.equal(await succeeded(normal, RESULT_WAIT_MS), "normal");
    assert.equal(await succeeded(low, RESULT_WAIT_MS), "low");
  });

  e2eTest("a send stores the queue name and its priority", "custom_queues", async (suite) => {
    const sends = [
      { handle: await sentHandle("e2e_high", suite.app.send(custom.high, {})), queue: "high", priority: 1 },
      { handle: await sentHandle("e2e_normal", suite.app.send(custom.normal, {})), queue: "normal", priority: 50 },
      { handle: await sentHandle("e2e_low", suite.app.send(custom.low, {})), queue: "low", priority: 100 },
    ];

    for (const { handle, queue, priority } of sends) {
      const row = await suite.rows.taskRow(handle.taskId);

      assert.deepEqual([row.queueName, row.priority], [queue, priority]);
    }
  });

  e2eTest("a success records one completed attempt", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x: 1 }));

    await succeeded(handle, RESULT_WAIT_MS);

    const [attempt, ...rest] = (await infoOf(handle)).attempts;

    assert.deepEqual(rest, []);
    assert.deepEqual([attempt?.attempt, attempt?.outcome, attempt?.willRetry, attempt?.errorCode], [1, "COMPLETED", false, { kind: "notRecorded" }]);
  });

  e2eTest("a returned error records one failed attempt with its code", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_error", suite.app.send(basic.deliberateError, {}));

    await failed(handle, RESULT_WAIT_MS);

    const [attempt, ...rest] = (await infoOf(handle)).attempts;

    assert.deepEqual(rest, []);
    assert.deepEqual([attempt?.outcome, attempt?.willRetry, attempt?.errorCode], ["FAILED", false, { kind: "recorded", value: "DELIBERATE_ERROR" }]);
  });

  e2eTest("an exhausted policy records each attempt, newest first", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_retry_exhausted", suite.app.send(retry.retryExhausted, {}));

    await failed(handle, RESULT_WAIT_MS);

    const { attempts } = await infoOf(handle);

    assert.deepEqual(
      attempts.map((attempt) => [attempt.attempt, attempt.willRetry]),
      [
        [4, false],
        [3, true],
        [2, true],
        [1, true],
      ],
    );
    assert.ok(attempts.every((attempt) => attempt.errorCode.kind === "recorded" && attempt.errorCode.value === "TRANSIENT"));
  });

  e2eTest("a second send with the same key and input gives the first task", "default", async (suite) => {
    const first = await sentHandle("first", suite.app.send(basic.simple, { x: 7 }, { idempotencyKey: "order-7" }));
    const second = await sentHandle("second", suite.app.send(basic.simple, { x: 7 }, { idempotencyKey: "order-7" }));

    assert.equal(second.taskId, first.taskId);
    await suite.worker("isolated-1");
    assert.equal(await succeeded(first, RESULT_WAIT_MS), 14);
    assert.equal(await suite.rows.countTasks("e2e_simple", "COMPLETED"), 1);
  });

  e2eTest("a second send with the same key and another input is IDEMPOTENCY_KEY_CONFLICT", "default", async (suite) => {
    await sentHandle("first", suite.app.send(basic.simple, { x: 8 }, { idempotencyKey: "order-8" }));

    const second = await suite.app.send(basic.simple, { x: 9 }, { idempotencyKey: "order-8" });

    assert.equal(second.ok ? "sent" : second.error.code, "IDEMPOTENCY_KEY_CONFLICT");
  });

  e2eTest("a task cannot wait for another task: the wait is waitInsideTask", "default", async (suite) => {
    await suite.worker("isolated-2");

    const handle = await sentHandle("e2e_wait_inside_task", suite.app.send(basic.waitInsideTask, {}));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), true, "the wait inside the task was not refused with waitInsideTask");
  });

  e2eTest("an isolated body sends a task that a worker runs", "default", async (suite) => {
    await suite.worker("isolated-2");

    const sentId = await succeeded(await sentHandle("e2e_async_sends", suite.app.send(basic.asyncSends, { x: 4 })), RESULT_WAIT_MS);
    const taskId = TaskId.parse(sentId);

    assert.ok(taskId.ok, `the body gave the id ${sentId}`);

    const read = await outcomeOf(suite.app.taskHandle(basic.simple, taskId.value), RESULT_WAIT_MS);

    assert.deepEqual(read, { kind: "succeeded", value: 8 });
  });

  e2eTest("a send while HORSIES_SUPPRESS_SENDS=1 is SEND_SUPPRESSED and writes nothing", "default", async (suite) => {
    process.env["HORSIES_SUPPRESS_SENDS"] = "1";

    try {
      const refused = await suite.app.send(basic.simple, { x: 1 });

      assert.equal(refused.ok ? "sent" : refused.error.code, "SEND_SUPPRESSED");
    } finally {
      delete process.env["HORSIES_SUPPRESS_SENDS"];
    }

    assert.deepEqual(await suite.rows.tasksNamed("e2e_simple"), []);
  });

  e2eTest("an isolated run reads the token of its task with the value 1, async and sync bodies", "default", async (suite) => {
    await suite.worker("isolated-1");

    const asyncRun = await sentHandle("e2e_fence_isolated_async", suite.app.send(fencing.fenceIsolatedAsync, {}));
    const syncRun = await sentHandle("e2e_fence_isolated_sync", suite.app.send(fencing.fenceIsolatedSync, {}));

    assert.deepEqual(await succeeded(asyncRun, RESULT_WAIT_MS), { taskId: asyncRun.taskId, value: 1 });
    assert.deepEqual(await succeeded(syncRun, RESULT_WAIT_MS), { taskId: syncRun.taskId, value: 1 });
  });

  e2eTest("two reads in one attempt and a read in spawned work give the same token", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_fence_reads_twice", suite.app.send(fencing.fenceReadsTwice, {}));
    const expected = { taskId: handle.taskId, value: 1 };

    assert.deepEqual(await succeeded(handle, RESULT_WAIT_MS), [expected, expected, expected]);
  });

  e2eTest("the retry of a task runs with a greater token", "default", async (suite) => {
    await suite.worker("isolated-1");

    const handle = await sentHandle("e2e_fence_retry", suite.app.send(fencing.fenceRetry, { marker: randomUUID() }));
    const run = await succeeded(handle, RESULT_WAIT_MS);

    assert.deepEqual(run, { fence: { taskId: handle.taskId, value: 2 }, countedRun: 2 });

    const info = await infoOf(handle);

    assert.deepEqual([info.status, info.retryCount], ["COMPLETED", 1]);
  });
});

/** The parts of a `<pid>:<start ms>:<end ms>` stamp. */
function stampParts(stamp: string): { readonly pid: string; readonly start: number; readonly end: number } {
  const [pid, start, end, ...rest] = stamp.split(":");

  assert.ok(pid !== undefined && start !== undefined && end !== undefined && rest.length === 0, `a stamp is pid:start:end, not ${stamp}`);

  return { pid, start: Number(start), end: Number(end) };
}

/** Cancel the PENDING task `taskId` with no worker: the cancel moves it from PENDING. */
async function cancelPending(suite: Suite, taskId: TaskId): Promise<void> {
  const cancelled = await suite.app.monitoring.cancelTask(taskId, { policy: "pendingOrClaimedOnly" });

  assert.deepEqual(cancelled, { ok: true, value: { taskId, wasStatus: "PENDING" } });
}

/** Fail the test when task `taskId` leaves PENDING for any status but EXPIRED within 2 s. */
async function neverRuns(suite: Suite, taskId: TaskId): Promise<void> {
  await neverDuring(2_000, "the expired task runs", async () => {
    const status = await suite.rows.taskStatus(taskId);

    return status !== "PENDING" && status !== "EXPIRED";
  });
}

/** Send `count` runs of `e2e_idempotent` with the tokens `<prefix><n>`; each body runs once. */
async function idempotentRuns(suite: Suite, prefix: string, count: number): Promise<void> {
  const handles = [];

  for (let index = 0; index < count; index += 1) {
    handles.push(await sentHandle("e2e_idempotent", suite.app.send(basic.idempotent, { token: `${prefix}${String(index)}` })));
  }

  for (const handle of handles) {
    await succeeded(handle, RESULT_WAIT_MS);
  }

  assert.equal(suite.markers.count(`idempotent-${prefix}`), count);
}
