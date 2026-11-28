// `SHARED_THREAD` children: each seat is a `worker_threads` isolate of the child with one run at a time, and the main
// thread of the child hands the runs to the isolates (Rust `clients/rust/tests/worker/tests/child_model.rs`, Python
// `clients/python/tests/e2e/test_shared_thread_tasks.py`).
//
// - A thread run cannot be stopped: at its deadline the child closes, and the worker kills it when no run in it has
//   time left. The deadline also writes the cause into the cancel cell of the run's seat and fires `runtime.signal`, so
//   a body that reads either one can return; its run still ends `TASK_TIMEOUT`.
// - A seat isolate never loads the addon: its sends go through the relay of the main thread.
// - The main thread of the child keeps turning while the isolates compute. When it stalls, the core reports
//   `loop_blocked` with no named run, and the worker kills the child.
//
// On Bun the worker refuses a profile with `SHARED_THREAD` children (Bun ignores the heap cap of a worker thread). The
// first test asserts the refusal on Bun and the start on Node; Bun skips every other test with `BUN_SKIP_REASON`.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe, test } from "node:test";

import { type TaskHandle, TaskId } from "#horsies";
import { loadAddon } from "#horsies";
import { threadProvidedState } from "../src/app.ts";
import { type Instance, THREAD_MEMORY_BOUND_MB, type ThreadProfile } from "../src/instances.ts";
import * as basic from "../src/tasks/basic.ts";
import * as fencing from "../src/tasks/fencing.ts";
import * as threads from "../src/tasks/threads.ts";
import { runtimeName } from "../src/tasks/waits.ts";
import { dataMembers, errorCode, failed, failedCode, infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { exitText } from "../support/process.ts";
import { pollUntil } from "../support/rows.ts";
import { e2eTest, type Suite, TEST_LIMIT_MS, WORKER_READY_LINE } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** Why Bun skips the tests of this file but the first. */
const BUN_SKIP_REASON = "SHARED_THREAD children run on Node only: on Bun the worker refuses a profile with them (the first test of this file)";

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The longest wait for the outcome of a task whose runs end with the kill and the replacement of their child. */
const REPLACED_WAIT_MS = 90_000;

/** The longest wait for a worker whose start is refused to end, and for a worker to log `WORKER_READY_LINE`. */
const START_WAIT_MS = 60_000;

/** The worker exit code of every run error (a refused start among them). */
const EXIT_FAILED = 1;

/** The seats of each thread child of the thread profiles (`instances.ts`). */
const SEATS = 4;

/** The log line of the worker when the main thread of a shared child stalls (core `slots/shared_slot.rs`). */
const LOOP_KILL_LINE = "the event loop of a shared child did not turn; killing the child";

/** How long the neighbour of a timed-out thread run runs (Python `NEIGHBOUR_S`). */
const NEIGHBOUR_MS = 4_000;

/** How long each sleeper of the run-at-once case sleeps (Rust and Python `1_500` ms). */
const SLEEPER_MS = 1_500;

/** The cause that the deadline writes into the cancel cell and gives to the signal. */
const DEADLINE_LINE = "DEADLINE\n";

/** The exit code of a thread child that its memory watch ended at its bound (core `MEMORY_LIMIT_EXIT_CODE`). */
const MEMORY_LIMIT_EXIT_CODE = 81;

/** The part of the line of a thread child that its memory watch ended (`child/memory_limit.rs` of the addon). */
const MEMORY_LIMIT_LINE = `at or above its bound recycle.above = ${String(THREAD_MEMORY_BOUND_MB)} MiB, while it holds runs; the child ends with exit code ${String(MEMORY_LIMIT_EXIT_CODE)} (MEMORY_LIMIT)`;

switch (runtimeName()) {
  case "bun":
    process.stderr.write(`shared-thread: ${BUN_SKIP_REASON}\n`);
    break;
  case "node":
    break;
}

/** A test that runs on Node; on Bun it is skipped with `BUN_SKIP_REASON`. */
function seatTest(name: string, instance: Instance, body: (suite: Suite) => Promise<void>, limitMs: number = TEST_LIMIT_MS): void {
  switch (runtimeName()) {
    case "node":
      e2eTest(name, instance, body, limitMs);

      return;
    case "bun":
      test(name, { skip: BUN_SKIP_REASON }, () => undefined);
  }
}

/** The process id of the isolated child of the split profile. */
async function isolatedPid(suite: Suite): Promise<number> {
  return succeeded(await sentHandle("e2e_pid", suite.app.send(basic.pid, { sleepMs: 0 })), RESULT_WAIT_MS);
}

/** The process id of the thread child of a profile, from a run of `e2e_thread_place`. */
async function threadChildPid(suite: Suite): Promise<number> {
  return (await succeeded(await sentHandle("e2e_thread_place", suite.app.send(threads.threadPlace, {})), RESULT_WAIT_MS)).pid;
}

/** Whether the time ranges of `stamps` overlap: the latest start comes before the earliest end. Pure. */
function ranAtOnce(stamps: readonly threads.Stamp[]): boolean {
  return Math.max(...stamps.map((stamp) => stamp.startMs)) < Math.min(...stamps.map((stamp) => stamp.endMs));
}

/** The error of a thread run that timed out: `TASK_TIMEOUT`, with no `cancel_end` (only a shared async timeout stores one). */
async function threadTimeout<O, E extends string>(handle: TaskHandle<O, E>, limitMs: number): Promise<void> {
  const error = await failed(handle, limitMs);

  assert.equal(errorCode(error).code, "TASK_TIMEOUT");
  assert.equal("cancel_end" in dataMembers(error), false, `only a shared async timeout stores cancel_end: ${JSON.stringify(error.data)}`);
}

/** The time of the failure of a task that failed. */
async function failedAtMs<O, E extends string>(handle: TaskHandle<O, E>): Promise<number> {
  const info = await infoOf(handle);

  if (info.failedAt.kind === "notRecorded") {
    assert.fail(`task ${handle.taskId} has no failed_at`);
  }

  return info.failedAt.value.epochMilliseconds;
}

/** The text of the marker `name`. Fails the test when it is absent. */
function markerText(suite: Suite, name: string): string {
  const read = suite.markers.read(name);

  if (read.kind === "absent") {
    assert.fail(`the marker ${name} exists`);
  }

  return read.text;
}

describe("SHARED_THREAD children", () => {
  // On Bun, `SHARED_THREAD` stays off (the thread-seat result of the runtime spike): the worker refuses the profile. On
  // Node the worker starts the thread child, whose 4 seat isolates each run the provider once.
  e2eTest("a profile with SHARED_THREAD children is refused at start on Bun and starts on Node", "thread_profiles", async (suite) => {
    const worker = suite.spawnWorker("thread-only-1x4");

    switch (runtimeName()) {
      case "bun": {
        const exit = await worker.waitExit(START_WAIT_MS);

        assert.deepEqual(exit, { kind: "exitedWithCode", code: EXIT_FAILED }, `${exitText(exit)}\n${worker.report()}`);
        assert.ok(worker.logCount('workerProfiles["thread-only-1x4"].sharedThread') > 0, worker.report());
        assert.ok(worker.logCount("SHARED_THREAD") > 0, worker.report());

        return;
      }

      case "node": {
        await worker.waitForLog(WORKER_READY_LINE, START_WAIT_MS);

        await pollUntil(RESULT_WAIT_MS, "the provider ran in the 4 seat isolates", async () => suite.markers.names("provider-").filter((name) => name.includes("-seat")).length === SEATS);

        const end = await worker.stop();

        assert.deepEqual(end, { kind: "exitedAfterStop", exit: { kind: "exitedWithCode", code: 0 } }, worker.report());
      }
    }
  });

  // Rust `child_model.rs::thread_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished`, Python
  // `test_shared_thread_tasks.py::test_thread_tasks_run_at_once_and_a_timeout_leaves_neighbours_finished`.
  seatTest("thread tasks run at once in seat isolates; a timeout leaves its neighbours finished", "thread_profiles", async (suite) => {
    await suite.worker("split-isolated-1-thread-1x4");

    const isolated = await isolatedPid(suite);
    const hang = await sentHandle("e2e_thread_hang", suite.app.send(threads.threadHang, {}));
    const sleepers = [];

    for (let index = 0; index < SEATS - 1; index += 1) {
      sleepers.push(await sentHandle("e2e_thread_sleep", suite.app.send(threads.threadSleep, { milliseconds: SLEEPER_MS })));
    }

    const stamps = [];

    for (const sleeper of sleepers) {
      stamps.push(await succeeded(sleeper, RESULT_WAIT_MS));
    }

    await threadTimeout(hang, RESULT_WAIT_MS);

    assert.equal(new Set(stamps.map((stamp) => stamp.pid)).size, 1, "the sleepers share one thread child");
    assert.ok(stamps.every((stamp) => stamp.pid !== isolated), "the sleepers ran in the thread child");
    assert.ok(stamps.every((stamp) => stamp.thread === "seatIsolate"), "each sleeper ran in a seat isolate");
    assert.equal(new Set(stamps.map((stamp) => stamp.threadId)).size, stamps.length, "each sleeper had its own isolate");
    assert.ok(ranAtOnce(stamps), "the sleepers ran at once");
  });

  // Python `test_shared_thread_tasks.py::test_a_timed_out_task_ends_after_its_neighbour_with_time_left`.
  seatTest("a timed-out thread task ends after its neighbour with time left; the next task runs in a new child", "thread_profiles", async (suite) => {
    await suite.worker("thread-only-1x4");

    const hang = await sentHandle("e2e_thread_hang", suite.app.send(threads.threadHang, {}));
    const neighbour = await succeeded(await sentHandle("the neighbour", suite.app.send(threads.threadSleep, { milliseconds: NEIGHBOUR_MS })), RESULT_WAIT_MS);

    await threadTimeout(hang, RESULT_WAIT_MS);

    const after = await succeeded(await sentHandle("a later task", suite.app.send(threads.threadSleep, { milliseconds: 0 })), RESULT_WAIT_MS);

    assert.ok((await failedAtMs(hang)) >= neighbour.endMs, "the timeout was written before its neighbour ended");
    assert.notEqual(after.pid, neighbour.pid, "the closed child was replaced");
  });

  // Rust `child_model.rs::a_timed_out_thread_does_not_overlap_its_retry` (#560): the runs write their lines to one marker.
  seatTest(
    "a timed-out thread run does not overlap its retry",
    "thread_profiles",
    async (suite) => {
      await suite.worker("split-isolated-1-thread-1x4");

      const token = randomUUID();
      const handle = await sentHandle("e2e_thread_overrun", suite.app.send(threads.threadOverrun, { token }));

      assert.equal(await failedCode(handle, REPLACED_WAIT_MS), "TASK_TIMEOUT");

      const info = await infoOf(handle);

      assert.deepEqual([info.status, info.retryCount], ["FAILED", 3]);

      const runs = runLines(markerText(suite, `${token}-runs`));

      assert.equal(runs.starts.size, 4, `four runs started: ${JSON.stringify([...runs.starts])}`);

      for (const [attempt, start] of runs.starts) {
        const previousEnd = runs.lastLines.get(attempt - 1);

        if (previousEnd !== undefined) {
          assert.ok(previousEnd < start, `run ${String(attempt - 1)} wrote a line at ${String(previousEnd)} after run ${String(attempt)} started at ${String(start)}`);
        }
      }
    },
    REPLACED_WAIT_MS + TEST_LIMIT_MS / 2,
  );

  // Rust `child_model.rs::shared_thread_runs_read_their_own_fencing_tokens`.
  seatTest("two runs on one shared thread child each read the token of their own task", "thread_profiles", async (suite) => {
    await suite.worker("thread-only-1x4");

    const first = await sentHandle("e2e_fence_shared_thread", suite.app.send(fencing.fenceSharedThread, {}));
    const second = await sentHandle("e2e_fence_shared_thread", suite.app.send(fencing.fenceSharedThread, {}));

    for (const handle of [first, second]) {
      assert.deepEqual(await succeeded(handle, RESULT_WAIT_MS), { taskId: handle.taskId, value: 1 });
    }
  });

  // Python `test_shared_thread_tasks.py::test_a_thread_row_with_no_stored_timeout_fails_once`.
  for (const profile of ["split-isolated-1-thread-1x4", "thread-only-1x4"] as const satisfies readonly ThreadProfile[]) {
    seatTest(`a thread row with no stored timeout_ms fails once (${profile})`, "thread_profiles", async (suite) => {
      const handle = await sentHandle("e2e_thread_sleep", suite.app.send(threads.threadSleep, { milliseconds: 0 }));

      await suite.rows.changeStoredOptions(handle.taskId, { kind: "remove", key: "timeout_ms" });
      await suite.worker(profile);

      const error = await failed(handle, RESULT_WAIT_MS);

      assert.equal(errorCode(error).code, "WORKER_SERIALIZATION_ERROR");
      assert.equal((await infoOf(handle)).retryCount, 0);
      assert.equal(error.message, "stored timeout_ms is absent for execution SHARED_THREAD");
      assert.ok(JSON.stringify(error).includes('"field":"timeout_ms"'), `the refusal names the field timeout_ms: ${JSON.stringify(error)}`);
    });
  }

  seatTest("a sync and an async thread body run in seat isolates of the thread child", "thread_profiles", async (suite) => {
    await suite.worker("split-isolated-1-thread-1x4");

    const isolated = await isolatedPid(suite);
    const sync = await succeeded(await sentHandle("e2e_thread_place", suite.app.send(threads.threadPlace, {})), RESULT_WAIT_MS);
    const async = await succeeded(await sentHandle("e2e_thread_place_async", suite.app.send(threads.threadPlaceAsync, {})), RESULT_WAIT_MS);

    for (const place of [sync, async]) {
      assert.equal(place.thread, "seatIsolate");
      assert.notEqual(place.pid, isolated, "a thread body ran in the isolated child");
    }

    assert.equal(sync.pid, async.pid, "both bodies ran in the one thread child");
  });

  seatTest("at the deadline a sync thread body reads its cancel cell and an async thread body sees runtime.signal", "thread_profiles", async (suite) => {
    await suite.worker("thread-only-1x4");

    // The neighbour has time left, so the child is not killed at the deadline: each body ends on its own read.
    const neighbour = await sentHandle("the neighbour", suite.app.send(threads.threadSleep, { milliseconds: NEIGHBOUR_MS + threads.THREAD_STOP_TIMEOUT_MS }));
    const syncToken = randomUUID();
    const asyncToken = randomUUID();
    const sync = await sentHandle("e2e_thread_sync_cooperative", suite.app.send(threads.threadSyncCooperative, { token: syncToken }));
    const async = await sentHandle("e2e_thread_async_cooperative", suite.app.send(threads.threadAsyncCooperative, { token: asyncToken }));

    await threadTimeout(sync, RESULT_WAIT_MS);
    await threadTimeout(async, RESULT_WAIT_MS);

    const neighbourEnd = (await succeeded(neighbour, RESULT_WAIT_MS)).endMs;

    assert.deepEqual(suite.markers.read(`${syncToken}-cause`), { kind: "found", text: DEADLINE_LINE });
    assert.deepEqual(suite.markers.read(`${asyncToken}-cause`), { kind: "found", text: DEADLINE_LINE });

    for (const handle of [sync, async]) {
      assert.ok((await failedAtMs(handle)) < neighbourEnd, `task ${handle.taskId} ended at the kill of its child, not at its own return`);
    }
  });

  seatTest("the provider's value reaches each thread body from its own seat isolate; the main thread runs the hooks only", "thread_profiles", async (suite) => {
    await suite.worker("thread-only-1x4");

    // Each run holds its isolate, so the runs sent at once take the four seats.
    const handles = [];

    for (let seat = 0; seat < SEATS; seat += 1) {
      handles.push(await sentHandle("e2e_thread_provided_state", suite.app.send(threadProvidedState, { holdMs: SLEEPER_MS })));
    }

    const states = [];

    for (const handle of handles) {
      states.push(await succeeded(handle, RESULT_WAIT_MS));
    }

    const [childPid, ...otherPids] = new Set(states.map((state) => state.bodyPid));

    assert.ok(childPid !== undefined && otherPids.length === 0, "the runs share one thread child");

    for (const state of states) {
      assert.deepEqual([state.providerPid, state.providerThreadId], [state.bodyPid, state.bodyThreadId], "the state came from the provider of the body's own isolate");
    }

    const isolates = [...new Set(states.map((state) => state.bodyThreadId))];

    assert.equal(isolates.length, SEATS, "the runs took the four seats");
    // The only child of the profile is the thread child: one provider marker per isolate, none of the main thread.
    assert.deepEqual(
      suite.markers.names("provider-").toSorted(),
      isolates.map((isolate) => `provider-${String(childPid)}-seat${String(isolate)}`).toSorted(),
      "the provider ran once in each isolate and not on the main thread",
    );
    assert.deepEqual(suite.markers.names("hook-"), [`hook-${String(childPid)}`], "the onChildStart hook ran once, on the main thread");
  });

  seatTest("a seat isolate loads no addon: a thread body sends through the relay, and the sent task runs", "thread_profiles", async (suite) => {
    await suite.worker("split-isolated-1-thread-1x4");

    const isolated = await isolatedPid(suite);
    const answer = await succeeded(await sentHandle("e2e_thread_sends_from_seat", suite.app.send(threads.threadSendsFromSeat, { x: 7 })), RESULT_WAIT_MS);
    const sentId = TaskId.parse(answer.sentId);

    assert.equal(answer.place.thread, "seatIsolate");
    assert.notEqual(answer.place.pid, isolated);
    assert.equal(answer.addonLoads, 0, "the seat isolate opened a .node file");
    assert.ok(sentId.ok, `the body gave the id ${answer.sentId}`);
    assert.equal(await succeeded(suite.app.taskHandle(basic.simple, sentId.value), RESULT_WAIT_MS), 14);
  });

  seatTest("two CPU-heavy thread bodies run at once past the block limit of the main thread; the child lives", "thread_profiles", async (suite) => {
    const worker = await suite.worker("thread-only-1x4");

    const handles = [
      await sentHandle("e2e_thread_cpu_work", suite.app.send(threads.threadCpuWork, { milliseconds: threads.CPU_WORK_MS })),
      await sentHandle("e2e_thread_cpu_work", suite.app.send(threads.threadCpuWork, { milliseconds: threads.CPU_WORK_MS })),
    ];

    const ends = [];

    for (const handle of handles) {
      ends.push(await succeeded(handle, RESULT_WAIT_MS));
    }

    const stamps = ends.map((end) => end.stamp);

    assert.ok(ends.every((end) => end.turns > 0), "each body computed");
    assert.equal(new Set(stamps.map((stamp) => stamp.threadId)).size, 2, "each body had its own isolate");
    assert.ok(ranAtOnce(stamps), "the bodies ran at once");
    assert.equal(await threadChildPid(suite), stamps[0]?.pid, "the child was not replaced");
    assert.equal(worker.logCount(LOOP_KILL_LINE), 0, worker.report());
  });

  seatTest("a thread child past its memory bound while it holds a run ends with exit code 81; the run retries in a new child", "thread_profiles", async (suite) => {
    const worker = await suite.worker("thread-memory-bound-1x1");
    const token = randomUUID();
    const handle = await sentHandle("e2e_thread_holds_memory", suite.app.send(threads.threadHoldsMemory, { token }));
    const retryPid = await succeeded(handle, REPLACED_WAIT_MS);
    const heldPid = Number(markerText(suite, `${token}-held`));
    const info = await infoOf(handle);
    const first = info.attempts.find((attempt) => attempt.attempt === 1);

    assert.notEqual(retryPid, heldPid, "the retry ran in a new child");
    assert.deepEqual([info.status, info.retryCount], ["COMPLETED", 1]);
    assert.deepEqual(first?.errorCode, { kind: "recorded", value: "WORKER_CRASHED" });
    assert.ok(
      first?.errorMessage.kind === "recorded" && first.errorMessage.value.includes(`child died: exit code ${String(MEMORY_LIMIT_EXIT_CODE)}`),
      `the worker read the loss of the child with exit code ${String(MEMORY_LIMIT_EXIT_CODE)}: ${JSON.stringify(first?.errorMessage)}`,
    );
    assert.equal(worker.logCount(MEMORY_LIMIT_LINE), 1, worker.report());
  }, REPLACED_WAIT_MS + TEST_LIMIT_MS / 2);

  seatTest("a blocked main thread of a thread child gives loop_blocked: the worker kills the child, and the run retries in a new child", "thread_profiles", async (suite) => {
    const worker = await suite.worker("thread-only-1x4");
    const token = randomUUID();
    const handle = await sentHandle("e2e_thread_blocks_main", suite.app.send(threads.threadBlocksMain, { token }));
    const retryPid = await succeeded(handle, REPLACED_WAIT_MS);
    const blockedPid = Number(markerText(suite, `${token}-blocked`));
    const info = await infoOf(handle);
    const first = info.attempts.find((attempt) => attempt.attempt === 1);

    assert.notEqual(retryPid, blockedPid, "the retry ran in a new child");
    assert.deepEqual([info.status, info.retryCount], ["COMPLETED", 1]);
    assert.deepEqual(first?.errorCode, { kind: "recorded", value: "WORKER_CRASHED" });
    assert.ok(worker.logCount(LOOP_KILL_LINE) > 0, worker.report());
    assert.ok(worker.logCount("the event loop of the host program") > 0, worker.report());
  }, REPLACED_WAIT_MS + TEST_LIMIT_MS / 2);
});

/** The runs of the lines of `e2e_thread_overrun`: the start time of each attempt, and the time of its last line. */
type RunLines = { readonly starts: ReadonlyMap<number, number>; readonly lastLines: ReadonlyMap<number, number> };

/** The runs of `text`, the lines `<start|tick> <attempt> <ms>`. Fails the test for another line. */
function runLines(text: string): RunLines {
  const starts = new Map<number, number>();
  const lastLines = new Map<number, number>();

  for (const line of text.split("\n").filter((read) => read !== "")) {
    const [kind, attemptText, atText, ...rest] = line.split(" ");
    const attempt = Number(attemptText);
    const at = Number(atText);

    assert.ok((kind === "start" || kind === "tick") && Number.isInteger(attempt) && Number.isInteger(at) && rest.length === 0, `a run line is <kind> <attempt> <ms>, not ${JSON.stringify(line)}`);

    if (kind === "start" && !starts.has(attempt)) {
      starts.set(attempt, at);
    }

    lastLines.set(attempt, Math.max(lastLines.get(attempt) ?? at, at));
  }

  return { starts, lastLines };
}
