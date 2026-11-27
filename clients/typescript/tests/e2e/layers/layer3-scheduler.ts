// Layer 3: the scheduler (Rust `clients/rust/tests/worker/tests/layer3_scheduler.rs`, Python
// `clients/python/tests/e2e/test_layer3_scheduler.py`).
//
// The scheduler is the worker entry with `scheduler`: `cli.run` runs the schedules of the `scheduler` instance. A
// worker of the same instance runs what it sends. The `scheduler_invalid_rules` and `scheduler_invalid_task`
// instances declare schedules that the scheduler start refuses. Run it with `run.ts` (see that file).

import assert from "node:assert/strict";
import { describe } from "node:test";

import { TaskId } from "#horsies";
import { loadAddon } from "#horsies";
import * as scheduled from "../src/tasks/scheduled.ts";
import { succeeded } from "../support/outcome.ts";
import { found, NOT_YET, pollFor } from "../support/poll.ts";
import { exitText, type TestProcess } from "../support/process.ts";
import { type RowTime, sqlText } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for a schedule to fire and its task to run. */
const FIRE_WAIT_MS = 20_000;

/** The longest wait for a scheduler to write its first schedule state. */
const SCHEDULER_READY_LIMIT_MS = 30_000;

/** The longest wait for the exit of a scheduler start that the check refuses. */
const REFUSED_START_WAIT_MS = 60_000;

/** The result wait of a task that the worker already ran. */
const DONE_READ_MS = 1_000;

/** The exit code of a scheduler run error (`cli/scheduler-command.ts`). */
const EXIT_FAILED = 1;

/** A 32-byte config hash (the column's length) that no schedule config hashes to, as hex. */
const CORRUPTED_CONFIG_HASH = "00".repeat(32);

const MICROS_PER_SECOND = 1_000_000n;
const SECONDS_PER_MINUTE = 60n;
const MILLIS_PER_MINUTE = 60_000;

/** One row of `horsies_schedule_state`; the times in microseconds since 1970, the hash as hex. */
type ScheduleState = {
  readonly nextRunAt: bigint;
  readonly lastRunAt: RowTime;
  readonly runCount: number;
  readonly configHash: string;
};

/** The SQL text of the microseconds since 1970 of `column`, as text. */
const MICROS = (column: string): string => `floor(extract(epoch FROM ${column}) * 1000000)::bigint::text`;

/** The state of schedule `name`, or `absent` when it has none. */
async function scheduleState(suite: Suite, name: string): Promise<ScheduleState | "absent"> {
  const read = await suite.rows.query(
    `read the state of schedule ${name}`,
    `SELECT ${MICROS("next_run_at")}, ${MICROS("last_run_at")}, run_count::text, encode(config_hash, 'hex')
     FROM horsies_schedule_state WHERE schedule_name = ${sqlText(name)}`,
  );

  const [row] = read.rows;

  if (row === undefined) {
    return "absent";
  }

  const [nextRunAt, lastRunAt, runCount, configHash] = row;

  if (nextRunAt === undefined || nextRunAt === null || runCount === undefined || runCount === null || configHash === undefined || configHash === null) {
    assert.fail(`the state of schedule ${name} has no next run, run count or config hash: ${JSON.stringify(row)}`);
  }

  return {
    nextRunAt: BigInt(nextRunAt),
    lastRunAt: lastRunAt === undefined || lastRunAt === null ? { kind: "notSet" } : { kind: "set", epochMicros: BigInt(lastRunAt) },
    runCount: Number(runCount),
    configHash,
  };
}

/** Wait until the state of `name` passes `reached`, and give it. Fails the test when `limitMs` passes first. */
async function waitForState(suite: Suite, name: string, limitMs: number, reached: (state: ScheduleState) => boolean): Promise<ScheduleState> {
  return pollFor(limitMs, `the state of schedule ${name}`, async () => {
    const state = await scheduleState(suite, name);

    return state !== "absent" && reached(state) ? found(state) : NOT_YET;
  });
}

/** Any state: the scheduler wrote the row. */
const anyState = (): boolean => true;

/** Move the next run of schedule `name` to `epochMicros`. */
async function setNextRunAt(suite: Suite, name: string, epochMicros: bigint): Promise<void> {
  const changed = await suite.rows.query(
    `move the next run of ${name}`,
    `UPDATE horsies_schedule_state SET next_run_at = TIMESTAMPTZ 'epoch' + ${String(epochMicros)} * INTERVAL '1 microsecond'
     WHERE schedule_name = ${sqlText(name)}`,
  );

  assert.equal(changed.tag, "UPDATE 1", `schedule ${name} has a state row`);
}

/** The send times of the tasks named `taskName`, in microseconds since 1970, in send order. */
async function sendTimes(suite: Suite, taskName: string): Promise<readonly bigint[]> {
  const read = await suite.rows.query(
    `read the sends of ${taskName}`,
    `SELECT sent::text FROM (
       SELECT floor(extract(epoch FROM sent_at) * 1000000)::bigint AS sent, id AS task_id FROM horsies_tasks
       WHERE task_name = ${sqlText(taskName)}
       UNION ALL
       SELECT floor(extract(epoch FROM sent_at) * 1000000)::bigint, task_id FROM horsies_task_history
       WHERE task_name = ${sqlText(taskName)}
     ) AS sends ORDER BY sent, task_id`,
  );

  return read.rows.map((row) => {
    const sent = row[0];

    if (sent === undefined || sent === null) {
      assert.fail(`a task named ${taskName} has no send time`);
    }

    return BigInt(sent);
  });
}

/** Microseconds since 1970 now, on the clock of this process. */
function nowMicros(): bigint {
  return BigInt(Date.now()) * 1_000n;
}

/** Whether each value of `values` is greater than the one before it. Pure. */
function strictlyIncreasing(values: readonly bigint[]): boolean {
  let previous: bigint | "first" = "first";

  for (const value of values) {
    if (previous !== "first" && value <= previous) {
      return false;
    }

    previous = value;
  }

  return true;
}

/** Whether `epochMicros` is second 0 of a minute. Pure. */
function startsAMinute(epochMicros: bigint): boolean {
  return epochMicros % (SECONDS_PER_MINUTE * MICROS_PER_SECOND) === 0n;
}

/** Start the scheduler and wait until it wrote a schedule state row (Rust `Suite::scheduler`). */
async function startScheduler(suite: Suite): Promise<TestProcess> {
  const scheduler = suite.spawnEntry("scheduler", ["scheduler", "--log-level", "INFO"]);

  await scheduler.waitUntil(SCHEDULER_READY_LIMIT_MS, "the scheduler writes its state", async () => (await stateRows(suite)) > 0);

  return scheduler;
}

/** Stop `scheduler`: an operator stop ends the scheduler with 0. */
async function stopScheduler(scheduler: TestProcess): Promise<void> {
  const end = await scheduler.stop();

  assert.deepEqual(end, { kind: "exitedAfterStop", exit: { kind: "exitedWithCode", code: 0 } }, scheduler.report());
}

/** The number of schedule state rows. */
async function stateRows(suite: Suite): Promise<number> {
  return suite.rows.count("count the schedule states", "SELECT count(*) FROM horsies_schedule_state");
}

/** Start the scheduler of an instance that the scheduler start refuses; its output after the exit with 1. */
async function refusedSchedulerOutput(suite: Suite): Promise<string> {
  const scheduler = suite.spawnEntry("scheduler", ["scheduler", "--log-level", "INFO"]);
  const exit = await scheduler.waitExit(REFUSED_START_WAIT_MS);

  assert.deepEqual(exit, { kind: "exitedWithCode", code: EXIT_FAILED }, `${exitText(exit)}\n${scheduler.report()}`);
  assert.equal(await stateRows(suite), 0, "a refused start writes no schedule state");

  return scheduler.logLines().join("\n");
}

describe("layer 3: the scheduler", () => {
  e2eTest("the scheduler stores the state of a schedule at its start: no run yet, the next run ahead", "scheduler", async (suite) => {
    const scheduler = await startScheduler(suite);
    const state = await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, anyState);

    assert.ok(state.nextRunAt > nowMicros() - MICROS_PER_SECOND, `next run ${String(state.nextRunAt)} is not ahead`);
    assert.equal(state.runCount, 0);
    await stopScheduler(scheduler);
  });

  e2eTest("a schedule sends its task once it is due, after the scheduler started", "scheduler", async (suite) => {
    const start = nowMicros();

    await startScheduler(suite);
    await suite.worker("isolated-1");

    const firstSentAt = await pollFor(FIRE_WAIT_MS, "a scheduled task", async () => {
      const [sentAt] = await sendTimes(suite, scheduled.scheduledSimple.name);

      return sentAt === undefined ? NOT_YET : found(sentAt);
    });

    assert.ok(firstSentAt >= start, `sent at ${String(firstSentAt)} before the start ${String(start)}`);
  });

  e2eTest("no task is sent before the first next run of a schedule", "scheduler", async (suite) => {
    await startScheduler(suite);
    await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, anyState);

    assert.deepEqual(await sendTimes(suite, scheduled.scheduledSimple.name), [], "no task before the first next_run_at");
  });

  e2eTest("an interval schedule sends its task at each interval, one send after the other", "scheduler", async (suite) => {
    await startScheduler(suite);
    await suite.worker("isolated-1");

    const state = await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, (read) => read.runCount >= 2);

    assert.equal(state.lastRunAt.kind, "set");

    const sent = await sendTimes(suite, scheduled.scheduledSimple.name);

    assert.ok(sent.length >= 2, `${String(sent.length)} tasks sent`);
    assert.ok(strictlyIncreasing(sent), `each send follows the one before: ${sent.join(", ")}`);
  });

  e2eTest("a schedule config that breaks a rule is refused at the scheduler start: an interval of zero seconds, a repeated name", "scheduler_invalid_rules", async (suite) => {
    const output = await refusedSchedulerOutput(suite);

    assert.ok(output.includes("IntervalSchedule every must be between 1 and 31536000 seconds (1 second to 365 days), got 0 seconds"), output);
    assert.ok(output.includes(`has the name "${scheduled.DUPLICATE_SCHEDULE}"`), output);
  });

  e2eTest("a scheduler whose schedule names a task that the app does not list does not start", "scheduler_invalid_task", async (suite) => {
    const output = await refusedSchedulerOutput(suite);

    assert.ok(output.includes(`the task ${scheduled.unlistedTask.name} is not in the tasks list of this app`), output);
  });

  e2eTest("a disabled schedule has no state", "scheduler", async (suite) => {
    await startScheduler(suite);
    await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, anyState);

    assert.equal(await scheduleState(suite, scheduled.SCHEDULE_DISABLED), "absent");
  });

  e2eTest("a schedule with an input sends it, and its task returns twice the value", "scheduler", async (suite) => {
    await startScheduler(suite);
    await suite.worker("isolated-1");
    await waitForState(suite, scheduled.SCHEDULE_WITH_ARGS, FIRE_WAIT_MS, (state) => state.runCount >= 1);

    const completed = await pollFor(FIRE_WAIT_MS, "a completed scheduled task", async () => {
      const done = (await suite.rows.tasksNamed(scheduled.scheduledWithArgs.name)).find((row) => row.status === "COMPLETED");

      return done === undefined ? NOT_YET : found(done.taskId);
    });

    const taskId = TaskId.parse(completed);

    assert.ok(taskId.ok, `the completed row has a task id: ${completed}`);
    assert.equal(await succeeded(suite.app.taskHandle(scheduled.scheduledWithArgs, taskId.value), DONE_READ_MS), scheduled.SCHEDULED_VALUE * 2);
  });

  e2eTest("a config hash that changed in the database is restored at the next start", "scheduler", async (suite) => {
    const first = await startScheduler(suite);
    const original = await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, anyState);

    await stopScheduler(first);

    const changed = await suite.rows.query(
      "corrupt the config hash",
      `UPDATE horsies_schedule_state SET config_hash = decode('${CORRUPTED_CONFIG_HASH}', 'hex')
       WHERE schedule_name = ${sqlText(scheduled.SCHEDULE_SIMPLE_INTERVAL)}`,
    );

    assert.equal(changed.tag, "UPDATE 1");
    await startScheduler(suite);

    const restored = await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, (state) => state.configHash !== CORRUPTED_CONFIG_HASH);

    assert.equal(restored.configHash, original.configHash);
  });

  e2eTest("a catch-up schedule sends the slots that passed while no scheduler ran", "scheduler", async (suite) => {
    const first = await startScheduler(suite);

    await waitForState(suite, scheduled.SCHEDULE_CATCH_UP, FIRE_WAIT_MS, anyState);
    await stopScheduler(first);

    const past = nowMicros() - 5n * MICROS_PER_SECOND;

    await setNextRunAt(suite, scheduled.SCHEDULE_CATCH_UP, past);

    const before = (await sendTimes(suite, scheduled.catchUpTask.name)).length;

    await startScheduler(suite);
    await suite.worker("isolated-1");
    await waitForState(suite, scheduled.SCHEDULE_CATCH_UP, FIRE_WAIT_MS, (state) => state.nextRunAt > past);

    const caughtUp = (await sendTimes(suite, scheduled.catchUpTask.name)).length - before;

    assert.ok(caughtUp >= 2, `catch-up sent ${String(caughtUp)} tasks for at least 5 missed 1 s slots`);
  });

  e2eTest("a restarted scheduler keeps the run count and the config hash of a schedule", "scheduler", async (suite) => {
    const first = await startScheduler(suite);
    const worker = await suite.worker("isolated-1");
    const before = await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, (state) => state.runCount >= 1);

    await stopScheduler(first);
    await worker.stop();
    await startScheduler(suite);

    const after = await waitForState(suite, scheduled.SCHEDULE_SIMPLE_INTERVAL, FIRE_WAIT_MS, anyState);

    assert.ok(after.runCount >= before.runCount, `the restart kept the run count: ${String(before.runCount)} then ${String(after.runCount)}`);
    assert.equal(after.configHash, before.configHash);
  });

  // No worker: the scheduler writes the task row itself.
  e2eTest("a cron schedule fires on its due minute and moves to the start of a later minute", "scheduler", async (suite) => {
    const first = await startScheduler(suite);
    const initial = await waitForState(suite, scheduled.SCHEDULE_CRON_EVERY_MINUTE, FIRE_WAIT_MS, anyState);

    assert.equal(initial.runCount, 0);
    assert.ok(startsAMinute(initial.nextRunAt), `a cron slot starts a minute: ${String(initial.nextRunAt)}`);
    await stopScheduler(first);

    const dueSlot = BigInt(Math.floor(Date.now() / MILLIS_PER_MINUTE) * MILLIS_PER_MINUTE) * 1_000n;

    await setNextRunAt(suite, scheduled.SCHEDULE_CRON_EVERY_MINUTE, dueSlot);
    await startScheduler(suite);

    const fired = await waitForState(suite, scheduled.SCHEDULE_CRON_EVERY_MINUTE, FIRE_WAIT_MS, (state) => state.runCount >= 1 && state.nextRunAt > dueSlot);

    assert.ok(startsAMinute(fired.nextRunAt), `a cron slot starts a minute: ${String(fired.nextRunAt)}`);
    assert.ok((await sendTimes(suite, scheduled.cronTask.name)).length > 0, "the cron slot sent its task");
  });
});
