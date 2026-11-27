// The lifecycle record of each attempt on a live worker (Rust `clients/rust/tests/worker/tests/lifecycle_record.rs`),
// read through the monitoring task detail: the attempt that the worker recorded carries its phases, with the
// placement of the attempt on a child. The worker follows the placement whether its telemetry is off (an app with no
// event handler) or on (an app with an event handler, which starts a handler child). Run it with `run.ts` (see that
// file).

import assert from "node:assert/strict";
import { describe } from "node:test";

import type { TaskAttemptInfo, TaskId } from "#horsies";
import { loadAddon } from "#horsies";
import type { Instance } from "../src/instances.ts";
import * as basic from "../src/tasks/basic.ts";
import { sentHandle, succeeded } from "../support/outcome.ts";
import { e2eTest, type Suite } from "../support/suite.ts";

// The addon of these tests is the one that the entry of the lane loads (`entry/`).
void loadAddon();

/** The longest wait for the outcome of a task that a running worker runs. */
const RESULT_WAIT_MS = 15_000;

/** The tasks of each test. */
const TASKS = 3;

/** The one attempt of task `taskId` in its monitoring detail. */
async function onlyAttempt(suite: Suite, taskId: TaskId): Promise<TaskAttemptInfo> {
  const read = await suite.app.monitoring.getTaskDetail(taskId);

  assert.ok(read.ok && read.value.kind === "found", `read the detail of task ${taskId}: ${read.ok ? read.value.kind : read.error.code}`);

  const [attempt, ...others] = read.value.taskDetail.attempts;

  assert.ok(attempt !== undefined && others.length === 0, `task ${taskId} has one attempt, not ${String(read.value.taskDetail.attempts.length)}`);

  return attempt;
}

/**
 * The phases of `attempt`: captured and written by the worker, one counted claim whose first claim is the claim that
 * ran it, a due time at or before the claim, and a placement at or after the claim.
 */
function assertPlacedByTheWorker(taskId: TaskId, attempt: TaskAttemptInfo): void {
  const phases = attempt.phases;

  assert.equal(attempt.outcome, "COMPLETED", `task ${taskId}`);
  assert.ok(phases.kind === "captured", `the attempt of task ${taskId} has its phases`);
  assert.ok(phases.recordedBy.kind === "worker", `the worker recorded the attempt of task ${taskId}, not ${phases.recordedBy.kind}`);
  assert.ok(phases.recordedBy.placement.kind === "placed", `the attempt of task ${taskId} has its placement`);
  assert.ok(phases.claims.kind === "counted", `the claims of task ${taskId} are counted`);
  assert.equal(phases.claims.count, 1, `task ${taskId}`);
  assert.ok(phases.claims.firstClaimedAt.equals(phases.claimedAt), `the first claim of task ${taskId} is the claim that ran it`);
  assert.ok(Temporal.Instant.compare(phases.enqueuedAt, phases.claimedAt) <= 0, `task ${taskId} was due at or before its claim`);
  assert.ok(Temporal.Instant.compare(phases.recordedBy.placement.placedAt, phases.claimedAt) >= 0, `task ${taskId} was placed at or after its claim`);
}

/** Send `TASKS` tasks to a worker of the suite's instance, wait for each result, and check the phases of each attempt. */
async function eachAttemptRecordsItsPlacement(suite: Suite): Promise<void> {
  await suite.worker("isolated-1");

  const taskIds: TaskId[] = [];

  for (let x = 0; x < TASKS; x += 1) {
    const handle = await sentHandle("e2e_simple", suite.app.send(basic.simple, { x }));

    assert.equal(await succeeded(handle, RESULT_WAIT_MS), x * 2);
    taskIds.push(handle.taskId);
  }

  for (const taskId of taskIds) {
    assertPlacedByTheWorker(taskId, await onlyAttempt(suite, taskId));
  }
}

/** The instances of the two tests: no event handler, and an event handler with its handler child. */
const TELEMETRY_OFF: Instance = "default";
const TELEMETRY_ON: Instance = "handlers";

describe("the lifecycle record", () => {
  e2eTest("with telemetry off, every attempt that the worker recorded keeps its placement", TELEMETRY_OFF, eachAttemptRecordsItsPlacement);

  e2eTest("with telemetry on, every attempt that the worker recorded keeps its placement", TELEMETRY_ON, eachAttemptRecordsItsPlacement);
});
