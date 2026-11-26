// Stored options that a worker cannot use fail their task in one claim (Rust
// `clients/rust/tests/worker/tests/stored_timeout_refusal.rs`, Python
// `clients/python/tests/e2e/test_shared_async_tasks.py::test_a_shared_row_with_unusable_options_fails_once`).
//
// A queued row can hold options that the current worker refuses: an older version accepted any `timeout_ms` of at
// least 1000, and a `SHARED_ASYNC` row needs `timeout_ms` and `cancel_grace_ms`. Each test changes the stored options
// before its worker starts, so the first claim of the row reads them. The worker fails the row with
// `WORKER_SERIALIZATION_ERROR` before the task resolves; its body never runs, and no later claim takes it. The task of
// a workflow node fails its node the same way, and the workflow goes on from that failure.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { describe } from "node:test";

import type { TaskId } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import type { Profile } from "../src/instances.ts";
import * as basic from "../src/tasks/basic.ts";
import * as shared from "../src/tasks/shared.ts";
import { linear } from "../src/workflows/structure.ts";
import { errorCode, failed, infoOf, sentHandle, succeeded } from "../support/outcome.ts";
import { sqlText, type TaskRow } from "../support/rows.ts";
import { e2eTest, type Suite } from "../support/suite.ts";
import { startedHandle } from "../support/workflow-reads.ts";
import { endedNodes, nodeById, statusesOf, taskOfNode, workflowNodes } from "../support/workflow-rows.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest wait for the end of a task that a running worker runs. */
const RESULT_WAIT_MS = 30_000;

/** The largest stored `timeout_ms` that a worker runs (core `worker::retry::MAX_TASK_TIMEOUT_MS`). */
const TIMEOUT_BOUND_MS = 4_294_967_295;

/** The code of a run that the worker refuses. */
const REFUSAL_CODE = "WORKER_SERIALIZATION_ERROR";

/** The message of the refusal of a stored timeout above `TIMEOUT_BOUND_MS` (core `worker::retry::timeout_refusal_message`). */
const ABOVE_BOUND_MESSAGE = `stored timeout_ms is above the limit of ${String(TIMEOUT_BOUND_MS)} ms (about 49.7 days)`;

/** Check that task `taskId` ended FAILED with the refusal in one claim: one attempt, which does not retry. */
async function refusedInOneClaim(suite: Suite, taskId: TaskId): Promise<TaskRow> {
  const info = await infoOf(suite.app.taskHandle(basic.idempotent, taskId));

  assert.deepEqual([info.status, info.retryCount], ["FAILED", 0]);

  const [attempt, ...rest] = info.attempts;

  assert.deepEqual(rest, []);
  assert.deepEqual(
    [attempt?.attempt, attempt?.outcome, attempt?.willRetry, attempt?.errorCode],
    [1, "FAILED", false, { kind: "recorded", value: REFUSAL_CODE }],
  );

  const row = await suite.rows.taskRow(taskId);

  assert.deepEqual([row.status, row.errorCode, row.retryCount], ["FAILED", REFUSAL_CODE, 0]);

  return row;
}

/** Run a later task, so that the worker makes claim passes after the end, then check that task `taskId` was not claimed again. */
async function notClaimedAgain(suite: Suite, taskId: TaskId, ended: TaskRow): Promise<void> {
  assert.equal(await succeeded(await sentHandle("the later task", suite.app.send(basic.simple, { x: 1 })), RESULT_WAIT_MS), 2);

  const live = await suite.rows.count(`count the live rows of ${taskId}`, `SELECT count(*) FROM horsies_tasks WHERE id = ${sqlText(taskId)}`);

  assert.equal(live, 0, `task ${taskId} has no live row to claim`);
  assert.deepEqual(await suite.rows.taskRow(taskId), ended, `task ${taskId} did not change after its end`);
  assert.equal((await infoOf(suite.app.taskHandle(basic.idempotent, taskId))).attempts.length, 1);
}

describe("stored options that a worker refuses", () => {
  e2eTest("a stored timeout above the bound fails in one claim and is not claimed again", "default", async (suite) => {
    const token = randomUUID();
    const handle = await sentHandle("e2e_idempotent", suite.app.send(basic.idempotent, { token }));

    await suite.rows.changeStoredOptions(handle.taskId, { kind: "set", key: "timeout_ms", value: TIMEOUT_BOUND_MS + 1 });
    await suite.worker("isolated-1");

    const error = await failed(handle, RESULT_WAIT_MS);

    assert.equal(errorCode(error).code, REFUSAL_CODE);
    assert.equal(error.message, ABOVE_BOUND_MESSAGE);
    assert.equal(suite.markers.read(`idempotent-${token}`).kind, "absent", "the body of the refused task ran");

    const ended = await refusedInOneClaim(suite, handle.taskId);

    assert.equal(ended.isWorkflowTask, false);
    await notClaimedAgain(suite, handle.taskId, ended);
  });

  e2eTest("a refused timeout of a root node fails the node in one claim; the nodes after it are skipped", "default", async (suite) => {
    const handle = await startedHandle("linear", suite.app.start(linear, {}));
    const taskId = await taskOfNode(suite.rows, handle.workflowId, "a", RESULT_WAIT_MS);

    await suite.rows.changeStoredOptions(taskId, { kind: "set", key: "timeout_ms", value: TIMEOUT_BOUND_MS + 1 });
    await suite.worker("isolated-1");

    const nodes = await endedNodes(suite.rows, handle.workflowId, "FAILED", RESULT_WAIT_MS);
    const root = nodeById(nodes, "a");

    assert.deepEqual(statusesOf(nodes), ["FAILED", "SKIPPED", "SKIPPED"]);
    assert.equal(root.taskId, taskId);
    assert.ok(root.result?.includes(REFUSAL_CODE), `node a stores the refusal: ${String(root.result)}`);

    const ended = await refusedInOneClaim(suite, taskId);

    assert.equal(ended.isWorkflowTask, true);
    await notClaimedAgain(suite, taskId, ended);
    assert.deepEqual(await workflowNodes(suite.rows, handle.workflowId), nodes, "the nodes did not change after the end");
  });

  for (const profile of ["split-isolated-1-shared-1x4", "shared-only-1x4"] as const satisfies readonly Profile[]) {
    for (const key of ["cancel_grace_ms", "timeout_ms"] as const) {
      e2eTest(`a shared async row with no ${key} fails once (${profile})`, "default", async (suite) => {
        const handle = await sentHandle("e2e_shared_sleep", suite.app.send(shared.sharedSleep, { milliseconds: 0 }));

        await suite.rows.changeStoredOptions(handle.taskId, { kind: "remove", key });
        await suite.worker(profile);

        const error = await failed(handle, RESULT_WAIT_MS);

        // The core's child host writes the data of Python's child runner: the key in `data.field`, and the reason as
        // the message.
        assert.equal(errorCode(error).code, REFUSAL_CODE);
        assert.equal((await infoOf(handle)).retryCount, 0);
        assert.equal(error.message, `stored ${key} is absent for execution SHARED_ASYNC`);
        assert.ok(JSON.stringify(error).includes(`"field":"${key}"`), `the refusal names the field ${key}: ${JSON.stringify(error)}`);
      });
    }
  }
});
