// The pure parts of the TypeScript parity program: the entry's commands, the scenario table, the send options, the
// read of a stored code, the step lines, the configs and the schedule, the doubled value, and the waits of the bodies. No addon, no
// database and no process: a test that builds the app or sends runs in the parity runs (`crossclient/parity`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { TaskError, TaskOutcome } from "../../../horsies/src/index.ts";
import {
  doubledValue,
  holdThread,
  ISOLATED,
  PARITY_SCHEDULE,
  parityConfig,
  PROFILES,
  REFUSED_BROKER_CHECKOUT_WAIT_S,
  REFUSED_BROKER_URL,
  refusedBrokerConfig,
  refusedMessage,
  SCHEDULE_NAME,
  SCHEDULE_SECTION,
  SHARED_ASYNC,
  SHARED_THREAD,
  TASKS,
  transientMessage,
  UPSTREAM_FAILED_MESSAGE,
  waitOrSignal,
  WORKFLOWS,
} from "../src/app.ts";
import { SCENARIOS, scenarioNamed } from "../src/scenarios.ts";
import {
  endedRefused,
  entryCommand,
  outcomeText,
  pingLineText,
  sendOptions,
  storedCode,
  taskLineText,
  workflowLineText,
} from "../src/send.ts";

/** The scenario names of `crossclient/parity/scenarios.py`, in its order. */
const PYTHON_SCENARIO_NAMES = [
  "isolated_outcomes",
  "shared_async_outcomes",
  "shared_thread_outcomes",
  "isolated_timeout",
  "shared_async_timeout_cancel_grace",
  "shared_thread_timeout",
  "idempotency_key",
  "workflow_chain",
  "several_workers",
  "send_retry",
  "workflow_pause_resume_cancel",
  "sub_workflow",
  "schedule_slots",
  "worker_health",
  "rerun",
];

/** A stored error with `code` and no other fact. */
function storedError(code: TaskError["code"]): TaskError {
  return { code, message: null, data: null, exception: null, subWorkflow: null };
}

const REFUSED_OUTCOME: TaskOutcome<number, string> = {
  kind: "failed",
  error: storedError({ kind: "user", code: "REFUSED" }),
};

describe("the entry's commands", () => {
  test("send with one scenario name is the send process", () => {
    assert.deepEqual(entryCommand(["send", "rerun"]), { kind: "send", scenario: "rerun" });
  });

  test("every other argument list goes to cli.run", () => {
    assert.deepEqual(entryCommand(["worker", "--profile=isolated-1", "--loglevel=info"]), { kind: "cli" });
    assert.deepEqual(entryCommand(["--horsies-child"]), { kind: "cli" });
    assert.deepEqual(entryCommand(["send"]), { kind: "cli" });
    assert.deepEqual(entryCommand(["send", "rerun", "again"]), { kind: "cli" });
    assert.deepEqual(entryCommand([]), { kind: "cli" });
  });
});

describe("the scenario table", () => {
  test("has the scenarios of the Python copy, in its order", () => {
    assert.deepEqual(
      SCENARIOS.map((scenario) => scenario.name),
      PYTHON_SCENARIO_NAMES,
    );
  });

  test("a name of the table is found", () => {
    const lookup = scenarioNamed("worker_health");

    assert.equal(lookup.kind === "found" ? lookup.scenario.health : lookup.kind, "pinged");
    assert.deepEqual(lookup.kind === "found" ? lookup.scenario.profiles : [], [ISOLATED, SHARED_ASYNC]);
  });

  test("another name is notFound with the name", () => {
    assert.deepEqual(scenarioNamed("no_such_scenario"), { kind: "notFound", name: "no_such_scenario" });
  });
});

describe("the send options", () => {
  test("no key gives no option", () => {
    assert.deepEqual(sendOptions({ kind: "noKey" }), {});
  });

  test("a key gives the idempotency key", () => {
    assert.deepEqual(sendOptions({ kind: "key", key: "parity-key" }), { idempotencyKey: "parity-key" });
  });
});

describe("the read of a stored code", () => {
  test("a user code reads as user and its code", () => {
    assert.deepEqual(storedCode(storedError({ kind: "user", code: "REFUSED" })), { origin: "user", code: "REFUSED" });
  });

  test("a library code reads as library and its code", () => {
    assert.deepEqual(storedCode(storedError({ kind: "library", code: "TASK_TIMEOUT" })), {
      origin: "library",
      code: "TASK_TIMEOUT",
    });
  });

  test("only a failure with the user code REFUSED ended refused", () => {
    assert.equal(endedRefused(REFUSED_OUTCOME), true);
    assert.equal(endedRefused({ kind: "failed", error: storedError({ kind: "user", code: "TRANSIENT" }) }), false);
    assert.equal(endedRefused({ kind: "failed", error: storedError({ kind: "library", code: "WORKER_CRASHED" }) }), false);
    assert.equal(endedRefused({ kind: "succeeded", value: 5 }), false);
  });

  test("the text of an outcome names the origin and the code of a failure", () => {
    assert.equal(outcomeText({ kind: "succeeded", value: 5 }), "succeeded");
    assert.equal(outcomeText(REFUSED_OUTCOME), "failed with the user code REFUSED");
  });
});

describe("the step lines", () => {
  const id = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";

  test("a task line and a workflow line hold the kind, the index and the id", () => {
    assert.deepEqual(JSON.parse(taskLineText(1, id)), { kind: "task", index: 1, id });
    assert.deepEqual(JSON.parse(workflowLineText(0, id)), { kind: "workflow", index: 0, id });
  });

  test("the ping line holds both answer counts", () => {
    assert.deepEqual(JSON.parse(pingLineText(2, 1)), { kind: "ping", index: 0, workers: 2, targeted: 1 });
  });
});

describe("the configs", () => {
  test("the parity app: the pool, the kept rerun input, the profiles and every contract", () => {
    const config = parityConfig("postgresql://postgres@127.0.0.1:55432/parity");

    assert.deepEqual(config.broker, {
      databaseUrl: { kind: "value", value: "postgresql://postgres@127.0.0.1:55432/parity" },
      poolSize: 5,
      maxOverflow: 5,
      retainRerunInputDefault: "keep",
    });
    assert.deepEqual(Object.keys(config.workerProfiles ?? {}), [ISOLATED, SHARED_ASYNC, SHARED_THREAD]);
    assert.equal(config.tasks, TASKS);
    assert.equal(config.workflows, WORKFLOWS);
  });

  test("each profile has one child of its class and the 5 s drain", () => {
    assert.deepEqual(PROFILES[ISOLATED], { isolated: { count: 1 }, drainTimeoutMs: 5_000 });
    assert.deepEqual(PROFILES[SHARED_ASYNC], { sharedAsync: { count: 1, seats: 4 }, drainTimeoutMs: 5_000 });
    assert.deepEqual(PROFILES[SHARED_THREAD], { sharedThread: { count: 1, seats: 4 }, drainTimeoutMs: 5_000 });
  });

  test("the schedule sends parity.add with 2 and 3 every hour in UTC, two catch-up slots per check of 1 s", () => {
    assert.equal(PARITY_SCHEDULE.name, SCHEDULE_NAME);
    assert.equal(PARITY_SCHEDULE.task.name, "parity.add");
    assert.deepEqual(PARITY_SCHEDULE.input, { a: 2, b: 3 });
    assert.equal(PARITY_SCHEDULE.pattern.kind, "interval");
    assert.equal(
      PARITY_SCHEDULE.pattern.kind === "interval" ? PARITY_SCHEDULE.pattern.every.total("hours") : 0,
      1,
    );
    assert.equal(PARITY_SCHEDULE.timezone, "UTC");
    assert.equal(PARITY_SCHEDULE.catchUpMissed, true);
    assert.equal(PARITY_SCHEDULE.maxCatchUpRuns, 2);
    assert.deepEqual(SCHEDULE_SECTION, { checkIntervalSeconds: 1 });
  });

  test("the refused broker app has the parity contracts on port 1 with the short checkout wait", () => {
    const config = refusedBrokerConfig();

    assert.deepEqual(config.broker.databaseUrl, { kind: "value", value: REFUSED_BROKER_URL });
    assert.equal(config.broker.poolTimeoutSeconds, REFUSED_BROKER_CHECKOUT_WAIT_S);
    assert.match(REFUSED_BROKER_URL, /@127\.0\.0\.1:1\//);
    assert.equal(config.tasks, TASKS);
  });
});

describe("the values of the bodies", () => {
  test("the messages of the task errors", () => {
    assert.equal(refusedMessage(7), "refused 7");
    assert.equal(transientMessage(1), "transient 1");
  });

  test("a succeeded upstream doubles; a failed one gives the message", () => {
    assert.deepEqual(doubledValue({ kind: "succeeded", value: 5 }), { ok: true, value: 10 });
    assert.deepEqual(doubledValue(REFUSED_OUTCOME), { ok: false, error: UPSTREAM_FAILED_MESSAGE });
  });
});

describe("the waits of the bodies", () => {
  test("a wait whose signal does not fire ends when its time passed", async () => {
    assert.equal(await waitOrSignal(10, new AbortController().signal), "timePassed");
  });

  test("a wait ends when its signal fires", async () => {
    const controller = new AbortController();
    const started = performance.now();
    const waited = waitOrSignal(60_000, controller.signal);

    setTimeout(() => {
      controller.abort({ cause: "DEADLINE" });
    }, 10);

    assert.equal(await waited, "signalled");
    assert.ok(performance.now() - started < 30_000);
  });

  test("a signal that fired already ends the wait at once", async () => {
    assert.equal(await waitOrSignal(60_000, AbortSignal.abort()), "signalled");
  });

  test("holdThread holds the thread for its time", () => {
    const started = performance.now();

    holdThread(20);

    assert.ok(performance.now() - started >= 19);
  });
});
