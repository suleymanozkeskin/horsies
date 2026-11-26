// The facade calls of a seat body go through the seat relay to the main thread of the child: each kind (a monitoring
// read, a health read, a worker ping, the database ping, the cancel, the rerun) runs the main thread's addon function
// with the main thread's core handle, and the seat call gives that answer as its own `Result`; none throws
// `AddonOutsideMainThread`. Past the bound of the relay, each kind gives `SEAT_RELAY_FULL` through its own error union.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CoreAccess } from "../../../horsies/src/app/core-access.ts";
import { createMonitoringArea, type MonitoringArea } from "../../../horsies/src/monitoring/app-area.ts";
import type { NativeCoreHandle } from "../../../horsies/src/native/handle/addon.ts";
import { rerunAreaOfCore } from "../../../horsies/src/rerun/app-area.ts";
import { type FacadeAddons, serveSeatOperation } from "../../../horsies/src/seats/operations.ts";
import { IsolateRelay } from "../../../horsies/src/seats/relay.ts";
import { fakeHandle, mainThreadAccess, taskIdOf, valueEnvelope } from "../monitoring/support/fakes.ts";
import { COMMAND, POLICY } from "../rerun/support.ts";
import type { Result } from "../../../horsies/src/wire/result.ts";
import { SeatRelayCode } from "../../../horsies/src/_catalog/task_send.ts";
import { TEST_RELAY_BOUNDS } from "./support.ts";

const TASK_ID = taskIdOf("01890a5d-ac96-774b-bcce-b302099a8057");

/** The main thread's facade addons: each logs its name and checks that it got the main thread's handle. */
function mainFacades(handle: NativeCoreHandle, called: string[]): FacadeAddons {
  const json = (name: string) => async (given: NativeCoreHandle) => {
    assert.equal(given, handle);
    called.push(name);

    return valueEnvelope({ kind: "json" as const, text: "[]" });
  };

  const done = <T>(name: string, record: T) => async (given: NativeCoreHandle) => {
    assert.equal(given, handle);
    called.push(name);

    return valueEnvelope({ kind: "done" as const, record });
  };

  const unused = async (): Promise<never> => assert.fail("not called");

  return {
    reads: () => ({
      readTaskStats: json("readTaskStats"),
      readTaskFacets: unused,
      readTaskBreakdown: unused,
      readTaskList: unused,
      readWorkflowNames: json("readWorkflowNames"),
      readWorkflowRuns: unused,
      readWorkflowRun: unused,
      readWorkflowNode: unused,
      readSchedules: json("readSchedules"),
      readTaskDetail: unused,
    }),
    health: () => ({
      pingWorkers: done("pingWorkers", []),
      listWorkerStates: done("listWorkerStates", []),
      getWorkerState: unused,
      getWorkerStateHistory: unused,
      getStaleTasks: unused,
      getExpiredTasks: done("getExpiredTasks", { kind: "complete" as const, rows: [] }),
      getClassCoverage: unused,
      getClassReport: unused,
    }),
    cancel: () => ({
      cancelTask: async (given, taskId) => done("cancelTask", { taskId, wasStatus: "CLAIMED" })(given),
    }),
    rerun: () => ({
      rerunTask: async (given) => {
        assert.equal(given, handle);
        called.push("rerunTask");

        return valueEnvelope({ kind: "done" as const, outcome: { kind: "sourceLive" as const, taskId: TASK_ID } });
      },
    }),
  };
}

/** A relay of a seat isolate whose main thread is this one: it serves each operation on `main` with `facades`. */
function loopbackRelay(main: CoreAccess, facades: FacadeAddons): IsolateRelay {
  const relay: IsolateRelay = new IsolateRelay((message) => {
    if (message.kind === "op") {
      void serveSeatOperation(main, message.request, facades).then((answer) => {
        relay.settle(message.requestId, answer);
      });
    }
  }, TEST_RELAY_BOUNDS);

  return relay;
}

/** The main thread's handle, with a database ping. */
function mainHandle(called: string[]): NativeCoreHandle {
  return {
    ...fakeHandle(),
    pingDatabase: async () => {
      called.push("pingDatabase");

      return valueEnvelope({ kind: "done" as const, record: { latencyMs: 4 } });
    },
  };
}

/** The monitoring area of a seat app on a loopback relay, and the calls of the main thread. */
function seatArea(): { readonly area: MonitoringArea; readonly called: string[]; readonly relay: IsolateRelay } {
  const called: string[] = [];
  const handle = mainHandle(called);
  const relay = loopbackRelay(mainThreadAccess(handle), mainFacades(handle, called));

  return { area: createMonitoringArea({ kind: "seatRelay", relay }), called, relay };
}

describe("each facade call kind of a seat body goes through the relay", () => {
  test("a monitoring read", async () => {
    const { area, called } = seatArea();

    assert.deepEqual(await area.monitoring.taskStats(), { ok: true, value: [] });
    assert.deepEqual(await area.monitoring.listWorkflowNames(), { ok: true, value: [] });
    assert.deepEqual(called, ["readTaskStats", "readWorkflowNames"]);
  });

  test("a health read", async () => {
    const { area, called } = seatArea();

    assert.deepEqual(await area.monitoring.listWorkerStates(), { ok: true, value: [] });
    assert.equal((await area.monitoring.getExpiredTasks()).ok, true);
    assert.deepEqual(called, ["listWorkerStates", "getExpiredTasks"]);
  });

  test("a worker ping", async () => {
    const { area, called } = seatArea();

    assert.deepEqual(await area.pingWorkers(), { ok: true, value: [] });
    assert.deepEqual(called, ["pingWorkers"]);
  });

  test("the database ping", async () => {
    const { area, called } = seatArea();

    assert.deepEqual(await area.pingDatabase(), { ok: true, value: { latencyMs: 4 } });
    assert.deepEqual(called, ["pingDatabase"]);
  });

  test("the cancel", async () => {
    const { area, called } = seatArea();
    const cancelled = await area.monitoring.cancelTask(TASK_ID, { policy: "includeRunning" });

    assert.equal(cancelled.ok, true);
    assert.deepEqual(called, ["cancelTask"]);
  });

  test("the rerun in the app's transaction", async () => {
    const called: string[] = [];
    const handle = mainHandle(called);
    const relay = loopbackRelay(mainThreadAccess(handle), mainFacades(handle, called));
    const rerun = rerunAreaOfCore({ kind: "seatRelay", relay }, () => ({ kind: "noCarrier" }));

    assert.deepEqual(await rerun.rerun(COMMAND, POLICY), { ok: true, value: { kind: "sourceLive", taskId: TASK_ID } });
    assert.deepEqual(called, ["rerunTask"]);
  });
});

/** A seat area whose relay holds its whole waiting bound: task reads that the main thread never answers. */
function fullSeatArea(): { readonly area: MonitoringArea; readonly relay: IsolateRelay } {
  const relay = new IsolateRelay(() => undefined, TEST_RELAY_BOUNDS);

  for (let index = 0; index < TEST_RELAY_BOUNDS.opsInFlight + TEST_RELAY_BOUNDS.opsWaiting; index += 1) {
    void relay.request("sends", { kind: "taskInfo", taskId: TASK_ID });
  }

  return { area: createMonitoringArea({ kind: "seatRelay", relay }), relay };
}

/** The text that names the relay bound and its value. */
const BOUND_TEXT = `its bound (MAX_RUN_WORK = ${String(TEST_RELAY_BOUNDS.opsWaiting)})`;

/** The code and the retryable flag of a failed facade `Result`, and whether its message names the bound. */
function failure<T, E extends { readonly code: string }>(
  result: Result<T, E>,
): { readonly code: string; readonly retryable: boolean; readonly bound: boolean } {
  if (result.ok) {
    return assert.fail("the call past the bound was answered");
  }

  const retryable: unknown = Reflect.get(result.error, "retryable");
  const message: unknown = Reflect.get(result.error, "message");

  return { code: result.error.code, retryable: retryable === true, bound: String(message).includes(BOUND_TEXT) };
}

/** The failure of every call kind past the bound: the one code of a full seat relay. */
const RELAY_FULL = { code: SeatRelayCode.SEAT_RELAY_FULL, retryable: true, bound: true };

describe("each facade call kind past the bound of the relay gives SEAT_RELAY_FULL", () => {
  test("a monitoring read", async () => {
    assert.deepEqual(failure(await fullSeatArea().area.monitoring.taskStats()), RELAY_FULL);
  });

  test("a monitoring read with no query", async () => {
    assert.deepEqual(failure(await fullSeatArea().area.monitoring.listSchedules()), RELAY_FULL);
  });

  test("a health read", async () => {
    assert.deepEqual(failure(await fullSeatArea().area.monitoring.listWorkerStates()), RELAY_FULL);
  });

  test("a worker ping", async () => {
    assert.deepEqual(failure(await fullSeatArea().area.pingWorkers()), RELAY_FULL);
  });

  test("the database ping", async () => {
    assert.deepEqual(failure(await fullSeatArea().area.pingDatabase()), RELAY_FULL);
  });

  test("the cancel", async () => {
    assert.deepEqual(failure(await fullSeatArea().area.monitoring.cancelTask(TASK_ID, { policy: "includeRunning" })), RELAY_FULL);
  });

  test("the rerun in the app's transaction", async () => {
    const { relay } = fullSeatArea();
    const rerun = await rerunAreaOfCore({ kind: "seatRelay", relay }, () => ({ kind: "noCarrier" })).rerun(COMMAND, POLICY);

    assert.deepEqual(failure(rerun), RELAY_FULL);
  });
});
