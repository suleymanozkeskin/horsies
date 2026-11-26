// The health reads and the pings with a fake addon: each record becomes its frozen value, each read gives only the
// broker codes that it lists, and a worker id that is not well-formed text is refused before the call.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { NativeEnvelope } from "../../../horsies/src/native/call/envelope.ts";
import type {
  ClassCoverageRecord,
  ClassReportRecord,
  ExpiredTaskList,
  HealthAddon,
  StaleTaskList,
  WorkerStateRecord,
} from "../../../horsies/src/native/facades/health.ts";
import type { NativeCoreHandle } from "../../../horsies/src/native/handle/addon.ts";
import type { NativeOutcome, RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import {
  expiredTasks,
  type HealthTarget,
  pingDatabase,
  pingWorkers,
  readClassCoverage,
  readClassReport,
  staleTasks,
  worker,
  workerHistory,
  workers,
} from "../../../horsies/src/monitoring/health.ts";
import {
  brokerInvalidInput,
  brokerListenerDisabled,
  brokerOperationFailed,
  fakeHandle,
  valueEnvelope,
} from "./support/fakes.ts";

const MICROS = 1_795_608_000_123_456n;
const INSTANT = "2026-11-25T12:00:00.123456Z";

const SNAPSHOT: WorkerStateRecord = {
  workerId: "w1",
  snapshotAtMicros: MICROS,
  hostname: "host",
  pid: 41,
  processes: 4,
  maxClaimBatch: 2,
  maxClaimPerWorker: 0,
  clusterWideCap: { kind: "uncapped" },
  queues: ["default"],
  queuePriorities: [{ queue: "default", value: 100n }],
  queueMaxConcurrency: [{ queue: "default", value: 9_007_199_254_740_993n }],
  recoveryConfigJson: '{"reaper": {"ok": true, "runs": 9007199254740993}}',
  tasksRunning: 1,
  tasksClaimed: 0,
  memoryUsageMb: { kind: "measured", value: 64.5 },
  memoryPercent: { kind: "notMeasured" },
  cpuPercent: { kind: "measured", value: 3 },
  childrenMemoryMb: { kind: "notMeasured" },
  workerStartedAtMicros: MICROS,
  slotHealth: {
    kind: "slotsDown",
    slots: [
      {
        slot: 1,
        cause: { kind: "resourceShortage", errno: "EMFILE" },
        detail: "too many files",
        downSinceMicros: MICROS,
        failedBoots: 2n,
      },
    ],
  },
  classCapacity: [{ budget: "ISOLATED", capacity: 4n, serves: ["ISOLATED"] }],
  profile: { kind: "named", name: "default" },
};

/** The answer of one fake function: a record, or a raw failure. */
function done<T>(record: T): Promise<NativeEnvelope<NativeOutcome<T>>> {
  return valueEnvelope<NativeOutcome<T>>({ kind: "done", record });
}

function failed<T>(error: RawCatalogError): Promise<NativeEnvelope<NativeOutcome<T>>> {
  return valueEnvelope<NativeOutcome<T>>({ kind: "failed", error });
}

/** A fake addon whose functions answer as `answers` say, else with an empty success. */
function fakeTarget(answers: Partial<HealthAddon> = {}): HealthTarget {
  const base: HealthAddon = {
    pingWorkers: async () => done([]),
    listWorkerStates: async () => done([SNAPSHOT]),
    getWorkerState: async () => done({ kind: "notFound" }),
    getWorkerStateHistory: async () => done([SNAPSHOT]),
    getStaleTasks: async () => done<StaleTaskList>({ kind: "complete", rows: [] }),
    getExpiredTasks: async () => done<ExpiredTaskList>({ kind: "complete", rows: [] }),
    getClassCoverage: async () => done<ClassCoverageRecord>({ checkedAtMicros: MICROS, liveWorkers: 1n, streams: [] }),
    getClassReport: async () => done<ClassReportRecord>({ checkedAtMicros: MICROS, streams: [], workers: [] }),
  };

  return { addon: { ...base, ...answers }, handle: fakeHandle() };
}

describe("the worker state reads", () => {
  test("workers gives frozen snapshots with instants, maps and the recovery config", async () => {
    const read = await workers(fakeTarget());

    assert.ok(read.ok);

    const [snapshot] = read.value;

    assert.ok(snapshot !== undefined && Object.isFrozen(snapshot));
    assert.equal(snapshot.snapshotAt.toString(), INSTANT);
    assert.equal(snapshot.queueMaxConcurrency.get("default"), 9_007_199_254_740_993n);
    assert.deepEqual(snapshot.recoveryConfig, { reaper: { ok: true, runs: 9_007_199_254_740_993n } });
    assert.deepEqual(snapshot.profile, { kind: "named", name: "default" });
    assert.ok(snapshot.slotHealth.kind === "slotsDown" && snapshot.slotHealth.slots[0]?.downSince.toString() === INSTANT);
    assert.deepEqual(snapshot.memoryPercent, { kind: "notMeasured" });
  });

  test("a recovery config that is not a JSON object is a defect", async () => {
    const target = fakeTarget({ listWorkerStates: async () => done([{ ...SNAPSHOT, recoveryConfigJson: "[]" }]) });

    await assert.rejects(workers(target), { name: "ClientDefect", subject: "recoveryConfig" });
  });

  test("worker found and not found", async () => {
    const found = await worker(fakeTarget({ getWorkerState: async () => done({ kind: "found", snapshot: SNAPSHOT }) }), "w1");
    const missing = await worker(fakeTarget(), "w2");

    assert.ok(found.ok && found.value.kind === "found" && found.value.snapshot.workerId === "w1");
    assert.deepEqual(missing, { ok: true, value: { kind: "notFound", workerId: "w2" } });
  });

  test("a worker id that is not well-formed text is QUERY_REFUSED before the call", async () => {
    const target = fakeTarget({ getWorkerState: async () => assert.fail("the addon is not called") });
    const read = await worker(target, "w\uDC00");

    assert.ok(!read.ok && read.error.code === "QUERY_REFUSED" && read.error.field === "workerId");
  });

  test("workerHistory passes its limit and gives INVALID_INPUT of the core", async () => {
    const seen: Array<object> = [];

    const target = fakeTarget({
      getWorkerStateHistory: async (_handle: NativeCoreHandle, _workerId: string, limit) => {
        seen.push(limit);

        return failed(brokerInvalidInput("limit must be positive when set, got 0"));
      },
    });

    const read = await workerHistory(target, "w1", { kind: "newest", count: 0 });

    assert.deepEqual(seen, [{ kind: "newest", count: 0 }]);
    assert.ok(!read.ok && read.error.code === "INVALID_INPUT");
  });
});

describe("each read gives only its broker codes", () => {
  test("a failed health read is MONITORING_QUERY_FAILED with its retry rule", async () => {
    const read = await workers(fakeTarget({ listWorkerStates: async () => failed(brokerOperationFailed("MONITORING_QUERY_FAILED", "list_worker_states")) }));

    assert.ok(!read.ok && read.error.code === "MONITORING_QUERY_FAILED" && read.error.retryable);
  });

  test("a code that a read cannot give is a defect", async () => {
    const target = fakeTarget({ listWorkerStates: async () => failed(brokerOperationFailed("DB_PING_FAILED", "list_worker_states")) });

    await assert.rejects(workers(target), { name: "ClientDefect", subject: "workers" });
  });

  test("staleTasks gives INVALID_INPUT for a threshold that the core refuses", async () => {
    const target = fakeTarget({ getStaleTasks: async () => failed(brokerInvalidInput("stale_threshold_minutes is below 0")) });
    const read = await staleTasks(target, -1);

    assert.ok(!read.ok && read.error.code === "INVALID_INPUT");
  });
});

describe("the task lists", () => {
  test("a stale list keeps its rows and a missing column is notRecorded", async () => {
    const target = fakeTarget({
      getStaleTasks: async () =>
        done<StaleTaskList>({
          kind: "truncated",
          bound: 10_000n,
          rows: [
            {
              id: "01890a5d-ac96-774b-bcce-b302099a8057",
              taskName: "resize",
              workerHostname: { kind: "notRecorded" },
              workerPid: { kind: "recorded", value: 7 },
              workerProcessName: { kind: "recorded", value: "child" },
              startedAtMicros: MICROS,
              lastHeartbeat: { kind: "noBeatSinceStart" },
            },
          ],
        }),
    });

    const read = await staleTasks(target, 5);

    assert.ok(read.ok && read.value.kind === "truncated");
    assert.equal(read.value.bound, 10_000);
    assert.deepEqual(read.value.rows[0]?.workerHostname, { kind: "notRecorded" });
    assert.deepEqual(read.value.rows[0]?.lastHeartbeat, { kind: "noBeatSinceStart" });
  });

  test("an expired row gives its interval as a duration with every microsecond", async () => {
    const target = fakeTarget({
      getExpiredTasks: async () =>
        done<ExpiredTaskList>({
          kind: "complete",
          rows: [
            {
              id: "01890a5d-ac96-774b-bcce-b302099a8058",
              taskName: "send",
              queueName: "default",
              priority: 100,
              sentAtMicros: MICROS,
              enqueuedAtMicros: MICROS,
              goodUntilMicros: MICROS,
              expiredForMicros: 3_600_000_001n,
            },
          ],
        }),
    });

    const read = await expiredTasks(target);

    assert.ok(read.ok && read.value.kind === "complete");
    assert.equal(read.value.rows[0]?.expiredFor.total({ unit: "microseconds" }), 3_600_000_001);
  });
});

describe("the class reads", () => {
  test("the coverage and the report give their instants and states", async () => {
    const target = fakeTarget({
      getClassCoverage: async () =>
        done<ClassCoverageRecord>({
          checkedAtMicros: MICROS,
          liveWorkers: 2n,
          streams: [{ queueName: "q", execution: "SHARED_ASYNC", state: { kind: "served", workers: 2n, capacity: 8n } }],
        }),
      getClassReport: async () =>
        done<ClassReportRecord>({
          checkedAtMicros: MICROS,
          streams: [
            {
              queueName: "q",
              execution: "ISOLATED",
              wait: { kind: "oldestWaiting", enqueuedAtMicros: MICROS, waitMicros: 1_500_000n },
              state: { kind: "unserved" },
            },
          ],
          workers: [{ workerId: "w1", snapshotAtMicros: MICROS, budgets: [{ budget: "ISOLATED", capacity: 4n, serves: ["ISOLATED"], inFlight: 1n }] }],
        }),
    });

    const coverage = await readClassCoverage(target);
    const report = await readClassReport(target);

    assert.ok(coverage.ok && coverage.value.checkedAt.toString() === INSTANT);
    assert.deepEqual(coverage.value.streams[0]?.state, { kind: "served", workers: 2n, capacity: 8n });
    assert.ok(report.ok);

    const wait = report.value.streams[0]?.wait;

    assert.ok(wait?.kind === "oldestWaiting" && wait.wait.total({ unit: "milliseconds" }) === 1_500);
    assert.equal(report.value.workers[0]?.budgets[0]?.inFlight, 1n);
  });
});

describe("the pings", () => {
  test("pingDatabase gives the latency, or DB_PING_FAILED", async () => {
    const handle = fakeHandle();
    const answered: NativeCoreHandle = { ...handle, pingDatabase: async () => done({ latencyMs: 1.5 }) };

    const refused: NativeCoreHandle = {
      ...handle,
      pingDatabase: async () => failed(brokerOperationFailed("DB_PING_FAILED", "ping_database")),
    };

    assert.deepEqual(await pingDatabase(async () => answered.pingDatabase()), { ok: true, value: { latencyMs: 1.5 } });

    const failure = await pingDatabase(async () => refused.pingDatabase());

    assert.ok(!failure.ok && failure.error.code === "DB_PING_FAILED");
  });

  test("pingWorkers passes the defaults of the Python client", async () => {
    const seen: Array<readonly [object, number, object]> = [];

    const target = fakeTarget({
      pingWorkers: async (_handle, pingTarget, timeoutMs, minResponses) => {
        seen.push([pingTarget, timeoutMs, minResponses]);

        return done([{ workerId: "w1", hostname: "h", pid: 1, roundTripMs: 2 }]);
      },
    });

    const read = await pingWorkers(target, {});

    assert.deepEqual(seen, [[{ kind: "everyWorker" }, 2_000, { kind: "fullWindow" }]]);
    assert.deepEqual(read, { ok: true, value: [{ workerId: "w1", hostname: "h", pid: 1, roundTripMs: 2 }] });
  });

  test("pingWorkers gives INVALID_INPUT, LISTENER_DISABLED and WORKER_PING_FAILED", async () => {
    const answers: ReadonlyArray<readonly [RawCatalogError, string]> = [
      [brokerInvalidInput("timeoutMs must be a whole number from 1"), "INVALID_INPUT"],
      [brokerListenerDisabled(), "LISTENER_DISABLED"],
      [brokerOperationFailed("WORKER_PING_FAILED", "ping_workers"), "WORKER_PING_FAILED"],
    ];

    for (const [raw, code] of answers) {
      const read = await pingWorkers(fakeTarget({ pingWorkers: async () => failed(raw) }), { timeoutMs: 0 });

      assert.ok(!read.ok && read.error.code === code, code);
    }
  });

  test("a target worker id that is not well-formed text is QUERY_REFUSED before the call", async () => {
    const target = fakeTarget({ pingWorkers: async () => assert.fail("the addon is not called") });
    const read = await pingWorkers(target, { target: { kind: "worker", workerId: "\uD800" } });

    assert.ok(!read.ok && read.error.code === "QUERY_REFUSED" && read.error.field === "workerId");
  });
});
