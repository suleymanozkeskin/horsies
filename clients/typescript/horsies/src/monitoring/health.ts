// The health reads and the pings.
//
// - Each health read connects the handle and installs the schema once per handle, as a send does; then it reads only.
//   The database ping reads only.
// - Expected failures are values: the broker codes that each read lists (`errors.ts`), or `QUERY_REFUSED` for a
//   worker id that is not well-formed text. A count or threshold outside its rule is the core's `INVALID_INPUT`,
//   answered before any connect. In a `SHARED_THREAD` body, `SEAT_RELAY_FULL` when the seat relay sent nothing.

import type { NativeEnvelope } from "../native/call/envelope.ts";
import { nativeValueAsync } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import type { HealthAddon, PingTargetArg } from "../native/facades/health.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import type { NativeOutcome, RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull, SeatRelayFullOutcome } from "../seats/relay-full.ts";
import { err, ok, type Result } from "../wire/result.ts";
import {
  type HealthInputError,
  healthInputError,
  type HealthReadError,
  healthReadError,
  type PingDatabaseError,
  pingDatabaseError,
  type PingWorkersError,
  pingWorkersError,
  queryRefused,
  type QueryRefused,
  type WorkerHistoryError,
  type WorkerReadError,
} from "./errors.ts";
import type {
  BoundedTaskList,
  ClassCoverage,
  ClassReport,
  DatabasePing,
  ExpiredTaskRow,
  PingWorkersOptions,
  StaleTaskRow,
  WorkerHistoryLimit,
  WorkerPong,
  WorkerStateRead,
  WorkerStateSnapshot,
} from "./health-types.ts";
import { classCoverage, classReport, expiredTaskList, staleTaskList, workerStateSnapshot } from "./health-values.ts";
import type { RelayedAddon, RelayedPing } from "./seat-calls.ts";

/** The window of a worker ping when the options name none: the default of the Python client. */
export const DEFAULT_PING_TIMEOUT_MS = 2_000;

/** The addon and the handle that the health reads call. */
export interface HealthTarget {
  readonly addon: RelayedAddon<HealthAddon>;
  readonly handle: NativeCoreHandle;
}

/** A timed `SELECT 1` round trip: the latency of the database. `ping` is the database ping of the app's core handle. */
export async function pingDatabase(ping: RelayedPing): Promise<Result<DatabasePing, PingDatabaseError>> {
  return healthCall(
    "pingDatabase",
    ping,
    (record) => Object.freeze({ ...record }),
    pingDatabaseError,
  );
}

/** Ping the workers and collect the distinct replies by worker id. A worker that did not reply is not listed. */
export async function pingWorkers(
  target: HealthTarget,
  options: PingWorkersOptions,
): Promise<Result<readonly WorkerPong[], PingWorkersError>> {
  const pingTarget = pingTargetArg(options);

  if (!pingTarget.ok) {
    return pingTarget;
  }

  const timeoutMs = options.timeoutMs ?? DEFAULT_PING_TIMEOUT_MS;
  const minResponses = options.minResponses ?? { kind: "fullWindow" };

  return healthCall(
    "pingWorkers",
    () => target.addon.pingWorkers(target.handle, pingTarget.value, timeoutMs, minResponses),
    (records) => Object.freeze(records.map((record) => Object.freeze({ ...record }))),
    pingWorkersError,
  );
}

/** The latest snapshot of every worker that wrote one, idle workers included. */
export async function workers(target: HealthTarget): Promise<Result<readonly WorkerStateSnapshot[], HealthReadError>> {
  return healthCall(
    "workers",
    () => target.addon.listWorkerStates(target.handle),
    (records) => Object.freeze(records.map(workerStateSnapshot)),
    (raw) => healthReadError("workers", raw),
  );
}

/** The latest snapshot of one worker, or `notFound`. */
export async function worker(
  target: HealthTarget,
  workerId: string,
): Promise<Result<WorkerStateRead, WorkerReadError>> {
  const id = workerIdText(workerId);

  if (!id.ok) {
    return id;
  }

  return healthCall(
    "worker",
    () => target.addon.getWorkerState(target.handle, id.value),
    (read): WorkerStateRead => {
      switch (read.kind) {
        case "found":
          return Object.freeze({ kind: "found", snapshot: workerStateSnapshot(read.snapshot) });
        case "notFound":
          return Object.freeze({ kind: "notFound", workerId: id.value });
        default:
          return unreachable(read);
      }
    },
    (raw) => healthReadError("worker", raw),
  );
}

/** The snapshots of one worker, newest first. */
export async function workerHistory(
  target: HealthTarget,
  workerId: string,
  limit: WorkerHistoryLimit,
): Promise<Result<readonly WorkerStateSnapshot[], WorkerHistoryError>> {
  const id = workerIdText(workerId);

  if (!id.ok) {
    return id;
  }

  return healthCall(
    "workerHistory",
    () => target.addon.getWorkerStateHistory(target.handle, id.value, limit),
    (records) => Object.freeze(records.map(workerStateSnapshot)),
    (raw) => healthInputError("workerHistory", raw),
  );
}

/**
 * The `RUNNING` tasks whose latest runner beat, or their start, is older than `thresholdMinutes` (whole minutes, 0 to
 * 525,600); at most 10,000 rows. Reads only: the reaper recovers stale tasks.
 */
export async function staleTasks(
  target: HealthTarget,
  thresholdMinutes: number,
): Promise<Result<BoundedTaskList<StaleTaskRow>, HealthInputError>> {
  return healthCall(
    "staleTasks",
    () => target.addon.getStaleTasks(target.handle, thresholdMinutes),
    staleTaskList,
    (raw) => healthInputError("staleTasks", raw),
  );
}

/** The `PENDING` tasks past their `goodUntil`, oldest deadline first; at most 10,000 rows. */
export async function expiredTasks(
  target: HealthTarget,
): Promise<Result<BoundedTaskList<ExpiredTaskRow>, HealthReadError>> {
  return healthCall(
    "expiredTasks",
    () => target.addon.getExpiredTasks(target.handle),
    expiredTaskList,
    (raw) => healthReadError("expiredTasks", raw),
  );
}

/** For each pending stream of an execution class, whether a live worker can run it. */
export async function readClassCoverage(target: HealthTarget): Promise<Result<ClassCoverage, HealthReadError>> {
  return healthCall(
    "classCoverage",
    () => target.addon.getClassCoverage(target.handle),
    classCoverage,
    (raw) => healthReadError("classCoverage", raw),
  );
}

/** The pending streams and the budget load of each live worker. */
export async function readClassReport(target: HealthTarget): Promise<Result<ClassReport, HealthReadError>> {
  return healthCall(
    "classReport",
    () => target.addon.getClassReport(target.handle),
    classReport,
    (raw) => healthReadError("classReport", raw),
  );
}

/** Call the addon once and give the converted record or the read's error. */
async function healthCall<Raw, Value, Failure>(
  read: string,
  answer: () => Promise<NativeEnvelope<NativeOutcome<Raw> | SeatRelayFullOutcome>>,
  convert: (record: Raw) => Value,
  failure: (raw: RawCatalogError) => Failure,
): Promise<Result<Value, Failure | SeatRelayFull>> {
  const outcome = await nativeValueAsync(read, answer);

  switch (outcome.kind) {
    case "done":
      return ok(convert(outcome.record));
    case "failed":
      return err(failure(outcome.error));
    case "seatRelayFull":
      return err(outcome.error);
    default:
      return unreachable(outcome);
  }
}

/** The addon target of a ping, or the refusal of a worker id that is not well-formed text. Pure. */
function pingTargetArg(options: PingWorkersOptions): Result<PingTargetArg, QueryRefused> {
  const target = options.target ?? { kind: "everyWorker" };

  switch (target.kind) {
    case "everyWorker":
      return ok(target);
    case "worker": {
      const id = workerIdText(target.workerId);

      return id.ok ? ok({ kind: "worker", workerId: id.value }) : id;
    }

    default:
      return unreachable(target);
  }
}

/** A worker id, or the refusal of text that is not well-formed. Pure. */
function workerIdText(workerId: string): Result<string, QueryRefused> {
  return workerId.isWellFormed()
    ? ok(workerId)
    : err(queryRefused("workerId", "the worker id is not well-formed text (it holds a lone surrogate)"));
}
