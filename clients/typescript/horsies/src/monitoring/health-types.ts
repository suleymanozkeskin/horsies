// The values of the health reads and the pings: the names of the Python and Rust clients, in TypeScript form.
//
// - A value that may be missing is a variant that names the missing state, not `null`.
// - A count of 64 bits is a `bigint`; an instant is a `Temporal.Instant`; a length of time is a `Temporal.Duration`.

import type { ShortageErrno } from "../_catalog/events.ts";
import type { Execution } from "../_catalog/monitoring_models.ts";
import type { JsonValue } from "../codec/json-value.ts";
import type { TaskId } from "../contract/ids.ts";
import type { Recorded } from "../read/info.ts";

/** One round trip of a database ping. */
export interface DatabasePing {
  readonly latencyMs: number;
}

/** One worker's reply to a ping. */
export interface WorkerPong {
  readonly workerId: string;
  readonly hostname: string;
  readonly pid: number;
  readonly roundTripMs: number;
}

/** Which workers a ping asks: every worker that listens, or one worker (the ping returns when it replies). */
export type PingTarget = { readonly kind: "everyWorker" } | { readonly kind: "worker"; readonly workerId: string };

/** When a ping returns before its window ends: never, or when `count` workers replied (1 or more). */
export type MinResponses = { readonly kind: "fullWindow" } | { readonly kind: "atLeast"; readonly count: number };

/** The options of a worker ping. The defaults: every worker, a window of 2,000 ms, the full window. */
export interface PingWorkersOptions {
  readonly target?: PingTarget;
  /** The window of the ping, a whole number of milliseconds above 0. */
  readonly timeoutMs?: number;
  readonly minResponses?: MinResponses;
}

/** How many snapshots a worker history read gives: every retained snapshot, or the newest `count` (1 or more). */
export type WorkerHistoryLimit = { readonly kind: "allRetained" } | { readonly kind: "newest"; readonly count: number };

/** The worker profile that a snapshot names. `notRecorded`: a worker of a build before schema migration 1004. */
export type SnapshotProfile = { readonly kind: "named"; readonly name: string } | { readonly kind: "notRecorded" };

// The errno of a resource shortage: the type of the generated event catalog, so the main entry point has one
// `ShortageErrno` for the health reads and the events.
export type { ShortageErrno };

/** A boot failure cause that can clear by itself. */
export type ClearableBootCause =
  | { readonly kind: "resourceShortage"; readonly errno: ShortageErrno }
  | { readonly kind: "threadStartRefused" };

/** A worker slot whose last child boot failed for a cause that can clear. */
export interface DownSlot {
  readonly slot: number;
  readonly cause: ClearableBootCause;
  readonly detail: string;
  readonly downSince: Temporal.Instant;
  readonly failedBoots: bigint;
}

/** Whether any slot of the worker was down when it wrote the snapshot. */
export type SlotHealth =
  | { readonly kind: "allSlotsServing" }
  | { readonly kind: "slotsDown"; readonly slots: readonly DownSlot[] };

/** One claim budget of a worker: its capacity in service and the classes whose rows it claims. */
export interface ClassBudget {
  readonly budget: Execution;
  readonly capacity: bigint;
  readonly serves: readonly Execution[];
}

/** A measure of a snapshot (memory or CPU), or the state that the worker could not take it. */
export type Measure = { readonly kind: "measured"; readonly value: number } | { readonly kind: "notMeasured" };

/** The cluster-wide cap of a worker's claims. */
export type ClusterWideCap = { readonly kind: "capped"; readonly cap: number } | { readonly kind: "uncapped" };

/**
 * The recovery settings and the health of a worker's maintenance passes, as the worker wrote them in its snapshot:
 * untyped JSON. The config input of the recovery settings is `RecoveryConfigInput`.
 */
export type RecoveryConfigSnapshot = { readonly [key: string]: JsonValue };

/** One worker state snapshot. */
export interface WorkerStateSnapshot {
  readonly workerId: string;
  readonly snapshotAt: Temporal.Instant;
  readonly hostname: string;
  readonly pid: number;
  readonly processes: number;
  readonly maxClaimBatch: number;
  readonly maxClaimPerWorker: number;
  readonly clusterWideCap: ClusterWideCap;
  readonly queues: readonly string[];
  readonly queuePriorities: ReadonlyMap<string, bigint>;
  readonly queueMaxConcurrency: ReadonlyMap<string, bigint>;
  readonly recoveryConfig: RecoveryConfigSnapshot;
  readonly tasksRunning: number;
  readonly tasksClaimed: number;
  /** Resident memory of the worker process only, in MB. */
  readonly memoryUsageMb: Measure;
  readonly memoryPercent: Measure;
  readonly cpuPercent: Measure;
  /** Summed resident memory of the worker's children, in MB. */
  readonly childrenMemoryMb: Measure;
  readonly workerStartedAt: Temporal.Instant;
  readonly slotHealth: SlotHealth;
  readonly classCapacity: readonly ClassBudget[];
  readonly profile: SnapshotProfile;
}

/** The latest snapshot of one worker, or `notFound`: it never wrote one, or retention removed its rows. */
export type WorkerStateRead =
  | { readonly kind: "found"; readonly snapshot: WorkerStateSnapshot }
  | { readonly kind: "notFound"; readonly workerId: string };

/** The latest runner beat of a task since its start: the start writes no beat. */
export type LastBeat = { readonly kind: "beat"; readonly at: Temporal.Instant } | { readonly kind: "noBeatSinceStart" };

/** A `RUNNING` task whose latest beat, or its start, is older than the threshold. */
export interface StaleTaskRow {
  readonly id: TaskId;
  readonly taskName: string;
  readonly workerHostname: Recorded<string>;
  readonly workerPid: Recorded<number>;
  readonly workerProcessName: Recorded<string>;
  readonly startedAt: Temporal.Instant;
  readonly lastHeartbeat: LastBeat;
}

/** A `PENDING` task past its `goodUntil`: no worker claims it. */
export interface ExpiredTaskRow {
  readonly id: TaskId;
  readonly taskName: string;
  readonly queueName: string;
  readonly priority: number;
  readonly sentAt: Temporal.Instant;
  readonly enqueuedAt: Temporal.Instant;
  readonly goodUntil: Temporal.Instant;
  /** How long ago `goodUntil` passed, on the database clock. */
  readonly expiredFor: Temporal.Duration;
}

/** A task list, and whether it stopped at its bound: `truncated` holds the first `bound` rows, and more rows match. */
export type BoundedTaskList<T> =
  | { readonly kind: "complete"; readonly rows: readonly T[] }
  | { readonly kind: "truncated"; readonly rows: readonly T[]; readonly bound: number };

/** Whether live workers serve a pending stream. */
export type StreamCoverageState =
  | { readonly kind: "served"; readonly workers: bigint; readonly capacity: bigint }
  | { readonly kind: "withoutCapacity"; readonly workers: bigint }
  | { readonly kind: "unserved" };

/** The coverage of one pending stream: the `PENDING` rows of one queue and one class. */
export interface StreamCoverage {
  readonly queueName: string;
  readonly execution: Execution;
  readonly state: StreamCoverageState;
}

/** The fleet check: the coverage of every pending stream, read at `checkedAt` (database time). */
export interface ClassCoverage {
  readonly checkedAt: Temporal.Instant;
  readonly liveWorkers: bigint;
  readonly streams: readonly StreamCoverage[];
}

/** How long the rows of a pending stream wait. */
export type StreamWait =
  | { readonly kind: "oldestWaiting"; readonly enqueuedAt: Temporal.Instant; readonly wait: Temporal.Duration }
  | { readonly kind: "noneDue"; readonly nextDueAt: Temporal.Instant };

/** One pending stream: how long its rows wait, and whether a live worker serves it. */
export interface PendingStream {
  readonly queueName: string;
  readonly execution: Execution;
  readonly wait: StreamWait;
  readonly state: StreamCoverageState;
}

/** One claim budget of a live worker, with its rows in flight. */
export interface BudgetLoad {
  readonly budget: Execution;
  readonly capacity: bigint;
  readonly serves: readonly Execution[];
  readonly inFlight: bigint;
}

/** The claim budgets of one live worker. */
export interface WorkerClassLoad {
  readonly workerId: string;
  readonly snapshotAt: Temporal.Instant;
  readonly budgets: readonly BudgetLoad[];
}

/** The class report: pending streams and the budget load of each live worker, read at `checkedAt`. */
export interface ClassReport {
  readonly checkedAt: Temporal.Instant;
  readonly streams: readonly PendingStream[];
  readonly workers: readonly WorkerClassLoad[];
}

/** The task that a cancel moved to `CANCELLED`, and the status that it left. */
export interface TaskCancelled {
  readonly taskId: TaskId;
  readonly wasStatus: "PENDING" | "CLAIMED" | "RUNNING";
}
