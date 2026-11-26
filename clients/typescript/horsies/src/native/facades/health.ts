// The health read functions of the `facades` area of the addon, their records, and the accessor.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";
import type { NativeOutcome } from "../values/records.ts";

/** Which workers a ping asks. */
export type PingTargetArg = { readonly kind: "everyWorker" } | { readonly kind: "worker"; readonly workerId: string };

/** When a ping returns before its window ends. */
export type MinResponsesArg = { readonly kind: "fullWindow" } | { readonly kind: "atLeast"; readonly count: number };

/** How many snapshots a worker history read returns. */
export type HistoryLimitArg = { readonly kind: "allRetained" } | { readonly kind: "newest"; readonly count: number };

/** One worker's reply to a ping. */
export interface WorkerPongRecord {
  readonly workerId: string;
  readonly hostname: string;
  readonly pid: number;
  readonly roundTripMs: number;
}

/** An execution class, by its wire value. */
export type ExecutionName = "ISOLATED" | "SHARED_ASYNC" | "SHARED_THREAD";

/** The worker profile that a snapshot names. */
export type SnapshotProfileRecord =
  | { readonly kind: "named"; readonly name: string }
  | { readonly kind: "notRecorded" };

/** The errno of a resource shortage. */
export type ShortageErrnoName = "EMFILE" | "ENFILE" | "ENOMEM" | "EAGAIN";

/** A boot failure cause that can clear by itself. */
export type BootCauseRecord =
  | { readonly kind: "resourceShortage"; readonly errno: ShortageErrnoName }
  | { readonly kind: "threadStartRefused" };

/** A worker slot whose last child boot failed for a cause that can clear. */
export interface DownSlotRecord {
  readonly slot: number;
  readonly cause: BootCauseRecord;
  readonly detail: string;
  readonly downSinceMicros: bigint;
  readonly failedBoots: bigint;
}

/** Whether any slot of the worker was down. */
export type SlotHealthRecord =
  | { readonly kind: "allSlotsServing" }
  | { readonly kind: "slotsDown"; readonly slots: DownSlotRecord[] };

/** One claim budget of a worker. */
export interface ClassBudgetRecord {
  readonly budget: ExecutionName;
  readonly capacity: bigint;
  readonly serves: ExecutionName[];
}

/** One entry of a queue map of a snapshot. */
export interface QueueNumberRecord {
  readonly queue: string;
  readonly value: bigint;
}

/** A measure of a snapshot. */
export type MeasureRecord = { readonly kind: "measured"; readonly value: number } | { readonly kind: "notMeasured" };

/** The cluster-wide cap of a worker's claims. */
export type ClusterCapRecord = { readonly kind: "capped"; readonly cap: number } | { readonly kind: "uncapped" };

/** One worker state snapshot. */
export interface WorkerStateRecord {
  readonly workerId: string;
  readonly snapshotAtMicros: bigint;
  readonly hostname: string;
  readonly pid: number;
  readonly processes: number;
  readonly maxClaimBatch: number;
  readonly maxClaimPerWorker: number;
  readonly clusterWideCap: ClusterCapRecord;
  readonly queues: string[];
  readonly queuePriorities: QueueNumberRecord[];
  readonly queueMaxConcurrency: QueueNumberRecord[];
  readonly recoveryConfigJson: string;
  readonly tasksRunning: number;
  readonly tasksClaimed: number;
  readonly memoryUsageMb: MeasureRecord;
  readonly memoryPercent: MeasureRecord;
  readonly cpuPercent: MeasureRecord;
  readonly childrenMemoryMb: MeasureRecord;
  readonly workerStartedAtMicros: bigint;
  readonly slotHealth: SlotHealthRecord;
  readonly classCapacity: ClassBudgetRecord[];
  readonly profile: SnapshotProfileRecord;
}

/** The latest snapshot of one worker, or none. */
export type WorkerStateRead =
  | { readonly kind: "found"; readonly snapshot: WorkerStateRecord }
  | { readonly kind: "notFound" };

/** A text column of a stale task that may hold no value. */
export type RecordedTextRecord =
  | { readonly kind: "recorded"; readonly value: string }
  | { readonly kind: "notRecorded" };

/** The process id column of a stale task. */
export type RecordedPidRecord =
  | { readonly kind: "recorded"; readonly value: number }
  | { readonly kind: "notRecorded" };

/** The latest runner beat of a stale task since its start. */
export type LastBeatRecord =
  | { readonly kind: "beat"; readonly atMicros: bigint }
  | { readonly kind: "noBeatSinceStart" };

/** A `RUNNING` task whose latest beat, or its start, is older than the threshold. */
export interface StaleTaskRecord {
  readonly id: string;
  readonly taskName: string;
  readonly workerHostname: RecordedTextRecord;
  readonly workerPid: RecordedPidRecord;
  readonly workerProcessName: RecordedTextRecord;
  readonly startedAtMicros: bigint;
  readonly lastHeartbeat: LastBeatRecord;
}

/** A `PENDING` task past its `good_until`. */
export interface ExpiredTaskRecord {
  readonly id: string;
  readonly taskName: string;
  readonly queueName: string;
  readonly priority: number;
  readonly sentAtMicros: bigint;
  readonly enqueuedAtMicros: bigint;
  readonly goodUntilMicros: bigint;
  readonly expiredForMicros: bigint;
}

/** The stale task list, and whether it stopped at its bound. */
export type StaleTaskList =
  | { readonly kind: "complete"; readonly rows: StaleTaskRecord[] }
  | { readonly kind: "truncated"; readonly rows: StaleTaskRecord[]; readonly bound: bigint };

/** The expired task list, and whether it stopped at its bound. */
export type ExpiredTaskList =
  | { readonly kind: "complete"; readonly rows: ExpiredTaskRecord[] }
  | { readonly kind: "truncated"; readonly rows: ExpiredTaskRecord[]; readonly bound: bigint };

/** Whether live workers serve a pending stream. */
export type StreamStateRecord =
  | { readonly kind: "served"; readonly workers: bigint; readonly capacity: bigint }
  | { readonly kind: "withoutCapacity"; readonly workers: bigint }
  | { readonly kind: "unserved" };

/** The coverage of one pending stream. */
export interface StreamCoverageRecord {
  readonly queueName: string;
  readonly execution: ExecutionName;
  readonly state: StreamStateRecord;
}

/** The fleet check. */
export interface ClassCoverageRecord {
  readonly checkedAtMicros: bigint;
  readonly liveWorkers: bigint;
  readonly streams: StreamCoverageRecord[];
}

/** How long the rows of a pending stream wait. */
export type StreamWaitRecord =
  | { readonly kind: "oldestWaiting"; readonly enqueuedAtMicros: bigint; readonly waitMicros: bigint }
  | { readonly kind: "noneDue"; readonly nextDueAtMicros: bigint };

/** One pending stream. */
export interface PendingStreamRecord {
  readonly queueName: string;
  readonly execution: ExecutionName;
  readonly wait: StreamWaitRecord;
  readonly state: StreamStateRecord;
}

/** One claim budget of a live worker, with its rows in flight. */
export interface BudgetLoadRecord {
  readonly budget: ExecutionName;
  readonly capacity: bigint;
  readonly serves: ExecutionName[];
  readonly inFlight: bigint;
}

/** The claim budgets of one live worker. */
export interface WorkerClassLoadRecord {
  readonly workerId: string;
  readonly snapshotAtMicros: bigint;
  readonly budgets: BudgetLoadRecord[];
}

/** The class report. */
export interface ClassReportRecord {
  readonly checkedAtMicros: bigint;
  readonly streams: PendingStreamRecord[];
  readonly workers: WorkerClassLoadRecord[];
}

/** A health read's Promise of its envelope. */
type HealthAnswer<T> = Promise<NativeEnvelope<NativeOutcome<T>>>;

/** The health read functions of the addon. Each takes the handle first. */
export interface HealthAddon {
  pingWorkers(
    handle: NativeCoreHandle,
    target: PingTargetArg,
    waitMs: number,
    minResponses: MinResponsesArg,
  ): HealthAnswer<WorkerPongRecord[]>;
  listWorkerStates(handle: NativeCoreHandle): HealthAnswer<WorkerStateRecord[]>;
  getWorkerState(handle: NativeCoreHandle, workerId: string): HealthAnswer<WorkerStateRead>;
  getWorkerStateHistory(
    handle: NativeCoreHandle,
    workerId: string,
    limit: HistoryLimitArg,
  ): HealthAnswer<WorkerStateRecord[]>;
  getStaleTasks(handle: NativeCoreHandle, thresholdMinutes: number): HealthAnswer<StaleTaskList>;
  getExpiredTasks(handle: NativeCoreHandle): HealthAnswer<ExpiredTaskList>;
  getClassCoverage(handle: NativeCoreHandle): HealthAnswer<ClassCoverageRecord>;
  getClassReport(handle: NativeCoreHandle): HealthAnswer<ClassReportRecord>;
}

/** The health read functions of the addon. Throws as `requireAddon` does. */
export function healthAddon(): HealthAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of the facades area
  // compares this interface with the declarations that the native build writes.
  return requireAddon() as HealthAddon;
}
