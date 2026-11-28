// The frozen values of the health records that the addon gives. Pure.

import type {
  BudgetLoadRecord,
  ClaimLimitsRecord,
  DownSlotRecord,
  ExecutionClassCoverageRecord,
  ExecutionClassReportRecord,
  ExpiredTaskList,
  ExpiredTaskRecord,
  LastBeatRecord,
  MeasureRecord,
  PendingStreamRecord,
  QueueNumberRecord,
  SlotHealthRecord,
  StaleTaskList,
  StaleTaskRecord,
  StreamStateRecord,
  StreamWaitRecord,
  WorkerExecutionClassLoadRecord,
  WorkerStateRecord,
} from "../native/facades/health.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import { ClientDefect } from "../thrown-errors.ts";
import type {
  BoundedTaskList,
  BudgetLoad,
  ClaimLimits,
  DownSlot,
  ExecutionClassCoverage,
  ExecutionClassReport,
  ExpiredTaskRow,
  LastBeat,
  Measure,
  PendingStream,
  SlotHealth,
  StaleTaskRow,
  StreamCoverageState,
  StreamWait,
  WorkerExecutionClassLoad,
  WorkerStateSnapshot,
} from "./health-types.ts";
import { taskIdFromCore } from "../contract/ids.ts";
import { jsonValueOfCoreText } from "./model-json.ts";
import type { Recorded } from "../read/info.ts";

/** Nanoseconds per microsecond. */
const NANOSECONDS_PER_MICROSECOND = 1000n;

/** Microseconds per second. */
const MICROSECONDS_PER_SECOND = 1_000_000n;

/** The instant of epoch microseconds. Pure. */
export function instantOfMicros(micros: bigint): Temporal.Instant {
  return Temporal.Instant.fromEpochNanoseconds(micros * NANOSECONDS_PER_MICROSECOND);
}

/** The duration of a count of microseconds, with every digit: whole seconds and the microseconds below one second. */
export function durationOfMicros(micros: bigint): Temporal.Duration {
  return Temporal.Duration.from({
    seconds: Number(micros / MICROSECONDS_PER_SECOND),
    microseconds: Number(micros % MICROSECONDS_PER_SECOND),
  });
}

/** The snapshot of a record. A recovery config that is not a JSON object is a defect of the core. */
export function workerStateSnapshot(record: WorkerStateRecord): WorkerStateSnapshot {
  return Object.freeze({
    workerId: record.workerId,
    snapshotAt: instantOfMicros(record.snapshotAtMicros),
    hostname: record.hostname,
    pid: record.pid,
    childCount: record.childCount,
    claimLimits: claimLimits(record.claimLimits),
    clusterWideCap: Object.freeze({ ...record.clusterWideCap }),
    queues: Object.freeze([...record.queues]),
    queuePriorities: queueMap(record.queuePriorities),
    queueMaxConcurrency: queueMap(record.queueMaxConcurrency),
    recoveryConfig: recoveryConfig(record.recoveryConfigJson),
    tasksRunning: record.tasksRunning,
    tasksClaimed: record.tasksClaimed,
    memoryUsageMb: measure(record.memoryUsageMb),
    memoryPercent: measure(record.memoryPercent),
    cpuPercent: measure(record.cpuPercent),
    childrenMemoryMb: measure(record.childrenMemoryMb),
    workerStartedAt: instantOfMicros(record.workerStartedAtMicros),
    slotHealth: slotHealth(record.slotHealth),
    executionClassBudgets: Object.freeze(
      record.executionClassBudgets.map((budget) =>
        Object.freeze({ budget: budget.budget, capacity: budget.capacity, serves: Object.freeze([...budget.serves]) }),
      ),
    ),
    profile: Object.freeze({ ...record.profile }),
  });
}

/** The stale task list of a record. */
export function staleTaskList(list: StaleTaskList): BoundedTaskList<StaleTaskRow> {
  return boundedList(list, staleTaskRow);
}

/** The expired task list of a record. */
export function expiredTaskList(list: ExpiredTaskList): BoundedTaskList<ExpiredTaskRow> {
  return boundedList(list, expiredTaskRow);
}

/** The execution class coverage of a record. */
export function executionClassCoverage(record: ExecutionClassCoverageRecord): ExecutionClassCoverage {
  return Object.freeze({
    checkedAt: instantOfMicros(record.checkedAtMicros),
    liveWorkers: record.liveWorkers,
    streams: Object.freeze(
      record.streams.map((stream) =>
        Object.freeze({ queue: stream.queue, execution: stream.execution, coverage: streamState(stream.coverage) }),
      ),
    ),
  });
}

/** The execution class report of a record. */
export function executionClassReport(record: ExecutionClassReportRecord): ExecutionClassReport {
  return Object.freeze({
    checkedAt: instantOfMicros(record.checkedAtMicros),
    liveWindow: durationOfMicros(record.liveWindowMicros),
    streams: Object.freeze(record.streams.map(pendingStream)),
    workers: Object.freeze(record.workers.map(workerClassLoad)),
  });
}

/** A list with its bound kept as a number. A bound past the safe range is a defect: the core's bound is 10,000. */
function boundedList<Row, Out>(
  list:
    | { readonly kind: "complete"; readonly rows: readonly Row[] }
    | { readonly kind: "truncated"; readonly rows: readonly Row[]; readonly bound: bigint },
  convert: (row: Row) => Out,
): BoundedTaskList<Out> {
  const rows = Object.freeze(list.rows.map(convert));

  switch (list.kind) {
    case "complete":
      return Object.freeze({ kind: "complete", rows });
    case "truncated":
      return list.bound > BigInt(Number.MAX_SAFE_INTEGER)
        ? raiseDefect(new ClientDefect({ subject: "taskList.bound", rule: `a list bound of ${list.bound} rows` }))
        : Object.freeze({ kind: "truncated", rows, bound: Number(list.bound) });
    default:
      return unreachable(list);
  }
}

function staleTaskRow(record: StaleTaskRecord): StaleTaskRow {
  return Object.freeze({
    taskId: taskIdFromCore(record.taskId, "staleTasks"),
    taskName: record.taskName,
    workerHostname: recorded(record.workerHostname),
    workerPid: recorded(record.workerPid),
    workerProcessName: recorded(record.workerProcessName),
    startedAt: instantOfMicros(record.startedAtMicros),
    lastHeartbeatAt: lastBeat(record.lastHeartbeatAt),
  });
}

function expiredTaskRow(record: ExpiredTaskRecord): ExpiredTaskRow {
  return Object.freeze({
    taskId: taskIdFromCore(record.taskId, "expiredTasks"),
    taskName: record.taskName,
    queueName: record.queueName,
    priority: record.priority,
    sentAt: instantOfMicros(record.sentAtMicros),
    enqueuedAt: instantOfMicros(record.enqueuedAtMicros),
    goodUntil: instantOfMicros(record.goodUntilMicros),
    expiredFor: durationOfMicros(record.expiredForMicros),
  });
}

function recorded<T>(
  record: { readonly kind: "recorded"; readonly value: T } | { readonly kind: "notRecorded" },
): Recorded<T> {
  switch (record.kind) {
    case "recorded":
      return Object.freeze({ kind: "recorded", value: record.value });
    case "notRecorded":
      return Object.freeze({ kind: "notRecorded" });
    default:
      return unreachable(record);
  }
}

function lastBeat(record: LastBeatRecord): LastBeat {
  switch (record.kind) {
    case "beat":
      return Object.freeze({ kind: "beat", at: instantOfMicros(record.atMicros) });
    case "noBeatSinceStart":
      return Object.freeze({ kind: "noBeatSinceStart" });
    default:
      return unreachable(record);
  }
}

function slotHealth(record: SlotHealthRecord): SlotHealth {
  switch (record.kind) {
    case "allSlotsServing":
      return Object.freeze({ kind: "allSlotsServing" });
    case "slotsDown":
      return Object.freeze({ kind: "slotsDown", slots: Object.freeze(record.slots.map(downSlot)) });
    default:
      return unreachable(record);
  }
}

function claimLimits(record: ClaimLimitsRecord): ClaimLimits {
  return Object.freeze({
    perQueuePerPass: Object.freeze({ ...record.perQueuePerPass }),
    claimedPerWorker: Object.freeze({ ...record.claimedPerWorker }),
  });
}

function measure(record: MeasureRecord): Measure {
  return Object.freeze({ ...record });
}

function queueMap(entries: readonly QueueNumberRecord[]): ReadonlyMap<string, bigint> {
  return new Map(entries.map((entry) => [entry.queue, entry.value]));
}

/** The recovery config of a snapshot: a JSON object. Another JSON value is a defect of the core. */
function recoveryConfig(text: string): WorkerStateSnapshot["recoveryConfig"] {
  const value = jsonValueOfCoreText("recoveryConfig", text);

  if (value !== null && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype) {
    // SAFETY: the value is a plain object that `JSON.parse` built, and its members are `JsonValue`.
    return Object.freeze(value as WorkerStateSnapshot["recoveryConfig"]);
  }

  return raiseDefect(
    new ClientDefect({ subject: "recoveryConfig", rule: "the core's recovery config is not a JSON object" }),
  );
}

function downSlot(record: DownSlotRecord): DownSlot {
  return Object.freeze({
    slot: record.slot,
    cause: Object.freeze({ ...record.cause }),
    detail: record.detail,
    downSince: instantOfMicros(record.downSinceMicros),
    failedBoots: record.failedBoots,
  });
}

function streamState(record: StreamStateRecord): StreamCoverageState {
  return Object.freeze({ ...record });
}

function streamWait(record: StreamWaitRecord): StreamWait {
  switch (record.kind) {
    case "due":
      return Object.freeze({
        kind: "due",
        oldestDueAt: instantOfMicros(record.oldestDueAtMicros),
        wait: durationOfMicros(record.waitMicros),
      });
    case "noneDue":
      return Object.freeze({ kind: "noneDue", nextDueAt: instantOfMicros(record.nextDueAtMicros) });
    default:
      return unreachable(record);
  }
}

function pendingStream(record: PendingStreamRecord): PendingStream {
  return Object.freeze({
    queue: record.queue,
    execution: record.execution,
    wait: streamWait(record.wait),
    coverage: streamState(record.coverage),
  });
}

function budgetLoad(record: BudgetLoadRecord): BudgetLoad {
  return Object.freeze({
    budget: record.budget,
    capacity: record.capacity,
    serves: Object.freeze([...record.serves]),
    inFlight: record.inFlight,
  });
}

function workerClassLoad(record: WorkerExecutionClassLoadRecord): WorkerExecutionClassLoad {
  return Object.freeze({
    workerId: record.workerId,
    snapshotAt: instantOfMicros(record.snapshotAtMicros),
    budgets: Object.freeze(record.budgets.map(budgetLoad)),
  });
}
