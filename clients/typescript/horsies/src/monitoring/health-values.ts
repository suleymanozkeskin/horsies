// The frozen values of the health records that the addon gives. Pure.

import type {
  BudgetLoadRecord,
  ClassCoverageRecord,
  ClassReportRecord,
  DownSlotRecord,
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
  WorkerClassLoadRecord,
  WorkerStateRecord,
} from "../native/facades/health.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import { ClientDefect } from "../thrown-errors.ts";
import type {
  BoundedTaskList,
  BudgetLoad,
  ClassCoverage,
  ClassReport,
  DownSlot,
  ExpiredTaskRow,
  LastBeat,
  Measure,
  PendingStream,
  SlotHealth,
  StaleTaskRow,
  StreamCoverageState,
  StreamWait,
  WorkerClassLoad,
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
    processes: record.processes,
    maxClaimBatch: record.maxClaimBatch,
    maxClaimPerWorker: record.maxClaimPerWorker,
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
    classCapacity: Object.freeze(
      record.classCapacity.map((budget) =>
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

/** The class coverage of a record. */
export function classCoverage(record: ClassCoverageRecord): ClassCoverage {
  return Object.freeze({
    checkedAt: instantOfMicros(record.checkedAtMicros),
    liveWorkers: record.liveWorkers,
    streams: Object.freeze(
      record.streams.map((stream) =>
        Object.freeze({ queueName: stream.queueName, execution: stream.execution, state: streamState(stream.state) }),
      ),
    ),
  });
}

/** The class report of a record. */
export function classReport(record: ClassReportRecord): ClassReport {
  return Object.freeze({
    checkedAt: instantOfMicros(record.checkedAtMicros),
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
    id: taskIdFromCore(record.id, "staleTasks"),
    taskName: record.taskName,
    workerHostname: recorded(record.workerHostname),
    workerPid: recorded(record.workerPid),
    workerProcessName: recorded(record.workerProcessName),
    startedAt: instantOfMicros(record.startedAtMicros),
    lastHeartbeat: lastBeat(record.lastHeartbeat),
  });
}

function expiredTaskRow(record: ExpiredTaskRecord): ExpiredTaskRow {
  return Object.freeze({
    id: taskIdFromCore(record.id, "expiredTasks"),
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
    case "oldestWaiting":
      return Object.freeze({
        kind: "oldestWaiting",
        enqueuedAt: instantOfMicros(record.enqueuedAtMicros),
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
    queueName: record.queueName,
    execution: record.execution,
    wait: streamWait(record.wait),
    state: streamState(record.state),
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

function workerClassLoad(record: WorkerClassLoadRecord): WorkerClassLoad {
  return Object.freeze({
    workerId: record.workerId,
    snapshotAt: instantOfMicros(record.snapshotAtMicros),
    budgets: Object.freeze(record.budgets.map(budgetLoad)),
  });
}
