// The monitoring area of the app: `app.monitoring`, `app.pingDatabase` and `app.pingWorkers`.
//
// The app hub builds the area with the core access of its thread and puts its three members on the app. On a main
// thread each call uses the app's core handle (in a worker child, the child's producer handle) and loads the addon at
// its first use. In a worker thread a call throws `AddonOutsideMainThread`: the seat relay serves no facade operation.

import { threadId } from "node:worker_threads";

import type { CoreAccess } from "../app/core-access.ts";
import type { TaskId, WorkflowId } from "../contract/ids.ts";
import { unreachable } from "../native/call/never.ts";
import { cancelAddon, type CancelAddon } from "../native/facades/cancel.ts";
import { healthAddon, type HealthAddon } from "../native/facades/health.ts";
import { monitoringReadsAddon, type MonitoringReadsAddon } from "../native/facades/monitoring-reads.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import { AddonOutsideMainThread } from "../thrown-errors.ts";
import type { Result } from "../wire/result.ts";
import type {
  Breakdown,
  Facets,
  ScheduleStateInfo,
  StatusCount,
  TaskListPage,
  WorkflowRunSummary,
} from "../_catalog/monitoring_models.ts";
import { cancelTask, type CancelTaskOptions } from "./cancel.ts";
import type {
  CancelTaskError,
  HealthQueryError,
  HealthReadError,
  MonitoringReadError,
  PingDatabaseError,
  PingWorkersError,
  WorkerReadError,
} from "./errors.ts";
import {
  expiredTasks,
  pingDatabase,
  pingWorkers,
  readClassCoverage,
  readClassReport,
  staleTasks,
  worker,
  workerHistory,
  workers,
} from "./health.ts";
import type {
  BoundedTaskList,
  ClassCoverage,
  ClassReport,
  DatabasePing,
  ExpiredTaskRow,
  PingWorkersOptions,
  StaleTaskRow,
  TaskCancelled,
  WorkerHistoryLimit,
  WorkerPong,
  WorkerStateRead,
  WorkerStateSnapshot,
} from "./health-types.ts";
import type { TaskBreakdownQuery, TaskFacetsQuery, TaskListQuery, TaskStatsQuery, WorkflowRunsQuery } from "./query.ts";
import {
  listTasks,
  schedules,
  taskBreakdown,
  taskDetail,
  type TaskDetailRead,
  taskFacets,
  taskStats,
  workflowNames,
  workflowNode,
  type WorkflowNodeRead,
  workflowRun,
  type WorkflowRunRead,
  workflowRuns,
} from "./reads.ts";

/** The addon functions of the area, each loaded at its first use. Tests pass their own. */
export interface MonitoringAddons {
  readonly reads: () => MonitoringReadsAddon;
  readonly health: () => HealthAddon;
  readonly cancel: () => CancelAddon;
}

/** The addon functions of the native addon. */
const NATIVE_ADDONS: MonitoringAddons = Object.freeze({
  reads: monitoringReadsAddon,
  health: healthAddon,
  cancel: cancelAddon,
});

/** The stale task read: RUNNING tasks with no beat for `thresholdMinutes` (whole minutes, 0 to 525,600). */
export interface StaleTasksQuery {
  readonly thresholdMinutes: number;
}

/** The reads and the cancel of `app.monitoring`. */
export interface Monitoring {
  taskStats(query?: TaskStatsQuery): Promise<Result<readonly StatusCount[], MonitoringReadError>>;
  taskFacets(query?: TaskFacetsQuery): Promise<Result<Facets, MonitoringReadError>>;
  taskBreakdown(query: TaskBreakdownQuery): Promise<Result<Breakdown, MonitoringReadError>>;
  listTasks(query?: TaskListQuery): Promise<Result<TaskListPage, MonitoringReadError>>;
  taskDetail(taskId: TaskId): Promise<Result<TaskDetailRead, MonitoringReadError>>;
  workflowNames(): Promise<Result<readonly string[], MonitoringReadError>>;
  workflowRuns(query?: WorkflowRunsQuery): Promise<Result<readonly WorkflowRunSummary[], MonitoringReadError>>;
  workflowRun(workflowId: WorkflowId): Promise<Result<WorkflowRunRead, MonitoringReadError>>;
  workflowNode(workflowId: WorkflowId, taskIndex: number): Promise<Result<WorkflowNodeRead, MonitoringReadError>>;
  schedules(): Promise<Result<readonly ScheduleStateInfo[], MonitoringReadError>>;
  workers(): Promise<Result<readonly WorkerStateSnapshot[], HealthReadError>>;
  worker(workerId: string): Promise<Result<WorkerStateRead, WorkerReadError>>;
  workerHistory(
    workerId: string,
    limit?: WorkerHistoryLimit,
  ): Promise<Result<readonly WorkerStateSnapshot[], HealthQueryError>>;
  staleTasks(query: StaleTasksQuery): Promise<Result<BoundedTaskList<StaleTaskRow>, HealthQueryError>>;
  expiredTasks(): Promise<Result<BoundedTaskList<ExpiredTaskRow>, HealthReadError>>;
  classCoverage(): Promise<Result<ClassCoverage, HealthReadError>>;
  classReport(): Promise<Result<ClassReport, HealthReadError>>;
  cancelTask(taskId: TaskId, options: CancelTaskOptions): Promise<Result<TaskCancelled, CancelTaskError>>;
}

/** The members that the area puts on the app. */
export interface MonitoringArea {
  readonly monitoring: Monitoring;
  pingDatabase(): Promise<Result<DatabasePing, PingDatabaseError>>;
  pingWorkers(options?: PingWorkersOptions): Promise<Result<readonly WorkerPong[], PingWorkersError>>;
}

/** The monitoring area of an app whose thread reaches the core through `core`. Starts nothing. */
export function createMonitoringArea(core: CoreAccess, addons: MonitoringAddons = NATIVE_ADDONS): MonitoringArea {
  const reads = () => ({ addon: addons.reads(), handle: mainThreadHandle(core) });
  const health = () => ({ addon: addons.health(), handle: mainThreadHandle(core) });

  const monitoring: Monitoring = Object.freeze({
    taskStats: async (query: TaskStatsQuery = {}) => taskStats(reads(), query),
    taskFacets: async (query: TaskFacetsQuery = {}) => taskFacets(reads(), query),
    taskBreakdown: async (query: TaskBreakdownQuery) => taskBreakdown(reads(), query),
    listTasks: async (query: TaskListQuery = {}) => listTasks(reads(), query),
    taskDetail: async (taskId: TaskId) => taskDetail(reads(), taskId),
    workflowNames: async () => workflowNames(reads()),
    workflowRuns: async (query: WorkflowRunsQuery = {}) => workflowRuns(reads(), query),
    workflowRun: async (workflowId: WorkflowId) => workflowRun(reads(), workflowId),
    workflowNode: async (workflowId: WorkflowId, taskIndex: number) => workflowNode(reads(), workflowId, taskIndex),
    schedules: async () => schedules(reads()),
    workers: async () => workers(health()),
    worker: async (workerId: string) => worker(health(), workerId),
    workerHistory: async (workerId: string, limit: WorkerHistoryLimit = { kind: "allRetained" }) =>
      workerHistory(health(), workerId, limit),
    staleTasks: async (query: StaleTasksQuery) => staleTasks(health(), query.thresholdMinutes),
    expiredTasks: async () => expiredTasks(health()),
    classCoverage: async () => readClassCoverage(health()),
    classReport: async () => readClassReport(health()),
    cancelTask: async (taskId: TaskId, options: CancelTaskOptions) =>
      cancelTask({ addon: addons.cancel(), handle: mainThreadHandle(core) }, taskId, options),
  });

  return Object.freeze({
    monitoring,
    pingDatabase: async () => pingDatabase(mainThreadHandle(core)),
    pingWorkers: async (options: PingWorkersOptions = {}) => pingWorkers(health(), options),
  });
}

/** The app's core handle on a main thread. A worker thread has none: `AddonOutsideMainThread`. */
function mainThreadHandle(core: CoreAccess): NativeCoreHandle {
  switch (core.kind) {
    case "addon":
      return core.handle.native;
    case "seatRelay":
    case "noCore":
      throw new AddonOutsideMainThread({ threadId });
    default:
      return unreachable(core);
  }
}
