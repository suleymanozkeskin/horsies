// The monitoring area of the app: `app.monitoring`, `app.pingDatabase` and `app.pingWorkers`.
//
// The app hub builds the area with the core access of its thread and puts its three members on the app. On a main
// thread each call uses the app's core handle (`appCoreHandle`; in a worker child, the child's producer handle) and
// loads the addon at its first use. In a seat isolate each call goes through the seat relay to the main thread of its
// child (`seat-calls.ts`). In another worker thread a call throws `AddonOutsideMainThread`.

import { appCoreHandle, type CoreAccess } from "../app/core-access.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { TaskId, WorkflowId } from "../contract/ids.ts";
import { cancelAddon, type CancelAddon } from "../native/facades/cancel.ts";
import { healthAddon, type HealthAddon } from "../native/facades/health.ts";
import { monitoringReadsAddon, type MonitoringReadsAddon } from "../native/facades/monitoring-reads.ts";
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
import { relayedFacades } from "./seat-calls.ts";
import type { MonitoringQueryCoreError } from "../_catalog/monitoring_errors.ts";
import type {
  CancelTaskError,
  HealthInputError,
  HealthReadError,
  MonitoringReadError,
  PingDatabaseError,
  PingWorkersError,
  WorkerHistoryError,
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
  taskDetail(taskId: TaskId): Promise<Result<TaskDetailRead, MonitoringQueryCoreError>>;
  workflowNames(): Promise<Result<readonly string[], MonitoringQueryCoreError>>;
  workflowRuns(query?: WorkflowRunsQuery): Promise<Result<readonly WorkflowRunSummary[], MonitoringReadError>>;
  workflowRun(workflowId: WorkflowId): Promise<Result<WorkflowRunRead, MonitoringQueryCoreError>>;
  workflowNode(workflowId: WorkflowId, taskIndex: number): Promise<Result<WorkflowNodeRead, MonitoringReadError>>;
  schedules(): Promise<Result<readonly ScheduleStateInfo[], MonitoringQueryCoreError>>;
  workers(): Promise<Result<readonly WorkerStateSnapshot[], HealthReadError>>;
  worker(workerId: string): Promise<Result<WorkerStateRead, WorkerReadError>>;
  workerHistory(
    workerId: string,
    limit?: WorkerHistoryLimit,
  ): Promise<Result<readonly WorkerStateSnapshot[], WorkerHistoryError>>;
  staleTasks(query: StaleTasksQuery): Promise<Result<BoundedTaskList<StaleTaskRow>, HealthInputError>>;
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

/** The addons and the core handle that the calls of the area use in its thread. */
type AreaTargets = { readonly addons: MonitoringAddons; readonly handle: () => NativeCoreHandle };

/** The targets of `core`: the relayed facades in a seat isolate, else `addons` and the app's core handle. */
function areaTargets(core: CoreAccess, addons: MonitoringAddons): AreaTargets {
  switch (core.kind) {
    case "seatRelay": {
      const relayed = relayedFacades(core.relay);

      return {
        addons: { reads: () => relayed.reads, health: () => relayed.health, cancel: () => relayed.cancel },
        handle: () => relayed.handle,
      };
    }

    case "addon":
    case "noCore":
      return { addons, handle: () => appCoreHandle(core).native };
    default:
      return unreachable(core, "areaTargets");
  }
}

/** The monitoring area of an app whose thread reaches the core through `core`. Starts nothing. */
export function createMonitoringArea(core: CoreAccess, native: MonitoringAddons = NATIVE_ADDONS): MonitoringArea {
  const { addons, handle } = areaTargets(core, native);
  const reads = () => ({ addon: addons.reads(), handle: handle() });
  const health = () => ({ addon: addons.health(), handle: handle() });

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
      cancelTask({ addon: addons.cancel(), handle: handle() }, taskId, options),
  });

  return Object.freeze({
    monitoring,
    pingDatabase: async () => pingDatabase(handle()),
    pingWorkers: async (options: PingWorkersOptions = {}) => pingWorkers(health(), options),
  });
}
