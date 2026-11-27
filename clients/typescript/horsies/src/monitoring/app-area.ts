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
import { type RelayedAddon, relayedFacades, type RelayedPing } from "./seat-calls.ts";
import type {
  CancelTaskError,
  HealthInputError,
  HealthReadError,
  MonitoringQueryError,
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
  readExecutionClassCoverage,
  readExecutionClassReport,
  staleTasks,
  worker,
  workerHistory,
  workers,
} from "./health.ts";
import type {
  BoundedTaskList,
  ExecutionClassCoverage,
  ExecutionClassReport,
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
import type { TaskBreakdownQuery, TaskFacetsQuery, TaskListQuery, TaskStatusCountsQuery, WorkflowRunsQuery } from "./query.ts";
import {
  listTasks,
  schedules,
  taskBreakdown,
  taskDetail,
  type TaskDetailRead,
  taskFacets,
  taskStatusCounts,
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
  taskStatusCounts(query?: TaskStatusCountsQuery): Promise<Result<readonly StatusCount[], MonitoringReadError>>;
  taskFacets(query?: TaskFacetsQuery): Promise<Result<Facets, MonitoringReadError>>;
  taskBreakdown(query: TaskBreakdownQuery): Promise<Result<Breakdown, MonitoringReadError>>;
  listTasks(query?: TaskListQuery): Promise<Result<TaskListPage, MonitoringReadError>>;
  getTaskDetail(taskId: TaskId): Promise<Result<TaskDetailRead, MonitoringQueryError>>;
  listWorkflowNames(): Promise<Result<readonly string[], MonitoringQueryError>>;
  listWorkflowRuns(query?: WorkflowRunsQuery): Promise<Result<readonly WorkflowRunSummary[], MonitoringReadError>>;
  getWorkflowRun(workflowId: WorkflowId): Promise<Result<WorkflowRunRead, MonitoringQueryError>>;
  getWorkflowNode(workflowId: WorkflowId, nodeIndex: number): Promise<Result<WorkflowNodeRead, MonitoringReadError>>;
  listSchedules(): Promise<Result<readonly ScheduleStateInfo[], MonitoringQueryError>>;
  listWorkerStates(): Promise<Result<readonly WorkerStateSnapshot[], HealthReadError>>;
  getWorkerState(workerId: string): Promise<Result<WorkerStateRead, WorkerReadError>>;
  getWorkerStateHistory(
    workerId: string,
    limit?: WorkerHistoryLimit,
  ): Promise<Result<readonly WorkerStateSnapshot[], WorkerHistoryError>>;
  getStaleTasks(query: StaleTasksQuery): Promise<Result<BoundedTaskList<StaleTaskRow>, HealthInputError>>;
  getExpiredTasks(): Promise<Result<BoundedTaskList<ExpiredTaskRow>, HealthReadError>>;
  getExecutionClassCoverage(): Promise<Result<ExecutionClassCoverage, HealthReadError>>;
  getExecutionClassReport(): Promise<Result<ExecutionClassReport, HealthReadError>>;
  cancelTask(taskId: TaskId, options: CancelTaskOptions): Promise<Result<TaskCancelled, CancelTaskError>>;
}

/** The members that the area puts on the app. */
export interface MonitoringArea {
  readonly monitoring: Monitoring;
  pingDatabase(): Promise<Result<DatabasePing, PingDatabaseError>>;
  pingWorkers(options?: PingWorkersOptions): Promise<Result<readonly WorkerPong[], PingWorkersError>>;
}

/**
 * The addons, the core handle and the database ping that the calls of the area use in its thread, in the form of a
 * relayed addon: in a seat isolate an answer can be the relay's `seatRelayFull`.
 */
type AreaTargets = {
  readonly addons: {
    readonly reads: () => RelayedAddon<MonitoringReadsAddon>;
    readonly health: () => RelayedAddon<HealthAddon>;
    readonly cancel: () => RelayedAddon<CancelAddon>;
  };
  readonly handle: () => NativeCoreHandle;
  readonly ping: () => RelayedPing;
};

/** The targets of `core`: the relayed facades in a seat isolate, else `addons` and the app's core handle. */
function areaTargets(core: CoreAccess, addons: MonitoringAddons): AreaTargets {
  switch (core.kind) {
    case "seatRelay": {
      const relayed = relayedFacades(core.relay);

      return {
        addons: { reads: () => relayed.reads, health: () => relayed.health, cancel: () => relayed.cancel },
        handle: () => relayed.handle,
        ping: () => relayed.ping,
      };
    }

    case "addon":
    case "noCore": {
      const handle = (): NativeCoreHandle => appCoreHandle(core).native;

      return { addons, handle, ping: () => async () => handle().pingDatabase() };
    }

    default:
      return unreachable(core, "areaTargets");
  }
}

/** The monitoring area of an app whose thread reaches the core through `core`. Starts nothing. */
export function createMonitoringArea(core: CoreAccess, native: MonitoringAddons = NATIVE_ADDONS): MonitoringArea {
  const { addons, handle, ping } = areaTargets(core, native);
  const reads = () => ({ addon: addons.reads(), handle: handle() });
  const health = () => ({ addon: addons.health(), handle: handle() });

  const monitoring: Monitoring = Object.freeze({
    taskStatusCounts: async (query: TaskStatusCountsQuery = {}) => taskStatusCounts(reads(), query),
    taskFacets: async (query: TaskFacetsQuery = {}) => taskFacets(reads(), query),
    taskBreakdown: async (query: TaskBreakdownQuery) => taskBreakdown(reads(), query),
    listTasks: async (query: TaskListQuery = {}) => listTasks(reads(), query),
    getTaskDetail: async (taskId: TaskId) => taskDetail(reads(), taskId),
    listWorkflowNames: async () => workflowNames(reads()),
    listWorkflowRuns: async (query: WorkflowRunsQuery = {}) => workflowRuns(reads(), query),
    getWorkflowRun: async (workflowId: WorkflowId) => workflowRun(reads(), workflowId),
    getWorkflowNode: async (workflowId: WorkflowId, nodeIndex: number) => workflowNode(reads(), workflowId, nodeIndex),
    listSchedules: async () => schedules(reads()),
    listWorkerStates: async () => workers(health()),
    getWorkerState: async (workerId: string) => worker(health(), workerId),
    getWorkerStateHistory: async (workerId: string, limit: WorkerHistoryLimit = { kind: "allRetained" }) =>
      workerHistory(health(), workerId, limit),
    getStaleTasks: async (query: StaleTasksQuery) => staleTasks(health(), query.thresholdMinutes),
    getExpiredTasks: async () => expiredTasks(health()),
    getExecutionClassCoverage: async () => readExecutionClassCoverage(health()),
    getExecutionClassReport: async () => readExecutionClassReport(health()),
    cancelTask: async (taskId: TaskId, options: CancelTaskOptions) =>
      cancelTask({ addon: addons.cancel(), handle: handle() }, taskId, options),
  });

  return Object.freeze({
    monitoring,
    pingDatabase: async () => pingDatabase(ping()),
    pingWorkers: async (options: PingWorkersOptions = {}) => pingWorkers(health(), options),
  });
}
