// The generated declarations of the monitoring reads, the health reads and the cancel of the `facades` area match the
// hand-kept ones, function by function (arguments and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type { CancelAddon, CancelPolicy, TaskCancelledRecord } from "../../horsies/src/native/facades/cancel.ts";
import type {
  BootCauseRecord,
  BudgetLoadRecord,
  ExecutionClassBudgetRecord,
  ExecutionClassCoverageRecord,
  ExecutionClassReportRecord,
  ClusterCapRecord,
  DownSlotRecord,
  ExecutionName,
  ExpiredTaskList,
  ExpiredTaskRecord,
  HealthAddon,
  HistoryLimitArg,
  LastBeatRecord,
  MeasureRecord,
  MinResponsesArg,
  PendingStreamRecord,
  PingTargetArg,
  QueueNumberRecord,
  RecordedPidRecord,
  RecordedTextRecord,
  ShortageErrnoName,
  SlotHealthRecord,
  SnapshotProfileRecord,
  StaleTaskList,
  StaleTaskRecord,
  StreamCoverageRecord,
  StreamStateRecord,
  StreamWaitRecord,
  WorkerExecutionClassLoadRecord,
  WorkerPongRecord,
  WorkerStateRead,
  WorkerStateRecord,
} from "../../horsies/src/native/facades/health.ts";
import type {
  CountArg,
  ErrorCategoryArg,
  FiltersArg,
  GroupByArg,
  MonitoringReadOutcome,
  MonitoringReadsAddon,
  QueryField,
  RetriedArg,
  WorkflowNameArg,
  RunStatusArg,
  SortArg,
  SortDirectionArg,
  SortFieldArg,
  TaskAbsenceReasonName,
  TaskBreakdownArg,
  TaskDetailOutcome,
  TaskFacetsArg,
  TaskListArg,
  TaskStatusCountsArg,
  WindowArg,
  WindowBoundArg,
  WorkflowRunsArg,
} from "../../horsies/src/native/facades/monitoring-reads.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

type Reads = MonitoringReadsAddon;

export const readTaskStatusCountsMatches: SameFunction<typeof Generated.readTaskStatusCounts, Reads["readTaskStatusCounts"]> = true;
export const readTaskFacetsMatches: SameFunction<typeof Generated.readTaskFacets, Reads["readTaskFacets"]> = true;

export const readTaskBreakdownMatches: SameFunction<typeof Generated.readTaskBreakdown, Reads["readTaskBreakdown"]> =
  true;

export const readTaskListMatches: SameFunction<typeof Generated.readTaskList, Reads["readTaskList"]> = true;

export const readWorkflowNamesMatches: SameFunction<typeof Generated.readWorkflowNames, Reads["readWorkflowNames"]> =
  true;

export const readWorkflowRunsMatches: SameFunction<typeof Generated.readWorkflowRuns, Reads["readWorkflowRuns"]> = true;
export const readWorkflowRunMatches: SameFunction<typeof Generated.readWorkflowRun, Reads["readWorkflowRun"]> = true;
export const readWorkflowNodeMatches: SameFunction<typeof Generated.readWorkflowNode, Reads["readWorkflowNode"]> = true;
export const readSchedulesMatches: SameFunction<typeof Generated.readSchedules, Reads["readSchedules"]> = true;
export const readTaskDetailMatches: SameFunction<typeof Generated.readTaskDetail, Reads["readTaskDetail"]> = true;

export const pingWorkersMatches: SameFunction<typeof Generated.pingWorkers, HealthAddon["pingWorkers"]> = true;

export const listWorkerStatesMatches: SameFunction<typeof Generated.listWorkerStates, HealthAddon["listWorkerStates"]> =
  true;

export const getWorkerStateMatches: SameFunction<typeof Generated.getWorkerState, HealthAddon["getWorkerState"]> = true;

export const getWorkerStateHistoryMatches: SameFunction<
  typeof Generated.getWorkerStateHistory,
  HealthAddon["getWorkerStateHistory"]
> = true;

export const getStaleTasksMatches: SameFunction<typeof Generated.getStaleTasks, HealthAddon["getStaleTasks"]> = true;

export const getExpiredTasksMatches: SameFunction<typeof Generated.getExpiredTasks, HealthAddon["getExpiredTasks"]> =
  true;

export const getClassCoverageMatches: SameFunction<typeof Generated.getExecutionClassCoverage, HealthAddon["getExecutionClassCoverage"]> =
  true;

export const getClassReportMatches: SameFunction<typeof Generated.getExecutionClassReport, HealthAddon["getExecutionClassReport"]> = true;

export const cancelTaskMatches: SameFunction<typeof Generated.cancelTask, CancelAddon["cancelTask"]> = true;

export const queryFieldMatches: Mutual<Generated.QueryField, QueryField> = true;

export const countMatches: Mutual<Generated.CountArg, CountArg> = true;

export const sortMatches: Mutual<Generated.SortArg, SortArg> = true;
export const windowBoundMatches: Mutual<Generated.WindowBoundArg, WindowBoundArg> = true;
export const windowMatches: Mutual<Generated.WindowArg, WindowArg> = true;
export const retriedMatches: Mutual<Generated.RetriedArg, RetriedArg> = true;
export const errorCategoryMatches: Mutual<Generated.ErrorCategoryArg, ErrorCategoryArg> = true;
export const sortFieldMatches: Mutual<Generated.SortFieldArg, SortFieldArg> = true;
export const sortDirectionMatches: Mutual<Generated.SortDirectionArg, SortDirectionArg> = true;
export const groupByMatches: Mutual<Generated.GroupByArg, GroupByArg> = true;
export const filtersMatches: Mutual<Generated.FiltersArg, FiltersArg> = true;
export const taskStatusCountsMatches: Mutual<Generated.TaskStatusCountsArg, TaskStatusCountsArg> = true;
export const taskFacetsMatches: Mutual<Generated.TaskFacetsArg, TaskFacetsArg> = true;
export const taskBreakdownMatches: Mutual<Generated.TaskBreakdownArg, TaskBreakdownArg> = true;
export const taskListMatches: Mutual<Generated.TaskListArg, TaskListArg> = true;
export const workflowNameMatches: Mutual<Generated.WorkflowNameArg, WorkflowNameArg> = true;
export const runStatusMatches: Mutual<Generated.RunStatusArg, RunStatusArg> = true;
export const workflowRunsMatches: Mutual<Generated.WorkflowRunsArg, WorkflowRunsArg> = true;
export const readOutcomeMatches: Mutual<Generated.MonitoringReadOutcome, MonitoringReadOutcome> = true;
export const absenceReasonMatches: Mutual<Generated.TaskAbsenceReasonName, TaskAbsenceReasonName> = true;
export const detailOutcomeMatches: Mutual<Generated.TaskDetailOutcome, TaskDetailOutcome> = true;

export const pingTargetMatches: Mutual<Generated.PingTargetArg, PingTargetArg> = true;
export const minResponsesMatches: Mutual<Generated.MinResponsesArg, MinResponsesArg> = true;
export const historyLimitMatches: Mutual<Generated.HistoryLimitArg, HistoryLimitArg> = true;
export const pongMatches: Mutual<Generated.WorkerPongRecord, WorkerPongRecord> = true;
export const executionMatches: Mutual<Generated.ExecutionName, ExecutionName> = true;
export const profileMatches: Mutual<Generated.SnapshotProfileRecord, SnapshotProfileRecord> = true;
export const errnoMatches: Mutual<Generated.ShortageErrnoName, ShortageErrnoName> = true;
export const bootCauseMatches: Mutual<Generated.BootCauseRecord, BootCauseRecord> = true;
export const downSlotMatches: Mutual<Generated.DownSlotRecord, DownSlotRecord> = true;
export const slotHealthMatches: Mutual<Generated.SlotHealthRecord, SlotHealthRecord> = true;
export const classBudgetMatches: Mutual<Generated.ExecutionClassBudgetRecord, ExecutionClassBudgetRecord> = true;
export const queueNumberMatches: Mutual<Generated.QueueNumberRecord, QueueNumberRecord> = true;
export const measureMatches: Mutual<Generated.MeasureRecord, MeasureRecord> = true;
export const clusterCapMatches: Mutual<Generated.ClusterCapRecord, ClusterCapRecord> = true;
export const workerStateMatches: Mutual<Generated.WorkerStateRecord, WorkerStateRecord> = true;
export const workerStateReadMatches: Mutual<Generated.WorkerStateRead, WorkerStateRead> = true;
export const recordedTextMatches: Mutual<Generated.RecordedTextRecord, RecordedTextRecord> = true;
export const recordedPidMatches: Mutual<Generated.RecordedPidRecord, RecordedPidRecord> = true;
export const lastBeatMatches: Mutual<Generated.LastBeatRecord, LastBeatRecord> = true;
export const staleRowMatches: Mutual<Generated.StaleTaskRecord, StaleTaskRecord> = true;
export const expiredRowMatches: Mutual<Generated.ExpiredTaskRecord, ExpiredTaskRecord> = true;
export const staleListMatches: Mutual<Generated.StaleTaskList, StaleTaskList> = true;
export const expiredListMatches: Mutual<Generated.ExpiredTaskList, ExpiredTaskList> = true;
export const streamStateMatches: Mutual<Generated.StreamStateRecord, StreamStateRecord> = true;
export const streamCoverageMatches: Mutual<Generated.StreamCoverageRecord, StreamCoverageRecord> = true;
export const classCoverageMatches: Mutual<Generated.ExecutionClassCoverageRecord, ExecutionClassCoverageRecord> = true;
export const streamWaitMatches: Mutual<Generated.StreamWaitRecord, StreamWaitRecord> = true;
export const pendingStreamMatches: Mutual<Generated.PendingStreamRecord, PendingStreamRecord> = true;
export const budgetLoadMatches: Mutual<Generated.BudgetLoadRecord, BudgetLoadRecord> = true;
export const workerClassLoadMatches: Mutual<Generated.WorkerExecutionClassLoadRecord, WorkerExecutionClassLoadRecord> = true;
export const classReportMatches: Mutual<Generated.ExecutionClassReportRecord, ExecutionClassReportRecord> = true;

export const cancelPolicyMatches: Mutual<Generated.CancelPolicy, CancelPolicy> = true;
export const cancelledMatches: Mutual<Generated.TaskCancelledRecord, TaskCancelledRecord> = true;
