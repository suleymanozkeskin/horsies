// The monitoring reads: each read builds its addon argument, calls the addon once, and decodes the core's JSON text
// into the generated read types.
//
// - A read sends one statement and writes nothing. It does not install the schema.
// - Expected failures are values: the core's `MonitoringQueryError` (`DB_OPERATION_FAILED`,
//   `HISTORY_CONTRACT_VIOLATION`) or `QUERY_REFUSED` before any connect.
// - A process-state refusal of the core throws (`CoreError`); a defect crashes loud (`CoreDefect`).

import type { TaskAbsenceReason } from "../_catalog/history.ts";
import {
  type Breakdown,
  decodeBreakdown,
  decodeFacets,
  decodeScheduleStateInfo,
  decodeStatusCount,
  decodeTaskDetail,
  decodeTaskListPage,
  decodeWorkflowRunDetail,
  decodeWorkflowRunSummary,
  decodeWorkflowTaskDetail,
  type Facets,
  type ScheduleStateInfo,
  type StatusCount,
  type TaskDetail,
  type TaskListPage,
  type WorkflowRunDetail,
  type WorkflowRunSummary,
  type WorkflowTaskDetail,
} from "../_catalog/monitoring_models.ts";
import type { TaskId, WorkflowId } from "../contract/ids.ts";
import type { NativeEnvelope } from "../native/call/envelope.ts";
import { nativeValueAsync } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import type { MonitoringReadOutcome, MonitoringReadsAddon } from "../native/facades/monitoring-reads.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { MonitoringQueryCoreError } from "../_catalog/monitoring_errors.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { type MonitoringReadError, monitoringQueryError, queryRefused, type QueryRefused } from "./errors.ts";
import {
  decodeCoreModel,
  decodedOrDefect,
  listItems,
  type ModelDecoder,
  textItem,
  wireJsonOfCoreText,
} from "./model-json.ts";
import {
  type TaskBreakdownQuery,
  taskBreakdownArg,
  type TaskFacetsQuery,
  taskFacetsArg,
  type TaskListQuery,
  taskListArg,
  type TaskStatsQuery,
  taskStatsArg,
  wholeNumber,
  type WorkflowRunsQuery,
  workflowRunsArg,
} from "./query.ts";

/** The smallest task index of a workflow node: the core reads it as a 32-bit integer. */
const MIN_TASK_INDEX = -2_147_483_648;

/** The largest task index of a workflow node. */
const MAX_TASK_INDEX = 2_147_483_647;

/** A task by id: its detail, or why no table holds it. */
export type TaskDetailRead =
  | { readonly kind: "found"; readonly detail: TaskDetail }
  | { readonly kind: "notFound"; readonly taskId: TaskId; readonly reason: TaskAbsenceReason };

/** A workflow run by id, or no run. */
export type WorkflowRunRead =
  | { readonly kind: "found"; readonly run: WorkflowRunDetail }
  | { readonly kind: "notFound"; readonly workflowId: WorkflowId };

/** A node of a workflow run by its task index, or no node. */
export type WorkflowNodeRead =
  | { readonly kind: "found"; readonly node: WorkflowTaskDetail }
  | { readonly kind: "notFound"; readonly workflowId: WorkflowId; readonly taskIndex: number };

/** The addon and the handle that the reads call. */
export interface ReadTarget {
  readonly addon: MonitoringReadsAddon;
  readonly handle: NativeCoreHandle;
}

/** The count of each task status in the window: all seven statuses, in a fixed order. */
export async function taskStats(
  target: ReadTarget,
  query: TaskStatsQuery,
): Promise<Result<readonly StatusCount[], MonitoringReadError>> {
  const arg = taskStatsArg(query);

  if (!arg.ok) {
    return arg;
  }

  return readModel("taskStats", () => target.addon.readTaskStats(target.handle, arg.value), (text) =>
    modelList("taskStats", text, decodeStatusCount),
  );
}

/** The workers, task names, queues and error codes of the window, with their counts. */
export async function taskFacets(
  target: ReadTarget,
  query: TaskFacetsQuery,
): Promise<Result<Facets, MonitoringReadError>> {
  const arg = taskFacetsArg(query);

  return readModel("taskFacets", () => target.addon.readTaskFacets(target.handle, arg), (text) =>
    decodeCoreModel("taskFacets", text, decodeFacets),
  );
}

/** The task counts by worker, task name or queue. */
export async function taskBreakdown(
  target: ReadTarget,
  query: TaskBreakdownQuery,
): Promise<Result<Breakdown, MonitoringReadError>> {
  const arg = taskBreakdownArg(query);

  if (!arg.ok) {
    return arg;
  }

  return readModel("taskBreakdown", () => target.addon.readTaskBreakdown(target.handle, arg.value), (text) =>
    decodeCoreModel("taskBreakdown", text, decodeBreakdown),
  );
}

/** One page of the task list, with the count of every matching task. */
export async function listTasks(
  target: ReadTarget,
  query: TaskListQuery,
): Promise<Result<TaskListPage, MonitoringReadError>> {
  const arg = taskListArg(query);

  if (!arg.ok) {
    return arg;
  }

  return readModel("listTasks", () => target.addon.readTaskList(target.handle, arg.value), (text) =>
    decodeCoreModel("listTasks", text, decodeTaskListPage),
  );
}

/** The names of the workflows that have runs, at most 10,000. */
export async function workflowNames(target: ReadTarget): Promise<Result<readonly string[], MonitoringQueryCoreError>> {
  return readCoreModel("workflowNames", () => target.addon.readWorkflowNames(target.handle), (text) => {
    const items = listItems("workflowNames", wireJsonOfCoreText("workflowNames", text));

    return Object.freeze(items.map((item) => textItem("workflowNames", item)));
  });
}

/** The newest workflow runs, by name and status. */
export async function workflowRuns(
  target: ReadTarget,
  query: WorkflowRunsQuery,
): Promise<Result<readonly WorkflowRunSummary[], MonitoringReadError>> {
  const arg = workflowRunsArg(query);

  if (!arg.ok) {
    return arg;
  }

  return readModel("workflowRuns", () => target.addon.readWorkflowRuns(target.handle, arg.value), (text) =>
    modelList("workflowRuns", text, decodeWorkflowRunSummary),
  );
}

/** The state of every schedule, at most 10,000. */
export async function schedules(
  target: ReadTarget,
): Promise<Result<readonly ScheduleStateInfo[], MonitoringQueryCoreError>> {
  return readCoreModel("schedules", () => target.addon.readSchedules(target.handle), (text) =>
    modelList("schedules", text, decodeScheduleStateInfo),
  );
}

/** One workflow run with its nodes and edges, or `notFound`. */
export async function workflowRun(
  target: ReadTarget,
  workflowId: WorkflowId,
): Promise<Result<WorkflowRunRead, MonitoringQueryCoreError>> {
  return readCoreModel("workflowRun", () => target.addon.readWorkflowRun(target.handle, workflowId), (text) => {
    const value = wireJsonOfCoreText("workflowRun", text);

    return value === null
      ? Object.freeze({ kind: "notFound", workflowId })
      : Object.freeze({ kind: "found", run: decodedOrDefect("workflowRun", decodeWorkflowRunDetail(value)) });
  });
}

/** One node of a workflow run by its task index, or `notFound`. */
export async function workflowNode(
  target: ReadTarget,
  workflowId: WorkflowId,
  taskIndex: number,
): Promise<Result<WorkflowNodeRead, MonitoringReadError>> {
  const index = taskIndexOf(taskIndex);

  if (!index.ok) {
    return index;
  }

  const answer = () => target.addon.readWorkflowNode(target.handle, workflowId, index.value);

  return readModel("workflowNode", answer, (text) => {
    const value = wireJsonOfCoreText("workflowNode", text);

    return value === null
      ? Object.freeze({ kind: "notFound", workflowId, taskIndex: index.value })
      : Object.freeze({ kind: "found", node: decodedOrDefect("workflowNode", decodeWorkflowTaskDetail(value)) });
  });
}

/** One task with its attempts (the lifecycle record), or why no table holds it. */
export async function taskDetail(
  target: ReadTarget,
  taskId: TaskId,
): Promise<Result<TaskDetailRead, MonitoringQueryCoreError>> {
  const outcome = await nativeValueAsync("readTaskDetail", () => target.addon.readTaskDetail(target.handle, taskId));

  switch (outcome.kind) {
    case "json":
      return ok(
        Object.freeze({ kind: "found", detail: decodeCoreModel("taskDetail", outcome.text, decodeTaskDetail) }),
      );
    case "absent":
      return ok(Object.freeze({ kind: "notFound", taskId, reason: outcome.reason }));
    case "failed":
      return err(monitoringQueryError(outcome.error));
    default:
      return unreachable(outcome);
  }
}

/** A task index that the core reads as a 32-bit integer, or the refusal. Pure. */
function taskIndexOf(taskIndex: number): Result<number, QueryRefused> {
  const whole = wholeNumber("taskIndex", taskIndex);

  if (!whole.ok) {
    return whole;
  }

  return whole.value >= MIN_TASK_INDEX && whole.value <= MAX_TASK_INDEX
    ? whole
    : err(queryRefused("taskIndex", `taskIndex must be from ${MIN_TASK_INDEX} to ${MAX_TASK_INDEX}; got ${taskIndex}`));
}

/** Call the addon once and give the decoded model, the refusal of the query, or the core's error. */
async function readModel<T>(
  read: string,
  answer: () => Promise<NativeEnvelope<MonitoringReadOutcome>>,
  decode: (text: string) => T,
): Promise<Result<T, MonitoringReadError>> {
  const outcome = await nativeValueAsync(read, answer);

  switch (outcome.kind) {
    case "json":
      return ok(decode(outcome.text));
    case "refused":
      return err(queryRefused(outcome.field, outcome.reason));
    case "failed":
      return err(monitoringQueryError(outcome.error));
    default:
      return unreachable(outcome);
  }
}

/**
 * Call the addon for a read that has no query to refuse, and give the decoded model or the core's error. A refusal is
 * a defect of the bridge: the read gives it no value to refuse.
 */
async function readCoreModel<T>(
  read: string,
  answer: () => Promise<NativeEnvelope<MonitoringReadOutcome>>,
  decode: (text: string) => T,
): Promise<Result<T, MonitoringQueryCoreError>> {
  const outcome = await nativeValueAsync(read, answer);

  switch (outcome.kind) {
    case "json":
      return ok(decode(outcome.text));
    case "refused":
      return raiseDefect(
        new ClientDefect({ subject: read, rule: `the bridge refused the ${outcome.field} of a read that has none` }),
      );
    case "failed":
      return err(monitoringQueryError(outcome.error));
    default:
      return unreachable(outcome);
  }
}

/** The models of a JSON list of the core, frozen. */
function modelList<T>(read: string, text: string, decode: ModelDecoder<T>): readonly T[] {
  const items = listItems(read, wireJsonOfCoreText(read, text));

  return Object.freeze(items.map((item) => decodedOrDefect(read, decode(item))));
}
