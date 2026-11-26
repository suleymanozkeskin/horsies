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
import type { NativeEnvelope } from "../native/call/envelope.ts";
import { nativeValueAsync } from "../native/call/envelope.ts";
import { unreachable } from "../native/call/never.ts";
import type { MonitoringReadOutcome, MonitoringReadsAddon } from "../native/facades/monitoring-reads.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import { err, ok, type Result } from "../wire/result.ts";
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

/** A UUID in hyphenated form, of either case. */
const UUID_TEXT = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

/** A task by id: its detail, or why no table holds it. */
export type TaskDetailRead =
  | { readonly kind: "found"; readonly detail: TaskDetail }
  | { readonly kind: "notFound"; readonly taskId: string; readonly reason: TaskAbsenceReason };

/** A workflow run by id, or no run. */
export type WorkflowRunRead =
  | { readonly kind: "found"; readonly run: WorkflowRunDetail }
  | { readonly kind: "notFound"; readonly workflowId: string };

/** A node of a workflow run by its task index, or no node. */
export type WorkflowNodeRead =
  | { readonly kind: "found"; readonly node: WorkflowTaskDetail }
  | { readonly kind: "notFound"; readonly workflowId: string; readonly taskIndex: number };

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
export async function workflowNames(target: ReadTarget): Promise<Result<readonly string[], MonitoringReadError>> {
  return readModel("workflowNames", () => target.addon.readWorkflowNames(target.handle), (text) => {
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
): Promise<Result<readonly ScheduleStateInfo[], MonitoringReadError>> {
  return readModel("schedules", () => target.addon.readSchedules(target.handle), (text) =>
    modelList("schedules", text, decodeScheduleStateInfo),
  );
}

/** One workflow run with its nodes and edges, or `notFound`. */
export async function workflowRun(
  target: ReadTarget,
  workflowId: string,
): Promise<Result<WorkflowRunRead, MonitoringReadError>> {
  const id = uuidText("workflowId", workflowId);

  if (!id.ok) {
    return id;
  }

  return readModel("workflowRun", () => target.addon.readWorkflowRun(target.handle, id.value), (text) => {
    const value = wireJsonOfCoreText("workflowRun", text);

    return value === null
      ? Object.freeze({ kind: "notFound", workflowId: id.value })
      : Object.freeze({ kind: "found", run: decodedOrDefect("workflowRun", decodeWorkflowRunDetail(value)) });
  });
}

/** One node of a workflow run by its task index, or `notFound`. */
export async function workflowNode(
  target: ReadTarget,
  workflowId: string,
  taskIndex: number,
): Promise<Result<WorkflowNodeRead, MonitoringReadError>> {
  const id = uuidText("workflowId", workflowId);

  if (!id.ok) {
    return id;
  }

  const index = taskIndexOf(taskIndex);

  if (!index.ok) {
    return index;
  }

  const answer = () => target.addon.readWorkflowNode(target.handle, id.value, index.value);

  return readModel("workflowNode", answer, (text) => {
    const value = wireJsonOfCoreText("workflowNode", text);

    return value === null
      ? Object.freeze({ kind: "notFound", workflowId: id.value, taskIndex: index.value })
      : Object.freeze({ kind: "found", node: decodedOrDefect("workflowNode", decodeWorkflowTaskDetail(value)) });
  });
}

/** One task with its attempts (the lifecycle record), or why no table holds it. */
export async function taskDetail(
  target: ReadTarget,
  taskId: string,
): Promise<Result<TaskDetailRead, MonitoringReadError>> {
  const id = uuidText("taskId", taskId);

  if (!id.ok) {
    return id;
  }

  const outcome = await nativeValueAsync("readTaskDetail", () => target.addon.readTaskDetail(target.handle, id.value));

  switch (outcome.kind) {
    case "json":
      return ok(
        Object.freeze({ kind: "found", detail: decodeCoreModel("taskDetail", outcome.text, decodeTaskDetail) }),
      );
    case "absent":
      return ok(Object.freeze({ kind: "notFound", taskId: outcome.taskId, reason: outcome.reason }));
    case "failed":
      return err(monitoringQueryError(outcome.error));
    default:
      return unreachable(outcome);
  }
}

/** The lower-case text of a UUID, or the refusal of `field`. Pure. */
export function uuidText(field: "taskId" | "workflowId", text: string): Result<string, QueryRefused> {
  return UUID_TEXT.test(text)
    ? ok(text.toLowerCase())
    : err(queryRefused(field, `${field} must be a UUID in hyphenated form; got ${JSON.stringify(text)}`));
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

/** The models of a JSON list of the core, frozen. */
function modelList<T>(read: string, text: string, decode: ModelDecoder<T>): readonly T[] {
  const items = listItems(read, wireJsonOfCoreText(read, text));

  return Object.freeze(items.map((item) => decodedOrDefect(read, decode(item))));
}
