// `report [--failed N]`: the state of the showcase from the monitoring reads and the health reads of the app: the
// database round trip, the workers and their ping replies, the tasks by status, the newest failed tasks with their
// codes, the workflow runs by name and status, and the schedules with their sent slots. Each read writes nothing but a schema install.

import { parseArgs } from "node:util";

import {
  type Horsies,
  MAX_WORKFLOW_RUNS_LIMIT,
  type Result,
  type ScheduleStateInfo,
  type StatusCount,
  type TaskSummary,
  type WorkflowRunSummary,
} from "horsies";

import { err, ok } from "../result.ts";

/** The failed tasks that a report lists when `--failed` is left out. */
export const DEFAULT_FAILED_ROWS = 10;

/** The most failed tasks of one report. */
export const MAX_FAILED_ROWS = 100;

/** The workflow runs that one report counts: the newest, as many as one read gives. */
export const WORKFLOW_RUNS_READ = MAX_WORKFLOW_RUNS_LIMIT;

/** How long the report waits for ping replies, in milliseconds. */
export const PING_WINDOW_MS = 2_000;

/** An error of a monitoring or health read: a code with a message, or a refused query field. */
type ReadFailure =
  | { readonly code: string; readonly message: string }
  | { readonly code: string; readonly field: string; readonly reason: string }
  | { readonly code: string; readonly subject: string; readonly cause: string };

function readFailureText(failure: ReadFailure): string {
  if ("message" in failure) {
    return `${failure.code}: ${failure.message}`;
  }

  return "field" in failure ? `${failure.code}: ${failure.field}: ${failure.reason}` : `${failure.code}: ${failure.subject}: ${failure.cause}`;
}

/** The line of a read: its text, or the read's failure. Pure. */
function lineOf<T>(label: string, read: Result<T, ReadFailure>, text: (value: T) => string): string {
  return read.ok ? `${label}: ${text(read.value)}` : `${label}: the read failed: ${readFailureText(read.error)}`;
}

function statusCountsText(counts: readonly StatusCount[]): string {
  const present = counts.filter((count) => count.count > 0);

  return present.length === 0 ? "none" : present.map((count) => `${count.status} ${String(count.count)}`).join(", ");
}

function failedRowText(row: TaskSummary): string {
  return `  ${row.taskName} ${row.errorCode ?? "no code"} ${row.taskId} (retries ${String(row.retryCount)})`;
}

function workflowRunsText(runs: readonly WorkflowRunSummary[]): string {
  const counts = new Map<string, number>();

  for (const run of runs) {
    const key = `${run.workflowName} ${run.status}`;

    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  return counts.size === 0
    ? "none"
    : [...counts.entries()]
        .toSorted(([first], [second]) => first.localeCompare(second))
        .map(([key, count]) => `${key} ${String(count)}`)
        .join(", ");
}

function schedulesText(states: readonly ScheduleStateInfo[]): string {
  const sent = states.reduce((slots, state) => slots + state.runCount, 0);

  return `${String(states.length)} with a stored state, ${String(sent)} slots sent`;
}

/** The lines of the report of `app`, with at most `failedRows` failed tasks. Effects: the reads. */
export async function reportLines(app: Horsies, failedRows: number): Promise<readonly string[]> {
  const ping = await app.pingDatabase();
  const workers = await app.monitoring.listWorkerStates();
  const pongs = await app.pingWorkers({ waitMs: PING_WINDOW_MS });
  const stats = await app.monitoring.taskStatusCounts();
  const failed = await app.monitoring.listTasks({ statuses: ["FAILED"], limit: failedRows });
  const runs = await app.monitoring.listWorkflowRuns({ limit: WORKFLOW_RUNS_READ });

  const lines = [
    lineOf("database", ping, (value) => `${value.latencyMs.toFixed(1)} ms`),
    lineOf("workers", workers, (value) => `${String(value.length)} live`),
    lineOf("ping replies", pongs, (value) => value.map((pong) => `${pong.hostname}:${String(pong.pid)}`).join(", ") || "none"),
    lineOf("tasks", stats, statusCountsText),
    lineOf("failed tasks", failed, (page) => `${String(page.total)} in all, the newest ${String(page.rows.length)}:`),
  ];

  if (failed.ok) {
    lines.push(...failed.value.rows.map(failedRowText));
  }

  lines.push(lineOf(`workflows (the newest ${String(WORKFLOW_RUNS_READ)})`, runs, workflowRunsText));
  lines.push(lineOf("schedules", await app.monitoring.listSchedules(), schedulesText));

  return lines;
}

/** The arguments of `report`. */
export type ReportArguments = { readonly failedRows: number };

/** The arguments of `report`, or the usage error. Pure. */
export function parseReportArguments(args: readonly string[]): Result<ReportArguments, string> {
  let failedText: string | undefined;

  try {
    failedText = parseArgs({ args: [...args], options: { failed: { type: "string" } }, strict: true }).values.failed;
  } catch (cause) {
    return err(cause instanceof Error ? cause.message : String(cause));
  }

  if (failedText === undefined) {
    return ok({ failedRows: DEFAULT_FAILED_ROWS });
  }

  const failedRows = Number(failedText);

  if (!Number.isSafeInteger(failedRows) || failedRows < 1 || failedRows > MAX_FAILED_ROWS) {
    return err(`--failed must be a whole number from 1 to ${String(MAX_FAILED_ROWS)}`);
  }

  return ok({ failedRows });
}
