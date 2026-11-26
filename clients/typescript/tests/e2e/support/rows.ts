// Reads and writes of the task tables, for the waits and checks that the client API does not offer (Rust
// `clients/rust/tests/support/src/rows.rs`).
//
// A task that reaches a terminal status leaves `horsies_tasks` and gets its row in `horsies_task_history` in one
// transaction, so every task read here looks at both tables in one statement. Each read fails the test with its
// subject on a database error. Values enter the SQL text only as a task id (a canonical UUID, by its type), an
// integer, or a literal that `sqlText` quotes.

import assert from "node:assert/strict";

import type { TaskId } from "../../../horsies/src/index.ts";
import { postgresErrorText, type PostgresSession, type QueryRows } from "./postgres/client.ts";
import { POLL_INTERVAL_MS, pause } from "./process.ts";

/** A time that a row may not hold, in microseconds since 1970. */
export type RowTime = { readonly kind: "set"; readonly epochMicros: bigint } | { readonly kind: "notSet" };

/** One task as the tests read it, from whichever table holds it. */
export type TaskRow = {
  readonly taskId: string;
  readonly taskName: string;
  readonly queueName: string;
  readonly priority: number;
  readonly status: string;
  readonly retryCount: number;
  readonly maxRetries: number;
  /** The stored error code; only a history row has one. */
  readonly errorCode: string | null;
  /** The worker that holds the claim now (live), or that held it last (history). */
  readonly workerId: string | null;
  readonly claimedAt: RowTime;
  readonly startedAt: RowTime;
  /** The time of the terminal status; only a history row has one. */
  readonly terminalAt: RowTime;
  readonly isWorkflowTask: boolean;
};

/** The SQL text of a time column as microseconds since 1970, as text. Pure. */
export const MICROS = (column: string): string => `floor(extract(epoch FROM ${column}) * 1000000)::bigint::text`;

/** Both tables as one relation with the columns of `TaskRow`. */
const TASK_ROWS = `(SELECT id::text AS task_id, task_name::text, queue_name::text, priority::text, status::text,
  retry_count::text, max_retries::text, NULL::text AS error_code, claimed_by_worker_id::text AS worker_id,
  ${MICROS("claimed_at")} AS claimed_at, ${MICROS("started_at")} AS started_at, NULL::text AS terminal_at,
  is_workflow_task::text
  FROM horsies_tasks
  UNION ALL
  SELECT task_id::text, task_name::text, queue_name::text, priority::text, status::text, retry_count::text,
  max_retries::text, error_code::text, last_claimed_worker_id::text AS worker_id, ${MICROS("claimed_at")},
  ${MICROS("started_at")}, ${MICROS("terminal_at")}, is_workflow_task::text
  FROM horsies_task_history) AS task_rows`;

const TASK_COLUMNS =
  "task_id, task_name, queue_name, priority, status, retry_count, max_retries, error_code, worker_id, claimed_at, started_at, terminal_at, is_workflow_task";

/** A SQL string literal of `text`, with `standard_conforming_strings` on (the server default). Pure. */
export function sqlText(text: string): string {
  return `'${text.replaceAll("'", "''")}'`;
}

/** The task reads of one test database. */
export class TaskRows {
  readonly #session: PostgresSession;

  constructor(session: PostgresSession) {
    this.#session = session;
  }

  /** The rows of `sql`. Fails the test with `subject` on a database error. */
  async query(subject: string, sql: string): Promise<QueryRows> {
    const rows = await this.#session.query(sql);

    if (!rows.ok) {
      assert.fail(`${subject}: ${postgresErrorText(rows.error)}`);
    }

    return rows.value;
  }

  /** The one integer of `sql` (a `count(*)`). */
  async count(subject: string, sql: string): Promise<number> {
    const rows = await this.query(subject, sql);
    const value = rows.rows[0]?.[0];

    if (value === undefined || value === null) {
      assert.fail(`${subject}: the statement gave no value`);
    }

    return Number(value);
  }

  /** The row of `taskId`, or `absent` when neither table holds it. */
  async readTask(taskId: TaskId): Promise<TaskRow | "absent"> {
    const rows = await this.query(`read task ${taskId}`, `SELECT ${TASK_COLUMNS} FROM ${TASK_ROWS} WHERE task_id = ${sqlText(taskId)}`);
    const [row] = rows.rows;

    return row === undefined ? "absent" : taskRowOf(row);
  }

  /** The row of `taskId`; fails the test when neither table holds it. */
  async taskRow(taskId: TaskId): Promise<TaskRow> {
    const row = await this.readTask(taskId);

    if (row === "absent") {
      assert.fail(`task ${taskId} has no row`);
    }

    return row;
  }

  /** The rows of every task named `taskName`. */
  async tasksNamed(taskName: string): Promise<readonly TaskRow[]> {
    const rows = await this.query(
      `read the tasks named ${taskName}`,
      `SELECT ${TASK_COLUMNS} FROM ${TASK_ROWS} WHERE task_name = ${sqlText(taskName)} ORDER BY task_id`,
    );

    return rows.rows.map((row) => taskRowOf(row));
  }

  /** The status of `taskId`, or `absent`. */
  async taskStatus(taskId: TaskId): Promise<string> {
    const row = await this.readTask(taskId);

    return row === "absent" ? "absent" : row.status;
  }

  /** Wait until `taskId` has `status`. Fails the test when `limitMs` passes first. */
  async waitForStatus(taskId: TaskId, status: string, limitMs: number): Promise<void> {
    await pollUntil(limitMs, `task ${taskId} reaches ${status}`, async () => (await this.taskStatus(taskId)) === status);
  }

  /** Wait until every task of `taskIds` has its history row. */
  async waitForTerminal(taskIds: readonly TaskId[], limitMs: number): Promise<void> {
    const ids = taskIds.map((id) => sqlText(id)).join(", ");

    await pollUntil(limitMs, `${String(taskIds.length)} tasks reach a terminal status`, async () => {
      const ended = await this.count("count the ended tasks", `SELECT count(*) FROM horsies_task_history WHERE task_id::text IN (${ids})`);

      return ended === taskIds.length;
    });
  }

  /** The number of live rows with `status`, of `queue` when it names one. */
  async liveCount(status: "RUNNING" | "CLAIMED", queue: string | "anyQueue"): Promise<number> {
    const queueFilter = queue === "anyQueue" ? "" : ` AND queue_name = ${sqlText(queue)}`;

    return this.count(`count the ${status} tasks`, `SELECT count(*) FROM horsies_tasks WHERE status = '${status}'${queueFilter}`);
  }

  /** The number of tasks named `taskName` with `status`, in both tables. */
  async countTasks(taskName: string, status: string): Promise<number> {
    return this.count(
      `count the ${status} tasks named ${taskName}`,
      `SELECT count(*) FROM ${TASK_ROWS} WHERE task_name = ${sqlText(taskName)} AND status = ${sqlText(status)}`,
    );
  }
}

function taskRowOf(row: readonly (string | null)[]): TaskRow {
  const text = (index: number): string => {
    const value = row[index];

    if (value === undefined || value === null) {
      assert.fail(`a task row has no value in column ${String(index)}`);
    }

    return value;
  };

  const optional = (index: number): string | null => row[index] ?? null;

  return {
    taskId: text(0),
    taskName: text(1),
    queueName: text(2),
    priority: Number(text(3)),
    status: text(4),
    retryCount: Number(text(5)),
    maxRetries: Number(text(6)),
    errorCode: optional(7),
    workerId: optional(8),
    claimedAt: rowTime(optional(9)),
    startedAt: rowTime(optional(10)),
    terminalAt: rowTime(optional(11)),
    isWorkflowTask: text(12) === "true",
  };
}

/** The time of a column that `MICROS` read. Pure. */
export function rowTime(value: string | null): RowTime {
  return value === null ? { kind: "notSet" } : { kind: "set", epochMicros: BigInt(value) };
}

/** The microseconds of a set time; fails the test for a time that is not set. */
export function setTime(time: RowTime, subject: string): bigint {
  if (time.kind === "notSet") {
    assert.fail(`${subject} is not set`);
  }

  return time.epochMicros;
}

/** Probe until `probe` answers true, every `POLL_INTERVAL_MS`. Fails the test with `subject` when `limitMs` passes first. */
export async function pollUntil(limitMs: number, subject: string, probe: () => Promise<boolean>): Promise<void> {
  const deadline = performance.now() + limitMs;

  for (;;) {
    if (await probe()) {
      return;
    }

    if (performance.now() >= deadline) {
      assert.fail(`${subject}: not reached within ${String(limitMs)} ms`);
    }

    await pause(POLL_INTERVAL_MS);
  }
}

/** Probe for `durationMs`, every `intervalMs`, and give the largest value that `probe` answered. */
export async function maxDuring(durationMs: number, intervalMs: number, probe: () => Promise<number>): Promise<number> {
  const deadline = performance.now() + durationMs;
  let largest = Number.NEGATIVE_INFINITY;

  while (performance.now() < deadline) {
    largest = Math.max(largest, await probe());
    await pause(intervalMs);
  }

  return largest;
}

/** Probe for `durationMs`, every `POLL_INTERVAL_MS`; fail the test with `subject` when `probe` answers true at any probe. */
export async function neverDuring(durationMs: number, subject: string, probe: () => Promise<boolean>): Promise<void> {
  const deadline = performance.now() + durationMs;

  while (performance.now() < deadline) {
    if (await probe()) {
      assert.fail(`${subject}: happened within ${String(durationMs)} ms`);
    }

    await pause(POLL_INTERVAL_MS);
  }
}
