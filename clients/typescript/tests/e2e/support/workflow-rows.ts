// Reads and writes of the workflow tables, for the waits and checks that the client API does not offer (Rust
// `clients/rust/tests/support/src/rows.rs`, the workflow reads; Python `clients/python/tests/e2e/helpers/workflow.py`).
//
// Each read fails the test with its subject on a database error. Values enter the SQL text only as an id that
// `sqlText` quotes, or as an integer.

import assert from "node:assert/strict";

import { TaskId, WorkflowId } from "../../../horsies/src/index.ts";
import { POLL_INTERVAL_MS, pause } from "./process.ts";
import { MICROS, pollUntil, type RowTime, rowTime, setTime, sqlText, type TaskRows } from "./rows.ts";

/** The statuses that end a workflow. */
export const WORKFLOW_END_STATUSES = ["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"] as const;

/** A status that ends a workflow. */
export type WorkflowEndStatus = (typeof WORKFLOW_END_STATUSES)[number];

/** One node of a workflow (`horsies_workflow_tasks`). */
export type NodeRow = {
  readonly taskIndex: number;
  readonly nodeId: string | null;
  readonly taskName: string;
  readonly status: string;
  /** The task of a task node, once the node has one. */
  readonly taskId: string | null;
  /** The child workflow of a sub-workflow node, once the node started it. */
  readonly subWorkflowId: string | null;
  /** The stored result envelope text. */
  readonly result: string | null;
  readonly startedAt: RowTime;
  readonly completedAt: RowTime;
};

/** One workflow and its place in its tree (`horsies_workflows`). */
export type WorkflowRow = {
  readonly id: string;
  readonly name: string;
  readonly status: string;
  readonly result: string | null;
  readonly error: string | null;
  readonly parentWorkflowId: string | null;
  readonly parentTaskIndex: number | null;
  readonly depth: number;
  readonly rootWorkflowId: string | null;
};

const NODE_COLUMNS = `task_index::text, node_id::text, task_name::text, status::text, task_id::text, sub_workflow_id::text,
  result, ${MICROS("started_at")}, ${MICROS("completed_at")}`;

const WORKFLOW_COLUMNS = `id::text, name::text, status::text, result, error, parent_workflow_id::text, parent_task_index::text,
  depth::text, root_workflow_id::text`;

/** The status of workflow `workflowId`, or `absent` when it has no row. */
export async function workflowStatus(rows: TaskRows, workflowId: string): Promise<string> {
  const read = await rows.query(
    `read the status of workflow ${workflowId}`,
    `SELECT status::text FROM horsies_workflows WHERE id = ${sqlText(workflowId)}`,
  );

  return read.rows[0]?.[0] ?? "absent";
}

/** Wait until workflow `workflowId` has `status`. Fails the test when `limitMs` passes first. */
export async function waitForWorkflowStatus(rows: TaskRows, workflowId: string, status: string, limitMs: number): Promise<void> {
  await pollUntil(limitMs, `workflow ${workflowId} reaches ${status}`, async () => (await workflowStatus(rows, workflowId)) === status);
}

/** What one probe of `pollFor` found: the value, or nothing yet. */
type Probe<T> = { readonly kind: "found"; readonly value: T } | { readonly kind: "notYet"; readonly last: string };

/** Probe every `POLL_INTERVAL_MS` until `probe` finds its value. Fails the test with `subject` and the last read. */
async function pollFor<T>(limitMs: number, subject: string, probe: () => Promise<Probe<T>>): Promise<T> {
  const deadline = performance.now() + limitMs;

  for (;;) {
    const found = await probe();

    if (found.kind === "found") {
      return found.value;
    }

    if (performance.now() >= deadline) {
      assert.fail(`${subject}: not reached within ${String(limitMs)} ms (last: ${found.last})`);
    }

    await pause(POLL_INTERVAL_MS);
  }
}

/** Wait until workflow `workflowId` has an end status, and give it. Fails the test when `limitMs` passes first. */
export async function waitForWorkflowEnd(rows: TaskRows, workflowId: string, limitMs: number): Promise<WorkflowEndStatus> {
  return pollFor(limitMs, `workflow ${workflowId} ends`, async () => {
    const status = await workflowStatus(rows, workflowId);
    const ended = WORKFLOW_END_STATUSES.find((end) => end === status);

    return ended === undefined ? { kind: "notYet", last: status } : { kind: "found", value: ended };
  });
}

/** The nodes of workflow `workflowId`, by task index. */
export async function workflowNodes(rows: TaskRows, workflowId: string): Promise<readonly NodeRow[]> {
  const read = await rows.query(
    `read the nodes of workflow ${workflowId}`,
    `SELECT ${NODE_COLUMNS} FROM horsies_workflow_tasks WHERE workflow_id = ${sqlText(workflowId)} ORDER BY task_index`,
  );

  return read.rows.map((row) => nodeRowOf(row));
}

/** The node of `workflowId` with the node id `nodeId`; fails the test when the workflow has none. */
export async function workflowNode(rows: TaskRows, workflowId: string, nodeId: string): Promise<NodeRow> {
  return nodeById(await workflowNodes(rows, workflowId), nodeId);
}

/** The node with the node id `nodeId` of `nodes`; fails the test when there is none. Pure. */
export function nodeById(nodes: readonly NodeRow[], nodeId: string): NodeRow {
  const found = nodes.find((node) => node.nodeId === nodeId);

  if (found === undefined) {
    assert.fail(`no node ${nodeId} in ${JSON.stringify(nodes.map((node) => node.nodeId))}`);
  }

  return found;
}

/** The node at `taskIndex` of `nodes`; fails the test when there is none. Pure. */
export function nodeAt(nodes: readonly NodeRow[], taskIndex: number): NodeRow {
  const found = nodes.find((node) => node.taskIndex === taskIndex);

  if (found === undefined) {
    assert.fail(`no node at index ${String(taskIndex)} in ${JSON.stringify(nodes.map((node) => node.taskIndex))}`);
  }

  return found;
}

/** The statuses of `nodes`, by task index. Pure. */
export function statusesOf(nodes: readonly NodeRow[]): readonly string[] {
  return nodes.map((node) => node.status);
}

/** Read the node `nodeId` of `workflowId` until `reached` answers true, and give it. Fails the test at `limitMs`. */
export async function waitForNode(
  rows: TaskRows,
  workflowId: string,
  nodeId: string,
  limitMs: number,
  subject: string,
  reached: (node: NodeRow) => boolean,
): Promise<NodeRow> {
  return pollFor(limitMs, `node ${nodeId} of workflow ${workflowId}: ${subject}`, async () => {
    const nodes = await workflowNodes(rows, workflowId);
    const found = nodes.find((node) => node.nodeId === nodeId);

    if (found === undefined) {
      return { kind: "notYet", last: "no such node" };
    }

    return reached(found) ? { kind: "found", value: found } : { kind: "notYet", last: nodesText([found]) };
  });
}

/** The text of a row time, for a message. Pure. */
function timeText(time: RowTime): string {
  switch (time.kind) {
    case "set":
      return String(time.epochMicros);
    case "notSet":
      return "not set";
  }
}

/** The child workflow of the sub-workflow node `nodeId`, once the node started it. */
export async function childWorkflowOf(rows: TaskRows, workflowId: string, nodeId: string, limitMs: number): Promise<WorkflowId> {
  const node = await waitForNode(rows, workflowId, nodeId, limitMs, "starts its child", (found) => found.subWorkflowId !== null);

  return childIdOf(node);
}

/** The child workflow of the sub-workflow node `node`; fails the test when it has none. Pure. */
export function childIdOf(node: NodeRow): WorkflowId {
  const parsed = WorkflowId.parse(node.subWorkflowId ?? "");

  if (!parsed.ok) {
    assert.fail(`node ${String(node.nodeId)} has no child workflow id: ${String(node.subWorkflowId)}`);
  }

  return parsed.value;
}

/** The task of the task node `nodeId`, once the node has one. */
export async function taskOfNode(rows: TaskRows, workflowId: string, nodeId: string, limitMs: number): Promise<TaskId> {
  const node = await waitForNode(rows, workflowId, nodeId, limitMs, "gets its task", (found) => found.taskId !== null);
  const parsed = TaskId.parse(node.taskId ?? "");

  if (!parsed.ok) {
    assert.fail(`node ${nodeId} has no task id: ${String(node.taskId)}`);
  }

  return parsed.value;
}

/** The nodes of workflow `workflowId` once it ended with `status` within `limitMs`. Fails the test for another end. */
export async function endedNodes(
  rows: TaskRows,
  workflowId: string,
  status: WorkflowEndStatus,
  limitMs: number,
): Promise<readonly NodeRow[]> {
  const ended = await waitForWorkflowEnd(rows, workflowId, limitMs);
  const nodes = await workflowNodes(rows, workflowId);

  assert.equal(ended, status, `workflow ${workflowId} ended ${ended}: ${nodesText(nodes)}`);

  return nodes;
}

/** Check that node `nodeId` of `nodes` started after each node of `sources` ended (strictly). Pure. */
export function startedAfterEnd(nodes: readonly NodeRow[], nodeId: string, sources: readonly string[]): void {
  const started = setTime(nodeById(nodes, nodeId).startedAt, `the start of node ${nodeId}`);

  for (const source of sources) {
    const ended = setTime(nodeById(nodes, source).completedAt, `the end of node ${source}`);

    assert.ok(started > ended, `node ${nodeId} started at ${String(started)}, before node ${source} ended at ${String(ended)}`);
  }
}

/** The nodes as a text, for a message: index, node id, status, start and end of each. Pure. */
export function nodesText(nodes: readonly NodeRow[]): string {
  return nodes
    .map((node) => `${String(node.taskIndex)} ${String(node.nodeId)} ${node.status} ${timeText(node.startedAt)} ${timeText(node.completedAt)}`)
    .join("; ");
}

/** The row of workflow `workflowId`; fails the test when it has none. */
export async function workflowRow(rows: TaskRows, workflowId: string): Promise<WorkflowRow> {
  const read = await rows.query(
    `read workflow ${workflowId}`,
    `SELECT ${WORKFLOW_COLUMNS} FROM horsies_workflows WHERE id = ${sqlText(workflowId)}`,
  );

  const [row] = read.rows;

  assert.ok(row !== undefined, `workflow ${workflowId} has no row`);

  return workflowRowOf(row);
}

/** The workflow `rootId` and its descendants, by depth, then id (Python `_workflow_tree_rows`). */
export async function workflowTree(rows: TaskRows, rootId: string): Promise<readonly WorkflowRow[]> {
  const read = await rows.query(
    `read the tree of workflow ${rootId}`,
    `SELECT ${WORKFLOW_COLUMNS} FROM horsies_workflows WHERE id = ${sqlText(rootId)} OR root_workflow_id = ${sqlText(rootId)}
     ORDER BY depth, id`,
  );

  return read.rows.map((row) => workflowRowOf(row));
}

/** Run one statement on the workflow tables that must change `expectedRows` rows (`UPDATE <n>`). */
export async function changeWorkflowRows(rows: TaskRows, subject: string, sql: string, expectedRows: number): Promise<void> {
  const changed = await rows.query(subject, sql);

  assert.equal(changed.tag, `UPDATE ${String(expectedRows)}`, subject);
}

function nodeRowOf(row: readonly (string | null)[]): NodeRow {
  return {
    taskIndex: Number(requiredText(row, 0)),
    nodeId: row[1] ?? null,
    taskName: requiredText(row, 2),
    status: requiredText(row, 3),
    taskId: row[4] ?? null,
    subWorkflowId: row[5] ?? null,
    result: row[6] ?? null,
    startedAt: rowTime(row[7] ?? null),
    completedAt: rowTime(row[8] ?? null),
  };
}

function workflowRowOf(row: readonly (string | null)[]): WorkflowRow {
  const parentTaskIndex = row[6] ?? null;

  return {
    id: requiredText(row, 0),
    name: requiredText(row, 1),
    status: requiredText(row, 2),
    result: row[3] ?? null,
    error: row[4] ?? null,
    parentWorkflowId: row[5] ?? null,
    parentTaskIndex: parentTaskIndex === null ? null : Number(parentTaskIndex),
    depth: Number(requiredText(row, 7)),
    rootWorkflowId: row[8] ?? null,
  };
}

function requiredText(row: readonly (string | null)[], index: number): string {
  const value = row[index];

  if (value === undefined || value === null) {
    assert.fail(`a workflow row has no value in column ${String(index)}`);
  }

  return value;
}
