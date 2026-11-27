import { describe, expect, test } from "bun:test";

import { HorsiesErrorCode } from "../../../horsies/src/_catalog/error_codes.ts";
import {
  TASK_TERMINAL_STATES,
  TaskStatus,
  WORKFLOW_NODE_TERMINAL_STATES,
  WORKFLOW_TERMINAL_STATES,
  WorkflowStatus,
  WorkflowNodeStatus,
} from "../../../horsies/src/_catalog/status.ts";
import { RetrievalCode } from "../../../horsies/src/_catalog/task_codes.ts";
import { TaskSendErrorCode } from "../../../horsies/src/_catalog/task_send.ts";

describe("code catalogs", () => {
  test("a member name maps to its wire value", () => {
    expect(TaskStatus.PENDING).toBe("PENDING");
    expect(HorsiesErrorCode.WORKFLOW_NO_NAME).toBe("HRS-001");
  });

  test("a member that only a blocking form gives stays in its catalog", () => {
    expect(RetrievalCode.ASYNC_CONTEXT).toBe("ASYNC_CONTEXT");
    expect(Object.values(TaskSendErrorCode)).toContain("ASYNC_CONTEXT");
  });

  test("the task terminal set holds the four final statuses", () => {
    expect([...TASK_TERMINAL_STATES]).toEqual([
      TaskStatus.COMPLETED,
      TaskStatus.FAILED,
      TaskStatus.CANCELLED,
      TaskStatus.EXPIRED,
    ]);
    expect(TASK_TERMINAL_STATES.has(TaskStatus.RUNNING)).toBe(false);
  });

  test("the workflow terminal set holds the four final statuses", () => {
    expect([...WORKFLOW_TERMINAL_STATES]).toEqual([
      WorkflowStatus.COMPLETED,
      WorkflowStatus.FAILED,
      WorkflowStatus.CANCELLED,
      WorkflowStatus.EXPIRED,
    ]);
    expect(WORKFLOW_TERMINAL_STATES.has(WorkflowStatus.PAUSED)).toBe(false);
  });

  test("the workflow task terminal set holds the three final statuses", () => {
    expect([...WORKFLOW_NODE_TERMINAL_STATES]).toEqual([
      WorkflowNodeStatus.COMPLETED,
      WorkflowNodeStatus.FAILED,
      WorkflowNodeStatus.SKIPPED,
    ]);
    expect(WORKFLOW_NODE_TERMINAL_STATES.has(WorkflowNodeStatus.ENQUEUED)).toBe(false);
  });
});
