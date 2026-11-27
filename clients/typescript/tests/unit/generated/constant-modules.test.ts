import { describe, expect, test } from "bun:test";

import { TASK_LIST_ROW_BOUND } from "../../../horsies/src/_catalog/broker_bounds.ts";
import {
  MAX_ATTEMPT,
  MAX_EVENT_CUSTOM_CODE_CHARS,
  MAX_EVENT_HANDLERS,
  MAX_EVENT_QUEUE_NAME_CHARS,
  MAX_EVENT_TASK_NAME_CHARS,
  MAX_EVENT_TRACE_CARRIER_CHARS,
  MAX_EVENT_WORKER_ID_CHARS,
  MAX_HANDLER_NAME_CHARS,
  MAX_RETRY_COUNT,
  UNKNOWN_HANDLER_NAME,
} from "../../../horsies/src/_catalog/event_bounds.ts";
import {
  BAD_ARGUMENTS_EXIT_CODE,
  BAD_PARENT_FRAME_EXIT_CODE,
  BOOT_FAILED_EXIT_CODE,
  CHILD_HOOK_FAILED_EXIT_CODE,
  CORE_STOPPED_EXIT_CODE,
  LIVENESS_LOCK_LOST_EXIT_CODE,
  PARENT_GONE_EXIT_CODE,
  PROTOCOL_MISMATCH_EXIT_CODE,
  REPARENT_FAILED_EXIT_CODE,
  RUNNER_HEARTBEAT_LOST_EXIT_CODE,
  STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
} from "../../../horsies/src/_catalog/exit_codes.ts";
import {
  LIST_ROW_BOUND,
  MAX_BREAKDOWN_LIMIT,
  MAX_TASK_PAGE_LIMIT,
  MAX_TASK_PAGE_REACH,
  MAX_WORKFLOW_RUNS_LIMIT,
  MONITORING_WINDOW_DEFAULT_SECONDS,
  MONITORING_WINDOW_MAX_SECONDS,
} from "../../../horsies/src/_catalog/monitoring_bounds.ts";
import { MAX_WAIT_LIMIT_MS } from "../../../horsies/src/_catalog/send_bounds.ts";
import * as sendBounds from "../../../horsies/src/send/bounds.ts";

// The largest exit status that a process reports to its parent.
const maxExitStatus = 255;

const exitCodes = [
  PARENT_GONE_EXIT_CODE,
  BAD_PARENT_FRAME_EXIT_CODE,
  BAD_ARGUMENTS_EXIT_CODE,
  STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
  BOOT_FAILED_EXIT_CODE,
  REPARENT_FAILED_EXIT_CODE,
  RUNNER_HEARTBEAT_LOST_EXIT_CODE,
  PROTOCOL_MISMATCH_EXIT_CODE,
  LIVENESS_LOCK_LOST_EXIT_CODE,
  CORE_STOPPED_EXIT_CODE,
  CHILD_HOOK_FAILED_EXIT_CODE,
];

const bounds = [
  MONITORING_WINDOW_DEFAULT_SECONDS,
  MONITORING_WINDOW_MAX_SECONDS,
  MAX_TASK_PAGE_REACH,
  MAX_TASK_PAGE_LIMIT,
  MAX_BREAKDOWN_LIMIT,
  MAX_WORKFLOW_RUNS_LIMIT,
  LIST_ROW_BOUND,
  MAX_ATTEMPT,
  MAX_RETRY_COUNT,
  MAX_EVENT_TASK_NAME_CHARS,
  MAX_EVENT_QUEUE_NAME_CHARS,
  MAX_EVENT_CUSTOM_CODE_CHARS,
  MAX_EVENT_TRACE_CARRIER_CHARS,
  MAX_EVENT_WORKER_ID_CHARS,
  TASK_LIST_ROW_BOUND,
  MAX_EVENT_HANDLERS,
  MAX_HANDLER_NAME_CHARS,
  MAX_WAIT_LIMIT_MS,
];

describe("generated constant modules", () => {
  test("every bound and exit code is a safe integer", () => {
    for (const value of [...bounds, ...exitCodes]) {
      expect(Number.isSafeInteger(value), String(value)).toBe(true);
    }
  });

  test("the exit codes are distinct exit statuses", () => {
    expect(new Set(exitCodes).size).toBe(exitCodes.length);

    for (const code of exitCodes) {
      expect(code).toBeGreaterThanOrEqual(0);
      expect(code).toBeLessThanOrEqual(maxExitStatus);
    }
  });

  test("the largest attempt is one more than the largest retry count", () => {
    expect(MAX_ATTEMPT).toBe(MAX_RETRY_COUNT + 1);
  });

  test("the longest result wait is the largest u32 of milliseconds, and the send bounds hold no copy", () => {
    expect(MAX_WAIT_LIMIT_MS).toBe(2 ** 32 - 1);
    expect(Object.keys(sendBounds)).not.toContain("MAX_WAIT_LIMIT_MS");
  });

  test("the label value of an unknown handler fits the length rule of a handler name", () => {
    expect(UNKNOWN_HANDLER_NAME.length).toBeGreaterThan(0);
    expect(UNKNOWN_HANDLER_NAME.length).toBeLessThanOrEqual(MAX_HANDLER_NAME_CHARS);
  });
});
