import { expect } from "bun:test";

import type { BrokerCoreError } from "../../../horsies/src/_catalog/broker_errors.ts";
import type { CoreCause, NoCause } from "../../../horsies/src/_catalog/error_detail.ts";
import type {
  MonitoringQueryCoreError,
  TaskActionCoreError,
} from "../../../horsies/src/_catalog/monitoring_errors.ts";
import type { TaskSendCoreError } from "../../../horsies/src/_catalog/task_send_errors.ts";
import type { HandleCoreError } from "../../../horsies/src/_catalog/workflow_handle_errors.ts";
import type { WorkflowStartCoreError } from "../../../horsies/src/_catalog/workflow_start_errors.ts";

/** A value of any generated catalog error union. */
export type CatalogError =
  | BrokerCoreError
  | MonitoringQueryCoreError
  | TaskActionCoreError
  | TaskSendCoreError
  | HandleCoreError
  | WorkflowStartCoreError;

/** What a converter gives for one sample: the enumerable part, the hidden names, the message and the cause. */
export interface Converted {
  readonly shown: Partial<CatalogError>;
  readonly hidden: ReadonlyArray<string>;
  readonly message: string;
  readonly cause: CoreCause | NoCause | null;
}

/** `value` is frozen and equals `expected`; its cause is a frozen copy. */
export function expectConverted(value: CatalogError, expected: Converted): void {
  expect<Partial<CatalogError>>(value).toEqual(expected.shown);
  expect(hiddenNames(value)).toEqual(expected.hidden);
  expect(value.message).toBe(expected.message);
  expect(Object.isFrozen(value)).toBe(true);

  const cause = causeOf(value);

  expect(cause).toEqual(expected.cause);

  if (cause !== null) {
    expect(cause).not.toBe(expected.cause);
    expect(Object.isFrozen(cause)).toBe(true);
  }
}

/** The names of the own properties of `value` that are not enumerable, in definition order. */
export function hiddenNames(value: CatalogError): ReadonlyArray<string> {
  return Object.entries(Object.getOwnPropertyDescriptors(value))
    .filter(([, descriptor]) => descriptor.enumerable === false)
    .map(([name]) => name);
}

/** The cause of `value`, or `null` for a class without one. */
export function causeOf(value: CatalogError): CoreCause | NoCause | null {
  return "exception" in value ? value.exception : null;
}

export const TASK_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";

export const OTHER_TASK_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5c";

export const WORKFLOW_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5d";

export const CONNECTION_FAILURE: CoreCause = {
  kind: "coreFailure",
  failure: "connection",
  subject: "enqueue",
  message: "connection refused",
  retryable: true,
};

export const SERVER_FAILURE: CoreCause = {
  kind: "coreFailure",
  failure: "server",
  subject: "cancel_task",
  message: "canceling statement due to statement timeout",
  retryable: true,
  sqlstate: "57014",
};

export const CONTRACT_VIOLATION: CoreCause = {
  kind: "coreFailure",
  failure: "contractViolation",
  subject: "task_info",
  message: "the stored status is not a status",
};

export const RULE_REFUSED: CoreCause = {
  kind: "ruleRefused",
  message: "priority must be between 1 and 100",
};

export const JSON_UNREADABLE: CoreCause = {
  kind: "jsonUnreadable",
  message: "expected value at line 1 column 1",
};

export const HISTORY_CONTRACT: CoreCause = {
  kind: "historyContract",
  message: "a period row has no class",
};

export const NO_CAUSE: NoCause = { kind: "noCause" };
