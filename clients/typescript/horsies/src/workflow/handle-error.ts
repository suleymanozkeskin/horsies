// The errors of a workflow handle: the handle catalog of the core, generated (`src/_catalog/workflow_handle_errors.ts`),
// told apart by `code`; and the errors of a read of an outcome (`get`, `resultFor`), told apart by `kind`.

import type { HandleErrorCode } from "../_catalog/workflow_handle.ts";
import {
  type HandleCoreError,
  handleCoreErrorFromNative,
  type HandleStoredDataInvalid,
  type HandleWorkflowNotFound,
  type NativeHandleCoreError,
} from "../_catalog/workflow_handle_errors.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import { nativeCatalogValue } from "../send/catalog-value.ts";
import { MAX_WAIT_LIMIT_MS } from "../send/bounds.ts";
import type { WaitBound } from "../native/sends/addon.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";

/**
 * The error of a handle call (`status`, `results`, `tasks`, `pause`, `resume`, `cancel`): the handle catalog of the
 * core, or, in a `SHARED_THREAD` body, the seat relay at its bound (nothing was sent).
 */
export type WorkflowHandleError = HandleCoreError | SeatRelayFull;

/** Why a read of a workflow outcome failed. A read error is not the workflow's outcome. */
export type WorkflowReadError =
  /** The workflow had not ended when the wait ended; it may still run. */
  | { readonly kind: "waitTimedOut" }
  | { readonly kind: "workflowNotFound" }
  /** The node stores no result yet, or the workflow has no node of this id. */
  | { readonly kind: "resultNotReady" }
  /** The stored value does not read with the handle's types. `cause` holds no value. */
  | { readonly kind: "storedResultUnreadable"; readonly cause: string }
  /** The stored error has a user code that the node's contract does not list. */
  | { readonly kind: "unknownUserCode"; readonly code: string }
  /** A wait in a process that runs task bodies; nothing was read. */
  | { readonly kind: "waitInsideTask" }
  /** The app does not list the workflow, so its output has no read form; nothing was read. */
  | { readonly kind: "workflowNotListed"; readonly workflowName: string }
  /** The database or the connection failed. */
  | {
      readonly kind: "database";
      readonly code: HandleErrorCode;
      readonly retryable: boolean;
      readonly message: string;
    }
  /** A `timeoutMs` that is not a whole number of milliseconds from 0 to `maxMs`; nothing was read. */
  | { readonly kind: "invalidWait"; readonly maxMs: number }
  /** In a `SHARED_THREAD` body: the seat relay held its bound of waiting calls; nothing was read; retry. */
  | { readonly kind: "seatRelayFull"; readonly error: SeatRelayFull };

/** The public error of a raw record of the handle catalog. An unknown class throws (a build defect). */
export function handleErrorOfRaw(raw: RawCatalogError): HandleCoreError {
  // SAFETY: the bridge builds the slots of a handle class from the core's slot list of that class
  // (`ErrorClassValues`), the list from which the codegen renders `NativeHandleCoreError`; the generated converter
  // throws for a class name that it does not know.
  return handleCoreErrorFromNative(nativeCatalogValue(raw) as unknown as NativeHandleCoreError);
}

/** `WORKFLOW_NOT_FOUND`: no workflow has the id. Pure. */
export function workflowNotFound(workflowId: string): HandleWorkflowNotFound {
  const error = handleCoreErrorFromNative({
    class: "HandleWorkflowNotFound",
    slots: { message: `workflow ${workflowId} not found`, workflow_id: workflowId },
  });

  return error.code === "WORKFLOW_NOT_FOUND" ? error : wrongCode(error);
}

/** `STORED_DATA_INVALID`: the handle's decode refused a stored value. Not retryable. Pure. */
export function storedDataInvalid(workflowId: string, message: string): HandleStoredDataInvalid {
  const error = handleCoreErrorFromNative({
    class: "HandleStoredDataInvalid",
    slots: { message, workflow_id: workflowId, exception: { kind: "jsonUnreadable", message } },
  });

  return error.code === "STORED_DATA_INVALID" ? error : wrongCode(error);
}

/** The read error of a handle error: not found, unreadable, or the database. Pure. */
export function readErrorOfHandle(error: HandleCoreError): WorkflowReadError {
  switch (error.code) {
    case "WORKFLOW_NOT_FOUND":
      return Object.freeze({ kind: "workflowNotFound" });
    case "STORED_DATA_INVALID":
      return Object.freeze({ kind: "storedResultUnreadable", cause: error.message });
    case "DB_OPERATION_FAILED":
    case "RESUME_RECOVERY_FAILED":
      return Object.freeze({ kind: "database", code: error.code, retryable: error.retryable, message: error.message });
    default:
      return unreachable(error, "readErrorOfHandle");
  }
}

/** The wait of a `timeoutMs`, or `invalidWait`. Left out: no bound. Pure. */
export function workflowWait(timeoutMs: number | undefined): Result<WaitBound, WorkflowReadError> {
  if (timeoutMs === undefined) {
    return ok({ kind: "untilEnd" });
  }

  return Number.isSafeInteger(timeoutMs) && timeoutMs >= 0 && timeoutMs <= MAX_WAIT_LIMIT_MS
    ? ok({ kind: "upTo", milliseconds: timeoutMs })
    : err(Object.freeze({ kind: "invalidWait", maxMs: MAX_WAIT_LIMIT_MS }));
}

function wrongCode(error: HandleCoreError): never {
  throw new TypeError(`handleCoreErrorFromNative: the converter gave the code ${error.code} for another class`);
}
