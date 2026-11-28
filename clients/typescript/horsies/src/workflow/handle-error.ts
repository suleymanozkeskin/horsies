// The errors of a workflow handle: the handle catalog of the core, generated (`src/_catalog/workflow_handle_errors.ts`),
// and the errors of a read of an outcome (`get`, `resultFor`). Both are told apart by `kind`.

import { OperationalCode, RetrievalCode } from "../_catalog/task_codes.ts";
import type { WorkflowHandleErrorCode } from "../_catalog/workflow_handle.ts";
import {
  type HandleCoreError,
  handleCoreErrorFromNative,
  type WorkflowHandleStoredDataInvalid,
  type WorkflowHandleNotFound,
  type NativeHandleCoreError,
} from "../_catalog/workflow_handle_errors.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import { nativeCatalogValue } from "../send/catalog-value.ts";
import { MAX_WAIT_LIMIT_MS } from "../_catalog/send_bounds.ts";
import type { WaitBound } from "../native/sends/addon.ts";
import type { ReadErrorTag } from "../read/errors.ts";
import { err, ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";

/**
 * The error of a handle call (`status`, `results`, `nodes`, `pause`, `resume`, `cancel`): the handle catalog of the
 * core, or, in a `SHARED_THREAD` body, the seat relay at its bound (nothing was sent).
 */
export type WorkflowHandleError = HandleCoreError | SeatRelayFull;

/**
 * Why a read of a workflow outcome failed, told apart by `kind`; `code` is the catalog code of the kind
 * (`ReadErrorCodes`). A read error is not the workflow's outcome. `databaseError` holds the code of the failed handle
 * operation in `databaseCode`, and `unknownCustomCode` holds the stored custom code in `storedCode`.
 */
export type WorkflowReadError =
  /** The workflow had not ended when the wait ended; it may still run. */
  | ReadErrorTag<"waitTimeout">
  | ReadErrorTag<"workflowNotFound">
  /** The workflow has no node of the id. */
  | ReadErrorTag<"nodeNotFound">
  /** The node exists and stores no result yet. */
  | ReadErrorTag<"resultNotReady">
  /** The stored value does not read with the handle's types. `cause` holds no value. */
  | (ReadErrorTag<"storedResultUnreadable"> & { readonly cause: string })
  /** The stored error has a custom code that the node's contract does not list. */
  | (ReadErrorTag<"unknownCustomCode"> & { readonly storedCode: string })
  /** A wait in a process that runs task bodies; nothing was read. */
  | ReadErrorTag<"waitInsideTask">
  /** The app does not list the workflow, so its output has no type to read with; nothing was read. */
  | (ReadErrorTag<"noTypeAvailable"> & { readonly workflowName: string })
  /** The database or the connection failed. */
  | (ReadErrorTag<"databaseError"> & {
      readonly databaseCode: WorkflowHandleErrorCode;
      readonly retryable: boolean;
      readonly message: string;
    })
  /** A `waitMs` that is not a whole number of milliseconds from 0 to `maxMs`; nothing was read. */
  | (ReadErrorTag<"invalidInput"> & { readonly maxMs: number })
  /**
   * In a `SHARED_THREAD` body: the seat relay held its bound of waiting calls; nothing was read; retry. `message` names
   * the bound and its value.
   */
  | (ReadErrorTag<"seatRelayFull"> & { readonly retryable: true; readonly message: string });

/** The public error of a raw record of the handle catalog. An unknown class throws (a build defect). */
export function handleErrorOfRaw(raw: RawCatalogError): HandleCoreError {
  // SAFETY: the bridge builds the slots of a handle class from the core's slot list of that class
  // (`ErrorClassValues`), the list from which the codegen renders `NativeHandleCoreError`; the generated converter
  // throws for a class name that it does not know.
  return handleCoreErrorFromNative(nativeCatalogValue(raw) as unknown as NativeHandleCoreError);
}

/** `WORKFLOW_NOT_FOUND`: no workflow has the id. Pure. */
export function workflowNotFound(workflowId: string): WorkflowHandleNotFound {
  const error = handleCoreErrorFromNative({
    class: "WorkflowHandleNotFound",
    slots: { message: `workflow ${workflowId} not found`, workflow_id: workflowId },
  });

  return error.kind === "notFound" ? error : wrongClass(error);
}

/** `STORED_DATA_INVALID`: the handle's decode refused a stored value. Not retryable. Pure. */
export function storedDataInvalid(workflowId: string, message: string): WorkflowHandleStoredDataInvalid {
  const error = handleCoreErrorFromNative({
    class: "WorkflowHandleStoredDataInvalid",
    slots: { message, workflow_id: workflowId, cause: { kind: "jsonUnreadable", message } },
  });

  return error.kind === "storedDataInvalid" ? error : wrongClass(error);
}

/** The read error of a handle error: not found, unreadable, or the database. Pure. */
export function readErrorOfHandle(error: HandleCoreError): WorkflowReadError {
  switch (error.kind) {
    case "notFound":
      return Object.freeze({ kind: "workflowNotFound", code: RetrievalCode.WORKFLOW_NOT_FOUND });
    case "storedDataInvalid":
      return Object.freeze({
        kind: "storedResultUnreadable",
        code: OperationalCode.RESULT_DESERIALIZATION_ERROR,
        cause: error.message,
      });
    case "databaseOperationFailed":
    case "resumeRecoveryFailed":
      return Object.freeze({
        kind: "databaseError",
        code: OperationalCode.DATABASE_ERROR,
        databaseCode: error.code,
        retryable: error.retryable,
        message: error.message,
      });
    default:
      return unreachable(error, "readErrorOfHandle");
  }
}

/** The wait of a `waitMs`, or `invalidInput`. Left out: no bound. Pure. */
export function workflowWait(waitMs: number | undefined): Result<WaitBound, WorkflowReadError> {
  if (waitMs === undefined) {
    return ok({ kind: "untilEnd" });
  }

  return Number.isSafeInteger(waitMs) && waitMs >= 0 && waitMs <= MAX_WAIT_LIMIT_MS
    ? ok({ kind: "upTo", milliseconds: waitMs })
    : err(Object.freeze({ kind: "invalidInput", code: RetrievalCode.INVALID_INPUT, maxMs: MAX_WAIT_LIMIT_MS }));
}

function wrongClass(error: HandleCoreError): never {
  throw new TypeError(`handleCoreErrorFromNative: the converter gave the kind ${error.kind} for another class`);
}
