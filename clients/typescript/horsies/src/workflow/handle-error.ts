// The errors of a workflow handle: the handle catalog of the core, generated (`src/_catalog/workflow_handle_errors.ts`),
// told apart by `code`; and the errors of a read of an outcome (`get`, `resultFor`), told apart by `kind`.

import { OperationalErrorCode, RetrievalCode } from "../_catalog/task_codes.ts";
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

/**
 * Why a read of a workflow outcome failed, told apart by `kind`, which is named after the catalog code in `code`. A
 * read error is not the workflow's outcome. One variant has no `code` member, `unknownCustomCode`
 * (`RESULT_DESERIALIZATION_ERROR`), and `database` holds the handle code in `code` (`BROKER_ERROR`).
 */
export type WorkflowReadError =
  /** The workflow had not ended when the wait ended; it may still run. */
  | { readonly kind: "waitTimeout"; readonly code: "WAIT_TIMEOUT" }
  | { readonly kind: "workflowNotFound"; readonly code: "WORKFLOW_NOT_FOUND" }
  /** The node stores no result yet, or the workflow has no node of this id. */
  | { readonly kind: "resultNotReady"; readonly code: "RESULT_NOT_READY" }
  /** The stored value does not read with the handle's types. `cause` holds no value. */
  | {
      readonly kind: "resultDeserializationError";
      readonly code: "RESULT_DESERIALIZATION_ERROR";
      readonly cause: string;
    }
  /** The stored error has a custom code that the node's contract does not list. */
  | { readonly kind: "unknownCustomCode"; readonly code: string }
  /** A wait in a process that runs task bodies; nothing was read. */
  | { readonly kind: "waitInsideTask"; readonly code: "WAIT_INSIDE_TASK" }
  /** The app does not list the workflow, so its output has no type to read with; nothing was read. */
  | { readonly kind: "noTypeAvailable"; readonly code: "NO_TYPE_AVAILABLE"; readonly workflowName: string }
  /** The database or the connection failed. */
  | {
      readonly kind: "database";
      readonly code: HandleErrorCode;
      readonly retryable: boolean;
      readonly message: string;
    }
  /** A `waitMs` that is not a whole number of milliseconds from 0 to `maxMs`; nothing was read. */
  | { readonly kind: "invalidInput"; readonly code: "INVALID_INPUT"; readonly maxMs: number }
  /**
   * In a `SHARED_THREAD` body: the seat relay held its bound of waiting calls; nothing was read; retry. `message` names
   * the bound and its value.
   */
  | {
      readonly kind: "seatRelayFull";
      readonly code: "SEAT_RELAY_FULL";
      readonly retryable: true;
      readonly message: string;
    };

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
      return Object.freeze({ kind: "workflowNotFound", code: RetrievalCode.WORKFLOW_NOT_FOUND });
    case "STORED_DATA_INVALID":
      return Object.freeze({
        kind: "resultDeserializationError",
        code: OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
        cause: error.message,
      });
    case "DB_OPERATION_FAILED":
    case "RESUME_RECOVERY_FAILED":
      return Object.freeze({ kind: "database", code: error.code, retryable: error.retryable, message: error.message });
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

function wrongCode(error: HandleCoreError): never {
  throw new TypeError(`handleCoreErrorFromNative: the converter gave the code ${error.code} for another class`);
}
