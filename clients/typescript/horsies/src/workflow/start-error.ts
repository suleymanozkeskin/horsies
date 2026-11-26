// The errors of a workflow start: the workflow start catalog of the core, generated
// (`src/_catalog/workflow_start_errors.ts`), told apart by `code`. `ENQUEUE_FAILED` carries the types of its workflow
// for the handle that `retryStart` gives; the spec data text and the facts of the retry are kept out of the error
// (not a member, so `JSON.stringify` of an error never holds the params).

import type { CoreCause, NoCause } from "../_catalog/error_detail.ts";
import {
  type NativeWorkflowStartCoreError,
  type WorkflowStartCommandConflict,
  type WorkflowStartCoreError,
  workflowStartCoreErrorFromNative,
  type WorkflowStartEnqueueFailed,
  type WorkflowStartIdConflict,
  type WorkflowStartValidationFailed,
} from "../_catalog/workflow_start_errors.ts";
import type { RawCatalogError } from "../native/values/records.ts";
import type { SeatRelayFull } from "../seats/relay-full.ts";
import { nativeCatalogValue } from "../send/catalog-value.ts";
import type { NodeTarget } from "./node-checks.ts";
import type { BuildReturn } from "./node.ts";

/** Type-only key of the types of a started workflow. It does not exist at run time. */
export declare const startTypes: unique symbol;

/** `ENQUEUE_FAILED` of a start: the tree may or may not exist; `retryStart` sends the same spec again. */
export type StartEnqueueFailed<R extends BuildReturn> = WorkflowStartEnqueueFailed & { readonly [startTypes]?: R };

/**
 * The error of a workflow start, told apart by `code`. `SEAT_RELAY_FULL` comes in a `SHARED_THREAD` body only: the seat
 * relay held its bound and sent nothing, so no tree was written; start again (`retryStart` does not take it).
 */
export type WorkflowStartError<R extends BuildReturn> =
  | WorkflowStartValidationFailed
  | WorkflowStartIdConflict
  | WorkflowStartCommandConflict
  | StartEnqueueFailed<R>
  | SeatRelayFull;

/** What a retry of a failed start sends again: the same text, id, sent instant, and the nodes for typed reads. */
export type StartRetry = {
  /** The workflow contract: the handle of the retry reads its output with it. */
  readonly workflow: object;
  readonly specData: string;
  readonly workflowId: string;
  readonly workflowName: string;
  readonly sentAtMicros: bigint;
  readonly targets: ReadonlyMap<string, NodeTarget>;
};

const retries = new WeakMap<object, StartRetry>();

/** The public error of a raw record of the workflow start catalog. An unknown class throws (a build defect). */
export function startErrorOfRaw(raw: RawCatalogError): WorkflowStartCoreError {
  // SAFETY: the bridge builds the slots of a start class from the core's slot list of that class
  // (`ErrorClassValues`), the list from which the codegen renders `NativeWorkflowStartCoreError`; the generated
  // converter throws for a class name that it does not know.
  return workflowStartCoreErrorFromNative(nativeCatalogValue(raw) as unknown as NativeWorkflowStartCoreError);
}

/** `ENQUEUE_FAILED` with its retry kept out of the error. Pure apart from the kept retry. */
export function withRetry<R extends BuildReturn>(
  error: WorkflowStartEnqueueFailed,
  retry: StartRetry,
): StartEnqueueFailed<R> {
  retries.set(error, Object.freeze(retry));

  return error;
}

/** The retry kept for an error, or `missing` for an error that no start of this process gave. */
export function keptRetry(
  error: object,
): { readonly kind: "kept"; readonly retry: StartRetry } | { readonly kind: "missing" } {
  const retry = retries.get(error);

  return retry === undefined ? { kind: "missing" } : { kind: "kept", retry };
}

/** `VALIDATION_FAILED`: the start breaks a rule; nothing is written. Pure. */
export function startValidationFailed(
  message: string,
  workflowName: string,
  workflowId: string,
  exception: CoreCause | NoCause,
): WorkflowStartValidationFailed {
  const error = workflowStartCoreErrorFromNative({
    class: "WorkflowStartValidationFailed",
    slots: { message, workflow_name: workflowName, workflow_id: workflowId, exception },
  });

  if (error.code !== "VALIDATION_FAILED") {
    throw new TypeError(
      `workflowStartCoreErrorFromNative: the converter gave the code ${error.code} for another class`,
    );
  }

  return error;
}
