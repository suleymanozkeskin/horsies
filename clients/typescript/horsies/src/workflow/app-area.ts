// The workflows area of the app: `app.start`, `app.retryStart` and `app.workflowHandle`. The app hub builds the area
// with the core of its thread and the app's workflows, and puts the three members on the app.

import type { SendableInput } from "../contract/schema.ts";
import type { WorkflowId } from "../contract/ids.ts";
import type { ParameterSchemas } from "../contract/schema.ts";
import type { Result } from "../wire/result.ts";
import type { WorkflowContract } from "./define.ts";
import { type WorkflowHandle, type WorkflowHandleById, workflowHandleByIdOf } from "./handle.ts";
import type { BuildReturn } from "./node.ts";
import { retryStart, type StartContext, type StartOptions, startWorkflow } from "./start.ts";
import type { StartEnqueueFailed, WorkflowStartError } from "./start-error.ts";

/** The members of the workflows area. */
export interface WorkflowArea {
  start<P extends ParameterSchemas, R extends BuildReturn>(
    workflow: WorkflowContract<P, R>,
    params: NoInfer<SendableInput<P>>,
    options?: StartOptions,
  ): Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>>;
  retryStart<R extends BuildReturn>(
    error: StartEnqueueFailed<R>,
  ): Promise<Result<WorkflowHandle<R>, WorkflowStartError<R>>>;
  workflowHandle<P extends ParameterSchemas, R extends BuildReturn>(
    workflow: WorkflowContract<P, R>,
    workflowId: WorkflowId,
  ): WorkflowHandleById<R>;
}

/** The workflows area over `context`. Pure. */
export function createWorkflowArea(context: StartContext): WorkflowArea {
  return Object.freeze({
    start: async <P extends ParameterSchemas, R extends BuildReturn>(
      workflow: WorkflowContract<P, R>,
      params: NoInfer<SendableInput<P>>,
      options: StartOptions = {},
    ) => startWorkflow<R>(context, workflow, Object(params), options),
    retryStart: async <R extends BuildReturn>(error: StartEnqueueFailed<R>) => retryStart<R>(context, error),
    // The contract gives the types of the handle only: every read takes its form from the stored run.
    workflowHandle: <P extends ParameterSchemas, R extends BuildReturn>(
      _workflow: WorkflowContract<P, R>,
      workflowId: WorkflowId,
    ) => workflowHandleByIdOf<R>({ core: context.core, registry: context.registry, workflowId, targets: new Map() }),
  });
}
