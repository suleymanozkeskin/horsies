// The members of an app instance besides its tasks and its failing start members (`app.ts`): the schedules of the
// scheduler instances, the event handlers of `handlers`, the OpenTelemetry SDK of the children of
// `worker_ended_spans`, the main-thread block of the children of `thread_profiles` (a thread body asks for it,
// `tasks/threads.ts`), and the workflows of an instance.

import type { AnyWorkflowContract, Horsies } from "../../../horsies/src/index.ts";
import { registerHandlers } from "./handlers.ts";
import type { DefaultTaskInstance, Instance } from "./instances.ts";
import { declareInvalidRuleSchedules, declareSchedules, declareUnlistedTaskSchedule } from "./tasks/scheduled.ts";
import { endedSpansFlow } from "./tasks/ended-spans.ts";
import { RECOVERY_QUEUE_WORKFLOWS } from "./tasks/recovery-queues.ts";
import { blockMainThreadOnAsk } from "./tasks/threads.ts";
import { ADVANCED_WORKFLOWS } from "./workflows/advanced.ts";
import { CHILD_WORKFLOWS } from "./workflows/children.ts";
import { DATAFLOW_WORKFLOWS } from "./workflows/dataflow.ts";
import { STRUCTURE_WORKFLOWS } from "./workflows/structure.ts";
import { SUBWORKFLOW_PARENTS } from "./workflows/subworkflows.ts";

/**
 * The workflows of the workflow layers (4 to 7) and of `stored-timeout-refusal`. The instances that those files run,
 * `default` and `recovery`, list them; `check --live` checks them on `default`.
 */
export const LAYER_WORKFLOWS: readonly AnyWorkflowContract[] = [
  ...STRUCTURE_WORKFLOWS,
  ...DATAFLOW_WORKFLOWS,
  ...ADVANCED_WORKFLOWS,
  ...CHILD_WORKFLOWS,
  ...SUBWORKFLOW_PARENTS,
];

/** Set the span line SDK in this process. The import runs only in the processes that set it. */
async function installSpanLinesSdk(): Promise<void> {
  const sdk = await import("./span-sdk.ts");

  sdk.installSpanLines();
}

/** Add the members of `instance` to `app`: none for most instances. Calls no I/O. */
export function addInstanceMembers(app: Horsies, instance: DefaultTaskInstance): void {
  switch (instance) {
    case "scheduler":
      declareSchedules(app);

      return;
    case "scheduler_invalid_rules":
      declareInvalidRuleSchedules(app);

      return;
    case "scheduler_invalid_task":
      declareUnlistedTaskSchedule(app);

      return;
    case "handlers":
      registerHandlers(app);

      return;
    case "worker_ended_spans":
      app.onChildStart(installSpanLinesSdk);
      app.onHandlerChildStart(installSpanLinesSdk);

      return;
    case "thread_profiles":
      app.onChildStart(blockMainThreadOnAsk);

      return;
    case "default":
    case "cluster_cap":
    case "recovery":
    case "soft_cap":
    case "queue_retention":
    case "declared_retention":
    case "hook_throws":
    case "provider_throws":
    case "hook_past_bound":
      return;
  }
}

/** The workflow contracts of `instance`. Pure. */
export function instanceWorkflows(instance: Instance): readonly AnyWorkflowContract[] {
  switch (instance) {
    case "worker_ended_spans":
      return [endedSpansFlow];
    case "default":
    case "recovery":
      return LAYER_WORKFLOWS;
    case "recovery_queues":
      return RECOVERY_QUEUE_WORKFLOWS;
    case "custom_queues":
    case "cluster_cap":
    case "soft_cap":
    case "queue_retention":
    case "declared_retention":
    case "hook_throws":
    case "provider_throws":
    case "hook_past_bound":
    case "thread_profiles":
    case "scheduler":
    case "scheduler_invalid_rules":
    case "scheduler_invalid_task":
    case "handlers":
      return [];
  }
}
