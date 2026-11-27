// The members of an app instance besides its tasks and its failing start members (`app.ts`): the event handlers of
// `handlers`, the OpenTelemetry SDK of the children of
// `worker_ended_spans`, the main-thread block of the children of `thread_profiles` (a thread body asks for it,
// `tasks/threads.ts`), and the workflows of an instance.

import { isMainThread } from "node:worker_threads";

import type { AnyWorkflowContract, Horsies, HorsiesConfig } from "#horsies";
import { entryThread } from "./entry-thread.ts";
import { registerFailingHandlerChildStart, registerHandlers } from "./handlers.ts";
import type { DefaultTaskInstance, Instance } from "./instances.ts";
import { INVALID_RULE_SECTION, SCHEDULER_SECTION, UNLISTED_TASK_SECTION } from "./tasks/scheduled.ts";
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

/**
 * Set the span line SDK in a seat isolate, whose globals are its own. A seat isolate runs the providers and not the
 * `onChildStart` hooks; the main thread of a child set its SDK in its hook.
 */
async function installSeatSpanLinesSdk(): Promise<void> {
  switch (entryThread(isMainThread)) {
    case "main":
      return;
    case "seatIsolate":
      return installSpanLinesSdk();
  }
}

/** Add the members of `instance` to `app`: none for most instances. Calls no I/O. */
export function addInstanceMembers(app: Horsies, instance: DefaultTaskInstance): void {
  switch (instance) {
    case "handlers":
      registerHandlers(app);

      return;
    case "handler_hook_throws":
    case "handler_hook_past_bound":
      registerFailingHandlerChildStart(app, instance);
      registerHandlers(app);

      return;
    case "worker_ended_spans":
      app.onChildStart(installSpanLinesSdk);
      app.onHandlerChildStart(installSpanLinesSdk);
      // The key is not read: the provider only sets the SDK of each seat isolate.
      void app.provide(installSeatSpanLinesSdk);

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
    case "scheduler":
    case "scheduler_invalid_rules":
    case "scheduler_invalid_task":
      return;
  }
}

/** The `schedule` section of the config of `instance`: the scheduler instances have one, the others none. Pure. */
export function instanceSchedule(instance: Instance): Pick<HorsiesConfig, "schedule"> {
  switch (instance) {
    case "scheduler":
      return { schedule: SCHEDULER_SECTION };
    case "scheduler_invalid_rules":
      return { schedule: INVALID_RULE_SECTION };
    case "scheduler_invalid_task":
      return { schedule: UNLISTED_TASK_SECTION };
    case "default":
    case "custom_queues":
    case "recovery_queues":
    case "cluster_cap":
    case "recovery":
    case "soft_cap":
    case "queue_retention":
    case "declared_retention":
    case "hook_throws":
    case "provider_throws":
    case "hook_past_bound":
    case "thread_profiles":
    case "handlers":
    case "handler_hook_throws":
    case "handler_hook_past_bound":
    case "worker_ended_spans":
      return {};
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
    case "handler_hook_throws":
    case "handler_hook_past_bound":
      return [];
  }
}
