// The members of an app instance besides its tasks and its failing start members (`app.ts`): the schedules of the
// scheduler instances, the event handlers of `handlers`, the OpenTelemetry SDK of the children of
// `worker_ended_spans`, and the workflows of an instance.

import { isMainThread } from "node:worker_threads";

import type { AnyWorkflowContract, Horsies } from "../../../horsies/src/index.ts";
import { entryThread } from "./entry-thread.ts";
import { registerHandlers } from "./handlers.ts";
import type { Instance } from "./instances.ts";
import { declareInvalidRuleSchedules, declareSchedules, declareUnlistedTaskSchedule } from "./tasks/scheduled.ts";
import { endedSpansFlow } from "./tasks/ended-spans.ts";

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
export function addInstanceMembers(app: Horsies, instance: Exclude<Instance, "custom_queues">): void {
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
      // The key is not read: the provider only sets the SDK of each seat isolate.
      void app.provide(installSeatSpanLinesSdk);

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
    case "thread_profiles":
      return;
  }
}

/** The workflow contracts of `instance`. Pure. */
export function instanceWorkflows(instance: Instance): readonly AnyWorkflowContract[] {
  switch (instance) {
    case "worker_ended_spans":
      return [endedSpansFlow];
    case "default":
    case "custom_queues":
    case "cluster_cap":
    case "recovery":
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
