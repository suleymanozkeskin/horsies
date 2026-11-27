// The app of the end-to-end tests: the contracts and implementations of an instance, and its start members (a
// provider and an `onChildStart` hook that mark each child start). The test process and every process of the worker
// entry build the same app for one instance, so the stored input of a send decodes in the child that runs it.
//
// A `SHARED_THREAD` child runs the provider once in each of its seat isolates and the `onChildStart` hooks on its main
// thread, so the provided value names the thread that built it.

import { isMainThread, threadId } from "node:worker_threads";

import { type AnyTaskContract, defineTask, h, Horsies } from "#horsies";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import type { Connection } from "./connection.ts";
import { entryThread, type StartMember, startMarkerName } from "./entry-thread.ts";
import { addInstanceMembers, instanceWorkflows } from "./instance-members.ts";
import { type DefaultTaskInstance, type Instance, instanceConfig } from "./instances.ts";
import { createOnce, markerErrorText } from "./markers.ts";
import { BASIC_CONTRACTS, implementBasic } from "./tasks/basic.ts";
import { CANCEL_CONTRACTS, implementCancel } from "./tasks/cancel.ts";
import { CORE_PANIC_CONTRACTS, implementCorePanic } from "./tasks/core-panic.ts";
import { CUSTOM_CONTRACTS, implementCustom } from "./tasks/custom.ts";
import { ENDED_SPANS_CONTRACTS, implementEndedSpans } from "./tasks/ended-spans.ts";
import { FENCING_CONTRACTS, implementFencing } from "./tasks/fencing.ts";
import { implementRecoveryQueues, RECOVERY_QUEUE_CONTRACTS } from "./tasks/recovery-queues.ts";
import { implementRetry, RETRY_CONTRACTS } from "./tasks/retry.ts";
import { implementScheduled, SCHEDULED_CONTRACTS } from "./tasks/scheduled.ts";
import { implementShared, SHARED_CONTRACTS } from "./tasks/shared.ts";
import { implementSpawned, SPAWNED_CONTRACTS } from "./tasks/spawned.ts";
import { implementThreads, THREAD_CONTRACTS } from "./tasks/threads.ts";
import { holdThread } from "./tasks/waits.ts";
import { implementWorkflowNodes, WORKFLOW_NODE_CONTRACTS } from "./tasks/workflow-nodes.ts";
import { CHILD_NODE_CONTRACTS, implementChildNodes } from "./workflows/children.ts";

/** How long the hook of `hook_past_bound` holds its child: past the 10 s bound of a start hook. */
export const HOOK_HOLD_MS = 15_000;

/** The value that the provider of the default tasks builds in each task child, or in each seat isolate. */
export type ProvidedValue = { readonly pid: number; readonly threadId: number };

/** The process id of the child that ran the body, and the one that the provider of its state ran in. */
export const providedState = defineTask({
  name: "e2e_provided_state",
  input: {},
  output: h.object({ bodyPid: h.int(), providerPid: h.int() }),
});

/** The process and the thread of a `SHARED_THREAD` body, and the ones that the provider of its state ran in. */
export const threadProvidedState = defineTask({
  name: "e2e_thread_provided_state",
  input: { holdMs: h.int() },
  output: h.object({ bodyPid: h.int(), bodyThreadId: h.int(), providerPid: h.int(), providerThreadId: h.int() }),
  execution: "SHARED_THREAD",
  timeoutMs: 30_000,
});

/** The contracts of the default tasks: every instance but `custom_queues` and `recovery_queues`. */
const DEFAULT_CONTRACTS = [
  ...BASIC_CONTRACTS,
  ...RETRY_CONTRACTS,
  ...SHARED_CONTRACTS,
  ...THREAD_CONTRACTS,
  ...FENCING_CONTRACTS,
  ...SPAWNED_CONTRACTS,
  ...SCHEDULED_CONTRACTS,
  ...CANCEL_CONTRACTS,
  ...ENDED_SPANS_CONTRACTS,
  ...CORE_PANIC_CONTRACTS,
  ...WORKFLOW_NODE_CONTRACTS,
  ...CHILD_NODE_CONTRACTS,
  providedState,
  threadProvidedState,
];

/** The app of `instance` on `connection`, with its tasks and start members. No I/O: the app connects at its first use. */
export function buildApp(instance: Instance, connection: Connection): Result<Horsies, string> {
  const created = Horsies.create({ ...instanceConfig(instance, connection), tasks: instanceTasks(instance), workflows: instanceWorkflows(instance) });

  if (!created.ok) {
    return err(created.error.refusals.map((refused) => `${refused.code} ${refused.path}: ${refused.message}`).join("; "));
  }

  const app = created.value;

  switch (instance) {
    case "custom_queues":
      implementCustom(app);
      break;
    case "recovery_queues":
      implementRecoveryQueues(app);
      break;
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
    case "scheduler":
    case "scheduler_invalid_rules":
    case "scheduler_invalid_task":
    case "handlers":
    case "handler_hook_throws":
    case "handler_hook_past_bound":
    case "worker_ended_spans":
      implementDefault(app, instance);
  }

  return ok(app);
}

/** The task contracts of the app of `instance`. Pure. */
function instanceTasks(instance: Instance): readonly AnyTaskContract[] {
  switch (instance) {
    case "custom_queues":
      return CUSTOM_CONTRACTS;
    case "recovery_queues":
      return RECOVERY_QUEUE_CONTRACTS;
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
    case "scheduler":
    case "scheduler_invalid_rules":
    case "scheduler_invalid_task":
    case "handlers":
    case "handler_hook_throws":
    case "handler_hook_past_bound":
    case "worker_ended_spans":
      return DEFAULT_CONTRACTS;
  }
}

function implementDefault(app: Horsies, instance: DefaultTaskInstance): void {
  implementBasic(app);
  implementRetry(app);
  implementShared(app);
  implementThreads(app);
  implementFencing(app);
  implementSpawned(app);
  implementScheduled(app);
  implementCancel(app);
  implementEndedSpans(app);
  implementCorePanic(app);
  implementWorkflowNodes(app);
  implementChildNodes(app);

  const key = app.provide(() => markedStart("provider"));

  app.onChildStart(() => {
    markedStart("hook");
  });
  app.implement(providedState, (_input, { ok, state }) =>
    ok({ bodyPid: process.pid, providerPid: state(key).pid }),
  );
  app.implement(threadProvidedState, ({ holdMs }, { ok, state }) => {
    const provided = state(key);

    holdThread(holdMs);

    return ok({ bodyPid: process.pid, bodyThreadId: threadId, providerPid: provided.pid, providerThreadId: provided.threadId });
  });
  addFailingStartMember(app, instance);
  addInstanceMembers(app, instance);
}

/** The failing start member of a hook instance; none for the others. */
function addFailingStartMember(app: Horsies, instance: DefaultTaskInstance): void {
  switch (instance) {
    case "hook_throws":
      app.onChildStart(() => {
        throw new Error("e2e start hook failure");
      });

      return;
    case "provider_throws":
      // The key is not read: the factory throws before any body runs.
      void app.provide((): ProvidedValue => {
        throw new Error("e2e provider failure");
      });

      return;
    case "hook_past_bound":
      app.onChildStart(() => {
        holdThread(HOOK_HOLD_MS);
      });

      return;
    case "default":
    case "cluster_cap":
    case "recovery":
    case "soft_cap":
    case "queue_retention":
    case "declared_retention":
    case "thread_profiles":
    case "scheduler":
    case "scheduler_invalid_rules":
    case "scheduler_invalid_task":
    case "handlers":
    case "handler_hook_throws":
    case "handler_hook_past_bound":
    case "worker_ended_spans":
      return;
  }
}

/**
 * Mark the start of `member` in this thread with the marker of `startMarkerName`: `<member>-<pid>` on the main thread,
 * one marker per seat isolate in a `SHARED_THREAD` child. A second start in the same thread throws.
 */
function markedStart(member: StartMember): ProvidedValue {
  const name = startMarkerName(member, process.pid, entryThread(isMainThread), threadId);
  const created = createOnce(name, String(process.pid));

  if (!created.ok) {
    throw new Error(`the ${member} marker: ${markerErrorText(created.error)}`);
  }

  if (created.value === "existed") {
    throw new Error(`the ${member} ran twice in one thread: the marker ${name} exists`);
  }

  return { pid: process.pid, threadId };
}
