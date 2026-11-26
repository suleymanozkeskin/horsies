// The app of the end-to-end tests: the contracts and implementations of an instance, and its start members (a
// provider and an `onChildStart` hook that mark each child start). The test process and every process of the worker
// entry build the same app for one instance, so the stored input of a send decodes in the child that runs it.

import { isMainThread, threadId } from "node:worker_threads";

import { type AnyTaskContract, type AnyWorkflowContract, defineTask, h, Horsies } from "../../../horsies/src/index.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import type { Connection } from "./connection.ts";
import { entryThread, type StartMember, startMarkerName } from "./entry-thread.ts";
import { type Instance, instanceConfig } from "./instances.ts";
import { createOnce, markerErrorText } from "./markers.ts";
import { BASIC_CONTRACTS, implementBasic } from "./tasks/basic.ts";
import { CUSTOM_CONTRACTS, implementCustom } from "./tasks/custom.ts";
import { FENCING_CONTRACTS, implementFencing } from "./tasks/fencing.ts";
import { implementRecoveryQueues, RECOVERY_QUEUE_CONTRACTS, RECOVERY_QUEUE_WORKFLOWS } from "./tasks/recovery-queues.ts";
import { implementRetry, RETRY_CONTRACTS } from "./tasks/retry.ts";
import { implementShared, SHARED_CONTRACTS } from "./tasks/shared.ts";
import { implementSpawned, SPAWNED_CONTRACTS } from "./tasks/spawned.ts";
import { holdThread } from "./tasks/waits.ts";
import { implementWorkflowNodes, WORKFLOW_NODE_CONTRACTS } from "./tasks/workflow-nodes.ts";
import { ADVANCED_WORKFLOWS } from "./workflows/advanced.ts";
import { CHILD_NODE_CONTRACTS, CHILD_WORKFLOWS, implementChildNodes } from "./workflows/children.ts";
import { DATAFLOW_WORKFLOWS } from "./workflows/dataflow.ts";
import { STRUCTURE_WORKFLOWS } from "./workflows/structure.ts";
import { SUBWORKFLOW_PARENTS } from "./workflows/subworkflows.ts";

/** How long the hook of `hook_past_bound` holds its child: past the 10 s bound of a start hook. */
export const HOOK_HOLD_MS = 15_000;

/** The value that the provider of the default tasks builds in each task child. */
export type ProvidedValue = { readonly pid: number };

/** The process id of the child that ran the body, and the one that the provider of its state ran in. */
export const providedState = defineTask({
  name: "e2e_provided_state",
  input: {},
  output: h.object({ bodyPid: h.int(), providerPid: h.int() }),
});

/** The contracts of the default tasks: every instance but `custom_queues` and `recovery_queues`. */
const DEFAULT_CONTRACTS = [
  ...BASIC_CONTRACTS,
  ...RETRY_CONTRACTS,
  ...SHARED_CONTRACTS,
  ...FENCING_CONTRACTS,
  ...SPAWNED_CONTRACTS,
  ...WORKFLOW_NODE_CONTRACTS,
  ...CHILD_NODE_CONTRACTS,
  providedState,
];

/** The workflows of the instances with the default tasks. */
const DEFAULT_WORKFLOWS = [
  ...STRUCTURE_WORKFLOWS,
  ...DATAFLOW_WORKFLOWS,
  ...ADVANCED_WORKFLOWS,
  ...CHILD_WORKFLOWS,
  ...SUBWORKFLOW_PARENTS,
];

/** An instance whose app has the default tasks. */
type DefaultTaskInstance = Exclude<Instance, "custom_queues" | "recovery_queues">;

/** The contract lists of an instance's app. */
type ContractLists = { readonly tasks: readonly AnyTaskContract[]; readonly workflows: readonly AnyWorkflowContract[] };

/** The app of `instance` on `connection`, with its tasks and start members. No I/O: the app connects at its first use. */
export function buildApp(instance: Instance, connection: Connection): Result<Horsies, string> {
  const created = Horsies.create({ ...instanceConfig(instance, connection), ...contractLists(instance) });

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
      implementDefault(app, instance);
  }

  return ok(app);
}

/** The task and workflow contracts of the app of `instance`. Pure. */
function contractLists(instance: Instance): ContractLists {
  switch (instance) {
    case "custom_queues":
      return { tasks: CUSTOM_CONTRACTS, workflows: [] };
    case "recovery_queues":
      return { tasks: RECOVERY_QUEUE_CONTRACTS, workflows: RECOVERY_QUEUE_WORKFLOWS };
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
      return { tasks: DEFAULT_CONTRACTS, workflows: DEFAULT_WORKFLOWS };
  }
}

function implementDefault(app: Horsies, instance: DefaultTaskInstance): void {
  implementBasic(app);
  implementRetry(app);
  implementShared(app);
  implementFencing(app);
  implementSpawned(app);
  implementWorkflowNodes(app);
  implementChildNodes(app);

  const key = app.provide(() => markedStart("provider"));

  app.onChildStart(() => {
    markedStart("hook");
  });
  app.implement(providedState, (_input, runtime) =>
    providedState.ok({ bodyPid: process.pid, providerPid: runtime.state(key).pid }),
  );
  addFailingStartMember(app, instance);
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

  return { pid: process.pid };
}
