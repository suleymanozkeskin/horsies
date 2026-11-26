// The app of the end-to-end tests: the contracts and implementations of an instance, and its start members (a
// provider and an `onChildStart` hook that mark each child start). The test process and every process of the worker
// entry build the same app for one instance, so the stored input of a send decodes in the child that runs it.

import { defineTask, h, Horsies } from "../../../horsies/src/index.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import type { Connection } from "./connection.ts";
import { type Instance, instanceConfig } from "./instances.ts";
import { createOnce, markerErrorText } from "./markers.ts";
import { BASIC_CONTRACTS, implementBasic } from "./tasks/basic.ts";
import { CUSTOM_CONTRACTS, implementCustom } from "./tasks/custom.ts";
import { FENCING_CONTRACTS, implementFencing } from "./tasks/fencing.ts";
import { implementRetry, RETRY_CONTRACTS } from "./tasks/retry.ts";
import { implementShared, SHARED_CONTRACTS } from "./tasks/shared.ts";
import { implementSpawned, SPAWNED_CONTRACTS } from "./tasks/spawned.ts";
import { holdThread } from "./tasks/waits.ts";

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

/** The contracts of the default tasks: every instance but `custom_queues`. */
const DEFAULT_CONTRACTS = [...BASIC_CONTRACTS, ...RETRY_CONTRACTS, ...SHARED_CONTRACTS, ...FENCING_CONTRACTS, ...SPAWNED_CONTRACTS, providedState];

/** The app of `instance` on `connection`, with its tasks and start members. No I/O: the app connects at its first use. */
export function buildApp(instance: Instance, connection: Connection): Result<Horsies, string> {
  const tasks = instance === "custom_queues" ? CUSTOM_CONTRACTS : DEFAULT_CONTRACTS;
  const created = Horsies.create({ ...instanceConfig(instance, connection), tasks });

  if (!created.ok) {
    return err(created.error.refusals.map((refused) => `${refused.code} ${refused.path}: ${refused.message}`).join("; "));
  }

  const app = created.value;

  if (instance === "custom_queues") {
    implementCustom(app);
  } else {
    implementDefault(app, instance);
  }

  return ok(app);
}

function implementDefault(app: Horsies, instance: Exclude<Instance, "custom_queues">): void {
  implementBasic(app);
  implementRetry(app);
  implementShared(app);
  implementFencing(app);
  implementSpawned(app);

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
function addFailingStartMember(app: Horsies, instance: Exclude<Instance, "custom_queues">): void {
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

/** Mark the start of `member` in this process with the marker `<member>-<pid>`. A second start throws. */
function markedStart(member: "provider" | "hook"): ProvidedValue {
  const created = createOnce(`${member}-${String(process.pid)}`, String(process.pid));

  if (!created.ok) {
    throw new Error(`the ${member} marker: ${markerErrorText(created.error)}`);
  }

  if (created.value === "existed") {
    throw new Error(`the ${member} ran twice in process ${String(process.pid)}`);
  }

  return { pid: process.pid };
}
