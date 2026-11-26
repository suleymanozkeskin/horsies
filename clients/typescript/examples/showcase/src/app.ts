// The Acme app: its config, worker profile, task and workflow contracts, the store that each task child gets, the
// implementations and the schedule table.
//
// Every process of the showcase builds the same app: the producer scenarios, the worker, each worker child, the
// monitoring server and the check. Building it connects nothing: the app connects at its first operation.

import { Horsies, type HorsiesConfig, type NonEmpty, type Refusal, type Result, type WorkerProfileInput } from "horsies";

import { ALL_TASKS } from "./contracts/index.ts";
import { QUEUES } from "./contracts/policies.ts";
import { err, ok } from "./result.ts";
import { declareAcmeSchedules, type DeclareSchedules } from "./schedules.ts";
import type { DatabaseSettings } from "./settings.ts";
import { openStore } from "./store/connection.ts";
import { implementAnalyticsTasks } from "./tasks/analytics.ts";
import { implementInventoryTasks } from "./tasks/inventory.ts";
import { implementNotifyTasks } from "./tasks/notify.ts";
import { implementOrderTasks } from "./tasks/orders.ts";
import { implementPaymentTasks } from "./tasks/payments.ts";
import { implementPromotionTasks } from "./tasks/promotions.ts";
import { implementReturnTasks } from "./tasks/returns.ts";
import { implementShippingTasks } from "./tasks/shipping.ts";
import type { StoreKey } from "./tasks/support.ts";
import {
  RECOVERY_CHECK_INTERVAL_MS,
  RERUN_RETENTION_DAYS,
  TERMINAL_RECORD_RETENTION_HOURS,
  WORKER_STATE_SNAPSHOT_INTERVAL_MS,
} from "./tuning.ts";
import { ALL_WORKFLOWS } from "./workflows/index.ts";

/** The worker profile that `worker` runs by default (`--profile acme`). */
export const WORKER_PROFILE = "acme";

/** The isolated children of the profile: the tasks that use the Acme tables, one task at a time each. */
export const ISOLATED_CHILDREN = 6;

/** The shared async children of the profile, and the seats of each: the light tasks run side by side. */
export const SHARED_ASYNC_CHILDREN = 1;
export const SHARED_ASYNC_SEATS = 32;

/** The worker profile of the app. */
export const ACME_PROFILE: WorkerProfileInput = {
  isolated: { kind: "isolatedChildren", count: ISOLATED_CHILDREN },
  sharedAsync: { kind: "sharedAsyncChildren", count: SHARED_ASYNC_CHILDREN, seats: SHARED_ASYNC_SEATS },
};

/** The built app and the key of the store that each task child provides. */
export type ShowcaseApp = { readonly app: Horsies; readonly store: StoreKey };

/** Why the app was not built: every refusal of `Horsies.create`. */
export type AppRefused = { readonly refusals: NonEmpty<Refusal> };

/** The config of the app on the database `url`. Pure. */
export function appConfig(url: string): HorsiesConfig {
  return {
    // Each send keeps its input, so `rerun` can run a failed task again.
    broker: { databaseUrl: { kind: "value", value: url }, retainRerunInputDefault: "keep" },
    queueMode: "custom",
    customQueues: QUEUES,
    workerProfiles: { [WORKER_PROFILE]: ACME_PROFILE },
    recovery: {
      workerStateSnapshotIntervalMs: WORKER_STATE_SNAPSHOT_INTERVAL_MS,
      checkIntervalMs: RECOVERY_CHECK_INTERVAL_MS,
    },
    retention: {
      terminalRecordRetention: { kind: "hours", hours: TERMINAL_RECORD_RETENTION_HOURS },
      // The class of the tasks that `rerun` enqueues: a rerun names a registered retention period.
      retentionPeriods: [{ days: RERUN_RETENTION_DAYS }],
    },
    resendOnTransientError: true,
    tasks: ALL_TASKS,
    workflows: ALL_WORKFLOWS,
  };
}

/**
 * The app of the showcase database of `settings`, with the schedules that `declareSchedules` declares (the schedule
 * table of the app when left out): the provider opens one Acme store in each task child. No I/O.
 */
export function buildShowcase(
  settings: DatabaseSettings,
  declareSchedules: DeclareSchedules = declareAcmeSchedules,
): Result<ShowcaseApp, AppRefused> {
  const created = Horsies.create(appConfig(settings.url));

  if (!created.ok) {
    return err({ refusals: created.error.refusals });
  }

  const app = created.value;
  const store = app.provide(() => openStore(settings.url));

  implementOrderTasks(app, store);
  implementInventoryTasks(app, store);
  implementPaymentTasks(app, store);
  implementShippingTasks(app, store);
  implementReturnTasks(app, store);
  implementPromotionTasks(app, store);
  implementNotifyTasks(app);
  implementAnalyticsTasks(app);
  declareSchedules(app);

  return ok({ app, store });
}

/** The text of every refusal, one per line. Pure. */
export function appRefusedText(refused: AppRefused): string {
  return refused.refusals.map((refusal) => refusal.message).join("\n");
}
