// The queues of the Acme tasks and their retry policies. Each policy names the codes that it retries; a code that a
// task does not declare fails typecheck in the task's contract.

import type { CustomQueueInput, RetryPolicy } from "horsies";

import {
  CHAOS_EXPORT_RETRY_INTERVALS_SECONDS,
  COURIER_MAX_RETRIES,
  COURIER_RETRY_BASE_SECONDS,
  CRASH_RETRY_INTERVALS_SECONDS,
  PSP_MAX_RETRIES,
  PSP_RETRY_BASE_SECONDS,
  SUPPLIER_RETRY_INTERVALS_SECONDS,
} from "../tuning.ts";

export const QUEUE_PAYMENTS = "payments";
export const QUEUE_FULFILLMENT = "fulfillment";
export const QUEUE_NOTIFICATIONS = "notifications";
export const QUEUE_ANALYTICS = "analytics";

/** The queues of the app: priority 1 is claimed first; the cap is the most tasks of the queue that run at once. */
export const QUEUES: readonly [CustomQueueInput, ...CustomQueueInput[]] = [
  { name: QUEUE_PAYMENTS, priority: 1, maxConcurrency: { kind: "cap", tasks: 4 } },
  { name: QUEUE_FULFILLMENT, priority: 10, maxConcurrency: { kind: "cap", tasks: 8 } },
  { name: QUEUE_NOTIFICATIONS, priority: 50, maxConcurrency: { kind: "cap", tasks: 6 } },
  { name: QUEUE_ANALYTICS, priority: 90, maxConcurrency: { kind: "cap", tasks: 4 } },
];

/** The retry of a task that a worker crash ended. */
export const CRASH_RETRY: RetryPolicy<never> = {
  kind: "fixed",
  intervalsSeconds: CRASH_RETRY_INTERVALS_SECONDS,
  jitter: false,
  autoRetryFor: ["WORKER_CRASHED"],
};

/** The retry of `flaky_export`: wider intervals, so a restarted worker is back before the retry. */
export const EXPORT_RETRY: RetryPolicy<never> = {
  kind: "fixed",
  intervalsSeconds: CHAOS_EXPORT_RETRY_INTERVALS_SECONDS,
  jitter: false,
  autoRetryFor: ["WORKER_CRASHED"],
};

/** The retry of `sync_supplier_feed`: a supplier timeout and a crash. */
export const SUPPLIER_RETRY: RetryPolicy<"SUPPLIER_TIMEOUT"> = {
  kind: "fixed",
  intervalsSeconds: SUPPLIER_RETRY_INTERVALS_SECONDS,
  jitter: false,
  autoRetryFor: ["SUPPLIER_TIMEOUT", "WORKER_CRASHED"],
};

/** The retry of `authorize_payment`: the provider is unavailable, or a crash. The delay grows with each attempt. */
export const PSP_RETRY: RetryPolicy<"PSP_UNAVAILABLE"> = {
  kind: "exponential",
  baseSeconds: PSP_RETRY_BASE_SECONDS,
  maxRetries: PSP_MAX_RETRIES,
  jitter: false,
  autoRetryFor: ["PSP_UNAVAILABLE", "WORKER_CRASHED"],
};

/** The retry of `book_courier`: the courier is unavailable, or a crash. */
export const COURIER_RETRY: RetryPolicy<"COURIER_UNAVAILABLE"> = {
  kind: "exponential",
  baseSeconds: COURIER_RETRY_BASE_SECONDS,
  maxRetries: COURIER_MAX_RETRIES,
  jitter: false,
  autoRetryFor: ["COURIER_UNAVAILABLE", "WORKER_CRASHED"],
};
