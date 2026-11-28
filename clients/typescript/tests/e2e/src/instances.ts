// The app instances of the end-to-end tests and their worker profiles (Rust `clients/rust/tests/worker/src/instance.rs`,
// Python `clients/python/tests/e2e/tasks/instance*.py`, `profiles.py`).
//
// A test builds the app of one instance in its own process, as a producer, and starts the worker entry
// (`src/main.ts`) with the same instance. The entry reads the instance from `HORSIES_TS_E2E_INSTANCE`; its children
// inherit it.

import type {
  AppConfigInput,
  DatabaseConfigInput,
  CustomQueueInput,
  RecoveryConfigInput,
  SharedAsyncSetting,
  SharedThreadSetting,
  WorkerProfileInput,
} from "#horsies";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import type { Connection } from "./connection.ts";

/** The instance of the worker entry: an `Instance` name. */
export const INSTANCE_ENV = "HORSIES_TS_E2E_INSTANCE";

/** The broker pool of every instance (Python `pool_size=5, max_overflow=5`). */
const POOL_SIZE = 5;
const POOL_OVERFLOW = 5;

/** The cluster-wide cap of the `cluster_cap` instance. */
export const CLUSTER_WIDE_CAP = 2;

/** The prefetch buffer and the claim lease of the `soft_cap` instance. */
export const SOFT_CAP_PREFETCH = 2;
export const SOFT_CAP_CLAIM_LEASE_MS = 2_000;

/** The custom queues of the `custom_queues` instance: name, priority, cap. */
export const CUSTOM_QUEUES = [
  { name: "high", priority: 1, cap: 5 },
  { name: "normal", priority: 50, cap: 10 },
  { name: "low", priority: 100, cap: 20 },
] as const;

/**
 * The queue of the `recovery_queues` instance that runs one task at a time. Its priority 30 is the one of the serial
 * queue of Python `instance_priority_binding.py`, so a stored priority tells it from the default queue's 1 and from 100.
 */
export const SERIAL_QUEUE = { name: "recovery", priority: 30, cap: 1 } as const;

/** The custom queues of the `recovery_queues` instance: name, priority, cap (Rust `RECOVERY_QUEUES`, Python `instance_recovery.py`). */
export const RECOVERY_QUEUES = [{ name: "default", priority: 1, cap: 5 }, SERIAL_QUEUE] as const;

/** How long a stopping worker of a test profile waits for its tasks. */
export const TEST_DRAIN_TIMEOUT_MS = 5_000;

/** The queue that the `queue_retention` instance maps to a period, the period in days, and its class key. */
export const MAPPED_QUEUE = "default";
export const MAPPED_PERIOD_DAYS = 7;
export const MAPPED_CLASS_KEY = "standard_7d";

/** The period that the `declared_retention` instance declares, in days, and its class key. */
export const DECLARED_PERIOD_DAYS = 5;
export const DECLARED_CLASS_KEY = "standard_5d";

/** The app instances. */
export const INSTANCES = [
  // The default queue mode (Python `instance.py`).
  "default",
  // The custom queues `high`, `normal`, `low` with caps (Python `instance_custom.py`).
  "custom_queues",
  // A cluster-wide cap of `CLUSTER_WIDE_CAP` (Python `instance_cluster_cap.py`).
  "cluster_cap",
  // Short heartbeats and stale thresholds, for crash recovery (Python `instance_recovery.py`).
  "recovery",
  // A prefetch buffer with a short claim lease (Python `instance_softcap.py`).
  "soft_cap",
  // The default queue mapped to a 7-day period (Python `instance_queue_retention.py`).
  "queue_retention",
  // A declared 5-day period (Python `instance_declared_retention.py`).
  "declared_retention",
  // The default tasks with an `onChildStart` hook that throws.
  "hook_throws",
  // The default tasks with a provider whose factory throws.
  "provider_throws",
  // The default tasks with an `onChildStart` hook that holds its child past the 10 s bound.
  "hook_past_bound",
  // The default tasks with the profiles that start `SHARED_THREAD` children.
  "thread_profiles",
  // The default tasks with the schedules of the scheduler tests (Python `instance_scheduler.py`).
  "scheduler",
  // The default tasks with an interval of zero seconds and two schedules of one name.
  "scheduler_invalid_rules",
  // The default tasks with a schedule of a task that the app does not list (Python `instance_scheduler_invalid.py`).
  "scheduler_invalid_task",
  // The default tasks with an `AttemptEnded` handler and a handler child start hook (Python
  // `instance_event_handlers.py`).
  "handlers",
  // The handlers instance with an `onHandlerChildStart` hook that throws, before the one that records its start.
  "handler_hook_throws",
  // The handlers instance with an `onHandlerChildStart` hook that holds its child past the 10 s bound.
  "handler_hook_past_bound",
  // The default tasks with OpenTelemetry tracing, a span SDK in each child and in each seat isolate, and the thread
  // profiles (Python `instance_worker_ended_spans.py`).
  "worker_ended_spans",
  // The custom queues `default` and `recovery` with short heartbeats and stale thresholds (Rust
  // `Instance::RecoveryQueues`, Python `instance_recovery.py` and `instance_priority_binding.py`).
  "recovery_queues",
] as const;

/** One app instance. */
export type Instance = (typeof INSTANCES)[number];

/** An instance whose app has the default tasks: every instance but the two of custom queues. */
export type DefaultTaskInstance = Exclude<Instance, "custom_queues" | "recovery_queues">;

/** The worker profiles that every instance defines (Python `profiles.py`). */
export const PROFILES = [
  // One isolated child.
  "isolated-1",
  // Two isolated children.
  "isolated-2",
  // Four isolated children.
  "isolated-4",
  // Eight isolated children, more than the cap of the `high` queue.
  "isolated-8",
  // One isolated child; one claim pass takes at most one task per queue.
  "claim-batch-1",
  // Two isolated children, each replaced after two tasks.
  "recycle-after-2-tasks",
  // One shared async child with 4 seats.
  "shared-only-1x4",
  // One isolated child and one shared async child with 4 seats.
  "split-isolated-1-shared-1x4",
] as const;

/** A worker profile of every instance. */
export type Profile = (typeof PROFILES)[number];

/** The profiles of the `thread_profiles` instance only: they start `SHARED_THREAD` children. */
export const THREAD_PROFILES = ["thread-only-1x4", "split-isolated-1-thread-1x4", "thread-memory-bound-1x1"] as const;

/**
 * The resident-memory bound of the thread child of `thread-memory-bound-1x1`, in MiB: above the memory of a thread child
 * with one seat that runs no task, and below what `e2e_thread_holds_memory` keeps (`HELD_MEMORY_MB`).
 */
export const THREAD_MEMORY_BOUND_MB = 320;

/** A profile with `SHARED_THREAD` children. */
export type ThreadProfile = (typeof THREAD_PROFILES)[number];

/** Which task proves that a worker of a profile is ready: one of the class of a child that the profile starts. */
export type ReadyClass = "ISOLATED" | "SHARED_ASYNC" | "SHARED_THREAD";

/** The instance of `name`, or the rule it breaks. Pure. */
export function parseInstance(name: string): Result<Instance, string> {
  const found = INSTANCES.find((instance) => instance === name);

  return found === undefined ? err(`${INSTANCE_ENV}=${JSON.stringify(name)} is not one of ${INSTANCES.join(", ")}`) : ok(found);
}

/** The class of the readiness task of a worker of `profile`. Pure. */
export function readyClass(profile: Profile | ThreadProfile): ReadyClass {
  switch (profile) {
    case "shared-only-1x4":
      return "SHARED_ASYNC";
    case "thread-only-1x4":
    case "thread-memory-bound-1x1":
      return "SHARED_THREAD";
    case "isolated-1":
    case "isolated-2":
    case "isolated-4":
    case "isolated-8":
    case "claim-batch-1":
    case "recycle-after-2-tasks":
    case "split-isolated-1-shared-1x4":
    case "split-isolated-1-thread-1x4":
      return "ISOLATED";
  }
}

/** The queue of the readiness task and of the tasks without a queue of their own. Pure. */
export function readyQueue(instance: Instance): string {
  return instance === "custom_queues" ? "high" : "default";
}

/** The config of `instance` on `connection`, without its task list. Pure. */
export function instanceConfig(instance: Instance, connection: Connection): AppConfigInput {
  const base = { database: brokerOf(connection), workerProfiles: workerProfiles(instance) };

  switch (instance) {
    case "default":
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
      return base;
    case "worker_ended_spans":
      // The rerun case keeps the input of each send, so a failed task can be rerun.
      return { ...base, database: { ...base.database, retainRerunInputDefault: "keep" }, tracing: { kind: "otelTracing" } };
    case "custom_queues":
      return { ...base, queueMode: "custom", customQueues: customQueues(CUSTOM_QUEUES) };
    case "recovery_queues":
      return { ...base, queueMode: "custom", customQueues: customQueues(RECOVERY_QUEUES), recovery: fastRecovery() };
    case "cluster_cap":
      return { ...base, clusterWideCap: { kind: "clusterCap", tasks: CLUSTER_WIDE_CAP } };
    case "recovery":
      return { ...base, recovery: fastRecovery() };
    case "soft_cap":
      return {
        ...base,
        claim: { kind: "softCap", prefetchBuffer: SOFT_CAP_PREFETCH, leaseMs: SOFT_CAP_CLAIM_LEASE_MS },
        recovery: { claimerHeartbeatIntervalMs: 1_000 },
      };
    case "queue_retention":
      return {
        ...base,
        recovery: { checkIntervalMs: 1_000 },
        retention: {
          partitionMaintenanceIntervalSeconds: 60,
          retentionSweepIntervalSeconds: 30,
          queueRetention: { [MAPPED_QUEUE]: { kind: "days", days: MAPPED_PERIOD_DAYS } },
        },
      };
    case "declared_retention":
      return {
        ...base,
        recovery: { checkIntervalMs: 1_000 },
        retention: {
          partitionMaintenanceIntervalSeconds: 60,
          retentionSweepIntervalSeconds: 30,
          retentionPeriods: [{ days: DECLARED_PERIOD_DAYS }],
        },
      };
  }
}

function brokerOf(connection: Connection): DatabaseConfigInput {
  const pool = { poolSize: POOL_SIZE, maxOverflow: POOL_OVERFLOW };

  switch (connection.kind) {
    case "direct":
      return { databaseUrl: { kind: "value", value: connection.databaseUrl }, ...pool };
    case "transactionPool":
      return {
        databaseUrl: { kind: "value", value: connection.poolUrl },
        connection: { kind: "pgbouncerTransaction", sessionDatabaseUrl: { kind: "value", value: connection.sessionUrl } },
        ...pool,
      };
  }
}

/** A queue of a custom queue instance: name, priority, cap. */
type QueueEntry = { readonly name: string; readonly priority: number; readonly cap: number };

function customQueues(queues: readonly QueueEntry[]): readonly [CustomQueueInput, ...CustomQueueInput[]] {
  const [first, ...rest] = queues.map(
    (queue): CustomQueueInput => ({ name: queue.name, priority: queue.priority, maxConcurrency: { kind: "cap", tasks: queue.cap } }),
  );

  if (first === undefined) {
    throw new TypeError("customQueues: the queue list is empty");
  }

  return [first, ...rest];
}

/**
 * Python `instance_recovery.py`: 1 s heartbeats and checks, the smallest stale thresholds that the validation admits,
 * and a worker state snapshot each second.
 */
export const FAST_RECOVERY = {
  runnerHeartbeatIntervalMs: 1_000,
  claimerHeartbeatIntervalMs: 1_000,
  runningStaleThresholdMs: 6_000,
  claimedStaleThresholdMs: 2_000,
  crashedWorkerRecoveryGraceMs: 2_000,
  checkIntervalMs: 1_000,
  workerStateSnapshotIntervalMs: 1_000,
} as const satisfies RecoveryConfigInput;

/** The recovery settings `FAST_RECOVERY`. Pure. */
export function fastRecovery(): RecoveryConfigInput {
  return FAST_RECOVERY;
}

/** The profiles of `instance`: every `Profile`, and the thread profiles for `thread_profiles` and `worker_ended_spans`. */
function workerProfiles(instance: Instance): Readonly<Record<string, WorkerProfileInput>> {
  const profiles: Record<string, WorkerProfileInput> = {};

  for (const profile of PROFILES) {
    profiles[profile] = workerProfile(profile);
  }

  if (instance === "thread_profiles" || instance === "worker_ended_spans") {
    for (const profile of THREAD_PROFILES) {
      profiles[profile] = threadProfile(profile);
    }
  }

  return profiles;
}

/** The settings of `profile`. Every profile drains in `TEST_DRAIN_TIMEOUT_MS`. Pure. */
export function workerProfile(profile: Profile): WorkerProfileInput {
  const drain = { drainTimeoutMs: TEST_DRAIN_TIMEOUT_MS };
  const sharedAsync: SharedAsyncSetting = { kind: "sharedAsyncChildren", count: 1, seats: 4 };

  switch (profile) {
    case "isolated-1":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 1 } };
    case "isolated-2":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 2 } };
    case "isolated-4":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 4 } };
    case "isolated-8":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 8 } };
    case "claim-batch-1":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 1 }, claim: { perQueuePerPass: { kind: "taskLimit", tasks: 1 } } };
    case "recycle-after-2-tasks":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 2, recycle: { after: { kind: "taskLimit", tasks: 2 } } } };
    case "shared-only-1x4":
      return { ...drain, sharedAsync };
    case "split-isolated-1-shared-1x4":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 1 }, sharedAsync };
  }
}

function threadProfile(profile: ThreadProfile): WorkerProfileInput {
  const drain = { drainTimeoutMs: TEST_DRAIN_TIMEOUT_MS };
  const sharedThread: SharedThreadSetting = { kind: "sharedThreadChildren", count: 1, seats: 4 };

  switch (profile) {
    case "thread-only-1x4":
      return { ...drain, sharedThread };
    case "split-isolated-1-thread-1x4":
      return { ...drain, isolated: { kind: "isolatedChildren", count: 1 }, sharedThread };
    case "thread-memory-bound-1x1":
      return {
        ...drain,
        sharedThread: {
          kind: "sharedThreadChildren",
          count: 1,
          seats: 1,
          recycle: { above: { kind: "memoryLimit", mb: THREAD_MEMORY_BOUND_MB } },
        },
      };
  }
}
