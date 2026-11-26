// The worker profiles of the config: the children of each execution class, their recycle limits and pools, the pool
// of the worker process, the claim limits and the drain timeout (the core's `WorkerProfile`,
// `core/src/core/config/worker_profile.rs`). A worker runs the profile of its name. The core checks every profile
// with the app config.

import { ErrorCode } from "../_catalog/error_codes.ts";
import {
  CHILD_POOL_CONNECTIONS_MAX,
  CHILD_POOL_CONNECTIONS_MIN,
  CHILD_POOL_MIN_SIZE_MAX,
  CHILD_POOL_MIN_SIZE_MIN,
  DEFAULT_EXECUTOR_THREADS_MAX,
  DEFAULT_EXECUTOR_THREADS_MIN,
  DRAIN_TIMEOUT_MS_MAX,
  DRAIN_TIMEOUT_MS_MIN,
  ISOLATED_CHILDREN_MAX,
  ISOLATED_CHILDREN_MIN,
  MEMORY_LIMIT_MB_MAX,
  MEMORY_LIMIT_MB_MIN,
  SHARED_ASYNC_CHILDREN_MAX,
  SHARED_ASYNC_CHILDREN_MIN,
  SHARED_ASYNC_SEATS_MAX,
  SHARED_ASYNC_SEATS_MIN,
  SHARED_THREAD_CHILDREN_MAX,
  SHARED_THREAD_CHILDREN_MIN,
  SHARED_THREAD_SEATS_MAX,
  SHARED_THREAD_SEATS_MIN,
  TASK_LIMIT_MAX,
  TASK_LIMIT_MIN,
} from "../_catalog/worker_manifest.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  accepted,
  type Checked,
  checkedInteger,
  checkedObject,
  checkedText,
  type ConfigJson,
  entry,
  type IntegerBound,
  keyPath,
  mapChecked,
  memberPath,
  optionalEntry,
} from "./checked.ts";

/** A limit on a number of tasks, or no limit. */
export type TaskLimit = { readonly kind: "tasks"; readonly tasks: number } | { readonly kind: "noLimit" };

/** A limit on the resident memory of a child in MB, or no limit. */
export type MemoryLimit = { readonly kind: "mb"; readonly mb: number } | { readonly kind: "noLimit" };

/** A pool of at most `connections` connections. */
export type PoolSize = { readonly kind: "connections"; readonly connections: number };

/** A pool size: a number of connections, or a size that follows the seats. */
export type SharedPoolSize = PoolSize | { readonly kind: "fromSeats" };

/** The threads of the default executor of a shared async child: a number, or a count that follows the seats. */
export type ExecutorThreads = { readonly kind: "threads"; readonly threads: number } | { readonly kind: "fromSeats" };

/** When the worker replaces an isolated child: after a number of tasks, or above a memory limit. */
export interface IsolatedRecycleInput {
  readonly after?: TaskLimit;
  readonly above?: MemoryLimit;
}

/** When the worker replaces a shared child: after a number of tasks per seat, or above a memory limit. */
export interface SharedRecycleInput {
  readonly afterPerSeat?: TaskLimit;
  readonly above?: MemoryLimit;
}

/** The pool of each isolated child: connections kept open, the most connections, a check of each connection. */
export interface IsolatedPoolInput {
  readonly minSize?: number;
  readonly maxSize?: PoolSize;
  readonly check?: boolean;
}

/** The pool of each shared child, which its seats share. */
export interface SharedPoolInput {
  readonly minSize?: number;
  readonly maxSize?: SharedPoolSize;
  readonly check?: boolean;
}

/** `count` isolated children: each runs one task at a time in its own process. */
export interface IsolatedChildrenInput {
  readonly count: number;
  readonly recycle?: IsolatedRecycleInput;
  readonly pool?: IsolatedPoolInput;
  /** The pool that task code in the child opens when it sends. */
  readonly taskSendPool?: PoolSize;
}

/** `count` shared async children of `seats` seats each, on one event loop per child. */
export interface SharedAsyncChildrenInput {
  readonly count: number;
  readonly seats: number;
  readonly recycle?: SharedRecycleInput;
  readonly pool?: SharedPoolInput;
  readonly executor?: ExecutorThreads;
  readonly taskSendPool?: PoolSize;
}

/** `count` shared thread children of `seats` seats each, one worker thread per seat. */
export interface SharedThreadChildrenInput {
  readonly count: number;
  readonly seats: number;
  readonly recycle?: SharedRecycleInput;
  readonly pool?: SharedPoolInput;
  readonly taskSendPool?: PoolSize;
}

/** Limits on the tasks that the worker claims. */
export interface ClaimLimitsInput {
  /** The most tasks that one claim pass takes from one queue. */
  readonly perQueuePerPass?: TaskLimit;
  /** The most claimed tasks that are not yet running that the worker holds. */
  readonly claimedPerWorker?: TaskLimit;
}

/** The pool of the worker process. */
export interface ParentPoolInput {
  readonly maxSize?: SharedPoolSize;
}

/** Where the worker process runs: on the CPUs it inherits, or on one physical core of its own (Linux only). */
export type ParentCpu = "shared" | "ownCore";

/** The settings of one worker. A class that the profile leaves out has no children. */
export interface WorkerProfileInput {
  readonly isolated?: IsolatedChildrenInput;
  readonly sharedAsync?: SharedAsyncChildrenInput;
  readonly sharedThread?: SharedThreadChildrenInput;
  readonly pool?: ParentPoolInput;
  readonly claim?: ClaimLimitsInput;
  /** How long a stopping worker waits for running tasks, in milliseconds. */
  readonly drainTimeoutMs?: number;
  readonly parentCpu?: ParentCpu;
}

/** A bound of a field of a worker profile, with the code of a worker profile refusal. */
function profileBound(min: number, max: number): IntegerBound {
  return Object.freeze({ min, max, code: ErrorCode.CONFIG_INVALID_WORKER_PROFILE });
}

const TASK_COUNT = profileBound(TASK_LIMIT_MIN, TASK_LIMIT_MAX);
const MEMORY_MB = profileBound(MEMORY_LIMIT_MB_MIN, MEMORY_LIMIT_MB_MAX);
const POOL_CONNECTIONS = profileBound(CHILD_POOL_CONNECTIONS_MIN, CHILD_POOL_CONNECTIONS_MAX);
const POOL_MIN_SIZE = profileBound(CHILD_POOL_MIN_SIZE_MIN, CHILD_POOL_MIN_SIZE_MAX);
const EXECUTOR_THREADS = profileBound(DEFAULT_EXECUTOR_THREADS_MIN, DEFAULT_EXECUTOR_THREADS_MAX);
const ISOLATED_CHILDREN = profileBound(ISOLATED_CHILDREN_MIN, ISOLATED_CHILDREN_MAX);
const SHARED_ASYNC_CHILDREN = profileBound(SHARED_ASYNC_CHILDREN_MIN, SHARED_ASYNC_CHILDREN_MAX);
const SHARED_ASYNC_SEATS = profileBound(SHARED_ASYNC_SEATS_MIN, SHARED_ASYNC_SEATS_MAX);
const SHARED_THREAD_CHILDREN = profileBound(SHARED_THREAD_CHILDREN_MIN, SHARED_THREAD_CHILDREN_MAX);
const SHARED_THREAD_SEATS = profileBound(SHARED_THREAD_SEATS_MIN, SHARED_THREAD_SEATS_MAX);
const DRAIN_TIMEOUT_MS = profileBound(DRAIN_TIMEOUT_MS_MIN, DRAIN_TIMEOUT_MS_MAX);

const MILLISECONDS_PER_SECOND = 1000;
const NANOSECONDS_PER_MILLISECOND = 1_000_000;

/** The core's `worker_profiles` object of `profiles`, by name. Pure. */
export function workerProfilesJson(
  profiles: Readonly<Record<string, WorkerProfileInput>>,
  path: string,
): Checked<ConfigJson> {
  return checkedObject(
    Object.entries(profiles).map(([name, profile]) => {
      const at = keyPath(path, name);
      const checkedName = checkedText(name, at);

      return checkedName.kind === "refused" ? checkedName : entry(name, profileJson(profile, at));
    }),
  );
}

function profileJson(profile: WorkerProfileInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    optionalEntry("isolated", profile.isolated, (children) =>
      isolatedJson(children, memberPath(path, "isolated")),
    ),
    optionalEntry("shared_async", profile.sharedAsync, (children) =>
      sharedAsyncJson(children, memberPath(path, "sharedAsync")),
    ),
    optionalEntry("shared_thread", profile.sharedThread, (children) =>
      sharedThreadJson(children, memberPath(path, "sharedThread")),
    ),
    optionalEntry("pool", profile.pool, (pool) =>
      checkedObject([
        optionalEntry("max_size", pool.maxSize, (size) => sharedPoolSizeJson(size, memberPath(path, "pool.maxSize"))),
      ]),
    ),
    optionalEntry("claim", profile.claim, (claim) => claimJson(claim, memberPath(path, "claim"))),
    optionalEntry("drain_timeout", profile.drainTimeoutMs, (milliseconds) =>
      drainTimeoutJson(milliseconds, memberPath(path, "drainTimeoutMs")),
    ),
    optionalEntry("parent_cpu", profile.parentCpu, (cpu) => accepted(parentCpuText(cpu))),
  ]);
}

function isolatedJson(children: IsolatedChildrenInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    accepted([["kind", "isolated_children"]]),
    entry("count", checkedInteger(children.count, ISOLATED_CHILDREN, memberPath(path, "count"))),
    optionalEntry("recycle", children.recycle, (recycle) =>
      checkedObject([
        optionalEntry("after", recycle.after, (limit) => taskLimitJson(limit, memberPath(path, "recycle.after"))),
        optionalEntry("above", recycle.above, (limit) => memoryLimitJson(limit, memberPath(path, "recycle.above"))),
      ]),
    ),
    optionalEntry("pool", children.pool, (pool) =>
      checkedObject([
        optionalEntry("min_size", pool.minSize, (size) =>
          checkedInteger(size, POOL_MIN_SIZE, memberPath(path, "pool.minSize")),
        ),
        optionalEntry("max_size", pool.maxSize, (size) => poolSizeJson(size, memberPath(path, "pool.maxSize"))),
        optionalEntry("check", pool.check, accepted),
      ]),
    ),
    optionalEntry("task_send_pool", children.taskSendPool, (size) =>
      poolSizeJson(size, memberPath(path, "taskSendPool")),
    ),
  ]);
}

function sharedAsyncJson(children: SharedAsyncChildrenInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    accepted([["kind", "shared_async_children"]]),
    entry("count", checkedInteger(children.count, SHARED_ASYNC_CHILDREN, memberPath(path, "count"))),
    entry("seats", checkedInteger(children.seats, SHARED_ASYNC_SEATS, memberPath(path, "seats"))),
    optionalEntry("recycle", children.recycle, (recycle) => sharedRecycleJson(recycle, memberPath(path, "recycle"))),
    optionalEntry("pool", children.pool, (pool) => sharedPoolJson(pool, memberPath(path, "pool"))),
    optionalEntry("executor", children.executor, (executor) => executorJson(executor, memberPath(path, "executor"))),
    optionalEntry("task_send_pool", children.taskSendPool, (size) =>
      poolSizeJson(size, memberPath(path, "taskSendPool")),
    ),
  ]);
}

function sharedThreadJson(children: SharedThreadChildrenInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    accepted([["kind", "shared_thread_children"]]),
    entry("count", checkedInteger(children.count, SHARED_THREAD_CHILDREN, memberPath(path, "count"))),
    entry("seats", checkedInteger(children.seats, SHARED_THREAD_SEATS, memberPath(path, "seats"))),
    optionalEntry("recycle", children.recycle, (recycle) => sharedRecycleJson(recycle, memberPath(path, "recycle"))),
    optionalEntry("pool", children.pool, (pool) => sharedPoolJson(pool, memberPath(path, "pool"))),
    optionalEntry("task_send_pool", children.taskSendPool, (size) =>
      poolSizeJson(size, memberPath(path, "taskSendPool")),
    ),
  ]);
}

function sharedRecycleJson(recycle: SharedRecycleInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    optionalEntry("after_per_seat", recycle.afterPerSeat, (limit) =>
      taskLimitJson(limit, memberPath(path, "afterPerSeat")),
    ),
    optionalEntry("above", recycle.above, (limit) => memoryLimitJson(limit, memberPath(path, "above"))),
  ]);
}

function sharedPoolJson(pool: SharedPoolInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    optionalEntry("min_size", pool.minSize, (size) => checkedInteger(size, POOL_MIN_SIZE, memberPath(path, "minSize"))),
    optionalEntry("max_size", pool.maxSize, (size) => sharedPoolSizeJson(size, memberPath(path, "maxSize"))),
    optionalEntry("check", pool.check, accepted),
  ]);
}

function claimJson(claim: ClaimLimitsInput, path: string): Checked<ConfigJson> {
  return checkedObject([
    optionalEntry("per_queue_per_pass", claim.perQueuePerPass, (limit) =>
      taskLimitJson(limit, memberPath(path, "perQueuePerPass")),
    ),
    optionalEntry("claimed_per_worker", claim.claimedPerWorker, (limit) =>
      taskLimitJson(limit, memberPath(path, "claimedPerWorker")),
    ),
  ]);
}

function taskLimitJson(limit: TaskLimit, path: string): Checked<ConfigJson> {
  switch (limit.kind) {
    case "tasks":
      return checkedObject([
        accepted([["kind", "task_limit"]]),
        entry("tasks", checkedInteger(limit.tasks, TASK_COUNT, memberPath(path, "tasks"))),
      ]);
    case "noLimit":
      return accepted({ kind: "no_task_limit" });
    default:
      return unreachable(limit, "taskLimitJson");
  }
}

function memoryLimitJson(limit: MemoryLimit, path: string): Checked<ConfigJson> {
  switch (limit.kind) {
    case "mb":
      return checkedObject([
        accepted([["kind", "memory_limit"]]),
        entry("mb", checkedInteger(limit.mb, MEMORY_MB, memberPath(path, "mb"))),
      ]);
    case "noLimit":
      return accepted({ kind: "no_memory_limit" });
    default:
      return unreachable(limit, "memoryLimitJson");
  }
}

function poolSizeJson(size: PoolSize, path: string): Checked<ConfigJson> {
  return checkedObject([
    accepted([["kind", "pool_size"]]),
    entry("connections", checkedInteger(size.connections, POOL_CONNECTIONS, memberPath(path, "connections"))),
  ]);
}

function sharedPoolSizeJson(size: SharedPoolSize, path: string): Checked<ConfigJson> {
  switch (size.kind) {
    case "connections":
      return poolSizeJson(size, path);
    case "fromSeats":
      return accepted({ kind: "pool_size_from_seats" });
    default:
      return unreachable(size, "sharedPoolSizeJson");
  }
}

function executorJson(executor: ExecutorThreads, path: string): Checked<ConfigJson> {
  switch (executor.kind) {
    case "threads":
      return checkedObject([
        accepted([["kind", "executor_threads"]]),
        entry("threads", checkedInteger(executor.threads, EXECUTOR_THREADS, memberPath(path, "threads"))),
      ]);
    case "fromSeats":
      return accepted({ kind: "executor_threads_from_seats" });
    default:
      return unreachable(executor, "executorJson");
  }
}

/** The core's form of a drain timeout: the serde form of a Rust `Duration`, `{ secs, nanos }`. Pure. */
function drainTimeoutJson(milliseconds: number, path: string): Checked<ConfigJson> {
  return mapChecked(checkedInteger(milliseconds, DRAIN_TIMEOUT_MS, path), (whole) =>
    Object.freeze({
      secs: Math.floor(whole / MILLISECONDS_PER_SECOND),
      nanos: (whole % MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND,
    }),
  );
}

/** The core's text of a parent CPU choice. Pure. */
function parentCpuText(cpu: ParentCpu): string {
  switch (cpu) {
    case "shared":
      return "SHARED";
    case "ownCore":
      return "OWN_CORE";
    default:
      return unreachable(cpu, "parentCpuText");
  }
}
