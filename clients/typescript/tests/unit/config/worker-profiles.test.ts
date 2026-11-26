// The serde form of each worker profile form (`WorkerProfile`, `core/src/core/config/worker_profile.rs`).

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { workerProfilesJson, type WorkerProfileInput } from "../../../horsies/src/config/worker-profiles.ts";

/** The serde object of one profile `io` that must be accepted. */
function profileObject(profile: WorkerProfileInput): object {
  const written = workerProfilesJson({ io: profile }, "workerProfiles");

  assert.equal(written.kind, "accepted", written.kind === "refused" ? JSON.stringify(written.refusals) : "");

  return written.kind === "accepted" ? JSON.parse(JSON.stringify(written.value)).io : {};
}

describe("workerProfilesJson", () => {
  test("isolated children with every field", () => {
    assert.deepEqual(
      profileObject({
        isolated: {
          kind: "isolatedChildren",
          count: 4,
          recycle: { after: { kind: "taskLimit", tasks: 50 }, above: { kind: "memoryLimit", mb: 512 } },
          pool: { minSize: 1, maxSize: { kind: "connections", connections: 3 }, prePing: true },
          taskSendPool: { kind: "connections", connections: 2 },
        },
      }),
      {
        isolated: {
          kind: "isolated_children",
          count: 4,
          recycle: { after: { kind: "task_limit", tasks: 50 }, above: { kind: "memory_limit", mb: 512 } },
          pool: { min_size: 1, max_size: { kind: "pool_size", connections: 3 }, check: true },
          task_send_pool: { kind: "pool_size", connections: 2 },
        },
      },
    );
  });

  test("shared async children with the pool size that follows the seats, and no limits", () => {
    assert.deepEqual(
      profileObject({
        sharedAsync: {
          kind: "sharedAsyncChildren",
          count: 2,
          seats: 50,
          recycle: { afterPerSeat: { kind: "noTaskLimit" }, above: { kind: "noMemoryLimit" } },
          pool: { maxSize: { kind: "fromSeats" } },
        },
      }),
      {
        shared_async: {
          kind: "shared_async_children",
          count: 2,
          seats: 50,
          recycle: { after_per_seat: { kind: "no_task_limit" }, above: { kind: "no_memory_limit" } },
          pool: { max_size: { kind: "pool_size_from_seats" } },
        },
      },
    );
  });

  test("shared async children with a set pool size and task send pool, and no executor: the core's default applies", () => {
    assert.deepEqual(
      profileObject({
        sharedAsync: {
          kind: "sharedAsyncChildren",
          count: 1,
          seats: 8,
          pool: { minSize: 0, maxSize: { kind: "connections", connections: 4 }, prePing: false },
          taskSendPool: { kind: "connections", connections: 2 },
        },
      }),
      {
        shared_async: {
          kind: "shared_async_children",
          count: 1,
          seats: 8,
          pool: { min_size: 0, max_size: { kind: "pool_size", connections: 4 }, check: false },
          task_send_pool: { kind: "pool_size", connections: 2 },
        },
      },
    );
  });

  test("shared async children with a set number of runtime threads", () => {
    assert.deepEqual(
      profileObject({ sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 8, executor: { kind: "threads", threads: 3 } } }),
      { shared_async: { kind: "shared_async_children", count: 1, seats: 8, executor: { kind: "executor_threads", threads: 3 } } },
    );
  });

  test("shared async children with runtime threads that follow the seats", () => {
    assert.deepEqual(
      profileObject({ sharedAsync: { kind: "sharedAsyncChildren", count: 1, seats: 8, executor: { kind: "fromSeats" } } }),
      { shared_async: { kind: "shared_async_children", count: 1, seats: 8, executor: { kind: "executor_threads_from_seats" } } },
    );
  });

  test("shared thread children", () => {
    assert.deepEqual(
      profileObject({
        sharedThread: {
          kind: "sharedThreadChildren",
          count: 1,
          seats: 4,
          recycle: { afterPerSeat: { kind: "taskLimit", tasks: 10 } },
        },
      }),
      {
        shared_thread: {
          kind: "shared_thread_children",
          count: 1,
          seats: 4,
          recycle: { after_per_seat: { kind: "task_limit", tasks: 10 } },
        },
      },
    );
  });

  test("the pool of the worker process, the claim limits, the drain timeout and the parent CPU", () => {
    assert.deepEqual(
      profileObject({
        isolated: { kind: "isolatedChildren", count: 1 },
        pool: { maxSize: { kind: "connections", connections: 6 } },
        claim: { perQueuePerPass: { kind: "taskLimit", tasks: 5 }, claimedPerWorker: { kind: "noTaskLimit" } },
        drainTimeoutMs: 90_500,
        parentCpu: "ownCore",
      }),
      {
        isolated: { kind: "isolated_children", count: 1 },
        pool: { max_size: { kind: "pool_size", connections: 6 } },
        claim: { per_queue_per_pass: { kind: "task_limit", tasks: 5 }, claimed_per_worker: { kind: "no_task_limit" } },
        drain_timeout: { secs: 90, nanos: 500_000_000 },
        parent_cpu: "OWN_CORE",
      },
    );
  });

  test("an empty profile gives an empty object: the core's defaults apply", () => {
    assert.deepEqual(profileObject({}), {});
  });

  test("no children of each class: the core's no-children settings", () => {
    assert.deepEqual(
      profileObject({
        isolated: { kind: "noIsolatedChildren" },
        sharedAsync: { kind: "noSharedAsyncChildren" },
        sharedThread: { kind: "noSharedThreadChildren" },
      }),
      {
        isolated: { kind: "no_isolated_children" },
        shared_async: { kind: "no_shared_async_children" },
        shared_thread: { kind: "no_shared_thread_children" },
      },
    );
  });

  test("a profile name with a lone surrogate is refused at its key with HRS-218", () => {
    const written = workerProfilesJson({ "io\uD800": { isolated: { kind: "isolatedChildren", count: 1 } } }, "workerProfiles");

    assert.equal(written.kind, "refused");
    assert.deepEqual(
      written.kind === "refused" ? written.refusals.map((one) => one.code) : [],
      ["HRS-218"],
    );
  });
});
