---
title: Fencing Token
section: shared
summary: The fencing token of a task run (the task id and a value that rises with each attempt), how a store refuses the late writes of a stale attempt with it, and its limits.
related: [task-lifecycle, ../../workers/heartbeats-recovery, ../../tasks/retry-policy, ../../tasks/defining-tasks, ../../rust/tasks/defining-tasks]
tags: [fencing token, FencingToken, TaskRuntime, stale attempt, retry, conditional write, idempotency]
---

# Fencing Token

The fencing token of a run is the task id and a value. Task code writes the token with each write to another system. The system then refuses a write whose token is lower than the token that it holds for the task id.

## Why and When

An attempt can run on after the worker retried its task:

- A worker process that is paused (for example by `SIGSTOP`) keeps its connection and its [liveness lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock). Its children run on.
- The reaper of another worker can then find the RUNNING row stale and retry the task ([Stale RUNNING Recovery](/horsies/workers/heartbeats-recovery/#stale-running-recovery)). A second attempt starts while the first attempt still runs.

horsies refuses each late write of the first attempt to its own tables: the writes of an attempt fence on its claim. horsies cannot refuse the writes of that attempt to your database, your payment provider or your cache. Use the fencing token for these writes.

## The Guarantee

- Two reads in one attempt give the same token.
- An attempt that runs after another attempt of the same task gives a strictly greater value.
- The value depends on no clock.
- A claim that never started runs no task code, so its value is never seen.

Today the value is the attempt number: 1 for the first attempt, and at most `max_retries + 1` (21). It is the same number as the `attempt` of the log records, of the attempt span and of `horsies_task_attempts`. Compare the values of one task id. Do not write code that depends on the number itself.

The tokens of two task ids have no order. Compare a token only with a token of the same task id.

## How To

### Read the Token

| Client | Read | Outside a run |
|--------|------|---------------|
| Python | A task whose first parameter is `runtime: TaskRuntime` reads `runtime.fencing_token`, a `FencingToken` with `task_id` and `value` | A direct call passes `TaskRuntime.for_test(...)`. See [Defining Tasks](/horsies/tasks/defining-tasks/#fencing-token) |
| Rust | `rt.fencing_token()` on the `TaskRuntime` of the body gives `Result<FencingToken, FencingTokenRefused>` | `Err(FencingTokenRefused::NotInARun)`. See [Defining Tasks](/horsies/rust/tasks/defining-tasks/#fencing-token) |
| TypeScript | A body reads `runtime.fencingToken`, a `FencingToken` with `taskId` and `value` | A test passes `TaskRuntime.forTest(...)`. See [Defining Tasks](/horsies/typescript/tasks/defining-tasks/#fencing-token) |

### Fence the Writes

1. Use the task id in the key of the fence. Each task has its own fence.
2. At the start of the body, take the fence: write the token when the stored value for the task id is lower or equal. When the write changes no row, a later attempt holds the fence. Stop the body.
3. Make each later write conditional on the stored value, in one of two forms:
   - One conditional statement on the row that the write changes, with the value in that row.
   - One transaction that locks the fence row (`FOR SHARE`), checks the value, and then writes.

A read of the fence with no lock, followed by a write, is not enough. A later attempt can take the fence between the read and the write.

The examples use PostgreSQL. `$1` is the task id and `$2` is the value of the token.

```sql
CREATE TABLE payment_fences (
    task_id uuid   PRIMARY KEY,
    fence   bigint NOT NULL
);

-- Step 2: take the fence. No row back: a later attempt holds the fence.
INSERT INTO payment_fences (task_id, fence)
VALUES ($1, $2)
ON CONFLICT (task_id) DO UPDATE SET fence = EXCLUDED.fence
WHERE payment_fences.fence <= EXCLUDED.fence
RETURNING fence;

-- Step 3, second form: lock the fence row, check it, then write.
BEGIN;
SELECT fence FROM payment_fences WHERE task_id = $1 FOR SHARE;
-- Roll back when the fence is greater than $2.
UPDATE payments SET state = 'charged' WHERE order_id = $3;
COMMIT;
```

The `FOR SHARE` lock holds the fence row until the commit. The `ON CONFLICT DO UPDATE` of a later attempt waits for that lock, so the later attempt takes the fence only after the write committed.

The client pages show the first form in one statement: the Python page in [Defining Tasks](/horsies/tasks/defining-tasks/#fencing-token), the Rust page in [Defining Tasks](/horsies/rust/tasks/defining-tasks/#fencing-token).

## Limits

- **A rerun or a new workflow node.** A rerun and a workflow node that a resume starts again get a new task id. Their value starts again at 1. The token does not order the attempts of the old task id against the new one. A workflow pause abandons a task that is claimed and not started; the resume starts a new task for the node.
- **No later attempt.** When the task failed, was cancelled or expired, no later attempt exists. No greater token exists, so the store does not refuse a stale attempt.
- **A write before the fence.** A stale attempt that writes before the later attempt took the fence is not refused. Take the fence at the start of the body.
- **An atomic store.** The store must do the compare and the write in one atomic step: one statement, or one transaction that locks the fence.
- **One attempt, many writes.** Two writes in one attempt have the same token. The token does not make a repeated write of one attempt safe. Use an idempotency key for those writes.
- **Not exactly-once.** The token refuses the writes of an earlier attempt after a later attempt took the fence. It does not make a body run once.
- **Work that the body starts.** A thread that a Python body starts gets the runtime only when the body passes it on. A Rust body reads the token in its async body, in work that it started with `rt.spawn`, and on the thread of a blocking body; plain `tokio::spawn` and a thread that the body starts get `NotInARun`. Read the token in the body and pass it on.
