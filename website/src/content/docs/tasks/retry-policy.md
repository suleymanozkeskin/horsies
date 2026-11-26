---
title: Retry Policy
section: shared
summary: When a failed task retries, how the retry delay is computed, the limits of a policy, and how a retry differs from a rerun.
related: [defining-tasks, ../rust/tasks/defining-tasks, ../concepts/task-lifecycle, ../concepts/fencing-token, ../monitoring/action-semantics]
tags: [tasks, retry, backoff]
---

A retry policy makes a failed task run again on the same task row. The policy is part of the task options: a send stores it with the task row, and the worker reads it from the row.

The API is on the client pages: [Configure Retries](/horsies/tasks/defining-tasks/#configure-retries) (Python) and [Retry Policy](/horsies/rust/tasks/defining-tasks/#retry-policy) (Rust).

## Policy Fields

| Field | Meaning | Rule |
|-------|---------|------|
| `max_retries` | The number of retries. The first run is not a retry | 1 to 20 |
| `intervals_seconds` | The delays, in seconds | Each value is 1 to 86,400 |
| `backoff_strategy` | `fixed` or `exponential` | `fixed`: one interval for each retry. `exponential`: exactly one interval, the base |
| `jitter` | Adds a random part to each delay | |
| `max_delay_seconds` | A cap on each delay, or no cap | Greater than 0 when set |
| `retry_for` | The error codes that start a retry | At least one code |

A client refuses a policy that breaks a rule when the policy is built.

## Retry Delay

The delay before retry `n` (`n` is 1 for the first retry) is computed in four steps:

1. Backoff. `fixed` takes interval `n`. `exponential` takes `base * 2^(n-1)`.
2. Floor. A delay under 1 second becomes 1 second.
3. Jitter. When `jitter` is on, a random part of 0% to 25% of the delay is added. The delay only increases: a 60-second delay becomes 60 to 75 seconds. Jitter spreads out the retries of many tasks that failed at the same time.
4. Cap. When `max_delay_seconds` is set, a longer delay becomes `max_delay_seconds`. The cap applies last, after the jitter.

Without a cap, exponential backoff has no upper bound: a base of 30 seconds with 10 retries waits 15,360 seconds (about 4.3 hours) before the last retry.

| Policy | Delays without jitter |
|--------|-----------------------|
| `fixed`, intervals 60, 300, 900 | 60 s, 300 s, 900 s |
| `exponential`, base 30, 5 retries | 30 s, 60 s, 120 s, 240 s, 480 s |
| `exponential`, base 30, 10 retries, cap 600 | 30 s, 60 s, 120 s, 240 s, 480 s, then 600 s for retries 6 to 10 |

## When a Retry Starts

The computed delay is the earliest time a retry can start, not the exact time. When the delay ends, the retry joins its queue in the same order as a task sent at that moment: tasks that became ready earlier are claimed first.

A retry cannot start at or after the task's `good_until` deadline. See [Task Expiry](/horsies/concepts/task-lifecycle/#task-expiry).

## Auto-Retry Triggers

A failure starts a retry only when its error code is in `retry_for` and retries remain. A failure with another code is terminal.

`retry_for` accepts:

- Codes of the task's own errors, for example `RATE_LIMITED` and `SERVICE_UNAVAILABLE`.
- Library codes, for example `UNHANDLED_EXCEPTION`, `WORKER_CRASHED` and `TASK_TIMEOUT`. The library codes are on the [Errors Reference](/horsies/tasks/errors/).

A code is `UPPER_SNAKE_CASE`. `WORKFLOW_STOPPED` is reserved and is not a task code, so `retry_for` refuses it.

## How Retries Work

1. A task attempt fails with a code in `retry_for`.
2. The worker checks `retry_count < max_retries`.
3. If retries remain, the task status goes back to `PENDING`.
4. `next_retry_at` is set from the retry policy.
5. The task cannot be claimed until `next_retry_at` passes.
6. The worker sends a delayed notification to start the claim.

Each attempt writes an immutable attempt row to `horsies_task_attempts`. A failed attempt that retries has `will_retry=True` and `outcome='FAILED'`. The final attempt (success or terminal failure) has `will_retry=False`. During retries, `horsies_tasks.error_code` stays `NULL`: it is set only when the task reaches its final terminal state.

A task info read returns the attempts. See [Retrieving Results](/horsies/tasks/retrieving-results/#task-metadata-and-attempt-history) (Python) and [Task Info](/horsies/rust/tasks/retrieving-results/#task-info) (Rust).

## Retry Count Tracking

The task row holds the retry counters:

| Field | Description |
|-------|-------------|
| `retry_count` | The number of retries so far |
| `max_retries` | The number of retries the policy allows |
| `next_retry_at` | When the task can be claimed again |

Each retry runs the body with a greater [fencing token](/horsies/concepts/fencing-token/): the token of an attempt is the task id and a value that rises with each attempt.

## Retry vs Rerun

A retry policy controls the **automatic retry of the same request**: the task row stays live, `retry_count` increases, and each attempt is recorded against one task id.

**A rerun is a different operation.** A task that has reached a terminal status has left the live table for the immutable history archive. There is no row to reset, and a terminal task cannot be retried in place. To run that work again is a **new request**: a rerun creates a new task with a new id and records the lineage back to the source.

| | Retry | Rerun |
|---|---|---|
| Applies to | A live task that failed an attempt | A terminal task record |
| Task id | Unchanged | New, with recorded lineage |
| Started by | `retry_for` matches the error code | An explicit call |
| Attempt history | Added to the same task | Belongs to the new task |
| Requires | A retry policy with retries remaining | A retained enqueue input |
| Fencing token | Same task id, a greater value | New task id, the value starts again at 1 |

Whether a task can be rerun is decided at enqueue by `retain_rerun_input_default` (with a per-task override), not at rerun time. See [Action Semantics](/horsies/monitoring/action-semantics/#task-rerun).
