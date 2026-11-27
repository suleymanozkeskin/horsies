---
title: Atomic Enqueue and Topology
section: shared
summary: Why a send cannot join the caller's database transaction, and the pattern that replaces it - send after commit, automatic resend, and an idempotency key.
related: [deployment-topology, ../tasks/sending-tasks, ../rust/tasks/sending-tasks, ../tasks/errors, ../monitoring/action-semantics]
tags: [operations, topology, enqueue, idempotency, idempotency_key, resend_on_transient_error, transaction]
---

# Atomic Enqueue and Topology

No send and no workflow start takes the caller's database connection. A send opens its own connection from the broker pool and commits on its own. The application write and the enqueue are always two transactions, also when both use the same PostgreSQL database.

The API is on the client pages: [Sending Tasks](/horsies/tasks/sending-tasks/#send-after-a-commit) (Python) and [Sending Tasks](/horsies/rust/tasks/sending-tasks/) (Rust).

The recommended instance for the horsies database, its connections and its failover limits are on [Deployment Topology](/horsies/operations/deployment-topology/).

## Why and When

Read this page when a task must exist if, and only if, an application write is committed. Examples: a receipt after an order row, a sync job after a status change.

Two failure windows exist:

| Order | Failure | Result |
| ----- | ------- | ------ |
| Send, then commit | The commit fails | A task exists for a write that did not happen |
| Commit, then send | The send fails, or the process stops between the two | A write exists without its task |

Horsies closes the first window by one rule: send after commit. Horsies narrows the second window with `resend_on_transient_error` and the idempotency key.

## What Joins a Caller Transaction

One operation takes the caller's connection: the rerun of a terminal task. It runs in the transaction that the caller owns, and it enqueues a rerun of a terminal task. See [Action Semantics](/horsies/monitoring/action-semantics/#task-rerun).

No other operation does this:

- A send and a delayed send take the task arguments only.
- A workflow start takes an optional workflow id only.

## How To

### Send After Commit

Commit the application transaction. Then send. Handle the error result of the send: after a retryable enqueue failure, repeat the same send with the resend method of the client. The repeat has the same task id and payload.

### Resend Transient Failures

Set `resend_on_transient_error` in the app config. The default is off.

Rules:

- A send makes at most 4 broker calls: 1 first call and 3 resends.
- The wait before a resend starts at 200 ms and doubles. The maximum wait is 2000 ms.
- Only a retryable enqueue failure is resent. Every other error returns at once.
- All resends of one send use the same task id. A resend of an enqueue that did commit returns the same task, also after that task finished: the resend reads the task history and does not run the task a second time.
- The setting applies to sends and to delayed sends.

A resend lives inside one send call. It does not survive a process stop. If the process stops after the commit and before the send, no task exists.

### Make the Send Repeatable with an Idempotency Key

Pass an idempotency key in the send options. Derive the key from the application write, for example `receipt:{order_id}`. A repeat of the same send, from a job rerun or a second process, then returns the first task and does not enqueue a second one.

Rules:

| Property | Value |
| -------- | ----- |
| Scope | The task name. Two task names can use the same key |
| Content | An opaque string. Horsies does not trim, fold case or normalize it |
| Size | 1 to 255 UTF-8 bytes. A key outside this range is an enqueue failure that is not retryable, and nothing is written |
| Same key, same command | The handle of the first task. The handle does not show that it was a repeat |
| Same key, different command | An idempotency key conflict that names the first task |

The key holds as long as its task is live (`PENDING`, `CLAIMED` or `RUNNING`). When the task reaches a terminal state, a reservation window starts. During the window, the key still returns the first task or a conflict. After the window, the key is free for a new request.

| Window | Value |
| ------ | ----- |
| Default | 24 hours |
| Maximum | 30 days (inclusive) |

The Python client sets the window on the broker ([Make a Send Repeatable](/horsies/tasks/sending-tasks/#make-a-send-repeatable)). The Rust client uses the default.

The command compared on a repeat includes the task name, queue, priority, kwargs, `good_until`, delay and task options. A repeat with changed kwargs is a conflict, not a replay.

A workflow start has no idempotency key. A start with a fixed workflow id compares its start command with the stored one: the same command returns the existing workflow, and another command is a conflict (`WORKFLOW_COMMAND_CONFLICT`). The start command holds the workflow name, the nodes and their kwargs, queues, priorities and task options. See [Start a Workflow Once per Entity](/horsies/guides/start-workflow-once/).

## Things to Avoid

**Don't send before the commit.** A rollback leaves a task for a write that does not exist.

**Don't use a random key.** A key from `uuid4()` differs on each attempt, so it never matches a repeat. Derive the key from data that the attempts share.

**Don't retry an idempotency key conflict.** It is not retryable, and the retry methods refuse it. While the owner task is live, a repeat returns the same error. After the reservation ends, a repeat starts a second task under the key, with the other command. Send the same command as the owner task, or use another key.
