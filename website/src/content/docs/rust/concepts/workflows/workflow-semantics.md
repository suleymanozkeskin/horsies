---
title: Workflow Semantics
summary: DAG behavior, dependency resolution, failure handling, and success rules.
related: [workflow-api, subworkflows, result-handling, ../../tasks/retry-policy]
tags: [concepts, workflows, DAG, semantics]
---

## Overview

Workflows in horsies are DAGs:

- nodes are tasks or sub-workflows
- edges are dependencies
- readiness is computed from dependency state, join mode, and failure policy

## Workflow Status Lifecycle

```text
Pending -> Running -> Completed
                  -> Failed
                  -> Paused -> Expired
                  -> Cancelled
```

Terminal workflow statuses:

- `Completed`
- `Failed`
- `Cancelled`
- `Expired`

`Paused` is non-terminal.

`Cancelled` means that a caller stopped the workflow. `Expired` means that a
paused workflow crossed its configured age limit. An expired child propagates
to its parent like a cancelled child.

## Workflow Task Status Lifecycle

```text
Pending -> Ready -> Enqueued -> Running -> Completed
                                     -> Failed
Pending/Ready/Enqueued/Running -> Skipped
```

Terminal workflow-task statuses:

- `Completed`
- `Failed`
- `Skipped`

## Workflow-node timestamps

A regular node is not started when its backing task is enqueued. Its
`started_at` stays `NULL` in `ENQUEUED`.

The first worker ownership handoff to `RUNNING` stamps `started_at`. A replay
against an already-running node preserves the first value.

A pause or recovery reset clears `started_at` when it returns the node to
`READY`. Resume then creates a fresh backing task.

A sub-workflow node has no worker claim. It stamps `started_at` when child
workflow launch begins.

## `OnError`

Rust currently supports two workflow error policies:

| Policy | Behavior |
|---|---|
| `Fail` | Continue DAG resolution, then mark workflow failed |
| `Pause` | Pause immediately and block new enqueues until resume |

There is no public `Continue` variant in the Rust API.

## Pause and resume

`OnError::Pause` stops new DAG progression. `WorkflowHandle::get()` returns a
paused result instead of waiting forever.

Pause handles backing tasks by their current state:

- A `PENDING` backing task stays in the live table.
- A `CLAIMED` backing task is abandoned as `CANCELLED`.
- The cancelled backing task moves to task history.
- Its workflow node returns to `READY` and clears the task ID.
- Resume creates a fresh backing task row for that node.

The history row names the pause terminalization. It is not an extra workflow
node result.

Pause also cascades to running child workflows. Resume applies to the selected
workflow tree. It does not scan or mutate unrelated workflows.

## Paused workflow expiry

Set `AppConfig.retention.paused_workflow_auto_cancel_after` to enable expiry.
The default is `None`.

The worker changes an old `PAUSED` workflow to `EXPIRED`. It stores a
structured `WORKFLOW_EXPIRED` error. The error data names the policy and the
configured age.

Expiry terminalizes any remaining backing tasks through the normal task-history
move. Parent propagation runs through workflow recovery. A parent error cannot
roll back the child expiry transaction.

`WorkflowStatus::Expired` is terminal in result waits, notifications,
sub-workflow propagation, and retention.

## Failure Semantics

With the default settings:

- `join_all()`
- `allow_failed_deps(false)`
- `on_error = OnError::Fail`

the behavior is:

1. a task failure does not instantly terminate the whole workflow
2. downstream tasks that require the failed dependency are skipped
3. independent branches continue
4. once the DAG reaches terminal state, the workflow becomes `Failed`

Example:

```text
A -> B -> C
A -> D
```

If `A` fails:

- `B` is skipped
- `C` is skipped
- `D` still runs if it is otherwise runnable
- workflow ends as `Failed`

## Dependency Semantics

### `waits_for(...)`

`waits_for(...)` means:

- do not consider this node runnable until the dependency reaches terminal state

It does not mean “require success”. Success requirements come from join mode and `allow_failed_deps`.

### Join Modes

| Join Mode | Meaning |
|---|---|
| `join_all()` | default; wait for all dependencies to become terminal |
| `join_any()` | run when any dependency completes successfully |
| `join_quorum(min)` | run when at least `min` dependencies complete successfully |

#### All-join

| Upstream state | `allow_failed_deps(false)` | `allow_failed_deps(true)` |
|---|---|---|
| all completed | runs | runs |
| any failed | skipped | runs, receives failed `TaskResult` |
| any skipped | skipped | runs, receives `UpstreamSkipped` sentinel |

#### Any-join

```rust
let aggregate = builder.task(
    aggregate_results::node()?
        .waits_for(branch_a)
        .waits_for(branch_b)
        .waits_for(branch_c)
        .join_any(),
);
```

Behavior:

- becomes ready when any dependency completes successfully
- is skipped if all dependencies fail or skip

#### Quorum

```rust
let quorum = builder.task(
    quorum_handler::node()?
        .waits_for(replica_a)
        .waits_for(replica_b)
        .waits_for(replica_c)
        .join_quorum(2),
);
```

Behavior:

- becomes ready when `min` dependencies complete successfully
- is skipped if the threshold becomes impossible to reach

## `allow_failed_deps(true)`

This lets a downstream node run even when upstream dependencies failed or were skipped.

The downstream task receives full `TaskResult<T>` values and can implement fallback or recovery logic itself.

```rust
use horsies::{task, TaskError, TaskResult};

#[task("recovery_handler")]
async fn recovery_handler(primary_result: TaskResult<String>) -> Result<String, TaskError> {
    match primary_result {
        TaskResult::Ok(v) => Ok(v),
        TaskResult::Err(_err) => Ok("fallback".into()),
    }
}
```

## Data Flow Semantics

### `.arg_from(...)`

`arg_from(...)` injects the upstream result as a `TaskResult<S>`, not the raw success value.

```rust
let process = builder.task(
    process_user::node()?
        .waits_for(fetch)
        .arg_from(process_user::params::user(), fetch),
);
```

That means the receiving task should declare:

```rust
#[horsies::task("process_user")]
async fn process_user(user: TaskResult<User>) -> Result<ProcessedUser, TaskError> {
    // ...
}
```

### `.workflow_ctx_from(...)`

`workflow_ctx_from(...)` selects upstream nodes whose results should be available through `WorkflowContext`.

```rust
let enrich = builder.task(
    enrich_user::node()?
        .waits_for(fetch)
        .workflow_ctx_from([fetch]),
);
```

Important:

- context sources are ref-based
- they do not add dependencies automatically
- every context source still needs to appear in `waits_for(...)` / `waits_for_all(...)`

## Retries and Crash Recovery

Workflow tasks use the same retry policy model as standalone tasks:

- retry only when `auto_retry_for` matches
- respect `retry_policy`

Crash recovery:

- claimed but never-started work is requeued safely
- running work may retry if policy allows, otherwise it fails
- workflow reconciliation then applies the normal completion path

## Success Policies

By default, any required task failure means the workflow ends `Failed`.

`SuccessPolicy` allows explicit partial-success rules.

```rust
use horsies::{SuccessCase, SuccessPolicy};

let policy = SuccessPolicy {
    cases: vec![
        SuccessCase {
            required_indices: vec![0],
            name: Some("delivery_a".into()),
        },
        SuccessCase {
            required_indices: vec![1],
            name: Some("delivery_b".into()),
        },
    ],
    optional_indices: Some(vec![2]),
};

builder.success_policy(policy);
```

Semantics:

1. a case is satisfied when all its required nodes completed
2. the workflow succeeds if any case is satisfied
3. optional indices do not affect success

## Limits

Dynamic task generation during workflow execution is not supported. The DAG is fixed at submission time.
