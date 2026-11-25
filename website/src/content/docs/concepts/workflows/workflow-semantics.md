---
title: Workflow Semantics
section: shared
summary: DAG-based workflow behavior, failure handling, and dependency resolution.
related: [workflow-examples, workflow-api, subworkflows, ../../tasks/retry-policy]
tags: [concepts, workflows, DAG, semantics]
---

## Overview

Workflows in Horsies are Directed Acyclic Graphs (DAGs) where:
- **Nodes** are tasks to execute
- **Edges** are dependencies between tasks
- **Execution** proceeds as dependencies are satisfied

The API is on the client pages: [Workflow API](/horsies/concepts/workflows/workflow-api/) and [Workflow Examples](/horsies/concepts/workflows/workflow-examples/) (Python), and [Workflow API](/horsies/rust/concepts/workflows/workflow-api/) and [Typed Node Builder](/horsies/rust/concepts/workflows/typed-node-builder/) (Rust).

## Workflow Status Lifecycle

```
PENDING → RUNNING → COMPLETED
                  ↘ FAILED
                  ↘ PAUSED (if on_error=OnError.PAUSE) → EXPIRED (paused past the declared age policy)
                  ↘ CANCELLED (manual)
```

**Terminal workflow statuses:** `COMPLETED`, `FAILED`, `CANCELLED`, `EXPIRED`. (`PAUSED` is non-terminal.)

`CANCELLED` means someone decided; `EXPIRED` means time ran out. An expired
child propagates to its parent exactly as a cancelled one.

## Task Status Lifecycle

```
PENDING → READY → ENQUEUED → RUNNING → COMPLETED
                                     ↘ FAILED
       ↘ SKIPPED (if upstream failed/skipped)
```

**Terminal task statuses (within workflows):** `COMPLETED`, `FAILED`, `SKIPPED`.

## Status Enums

`WorkflowStatus` values:

<!-- members: WorkflowStatus -->
| Enum | Value | Terminal |
|------|-------|----------|
| `PENDING` | `"PENDING"` | No |
| `RUNNING` | `"RUNNING"` | No |
| `COMPLETED` | `"COMPLETED"` | Yes |
| `FAILED` | `"FAILED"` | Yes |
| `PAUSED` | `"PAUSED"` | No |
| `CANCELLED` | `"CANCELLED"` | Yes |
| `EXPIRED` | `"EXPIRED"` | Yes |

`WorkflowTaskStatus` values:

<!-- members: WorkflowTaskStatus -->
| Enum | Value | Terminal |
|------|-------|----------|
| `PENDING` | `"PENDING"` | No |
| `READY` | `"READY"` | No |
| `ENQUEUED` | `"ENQUEUED"` | No |
| `RUNNING` | `"RUNNING"` | No |
| `COMPLETED` | `"COMPLETED"` | Yes |
| `FAILED` | `"FAILED"` | Yes |
| `SKIPPED` | `"SKIPPED"` | Yes |

The Python helpers for terminal statuses are in [Status Enums](/horsies/concepts/workflows/workflow-examples/#status-enums).

## Retries and Crash Recovery

To run several steps with effects so that a retry or a crash runs only the step that failed, see Crash-Safe Multi-Step Work ([Python](/horsies/guides/crash-safe-multi-step-work/), [Rust](/horsies/rust/guides/crash-safe-multi-step-work/)).

### Task retries inside workflows

Workflow tasks use the same retry mechanism as standalone tasks:

- A task retries only when it has a `retry_policy` **and** the failure matches `auto_retry_for` (configured on `RetryPolicy`).
- `auto_retry_for` is a required field on `RetryPolicy`, so retry triggers are always co-located with retry timing.
- While retrying, the workflow task remains `RUNNING` until the final attempt completes.

### Worker crash / restart

If a worker crashes mid-task, recovery behaves as follows:

- **CLAIMED task** (never started): requeued safely.
- **RUNNING task** (started): retried if the task has `WORKER_CRASHED` in `auto_retry_for` and retries remaining; otherwise marked `FAILED` with `WORKER_CRASHED`.
- **Workflow reconciliation**: if `tasks.status` is terminal but the corresponding
  `workflow_tasks.status` is still non-terminal, recovery triggers the normal
  completion path to update `workflow_tasks`, propagate to dependents, and finalize
  the workflow.

When a task is terminal but its result is missing, recovery synthesizes errors:

- `TASK_CANCELLED` for cancelled tasks
- `RESULT_NOT_AVAILABLE` for completed tasks missing results
- `WORKER_CRASHED` for failed tasks missing results

### Recovery limit

A recovery repair that raises rolls back, and the next reaper pass runs it again. After `workflow_recovery_failure_limit` passes with a persistent raise (default 25), the workflow fails closed with `WORKFLOW_RECOVERY_LIMIT_REACHED`:

- The node that raised goes `FAILED`. Other `PENDING` and `READY` nodes go `SKIPPED`.
- Live nodes finish, and the workflow goes `FAILED` when the last one ends. If no node is live, it goes `FAILED` at once.
- `on_error=OnError.PAUSE` does not apply, because a limit must end the workflow.
- A connection, lock or timeout error does not count, so a database outage cannot fail a workflow.

See [Workflow recovery limit](/horsies/configuration/recovery-config/#workflow-recovery-limit) for the rules of the count.

## Failure Semantics

### When is a workflow marked FAILED?

A workflow is marked **FAILED** if **any task fails** (when `on_error=OnError.FAIL`, the default).

The failure flow:
1. Task fails → workflow stores error, but status remains `RUNNING`
2. DAG continues resolving (dependents may be SKIPPED or run with `allow_failed_deps`)
3. Once all tasks reach terminal state → workflow marked `FAILED`

This design allows recovery handlers (`allow_failed_deps=True`) to execute before the workflow finalizes.

### on_error Policies

| Policy | Behavior |
|--------|----------|
| `fail` (default) | Store error, continue DAG resolution, mark FAILED when complete |
| `pause` | Immediately pause workflow, block new enqueues, await resume |

### Failure Propagation and Short-Circuiting

Horsies does **not** stop the entire workflow on first failure. It resolves the DAG to terminal state and applies failure rules **per dependency**. Short-circuiting happens **along the failed path**, not globally.

**Default behavior (`join="all"`, `allow_failed_deps=False`, `on_error=OnError.FAIL`):**

- When a dependency fails or is skipped, any downstream task that requires all dependencies is **SKIPPED**.
- That skip propagates to its dependents.
- Other branches that do not depend on the failed task still run.
- The workflow is marked FAILED **after** all tasks are terminal.

**Example:**

```
A → B → C
A → D
```

- If **A fails**:
  - **B** is SKIPPED
  - **C** is SKIPPED
  - **D** still runs (independent branch)
  - Workflow ends as FAILED

**To run recovery logic downstream:**

Set `allow_failed_deps=True` on the downstream task. It will run and receive the failed `TaskResult`.

**Join rules change the short-circuit behavior:**

- `join="any"`: a task can run if **any** dependency succeeds, even if others fail.
- `join="quorum"`: a task can run if the success threshold is met, even with some failures.

**Pause behaves differently:**

- `on_error=OnError.PAUSE` does not kill running tasks.
- It blocks new enqueues and waits for `resume()` or `cancel()`.

**Result access is separate:**

- `WorkflowHandle.get()` returns the workflow-level `TaskResult` (output value or error, not a status enum).
- A FAILED workflow stores no output. This is also true when its output node ran with `allow_failed_deps=True` and succeeded. Read that node's result with `WorkflowHandle.result_for()`.
- A COMPLETED workflow can have a SKIPPED or FAILED output node. See [Output Node Under a Success Policy](#output-node-under-a-success-policy).
- `WorkflowHandle.result_for()` is non-blocking and may return `RESULT_NOT_READY` if the task has not completed.

### Upstream Failure and args_from

When an upstream task fails and the downstream task uses `join="all"` (the default):

- **`allow_failed_deps=False` (default):** The downstream task is **SKIPPED**. It does not execute and does not receive the failed result.
- **`allow_failed_deps=True`:** The downstream task **runs** and receives the upstream `TaskResult` with `is_err() == True`.

`on_error=OnError.FAIL` does **not** short-circuit the DAG. The workflow stores the error and continues resolving dependents — downstream tasks are SKIPPED (or run if `allow_failed_deps=True`) before the workflow is marked FAILED.

With `join="any"`, a downstream task runs when **any** dependency succeeds, so a single upstream failure does not cause a skip unless **all** dependencies fail. With `join="quorum"`, the task runs when `min_success` dependencies succeed, so failures only matter if they make the threshold unreachable.

The Python example is in [Upstream Failure and args_from](/horsies/concepts/workflows/workflow-examples/#upstream-failure-and-args_from).

The manual `is_err()` check is the intended pattern when `allow_failed_deps=True`. With the defaults (`join="all"`, no `allow_failed_deps`), the task does not run on an upstream failure. An err still arrives with `join="any"` / `join="quorum"`, or when the stored upstream result fails to decode (`RESULT_DESERIALIZATION_ERROR`).

### PAUSE Semantics (Cooperative Stop)

When a task fails with `on_error=OnError.PAUSE`:

1. **Immediate status change**: Workflow status becomes `PAUSED`
2. **New enqueues blocked**: No new tasks will transition from PENDING → READY or READY → ENQUEUED
3. **Running tasks may complete**: Tasks already running will finish (cooperative, no force-kill)
4. **Already-enqueued tasks**: A worker may still claim one briefly. The check after the claim finds the paused workflow and abandons that claim, as described below, so the task does not execute until resume
5. **Resume required**: Workflow remains PAUSED until an explicit resume operation

**Pause has two paths per node, and they differ:**

- A node whose task is **still PENDING** moves nothing. Its task row stays
  live and claimable-but-filtered until resume.
- A node whose task is **already CLAIMED** has that claim abandoned: the task
  row terminalizes as `CANCELLED` with a message naming the pause, and — like
  every terminal task — its record moves to the history archive. The node
  returns to `READY` with its task id cleared, and resume enqueues a **fresh**
  task row. Seeing history records appear when a workflow is paused is
  expected, not a fault.

**The run keeps a record of each pause.** The pause writes one `WORKFLOW_PAUSED` event for the workflow and one
for each descendant that it pauses (table `horsies_workflow_events`). An abandoned claim writes `TASK_MOVED` with
the locator of its history record, and its node writes `NODE_READY`. A resume writes one `WORKFLOW_RESUMED` event
for each workflow that it resumes. A pause that a cancel or a paused expiry ends has no end event: the completion
time of the run ends it. See [horsies_workflow_events](/horsies/internals/database-schema/#horsies_workflow_events).

**Never-resumed pauses can be bounded by policy.** `paused_workflow_auto_cancel_after`
(a `timedelta` on `AppConfig.retention`, default `None` = disabled) expires a
workflow that has been PAUSED longer than the configured age, recording the
policy name and age on the run's `error` column. No deployment changes
behavior without declaring the rule.

**What PAUSE blocks:**

- Tasks cannot become READY while workflow is PAUSED
- READY tasks cannot be ENQUEUED while workflow is PAUSED
- Retries are blocked (retries are new enqueues)
- Downstream task propagation is halted

**What PAUSE allows:**

- Already-running tasks complete normally
- Tasks that started before PAUSE finish (cooperative stop). A task that is claimed but not started does not run: its claim is abandoned, as described above

### Resume

A paused workflow resumes on an explicit resume of its workflow handle. The Python example is in [Resume](/horsies/concepts/workflows/workflow-examples/#resume).

**Resume behavior:**

1. **Guard**: Only works if workflow status is `PAUSED`. Returns `False` (no-op) for other states.
2. **Transition**: Sets workflow status from `PAUSED` → `RUNNING`
3. **Re-evaluate PENDING tasks**: Checks if dependencies are terminal, marks READY if so
4. **Enqueue READY tasks**: All READY tasks are enqueued with their dependency results
5. **Commit, then recovery pass**: Steps 2–4 commit in one transaction. A recovery pass for the workflow tree then runs in a second transaction. It completes the propagation for a task that finished while the resume transaction ran.

**Resume failures:**

- A database error before the commit returns `Err(HandleDbOperationFailed)`: the transaction rolled back, or its commit outcome is unknown.
- A database error in the recovery pass returns `Err(HandleResumeRecoveryFailed)`: the workflow is `RUNNING`, and the worker reaper runs the same pass over all workflows. A repeated `resume()` returns `Ok(False)`.
- Any other exception propagates.

**What resume does NOT do:**

- Does not restart failed tasks (they remain FAILED)
- Does not affect already-running tasks
- Does not change COMPLETED/FAILED/CANCELLED workflows

## Dependency Semantics

### Join Modes

TaskNode supports three join modes via the `join` parameter:

| Join Mode | Behavior |
|-----------|----------|
| `all` (default) | Task runs when ALL dependencies are terminal |
| `any` | Task runs when ANY dependency succeeds (COMPLETED) |
| `quorum` | Task runs when at least `min_success` dependencies succeed |

The Python examples are in [Join Modes](/horsies/concepts/workflows/workflow-examples/#join-modes).

**Note:** `COMPLETED` means the dependency returned `TaskResult.ok`. A dependency that finished execution but returned `TaskResult.err` is `FAILED`, not `COMPLETED`. The same rule applies to a `SubWorkflowNode`: see [Output Node Under a Success Policy](#output-node-under-a-success-policy).

### AND-join (join="all")

```
Task runs when: ALL dependencies are in terminal state (COMPLETED/FAILED/SKIPPED)
```

**Behavior matrix:**

| Dependency State | allow_failed_deps=False | allow_failed_deps=True |
|------------------|-------------------------|------------------------|
| All COMPLETED | Task runs normally | Task runs normally |
| Any FAILED | Task is SKIPPED | Task runs with failed TaskResult |
| Any SKIPPED | Task is SKIPPED | Task runs with UPSTREAM_SKIPPED sentinel |

### OR-join (join="any")

**Semantics:**
- Task becomes READY when **any** dependency is COMPLETED
- Task is SKIPPED if **all** dependencies fail/skip (none succeeded)
- Failed/skipped deps do NOT block the task (unlike join="all")

### Quorum Join (join="quorum")

**Semantics:**
- Task becomes READY when `min_success` dependencies are COMPLETED
- Task is SKIPPED if it becomes **impossible** to reach threshold
  - Example: 2 of 3 deps failed, but need 2 successes → impossible → SKIPPED
- `min_success` must be >= 1 and <= number of dependencies

### SKIPPED Propagation

When a task is SKIPPED, all its dependents are also SKIPPED (unless they have `allow_failed_deps=True`). This cascades through the DAG.

```
A (FAILED) → B (SKIPPED) → C (SKIPPED) → D (SKIPPED)
```

### allow_failed_deps=True

Tasks with `allow_failed_deps=True` receive the `TaskResult` from dependencies:

- **FAILED deps**: Receive the actual `TaskResult` with the original error
- **SKIPPED deps**: Receive a sentinel `TaskResult` with `error_code=UPSTREAM_SKIPPED`

The Python example is in [Recovery Handlers](/horsies/concepts/workflows/workflow-examples/#recovery-handlers).

### UPSTREAM_SKIPPED Sentinel

When a dependency is SKIPPED (due to upstream failure), tasks with `allow_failed_deps=True` receive a sentinel error result instead of missing kwargs: the code `UPSTREAM_SKIPPED`, the message `Upstream dependency was SKIPPED`, and `data` `{"dependency_index": <index>}`. The Python form is in [Sentinels](/horsies/concepts/workflows/workflow-examples/#sentinels).

This applies to both `args_from` kwargs and `workflow_ctx_from` context data.

### A dependency with no stored result

A COMPLETED or FAILED dependency always stores its result. If its stored result is missing (for example a row changed by hand), the dependent task receives an error result in `args_from` kwargs and in `workflow_ctx_from` context data, in place of no entry: the code `RESULT_DESERIALIZATION_ERROR`, the message `Dependency node <node_id> (index <index>, task <task_name>) is <status> with no stored result`, and `data` `{"dependency_index": <index>}`. The Python form is in [Sentinels](/horsies/concepts/workflows/workflow-examples/#sentinels).

### args_from with join='any' / 'quorum'

With a non-`all` join, the node becomes ready while some dependencies may
still be running. An `args_from` kwarg whose source is not yet terminal is
simply **not injected** — the task receives only the kwargs of terminal
sources. The target parameter must therefore declare a default. The Python example is in [Partial Inputs](/horsies/concepts/workflows/workflow-examples/#partial-inputs).

Spec validation rejects (`WORKFLOW_INVALID_JOIN`) any `join='any'` /
`'quorum'` node whose `args_from` targets a parameter without a default —
without one, the task would crash with a `TypeError` depending on
completion order.

## DAG Examples

### Linear Chain

```
A → B → C → D
```

- If A fails: B, C, D are SKIPPED
- Workflow: FAILED

### Fan-out / Fan-in

```
    ┌→ B ─┐
A ──┼→ C ─┼→ E
    └→ D ─┘
```

- If B fails: E is SKIPPED (waiting on B)
- C and D still complete
- Workflow: FAILED

### Diamond with Partial Failure

```
        ┌→ B ─┐
    A ──┤     ├→ D
        └→ C ─┘
```

- If B fails: D is SKIPPED (waits for both B and C)
- Even though C succeeds, D doesn't run
- Workflow: FAILED

### Diamond with Recovery

```
        ┌→ B ─┐
    A ──┤     ├→ D (allow_failed_deps=True)
        └→ C ─┘
```

- If B fails: D still runs (receives B's error, C's success)
- D can implement recovery logic
- Workflow: FAILED (but D produced a result)
  > **Note:** `handle.get()` will return the workflow-level failure. To access D's result in this scenario, use `handle.result_for(node_d)`, or match `handle.results()` to `Ok(results)` and read `results['<node_id of D>']`.

### Multi-Branch with Nested Convergence

```
                    ┌── ca ── e₁
                    │
        ┌── c ──────┤
        │           └── cb ── e₂
a ── b ─┤
        │           ┌── da ── e₃
        └── d ──────┤
                    └── db ── e₄
```

**Current behavior if `c` fails:**
- `ca`, `cb` → SKIPPED
- `e₁`, `e₂` → SKIPPED
- `d`, `da`, `db`, `e₃`, `e₄` → may complete successfully
- Workflow: **FAILED** (because `c` failed)

## Limitations

### Dynamic Task Generation

A running workflow does not add nodes to its own DAG. The DAG of each workflow is fixed when that workflow starts.

To create nodes from runtime data, use a `SubWorkflowNode`. Its `build_with()` runs when the node becomes ready and receives the results of the `args_from` nodes. The child DAG can have one node per element of an upstream result. See [Dynamic Fan-Out with `args_from`](/horsies/concepts/workflows/subworkflows/#dynamic-fan-out-with-args_from).

## Support Matrix

| Pattern | Supported | Notes |
|---------|-----------|-------|
| Linear chain | Yes | Basic sequential execution |
| Fan-out (parallel branches) | Yes | Independent parallel execution |
| Fan-in (convergence) | Yes | AND-join: all branches must complete |
| Diamond pattern | Yes | Classic DAG pattern |
| AND-join with error handling | Yes | `allow_failed_deps=True` |
| OR-join (any branch succeeds) | Yes | `join="any"` parameter |
| Quorum join (N of M succeed) | Yes | `join="quorum"` with `min_success` |
| Partial success workflow | Yes | Via `success_policy` with success cases |
| Dynamic task generation | Yes, through subworkflows | `build_with()` builds the child DAG when the node becomes ready |
| Nested subworkflows | Yes, to depth 32 | A deeper child fails its `SubWorkflowNode` with `SUBWORKFLOW_DEPTH_LIMIT_REACHED` |
| Nodes per workflow | Up to 10,000 | Split more nodes into `SubWorkflowNode`s; each child workflow has its own limit |

## Retry Policies in Workflows

Workflow tasks honor the same retry policies configured on the `@task` decorator. When a task with a `retry_policy` is used in a workflow, those settings (including `auto_retry_for`) are preserved and applied when the workflow engine enqueues the task.

The Python example is in [Retry Policies in Workflows](/horsies/concepts/workflows/workflow-examples/#retry-policies-in-workflows).

**Behavior:**

- `max_retries` from the retry policy is applied to the underlying task
- `auto_retry_for` error codes (on `RetryPolicy`) trigger automatic retries on matching failures
- Retries happen at the task level (worker handles retry scheduling)
- A task that fails after exhausting retries propagates failure to the workflow

## Success Cases (Partial Success Policy)

By default, any task failure marks the workflow as **FAILED**. The `success_policy` option allows defining explicit success criteria, so a workflow can be **COMPLETED** even if some tasks fail.

### SuccessCase and SuccessPolicy

A success policy holds success cases. Each case lists the nodes that it requires. `optional` lists nodes that may fail. The Python examples are in [Success Policy](/horsies/concepts/workflows/workflow-examples/#success-policy).

### Semantics

1. **Case evaluation**: A `SuccessCase` is **satisfied** if ALL its `required` nodes are COMPLETED. A node is a `TaskNode` or a `SubWorkflowNode`.
2. **Workflow success**: Workflow is **COMPLETED** if **any** SuccessCase is satisfied.
3. **SKIPPED counts as not satisfied**: A SKIPPED task does not satisfy a required condition.
4. **`optional` is declarative**: It records which nodes may fail, and it has no runtime effect. A node that no case requires does not change the result, with or without `optional`. Spec validation checks that each node in `optional` is in the workflow.
5. **Default behavior**: If `success_policy` is None, any task failure → FAILED (unchanged).
6. **Stored policy that does not parse**: The start stores the policy as node indices. If the stored value is not that form (for example a row written by hand), the completion step fails with SQLSTATE `XX001` (`data_corrupted`), writes nothing, and does not fall back to the default rule. The [recovery limit](#recovery-limit) then fails the workflow closed with `WORKFLOW_RECOVERY_LIMIT_REACHED`.

### Error Handling with Success Policy

When no success case is satisfied:

1. If a **required** task from any case FAILED, the workflow error contains that task's error.
2. If no required task FAILED (all SKIPPED), the error is `WORKFLOW_SUCCESS_CASE_NOT_MET`.

### Output Node Under a Success Policy

A success case does not have to require the output node. The workflow is then COMPLETED, and its output node can be SKIPPED or FAILED. The workflow result follows the status of the output node:

<!-- members: WorkflowTaskStatus subset -->
| Output node | Workflow result | `WorkflowHandle.get()` returns |
|-------------|-----------------|--------------------------------|
| COMPLETED | The node's result | The node's `TaskResult` |
| FAILED | The node's error | `TaskResult(err=...)` with the node's error |
| SKIPPED | An error with `WORKFLOW_OUTPUT_SKIPPED` | `TaskResult(err=...)` with `OutcomeCode.WORKFLOW_OUTPUT_SKIPPED` |

`WORKFLOW_OUTPUT_SKIPPED` means that the workflow succeeded and has no output. It does not mean that stored data is corrupt. `data` holds `workflow_id`, `output_node_id`, `output_task_index` and `output_task_name`.

The Python example is in [Output Node Under a Success Policy](/horsies/concepts/workflows/workflow-examples/#output-node-under-a-success-policy).

A node is COMPLETED only when its result is ok, so the status of a `SubWorkflowNode` follows the output of its child, not the status of its child. A `SubWorkflowNode` of such a child workflow is FAILED and holds the child's err:

- `on_error` runs, as for a failed task.
- A dependent with `allow_failed_deps=False` is SKIPPED. A dependent with `allow_failed_deps=True` receives the err through `args_from` or `workflow_ctx_from`.
- `join="any"` and `join="quorum"` do not count the node.
- A `SuccessCase` that requires the node is not met.
- `result_for()`, `results()` and `tasks()` return the err.
- Without a success policy, the parent is FAILED, and its `get()` returns the err.
- `SubWorkflowSummary.status` is COMPLETED on this FAILED node, because the child is COMPLETED. `SubWorkflowSummary.output` is `None`.

A child workflow with no output node is not affected: its `SubWorkflowNode` is COMPLETED with `ok=None`.

**A parent that completes with this node.** The parent's success case can leave the node out: the node is in `optional`, or in no case. The node is still FAILED, and `on_error` still runs:

- With `OnError.FAIL`, the node's error is stored on the workflow, and the workflow stays RUNNING. The success case then decides COMPLETED. A COMPLETED parent keeps the node's error in its error column, as for a failed `TaskNode`.
- With `OnError.PAUSE`, the parent pauses.

If the node is also the parent's output node, the parent stores the same err, and its `get()` returns it. A `SubWorkflowNode` of that parent is FAILED in the same way, and `data` still names the workflow that holds the SKIPPED output node.

**Nesting without a success policy.** The parent is FAILED, so the next level sees a failed child: its node holds a `SubWorkflowError` with `SUBWORKFLOW_FAILED`. The message of the first error is in `SubWorkflowSummary.error_summary`.

**A child result that does not parse.** A COMPLETED child whose stored result is not a well-formed result envelope, or that stores no result, also makes a FAILED node. The node holds an error envelope with `RESULT_DESERIALIZATION_ERROR` that names the child and the cause, so each reader of the node, the parent's `get()` and the next level report that code. The stored text stays in the `result` column of the child workflow.

An outputless workflow has no output node, so its terminal nodes are its output. In its `get()` map, the entry of a SKIPPED terminal node holds `WORKFLOW_OUTPUT_SKIPPED`, and `data` names that node.

## Best Practices

1. **Use `allow_failed_deps` for recovery.** A recovery node with `allow_failed_deps=True` runs after a failed dependency and receives its error.
2. **Design for failure visibility.** An aggregator node with `allow_failed_deps=True` that waits for every branch reports all branch outcomes in one result. The workflow is still FAILED, so read the aggregator by its node result.
3. **Use `OnError.PAUSE` for critical workflows.** A failure then stops the workflow for review.
4. **Name the output node.** The workflow result is then the result of that node.

The Python examples are in [Best Practices](/horsies/concepts/workflows/workflow-examples/#best-practices).

## Result Keying

Results from `WorkflowHandle.results()` and `WorkflowHandle.get()` (when no output task is specified) use unique keys in the format `node_id`.

This ensures no result collisions when the same task function is used multiple times in a workflow.

The Python example is in [Result Keying](/horsies/concepts/workflows/workflow-examples/#result-keying).

## WorkflowContext (Type-Safe Result Access)

Tasks can access upstream results via `WorkflowContext` using the type-safe `result_for(...)` method.

### Enabling WorkflowContext

1. Set `workflow_ctx_from` on the TaskNode to specify which upstream results to include
2. Declare `workflow_ctx: WorkflowContext | None` parameter in the task function

**Important:** `workflow_ctx_from` is **per-task**. The context is built fresh for each task that opts in, and does **not** persist or propagate to downstream tasks.

**Scope:** Inside a task function, only results from nodes listed in `workflow_ctx_from` are accessible via `workflow_ctx.result_for()`. Accessing a node not in the list raises `KeyError` with `"TaskNode id '{id}' not in workflow context"`.

This is distinct from `WorkflowHandle.result_for()`, which queries the database for any node in the workflow and returns `RESULT_NOT_READY` if the task hasn't completed.

### Node IDs

The context looks up results by **node_id**, not by object identity. `node_id` is optional on a node. If omitted, it is auto-assigned when the spec is built, in the format `{slugified_workflow_name}:{task_index}`. The slug converts spaces to underscores and removes invalid characters: a workflow named `My Data Pipeline` gets the node ids `My_Data_Pipeline:0`, `My_Data_Pipeline:1`.

You can also provide an explicit `node_id` for stable external references.
Explicit node_ids must match the pattern `[A-Za-z0-9_\-:.]+`.

The Python API (typed `result_for`, `NodeKey`, `slugify`) is in [WorkflowContext](/horsies/concepts/workflows/workflow-examples/#workflowcontext).

### WorkflowMeta (Metadata Only)

A task that needs only the workflow metadata (the workflow id, the task index, the task name), and no results, declares a workflow meta parameter. The engine injects it when the task declares it, with no `workflow_ctx_from`. The Python example is in [WorkflowMeta](/horsies/concepts/workflows/workflow-examples/#workflowmeta).

### Comparison: args_from vs workflow_ctx_from

| Feature | `args_from` | `workflow_ctx_from` |
|---------|-------------|---------------------|
| Use case | Direct typed parameters | Access multiple upstream results |
| Type safety | Full (typed parameter) | Full (via `result_for(node)`) |
| Function signature | Explicit parameters | Single `WorkflowContext` param |
| Best for | 1-3 dependencies | Many dependencies, aggregators |

**Prefer `args_from`** for simple cases - it's more explicit and keeps data flow obvious.

**Use `workflow_ctx_from`** when you need to access many upstream results or want workflow metadata.

**Important:** When using `args_from` or `workflow_ctx_from`, positional `args` are not allowed.
Put static values in `kwargs` instead.

**Important:** `kwargs` and `args_from` keys must be disjoint. If the same key appears in both,
validation fails with `HRS-021` (`WORKFLOW_KWARGS_ARGS_FROM_OVERLAP`).

For type-safe node construction with static arguments, see [Typed Node Builder](/horsies/concepts/workflows/typed-node-builder/).

### args_from: What the Receiving Function Gets

`args_from` delivers the upstream task's full `TaskResult[T, TaskError]` — not just the raw `T`.

The Python example is in [What the Receiving Function Gets](/horsies/concepts/workflows/workflow-examples/#what-the-receiving-function-gets).

## Things to Avoid

### Reading a Node Result Before the Workflow Ends

A node result read does not block: it is one database query. When the task of the node has not completed, the read returns `RESULT_NOT_READY`. Workflows can run for minutes to hours, so wait for the workflow first, for example with the workflow result read and a timeout, and then read node results. The Python example is in [Things to Avoid](/horsies/concepts/workflows/workflow-examples/#things-to-avoid).
