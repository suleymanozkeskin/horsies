---
title: Typed Node Builder
summary: Create TaskNodes with full static type safety using the .node() API.
related: [workflow-api, workflow-semantics, subworkflows]
tags: [concepts, workflows, types, api]
---

# Typed Node Builder

The `.node()` method on `TaskFunction` provides a type-safe way to create `TaskNode` instances. Static type checkers validate argument names and types at development time.

## The Problem

Traditional `TaskNode` construction uses untyped `kwargs`:

```python
# Traditional approach - kwargs is dict[str, Any]
node = TaskNode(
    fn=compute_score,
    kwargs={
        'user_id': 'user-123',
        'base_score': 100,
        'multiplier': 1.5,
    },
)

# Typo in 'base_scor' - not caught until runtime!
node = TaskNode(
    fn=compute_score,
    kwargs={
        'user_id': 'user-123',
        'base_scor': 100,  # Silent bug
    },
)
```

## The Solution: `.node()` Builder

The `.node()` method returns a `NodeFactory` that preserves the task's type signature:

```python
@app.task(task_name='compute_score')
def compute_score(
    user_id: str,
    base_score: int,
    multiplier: float = 1.0,
) -> TaskResult[int, TaskError]:
    return TaskResult(ok=int(base_score * multiplier))

# Type-safe construction
node = compute_score.node(node_id='score')(
    user_id='user-123',   # Type checked: must be str
    base_score=100,       # Type checked: must be int
    multiplier=1.5,       # Type checked: must be float
)

# Typo caught by pyright/mypy!
node = compute_score.node(node_id='score')(
    user_id='user-123',
    base_scor=100,  # Error: unexpected keyword argument "base_scor"
)

# Wrong type caught!
node = compute_score.node(node_id='score')(
    user_id='user-123',
    base_score='100',  # Error: expected int, got str
)
```

## Two-Stage Call Pattern

The API separates workflow configuration from task arguments:

```python
task.node(workflow_options)(task_arguments)
```

**Stage 1: Workflow options** (keyword-only)
- `waits_for` - dependency nodes
- `args_from` - result injection mapping
- `workflow_ctx_from` - context sources
- `node_id` - stable identifier
- `allow_failed_deps`, `join`, `min_success`, etc.

**Stage 2: Task arguments** (typed)
- Positional and keyword arguments matching the task function signature
- Full IDE autocomplete and type checking

```python
# Stage 1: workflow config
factory = fetch_user.node(
    waits_for=[auth_node],
    node_id='fetch',
)

# Stage 2: typed task args
node = factory(
    user_id='user-123',
    include_profile=True,
)

# Or combined:
node = fetch_user.node(node_id='fetch')(
    user_id='user-123',
    include_profile=True,
)
```

## When to Use `.node()`

**Ideal for:**
- Root tasks with static arguments
- Tasks where all parameters are known at workflow construction time
- Workflows where type safety is critical

**Use traditional `TaskNode` for:**
- Tasks receiving `args_from` injections (parameters filled at runtime)
- Dynamic argument values computed at runtime

## Hybrid Approach

Combine `.node()` for root tasks with `TaskNode` for downstream injection:

```python
@app.task(task_name='fetch_user')
def fetch_user(user_id: str) -> TaskResult[UserData, TaskError]:
    ...

@app.task(task_name='process_user')
def process_user(
    user_data: TaskResult[UserData, TaskError],
    threshold: int = 50,
) -> TaskResult[ProcessedUser, TaskError]:
    ...

# Root: use .node() for type-safe static args
fetch_node = fetch_user.node(node_id='fetch')(
    user_id='user-123',  # Type checked
)

# Downstream with args_from: use TaskNode
# The 'user_data' parameter is injected at runtime
process_node = TaskNode(
    fn=process_user,
    waits_for=[fetch_node],
    args_from={'user_data': fetch_node},
    kwargs={'threshold': 70},  # Static args in kwargs
    node_id='process',
)
```

## Complete Example

```python
from horsies.core.models.workflow import TaskNode, WorkflowDefinition, OnError

@app.task(task_name='compute_score')
def compute_score(
    user_id: str,
    base_score: int,
    multiplier: float = 1.0,
    include_bonus: bool = False,
) -> TaskResult[int, TaskError]:
    score = int(base_score * multiplier)
    if include_bonus:
        score += 10
    return TaskResult(ok=score)

# Define nodes at module level for Meta reference
_score_a = compute_score.node(node_id='score_a')(
    user_id='user-1',
    base_score=100,
    multiplier=1.5,
)

_score_b = compute_score.node(node_id='score_b')(
    user_id='user-2',
    base_score=80,
)

_score_c = compute_score.node(
    waits_for=[_score_a, _score_b],
    node_id='score_c',
)(
    user_id='user-3',
    base_score=90,
    include_bonus=True,
)


class ScoreWorkflow(WorkflowDefinition[int]):
    name = 'score_workflow'

    score_a = _score_a
    score_b = _score_b
    score_c = _score_c

    class Meta:
        output = _score_c
        on_error = OnError.FAIL
```

## NodeFactory Signature

```python
def node(
    self,
    *,
    waits_for: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None,
    workflow_ctx_from: Sequence[TaskNode[Any] | SubWorkflowNode[Any]] | None = None,
    args_from: dict[str, TaskNode[Any] | SubWorkflowNode[Any]] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    allow_failed_deps: bool = False,
    run_when: Callable[[WorkflowContext], bool] | None = None,
    skip_when: Callable[[WorkflowContext], bool] | None = None,
    join: Literal['all', 'any', 'quorum'] = 'all',
    min_success: int | None = None,
    good_until: datetime | None = None,
    node_id: str | None = None,
) -> NodeFactory[P, T]
```

The returned `NodeFactory[P, T]` is callable with the task's original signature, returning `TaskNode[T]`.
