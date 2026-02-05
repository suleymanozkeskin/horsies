---
title: Typed Node Builder
summary: Create TaskNodes with static type safety using the .node() API.
related: [workflow-api, workflow-semantics, subworkflows]
tags: [concepts, workflows, types, api]
---

# Typed Node Builder

The `.node()` method on `TaskFunction` creates `TaskNode` instances with static type checking. Type checkers (pyright, mypy) validate argument names and types at development time.

## How To

### Basic Usage

```python
from horsies import Horsies, AppConfig, PostgresConfig, TaskResult, TaskError

app = Horsies(AppConfig(
    broker=PostgresConfig(database_url="postgresql+psycopg://..."),
))

@app.task(task_name='compute_score')
def compute_score(
    user_id: str,
    base_score: int,
    multiplier: float = 1.0,
) -> TaskResult[int, TaskError]:
    return TaskResult(ok=int(base_score * multiplier))

# Type-safe node construction
node = compute_score.node(node_id='score')(
    user_id='user-123',
    base_score=100,
    multiplier=1.5,
)
```

### Two-Stage Call Pattern

The API separates workflow configuration from task arguments:

```python
task.node(workflow_options)(task_arguments)
```

Stage 1 accepts workflow options (keyword-only): `waits_for`, `args_from`, `workflow_ctx_from`, `node_id`, `allow_failed_deps`, `join`, `min_success`, etc.

Stage 2 accepts task arguments matching the function signature.

```python
# Separate stages
factory = fetch_user.node(
    waits_for=[auth_node],
    node_id='fetch',
)
node = factory(user_id='user-123', include_profile=True)

# Combined
node = fetch_user.node(node_id='fetch')(
    user_id='user-123',
    include_profile=True,
)
```

### With Dependencies

```python
_score_a = compute_score.node(node_id='score_a')(
    user_id='user-1',
    base_score=100,
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
)
```

### Mixed with args_from

Use `.node()` for root tasks with static arguments. Use `TaskNode` for tasks receiving `args_from` injections:

```python
from horsies import TaskNode

@app.task(task_name='fetch_user')
def fetch_user(user_id: str) -> TaskResult[UserData, TaskError]:
    ...

@app.task(task_name='process_user')
def process_user(
    user_data: TaskResult[UserData, TaskError],
    threshold: int = 50,
) -> TaskResult[ProcessedUser, TaskError]:
    ...

# Root task: .node() with static args
fetch_node = fetch_user.node(node_id='fetch')(user_id='user-123')

# Downstream task: TaskNode with args_from
process_node = TaskNode(
    fn=process_user,
    waits_for=[fetch_node],
    args_from={'user_data': fetch_node},
    kwargs={'threshold': 70},
    node_id='process',
)
```

### In WorkflowDefinition

Define nodes at module level to reference in `Meta.output`:

```python
from horsies import WorkflowDefinition, OnError

_score_a = compute_score.node(node_id='score_a')(
    user_id='user-1',
    base_score=100,
)

_score_b = compute_score.node(node_id='score_b')(
    user_id='user-2',
    base_score=80,
)

class ScoreWorkflow(WorkflowDefinition[int]):
    name = 'score_workflow'

    score_a = _score_a
    score_b = _score_b

    class Meta:
        output = _score_b
        on_error = OnError.FAIL
```

## Things to Avoid

**Don't use `.node()` when parameters come from `args_from`.**

```python
# Wrong - user_data is injected at runtime, not known at construction
process_node = process_user.node(node_id='process')(
    user_data=???,  # No value to pass
    threshold=70,
)

# Correct - use TaskNode with args_from
process_node = TaskNode(
    fn=process_user,
    args_from={'user_data': fetch_node},
    kwargs={'threshold': 70},
    node_id='process',
)
```

## API Reference

### `TaskFunction.node(**workflow_opts) -> NodeFactory[P, T]`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `waits_for` | `Sequence[TaskNode \| SubWorkflowNode]` | `None` | Dependencies |
| `args_from` | `dict[str, TaskNode \| SubWorkflowNode]` | `None` | Result injection mapping |
| `workflow_ctx_from` | `Sequence[TaskNode \| SubWorkflowNode]` | `None` | Context sources |
| `node_id` | `str` | `None` | Stable identifier |
| `queue` | `str` | `None` | Queue override |
| `priority` | `int` | `None` | Priority override |
| `allow_failed_deps` | `bool` | `False` | Run despite failed deps |
| `run_when` | `Callable[[WorkflowContext], bool]` | `None` | Conditional execution |
| `skip_when` | `Callable[[WorkflowContext], bool]` | `None` | Conditional skip |
| `join` | `Literal['all', 'any', 'quorum']` | `'all'` | Dependency join semantics |
| `min_success` | `int` | `None` | Required for `join='quorum'` |
| `good_until` | `datetime` | `None` | Task expiry deadline |

**Returns:** `NodeFactory[P, T]` — callable with the task's original signature, returns `TaskNode[T]`.
