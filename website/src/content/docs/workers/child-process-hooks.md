---
title: Child-Process Hooks
summary: Per-child-process initialization with @app.on_child_process_start, for app-owned engine disposal and worker-specific pool policy.
related: [worker-architecture, ../configuration/app-config]
tags: [workers, hooks, connections, pooling, fork, spawn]
---

`@app.on_child_process_start` registers a sync function that runs once in every worker child process: after task-module imports (so app engines exist) and before the child opens horsies' own connection pool. It is the supported seam for application-owned per-child setup — the equivalent of Celery's `worker_process_init` signal and Dramatiq's `after_process_boot` middleware.

## Why and When

A horsies worker runs task code in N child processes. Application code commonly holds its own module-level SQLAlchemy engine, separate from horsies' broker. SQLAlchemy's `QueuePool` has no idle eviction, so each child retains its pool's connections for the life of the process: the app-owned connection floor plateaus at one pool per child and never drains. On fork, children additionally inherit the parent's engine state, which SQLAlchemy requires disposing inside the child.

Use the hook to set a worker-specific pool policy (e.g. a warm pool for web traffic, `NullPool` or a tiny pool in worker children) and to dispose inherited engines. Without it, neither is reliably possible: under fork the engine is built in the parent and inherited, so module-level construction never re-runs in the child.

## How To

Task code must read the engine through indirection — an `Engine`'s pool class is fixed at `create_engine()` time, so the hook replaces the engine object rather than mutating it:

```python
# app/databases/postgres.py
from sqlalchemy import Engine, create_engine, QueuePool

_app_engine: Engine | None = None

def _build_engine(*, worker_child: bool) -> Engine:
    if worker_child:
        # worker default: tiny bounded pool (NullPool behind PgBouncer)
        return create_engine(DB_URL, poolclass=QueuePool, pool_size=1, max_overflow=0)
    return create_engine(DB_URL, pool_size=10)

def get_engine() -> Engine:  # task code reads via this, never the global
    global _app_engine
    if _app_engine is None:
        _app_engine = _build_engine(worker_child=False)
    return _app_engine
```

```python
# app/configs/horsies.py
from app.databases import postgres

@app.on_child_process_start
def reset_db_for_child() -> None:  # idempotent, sync
    if postgres._app_engine is not None:
        postgres._app_engine.dispose(close=False)  # fork-safety: drop inherited FDs
    postgres._app_engine = postgres._build_engine(worker_child=True)
```

Hooks run in registration order and fire on every child start, including executor restarts — bodies must be idempotent. Register hooks in the app module or a discovered task module; a module the child never imports cannot fire its hooks under spawn.

### Pool policy

The hook is the mechanism; the pool policy is the application's choice:

| Situation | Recommended app-engine pool |
| --------- | --------------------------- |
| General default | `QueuePool(pool_size=1, max_overflow=0)` — warm reuse, floor capped at 1 per child |
| PgBouncer / transaction pooler with short DB touches | `NullPool` — floor ~0; connects to the pooler are cheap |
| DB-heavy tasks against direct Postgres | Small `QueuePool`; measure latency against connection count |

`NullPool` trades idle economy for a fresh connect (TCP/TLS/auth) per checkout and has no active-connection cap. It applies to the application's engines only — horsies' own child pool is not configurable through this hook.

## Failure Semantics

Fail-closed: a hook that raises, or runs past its 10-second budget, terminates the child with a dedicated exit code and the hook's name in the worker log. The parent recognizes that exit code and stops the worker instead of restart-looping — the hook re-runs in every replacement child, so retrying cannot succeed. For best-effort work, catch exceptions inside the hook body.

## Things to Avoid

**Don't dispose without rebinding.** `dispose()` alone does not change the floor: a `QueuePool` refills on the next checkout and retains idle connections again.

**Don't read the engine through a captured reference.**

```python
# Wrong — task captured the engine before the hook replaced it
from app.databases.postgres import get_engine
engine = get_engine()  # module-import time, parent's engine

@app.task("report")
def report() -> TaskResult[int, TaskError]:
    with engine.connect() as conn:  # rebind never took effect here
        ...

# Correct — resolve at call time
@app.task("report")
def report() -> TaskResult[int, TaskError]:
    with get_engine().connect() as conn:
        ...
```

**Don't register async functions.** Hooks run during child boot, before any event loop exists; registration rejects coroutine functions (`HRS-214`). Dispose async engines via `async_engine.sync_engine.dispose(close=False)`.

## Limits

- Engines created lazily after the hook runs, raw psycopg clients outside any pool, and pools hidden in C extensions are out of reach — only app-side close-per-use helps there.
- A sync hook cannot set pool policy for an async engine driven via `asyncio.run` inside a task; choose that policy at construction.

## API Reference

### `@app.on_child_process_start`

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `fn` | `ChildProcessStartHook` (`Callable[[], None]`) | Yes | Sync, zero-argument function. Registered once per identity; duplicate registrations are ignored. |

**Returns:** the function unchanged (usable as a decorator).

**Raises:** `ConfigurationError` (`HRS-214`) — `fn` is not callable or is an async function.

### `app.get_child_process_start_hooks() -> list[ChildProcessStartHook]`

Snapshot of registered hooks in registration order.
