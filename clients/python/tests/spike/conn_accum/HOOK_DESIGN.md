# Design: per-child-process init hook (`@app.on_child_process_start`)

Status: **design finalized after two adversarial rounds (internal workflow + Codex) and a prior-art check, pending implementation sign-off.**

## What this is (and is not)

This is **not** a connection-leak fix. There is no leak:

- The connections in question are **app-owned** (the application's own SQLAlchemy
  engines), not horsies'. In the production-candidate measurement, horsies' own
  footprint was ~1 (the LISTEN listener); the ~60+ were the app's engine pools.
- The behaviour is **bounded, by-design pool retention**: `QueuePool` returns and
  commits every connection (0 idle-in-transaction) and caps the count at
  `pool_size` per child. It simply has no idle eviction, so the count *plateaus
  and stays flat* rather than decaying. A leak would climb without bound and not
  drop cleanly to 0 on shutdown; the repro showed a flat floor and a clean 0.

What the hook provides is a **supported per-child-process seam so an application
can set its own worker-child connection-economy policy** (and, on fork, dispose
inherited engine state). It right-sizes a *legitimate but undesirable* standing
footprint; it does not repair a defect.

## Problem (grounded by step-1 repro, both start methods)

A horsies worker runs N child processes (`ProcessPoolExecutor`, `worker.py:272-320`).
Application task code commonly uses its own module-level SQLAlchemy engine(s),
separate from horsies' broker. Measured (4 children, 40 slow tasks, attribution
by `application_name`):

- **spawn (macOS)** and **fork (Linux container)** behaved identically: app-engine
  connections rose to **N (= child count)** during the burst and **stayed flat,
  idle, 0 idle-in-transaction, through a 12s drain after completion** — dropping
  to 0 only when the worker was killed.
- The fork **inheritance hazard did not trigger**: the parent never checks out the
  app engine, so its pool is empty at fork; children open fresh connections. A
  post-fork `dispose()` alone would not have changed the measured floor — the
  pool-policy half is what matters here, not the dispose half.

`QueuePool` has no idle eviction (`pool_recycle` recycles only *at next checkout*).
horsies disposes only its OWN broker per child (`_discard_inherited_broker`,
`child_runner.py:174-192`) and exposes no public seam for app-owned resources
(`WorkerConfig` has no callback; `_child_initializer` is private;
`HORSIES_CHILD_PROCESS` is internal + spawn-only).

## Why this belongs in horsies — prior art

The need follows from the **execution model**, not from "uses Postgres":

| Library | Worker model | Per-child hook? |
|---|---|---|
| **Celery** | prefork multiprocessing | **Yes** — `worker_process_init` signal: "dispatched in all pool child processes when they start"; the documented place to `engine.dispose()`. Handlers must not block >4s or the child is killed (precedent for our fixed timeout); not emitted under `solo`. |
| **Dramatiq** | prefork multiprocessing | **Yes** — `Middleware.after_process_boot(broker)` ("called immediately after subprocess start up"); plus `before_*_thread_shutdown` documented for "clean up thread-local resources (Django DB connections)", a `--worker-fork-timeout`, and `--use-spawn`. |
| **SQLAlchemy** (the engine itself) | — | Documents this as the **required** pattern: `engine.dispose(close=False)` "within the initialize phase of the child process," and names `multiprocessing.Pool(initializer=...)` as the place — structurally **exactly horsies' `_child_initializer`**. |
| **Procrastinate**, **PgQueuer** (PG-native) | async, single-process | **No — and don't need it.** They don't fork worker children; async sub-workers share one `AsyncConnectionPool` (+ one LISTEN connection outside the pool — same shape as horsies' listener). No process boundary → nothing to reset. |

**Conclusion:** horsies is architecturally in the Celery/Dramatiq camp (prefork),
not the Procrastinate/PgQueuer camp (async single-process). A per-child-process
init hook is the **standard, expected primitive for the prefork model** — horsies
is currently the outlier for lacking it. This is reaching parity with two
reference implementations, not inventing a mechanism. It also validates our
choices: the process-scoped name (cf. `worker_process_init`/`after_process_boot`),
the fixed timeout (Celery 4s / Dramatiq fork-timeout), and the
`dispose(close=False)` body (SQLAlchemy-blessed).

## Relationship to horsies' existing PgBouncer support (orthogonal)

horsies' `pgbouncer_transaction_mode` + `session_database_url` configure
**horsies' OWN connections** (broker/worker pools route through a transaction-mode
PgBouncer; LISTEN/NOTIFY + schema DDL use the direct session URL). They do **not**
touch app engines — horsies cannot know them. So PgBouncer support and this hook
address different connection populations.

PgBouncer is the heavyweight answer for the **real-backend budget** and remains
the recommendation where available — but it is orthogonal, like in Celery/Dramatiq
(users run PgBouncer *and* the per-child hook). The hook's **irreducible value**,
the part PgBouncer/uniform config does not cover:

- **Worker-child-*specific* pooling, reliably under both fork and spawn.** An app
  that wants a warm `QueuePool` in its web/API context but `NullPool`/tiny in
  workers (shared engine module) must detect "I'm in a worker child" and configure
  accordingly. That is impossible today: `HORSIES_CHILD_PROCESS` is spawn-only
  (under fork the engine is built in the parent and inherited; the child never
  re-runs construction), and there is no public in-child callback.
- **A documented fork-safety disposal seam** (belt-and-suspenders here, since the
  hazard does not trigger in the worker's access pattern).

If an app is content to use the **same** pooling everywhere (e.g. uniform
`NullPool`/small-pool + PgBouncer), the hook buys little — that path needs no
horsies change. The hook earns its place specifically for **context-specific**
worker pooling, which is the common real case for apps that also serve web traffic
from the same engine module.

## Decisions (settled)

| Choice | Decision | Why |
|---|---|---|
| Surface | `@app.on_child_process_start` decorator, registry `app._child_process_start_hooks`, **deduped by function identity** | App-level registration rides the same import-time path as `@app.task`; works under fork (inherited) and spawn (re-import). A `WorkerConfig` callable would have to survive `initargs` pickling across spawn/restart — rejected. |
| Name | `on_child_process_start` | Process-scoped boot, matching Celery `worker_process_init` / Dramatiq `after_process_boot`; avoids the "before every task" misread of `on_child_start`. |
| Signature | `Callable[[], None]`, **sync only** | App reachable via `get_current_app()` (set at `child_runner.py:232`); async engines via `async_engine.sync_engine.dispose(close=False)`. `close=False` reference-drop is the only correct fork-time op and is inherently sync. |
| Placement | After task-module imports, **before** `_initialize_worker_pool` (`child_runner.py:257`) | App engines exist (post-import); a failing hook aborts before horsies opens its own child pool. |
| Failure | **Fail-closed, clean terminal exit, no mode flag** | A failed pool-policy install must not silently proceed to the accumulation state. Routed OFF the `BrokenProcessPool` retry path (distinct exit code + `on_child_process_start hook <name> failed` message) so it is legible, not an opaque restart-loop. Best-effort = the app catches inside its own hook. |
| Timeout | Fixed in-child default; on timeout → same clean fatal | A hanging hook must not hang child init silently (cf. Celery's 4s limit). |
| Idempotency | Hooks fire **once per OS process, including every respawn**; bodies MUST be idempotent | Restart/lazy-respawn re-run the initializer (`worker.py:346-380, 1321`). |

## Hook = mechanism; app = policy

The hook is the reliable in-child *seam*. It is **necessary but not sufficient**;
the app supplies policy. Two operations belong in the body:

1. **Fork-safety:** `dispose(close=False)` inherited engines (drop FDs without
   closing the parent's sockets).
2. **Pool policy:** **rebind** the worker-child app engine to a worker-appropriate
   pool. This is the part that changes the floor — `dispose()` alone does not: a
   `QueuePool` refills on the next checkout and re-retains idle for the child's
   life. A SQLAlchemy `Engine`'s `poolclass` is **fixed at `create_engine()` time**
   and cannot be mutated, so the app must expose an engine **factory** (or a
   reassignable module global) that task code reads through indirection — otherwise
   the rebind is a no-op.

### Pool-policy matrix (documented; app chooses)

| Situation | Recommended app-engine pool |
|---|---|
| **General default** | `QueuePool(pool_size=1, max_overflow=0)` — warm reuse, bounded, floor capped at 1/child, gives backpressure (safe if threaded execution ever lands) |
| PgBouncer / transaction pooler + short DB touches | `NullPool` — floor ~0; app→bouncer connects are cheap |
| DB-heavy / frequent tasks (direct Postgres) | small `QueuePool`; measure latency + connection count |

`NullPool` trades idle economy for per-checkout connect (TCP/TLS/auth) churn and
has **no active-connection cap** — opt-in, not the blanket default. **Never** apply
NullPool to horsies' own control-plane child pool (`child_pool.py:35`, `min=0/max=2`):
per-op reconnect would worsen control-plane RTT chattiness.

### Canonical example (ships in docs)

```python
# app/databases/postgres.py
from sqlalchemy import Engine, create_engine, NullPool, QueuePool

_app_engine: Engine | None = None

def _build_engine(*, worker_child: bool) -> Engine:
    if worker_child:
        # general default: tiny bounded pool (use NullPool behind PgBouncer)
        return create_engine(DB_URL, poolclass=QueuePool, pool_size=1, max_overflow=0)
    return create_engine(DB_URL, pool_size=10)

def get_engine() -> Engine:            # task code MUST read via this, not a global
    global _app_engine
    if _app_engine is None:
        _app_engine = _build_engine(worker_child=False)
    return _app_engine

# app/configs/horsies.py
@app.on_child_process_start
def _reset_db_for_child() -> None:     # idempotent, sync, fail-closed
    global _app_engine
    if _app_engine is not None:
        _app_engine.dispose(close=False)   # fork-safety: drop inherited FDs
    _app_engine = _build_engine(worker_child=True)  # policy: rebind pool
```

## What the hook CANNOT fix (honest boundary)

- Engines created **lazily after** the hook runs.
- Hidden third-party / C-extension pools, or **raw psycopg clients** opened
  outside any SQLAlchemy pool — only app-side close-per-use helps.
- **Async engines driven via `asyncio.run` inside a sync task** — the sync hook can
  dispose the `sync_engine` view but cannot set a per-task pool policy for the
  async pool; that must be done at construction.
- Hooks registered in a module **not import-reachable** in the child (must live in
  the app module or a discovered task module) — silently does not fire under spawn.
- Floor math (`app_pool × children × hosts`) assumes **persistent children** (no
  `max_tasks_per_child` today, `worker.py:281-295`).

## Compatibility

Purely additive; no behaviour change when no hook is registered. Supersedes the
internal `HORSIES_CHILD_PROCESS` env var (kept internal). Pre-1.0; no contract break.

## Implementation plan (after sign-off)

1. **This doc** signed off.
2. `Horsies.on_child_process_start` + `_child_process_start_hooks` (dedup by
   identity); explicit `ChildProcessStartHook` type alias; all args/returns typed.
3. Invoke in `_child_initializer` after imports, before `_initialize_worker_pool`
   (`child_runner.py:257`): each hook wrapped with attribution + fixed timeout;
   any failure/timeout → clean terminal child exit with a distinct code, routed so
   it does NOT enter the `BrokenProcessPool`/retry path (`worker.py:451-455`).
4. `cd backend && uv run pyright` on changed files; targeted ruff.
5. **Regression test (graduates the spike):** register a hook using the EXACT
   documented body; assert post-burst `application_name` floor → ~0 (NullPool) or
   ≤1/child (tiny QueuePool) vs N-children unfixed. Run under **spawn** (macOS) and
   **fork** (Linux container) — both already exercised in step 1.
6. **Failure-path test:** raising hook → clean terminal exit + logged hook name +
   **no** restart-loop (assert no repeated `_restart_executor`).

### Must NOT do
- Frame this as a leak fix, or as horsies' connections — it is app-owned, bounded retention.
- Ship a "rebind via `dispose()` alone" body, or claim `dispose()` fixes the floor.
- Apply NullPool to horsies' own child pool, or make any pool policy a horsies default.
- Route hook failure through generic `BrokenProcessPool` (opaque restart-loop).
- Change `_discard_inherited_broker`'s contract or the restart classifier.

## References
- Celery — `worker_process_init` signal: https://docs.celeryq.dev/en/stable/userguide/signals.html
- Dramatiq — `Middleware.after_process_boot`: https://dramatiq.io/reference.html
- SQLAlchemy — Connection Pools with Multiprocessing or os.fork(): https://docs.sqlalchemy.org/en/20/core/pooling.html
- Procrastinate: https://procrastinate.readthedocs.io/ · PgQueuer: https://janbjorge.github.io/pgqueuer/
