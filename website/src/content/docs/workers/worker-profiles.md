---
title: Worker Profiles
section: python
summary: Named worker settings in AppConfig - isolated, shared async and shared thread children, recycling, connection pools of the children, of task code that sends and of the worker process and their measured default sizes, claim limits, drain timeout, preload and the core of the worker process, example profiles for one machine - and what horsies check and worker boot validate.
related: [execution-modes, worker-architecture, shared-async-tasks, shared-thread-tasks, concurrency, ../cli, ../configuration/app-config, ../migrations/migration-to-0-7-0]
tags: [workers, profiles, configuration, concurrency, recycling, connections, validation]
---

# Worker Profiles

A worker profile is a named set of worker settings in `AppConfig.worker_profiles`. `horsies worker <app> --profile <name>` starts a worker with the settings of that profile. The settings are part of the app config, so `horsies check` validates every profile before a worker starts.

## Why and When

An app that runs a worker defines at least one profile. Define more than one when the workers of one app run with different settings, for example a small profile for a shared host and a large one for a batch machine. An app that only sends tasks, runs the scheduler, or serves `horsies web` needs no profile.

## Define a Profile

```python
from datetime import timedelta

from pydantic import SecretStr

from horsies import (
    AppConfig,
    ChildPreload,
    ClaimLimits,
    Horsies,
    IsolatedChildren,
    IsolatedRecycle,
    MemoryLimit,
    PostgresConfig,
    TaskLimit,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        'default': WorkerProfile(isolated=IsolatedChildren(count=4)),
        'batch': WorkerProfile(
            isolated=IsolatedChildren(
                count=8,
                recycle=IsolatedRecycle(
                    after=TaskLimit(tasks=500),
                    above=MemoryLimit(mb=1024),
                ),
            ),
            claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=4)),
            drain_timeout=timedelta(seconds=120),
            preload=ChildPreload.APP,
        ),
    },
))
```

```bash
horsies worker myapp.instance:app --profile default
horsies worker myapp.instance:app --profile batch
```

`--profile` is required. There is no default profile. A missing or unknown name stops the command before the app check and before any child starts. The message lists the defined names:

```text
error[HRS-206]: --profile is required
   = note: a worker takes its settings from a named profile in AppConfig.worker_profiles; there is no default profile
   = note: defined profiles: batch, default

   = help:
        start the worker with: horsies worker <app> --profile batch
```

## Execution Modes

A profile sets a number of children for each execution mode. A child never changes its mode. To choose a mode for a task, see [Choosing an Execution Mode](/horsies/workers/execution-modes/).

| Mode | Field | A child runs |
|------|-------|--------------|
| isolated | `isolated=IsolatedChildren(count=N)` | one task at a time, in its own process. See [Worker Architecture](/horsies/workers/worker-architecture/) |
| shared async | `shared_async=SharedAsyncChildren(count=N, seats=K)` | up to K `SHARED_ASYNC` tasks at once, as coroutines on one event loop. See [Shared Async Tasks](/horsies/workers/shared-async-tasks/) |
| shared thread | `shared_thread=SharedThreadChildren(count=N, seats=K)` | up to K `SHARED_THREAD` tasks at once, each on a thread of its own. See [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/) |

The number of children of a worker is the sum of the per-mode counts. There is no separate total. A profile with only shared children is valid. A worker with no shared async children runs `SHARED_ASYNC` tasks in isolated children, and a worker with no shared thread children runs `SHARED_THREAD` tasks in isolated children.

```python
from horsies import SharedAsyncChildren, SharedRecycle

io_profile = WorkerProfile(
    isolated=IsolatedChildren(count=2),
    shared_async=SharedAsyncChildren(
        count=4,
        seats=25,
        recycle=SharedRecycle(
            after_per_seat=TaskLimit(tasks=200),
            above=MemoryLimit(mb=2048),
        ),
    ),
)
```

### Run Shared Tasks

A worker runs `SHARED_ASYNC` tasks on shared async children and `SHARED_THREAD` tasks on shared thread children. What runs where, the timeouts and the rules for task authors are on [Shared Async Tasks](/horsies/workers/shared-async-tasks/) and [Shared Thread Tasks](/horsies/workers/shared-thread-tasks/).

```python
import asyncio

from pydantic import SecretStr

from horsies import (
    AppConfig,
    Execution,
    Horsies,
    IsolatedChildren,
    Ok,
    PostgresConfig,
    Result,
    SharedAsyncChildren,
    TaskError,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        'io': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_async=SharedAsyncChildren(count=4, seats=25),
        ),
    },
))


@app.task(
    'fetch_quote',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
    cancel_grace_ms=2_000,
)
async def fetch_quote(*, symbol: str) -> Result[str, TaskError]:
    await asyncio.sleep(0.1)
    return Ok(symbol)
```

```bash
horsies worker myapp.instance:app --profile io
```

This worker runs `ISOLATED` tasks in 2 isolated children and `SHARED_ASYNC` tasks on 4 × 25 = 100 seats.

```python
import hashlib

from pydantic import SecretStr

from horsies import (
    AppConfig,
    Execution,
    Horsies,
    IsolatedChildren,
    Ok,
    PostgresConfig,
    Result,
    SharedThreadChildren,
    TaskError,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        'files': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_thread=SharedThreadChildren(count=2, seats=4),
        ),
    },
))


@app.task('digest_file', execution=Execution.SHARED_THREAD, timeout_ms=60_000)
def digest_file(*, path: str) -> Result[str, TaskError]:
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1 << 20), b''):
            digest.update(block)
    return Ok(digest.hexdigest())
```

```bash
horsies worker myapp.instance:app --profile files
```

This worker runs `ISOLATED` tasks in 2 isolated children and `SHARED_THREAD` tasks on 2 × 4 = 8 seats. The fields are in [`SharedAsyncChildren`](#sharedasyncchildren) and [`SharedThreadChildren`](#sharedthreadchildren).

### Mix the Modes

One app with three profiles, and one task of each mode. Which child runs each task, per profile, is on [Profile Examples](/horsies/workers/execution-modes/#profile-examples).

```python
from pydantic import SecretStr

from horsies import (
    AppConfig,
    Horsies,
    IsolatedChildren,
    PostgresConfig,
    SharedAsyncChildren,
    SharedThreadChildren,
    WorkerProfile,
)

app = Horsies(AppConfig(
    broker=PostgresConfig(
        database_url=SecretStr('postgresql://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        # Every task in an isolated child
        'default': WorkerProfile(isolated=IsolatedChildren(count=8)),
        # Network waits on 4 x 25 = 100 seats; other tasks in 2 isolated children
        'io': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_async=SharedAsyncChildren(count=4, seats=25),
        ),
        # C code that releases the GIL, on a machine with 8 CPUs: 2 x 4 = 8 seats
        'compute': WorkerProfile(
            isolated=IsolatedChildren(count=2),
            shared_thread=SharedThreadChildren(count=2, seats=4),
        ),
    },
))
```

One task of each mode:

```python
import asyncio
import zlib

from horsies import Execution, Ok, Result, TaskError


@app.task('fetch_rate', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def fetch_rate(*, currency: str) -> Result[str, TaskError]:
    await asyncio.sleep(0.1)  # stands in for an HTTP call with its own timeout
    return Ok(currency)


@app.task('compress_report', execution=Execution.SHARED_THREAD, timeout_ms=60_000)
def compress_report(*, text: str) -> Result[int, TaskError]:
    return Ok(len(zlib.compress(text.encode(), level=6)))


@app.task('render_invoice')
def render_invoice(*, invoice_id: int) -> Result[str, TaskError]:
    return Ok(f'invoice-{invoice_id}.pdf')
```

```bash
horsies worker myapp.instance:app --profile io
```

## Recycling

`IsolatedRecycle` sets when an isolated child is replaced: `after` a number of tasks, `above` a resident memory size, or both. What each limit does, and how to choose them, is on [Process Recycling](/horsies/workers/concurrency/#process-recycling) and [Memory Recycling](/horsies/workers/concurrency/#memory-recycling).

```python
# The default is 100; raise it for high-throughput or connection-constrained apps
recycle_after_500 = WorkerProfile(
    isolated=IsolatedChildren(
        count=8,
        recycle=IsolatedRecycle(after=TaskLimit(tasks=500)),
    ),
)

# Disable recycling entirely
no_recycling = WorkerProfile(
    isolated=IsolatedChildren(
        count=8,
        recycle=IsolatedRecycle(after=NoTaskLimit()),
    ),
)
```

```python
# Recycle any child once its RSS reaches 200 MB
memory_recycling = WorkerProfile(
    isolated=IsolatedChildren(
        count=6,
        recycle=IsolatedRecycle(above=MemoryLimit(mb=200)),
    ),
)

# Combine with count recycling: a child recycles when either limit is reached
count_and_memory = WorkerProfile(
    isolated=IsolatedChildren(
        count=6,
        recycle=IsolatedRecycle(
            after=TaskLimit(tasks=500),
            above=MemoryLimit(mb=200),
        ),
    ),
)
```

Memory only: disable count recycling explicitly, or its default `TaskLimit(tasks=100)` still applies.

```python
memory_only = WorkerProfile(
    isolated=IsolatedChildren(
        count=6,
        recycle=IsolatedRecycle(after=NoTaskLimit(), above=MemoryLimit(mb=180)),
    ),
)
```

## Claim Limits

`ClaimLimits` caps the claims of a worker. What each limit does is on [Claiming Controls](/horsies/workers/concurrency/#claiming-controls).

```python
fair_batches = WorkerProfile(
    isolated=IsolatedChildren(count=8),
    claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=4)),
)
```

```python
few_claimed = WorkerProfile(
    isolated=IsolatedChildren(count=8),
    claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=4)),
)
```

## Connection Pools and the Default Executor

A profile sizes three kinds of connection pools:

- The pool of each child, for its own writes: the RUNNING update at the start of a task and the finalizing marker at its end. Each child kind has its own `pool` setting. The seats of a shared child share its pool.
- The task-send pool of each child kind (`task_send_pool`): the pool that task code opens in its child when it sends through the app.
- The pool of the worker process (`WorkerProfile.pool`): the worker process claims tasks and writes the outcome of each task through it. See [The Pool of the Worker Process](#the-pool-of-the-worker-process).

| Child kind | Setting | Default `max_size` |
|------------|---------|--------------------|
| isolated | `IsolatedChildren.pool: IsolatedPool` | `PoolSize(connections=1)` |
| shared async | `SharedAsyncChildren.pool: SharedPool` | `PoolSizeFromSeats()` |
| shared thread | `SharedThreadChildren.pool: SharedPool` | `PoolSizeFromSeats()` |

A shared async child also sizes the default executor of its event loop (`asyncio.to_thread()`, `loop.run_in_executor(None, ...)`) with `SharedAsyncChildren.executor`. The default is `ExecutorThreadsFromSeats()`. An `on_shared_async_child_start` hook that sets a default executor of its own (`loop.set_default_executor()`) replaces it, and that executor keeps its own size.

```python
from horsies import (
    ExecutorThreads,
    IsolatedPool,
    PoolSize,
    SharedPool,
    SharedThreadChildren,
)

remote_profile = WorkerProfile(
    isolated=IsolatedChildren(
        count=4,
        pool=IsolatedPool(max_size=PoolSize(connections=1), check=False),
    ),
    shared_async=SharedAsyncChildren(
        count=2,
        seats=100,
        pool=SharedPool(min_size=2, max_size=PoolSize(connections=8)),
        executor=ExecutorThreads(threads=16),
    ),
    shared_thread=SharedThreadChildren(count=1, seats=16),
)
```

The worker logs the resolved values of each kind, and the resolved pool of the worker process, in its start line:

```text
Worker profile 'remote': isolated=4 (recycle after 100 tasks, above no limit; pool 0 to 1 connections, check off; task_send_pool 2 connections), shared_async=2x100 seats (recycle after 100 tasks per seat, above no limit; pool 2 to 8 connections, check off; executor 16 threads; task_send_pool 2 connections), shared_thread=1x16 seats (recycle after 100 tasks per seat, above no limit; pool 0 to 2 connections, derived from seats, check off; task_send_pool 2 connections), pool=20 connections (derived from 220 seats), ...
```

### The Task-Send Pool

Task code that sends through the app (`task.send()`, `app.get_broker()`) opens the app's broker in its child. The core pool of that broker holds up to `task_send_pool.connections` connections, with no overflow, for every thread and event loop of the child. Each child kind has its own `task_send_pool: PoolSize`. The default is `PoolSize(connections=2)` for every kind. The pool of `PostgresConfig` (`pool_size`, `max_overflow`) does not apply in a child; its `pool_pre_ping` does.

- The seats of a shared child share the one pool.
- The sync and the async tasks of an isolated child share the one pool.

Raise `task_send_pool` for a shared child whose seats send often at the same time:

```python
fanout_profile = WorkerProfile(
    shared_async=SharedAsyncChildren(
        count=2,
        seats=200,
        task_send_pool=PoolSize(connections=8),
    ),
)
```

### The Pool of the Worker Process

The worker process opens one pool of its own. It writes the outcome of each task through it, and its claim passes, claimer heartbeats, health replies and reaper share it. The runner heartbeats use a connection of their own. A task whose child has returned stays RUNNING until the worker process gets a connection of this pool and commits the outcome, so a pool that is too small holds finished tasks.

`WorkerProfile.pool` takes a `ParentPool(max_size)`. The pool holds at most `max_size` connections and has no overflow (`pool_size = max_size`, `max_overflow = 0`), so the connection check counts it exactly. `PostgresConfig.worker_pool_pre_ping` sets its pre-ping. The default, `PoolSizeFromSeats()`, derives the size from the seats of the whole profile; see [Sizes Derived from the Seats](#sizes-derived-from-the-seats).

```python
from horsies import ParentPool

remote_batch_profile = WorkerProfile(
    shared_async=SharedAsyncChildren(count=1, seats=500),
    pool=ParentPool(max_size=PoolSize(connections=30)),
)
```

A `PoolSize` for this pool has at least 3 connections (`PARENT_POOL_MIN_CONNECTIONS`). While the worker is busy, its claim loop holds 1 connection. The other 2 stay free for the other users: the reaper holds 2 during a pass (its gate session and one statement of the pass body), and the claimer heartbeats, the health replies and the workflow writes after a node ends hold 1 for each statement or transaction. The batched success write and the writes of failures and retries run in the core of the worker process, on a pool of the same `max_size`: the batched success write runs at most `max_size - 2` flushers at once, so 2 connections of that pool always stay free for the other writes. With fewer connections, the other users wait for the pool timeout and fail, so `horsies check` and the worker start refuse the profile (`HRS-218`).

The worker process writes plain successes in batches of up to 64 rows, one statement per batch, so they keep few connections busy: a worker of 1000 seats kept at most 3 to 8 of its 20 connections busy at 970-1,760 tasks/s. A failure, a retry and a workflow node are written per task. A worker of 1000 seats whose tasks all fail, 3.5 ms from Postgres, gives 1,010 tasks/s with 20 connections (all busy) and 500 with 5.

Set `PoolSize(connections=N)` when the database is far away or many tasks fail, retry, or run as workflow nodes. Each of these holds a connection for more round trips than a plain success. The connections in use are about:

```text
connections = outcomes written per second × time that one outcome holds a connection
time that one outcome holds a connection = its round trips × (RTT + 1 to 3 ms)
```

### Sizes Derived from the Seats

`PoolSizeFromSeats()` and `ExecutorThreadsFromSeats()` derive a size from seats. A child pool and the default executor use the seats of the child. The pool of the worker process uses the seats of the whole profile (`WorkerProfile.seat_count()`):

```text
size = min(cap, max(floor, ceil(seats / seats_per_unit)))
```

| Size | Seats | `seats_per_unit` | `floor` | `cap` |
|------|-------|------------------|---------|-------|
| Shared async pool `max_size` | of the child | 10 | 2 | 40 |
| Shared thread pool `max_size` | of the child | 10 | 2 | 26 |
| Shared async default executor threads | of the child | 1 | 2 | 32 |
| Pool of the worker process (`ParentPool.max_size`) | of the profile | 10 | 5 | 20 |

For example, a shared async child with 25 seats opens a pool of up to `ceil(25 / 10) = 3` connections and a default executor of 25 threads. A child with 500 seats stops at the caps: 40 connections and 32 threads. A shared thread child with 64 seats opens up to 7 connections; with 256 seats, the most, 26.

The worker process of a profile with 8 isolated children opens up to 5 connections; with 64 isolated children, 7; with one shared async child of 100 seats, 10; with 200 seats or more, 20. The floor of 5 is above the minimum of a `PoolSize` for this pool, 3; see [The Pool of the Worker Process](#the-pool-of-the-worker-process). All constants are in `horsies.core.models.child_sizing`.

A shared child writes 2 statements per task through its pool, the RUNNING update and the finalizing marker, each one round trip. On average its pool keeps this many connections busy:

```text
connections in use = 2 × tasks per second of the child × (RTT + server time + the delay of the child)
```

A seat whose task takes 0.1 s, with Postgres 3.5 ms away, keeps about one connection in 12 busy, so one connection per 10 seats leaves room for bursts. A shared async child with many seats is CPU-bound, and the delay of its event loop makes each write hold its connection longer; its cap is where its rate stops rising. Measured on one 8-core host (Apple M2), Postgres 17 in a 4-vCPU VM, tasks that sleep 0.1 s, one shared child or 14 isolated children per worker:

| Child | `max_size` | Tasks/s, RTT 0.25 ms | Tasks/s, 3.5 ms more |
|-------|------------|----------------------|----------------------|
| shared async, 100 seats | 5 | 915-929 | 570-594 |
| shared async, 100 seats | 10 (default) | 904-909 | 786-808 |
| shared async, 100 seats | 20 | 910-919 | 783-792 |
| shared async, 1000 seats | 10 | 1,228-1,347 | 970 |
| shared async, 1000 seats | 20 | 1,288-1,572 | 1,367-1,398 |
| shared async, 1000 seats | 40 (default) | 1,693-1,706 | 1,699-1,759 |
| shared async, 1000 seats | 80 | 1,697-1,730 | 1,715-1,722 |
| shared thread, 24 seats | 2 | 205-206 | 187-191 |
| shared thread, 24 seats | 6 | 204-205 | 181-186 |
| shared thread, 64 seats | 5 | 571-574 | 503-517 |
| shared thread, 64 seats | 10 | 570-572 | 517-520 |
| shared thread, 64 seats | 20 | 574-578 | 516 |
| isolated, 14 children | 1 (default) | 117-119 | 106 |
| isolated, 14 children | 2 | 116-118 | 106 |

The defaults follow the knees at 3.5 ms. A database on the same host or network needs fewer connections: at 0.25 ms, 5 connections give a 100-seat async child the rate of 20. There, set a smaller `PoolSize(connections=N)` to save server connections. A shared thread child of 24 or 64 seats derives 3 or 7 connections, between pool sizes of the same rate.

## The Core of the Worker Process

The worker process claims every task, hands it to a child, and writes every outcome. On a host with SMT, two threads of one physical core share its execution units, so a busy child on the other thread of the worker's core slows the worker process.

`parent_cpu=ParentCpu.OWN_CORE` (Linux) splits the CPUs that the worker may use (its CPU affinity, for example from `taskset` or a cgroup cpuset):

- The worker process gets the physical core of the lowest of those CPUs, both of its SMT threads. Every thread of the worker process runs there.
- The children get every other CPU of the set, recycled children too. The worker sets the CPUs of each child before it sends the child its first frame, so every thread of the child runs on them.
- With the preload parent (the Linux default), each child is forked off the worker's core. A child that starts with `exec` runs its interpreter start on the worker's core until the worker sets its CPUs.

The worker start fails on another platform, or when the CPUs of the worker hold fewer than 2 physical cores.

```python
from horsies import ParentCpu, SharedAsyncChildren, WorkerProfile

dedicated_host = WorkerProfile(
    shared_async=SharedAsyncChildren(count=6, seats=100),
    parent_cpu=ParentCpu.OWN_CORE,
)
```

Use it on a host or a CPU set that runs this worker alone, when the worker process is the limit: it runs at one CPU while the children have room. It gives one core to one process, so leave it at `SHARED` where several workers share a few cores.

The children lose the CPUs of that core. Where the children are the limit, for example tasks that keep a CPU busy, they run on the CPUs that are left. On 8 CPUs (4 physical cores with 2 SMT threads each) the children get 6: 8 isolated children that ran pure-Python work went from 52 to 39 tasks/s, and one shared thread child with 8 seats that ran C work that releases the GIL went from 71 to 56 tasks/s. Keep `SHARED` for those workers.

## Profiles for One Machine

The profiles below are for one worker on a machine with 16 vCPU and 64 GB of memory, one profile per kind of work. Each setting that a profile does not show is the default, so the pools are the derived sizes.

**Async I/O.** Tasks that wait on the network or a database through async clients. Each shared async child runs on one CPU at most; 4 children leave CPUs for the worker process and the rest of the machine.

```python
from horsies import IsolatedChildren, SharedAsyncChildren, SharedThreadChildren, WorkerProfile

async_io = WorkerProfile(
    shared_async=SharedAsyncChildren(count=4, seats=100),
)
```

**Blocking I/O on threads.** Tasks that wait in a sync client (`requests`, a sync driver). A waiting thread holds no CPU, so the seats follow the waits, not the CPUs.

```python
blocking_io = WorkerProfile(
    shared_thread=SharedThreadChildren(count=2, seats=64),
)
```

**CPU work in isolated children.** Pure-Python CPU work: one task per process, one process per CPU, 2 CPUs left for the worker process and the system.

```python
cpu_isolated = WorkerProfile(
    isolated=IsolatedChildren(count=14),
)
```

**CPU work in native code that releases the GIL.** `zlib`, `hashlib` and similar C code run in parallel on the threads of one process: one seat per CPU, 2 CPUs left.

```python
native_threads = WorkerProfile(
    shared_thread=SharedThreadChildren(count=1, seats=14),
)
```

**A mixed profile.** Each kind of task goes to the children of its execution mode.

```python
mixed = WorkerProfile(
    isolated=IsolatedChildren(count=4),
    shared_async=SharedAsyncChildren(count=2, seats=200),
    shared_thread=SharedThreadChildren(count=1, seats=32),
)
```

The connections that each profile opens, and the rate of the worker:

| Profile | Derived pools | Connections that `HRS-219` counts | Task code that sends adds, at most | Tasks/s of one worker |
|---------|---------------|-----------------------------------|------------------------------------|-----------------------|
| `async_io` | 4 shared async children × 10, worker process 20 | `20 + 1 + 1 + 4 × 10 = 62` | `4 × 2 = 8` | 2,490-2,630 |
| `blocking_io` | 2 shared thread children × 7, worker process 13 | `13 + 1 + 1 + 2 × 7 = 29` | `2 × 2 = 4` | 1,010-1,070 |
| `cpu_isolated` | 14 isolated children × 1, worker process 5 | `5 + 1 + 1 + 14 × 1 = 21` | `14 × 2 = 28` | 108-110 |
| `native_threads` | 1 shared thread child × 2, worker process 5 | `5 + 1 + 1 + 1 × 2 = 9` | `1 × 2 = 2` | 114-115 |
| `mixed` | 4 isolated children × 1, 2 shared async children × 20, 1 shared thread child × 4, worker process 20 | `20 + 1 + 1 + 4 × 1 + 2 × 20 + 1 × 4 = 70` | `7 × 2 = 14` | 2,020-2,500: async 1,810-2,270, thread 184-205, isolated 24-27 |

The rates were measured on an 8-core host (Apple M2) with Postgres 18 on the same host, two runs per profile, with a task that sleeps 0.1 s in place of the work: `asyncio.sleep` for the async tasks, `time.sleep` for the others, which releases the GIL as blocking I/O and native code do. They show what the worker itself carries, on a host with half the CPUs of the profiles. Real work sets its own rate; for CPU work it is about the children, or the seats, divided by the CPU seconds of one task. With Postgres 3.5 ms away, see the per-child rates in [Sizes Derived from the Seats](#sizes-derived-from-the-seats).

A pool opens a connection only when a write finds none free, so a worker can open fewer connections than `HRS-219` counts: in these runs Postgres saw at most 48 connections for `async_io` (62 counted) and 21 for `blocking_io` (29 counted), the measuring script's own connection included. To save server connections when Postgres is near, set a smaller `PoolSize(connections=N)` on the pool of a kind.

## API Reference

### `WorkerProfile`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `isolated` | `IsolatedChildren \| NoIsolatedChildren` | `NoIsolatedChildren()` | The isolated children |
| `shared_async` | `SharedAsyncChildren \| NoSharedAsyncChildren` | `NoSharedAsyncChildren()` | The shared async children |
| `shared_thread` | `SharedThreadChildren \| NoSharedThreadChildren` | `NoSharedThreadChildren()` | The shared thread children. A profile needs at least one child of any mode |
| `pool` | `ParentPool` | `ParentPool()` | The connection pool of the worker process. See [The Pool of the Worker Process](#the-pool-of-the-worker-process) |
| `claim` | `ClaimLimits` | `ClaimLimits()` | Limits on the tasks that the worker claims |
| `drain_timeout` | `timedelta` | 300 seconds | How long a stopping worker waits for running tasks. Above 0, at most 24 hours. See [Graceful Shutdown](/horsies/workers/worker-architecture/#graceful-shutdown) |
| `preload` | `ChildPreload` | `ChildPreload.LIBRARY` | What a Linux worker imports once before it forks children. See [Preload Parent](/horsies/workers/worker-architecture/#preload-parent) |
| `parent_cpu` | `ParentCpu` | `ParentCpu.SHARED` | Where the worker process runs. See [The Core of the Worker Process](#the-core-of-the-worker-process) |

`WorkerProfile.child_count()` returns the number of children of the profile. `WorkerProfile.seat_count()` returns the tasks that they run at once: one per isolated child, and `seats` per shared child. `WorkerProfile.resolved_parent_pool()` returns the most connections of the pool of the worker process.

### `IsolatedChildren`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `count` | `int` | required | Number of isolated children, 1 to 1024 |
| `recycle` | `IsolatedRecycle` | `IsolatedRecycle()` | When a child is replaced |
| `pool` | `IsolatedPool` | `IsolatedPool()` | The connection pool of each child |
| `task_send_pool` | `PoolSize` | `PoolSize(connections=2)` | The pool that task code opens in each child when it sends. See [The Task-Send Pool](#the-task-send-pool) |

`NoIsolatedChildren()` states that the profile starts no isolated children.

### `IsolatedRecycle`

The worker replaces a child with a new one when either limit is reached. Each child has its own counters, so children are replaced at different times. See [Process Recycling](/horsies/workers/concurrency/#process-recycling).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `after` | `TaskLimit \| NoTaskLimit` | `TaskLimit(tasks=100)` | Replace a child after it completes this many tasks |
| `above` | `MemoryLimit \| NoMemoryLimit` | `NoMemoryLimit()` | Replace a child when its own RSS reaches this many MB |

### `SharedAsyncChildren`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `count` | `int` | required | Number of shared async children, 1 to 1024 |
| `seats` | `int` | required | Tasks that one child runs at once, 1 to 1000. A seat that waits on the network holds a socket, and a process often has a limit of 1024 open files |
| `recycle` | `SharedRecycle` | `SharedRecycle()` | When a child is replaced |
| `pool` | `SharedPool` | `SharedPool()` | The async connection pool of each child, shared by its seats |
| `executor` | `ExecutorThreads \| ExecutorThreadsFromSeats` | `ExecutorThreadsFromSeats()` | The threads of the default executor of the event loop of each child |
| `task_send_pool` | `PoolSize` | `PoolSize(connections=2)` | The pool that task code opens in each child when it sends, shared by its seats |

`NoSharedAsyncChildren()` states that the profile starts no shared async children.

### `SharedThreadChildren`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `count` | `int` | required | Number of shared thread children, 1 to 1024 |
| `seats` | `int` | required | Tasks that one child runs at once, 1 to 256. Each seat is an OS thread with a stack of its own |
| `recycle` | `SharedRecycle` | `SharedRecycle()` | When a child is replaced |
| `pool` | `SharedPool` | `SharedPool()` | The sync connection pool of each child, shared by its seat threads |
| `task_send_pool` | `PoolSize` | `PoolSize(connections=2)` | The pool that task code opens in each child when it sends, shared by its seat threads |

`NoSharedThreadChildren()` states that the profile starts no shared thread children.

### `SharedRecycle`

A shared child that reaches a limit closes: it takes no new task, its running tasks finish, then it stops and a new child starts. See [Recycling](/horsies/workers/shared-async-tasks/#recycling) and [Recycling](/horsies/workers/shared-thread-tasks/#recycling).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `after_per_seat` | `TaskLimit \| NoTaskLimit` | `TaskLimit(tasks=100)` | Replace a child after it completes `after_per_seat.tasks × seats` tasks |
| `above` | `MemoryLimit \| NoMemoryLimit` | `NoMemoryLimit()` | Replace a child when its own RSS, read after each task, reaches this many MB |

### `IsolatedPool`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min_size` | `int` | `0` | Connections that the pool keeps open, 0 to 1000. With 0, a connection opens at its first use |
| `max_size` | `PoolSize` | `PoolSize(connections=1)` | The most connections of the pool. An isolated child runs one task, and its two writes run one after the other, so it never uses a second connection |
| `check` | `bool` | `False` | Check each connection when a write takes it from the pool: one database round trip per checkout. Each connection is in autocommit, and a write of the child is one statement, so the check doubles its round trips. Off, a dead connection fails the write that takes it. See [Remote PostgreSQL](/horsies/configuration/remote-postgres/#health-checks-at-high-rtt) |

### `SharedPool`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min_size` | `int` | `0` | Connections that the pool keeps open, 0 to 1000. With 0, a connection opens at its first use |
| `max_size` | `PoolSize \| PoolSizeFromSeats` | `PoolSizeFromSeats()` | The most connections of the pool. See [Sizes Derived from the Seats](#sizes-derived-from-the-seats) |
| `check` | `bool` | `False` | Check each connection when a write takes it from the pool, as for `IsolatedPool` |

Validation refuses a `min_size` above the resolved `max_size` of the pool.

### `ParentPool`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_size` | `PoolSize \| PoolSizeFromSeats` | `PoolSizeFromSeats()` | The most connections of the pool of the worker process. The pool has no overflow. See [Sizes Derived from the Seats](#sizes-derived-from-the-seats) |

Validation refuses a `PoolSize` below 3. See [The Pool of the Worker Process](#the-pool-of-the-worker-process).

### `ClaimLimits`

The capacity of a worker is its number of seats (`seat_count()`) plus `AppConfig.prefetch_buffer`. A worker never holds more tasks than its capacity, so validation refuses a limit above it. See [Claiming Controls](/horsies/workers/concurrency/#claiming-controls).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `per_queue_per_pass` | `TaskLimit \| NoTaskLimit` | `NoTaskLimit()` | The most tasks that one claim pass takes from one queue. With no limit, a pass fills the free capacity |
| `claimed_per_worker` | `TaskLimit \| NoTaskLimit` | `NoTaskLimit()` | The most CLAIMED tasks (claimed, not yet running) that the worker holds. With no limit, the capacity is the limit |

### Limits

| Type | Field | Bounds | Meaning |
|------|-------|--------|---------|
| `TaskLimit` | `tasks: int` | 1 to 2,147,483,647 | A limit of `tasks` tasks |
| `NoTaskLimit` | none | none | No limit on the number of tasks |
| `MemoryLimit` | `mb: int` | 1 to 16,777,216 | A limit of `mb` MB of resident memory (RSS) |
| `NoMemoryLimit` | none | none | No limit on the resident memory of a child |

### Sizes

| Type | Field | Bounds | Meaning |
|------|-------|--------|---------|
| `PoolSize` | `connections: int` | 1 to 1000 | A pool of at most `connections` connections. Each connection holds a socket, and a process often has a limit of 1024 open files |
| `PoolSizeFromSeats` | none | none | A pool size derived from the seats: of the child for a child pool, of the profile for `ParentPool` |
| `ExecutorThreads` | `threads: int` | 1 to 256 | A default executor of `threads` threads. Each thread has a stack of its own |
| `ExecutorThreadsFromSeats` | none | none | A default executor size derived from the seats of the child |

### `ChildPreload`

<!-- members: ChildPreload -->
| Member | Value | What a Linux worker imports before it forks children |
|--------|-------|-------------------------------------------------------|
| `LIBRARY` | `library` | Library code. Each child imports the app itself |
| `APP` | `app` | Library and app. Children share module state from the app's import |
| `NONE` | `none` | Nothing. Every child starts with `exec` |

Other platforms start every child with `exec`.

### `ParentCpu`

<!-- members: ParentCpu -->
| Member | Value | Where the worker process and its children run |
|--------|-------|-----------------------------------------------|
| `SHARED` | `SHARED` | On the CPUs that each process inherits. The default |
| `OWN_CORE` | `OWN_CORE` | The worker process on one physical core, both SMT threads; the children on the other CPUs. Linux only |

## Validation

Validation runs in two places. `horsies check` validates every profile with the facts that need no machine. A worker validates again at boot, and adds facts that only the machine and the server know.

| Check | `horsies check` | `horsies check --live` | Worker boot |
|-------|-----------------|------------------------|-------------|
| Bounds of each value (count, limits, pool sizes, executor threads, drain timeout) | error `HRS-210` | error `HRS-210` | error: the app config does not build |
| Profile name: 1 to 64 letters, digits, `_`, `.`, `-`; does not start with `.` or `-` | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| At least one child | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| Claim limit above the capacity (seats plus `prefetch_buffer`) | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| Pool `min_size` above the resolved `max_size` | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| A `PoolSize` below 3 for the pool of the worker process (`ParentPool.max_size`) | error `HRS-218` | error `HRS-218` | error: the app config does not build |
| `--profile` names a defined profile | not checked | not checked | error `HRS-206`, exit 1 |
| Postgres connections of one worker against the server | not checked | error `HRS-219` | warning |
| Sum of the per-child memory limits against the usable memory (container limit or host RAM) | not checked | not checked | warning |
| Memory limit against the measured child baseline RSS | not checked | not checked | refusal, exit 1 |

The app config is built when the worker imports the app module, so a worker boot validates every profile again, the chosen one included. A worker does not start from a config that has an invalid profile.

A value out of bounds fails where it is built, before the profile is in the dict, so its error (`HRS-210`, from the import of the app module) names the type and the field, for example `IsolatedChildren` `count`. An `HRS-218` error names the profile and the field path:

```text
error[HRS-218]: worker_profiles['batch'].claim.claimed_per_worker: 10 tasks is above the capacity of the worker (4)
   = note: capacity = 4 isolated children + prefetch_buffer 0
   = note: the worker never holds more tasks than its capacity, so this limit has no effect

   = help:
        set the limit to 4 or less, or use NoTaskLimit()
```

### Connections

One worker opens this many Postgres connections itself:

```text
pool max_size                                           # pool of the worker process
+ pool max_size                                         # session pool, when the session database URL differs from database_url
+ 1                                                     # listener
+ 1                                                     # runner heartbeat
+ 1                                                     # liveness lock
+ isolated children × isolated.pool max_size            # isolated child pools
+ shared async children × shared_async.pool max_size    # shared async child pools
+ shared thread children × shared_thread.pool max_size  # shared thread child pools
```

The worker process writes the runner heartbeats of its RUNNING tasks over one connection of its own. One listener connection carries every `LISTEN` of the worker process. One connection on the session URL holds the [liveness lock](/horsies/workers/heartbeats-recovery/#worker-liveness-lock) of the worker process. When the URL of a separate `session_database` differs from `database_url`, the worker process opens a session pool there for schema work and partition maintenance, of the size of its own pool. Each `max_size` is the resolved value: a `PoolSize`, or the size that `PoolSizeFromSeats()` derives from the seats. An isolated child and a shared thread child open a sync pool; a shared async child opens an async pool. At the defaults and with no separate `session_database`, a profile of 8 isolated children needs `5 + 1 + 1 + 1 + 8 × 1 = 16`. A profile of 2 isolated children, 2 shared async children with 50 seats and 1 shared thread child with 16 seats has 118 seats and needs `12 + 1 + 1 + 1 + 2 × 1 + 2 × 5 + 1 × 2 = 29`. Each `HRS-219` error and the boot log state the formula of the profile, one term per mode, with a `session pool max_size N` term after the parent pool when the session pool is separate:

```text
parent pool max_size 12 + listener 1 + runner heartbeat 1 + liveness lock 1 + 2 isolated children x isolated.pool max_size 1 + 2 shared async children x shared_async.pool max_size 5 + 1 shared thread children x shared_thread.pool max_size 2 = 29
```

Task code that sends through the app opens more: the `task_send_pool` of its child kind in each child (see [The Task-Send Pool](#the-task-send-pool)). This is a ceiling for the children whose tasks send:

```text
isolated children × isolated.task_send_pool
+ shared async children × shared_async.task_send_pool
+ shared thread children × shared_thread.task_send_pool
```

8 isolated children at the defaults add `8 × 2 = 16`, so `32` in all. The comparison below does not count these connections, because only the run time shows whether a task sends. Each `HRS-219` error and the boot log state them next to the compared number, term by term:

```text
not counted: task code that sends opens a pool of the task_send_pool of its child kind in its child, for each event loop that it sends from; with one such loop in each child, 8 isolated children x isolated.task_send_pool 2 = 16 more, 32 in all
```

The server has `max_connections` minus `superuser_reserved_connections` minus `reserved_connections` (PostgreSQL 16 and later) usable slots. `horsies check --live` reports `HRS-219` for each profile whose worker opens more than that number itself. At boot the worker reads the same settings before it builds its children and logs a warning. With `pgbouncer_transaction_mode=True` the pooled connections do not map to server connections, and nothing is compared; see [PgBouncer pool size](/horsies/troubleshooting/pgbouncer/#pool-size) for the PgBouncer side. See [Remote PostgreSQL](/horsies/configuration/remote-postgres/#connection-budget).

The comparison is for one worker. Several workers, producers and other clients share the same slots.

### Memory

At boot the worker compares the sum of `count × MemoryLimit.mb` over the modes with the memory that it can use, and logs a warning when the sum is above it. The worker starts in every case: a deployment can move between machine sizes. A profile with a mode that has children and `NoMemoryLimit()` has no sum to compare.

The memory limit of each mode is also compared with the measured baseline RSS of a booted child of that mode, and a limit at or below it refuses the start. A mode whose children were all down at the start is compared when its first child becomes ready; a limit at or below that baseline stops the worker, which exits non-zero.

The memory that the worker can use is the smaller of two values:

| Source | Read from | Named in the warning |
|--------|-----------|----------------------|
| Container limit | The group that `/proc/self/cgroup` names: its cgroup v1 memory group when it has one, else its cgroup v2 group. The limit is the smallest `memory.limit_in_bytes` (v1, under `/sys/fs/cgroup/memory`) or `memory.max` (v2, under `/sys/fs/cgroup`) of the group and each parent group. Without `/proc/self/cgroup`: `/sys/fs/cgroup/memory.max`, else `/sys/fs/cgroup/memory/memory.limit_in_bytes` | `container limit` |
| Host RAM | `psutil.virtual_memory().total` | `host RAM` |

- A cgroup value of `max`, or no cgroup file, means no container limit.
- A cgroup limit at or above the host RAM, such as the cgroup v1 value for no limit, leaves the host RAM.
- A cgroup file that exists but cannot be read or parsed gives a separate warning, and the host RAM is used.

```text
Worker profile 'default': the per-child memory limits add up to 2048 MB, above the 1024 MB container limit. Children can use more memory than the worker has before they are recycled.
```

## Things to Avoid

**Don't set a claim limit above the capacity.** It has no effect, and validation refuses it.

```python
# Wrong: 2 children and prefetch_buffer 0 hold at most 2 tasks
claim_too_high = WorkerProfile(
    isolated=IsolatedChildren(count=2),
    claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=10)),
)

# Correct
claim_at_capacity = WorkerProfile(
    isolated=IsolatedChildren(count=2),
    claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=2)),
)
```

**Don't size the memory limits for one machine and ignore the boot warning on another.** The warning names the sum, the usable memory of the worker that started, and its source.

**Don't set `PSYCOPG_IMPL` in a `.env` file that the app loads.** `ChildPreload.LIBRARY` and `ChildPreload.APP` import psycopg, SQLAlchemy and pydantic before the app runs, so `load_dotenv()` in the app is too late. See [Things to Avoid](/horsies/workers/worker-architecture/#things-to-avoid) of Worker Architecture.

```bash
# Wrong: in a .env file that the app loads
PSYCOPG_IMPL=binary

# Correct: in the environment of the worker process
PSYCOPG_IMPL=binary horsies worker myapp.instance:app --profile default
```

Monkeypatching has the same limit: a patch that must run before those imports, such as gevent, is too late. Use `preload=ChildPreload.NONE`.

## Migrating from Worker Flags

The worker flags `--processes`, `--max-claim-batch`, `--max-claim-per-worker`, `--max-tasks-per-child`, `--max-memory-per-child-mb`, `--drain-timeout` and `--child-preload` are removed. Each one exits with a message that names the profile field that replaces it. The `PostgresConfig` fields `worker_child_pool_min_size`, `worker_child_pool_max_size` and `worker_child_pool_check` are removed: the `pool` of each child kind replaces them. `worker_pool_size` and `worker_max_overflow` are removed: `WorkerProfile.pool` replaces them for the worker process, and the `task_send_pool` of each child kind for task code that sends. A config that still sets one of these fields fails with `HRS-213`. See [Migrating to 0.7.0](/horsies/migrations/migration-to-0-7-0/) for the tables.
