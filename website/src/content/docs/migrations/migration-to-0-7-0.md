---
title: Migrating to 0.7.0
summary: What to change to upgrade from 0.6.x to 0.7.0 - worker settings move from command-line flags into named worker profiles in the app config.
related: [../changelog, ../workers/worker-profiles, ../cli, migration-to-0-6-0]
tags: [migration, 0.7.0, breaking-changes, worker, profiles, cli]
---

## Stop 0.6.x workers before the first 0.7.0 worker starts

The first 0.7.0 process that starts migrates the schema to v55. v55 replaces
the claim function with one that takes a budget per execution class, and drops
the earlier one. A 0.6.x worker that still runs then fails each claim pass and
exits. Stop every 0.6.x worker first, then start the 0.7.0 workers.

## Worker settings move into profiles

`horsies worker` no longer takes execution flags. The settings live in named worker profiles in `AppConfig.worker_profiles`, and a worker starts with `--profile <name>`. `--loglevel` and the app locator stay.

1. Add one profile for each set of flags that your deployment starts workers with.
2. Replace the flags on each `horsies worker` command with `--profile <name>`.
3. Run `horsies check <app>`, and `horsies check <app> --live` against the target database.

`--profile` is required. There is no default profile. A removed flag exits with code 2 and a message that names the field that replaces it:

```text
horsies worker: error: --processes was removed in 0.7.0: worker settings live in a worker profile. Set WorkerProfile(isolated=IsolatedChildren(count=N)) in AppConfig.worker_profiles, and start the worker with --profile <name>.
```

## Flag to profile field

| 0.6.x flag | Default | 0.7.0 profile field |
|------------|---------|---------------------|
| `--processes N` | `1` | `isolated=IsolatedChildren(count=N)` (required, no default) |
| `--max-tasks-per-child N` | `100` | `isolated=IsolatedChildren(count=..., recycle=IsolatedRecycle(after=TaskLimit(tasks=N)))` |
| `--max-tasks-per-child 0` | | `recycle=IsolatedRecycle(after=NoTaskLimit())` |
| `--max-memory-per-child-mb N` | off | `recycle=IsolatedRecycle(above=MemoryLimit(mb=N))` |
| no `--max-memory-per-child-mb` | | `recycle=IsolatedRecycle(above=NoMemoryLimit())` (the default) |
| `--max-claim-batch N` | `0` | `claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=N))` |
| `--max-claim-batch 0` | | `claim=ClaimLimits(per_queue_per_pass=NoTaskLimit())` (the default) |
| `--max-claim-per-worker N` | `0` | `claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=N))` |
| `--max-claim-per-worker 0` | | `claim=ClaimLimits(claimed_per_worker=NoTaskLimit())` (the default) |
| `--drain-timeout SECONDS` | `300` | `drain_timeout=timedelta(seconds=SECONDS)` |
| `--child-preload library\|app\|none` | `library` | `preload=ChildPreload.LIBRARY`, `ChildPreload.APP` or `ChildPreload.NONE` |

The defaults of the profile fields are the defaults of the old flags, except the number of children: `IsolatedChildren.count` has no default.

## Example

A 0.6.x command:

```bash
horsies worker myapp.instance:app --processes=8 --max-tasks-per-child=500 \
    --max-memory-per-child-mb=1024 --drain-timeout=120 --child-preload=app
```

The 0.7.0 profile and command:

```python
from datetime import timedelta

from pydantic import SecretStr

from horsies import (
    AppConfig,
    ChildPreload,
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
        database_url=SecretStr('postgresql+psycopg://user:pass@localhost:5432/mydb'),
    ),
    worker_profiles={
        'default': WorkerProfile(
            isolated=IsolatedChildren(
                count=8,
                recycle=IsolatedRecycle(
                    after=TaskLimit(tasks=500),
                    above=MemoryLimit(mb=1024),
                ),
            ),
            drain_timeout=timedelta(seconds=120),
            preload=ChildPreload.APP,
        ),
    },
))
```

```bash
horsies worker myapp.instance:app --profile default
```

## Shared async children

A profile can also start shared async children: `shared_async=SharedAsyncChildren(count=N, seats=K)`. Each one runs up to K tasks of `Execution.SHARED_ASYNC` at once, as coroutines. It is new in 0.7.0 and has no flag before it; a profile without it runs every task in isolated children, as 0.6.x did. See [Shared Async Tasks](../workers/shared-async-tasks).

## New validation

These were not checked before 0.7.0, because the values were command-line flags:

- `horsies check` validates every profile. A claim limit above the capacity of the worker (its seats plus `prefetch_buffer`; one seat per isolated child) is refused with `HRS-218`; before, it had no effect.
- `horsies check --live` reports `HRS-219` when one worker of a profile can open more Postgres connections than the server has usable slots.
- At boot the worker warns when the per-child memory limits add up to more than the memory that the worker can use: the container (cgroup) limit when it is below the host RAM, else the host RAM, and when one worker can open more connections than the server has usable slots.

Each bound of the old flags still applies: at least one child, a task limit of at least 1, a memory limit of at least 1 MB, and a drain timeout above 0. New upper bounds: 1024 isolated children, 2,147,483,647 tasks for a task limit, 16,777,216 MB for a memory limit, and 24 hours for the drain timeout.

See [Worker Profiles](../workers/worker-profiles) for every field and for the validation table.
