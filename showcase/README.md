# Acme Clothing

A runnable demonstration of [horsies](../README.md): a fictional fast-fashion
retailer whose orders, payments, stock, and shipments are real rows in a real
PostgreSQL database. Only the outside world is simulated — the payment
provider, the courier API, the label printer, the mail gateway.

Nothing about the failures is scripted. An order fails to reserve stock because
the stock row says there is none. A second capture is refused by a unique
constraint. A promotions crash is a real `ZeroDivisionError` in real pricing
code, reached by real data. What the showcase demonstrates is what horsies does
with all of it.

## Run it

Everything runs from the repository root.

```bash
# once: create the demo database, tables, and catalog
uv run python -m showcase.acme.scenarios seed

# terminal 1
uv run horsies worker showcase.acme.app:app --processes 12

# terminal 2
uv run horsies scheduler showcase.acme.app:app

# terminal 3
uv run horsies web showcase.acme.app:app --enable-actions

# terminal 4 — places an order every 4-8 s until Ctrl-C
uv run python -m showcase.acme.scenarios steady
```

Then open <http://127.0.0.1:8600>.

Working from a clone, build the dashboard's assets once before terminal 3
(`horsies` installed from PyPI already ships them):

```bash
cd webui && bun install && bun run build
```

Validate the app without a database or a worker at any time:

```bash
uv run horsies check showcase.acme.app:app
```

## The database

Acme Clothing uses its own database, `acme_demo`, so a demo run never mixes rows
with a horsies development database. The `acme_*` tables and the horsies
tables share it.

`showcase/acme/settings.py` resolves the URL, first match wins:

1. `ACME_DATABASE_URL` in the environment;
2. `ACME_DATABASE_URL` in the repository `.env`;
3. `DATABASE_URL` in the environment, with the database name replaced by
   `acme_demo`;
4. `DATABASE_URL` in the repository `.env`, same replacement;
5. `postgresql+psycopg://postgres:postgres@localhost:5432/acme_demo`.

Rules 3 and 4 only ever rewrite the database name — host, port, credentials,
and query parameters carry over. The `seed` scenario creates the database if it
does not exist, and prints which rule it resolved from.

## What steady mode produces

One order every 4-8 seconds. Each one starts an `order_fulfillment` workflow
and two standalone task sends. Every failure the demo can produce appears on
its own within a few minutes:

| Rate | What happens | Where to look |
|---|---|---|
| 20% of orders | payment provider unreachable for the first two attempts, then clears | `/?retried=true` — attempt cards with per-try errors |
| 8% | card declined; permanent, and a dashboard retry declines again | `/?error_code=CARD_DECLINED` |
| 5% | a discontinued SKU on the order, so the reservation genuinely fails | `/?error_code=INSUFFICIENT_STOCK` |
| 3% | invoice render stalls past its 8 s deadline | `/?error_code=TASK_TIMEOUT` |
| 10% | courier API refuses the first booking, retried inside the child workflow | the child run's `book_courier` node |
| 4% | bundle-pricing `ZeroDivisionError`, unmapped anywhere | `/?error_code=UNHANDLED_EXCEPTION` |
| 4% | missing size code raises `KeyError`, mapped globally | `/?error_code=DATA_CORRUPTION` |
| 2% of customers | loyalty tier table dereference, under the task's own code | `/?error_code=LOYALTY_ENGINE_BUG` |

Every one of those numbers lives in `showcase/acme/tuning.py`. Nothing else
in the showcase hard-codes a rate, an interval, or a sleep.

## Nothing raises into the void

This is the story the showcase is built around, and `apply_promotions` tells it
twice in one task.

The bundle-pricing division is a real bug: an order marked down to clearance
prices has bundled lines but no line above the bundle price floor, so the pot is
divided by zero. `ZeroDivisionError` has no mapper entry anywhere. horsies
intercepts it, and the crash arrives in the dashboard as a **data structure** —
open the task, read the exception type, the message, and the traceback inside
`TaskError`, categorized OPERATIONAL under `UNHANDLED_EXCEPTION`. The worker
does not die. The task does not vanish. Nothing is printed to a log you have to
go find.

The same task also raises `KeyError` when a line carries a size code the
promotions engine has no multiplier for. That one **is** mapped, globally in
`app.py`, so the identical interception reports the domain code
`DATA_CORRUPTION` instead.

And `compute_loyalty_points` sets `default_unhandled_error_code='LOYALTY_ENGINE_BUG'`,
so its own unmapped `AttributeError` — dereferencing a tier table row that was
authored as a bare string — surfaces under the team's vocabulary rather than the
generic code.

Three exceptions, three different codes, one interception path, zero silent
raises.

## Determinism

Every draw is a stable hash of a domain identifier, not a random number. The
same order id always draws the same faults, in any process, on any machine,
across restarts. That is what makes "retry this declined payment from the
dashboard" decline again, and it is why a demo can be re-run and reasoned about.

Order ids come from a Postgres sequence, so ids are never reused and each one
keeps its own outcome.

## The flagship workflow

`order_fulfillment` is built per order — the DAG's shape depends on how many
lines the order has:

```
validate_order
  |- reserve_stock[line 1..n]        (one node per line, parallel)
  \-> authorize_payment              (waits for every reservation)
         |- pick_pack
         |- generate_invoice         (timeout_ms = 8 s)
         \-> SubWorkflow: shipping(courier, express)
                \-> capture_payment  (args_from = authorize_payment)
                      \-> send_order_email  (allow_failed_deps=True)
```

`shipping` is a child workflow in its own right — its own run, its own graph,
its own node results — parameterized per order through `build_with`.

`send_order_email` is the recovery handler. It runs whether the capture
completed, failed, or was skipped, reads the upstream `TaskResult`, and picks
between the confirmation and the apology. It completes; the workflow still
fails, because `OnError.FAIL` accounts for the failure regardless of the
handler. A COMPLETED node underneath a FAILED one is the point.

## Idempotency, and why the ripple heals

A `timeout_ms` kill SIGKILLs the child process, which breaks the worker's
executor pool. Every task in flight on that worker is reported `WORKER_CRASHED`
and comes back through its own retry policy. That is honest behaviour, not a
defect, and the showcase leans into it: every task that writes checks for its
own earlier work first.

An authorization that already exists is reported, not charged again. A line
already marked reserved does not reserve twice. A capture whose row already
exists under the same authorization is reported as a replay — under a
*different* authorization it is refused with `PAYMENT_ALREADY_CAPTURED`, which
is the unique constraint on `(order_id, kind)` doing its job.

Watch `/workers` when it happens: a pool restart, a burst of `WORKER_CRASHED`
attempts, and every one of them recovering.

**The production lesson, stated plainly.** A pool break takes innocent
siblings. One task exceeding its `timeout_ms` is enough to kill every task that
happened to be running on that worker at that moment — they did nothing wrong,
and they are reported `WORKER_CRASHED` regardless. Two things follow, and they
are not optional in a real deployment:

- **Tasks must be replay-safe.** A task that can be killed mid-write and re-run
  must check for its own earlier work before doing it again. Every writing task
  in this showcase does, and the `replayed` field on its result says whether
  this run did the work or found it already done.
- **Retry the crash, not the cause.** `WORKER_CRASHED` belongs in
  `auto_retry_for`; the error that caused the kill does not. `generate_invoice`
  omits `TASK_TIMEOUT` on purpose — its stall is a stable property of the order,
  so a retry would stall again, break the pool again, and take another set of
  innocent siblings with it.

In a 10-minute steady run, two invoice timeouts broke the pool twice and
produced 13 `WORKER_CRASHED` attempts. All 13 recovered; the two timed-out
invoices stayed failed, which is correct.

## Infrastructure failures are typed too

`store.py` contains every psycopg exception at the one call that performs I/O
and converts it into a `StoreError` carrying the operation name. Tasks turn that
into `TaskError(error_code='STORE_UNAVAILABLE')` with the failing operation in
`data`.

This was demonstrated by accident. During development the host volume filled
while a run was in flight, and the failure arrived in the dashboard as:

```
STORE_UNAVAILABLE
count_courier_attempt failed: could not extend file "base/…": No space left on device
HINT:  Check free disk space.
data: {"operation": "count_courier_attempt"}
```

A disk filling up is not a domain event, and nothing in the showcase anticipates
it. It still arrived as a typed error naming the operation that failed, in a
task you can open and read — not a psycopg traceback in a worker log, and not a
dead task with no explanation. That is the same interception path the promotions
crashes take, and it is the reason to route fallible I/O through one containment
point rather than letting exceptions escape from wherever they happen.

## Scenarios

Run from the repository root as `uv run python -m showcase.acme.scenarios <name>`.
Every one ends by printing the dashboard links for what it just started.

| Scenario | What it does | Runs for |
|---|---|---|
| `seed` | Creates the database, the `acme_*` tables, and 60 products | one-shot |
| `steady` | An order every 4-8 s; a return every 6th order, a restock every 20th | until Ctrl-C |
| `rush` | 50 orders in 30 s — the per-queue caps start to bite | ~30 s to place |
| `problem-child` | 8 orders that will be declined, one against a discontinued SKU, and 10 returns | one-shot |
| `bulk-import` | Starts a 40-chunk import built to be cancelled | until cancelled |
| `flash-sale` | Two campaigns, one that succeeds and one engineered to miss, plus 80 expiring price updates | ~2 min |
| `chaos` | Exports that kill their own child process, plus the `kill -9` drill | ~2 min |
| `maintenance` | Starts the five back-office workflows once each | one-shot |

`steady` is the one to leave running. The rest are things to do while it does.

## What each horsies capability looks like here

The showcase exists to make this table true. Everything in the right-hand column
is a thing to look at in the dashboard, not a claim to take on trust.

| Capability | Where it lives | What you see |
|---|---|---|
| `@app.task` + `TaskResult` both branches | all 35 tasks | tasks across all 7 statuses |
| Custom queues, priority, `max_concurrency` | 4 queues; `marketing_blast` floods `notifications` (cap 3) | a deep backlog on one queue while `payments` stays clear |
| `RetryPolicy.exponential` + attempt history | `authorize_payment` | attempt cards, per-try errors, `?retried=true` |
| `RetryPolicy.fixed` | `sync_supplier_feed` on `SUPPLIER_TIMEOUT` | a scheduled task that sometimes retries |
| `exception_mapper`, per-task | `sync_supplier_feed` raises `TimeoutError` | reported as `SUPPLIER_TIMEOUT` |
| `exception_mapper`, global | `apply_promotions` raises `KeyError` | reported as `DATA_CORRUPTION` |
| **Unhandled exception, unmapped** | `apply_promotions` bundle-pricing `ZeroDivisionError` | `UNHANDLED_EXCEPTION` with the exception as data |
| `default_unhandled_error_code` | `compute_loyalty_points` `AttributeError` | `LOYALTY_ENGINE_BUG` — the team's own code |
| `timeout_ms` | `generate_invoice`, 8 s | `TASK_TIMEOUT`, pool restart, siblings recovering |
| `good_until` / EXPIRED | `flash-sale` sends 80 price updates with a 45 s deadline onto a 2-slot queue | ~48 of them EXPIRED, which is not FAILED |
| Domain error codes | `CARD_DECLINED`, `INSUFFICIENT_STOCK`, `DAMAGED_ITEM`, and the rest | the error-code facet |
| Class-based `WorkflowDefinition` + `build_with` | `order_fulfillment`, per order | a DAG whose shape follows the order's lines |
| `SubWorkflowNode` | `shipping`, parameterized per order | a child run with its own graph |
| `args_from` | `capture_payment` takes the authorization | node detail shows what was injected |
| `from_node` | `daily_report`, functional style | the same wiring, one call shorter |
| `allow_failed_deps` | `send_order_email`, `restock_or_writeoff` | a COMPLETED node under a FAILED one |
| `join='quorum'` + `WorkflowContext` | `restock`: three feeds, two are enough | a run that survives a failed branch |
| `join='any'` | `flash_sale`: `warm_cache_edge` | fires on the first publish to land |
| `SuccessPolicy` / `SuccessCase` | `flash_sale` (either target) and `restock` (the aggregate) | COMPLETED despite a failed node |
| `OnError.PAUSE` + resume | `returns_review`, `fraud_review` | runs that stop and wait for a person |
| Cancel + draining | `catalog_import`, 40 chunks | PENDING go SKIPPED, RUNNING drain |
| Cancel / retry a task | `marketing_blast` backlog; declined payments | both actions, with attempt history kept |
| `IntervalSchedule` | supplier feeds, cache warms | live next-run countdowns |
| `HourlySchedule` | cart sweep, regional rollups | — |
| `DailySchedule` | rollups, reconciliation, campaigns | — |
| `WeeklySchedule` | supplier audit, weekend prep | — |
| `MonthlySchedule` | month-end close, catalog audit | — |
| `CronSchedule`, typed | every 4 h at :15; Friday the 13th via `BothDays` | the case a cron string cannot express |
| Disabled schedules | 3 of the 31 | what a disabled schedule looks like |
| `WorkflowMeta` | `pick_pack` | workflow id and node index in the result |
| Crash recovery | `chaos`: `flaky_export` calls `os._exit(1)` | `WORKER_CRASHED` attempts, then recovery |
| Worker health | 12 executor children, 10 s snapshots | CPU and memory history |
| `resend_on_transient_err` | `AppConfig` | infra retry, distinct from `RetryPolicy` |

## Layout

```
showcase/acme/
  app.py         queues, recovery tuning, 31 schedules, the global exception mapper
  settings.py    database URL resolution
  tuning.py      every rate and duration
  domain.py      error codes, entities, task payloads
  store.py       the acme_* tables and typed helpers
  simulate.py    stable-hash draws and simulated work
  tasks/         payments, inventory, orders, promotions, shipping, returns,
                 notify, analytics
  workflows/     order_fulfillment, shipping, returns_review, restock,
                 flash_sale, catalog_import, daily_report, price_sync,
                 customer_winback, warehouse_transfer, seasonal_markdown,
                 fraud_review
  scenarios/     seed, steady, rush, problem-child, bulk-import, flash-sale,
                 chaos, maintenance
```

## Running it as a group

`showcase/Procfile` starts the worker, scheduler, and web UI together under any
Procfile runner. The scenarios stay manual — they are the part you drive.

If there is no Postgres to hand, `showcase/docker-compose.yml` brings one up on
port 5433, away from anything already listening on 5432.

## The two workflows that stop and wait

`returns_review` and `fraud_review` both declare `OnError.PAUSE`, for different
reasons, and reading them side by side is the point.

A damaged return is a judgement call: the item is in someone's hands, and
whether it goes back on the shelf or is written off is not a decision code
should make. `inspect_item` reports `DAMAGED_ITEM`, the run goes PAUSED, and
nothing downstream starts. Resume and `restock_or_writeoff` executes on the
failed branch — it sets `allow_failed_deps=True`, so it receives the inspection
error and writes the item off. Cancel instead and the return simply stays open,
which is also a legitimate answer.

`fraud_review` pauses in front of a refund. Nothing is reversible after money
moves, so if reconciliation cannot be completed the run stops rather than
guessing. That is the difference between PAUSE and FAIL: a failed run is over,
while a paused run is still holding its place in the graph with everything
upstream intact, one instruction away from continuing.

`problem-child` creates a pile of both. It tells you in advance which returns
will pause, because the draw is a hash of the return id.

## Deliberately not used

- `cluster_wide_cap` — mutually exclusive with a non-zero `prefetch_buffer`,
  and the showcase demonstrates exact per-queue caps instead.
- `on_child_process_start` — there is no per-child engine to rebind here.
- `catch_up_missed=True` — restarting a demo would replay a backlog of noise.

## Configuration on show

Four queues, so priority claiming and per-queue caps are visible in the workers
view:

| Queue | Priority | Max concurrency |
|---|---|---|
| `payments` | 1 | 4 |
| `fulfillment` | 10 | 8 |
| `notifications` | 50 | 3 |
| `analytics` | 90 | 2 |

`--processes 12` gives about 1.6x headroom over steady mode's demand, and sits
just under the sum of the caps, so a burst still drives them into a visible
backlog while steady stays smooth.

Recovery is demo-tuned: worker snapshots every 10 s (so the CPU and memory
charts move), the reaper polling at the same cadence, and terminal rows kept for
24 hours.

`resend_on_transient_err=True` retries transient enqueue failures on the send
and start path. That is infrastructure retry, and it is a different thing from
`RetryPolicy`, which retries task *execution*.
