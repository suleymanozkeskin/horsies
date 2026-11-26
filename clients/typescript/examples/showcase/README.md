# Acme Clothing showcase (TypeScript)

The Acme Clothing application of the Rust showcase (`clients/rust/examples/showcase/`) on the TypeScript client of
horsies. It runs on Node 26 and on Bun 1.4. It shows typed contracts and their implementations, a worker profile with
isolated and shared async children, workflows with a sub-workflow, a quorum join and a pause, a table of typed
schedules, deterministic business faults, an operator cancel, a rerun in the app's own transaction, and the monitoring
reads and web page.

`src/main.ts` is one entry for every process: the scenarios, the operator commands, and, through `cli.run(app)`, the
worker, each worker child, the scheduler, `check`, `web` and `metrics`. The worker starts each child with the same
entry.

The package is private (`"private": true`): it is never published.

## Run it in this workspace

The workspace does not build the `horsies` package. Each process runs with the dev start module
`examples/dev/source-package.ts`: it gives the package names their source entries and loads the dev build of the addon.
An app that installs `horsies` from npm runs `node src/main.ts ...` with no start module.

From `clients/typescript`, after `bun run --cwd bridge build:dev`:

```text
export HORSIES_NATIVE_ADDON_PATH="$PWD/bridge/horsies.darwin-arm64.node"
export ACME_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/acme_demo_ts
cd examples/showcase
alias acme='node --import "$PWD/../dev/source-package.ts" src/main.ts'   # Bun: bun --preload ... src/main.ts
acme seed
acme worker                 # in another terminal; SIGTERM or SIGINT stops it after the drain
acme scheduler              # in another terminal: the schedule table
acme steady --orders 10 --cover-errors
acme report
acme web --authorization none        # the monitoring page at http://127.0.0.1:8600
```

`scripts/run.sh <node|bun>` runs `seed`, a worker and a scheduler, every scenario, the stop of the scheduler, the wait
until no task is live, and `report`, under the watchdog (2 GiB, 900 s).

### The database

One database holds the horsies tables and the Acme tables. It is of the TypeScript client family: the first process
that uses it installs the horsies schema as that family, so a database of the Python or the Rust showcase does not
serve this app. The settings rules, in order:

1. `ACME_DATABASE_URL`, as it is.
2. `DATABASE_URL`: the same server, with the database name `acme_demo_ts`.

There is no default URL. `seed` creates the database through the server's `postgres` database when it does not exist.

## Surface

36 tasks, 12 workflows, 4 queues, and the worker profile `acme`: 6 isolated children for the 16 tasks that use the
Acme tables or that can crash their process, and 1 shared async child of 32 seats for the other 20 (each with a
`timeoutMs`).

| Queue | Priority | Running-task cap |
| --- | ---: | ---: |
| payments | 1 | 4 |
| fulfillment | 10 | 8 |
| notifications | 50 | 6 |
| analytics | 90 | 4 |

Each task declares the codes that it returns (`errorCodes`). A contract imports no implementation: `src/contracts/` is
what a sender needs, `src/tasks/` is what a worker runs.

## Failure table

| Rate | Condition | Code | Result |
| ---: | --- | --- | --- |
| 20% | Payment provider unavailable | `PSP_UNAVAILABLE` | The authorization retries with a growing delay. |
| 8% | Card declined | `CARD_DECLINED` | The order fails with no retry and releases its reservations. |
| 5% | Stock is unavailable | `INSUFFICIENT_STOCK` | The order fails before the payment and releases its reservations. |
| 10% | Courier booking fails | `COURIER_UNAVAILABLE` | The booking retries. |
| 25% | Supplier feed times out | `SUPPLIER_TIMEOUT` | The feed retries; the restock needs 2 of 3 feeds (quorum join). |
| 4% | Bundle pricing reaches a zero divisor | `UNHANDLED_EXCEPTION` | A thrown error that no map names; the worker stays up. |
| 4% | Invalid size code | `DATA_CORRUPTION` | A thrown `SizeCodeError` that the implementation's exception map names. |
| 2% | Loyalty engine fault | `LOYALTY_ENGINE_BUG` | The body catches it and returns the code. |
| 50% of chaos exports | The export ends its child process | `WORKER_CRASHED` | The worker starts a new child; the retry completes the export. |
| 30% of returns | Item came back damaged | `DAMAGED_ITEM` | The returns workflow pauses; a resume writes the item off. |

Each rate is a draw over a domain id (`src/simulate.ts`), so the same id gives the same fault in every run.

## Schedules

`src/schedules.ts` holds the schedule table of the Rust showcase: 32 schedules, 29 enabled, for the supplier feeds,
the regional rollups, cache and search warm-ups, sales and retention work, reconciliation, marketing and catalog
maintenance. Each schedule names its task contract, so its input typechecks against the task's input; the patterns are
typed (`interval`, `hourly`, `daily`, `weekly`, `monthly`, `cron`), and `check` checks the table with the app.
`acme scheduler` runs it; `report` shows each schedule's stored state.

## Scenarios

```text
acme steady [--orders N] [--cover-errors] [--pace X]
acme rush
acme problem-child
acme bulk-import
acme flash-sale
acme chaos
acme maintenance
acme simulate
```

`steady` places orders at the tuned demand rate, each with its order workflow and its promotion and loyalty tasks, and
starts a return workflow for every sixth order and a restock workflow for every twentieth. `--cover-errors` first sends
one run of each row of the failure table. `rush` places 50 orders in 30 seconds. `problem-child` creates declined, short-stock and return cases. `bulk-import` starts the
40-chunk import workflow. `flash-sale` starts two campaigns and sends 80 price updates that expire after 45 seconds.
`chaos` sends the export drills that crash their child. `maintenance` starts the maintenance workflows.

## Operator commands

```text
acme report [--failed N]                     the monitoring and health reads
acme cancel <task-id> [--policy include-running|pending-or-claimed-only]
acme rerun <task-id> [--reason TEXT]         a rerun and its audit row in one transaction
```

`rerun` calls `app.rerunIn` with the `pg` adapter of `horsies/rerun/pg` in a transaction of the Acme store, and writes
the row of `acme_reruns` in the same transaction: both commit, or neither is written. A second rerun of the same task
in 24 hours is a replay of the first.

## Tests

```text
scripts/test.sh node      # each test file under the watchdog; Node runs one file at a time
scripts/test.sh bun
```

`ACME_DATABASE_URL` turns the database tests on; without it they pass with no check. `tests/worker.test.ts` starts a
worker of the entry and runs the order workflow, the promotion story, the crash and its retry, the cancel, the rerun
and the report through it. A test or the run starts each entry process with the start module that
`SHOWCASE_START_MODULE` names; the two scripts set it to the dev start module.

## Differences from the Rust showcase

- The restock workflow feeds `update_stock_levels` through three optional upstream parameters under a quorum join,
  because the workflow context does not reach a TypeScript body yet (#2460).
- `flaky_export` crashes its own child on a drawn first attempt, so it runs in an isolated child.
- The database name of rule 2 is `acme_demo_ts`, and there is no default URL.
