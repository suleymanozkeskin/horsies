# Acme Clothing showcase

The showcase is a runnable Acme Clothing application. It demonstrates tasks,
workflows, queues, schedules, deterministic business faults, and the Horsies
monitoring UI.

## Start the demo

Run PostgreSQL with `docker compose -f showcase/docker-compose.yml up -d`.
The compose service listens on port `5433`.

For that compose service, set
`ACME_DATABASE_URL=postgresql://postgres:postgres@localhost:5433/acme_demo`.

Set `ACME_DATABASE_URL` to a native SQLx URL. The accepted forms are
`postgresql://`, `postgres://`, and `postgresql+psycopg://`. The last form is
accepted for Python `.env` compatibility and is normalized before SQLx uses it.
The resolver checks these sources in order:

1. `ACME_DATABASE_URL` in the environment.
2. `ACME_DATABASE_URL` in the repository `.env` file.
3. `DATABASE_URL` in the environment, with the database name changed to
   `acme_demo`.
4. `DATABASE_URL` in `.env`, with the database name changed to `acme_demo`.
5. `postgresql://postgres:postgres@localhost:5432/acme_demo`.

Create the schema and the deterministic catalog:

```text
cargo run -p acme-showcase --bin acme -- seed
```

The command prints the selected resolution rule. Start the processes in
separate terminals:

```text
cargo run -p acme-showcase --bin acme -- worker
cargo run -p acme-showcase --bin acme -- scheduler
cargo run -p acme-showcase --features web --bin acme -- web --auth none
```

`--auth none` is valid only on a loopback host. Use the Horsies trusted-header
policy behind a reverse proxy for a non-loopback host. The proxy must replace,
not forward, the trusted header from a client.

Place bounded orders and stop cleanly:

```text
cargo run -p acme-showcase --bin acme -- steady --orders 10 --cover-errors
```

The monitoring UI is at `http://127.0.0.1:8600`. The web command uses the
read-only monitoring mode. It never runs schema migrations.

## Surface and counts

The Rust showcase registers 36 tasks, 12 workflows, 32 schedule entries, and
29 enabled schedule entries. It serves four queues:

| Queue | Priority | Limit |
| --- | ---: | ---: |
| payments | 1 | 4 |
| fulfillment | 10 | 8 |
| notifications | 50 | 6 |
| analytics | 90 | 4 |

Order fulfillment validates, reserves stock, authorizes and captures payment,
picks and packs, books a courier, prints a label, seeds tracking, and sends
the receipt. The workflow includes a courier retry path and a stock failure
path. Schedules cover supplier feeds, regional rollups, cache warming, sales,
retention, reconciliation, marketing, and catalog maintenance.

## Failure table

The following table is the stable error vocabulary used by the demo. Run
`steady --cover-errors`, wait for the worker, and inspect the task details in
the monitoring API or UI. Retryable codes remain visible in the task's
attempts even when the final task row completes.

| Rate | Condition | Code | Result |
| ---: | --- | --- | --- |
| 20% | Payment provider unavailable | `PSP_UNAVAILABLE` | Payment task retries. |
| 8% | Card declined | `CARD_DECLINED` | The order fails without a retry. |
| 5% | Stock is unavailable | `INSUFFICIENT_STOCK` | The order fails and releases reservations. |
| 10% | Courier booking fails | `COURIER_UNAVAILABLE` | The booking retries once. |
| 4% | Bundle pricing divides by zero | `UNHANDLED_ERROR` | The worker captures the panic as data. |
| 4% | Invalid size code | `DATA_CORRUPTION` | The showcase mapping returns a typed task error. |
| 2% | Loyalty engine panics | `LOYALTY_ENGINE_BUG` | The showcase catch helper returns a typed task error. |

The library captures task panics. The worker remains alive. The Rust showcase
does not add a global error mapper. Its size-code mapper and loyalty panic
helper are demo-owned functions.

The old manual-retry claim is not part of this application. Cancel a live
operation or use the history rerun API when a retained task is eligible.

## Scenarios

Each scenario uses the same deterministic hash over its domain identifiers.
Rates and work envelopes live in `showcase/src/tuning.rs`.

`steady` places orders at the tuned demand rate. It starts a return workflow
for every sixth order and a supplier restock workflow for every twentieth.

```text
cargo run -p acme-showcase --bin acme -- rush
cargo run -p acme-showcase --bin acme -- problem-child
cargo run -p acme-showcase --bin acme -- bulk-import
cargo run -p acme-showcase --bin acme -- flash-sale
cargo run -p acme-showcase --bin acme -- chaos
cargo run -p acme-showcase --bin acme -- maintenance
cargo run -p acme-showcase --bin acme -- simulate
```

`rush` submits 50 orders over the 30-second source window. `bulk-import`
starts the 40-chunk import workflow. `flash-sale` starts two campaigns and
sends 80 expiring price updates. `problem-child` creates declined, short-stock,
and return cases. `chaos` submits export recovery drills. `maintenance` starts
the five maintenance workflows.

## Development

The showcase is a workspace member with `publish = false`. Default builds do
not enable the web server. Enable it with `--features web`.

```text
cargo test -p acme-showcase --all-targets
cargo test -p acme-showcase --features web --all-targets
```

Database tests are opt-in through `ACME_DATABASE_URL`. Use a disposable
database and drop it after the run.

## Deployment inputs

The release-build recipe, environment contract, and systemd unit content are
in [deployment/README.md](deployment/README.md). The showcase is built from
the repository because its package is not published. The deployment owner
chooses the host, database, proxy, cadence, and rollback.
