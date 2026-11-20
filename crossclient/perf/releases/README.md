# Harness variants of released versions

A speed comparison of two releases runs the harness of each release on one
server in one session. The harness of a release can fail on the schema of the
same release, when a schema change of that release came after the last run of
its harness. Each folder here holds one patch for the `tests/perf` folder of
one released tag. The patch makes that harness run on the schema of that
release. The patch changes the harness only. It changes no product code.

The package of the harnesses is `tests` in `v0.7.0` and `crossclient` in the
current checkout. A path or a module name of the v0.7.0 checkout starts with
`tests` (`tests/perf/`, `tests.perf`); one of the current checkout starts with
`crossclient` (`crossclient/perf/`, `crossclient.perf`). The patch names its
files by their paths in `v0.7.0`.

| Folder | Applies to | Changes |
|---|---|---|
| `v0.7.0/harness.patch` | tag `v0.7.0` (schema 61) | See "The v0.7.0 patch" |

## The v0.7.0 patch

On schema 61 the harness of `v0.7.0` stops before its first measured
operation. The patch makes these changes:

- **Seed ids.** The seeds wrote text ids (`pf-<hash>-base-<n>`) into `uuid`
  columns, and the cleanup used `LIKE` on them. The patch uses the id form of
  the current harness: one UUIDv7 for each scenario, role, kind of row and
  index. The ids of one role and kind share their first 22 hex digits. The
  cleanup deletes live rows, history rows and workflow rows by an id range on
  the primary key.
- **Seed columns.** Schema 61 has five `NOT NULL` task columns without a
  default. The seeds write a 32-byte command fingerprint (version 1), the
  retention class `forever`, `retain_rerun_input` FALSE, and the rerun input
  disposition of a send that does not keep the input (`DECLINED_BY_POLICY`,
  or `NEVER_ELIGIBLE` for a workflow task). RUNNING rows also get a worker
  host, pid and process name, as in the current harness.
- **Ballast.** The check `horsies_tasks_live_status_only` refuses a terminal
  row in the live table. The ballast rows are seeded RUNNING and completed by
  the batch success statement of the product (`horsies_complete_tasks_fused`,
  at most 64 rows for each transaction). The rows then are in history.
- **One implementation for each scenario.** The older statements of
  `legacy_terminalization_sql.py` write a terminal status into the live
  table, which schema 61 refuses. Each scenario runs one implementation on
  both sides: a control run.
- **Worker statements.** `terminal-application-failure` and
  `workflow-success-phase2` send the statements that the v0.7.0 worker sends
  for their case (see "Scenario map"). The other scenarios run their former
  candidate side.
- **Schema install.** A terminal write needs the history leaves and the
  published history readers. `--apply-schema` runs the coverage pass of a
  worker startup after the install, with the default `RetentionConfig`.
- **Engine.** The engine of a run reads `uuid` columns as text, as the runtime
  engine of the product does. The 0.7.0 workflow engine handles ids as
  strings.
- **Selection.** `--scenario candidates` selects no scenario now. A selection
  of no scenario is refused.
- **Public call scenarios.** The patch adds `tests/perf/public_calls.py`, the
  same file as `crossclient/perf/public_calls.py` of the current harness with
  the package name `tests`, and the scenarios `send` and
  `workflow-start` (see "Scenario map"). The runner, the entry point and the
  report change as in the current harness to run them; the report names the
  payload "payload per operation".
- **Worker drain scenario.** The patch adds `tests/perf/worker_process.py`,
  `tests/perf/worker_drain.py` and `tests/perf/worker_drain_app.py`, the same
  files as in `crossclient/perf/` of the current harness with the package name
  `tests`, and the scenario `claim-finish` (see
  "Claim and finish"). The runner, the entry point, the report and the server
  counts change as in the current harness to run it.

The patch does not change the unit tests of the 0.7.0 harness
(`tests/unit/test_perf_run_exit.py`). Some of these tests need a scenario
with a candidate side, so they fail with the patch. A session does not run
them.

## Scenario map

The scenarios of the current harness call public operations of the library,
or drain sent tasks with a worker that the public CLI starts ("Claim and
finish"). Each one has a v0.7.0 scenario of the same name that runs the same
files. The calls of `public_calls.py` (`tests/perf/` in v0.7.0,
`crossclient/perf/` in the current checkout) are public API of both
releases with the same signature, so a pair measures the same call through
the product of each release.

| Scenario (both releases) | Public call | Rows that one call writes |
|---|---|---|
| `send` | `send(text=...)` of one task, with a 200-byte text argument | one PENDING task row |
| `workflow-start` | `WorkflowSpec.start()` of a one-node workflow; the node has a 200-byte text argument | the workflow row, its node row, the task row of the node |

- The client of a scenario connects with the `--dsn` of the run. Both sides
  of a run share one client; the first block of each side is a warmup.
- The library mints the ids of the rows. The run deletes the rows by the
  task name or the workflow name of the scenario (`perf.public.*`), before
  and after it measures.
- No plan evidence: the statements are the library's. The server counts are
  the statements that the library sent.

### Claim and finish

No public API of either release claims or finishes a task: the worker does
both. The scenario `claim-finish` measures them through a worker that the
public CLI starts, `horsies worker`, which takes the same arguments in both
releases. Both harnesses run the same three files: `worker_process.py`
starts, watches and stops the process, `worker_drain.py` holds the app, the
sends and the count, and `worker_drain_app.py` is the app module that the
worker loads.

| Scenario (both releases) | One operation (a drain) | Reported |
|---|---|---|
| `claim-finish` | `send()` of 10,000 no-op tasks (not timed), then `horsies worker <package>.perf.worker_drain_app:app --profile perf-drain --loglevel WARNING` (`<package>`: `tests` in v0.7.0, `crossclient` in the current checkout); the time from the start of the worker process until all 10,000 tasks are COMPLETED or FAILED | tasks per second over the drains of each side |

- The constants are in `worker_drain.py`: `DRAIN_TASK_COUNT` (10,000 tasks),
  `DRAIN_WORKER_PROFILE` (4 isolated children; a child is not replaced after
  a count of tasks), `DRAIN_TIMEOUT` (10 minutes), `DRAIN_COUNT_INTERVAL`
  (100 ms) and `WORKER_STOP_TIMEOUT` (60 seconds).
- The harness runs the `horsies` script of its own environment, with the
  root of its checkout as the working directory. The worker gets the `--dsn`
  of the run in `HORSIES_PERF_DRAIN_DSN`, so each release gets the URL form
  that it accepts. The worker writes its output to a log file in the
  temporary directory.
- The end of a drain is read by one SQL count, the same in both releases:
  the rows of `horsies_task_history` with the task name `perf.drain.noop`
  and the status COMPLETED or FAILED, once every 100 ms. The time ends at the
  start of the count that reads 10,000. It includes the start of the worker
  process: its imports, its startup checks and the start of its children.
- After the drain the worker gets SIGTERM and must exit 0. A drain that
  takes longer than the timeout, a worker that exits before the drain ends,
  and a worker that does not exit 0 stop the run; the message names the log
  of the worker.
- A run deletes the rows of the task, live and in history, before each
  drain and after the run. Each drain starts a new worker. The first drain
  of each side is a warmup. A smoke run makes 1 drain for each side, a gate
  run 10.
- The server counts are the statements of the worker from its start until
  the drain ends. The count of the harness is left out (its column alias
  `perf_harness_read`). The statements of a worker depend on its timing, so
  the two sides of a run are not held to equal counts.

### Statement scenarios of the v0.7.0 harness

The v0.7.0 harness keeps its statement scenarios. The current harness had the
same scenarios until 0.8.0 deleted the Python core implementation, whose
statements and decoders they measured; no current scenario pairs with them.
The table records the pairs that the harness measured before that deletion.
Each cell names what the scenario sent in one operation: the statements that
the product of that release sends for the same case.

| Branch scenario before the deletion | Branch statements | v0.7.0 scenario | v0.7.0 statements |
|---|---|---|---|
| `fused-completion-small-result` | `horsies_complete_task_fused` | `fused-completion-small-result` | `horsies_complete_task_fused` |
| `fused-completion-1mib-result` | `horsies_complete_task_fused` | `fused-completion-1mib-result` | `horsies_complete_task_fused` |
| `owned-claim-expiry` | `horsies_expire_owned_claim`, on the synchronous cursor of the child | `owned-claim-expiry` | `horsies_expire_owned_claim`, on the synchronous cursor of the child |
| `pending-expiry-batch` | `horsies_expire_pending_tasks` | `pending-expiry-batch` | `horsies_expire_pending_tasks` |
| `failed-attempt-terminal` | `horsies_finish_failed_attempt` (retry `NONE`, claimed-row rule `REFUSE`, wake `TRUE`) | `terminal-application-failure` | The retry-facts read (`READ_IN_FLIGHT_ATTEMPT_SQL`), then `horsies_finish_failed_attempt` with no retry |
| `stale-attempt-terminal` | `horsies_finish_stale_attempt` | `stale-terminal-failure` | The task advisory lock, the locking read, the attempt upsert, `horsies_fail_stale_task` |
| `workflow-success-phase2` | Phase 1: `horsies_finish_workflow_success`. Phase 2: `horsies_workflow_node_finish(task, TRUE, 'NONE')` | `workflow-success-phase2` | Phase 1: `horsies_finish_workflow_success`. Phase 2: `on_workflow_task_complete`, with the capacity wake in its node write |
| `workflow-success-phase2-dependent` | Phase 1: `horsies_finish_workflow_success`. Phase 2: `horsies_workflow_node_finish(task, TRUE, 'READ')`, which promotes the dependent node and mints its task row | none | none |
| none | none | `locked-completion` | `horsies_complete_locked_task` |

Where each release sends these statements:

- Current, failed attempt: one statement, with the retry facts of the claim,
  through the core (`crates/horsies-core/src/broker/attempt_writes.rs`).
- Current, phase 2: the form follows the step that phase 1 returns:
  `NO_DEPENDENTS` sends `'NONE'`, and `DEPENDENTS_WITHOUT_SUBWORKFLOW` sends
  `'READ'`. Each form is one statement.
- v0.7.0, failed attempt: `_persist_failed_attempt`
  (`horsies/core/worker/finalize.py:1467-1523` at `v0.7.0`) reads the retry
  facts (`_finalize_retry_request`, `:1525-1554`), then sends
  `horsies_finish_failed_attempt`.
- v0.7.0, workflow success: `_finish_workflow_success` (`:1322-1373`) sends
  phase 1. `_finalize_workflow_phase` (`:1619-1674`) runs phase 2 in one
  transaction, with the capacity wake in the node write.
- v0.7.0, stale recovery: `mark_stale_tasks_as_failed`
  (`horsies/core/brokers/postgres.py:3999-4196` at `v0.7.0`) sends the four
  statements in one transaction.
- `locked-completion`: no path of the v0.7.0 worker sends
  `horsies_complete_locked_task`. A locked completion goes to
  `horsies_finish_workflow_success` (`finalize.py:1285-1292` at `v0.7.0`), and a plain
  success to `horsies_complete_task_fused`. The scenario has no pair.
- `workflow-success-phase2-dependent`: the v0.7.0 harness seeds one-node
  workflows only. The scenario has no pair.

Each harness sends the statements of one operation in one transaction, and
a workflow success in one transaction for each phase. The workers of both
releases send a one-statement write, and the v0.7.0 retry-facts read, in
autocommit. Thus each measured operation of such a write has a `BEGIN` and a
`COMMIT` that the worker does not send, on both releases. Phase 2 of v0.7.0
is a transaction in the worker too.

## A session: both releases on one server

Use one PostgreSQL server for both releases. Use one database for each
release: the two schemas have the same table names.

1. Start the server with the settings of the measurement contract
   (`crossclient/fixtures/perf/compose.yaml`): `pg_stat_statements` preloaded with
   `track=all` and `track_utility=off`, `fsync`, `synchronous_commit`,
   `full_page_writes` and `autovacuum` off. Stop every other workload on the
   host.
2. Create the two databases, for example `horsies_v070` and `horsies_v080`.
3. Make the v0.7.0 checkout, from the root of the current checkout:

   ```sh
   git worktree add ../horsies-v0.7.0 v0.7.0
   git -C ../horsies-v0.7.0 apply "$PWD/crossclient/perf/releases/v0.7.0/harness.patch"
   (cd ../horsies-v0.7.0 && uv sync --frozen --dev)
   ```

   The current checkout runs in its own environment, with a built
   `horsies._core`. The scenario `claim-finish` runs the `horsies` script of
   each environment; `uv sync` installs it.

4. Install the two schemas. The two releases accept different URL schemes:
   v0.7.0 needs `postgresql+psycopg://`, the current release accepts
   `postgresql://` and `postgres://` only.

   ```sh
   (cd ../horsies-v0.7.0 && uv run python -m tests.perf --apply-schema \
       --dsn postgresql+psycopg://USER:PASSWORD@HOST:PORT/horsies_v070)
   uv run --project clients/python python -m crossclient.perf --apply-schema \
       --dsn postgresql://USER:PASSWORD@HOST:PORT/horsies_v080
   ```

5. Run each pair of the scenario map. Run the two releases one after the
   other, never at the same time. Run each pair three times, and change the
   release that runs first in each repetition. Every run is a control run:

   ```sh
   (cd ../horsies-v0.7.0 && uv run python -m tests.perf \
       --scenario send --scale gate \
       --demo-quiesced --control --write-summary \
       --dsn postgresql+psycopg://USER:PASSWORD@HOST:PORT/horsies_v070)
   uv run --project clients/python python -m crossclient.perf \
       --scenario send --scale gate \
       --demo-quiesced --control --write-summary \
       --dsn postgresql://USER:PASSWORD@HOST:PORT/horsies_v080
   ```

6. Compare the runs. The harness compares the two sides of one run only. In
   a control run both sides send the same statement. Each checkout writes its
   summaries to the `perf/results/` folder of its harness package
   (`tests/perf/results/` in v0.7.0, `crossclient/perf/results/` in the
   current checkout) and the samples to `raw/` in that folder. Compare the
   latency percentiles and the server
   counts of the two releases from these files.

Each run seeds 100,000 ballast rows through the terminal write of its own
release, and deletes its own rows when it ends.

## A session: the overhead of telemetry

The session measures what each telemetry level costs against no telemetry,
and checks that a build with telemetry off costs the same as the build
before the telemetry work. Run it on the benchmark server; numbers from a
laptop are not evidence.

### The levels and the cells

A run with `--telemetry LEVEL` runs the baseline side with no telemetry and
the candidate side with `LEVEL` (`crossclient/perf/telemetry_levels.py`).
`--telemetry off` (the default) is the control run: both sides with no
telemetry. Each level holds every part of the level before it:

| Level | The candidate side adds |
|---|---|
| `off` | nothing |
| `metrics` | `--metrics-listen 127.0.0.1:19464`: the worker records its metrics; nothing reads the endpoint during the drain |
| `scraped` | a read of `/metrics` and of `/livez` every 1 s during the drain: 15 times the common 15 s interval, so a drain of a few seconds sees several reads |
| `tracing` | `OtelTracing(worker_ended_spans=NO_SPANS)`: send spans and producer metrics in the harness, attempt spans in the task children. Each process has the SDK `TracerProvider` with a `BatchSpanProcessor` and an exporter that discards, and the SDK `MeterProvider` with a reader that nothing collects (`crossclient/perf/telemetry_apps.py`) |
| `handler` | one `def` handler of `AttemptEnded` that returns at once: the worker runs its handler child |
| `worker-ended-spans` | `worker_ended_spans=MAKE_SPANS`, with the same providers in the handler child |

| Scenario | Levels | One cell reports |
|---|---|---|
| `claim-finish` | all six | tasks per second; worker CPU per task (the worker parent, the processes below it, both); PostgreSQL CPU per task; worker stop time; and from one read of `/metrics` at the end of each drain: the series count, events dropped (queue full, handler child down), series dropped, handler events dropped, handler errors; the `/livez` answers of the reads during the drains |
| `send`, `workflow-start` | `off`, `tracing` | calls per second, p50 and p99 latency; producer CPU per call (the harness process); PostgreSQL CPU per call |

- A public call scenario runs no worker, so it takes `off` and `tracing`
  only; the entry point refuses the other levels for it.
- In the no-op drain the worker ends no attempt itself, so the
  `worker-ended-spans` cell measures the switch on the event path, not the
  cost of one span that the handler child makes.
- The worker CPU is read at the end of each drain, from the start of the
  worker: the start of the worker is in the throughput time too. A process
  below the worker that ended before the read is not counted; the drain
  replaces no child.
- With tracing, the candidate side also sends its tasks through an app with
  tracing, so each task row holds a trace carrier, as in a traced
  application. The sends are not timed.

### Before the session

1. Use the server and the PostgreSQL settings of "A session: both releases
   on one server" (step 1). Stop every other workload on the host. Port
   19464 must be free.
2. Find the `cpu.stat` file of the cgroup v2 group of the PostgreSQL server,
   for `--postgres-cpu-stat`. For the compose service on a host with the
   systemd cgroup driver:

   ```sh
   CONTAINER=$(docker compose -f crossclient/fixtures/perf/compose.yaml ps -q pg16)
   CPU_STAT=/sys/fs/cgroup/system.slice/docker-${CONTAINER}.scope/cpu.stat
   grep usage_usec "$CPU_STAT"
   ```

   For PostgreSQL as a systemd service, the file is
   `/sys/fs/cgroup/system.slice/<unit>/cpu.stat`. Without the option the
   PostgreSQL columns are "not measured".
3. Make the checkout of the build before telemetry: the merge of #2128
   (`2c8aea1072023a056f5e239251d6dc00a27c212a`), the last commit of
   `program/0.8.0` before the first telemetry change. It runs the harness of
   this checkout, which imports no telemetry API at the level `off`:

   ```sh
   git worktree add ../horsies-before-telemetry 2c8aea1072023a056f5e239251d6dc00a27c212a
   git -C ../horsies-before-telemetry checkout "$(git rev-parse HEAD)" -- crossclient/perf
   (cd ../horsies-before-telemetry && uv sync --project clients/python --frozen --dev)
   ```

   Build `horsies._core` in each checkout, as for the release session.
4. Make one database for each checkout (the two schemas differ by the
   telemetry migration) and install each schema with its own checkout:

   ```sh
   (cd ../horsies-before-telemetry && uv run --project clients/python python -m crossclient.perf \
       --apply-schema --dsn postgresql://USER:PASSWORD@HOST:PORT/horsies_before_telemetry)
   uv run --project clients/python python -m crossclient.perf --apply-schema \
       --dsn postgresql://USER:PASSWORD@HOST:PORT/horsies_overhead
   ```

### The runs

Run three rounds. A round runs every command below once, one after the
other, never two at the same time. Change the checkout that runs first in
each round. Each run interleaves its drains or blocks between its two sides,
and the first drain or block of each side is a warm-up.

The control runs of both checkouts (they give the noise and the rule for
`off`):

```sh
DSN_NOW=postgresql://USER:PASSWORD@HOST:PORT/horsies_overhead
DSN_BEFORE=postgresql://USER:PASSWORD@HOST:PORT/horsies_before_telemetry
for scenario in claim-finish send workflow-start; do
  uv run --project clients/python python -m crossclient.perf --scenario "$scenario" --scale gate \
      --demo-quiesced --control --telemetry off \
      --postgres-cpu-stat "$CPU_STAT" --write-summary --dsn "$DSN_NOW"
  (cd ../horsies-before-telemetry && uv run --project clients/python python -m crossclient.perf \
      --scenario "$scenario" --scale gate --demo-quiesced --control \
      --telemetry off --postgres-cpu-stat "$CPU_STAT" --write-summary \
      --dsn "$DSN_BEFORE")
done
```

The level cells, in this checkout only:

```sh
for level in metrics scraped tracing handler worker-ended-spans; do
  uv run --project clients/python python -m crossclient.perf --scenario claim-finish --scale gate \
      --demo-quiesced --telemetry "$level" \
      --postgres-cpu-stat "$CPU_STAT" --write-summary --dsn "$DSN_NOW"
done
for scenario in send workflow-start; do
  uv run --project clients/python python -m crossclient.perf --scenario "$scenario" --scale gate \
      --demo-quiesced --telemetry tracing \
      --postgres-cpu-stat "$CPU_STAT" --write-summary --dsn "$DSN_NOW"
done
```

Each run writes its summary to `crossclient/perf/results/` of its checkout,
one file per run (`<day>-<time>-<scenario>-telemetry-<level>-pg<major>.md`),
and its samples and readings to `results/raw/`. A gate run exits 1 when a
verdict of its scenario is not a pass (for a drain: a throughput ratio under
0.90); it writes its summary first. Keep the summaries out of the commit:
the results go into the template below.

### Noise and the rule

The noise is measured as the speed gate measures it: from interleaved
rounds after a warm-up, over three runs.

- The value of a run, for one measure: the mean of its two sides (in a
  control run both sides run the same build). The measures are tasks or
  calls per second, worker or producer CPU per task or call, and PostgreSQL
  CPU per task or call.
- The noise band of a measure in one checkout: (largest − smallest) / mean
  of the values of its three control runs. The band of a measure is the
  larger of the bands of the two checkouts.
- Write the bands, and the tolerance of a level cell, in the template
  before you read any comparison. The tolerance lies between the band and
  the cost of a wrong design (for example a write that waits on the handler
  child).

**The rule for `off`:** for each scenario and each measure, the mean of the
three values of this checkout over the mean of the three values of the build
before telemetry is within 1 ± the band. Telemetry off passes when every
measure of every scenario holds.

**A level cell:** the cost of a level is the candidate/baseline ratio of each
measure in its own run, with its 95% interval in the summary. Read it
against the band and the tolerance. In every `claim-finish` cell the
events dropped (queue full), the series dropped, the handler events dropped
and the `/livez` late answers are 0.

### Results template

Copy it to `crossclient/perf/results/telemetry-overhead/<day>.md` and fill
it from the summaries.

```markdown
# Telemetry overhead, <day>

Server: <CPU model, vCPUs, the CPUs of PostgreSQL, of the worker and of the
harness>. PostgreSQL <version>, the settings of
`crossclient/fixtures/perf/compose.yaml`. This checkout: <commit>. The build
before telemetry: 2c8aea1072023a056f5e239251d6dc00a27c212a with the harness
of <commit>.

## Recorded before any comparison is read

| Scenario | Measure | Band, this checkout | Band, before telemetry | Band used |
|---|---|---|---|---|
| claim-finish | tasks per second | | | |
| claim-finish | worker CPU per task | | | |
| claim-finish | PostgreSQL CPU per task | | | |
| send | calls per second | | | |
| send | producer CPU per call | | | |
| send | PostgreSQL CPU per call | | | |
| workflow-start | calls per second | | | |
| workflow-start | producer CPU per call | | | |
| workflow-start | PostgreSQL CPU per call | | | |

Tolerance of a level cell: <ratio>, because <reason>.

## Telemetry off against the build before telemetry

| Scenario | Measure | This checkout (3 runs) | Before telemetry (3 runs) | Ratio | Within the band |
|---|---|---|---|---|---|
| claim-finish | tasks per second | | | | |
| ... | | | | | |

Telemetry off: PASS | FAIL.

## Level cells: claim-finish

| Level | Round | Tasks/s ratio | Worker CPU/task ratio | Parent | Children | PostgreSQL CPU/task ratio | Stop time ratio | Series | Dropped: queue full | Dropped: child down | Series dropped | Handler dropped | Handler errors | /livez late |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| metrics | 1 | | | | | | | | | | | | | |
| ... | | | | | | | | | | | | | | |

## Level cells: send and workflow-start

| Scenario | Round | Calls/s ratio | Producer CPU/call ratio | PostgreSQL CPU/call ratio |
|---|---|---|---|---|
| send | 1 | | | |
| ... | | | | |

## Constants

| Constant | Value before | Value after | The cell and the number that set it |
|---|---|---|---|
| ... | | | |
```

### The constants that the session sets

These constants of the telemetry code hold a provisional value. Each row
names the cell whose numbers set it. The session changes no value; a change
follows from the filled template. `test_perf_overhead_constants.py` checks
that each constant is defined in the file of its row.

| Constant | File | Value | What it bounds | The cell that sets it |
|---|---|---|---|---|
| `MAX_SERIES_PER_METRIC` | `core/src/telemetry/registry.rs` | 10,000 series | The series of one metric in the registry of a worker. A sample past it is dropped and counts `horsies.telemetry.series.dropped`. | `claim-finish` `metrics`: the worker parent CPU per task (the series lookup of each sample), the series count, and series dropped = 0. |
| `ENDPOINT_MAX_CONNECTIONS` | `core/src/telemetry/endpoint.rs` | 16 connections | The open connections of a metrics endpoint (the worker and `horsies metrics`). A connection over it is closed with no answer. | `claim-finish` `scraped` against `metrics`: the worker parent CPU per task over the reads per drain is the cost of one read; the bound times that cost is the most that reads can take at once. |
| `ENDPOINT_REQUEST_TIMEOUT` | `core/src/telemetry/endpoint.rs` | 5 s | The time to read one request head; also the time of the open connections at the stop. | `claim-finish` `scraped`: every read answered within `READ_TIMEOUT` (2 s). The slow scrape fault test checks the bound. |
| `ENDPOINT_MAX_REQUEST_BYTES` | `core/src/telemetry/endpoint.rs` | 8 KiB | The request buffer of one connection; 8 KiB is the least that HTTP/1 serving takes. | `claim-finish` `scraped`: no read refused. It stays at its floor unless a cell shows a refused head. |
| `LIVEZ_TURN_ALLOWANCE` | `core/src/worker/probes.rs` | 30 s | How late a turn of the claim loop may be before `/livez` answers 503. | Every `claim-finish` cell from `scraped` on: `/livez` late answers = 0 at full drain speed. |
| `HANDLER_READY_TIMEOUT` | `core/src/worker/executor/handler_child/mod.rs` | 30 s | The wait for the ready frame of one boot of the handler child, before the first claim. | No cell: the cells do not time the boot of the handler child. The tasks per second of `handler` against `tracing` include it, with the cost of the events. |
| `EVENT_QUEUE_CAPACITY` | `core/src/telemetry/worker.rs` | 8,192 events | The events that wait for the writer to the handler child. An offer to a full queue drops the event and counts `queue_full`. | `claim-finish` `handler`: events dropped (queue full) = 0 at full drain speed. |
| `EVENT_BATCH_MAX` | `core/src/worker/executor/handler_child/mod.rs` | 256 events (`MAX_EVENTS_PER_FRAME`) | The events of one `events` frame. | `claim-finish` `handler` against `tracing`: the worker parent CPU per task (the frame cost of each event). |
| `MAX_EVENTS_PER_FRAME` | `core/src/worker/executor/protocol/bounds.rs` | 256 events | The protocol bound of an `events` frame; `EVENT_BATCH_MAX` takes it. | As `EVENT_BATCH_MAX`. |
| `EVENT_FLUSH_INTERVAL` | `core/src/worker/executor/handler_child/mod.rs` | 25 ms | The wait for more events after the first event of a batch. | `claim-finish` `handler` against `tracing`: the worker parent CPU per task and the handler child CPU (the children CPU per task). |
| `HANDLER_STOP_FLUSH` | `core/src/worker/executor/handler_child/mod.rs` | 2 s | At the stop: the write of the queue and of the stop frame to the handler child. | `claim-finish` `handler` against `tracing`: the worker stop time. |
| `MAX_EVENT_HANDLERS` | `core/src/worker/executor/protocol/bounds.rs` | 32 handlers | The handlers of one app, and the entries of one `handler_stats` frame. | `claim-finish` `handler`: the handler child CPU per task of one handler; 32 times it is the most. |
| `MAX_EVENT_WIRE_BYTES` | `core/src/worker/executor/protocol/bounds.rs` | 8 KiB | The wire form of one event. | No cell: the bounds of the fields of the largest event set it (its comment). |
| `MAX_TRACE_CARRIER_KEYS` | `core/src/telemetry/trace_carrier.rs` | 8 keys | The keys of a stored trace carrier. Over it, the send stores NULL. | `claim-finish` `tracing`: the PostgreSQL CPU per task with a W3C carrier. The carrier fault test checks the bound. |
| `MAX_TRACE_CARRIER_BYTES` | `core/src/telemetry/trace_carrier.rs` | 2,048 bytes | The text of a stored trace carrier. Over it, the send stores NULL. | As `MAX_TRACE_CARRIER_KEYS`. |
| `HANDLER_QUEUE_EVENTS` | `clients/python/horsies/core/worker/handler_child.py` | 10,000 events | The inbox of one handler in the Python handler child. An event that does not fit drops for that handler and counts `horsies.telemetry.handler.dropped`. | `claim-finish` `handler`: handler events dropped = 0 at full drain speed. |
| `HANDLER_STATS_INTERVAL_S` | `clients/python/horsies/core/worker/handler_child.py` | 5 s | The time between two `handler_stats` frames. | `claim-finish` `handler` against `tracing`: the children CPU per task. |
| `HANDLER_DRAIN_S` | `clients/python/horsies/core/worker/handler_child.py` | 2 s | At the stop: the time of the handler threads for the events left in their inboxes. | `claim-finish` `handler` against `tracing`: the worker stop time. |
| `HANDLER_ERROR_LOG_EVERY` | `clients/python/horsies/core/worker/handler_child.py` | 1,000 failures | One log line per this many failures of a handler, after the first. | No cell: a log volume bound; the handler of the cells does not fail. |
| `HANDLER_QUEUE_EVENTS` | `core/src/worker/executor/child_host/handlers.rs` | 10,000 events | The inbox of one handler in the Rust handler runtime. | No cell of this harness: its drain runs the Python worker. The Python row sets the value of both. |
| `HANDLER_STATS_INTERVAL` | `core/src/worker/executor/child_host/handlers.rs` | 5 s | As `HANDLER_STATS_INTERVAL_S`, in the Rust handler runtime. | As the Rust `HANDLER_QUEUE_EVENTS`. |
| `HANDLER_DRAIN` | `core/src/worker/executor/child_host/handlers.rs` | 2 s | As `HANDLER_DRAIN_S`, in the Rust handler runtime. | As the Rust `HANDLER_QUEUE_EVENTS`. |
| `HANDLER_ERROR_LOG_EVERY` | `core/src/worker/executor/child_host/handlers.rs` | 1,000 failures | As the Python row, in the Rust handler runtime. | No cell: a log volume bound. |
| `ATTEMPT_SPAN_FLUSH_BOUND_S` | `clients/python/horsies/core/telemetry/attempt_span.py` | 1.0 s | The flush of the attempt spans at the stop of a task child. | `claim-finish` `tracing` against `scraped`: the worker stop time. The hung exporter fault test checks the bound. |
