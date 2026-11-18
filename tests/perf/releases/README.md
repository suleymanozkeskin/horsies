# Harness variants of released versions

A speed comparison of two releases runs the harness of each release on one
server in one session. The harness of a release can fail on the schema of the
same release, when a schema change of that release came after the last run of
its harness. Each folder here holds one patch for the `tests/perf` folder of
one released tag. The patch makes that harness run on the schema of that
release. The patch changes the harness only. It changes no product code.

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
  same file as in the current harness, and the scenarios `send` and
  `workflow-start` (see "Scenario map"). The runner, the entry point and the
  report change as in the current harness to run them; the report names the
  payload "payload per operation".

The patch does not change the unit tests of the 0.7.0 harness
(`tests/unit/test_perf_run_exit.py`). Some of these tests need a scenario
with a candidate side, so they fail with the patch. A session does not run
them.

## Scenario map

The scenarios of the current harness call public operations of the library.
Each one has a v0.7.0 scenario of the same name that runs the same file,
`tests/perf/public_calls.py`. Every call in that file is public API of both
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
- Claim and finish have no scenario. No public API of either release claims
  or finishes a task: the worker does both (#2049).

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
   (`tests/fixtures/perf/compose.yaml`): `pg_stat_statements` preloaded with
   `track=all` and `track_utility=off`, `fsync`, `synchronous_commit`,
   `full_page_writes` and `autovacuum` off. Stop every other workload on the
   host.
2. Create the two databases, for example `horsies_v070` and `horsies_v080`.
3. Make the v0.7.0 checkout, from the root of the current checkout:

   ```sh
   git worktree add ../horsies-v0.7.0 v0.7.0
   git -C ../horsies-v0.7.0 apply "$PWD/tests/perf/releases/v0.7.0/harness.patch"
   (cd ../horsies-v0.7.0 && uv sync --frozen --dev)
   ```

   The current checkout runs in its own environment, with a built
   `horsies._core`.

4. Install the two schemas. The two releases accept different URL schemes:
   v0.7.0 needs `postgresql+psycopg://`, the current release accepts
   `postgresql://` and `postgres://` only.

   ```sh
   (cd ../horsies-v0.7.0 && uv run python -m tests.perf --apply-schema \
       --dsn postgresql+psycopg://USER:PASSWORD@HOST:PORT/horsies_v070)
   uv run python -m tests.perf --apply-schema \
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
   uv run python -m tests.perf \
       --scenario send --scale gate \
       --demo-quiesced --control --write-summary \
       --dsn postgresql://USER:PASSWORD@HOST:PORT/horsies_v080
   ```

6. Compare the runs. The harness compares the two sides of one run only. In
   a control run both sides send the same statement. Each checkout writes its
   summaries to its own `tests/perf/results/` and the samples to
   `tests/perf/results/raw/`. Compare the latency percentiles and the server
   counts of the two releases from these files.

Each run seeds 100,000 ballast rows through the terminal write of its own
release, and deletes its own rows when it ends.
