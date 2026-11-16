---
title: Task-history cutover
summary: Offline upgrade from migration 0032 to the task-history migration 0042.
related: [../internals/database-schema, ../configuration/retention-config]
tags: [operations, cutover, migration, task-history, backup]
---

## Scope

This runbook upgrades a database whose Rust migration ledger ends at 0032.
Migration 0032 is the schema-v26 compatibility boundary. The new binary applies
migrations 0033–0042. The final table shape matches task-history schema v35.
The upgrade keeps live tasks, terminal task records, attempts, and workflow
data.

The upgrade is offline. There is no rolling upgrade path. Stop every producer,
worker, scheduler, and monitoring process before the program replacement.
Keep every process stopped until validation writes the cutover attestation.

A fresh database applies migration 0042 and starts at the final schema shape.
It does not need this cutover.

## Before the window

1. Stop producers.
2. Drain in-flight work when possible.
3. Stop workers, schedulers, and monitoring processes.
4. Confirm that no application transaction is open.
5. Take a named backup.
6. Verify that the backup can be read.
7. Record the measured coefficients for this database server.

Install the new binary after every process is stopped. Run one broker schema
initialization. It applies migrations 0033–0042. Initialization then refuses
normal startup because the cutover attestation is absent. That refusal is
expected. Keep the fleet stopped.

Use a direct or session-pooled PostgreSQL URL. Do not use a transaction-pooled
URL for the cutover.

Create and verify the backup before `tighten`:

```sh
pg_dump -Fc "$DATABASE_URL" -f pre-task-history.dump
pg_restore --list pre-task-history.dump >/dev/null
```

Keep the file name as the backup label. The tighten command requires the exact
phrase `point-of-no-return: <backup-label>`.

## Measure the coefficients

Do not copy coefficients from another host. Measure them on the server that
will run the cutover.

Use one disposable database. Seed 100,000 terminal rows. Seed one attempt per
task. Use a batch size of 10,000.

Run the complete cutover on that database. Record every committed preparation
batch. Record every committed relocation batch. Record the stage boundaries.

Fit these values:

- `fixed_seconds`: preflight, tighten, and validation time.
- `relocation_seconds_per_million`: least-squares slope over the committed
  relocation trajectory.
- `preparation_seconds_per_million`: least-squares slope over the committed
  preparation trajectory.

Pass the fitted values to `preflight` and `run`. Keep the raw trajectories with
the change record for the cutover.

```sh
horsies cutover \
  --database-url "$DATABASE_URL" \
  preflight \
  --relocation-seconds-per-million "$RELOCATION_SECONDS_PER_MILLION" \
  --fixed-seconds "$FIXED_SECONDS" \
  --preparation-seconds-per-million "$PREPARATION_SECONDS_PER_MILLION"
```

Use `ladder-evaluate` for measured scale rungs. A busted ceiling or a result
below the prediction floor returns a non-zero exit. Stop the campaign when
either bound fails.

## Stage order

Run the stages in this order:

1. `preflight`
2. `drain`
3. `install-programs`
4. `prepare`
5. `relocate`
6. `tighten`
7. `validate`

`install-programs` runs drain verification again. It then normalizes attempt
identity. It installs the UUID-aware move program last.

Preparation and relocation commit bounded batches. Both stages are resumable.
Run the same command again after an interruption.

### Check status

```sh
horsies cutover --database-url "$DATABASE_URL" status
```

Status prints the stored schema, identity types, program posture, row counts,
ledger count, and attestation state.

### Verify drain

```sh
horsies cutover \
  --database-url "$DATABASE_URL" \
  drain \
  --heartbeat-quiet-seconds 60
```

`PENDING` rows may remain. They survive the cutover. `CLAIMED`, `RUNNING`,
finalizing, or recently heartbeating rows block the stage. Recover stale claims
through the normal stale-claim recovery path. Do not edit task rows by hand.

### Install the move program

```sh
horsies cutover \
  --database-url "$DATABASE_URL" \
  install-programs \
  --heartbeat-quiet-seconds 60
```

This stage requires a drained database. Keep the old fleet stopped after it
passes.

### Prepare legacy rows

```sh
horsies cutover \
  --database-url "$DATABASE_URL" \
  prepare \
  --batch-size 10000 \
  --retain-rerun-input-default false
```

Set `--retain-rerun-input-default true` only when old task input must support
rerun. The stage reports inline, over-bound, policy-declined, and decode-failed
counts.

Rows without a retention class use `forever`. Assign a finite class before the
cutover if those rows must age out.

### Relocate terminal rows

```sh
horsies cutover \
  --database-url "$DATABASE_URL" \
  relocate \
  --batch-size 10000
```

Relocation moves terminal rows to history. It archives attempts in the same
transaction. It records each committed batch in the relocation ledger.

### Tighten the schema

`tighten` is the point of no return. The only rollback after this stage is a
restore from the named backup.

```sh
horsies cutover \
  --database-url "$DATABASE_URL" \
  tighten \
  --backup-label pre-task-history.dump \
  --operator-confirmation "point-of-no-return: pre-task-history.dump"
```

The stage refuses missing enqueue facts. It also refuses invalid UUID values.
It converts the frozen identity set to PostgreSQL `uuid`. It restores the
required foreign keys. It enforces the live-only task status domain.

### Validate and attest

```sh
horsies cutover --database-url "$DATABASE_URL" validate
```

Validation checks the frozen schema and the relocation ledger. It writes
`task_history_v1_validated_v1` only after every check passes. An invalid result
removes the attestation.

Do not restart any process until validation reports:

```text
validation passed and attested
```

## R2 rollback before tighten

`rollback-programs` is the reversible program rollback. Use it only before
`tighten`.

```sh
horsies cutover --database-url "$DATABASE_URL" rollback-programs
```

R2 restores the pre-cutover terminalization program. It restores attempt task
identity to `varchar(36)`. It restores the canonical attempt foreign key. The
old fleet may restart only after this command succeeds.

R2 refuses an attested or UUID-born live schema. R2 also refuses an active
fleet. After `tighten`, restore the named backup instead.

## Run all stages

The `run` command uses the same order. It requires an explicit confirmation
for every mutating stage.

```sh
horsies cutover \
  --database-url "$DATABASE_URL" \
  run \
  --relocation-seconds-per-million "$RELOCATION_SECONDS_PER_MILLION" \
  --fixed-seconds "$FIXED_SECONDS" \
  --preparation-seconds-per-million "$PREPARATION_SECONDS_PER_MILLION" \
  --preparation-batch-size 10000 \
  --relocation-batch-size 10000 \
  --retain-rerun-input-default false \
  --backup-label pre-task-history.dump \
  --operator-confirmation "point-of-no-return: pre-task-history.dump" \
  --confirm-stage drain \
  --confirm-stage normalize-identity \
  --confirm-stage install-programs \
  --confirm-stage prepare \
  --confirm-stage relocate \
  --confirm-stage tighten \
  --confirm-stage validate
```

## After validation

Check both schema dimensions:

```sql
SELECT max(version)
FROM horsies_migrations
WHERE success;

SELECT completed_at
FROM horsies_cutover_state
WHERE cutover_name = 'task_history_v1_validated_v1';
```

The migration version must be 42. The attestation query must return one row.

Start only the upgraded processes. Confirm these facts:

- `horsies_tasks` has no terminal rows.
- New terminal tasks appear in `horsies_task_history`.
- History and heartbeat partitions exist ahead of writes.
- Task result and info reads resolve moved tasks.
