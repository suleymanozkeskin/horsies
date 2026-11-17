---
title: Rerun in Your Transaction
summary: Re-execute a terminal task as a new task with horsies::rerun, in a transaction of the app or in a transaction that the caller owns, with its command, policy, outcomes and errors.
related: [../tasks/sending-tasks, ../monitoring/monitoring-queries, ../../monitoring/action-semantics, ../../configuration/retention-config]
tags: [rust, rerun, rerun_task, rerun_task_in, RerunTask, RerunEnqueuePolicy, RerunOutcome, sqlx, tokio-postgres]
---

# Rerun in Your Transaction

A rerun reads a terminal source task and, for an eligible source with a retained input, writes one new task with a new id and the lineage `RERUN`. Every outcome but `Enqueued` writes nothing. Which tasks can be rerun, and what each outcome means, is on [Task rerun](/horsies/monitoring/action-semantics/#task-rerun).

| Call | Transaction |
|------|-------------|
| `app.rerun_task(command, policy)` | A transaction of the app's handle, which the call commits |
| `horsies::rerun::rerun_task_in(transaction, &command, &policy, sent_at)` | A transaction that the caller owns. The rerun commits or rolls back with the caller's own writes |

## Rerun in a Transaction of the App

```rust
use horsies::rerun::{AppRerunError, RerunEnqueuePolicy, RerunOutcome, RerunTask};
use horsies::Horsies;
use uuid::Uuid;

async fn rerun_failed(app: &Horsies, source_task_id: Uuid) -> Result<Option<Uuid>, AppRerunError> {
    let command = RerunTask::new(source_task_id, None, None);
    let policy = RerunEnqueuePolicy::standard(true);
    match app.rerun_task(command, policy).await? {
        RerunOutcome::Enqueued { new_task_id, .. } => Ok(Some(new_task_id)),
        RerunOutcome::KeyReplay { existing_task_id } => Ok(Some(existing_task_id)),
        RerunOutcome::SourceLive { .. }
        | RerunOutcome::SourceAbsent { .. }
        | RerunOutcome::SourceCorrupt { .. }
        | RerunOutcome::NotEligible { .. }
        | RerunOutcome::InputUnavailable { .. }
        | RerunOutcome::InputCorrupt { .. }
        | RerunOutcome::KeyConflict { .. } => Ok(None),
    }
}
```

The handle checks the client family of the database before the transaction, and installs the schema once per handle, as a send does. In a worker child the call goes through the child's producer handle.

`AppRerunError`:

| Variant | When |
|---------|------|
| `Connect(BrokerError)` | The handle could not connect or install the schema. Nothing ran. The retry rule is the cause's |
| `Rerun(RerunError)` | The rerun failed. Nothing committed, except after a failure at `COMMIT` whose outcome is unknown: then the same keyed rerun sent again answers `KeyReplay` |

## Rerun in the Caller's Transaction

`rerun_task_in` runs its statements on a transaction of the caller's driver, on the database of the app. The adapters are behind features of the crate:

| Feature | Adapter | Transaction |
|---------|---------|-------------|
| `sqlx` | `SqlxCaller::new(&mut transaction)` | `sqlx::Transaction<'_, Postgres>` |
| `tokio-postgres` | `TokioPostgresCaller::new(&mut transaction)` | `tokio_postgres::Transaction<'_>` |

```rust
use chrono::Utc;
use horsies::rerun::{rerun_task_in, RerunEnqueuePolicy, RerunOutcome, RerunTask, SqlxCaller};
use sqlx::PgPool;
use uuid::Uuid;

async fn rerun_with_audit(
    pool: &PgPool,
    source_task_id: Uuid,
) -> Result<RerunOutcome, Box<dyn std::error::Error>> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO audit_log (action, task_id) VALUES ('rerun', $1)")
        .bind(source_task_id)
        .execute(&mut *transaction)
        .await?;
    let outcome = rerun_task_in(
        &mut SqlxCaller::new(&mut transaction),
        &RerunTask::new(source_task_id, None, None),
        &RerunEnqueuePolicy::standard(true),
        Utc::now(),
    )
    .await?;
    transaction.commit().await?;
    Ok(outcome)
}
```

- Statements, on the caller's transaction: the read of the source; then, for an eligible source with a retained input, one enqueue of the new task, which first claims the caller key of a keyed rerun.
- The read keeps an `ACCESS SHARE` lock on the history leaf of the source until the transaction ends.
- `rerun_task_in` commits nothing. The caller's commit keeps the new task and the key claim; its rollback drops them.
- After an error, the transaction may hold the key claim or the new row: do not commit it.
- `sent_at` is the sent instant of the new task.
- `SqlxCaller` runs each statement unnamed, so a PgBouncer transaction pool with `max_prepared_statements = 0` serves it.

`CallerRerunError<E>`, with `E` the error of the caller's driver:

| Variant | When |
|---------|------|
| `Driver(E)` | A statement failed in the caller's driver. Its retry rule is the driver's |
| `ClassNotRegistered { key }` | The retention class of the new task has no class row. Nothing is written |
| `ClientFamilyMismatch { stored }` | Another client installed the database, or it records none. Nothing is written. Not retryable |
| `CallerKeyRefused(IdempotencyKeyError)` | The caller key is not a valid idempotency key. Nothing is written |
| `IdMintRefused(Uuid7Error)` | The id of the new task could not be minted. Nothing is written |
| `Row(RerunRowError)` | A row did not fit its statement. Not retryable |

### Another Driver

Implement `CallerTransaction` for the transaction type of another driver:

```rust
use std::future::Future;

use horsies::rerun::{CallerRow, CallerStatement};

pub trait CallerTransaction: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    fn query_one(
        &mut self,
        statement: &CallerStatement,
    ) -> impl Future<Output = Result<CallerRow, Self::Error>> + Send;
}
```

`query_one` runs `statement.sql()` with `statement.parameters()` (each a `StatementValue`) in the transaction, and returns its one row as `CallerRow::new(values)`: one `StatementValue` per `statement.columns()` entry, decoded as the declared `CallerType`, SQL `NULL` as `StatementValue::Null`. It does not commit.

## RerunTask

`RerunTask::new(source_task_id, deadline, caller_key)`:

| Argument | Type | Meaning |
|----------|------|---------|
| `source_task_id` | `Uuid` | The terminal task to rerun |
| `deadline` | `Option<DateTime<Utc>>` | The `good_until` of the new task; `None` for no deadline |
| `caller_key` | `Option<String>` | The idempotency key of the rerun. The key rules are on [Atomic Enqueue and Topology](/horsies/operations/atomic-enqueue-and-topology/#make-the-send-repeatable-with-an-idempotency-key) |

## RerunEnqueuePolicy

The new task does not take the policy of its source.

| Constructor | Policy |
|-------------|--------|
| `RerunEnqueuePolicy::new(retention_class_key, retain_rerun_input, reservation_window)` | `Result<_, RerunError>`: refuses an empty class key (`InvalidPolicy`), and a reservation window that is not above zero, is above 30 days, or is finer than a microsecond (`Idempotency`) |
| `RerunEnqueuePolicy::standard(retain_rerun_input)` | The retention class `standard_30d` and a 24-hour reservation window |

- `retention_class_key` is the class key of the new task's retention period, for example `standard_30d`. A class that has no class row is refused when the rerun runs. See [Retention Config](/horsies/configuration/retention-config/).
- `retain_rerun_input: true` keeps the input of the new task, so that it can be rerun in turn.
- `reservation_window: chrono::Duration` is how long the caller key stays reserved.

## RerunOutcome

| Variant | Meaning |
|---------|---------|
| `Enqueued { new_task_id, source_task_id, rerun_root_task_id }` | The new task is written |
| `KeyReplay { existing_task_id }` | The caller key already holds this rerun command; the existing task answers |
| `KeyConflict { task_id, reserved_by_task_id }` | Another command holds the caller key |
| `SourceLive { task_id }` | The source has not ended |
| `SourceAbsent { task_id, reason }` | No record has the id; `reason` is a `TaskAbsenceReason` |
| `SourceCorrupt { task_id, detail }` | The source history row does not decode |
| `NotEligible { task_id, reason }` | `NotEligibleReason::CompletedSource` or `WorkflowTask` |
| `InputUnavailable { task_id, disposition }` | The source stores no input; `disposition` is the stored reason, for example `DECLINED_BY_POLICY` or `OVER_BOUND` |
| `InputCorrupt { task_id, detail }` | The stored input fails its integrity or shape check |

## Things to Avoid

**Do not commit a transaction after an error of `rerun_task_in`.** It may hold the key claim or the new row of a rerun that did not complete.

**Do not run `rerun_task_in` on a transaction of another database.** The rerun reads and writes the tables of the app's database.
