//! Ledgered, resumable relocation of pre-cutover terminal rows.

use chrono::{DateTime, Duration, Utc};
use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::core::history::commands::{CreateDailyHistoryLeaf, LeafBounds, LeafRef};
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::ddl::runtime_names::daily_leaf_name;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{
    LIVE_ATTEMPTS, LIVE_TASKS, TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT,
};
use crate::core::history::outcomes::LeafCreation;
use crate::core::history::partitions::catalog::{
    read_leaf_catalog_row, read_leaf_physical_state, read_retention_class,
};
use crate::core::history::partitions::forever::FOREVER_LEGACY_LEAF;
use crate::core::history::partitions::manager::create_daily_leaf;
use crate::core::history::partitions::publication::{LoaderPublication, UnpublishedLoader};
use crate::core::history::projection::render_relocation_insert_sql;
use crate::core::history::reads::publisher::StagedLoaderPublisher;

pub const RELOCATION_LEDGER: &str = "horsies_cutover_relocation_ledger";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelocationOutcome {
    Batch {
        batch_number: i64,
        rows_relocated: usize,
        legacy_kind_rows: i64,
    },
    Complete {
        batches_committed: i64,
        rows_relocated: i64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RelocationError {
    #[error("relocation batch size must be positive")]
    InvalidBatchSize,
    #[error("legacy task identity {0:?} is not a UUID")]
    InvalidTaskIdentity(String),
    #[error(transparent)]
    History(#[from] HistoryError),
}

#[derive(FromRow)]
struct Destination {
    class_key: String,
    lower_anchor: DateTime<Utc>,
}

pub async fn relocate_terminal_batch(
    connection: &mut PgConnection,
    batch_size: i64,
) -> Result<RelocationOutcome, RelocationError> {
    if batch_size <= 0 {
        return Err(RelocationError::InvalidBatchSize);
    }
    let task_ids: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT t.id::text FROM {LIVE_TASKS} AS t
         WHERE t.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING')
           AND NOT EXISTS (
               SELECT 1 FROM {TASK_HISTORY_PARENT} AS h
               WHERE h.task_id = CAST(t.id AS uuid)
           )
         ORDER BY t.id LIMIT $1"
    ))
    .bind(batch_size)
    .fetch_all(&mut *connection)
    .await
    .map_err(HistoryError::from)?;
    if task_ids.is_empty() {
        let (batches, rows): (i64, i64) = sqlx::query_as(&format!(
            "SELECT COALESCE(count(*), 0), COALESCE(sum(rows_relocated), 0)
             FROM {RELOCATION_LEDGER}"
        ))
        .fetch_one(connection)
        .await
        .map_err(HistoryError::from)?;
        return Ok(RelocationOutcome::Complete {
            batches_committed: batches,
            rows_relocated: rows,
        });
    }

    ensure_batch_leaf_coverage(connection, &task_ids).await?;
    let insert = format!(
        "{}\n    RETURNING (terminalization_kind = 'LEGACY_TERMINAL')::int",
        render_relocation_insert_sql("$1::text[]")
    );
    let flags: Vec<i32> = sqlx::query_scalar(&insert)
        .bind(&task_ids)
        .fetch_all(&mut *connection)
        .await
        .map_err(HistoryError::from)?;
    if flags.len() != task_ids.len() {
        return Err(RelocationError::History(HistoryError::contract(format!(
            "relocation inserted {} of {} selected rows",
            flags.len(),
            task_ids.len()
        ))));
    }
    let uuid_ids: Vec<Uuid> = task_ids
        .iter()
        .map(|task_id| {
            Uuid::parse_str(task_id)
                .map_err(|_| RelocationError::InvalidTaskIdentity(task_id.clone()))
        })
        .collect::<Result<_, _>>()?;
    sqlx::query(&format!(
        "DELETE FROM {LIVE_ATTEMPTS} WHERE task_id = ANY($1::uuid[])"
    ))
    .bind(&uuid_ids)
    .execute(&mut *connection)
    .await
    .map_err(HistoryError::from)?;
    let deleted: Vec<i32> = sqlx::query_scalar(&format!(
        "DELETE FROM {LIVE_TASKS} WHERE id::text = ANY($1::text[]) RETURNING 1"
    ))
    .bind(&task_ids)
    .fetch_all(&mut *connection)
    .await
    .map_err(HistoryError::from)?;
    if deleted.len() != task_ids.len() {
        return Err(RelocationError::History(HistoryError::contract(format!(
            "relocation deleted {} of {} relocated rows",
            deleted.len(),
            task_ids.len()
        ))));
    }
    let batch_number: i64 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(max(batch_number), 0) + 1 FROM {RELOCATION_LEDGER}"
    ))
    .fetch_one(&mut *connection)
    .await
    .map_err(HistoryError::from)?;
    let legacy_kind_rows = i64::from(flags.iter().sum::<i32>());
    sqlx::query(&format!(
        "INSERT INTO {RELOCATION_LEDGER} (
             batch_number, task_ids, rows_relocated, legacy_kind_rows, committed_at
         ) VALUES ($1, $2, $3, $4, statement_timestamp())"
    ))
    .bind(batch_number)
    .bind(&uuid_ids)
    .bind(i32::try_from(task_ids.len()).map_err(|_| {
        RelocationError::History(HistoryError::contract(
            "relocation batch exceeds integer range",
        ))
    })?)
    .bind(i32::try_from(legacy_kind_rows).map_err(|_| {
        RelocationError::History(HistoryError::contract(
            "legacy-kind count exceeds integer range",
        ))
    })?)
    .execute(connection)
    .await
    .map_err(HistoryError::from)?;
    Ok(RelocationOutcome::Batch {
        batch_number,
        rows_relocated: task_ids.len(),
        legacy_kind_rows,
    })
}

async fn ensure_batch_leaf_coverage(
    connection: &mut PgConnection,
    task_ids: &[String],
) -> Result<(), HistoryError> {
    let destinations: Vec<Destination> = sqlx::query_as(&format!(
        "SELECT COALESCE(retention_class_key, '{FOREVER_CLASS_KEY}') AS class_key,
                date_trunc('day', terminal_at, 'UTC') AS lower_anchor
         FROM {LIVE_TASKS}
         WHERE id::text = ANY($1::text[])
         GROUP BY COALESCE(retention_class_key, '{FOREVER_CLASS_KEY}'),
                  date_trunc('day', terminal_at, 'UTC')
         ORDER BY class_key, lower_anchor"
    ))
    .bind(task_ids)
    .fetch_all(&mut *connection)
    .await?;
    let mut created = false;
    let legacy_forever_upper = attached_legacy_forever_upper(connection).await?;
    for destination in destinations {
        let retention_class = read_retention_class(connection, &destination.class_key)
            .await?
            .ok_or_else(|| {
                HistoryError::contract(format!(
                    "relocation destination class {:?} is not registered",
                    destination.class_key
                ))
            })?;
        let parent =
            if destination.class_key == FOREVER_CLASS_KEY && retention_class.duration.is_none() {
                TASK_HISTORY_FOREVER.to_owned()
            } else {
                retention_class.finite_parent_name.ok_or_else(|| {
                    HistoryError::contract(format!(
                        "relocation destination class {:?} has no RANGE parent",
                        destination.class_key
                    ))
                })?
            };
        if destination.class_key == FOREVER_CLASS_KEY
            && legacy_forever_upper.is_some_and(|upper| destination.lower_anchor < upper)
        {
            continue;
        }
        let leaf_name = daily_leaf_name(&parent, destination.lower_anchor)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        let bounds = LeafBounds::new(
            destination.lower_anchor,
            destination.lower_anchor + Duration::days(1),
        )
        .map_err(|error| HistoryError::contract(error.to_string()))?;
        let leaf = LeafRef::new(leaf_name, &destination.class_key, bounds)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        let command = CreateDailyHistoryLeaf::new(leaf)
            .map_err(|error| HistoryError::contract(error.to_string()))?;
        match create_daily_leaf(connection, &command, &UnpublishedLoader).await? {
            LeafCreation::Created { .. } => created = true,
            LeafCreation::AlreadyConformant { .. } | LeafCreation::IndexRepaired { .. } => {}
            refusal => {
                return Err(HistoryError::contract(format!(
                    "relocation destination leaf refused: {refusal:?}"
                )));
            }
        }
    }
    let publisher = StagedLoaderPublisher;
    if created {
        publisher.republish(connection).await?;
    }
    Ok(())
}

async fn attached_legacy_forever_upper(
    connection: &mut PgConnection,
) -> Result<Option<DateTime<Utc>>, HistoryError> {
    let Some(catalog) = read_leaf_catalog_row(connection, FOREVER_LEGACY_LEAF).await? else {
        return Ok(None);
    };
    if catalog.class_key != FOREVER_CLASS_KEY
        || catalog.parent_name != TASK_HISTORY_FOREVER
        || catalog.detached_at.is_some()
        || catalog.dropped_at.is_some()
    {
        return Ok(None);
    }
    let physical = read_leaf_physical_state(
        connection,
        &catalog.leaf_name,
        &catalog.parent_name,
        &catalog.id_index_name,
    )
    .await?;
    let conformant = physical.detach_pending == Some(false)
        && physical.partition_bound.as_deref() == Some(catalog.partition_bound.as_str());
    Ok(conformant.then_some(catalog.upper_anchor))
}
