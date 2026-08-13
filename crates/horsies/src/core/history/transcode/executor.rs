//! Five-stage replacement-partition archive transcode executor.
//!
//! Copy cursors compare PostgreSQL `tid` values, verification completes its
//! content scan before swap locks, and swap only rechecks the durable identity
//! token and catalog attachment inside a non-queuing lock window.

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::core::history::archive::versions::JSON_UTF8_CODEC;
use crate::core::history::names::{LEAF_CATALOG, TASK_HISTORY_PARENT};
use crate::core::history::partitions::locks::lock_leaf_for_transaction;

use super::jobs::{
    job_relations, lock_job, RelationVerificationToken, TranscodeJobRow, TranscodeRelationRow,
    TRANSCODE_BATCHES, TRANSCODE_JOBS, TRANSCODE_MUTATION_FUNCTION, TRANSCODE_RELATIONS,
};
use super::maintenance::{active_maintenance_session, lock_transcode_program};
use super::outcomes::{
    ArchiveComponent, SwapBlocker, SwapLockMode, TranscodeCopyBatch, TranscodeCopyOutcome,
    TranscodeCopyRejected, TranscodeCopyRejectionKind, TranscodeFinalized, TranscodeJobState,
    TranscodePlan, TranscodePlanOutcome, TranscodePlanRejected, TranscodeReadyForVerification,
    TranscodeSwap, TranscodeSwapBusy, TranscodeSwapExhausted, TranscodeSwapOutcome,
    TranscodeVerification, BLOCKER_QUERY_TRUNCATION_CHARS, SWAP_LOCK_ATTEMPTS_MAXIMUM,
    SWAP_RETRY_BACKOFF_SECONDS,
};
use super::signature::relation_schema_signature;
use super::transforms::{
    backup_relation_name, column_list, component_columns, encoded_source_select, quoted_identifier,
    replacement_bound_name, replacement_index_name, replacement_ordering_index_name,
    replacement_relation_name, transformed_select,
};
use super::TranscodeError;

#[derive(Debug, FromRow)]
struct InventoryRow {
    relation_oid: i64,
    relation_name: String,
    parent_oid: i64,
    parent_name: String,
    partition_bound: String,
    partition_constraint: String,
    row_count: i64,
    transformed_rows: i64,
    payload_rows: i64,
    payload_bytes: i64,
    relation_bytes: i64,
    distinct_task_ids: i64,
}

pub async fn plan_transcode(
    connection: &mut PgConnection,
    job_id: Uuid,
    component: ArchiveComponent,
    source_version: i16,
    target_version: i16,
    source_codec: &str,
    target_codec: &str,
) -> Result<TranscodePlanOutcome, TranscodeError> {
    if (i32::from(target_version) - i32::from(source_version)).abs() != 1 {
        return Ok(TranscodePlanOutcome::Rejected(TranscodePlanRejected {
            component,
            reason: "unsupported transcode direction".to_owned(),
            affected_rows: 0,
        }));
    }
    lock_transcode_program(&mut *connection).await?;
    crate::core::history::maintenance::gate::lock_archive_gate_row(&mut *connection).await?;
    let Some(session_id) = active_maintenance_session(&mut *connection).await? else {
        return Ok(TranscodePlanOutcome::Rejected(TranscodePlanRejected {
            component,
            reason: "archive maintenance is required".to_owned(),
            affected_rows: 0,
        }));
    };
    let active: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {TRANSCODE_JOBS} WHERE state <> 'COMPLETE'"
    ))
    .fetch_one(&mut *connection)
    .await?;
    if active != 0 {
        return Ok(TranscodePlanOutcome::Rejected(TranscodePlanRejected {
            component,
            reason: "another replacement job is active".to_owned(),
            affected_rows: active,
        }));
    }

    let corrupt = invalid_component_rows(
        &mut *connection,
        TASK_HISTORY_PARENT,
        component,
        source_version,
        source_codec,
    )
    .await?;
    if corrupt != 0 {
        return Ok(TranscodePlanOutcome::Rejected(TranscodePlanRejected {
            component,
            reason: "source rows fail component validity".to_owned(),
            affected_rows: corrupt,
        }));
    }

    let columns = component_columns(component);
    let inventory_sql = format!(
        r#"
        SELECT history.tableoid::oid::bigint AS relation_oid,
               child.relname AS relation_name,
               parent.oid::bigint AS parent_oid,
               parent.relname AS parent_name,
               pg_get_expr(child.relpartbound, child.oid) AS partition_bound,
               pg_get_partition_constraintdef(child.oid) AS partition_constraint,
               count(*) AS row_count,
               count(*) FILTER (
                   WHERE {version} = $1 AND {codec} = $2
                     AND ({presence})
               ) AS transformed_rows,
               count({payload}) FILTER (
                   WHERE {version} = $1 AND {codec} = $2
                     AND ({presence})
               ) AS payload_rows,
               COALESCE(sum(octet_length({payload})) FILTER (
                   WHERE {version} = $1 AND {codec} = $2
                     AND ({presence})
               ), 0)::bigint AS payload_bytes,
               pg_total_relation_size(history.tableoid)::bigint AS relation_bytes,
               count(DISTINCT task_id) AS distinct_task_ids
        FROM {TASK_HISTORY_PARENT} AS history
        JOIN pg_class AS child ON child.oid = history.tableoid
        JOIN pg_inherits AS inheritance ON inheritance.inhrelid = child.oid
        JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
        GROUP BY history.tableoid, child.relname, parent.oid,
                 parent.relname, child.oid
        HAVING count(*) FILTER (
            WHERE {version} = $1 AND {codec} = $2 AND ({presence})
        ) > 0
        ORDER BY child.relname
        "#,
        version = columns.version,
        codec = columns.codec,
        presence = columns.presence_predicate,
        payload = columns.payload,
    );
    let inventory: Vec<InventoryRow> = sqlx::query_as(&inventory_sql)
        .bind(source_version)
        .bind(source_codec)
        .fetch_all(&mut *connection)
        .await?;
    let duplicate_identities: i64 = inventory
        .iter()
        .map(|row| row.row_count - row.distinct_task_ids)
        .sum();
    if duplicate_identities != 0 {
        return Ok(TranscodePlanOutcome::Rejected(TranscodePlanRejected {
            component,
            reason: "source relations carry duplicate task identities".to_owned(),
            affected_rows: duplicate_identities,
        }));
    }

    let transformed_rows = checked_sum(inventory.iter().map(|row| row.transformed_rows))?;
    let copied_rows = checked_sum(inventory.iter().map(|row| row.row_count))?;
    let payload_rows = checked_sum(inventory.iter().map(|row| row.payload_rows))?;
    let payload_bytes = checked_sum(inventory.iter().map(|row| row.payload_bytes))?;
    let affected_relation_bytes = checked_sum(inventory.iter().map(|row| row.relation_bytes))?;
    let projected = if target_version > source_version {
        i128::from(payload_bytes) + i128::from(payload_rows) * 2
    } else {
        i128::from(payload_bytes) - i128::from(payload_rows) * 2
    };
    let projected_payload_bytes = i64::try_from(projected.max(0))
        .map_err(|_| TranscodeError::contract("projected payload bytes exceed bigint"))?;
    sqlx::query(&format!(
        r#"
        INSERT INTO {TRANSCODE_JOBS} (
            job_id, maintenance_session_id, component,
            source_version, target_version, source_codec, target_codec,
            state, transformed_rows, copied_rows_total,
            copied_rows_completed, payload_rows, payload_bytes_before,
            projected_payload_bytes, affected_relation_bytes,
            started_at, start_lsn
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'PLANNED',
            $8, $9, 0, $10, $11, $12, $13,
            statement_timestamp(), pg_current_wal_insert_lsn()
        )
        "#
    ))
    .bind(job_id)
    .bind(session_id)
    .bind(component.as_str())
    .bind(source_version)
    .bind(target_version)
    .bind(source_codec)
    .bind(target_codec)
    .bind(transformed_rows)
    .bind(copied_rows)
    .bind(payload_rows)
    .bind(payload_bytes)
    .bind(projected_payload_bytes)
    .bind(affected_relation_bytes)
    .execute(&mut *connection)
    .await?;

    for (index, row) in inventory.iter().enumerate() {
        let ordinal = i32::try_from(index + 1)
            .map_err(|_| TranscodeError::contract("too many transcode relations"))?;
        sqlx::query(&format!(
            r#"
            INSERT INTO {TRANSCODE_RELATIONS} (
                job_id, relation_ordinal, source_relation_oid,
                source_relation_name, parent_relation_oid,
                parent_relation_name, partition_bound,
                partition_constraint, replacement_relation_name,
                backup_relation_name, state, row_count,
                transformed_rows, rows_copied, relation_bytes
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                'PLANNED', $11, $12, 0, $13
            )
            "#
        ))
        .bind(job_id)
        .bind(ordinal)
        .bind(row.relation_oid)
        .bind(&row.relation_name)
        .bind(row.parent_oid)
        .bind(&row.parent_name)
        .bind(&row.partition_bound)
        .bind(&row.partition_constraint)
        .bind(replacement_relation_name(job_id, ordinal))
        .bind(backup_relation_name(job_id, ordinal))
        .bind(row.row_count)
        .bind(row.transformed_rows)
        .bind(row.relation_bytes)
        .execute(&mut *connection)
        .await?;
    }

    Ok(TranscodePlanOutcome::Planned(TranscodePlan {
        job_id,
        component,
        source_version,
        target_version,
        transformed_rows,
        copied_rows,
        payload_bytes,
        projected_payload_bytes,
        affected_relation_bytes,
        relation_count: inventory.len(),
        peak_additional_disk_budget_bytes: ratio_ceiling(affected_relation_bytes, 5, 4)?,
        wal_budget_bytes: ratio_ceiling(affected_relation_bytes, 3, 2)?,
        rollback_wal_budget_bytes: ratio_ceiling(affected_relation_bytes, 3, 2)?,
        rollback_peak_additional_disk_budget_bytes: ratio_ceiling(affected_relation_bytes, 5, 4)?,
        reversible: true,
    }))
}

pub async fn run_copy_batch(
    connection: &mut PgConnection,
    job_id: Uuid,
    batch_size: i64,
) -> Result<TranscodeCopyOutcome, TranscodeError> {
    if batch_size <= 0 {
        return Err(TranscodeError::InvalidArgument(
            "batch size must be positive".to_owned(),
        ));
    }
    lock_transcode_program(&mut *connection).await?;
    crate::core::history::maintenance::gate::lock_archive_gate_row(&mut *connection).await?;
    let job = lock_job(&mut *connection, job_id).await?;
    match job.state {
        TranscodeJobState::Planned | TranscodeJobState::Copying => {}
        TranscodeJobState::Copied => {
            return Ok(TranscodeCopyOutcome::Ready(TranscodeReadyForVerification {
                job_id,
                copied_rows_total: job.copied_rows_total,
            }));
        }
        _ => {
            return Err(TranscodeError::state(
                "replacement copy is not mutable in this job state",
            ));
        }
    }
    require_job_maintenance(&mut *connection, &job).await?;
    let relation: Option<TranscodeRelationRow> = sqlx::query_as(&format!(
        r#"
        SELECT job_id, relation_ordinal, source_relation_oid,
               source_relation_name, parent_relation_oid,
               parent_relation_name, partition_bound, partition_constraint,
               replacement_relation_name, replacement_relation_oid,
               backup_relation_name, state, row_count, transformed_rows,
               rows_copied, last_source_ctid::text AS last_source_ctid,
               source_mutation_generation, replacement_mutation_generation,
               verified_source_generation, verified_replacement_generation,
               verified_source_filenode, verified_replacement_filenode,
               verified_source_schema_signature,
               verified_replacement_schema_signature
        FROM {TRANSCODE_RELATIONS}
        WHERE job_id = $1 AND state IN ('PLANNED', 'COPYING')
        ORDER BY relation_ordinal LIMIT 1 FOR UPDATE
        "#
    ))
    .bind(job_id)
    .fetch_optional(&mut *connection)
    .await?;
    let Some(mut relation) = relation else {
        sqlx::query(&format!(
            "UPDATE {TRANSCODE_JOBS} SET state = 'COPIED', copied_at = statement_timestamp() WHERE job_id = $1"
        ))
        .bind(job_id)
        .execute(&mut *connection)
        .await?;
        return Ok(TranscodeCopyOutcome::Ready(TranscodeReadyForVerification {
            job_id,
            copied_rows_total: job.copied_rows_total,
        }));
    };
    lock_relation_leaf(&mut *connection, &relation).await?;

    if relation.parsed_state()? == TranscodeJobState::Planned {
        if let Some(rejection) =
            prepare_replacement_relation(&mut *connection, &job, &relation).await?
        {
            return Ok(TranscodeCopyOutcome::Rejected(rejection));
        }
        relation = relation_for_update(&mut *connection, job_id, relation.relation_ordinal).await?;
    }

    let source = quoted_identifier(&relation.source_relation_name)?;
    let replacement = quoted_identifier(&relation.replacement_relation_name)?;
    let columns = relation_columns(&mut *connection, relation.source_relation_oid).await?;
    let insert_sql = format!(
        r#"
        WITH source_batch AS MATERIALIZED (
            SELECT ctid AS source_ctid, source_table.*
            FROM {source} AS source_table
            WHERE ($1::tid IS NULL OR ctid > $1::tid)
            ORDER BY ctid LIMIT $2
        ), encoded AS MATERIALIZED (
            SELECT {encoded}
            FROM source_batch AS source
        ), inserted AS (
            INSERT INTO {replacement} ({column_list})
            SELECT {transformed}
            FROM encoded AS source
            RETURNING task_id
        )
        SELECT count(*)::bigint AS rows_copied,
               (SELECT source_ctid::text FROM source_batch
                ORDER BY source_batch.source_ctid DESC LIMIT 1) AS last_source_ctid
        FROM inserted
        "#,
        encoded = encoded_source_select(
            job.component,
            "source",
            job.source_version,
            &job.source_codec,
            job.target_version > job.source_version,
        )?,
        column_list = column_list(&columns)?,
        transformed = transformed_select(
            &columns,
            job.component,
            job.source_version,
            &job.source_codec,
            job.target_version,
            &job.target_codec,
            "source",
        )?,
    );
    let inserted = sqlx::query(&insert_sql)
        .bind(relation.last_source_ctid.as_deref())
        .bind(batch_size)
        .fetch_one(&mut *connection)
        .await?;
    let inserted_rows: i64 = inserted.try_get("rows_copied")?;
    let last_source_ctid: Option<String> = inserted.try_get("last_source_ctid")?;
    let rows_copied = relation
        .rows_copied
        .checked_add(inserted_rows)
        .ok_or_else(|| TranscodeError::contract("relation copied rows exceed bigint"))?;
    if (inserted_rows == 0 && relation.rows_copied < relation.row_count)
        || rows_copied > relation.row_count
    {
        return Ok(TranscodeCopyOutcome::Rejected(TranscodeCopyRejected {
            job_id,
            relation_ordinal: relation.relation_ordinal,
            kind: TranscodeCopyRejectionKind::SourceSetChanged,
            observed_rows: rows_copied,
        }));
    }
    let relation_complete = rows_copied == relation.row_count;
    if relation_complete {
        sqlx::query(&format!(
            "CREATE INDEX {} ON {replacement} (task_id)",
            quoted_identifier(&replacement_index_name(job_id, relation.relation_ordinal))?
        ))
        .execute(&mut *connection)
        .await?;
        sqlx::query(&format!(
            "CREATE INDEX {} ON {replacement} (enqueued_at)",
            quoted_identifier(&replacement_ordering_index_name(
                job_id,
                relation.relation_ordinal,
            ))?
        ))
        .execute(&mut *connection)
        .await?;
    }
    let batch_number: i32 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(max(batch_number), 0) + 1 FROM {TRANSCODE_BATCHES} WHERE job_id = $1"
    ))
    .bind(job_id)
    .fetch_one(&mut *connection)
    .await?;
    let inserted_rows_i32 = i32::try_from(inserted_rows)
        .map_err(|_| TranscodeError::contract("copy batch row count exceeds integer"))?;
    sqlx::query(&format!(
        r#"
        INSERT INTO {TRANSCODE_BATCHES} (
            job_id, batch_number, relation_ordinal, rows_copied, committed_at
        ) VALUES ($1, $2, $3, $4, statement_timestamp())
        "#
    ))
    .bind(job_id)
    .bind(batch_number)
    .bind(relation.relation_ordinal)
    .bind(inserted_rows_i32)
    .execute(&mut *connection)
    .await?;
    let relation_state = if relation_complete {
        "COPIED"
    } else {
        "COPYING"
    };
    sqlx::query(&format!(
        r#"
        UPDATE {TRANSCODE_RELATIONS}
        SET state = $1, rows_copied = $2, last_source_ctid = $3::tid,
            copied_at = CASE WHEN $1 = 'COPIED'
                             THEN statement_timestamp() ELSE copied_at END
        WHERE job_id = $4 AND relation_ordinal = $5
        "#
    ))
    .bind(relation_state)
    .bind(rows_copied)
    .bind(last_source_ctid.as_deref())
    .bind(job_id)
    .bind(relation.relation_ordinal)
    .execute(&mut *connection)
    .await?;
    let completed = job
        .copied_rows_completed
        .checked_add(inserted_rows)
        .ok_or_else(|| TranscodeError::contract("job copied rows exceed bigint"))?;
    let all_copied = completed == job.copied_rows_total;
    let job_state = if all_copied { "COPIED" } else { "COPYING" };
    sqlx::query(&format!(
        r#"
        UPDATE {TRANSCODE_JOBS}
        SET state = $1, copied_rows_completed = $2,
            last_batch_at = statement_timestamp(),
            copied_at = CASE WHEN $1 = 'COPIED'
                             THEN statement_timestamp() ELSE copied_at END
        WHERE job_id = $3
        "#
    ))
    .bind(job_state)
    .bind(completed)
    .bind(job_id)
    .execute(&mut *connection)
    .await?;
    Ok(TranscodeCopyOutcome::Batch(TranscodeCopyBatch {
        job_id,
        relation_ordinal: relation.relation_ordinal,
        batch_number,
        rows_copied: inserted_rows_i32,
        copied_rows_completed: completed,
        copied_rows_total: job.copied_rows_total,
    }))
}

pub async fn verify_transcode(
    connection: &mut PgConnection,
    job_id: Uuid,
) -> Result<TranscodeVerification, TranscodeError> {
    lock_transcode_program(&mut *connection).await?;
    crate::core::history::maintenance::gate::lock_archive_gate_row(&mut *connection).await?;
    let job = lock_job(&mut *connection, job_id).await?;
    if !matches!(
        job.state,
        TranscodeJobState::Copied | TranscodeJobState::Verified
    ) {
        return Err(TranscodeError::state(
            "replacement relations are not ready for verification",
        ));
    }
    require_job_maintenance(&mut *connection, &job).await?;

    let mut changed = 0_i64;
    let mut mismatches = 0_i64;
    let mut invalid_targets = 0_i64;
    for relation in job_relations(&mut *connection, job_id).await? {
        lock_relation_leaf(&mut *connection, &relation).await?;
        let initial_token = verification_token(&mut *connection, &relation, false).await?;
        if initial_token.is_none() || !bindings_match(&mut *connection, &relation, false).await? {
            changed += 1;
            clear_verification(&mut *connection, &relation).await?;
            continue;
        }
        let source = quoted_identifier(&relation.source_relation_name)?;
        let observed: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {source}"))
            .fetch_one(&mut *connection)
            .await?;
        if observed != relation.row_count {
            changed += 1;
            clear_verification(&mut *connection, &relation).await?;
            continue;
        }
        let columns = relation_columns(&mut *connection, relation.source_relation_oid).await?;
        let mismatch = mismatch_count(
            &mut *connection,
            &relation,
            &columns,
            job.component,
            job.source_version,
            &job.source_codec,
            job.target_version,
            &job.target_codec,
        )
        .await?;
        mismatches += mismatch;
        let relation_invalid = if mismatch == 0 {
            0
        } else {
            invalid_component_rows(
                &mut *connection,
                &quoted_identifier(&relation.replacement_relation_name)?,
                job.component,
                job.target_version,
                &job.target_codec,
            )
            .await?
        };
        invalid_targets += relation_invalid;
        let final_token = verification_token(&mut *connection, &relation, true).await?;
        let stable = final_token.as_ref() == initial_token.as_ref();
        if !stable {
            changed += 1;
        }
        match final_token {
            Some(token) if mismatch == 0 && relation_invalid == 0 && stable => {
                record_verification(&mut *connection, &relation, &token).await?;
            }
            _ => clear_verification(&mut *connection, &relation).await?,
        }
    }
    let verified = changed == 0 && mismatches == 0 && invalid_targets == 0;
    sqlx::query(&format!(
        r#"
        UPDATE {TRANSCODE_JOBS}
        SET state = $1,
            verified_at = CASE WHEN $2 THEN statement_timestamp() ELSE NULL END
        WHERE job_id = $3
        "#
    ))
    .bind(if verified { "VERIFIED" } else { "COPIED" })
    .bind(verified)
    .bind(job_id)
    .execute(&mut *connection)
    .await?;
    Ok(TranscodeVerification {
        job_id,
        verified,
        source_relations_changed: changed,
        replacement_row_mismatches: mismatches,
        invalid_target_rows: invalid_targets,
        copied_rows_total: job.copied_rows_total,
        wal_bytes: None,
    })
}

pub async fn swap_transcode(
    connection: &mut PgConnection,
    job_id: Uuid,
) -> Result<TranscodeSwapOutcome, TranscodeError> {
    lock_transcode_program(&mut *connection).await?;
    crate::core::history::maintenance::gate::lock_archive_gate_row(&mut *connection).await?;
    let job = lock_job(&mut *connection, job_id).await?;
    match job.state {
        TranscodeJobState::Swapped | TranscodeJobState::Complete => {
            return Ok(TranscodeSwapOutcome::Swapped(TranscodeSwap {
                job_id,
                relations_swapped: usize::try_from(job.relation_count).unwrap_or(usize::MAX),
            }));
        }
        TranscodeJobState::Verified => {}
        _ => {
            return Err(TranscodeError::state(
                "replacement relations must be verified before binding swap",
            ));
        }
    }
    require_job_maintenance(&mut *connection, &job).await?;
    let relations = job_relations(&mut *connection, job_id).await?;
    for relation in &relations {
        lock_relation_leaf(&mut *connection, relation).await?;
    }
    if let Some(busy) = try_swap_locks(&mut *connection, job_id, &relations).await? {
        return Ok(TranscodeSwapOutcome::Busy(busy));
    }
    for relation in &relations {
        if !verified_token_matches(&mut *connection, relation).await? {
            return Err(TranscodeError::state(
                "replacement verification changed before binding swap",
            ));
        }
        if !catalog_attachment_holds(&mut *connection, relation).await? {
            return Err(TranscodeError::state(
                "leaf catalog attachment changed before binding swap",
            ));
        }
    }
    for relation in &relations {
        let source = quoted_identifier(&relation.source_relation_name)?;
        let parent = quoted_identifier(&relation.parent_relation_name)?;
        let replacement = quoted_identifier(&relation.replacement_relation_name)?;
        let backup = quoted_identifier(&relation.backup_relation_name)?;
        sqlx::query(&format!("ALTER TABLE {parent} DETACH PARTITION {source}"))
            .execute(&mut *connection)
            .await?;
        sqlx::query(&format!("ALTER TABLE {source} RENAME TO {backup}"))
            .execute(&mut *connection)
            .await?;
        sqlx::query(&format!("ALTER TABLE {replacement} RENAME TO {source}"))
            .execute(&mut *connection)
            .await?;
        sqlx::query(&format!(
            "ALTER TABLE {parent} ATTACH PARTITION {source} {}",
            relation.partition_bound
        ))
        .execute(&mut *connection)
        .await?;
        sqlx::query(&format!(
            "UPDATE {TRANSCODE_RELATIONS} SET state = 'SWAPPED', swapped_at = statement_timestamp() WHERE job_id = $1 AND relation_ordinal = $2"
        ))
        .bind(job_id)
        .bind(relation.relation_ordinal)
        .execute(&mut *connection)
        .await?;
    }
    sqlx::query(&format!(
        "UPDATE {TRANSCODE_JOBS} SET state = 'SWAPPED', swapped_at = statement_timestamp() WHERE job_id = $1"
    ))
    .bind(job_id)
    .execute(&mut *connection)
    .await?;
    Ok(TranscodeSwapOutcome::Swapped(TranscodeSwap {
        job_id,
        relations_swapped: relations.len(),
    }))
}

pub async fn swap_with_retries(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<TranscodeSwapOutcome, TranscodeError> {
    swap_with_retry_policy(
        pool,
        job_id,
        SWAP_LOCK_ATTEMPTS_MAXIMUM,
        Duration::from_secs_f64(SWAP_RETRY_BACKOFF_SECONDS),
    )
    .await
}

pub(super) async fn swap_with_retry_policy(
    pool: &PgPool,
    job_id: Uuid,
    maximum_attempts: u32,
    backoff: Duration,
) -> Result<TranscodeSwapOutcome, TranscodeError> {
    if maximum_attempts == 0 {
        return Err(TranscodeError::InvalidArgument(
            "swap retry attempts must be positive".to_owned(),
        ));
    }
    let mut attempts = 0_u32;
    let mut last_busy = None;
    while attempts < maximum_attempts {
        attempts += 1;
        let mut transaction = pool.begin().await?;
        let outcome = swap_transcode(&mut transaction, job_id).await?;
        transaction.commit().await?;
        match outcome {
            TranscodeSwapOutcome::Busy(busy) => {
                last_busy = Some(busy);
                if attempts < maximum_attempts {
                    tokio::time::sleep(backoff).await;
                }
            }
            other => return Ok(other),
        }
    }
    let last_busy = last_busy
        .ok_or_else(|| TranscodeError::contract("swap retry exhaustion has no busy attempt"))?;
    let blockers = match pool.acquire().await {
        Ok(mut connection) => capture_swap_blockers(
            &mut connection,
            last_busy.lock_mode,
            &last_busy.relation_names,
        )
        .await
        .ok(),
        Err(_) => None,
    };
    Ok(TranscodeSwapOutcome::Exhausted(build_swap_exhausted(
        job_id, last_busy, attempts, backoff, blockers,
    )))
}

pub(super) fn build_swap_exhausted(
    job_id: Uuid,
    busy: TranscodeSwapBusy,
    attempts: u32,
    backoff: Duration,
    blockers: Option<Vec<SwapBlocker>>,
) -> TranscodeSwapExhausted {
    let blocker_capture_failed = blockers.is_none();
    TranscodeSwapExhausted {
        job_id,
        lock_mode: busy.lock_mode,
        relation_names: busy.relation_names,
        attempts,
        retry_sleep_seconds: backoff.as_secs_f64() * f64::from(attempts.saturating_sub(1)),
        blockers: blockers.unwrap_or_default(),
        blocker_capture_failed,
    }
}

pub async fn finalize_transcode(
    connection: &mut PgConnection,
    job_id: Uuid,
) -> Result<TranscodeFinalized, TranscodeError> {
    lock_transcode_program(&mut *connection).await?;
    crate::core::history::maintenance::gate::lock_archive_gate_row(&mut *connection).await?;
    let job = lock_job(&mut *connection, job_id).await?;
    if job.state == TranscodeJobState::Complete {
        return Ok(TranscodeFinalized {
            job_id,
            retired_source_version: job.source_version,
            decoder_retirement_ready: true,
        });
    }
    if job.state != TranscodeJobState::Swapped {
        return Err(TranscodeError::state(
            "replacement partitions have not been swapped",
        ));
    }
    require_job_maintenance(&mut *connection, &job).await?;
    let columns = component_columns(job.component);
    let remaining: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {TASK_HISTORY_PARENT} WHERE {} = $1 AND ({})",
        columns.version, columns.presence_predicate
    ))
    .bind(job.source_version)
    .fetch_one(&mut *connection)
    .await?;
    if remaining != 0 {
        return Err(TranscodeError::state(format!(
            "{remaining} source-version rows remain after swap"
        )));
    }
    for relation in job_relations(&mut *connection, job_id).await? {
        let source = quoted_identifier(&relation.source_relation_name)?;
        let backup = quoted_identifier(&relation.backup_relation_name)?;
        sqlx::query(&format!(
            "DROP TRIGGER archive_replacement_target_guard ON {source}"
        ))
        .execute(&mut *connection)
        .await?;
        sqlx::query(&format!("DROP TABLE {backup}"))
            .execute(&mut *connection)
            .await?;
        sqlx::query(&format!(
            "UPDATE {TRANSCODE_RELATIONS} SET state = 'COMPLETE', completed_at = statement_timestamp() WHERE job_id = $1 AND relation_ordinal = $2"
        ))
        .bind(job_id)
        .bind(relation.relation_ordinal)
        .execute(&mut *connection)
        .await?;
    }
    let wal_bytes: i64 = sqlx::query_scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::bigint",
    )
    .bind(&job.start_lsn)
    .fetch_one(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "UPDATE {TRANSCODE_JOBS} SET state = 'COMPLETE', completed_at = statement_timestamp(), wal_bytes = $1 WHERE job_id = $2"
    ))
    .bind(wal_bytes)
    .bind(job_id)
    .execute(&mut *connection)
    .await?;
    Ok(TranscodeFinalized {
        job_id,
        retired_source_version: job.source_version,
        decoder_retirement_ready: true,
    })
}

fn checked_sum(mut values: impl Iterator<Item = i64>) -> Result<i64, TranscodeError> {
    values.try_fold(0_i64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| TranscodeError::contract("transcode inventory exceeds bigint"))
    })
}

fn ratio_ceiling(value: i64, numerator: i64, denominator: i64) -> Result<i64, TranscodeError> {
    let rounded = (i128::from(value) * i128::from(numerator) + i128::from(denominator - 1))
        / i128::from(denominator);
    i64::try_from(rounded).map_err(|_| TranscodeError::contract("transcode budget exceeds bigint"))
}

fn digest_column(component: ArchiveComponent) -> &'static str {
    match component {
        ArchiveComponent::HistoryRow => "NULL::bytea",
        ArchiveComponent::Result => "result_digest",
        ArchiveComponent::Attempts => "attempt_snapshot_digest",
        ArchiveComponent::RerunInput => "rerun_input_digest",
    }
}

async fn invalid_component_rows(
    connection: &mut PgConnection,
    relation: &str,
    component: ArchiveComponent,
    version: i16,
    codec: &str,
) -> Result<i64, TranscodeError> {
    let columns = component_columns(component);
    if columns.metadata_only {
        return Ok(0);
    }
    let expected_codec = if version == 1 { JSON_UTF8_CODEC } else { codec };
    Ok(sqlx::query_scalar(&format!(
        r#"
        SELECT count(*) FROM {relation}
        WHERE {version_column} = $1 AND ({presence})
          AND ({codec_column} <> $2
               OR sha256({payload}) IS DISTINCT FROM {digest})
        "#,
        version_column = columns.version,
        presence = columns.presence_predicate,
        codec_column = columns.codec,
        payload = columns.payload,
        digest = digest_column(component),
    ))
    .bind(version)
    .bind(expected_codec)
    .fetch_one(connection)
    .await?)
}

async fn require_job_maintenance(
    connection: &mut PgConnection,
    job: &TranscodeJobRow,
) -> Result<(), TranscodeError> {
    if active_maintenance_session(connection).await? != Some(job.maintenance_session_id) {
        return Err(TranscodeError::state(
            "the job's maintenance session is not active",
        ));
    }
    Ok(())
}

async fn relation_for_update(
    connection: &mut PgConnection,
    job_id: Uuid,
    ordinal: i32,
) -> Result<TranscodeRelationRow, TranscodeError> {
    sqlx::query_as(&format!(
        r#"
        SELECT job_id, relation_ordinal, source_relation_oid,
               source_relation_name, parent_relation_oid,
               parent_relation_name, partition_bound, partition_constraint,
               replacement_relation_name, replacement_relation_oid,
               backup_relation_name, state, row_count, transformed_rows,
               rows_copied, last_source_ctid::text AS last_source_ctid,
               source_mutation_generation, replacement_mutation_generation,
               verified_source_generation, verified_replacement_generation,
               verified_source_filenode, verified_replacement_filenode,
               verified_source_schema_signature,
               verified_replacement_schema_signature
        FROM {TRANSCODE_RELATIONS}
        WHERE job_id = $1 AND relation_ordinal = $2 FOR UPDATE
        "#
    ))
    .bind(job_id)
    .bind(ordinal)
    .fetch_one(connection)
    .await
    .map_err(Into::into)
}

async fn lock_relation_leaf(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
) -> Result<(), TranscodeError> {
    let row: Option<(String, DateTime<Utc>)> = sqlx::query_as(&format!(
        "SELECT class_key, lower_anchor FROM {LEAF_CATALOG} WHERE leaf_name = $1"
    ))
    .bind(&relation.source_relation_name)
    .fetch_optional(&mut *connection)
    .await?;
    let (class_key, lower_anchor) = row.ok_or_else(|| {
        TranscodeError::state(format!(
            "source relation {:?} has no leaf catalog row",
            relation.source_relation_name
        ))
    })?;
    lock_leaf_for_transaction(connection, &class_key, lower_anchor).await?;
    Ok(())
}

async fn catalog_attachment_holds(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
) -> Result<bool, TranscodeError> {
    Ok(sqlx::query_scalar::<_, bool>(&format!(
        "SELECT detached_at IS NULL AND dropped_at IS NULL FROM {LEAF_CATALOG} WHERE leaf_name = $1"
    ))
    .bind(&relation.source_relation_name)
    .fetch_optional(connection)
    .await?
    .unwrap_or(false))
}

async fn prepare_replacement_relation(
    connection: &mut PgConnection,
    job: &TranscodeJobRow,
    relation: &TranscodeRelationRow,
) -> Result<Option<TranscodeCopyRejected>, TranscodeError> {
    if !bindings_match(&mut *connection, relation, true).await? {
        return Ok(Some(TranscodeCopyRejected {
            job_id: job.job_id,
            relation_ordinal: relation.relation_ordinal,
            kind: TranscodeCopyRejectionKind::SourceSetChanged,
            observed_rows: 0,
        }));
    }
    let source = quoted_identifier(&relation.source_relation_name)?;
    let observed = sqlx::query(&format!(
        "SELECT count(*) AS row_count, count(DISTINCT task_id) AS distinct_task_ids FROM {source}"
    ))
    .fetch_one(&mut *connection)
    .await?;
    let row_count: i64 = observed.try_get("row_count")?;
    let distinct_task_ids: i64 = observed.try_get("distinct_task_ids")?;
    if row_count != relation.row_count || distinct_task_ids != relation.row_count {
        return Ok(Some(TranscodeCopyRejected {
            job_id: job.job_id,
            relation_ordinal: relation.relation_ordinal,
            kind: TranscodeCopyRejectionKind::SourceSetChanged,
            observed_rows: row_count,
        }));
    }
    let invalid = invalid_component_rows(
        &mut *connection,
        &source,
        job.component,
        job.source_version,
        &job.source_codec,
    )
    .await?;
    if invalid != 0 {
        return Ok(Some(TranscodeCopyRejected {
            job_id: job.job_id,
            relation_ordinal: relation.relation_ordinal,
            kind: TranscodeCopyRejectionKind::SourceCorrupt,
            observed_rows: invalid,
        }));
    }
    for trigger in [
        format!(
            "CREATE TRIGGER archive_replacement_source_row_guard AFTER INSERT OR UPDATE OR DELETE ON {source} FOR EACH ROW EXECUTE FUNCTION {TRANSCODE_MUTATION_FUNCTION}()"
        ),
        format!(
            "CREATE TRIGGER archive_replacement_source_truncate_guard AFTER TRUNCATE ON {source} FOR EACH STATEMENT EXECUTE FUNCTION {TRANSCODE_MUTATION_FUNCTION}()"
        ),
    ] {
        sqlx::query(&trigger).execute(&mut *connection).await?;
    }
    let replacement = quoted_identifier(&relation.replacement_relation_name)?;
    sqlx::query(&format!(
        "CREATE TABLE {replacement} (LIKE {source} INCLUDING ALL EXCLUDING INDEXES)"
    ))
    .execute(&mut *connection)
    .await?;
    let replacement_oid: i64 = sqlx::query_scalar("SELECT $1::regclass::oid::bigint")
        .bind(&relation.replacement_relation_name)
        .fetch_one(&mut *connection)
        .await?;
    sqlx::query(&format!(
        "UPDATE {TRANSCODE_RELATIONS} SET replacement_relation_oid = $1 WHERE job_id = $2 AND relation_ordinal = $3"
    ))
    .bind(replacement_oid)
    .bind(job.job_id)
    .bind(relation.relation_ordinal)
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "CREATE TRIGGER archive_replacement_target_guard AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {replacement} FOR EACH STATEMENT EXECUTE FUNCTION {TRANSCODE_MUTATION_FUNCTION}()"
    ))
    .execute(&mut *connection)
    .await?;
    let bound_name = quoted_identifier(&replacement_bound_name(
        job.job_id,
        relation.relation_ordinal,
    ))?;
    sqlx::query(&format!(
        "ALTER TABLE {replacement} ADD CONSTRAINT {bound_name} CHECK ({})",
        relation.partition_constraint
    ))
    .execute(&mut *connection)
    .await?;
    sqlx::query(&format!(
        "UPDATE {TRANSCODE_RELATIONS} SET state = 'COPYING', prepared_at = statement_timestamp() WHERE job_id = $1 AND relation_ordinal = $2"
    ))
    .bind(job.job_id)
    .bind(relation.relation_ordinal)
    .execute(&mut *connection)
    .await?;
    Ok(None)
}

async fn relation_columns(
    connection: &mut PgConnection,
    relation_oid: i64,
) -> Result<Vec<String>, TranscodeError> {
    let columns: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT attname FROM pg_attribute
        WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
        "#,
    )
    .bind(relation_oid)
    .fetch_all(connection)
    .await?;
    if columns.is_empty() {
        return Err(TranscodeError::state(
            "replacement source relation has no visible columns",
        ));
    }
    Ok(columns)
}

async fn bindings_match(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
    source_only: bool,
) -> Result<bool, TranscodeError> {
    let source_ok: bool = sqlx::query_scalar(
        r#"
        SELECT to_regclass($1)::oid::bigint = $2
           AND EXISTS (
               SELECT 1 FROM pg_inherits
               WHERE inhrelid = $2::oid AND inhparent = $3::oid
           )
        "#,
    )
    .bind(&relation.source_relation_name)
    .bind(relation.source_relation_oid)
    .bind(relation.parent_relation_oid)
    .fetch_one(&mut *connection)
    .await?;
    if !source_ok {
        return Ok(false);
    }
    if source_only || relation.replacement_relation_oid.is_none() {
        return Ok(true);
    }
    Ok(
        sqlx::query_scalar("SELECT to_regclass($1)::oid::bigint = $2")
            .bind(&relation.replacement_relation_name)
            .bind(relation.replacement_relation_oid)
            .fetch_one(connection)
            .await?,
    )
}

#[derive(FromRow)]
struct VerificationFacts {
    source_mutation_generation: i64,
    replacement_mutation_generation: i64,
    source_filenode: Option<i64>,
    replacement_filenode: Option<i64>,
}

async fn verification_token(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
    lock_record: bool,
) -> Result<Option<RelationVerificationToken>, TranscodeError> {
    let Some(replacement_oid) = relation.replacement_relation_oid else {
        return Ok(None);
    };
    let lock_clause = if lock_record { "FOR UPDATE" } else { "" };
    let row: Option<VerificationFacts> = sqlx::query_as(&format!(
        r#"
        SELECT source_mutation_generation, replacement_mutation_generation,
               pg_relation_filenode(source_relation_oid::oid)::bigint AS source_filenode,
               pg_relation_filenode(replacement_relation_oid::oid)::bigint AS replacement_filenode
        FROM {TRANSCODE_RELATIONS}
        WHERE job_id = $1 AND relation_ordinal = $2 {lock_clause}
        "#
    ))
    .bind(relation.job_id)
    .bind(relation.relation_ordinal)
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let (Some(source_filenode), Some(replacement_filenode)) =
        (row.source_filenode, row.replacement_filenode)
    else {
        return Ok(None);
    };
    let source_schema_signature =
        relation_schema_signature(&mut *connection, relation.source_relation_oid).await?;
    let replacement_schema_signature =
        relation_schema_signature(&mut *connection, replacement_oid).await?;
    let (Some(source_schema_signature), Some(replacement_schema_signature)) =
        (source_schema_signature, replacement_schema_signature)
    else {
        return Ok(None);
    };
    Ok(Some(RelationVerificationToken {
        source_generation: row.source_mutation_generation,
        replacement_generation: row.replacement_mutation_generation,
        source_filenode,
        replacement_filenode,
        source_schema_signature,
        replacement_schema_signature,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn mismatch_count(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
    columns: &[String],
    component: ArchiveComponent,
    source_version: i16,
    source_codec: &str,
    target_version: i16,
    target_codec: &str,
) -> Result<i64, TranscodeError> {
    let source = quoted_identifier(&relation.source_relation_name)?;
    let replacement = quoted_identifier(&relation.replacement_relation_name)?;
    let encoded = encoded_source_select(
        component,
        "source",
        source_version,
        source_codec,
        target_version > source_version,
    )?;
    let expected = transformed_select(
        columns,
        component,
        source_version,
        source_codec,
        target_version,
        target_codec,
        "source",
    )?;
    let expected_columns = columns
        .iter()
        .map(|column| Ok(format!("expected.{}", quoted_identifier(column)?)))
        .collect::<Result<Vec<_>, TranscodeError>>()?
        .join(", ");
    let replacement_columns = columns
        .iter()
        .map(|column| Ok(format!("replacement.{}", quoted_identifier(column)?)))
        .collect::<Result<Vec<_>, TranscodeError>>()?
        .join(", ");
    Ok(sqlx::query_scalar(&format!(
        r#"
        WITH encoded AS MATERIALIZED (
            SELECT {encoded} FROM {source} AS source
        ), expected ({column_list}) AS MATERIALIZED (
            SELECT {expected} FROM encoded AS source
        )
        SELECT count(*)
        FROM expected
        FULL OUTER JOIN {replacement} AS replacement USING (task_id)
        WHERE expected.task_id IS NULL OR replacement.task_id IS NULL
           OR ROW({expected_columns}) IS DISTINCT FROM ROW({replacement_columns})
        "#,
        column_list = column_list(columns)?,
    ))
    .fetch_one(connection)
    .await?)
}

async fn record_verification(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
    token: &RelationVerificationToken,
) -> Result<(), TranscodeError> {
    sqlx::query(&format!(
        r#"
        UPDATE {TRANSCODE_RELATIONS}
        SET state = 'VERIFIED', verified_at = statement_timestamp(),
            verified_source_generation = $1,
            verified_replacement_generation = $2,
            verified_source_filenode = $3,
            verified_replacement_filenode = $4,
            verified_source_schema_signature = $5,
            verified_replacement_schema_signature = $6
        WHERE job_id = $7 AND relation_ordinal = $8
        "#
    ))
    .bind(token.source_generation)
    .bind(token.replacement_generation)
    .bind(token.source_filenode)
    .bind(token.replacement_filenode)
    .bind(&token.source_schema_signature)
    .bind(&token.replacement_schema_signature)
    .bind(relation.job_id)
    .bind(relation.relation_ordinal)
    .execute(connection)
    .await?;
    Ok(())
}

async fn clear_verification(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
) -> Result<(), TranscodeError> {
    sqlx::query(&format!(
        r#"
        UPDATE {TRANSCODE_RELATIONS}
        SET state = CASE WHEN state = 'VERIFIED' THEN 'COPIED' ELSE state END,
            verified_at = NULL,
            verified_source_generation = NULL,
            verified_replacement_generation = NULL,
            verified_source_filenode = NULL,
            verified_replacement_filenode = NULL,
            verified_source_schema_signature = NULL,
            verified_replacement_schema_signature = NULL
        WHERE job_id = $1 AND relation_ordinal = $2
        "#
    ))
    .bind(relation.job_id)
    .bind(relation.relation_ordinal)
    .execute(connection)
    .await?;
    Ok(())
}

async fn verified_token_matches(
    connection: &mut PgConnection,
    relation: &TranscodeRelationRow,
) -> Result<bool, TranscodeError> {
    let Some(current) = verification_token(connection, relation, false).await? else {
        return Ok(false);
    };
    Ok(
        Some(current.source_generation) == relation.verified_source_generation
            && Some(current.replacement_generation) == relation.verified_replacement_generation
            && Some(current.source_filenode) == relation.verified_source_filenode
            && Some(current.replacement_filenode) == relation.verified_replacement_filenode
            && Some(current.source_schema_signature.as_str())
                == relation.verified_source_schema_signature.as_deref()
            && Some(current.replacement_schema_signature.as_str())
                == relation.verified_replacement_schema_signature.as_deref(),
    )
}

async fn try_swap_locks(
    connection: &mut PgConnection,
    job_id: Uuid,
    relations: &[TranscodeRelationRow],
) -> Result<Option<TranscodeSwapBusy>, TranscodeError> {
    let parent_names: BTreeSet<&str> = relations
        .iter()
        .map(|relation| relation.parent_relation_name.as_str())
        .collect();
    sqlx::query("SAVEPOINT horsies_transcode_swap_locks")
        .execute(&mut *connection)
        .await?;
    for parent_name in parent_names {
        let statement = format!(
            "LOCK TABLE {} IN ACCESS EXCLUSIVE MODE NOWAIT",
            quoted_identifier(parent_name)?
        );
        if let Err(error) = sqlx::query(&statement).execute(&mut *connection).await {
            rollback_swap_savepoint(&mut *connection).await?;
            if is_lock_not_available(&error) {
                return Ok(Some(TranscodeSwapBusy {
                    job_id,
                    lock_mode: SwapLockMode::Parent,
                    relation_names: vec![parent_name.to_owned()],
                }));
            }
            return Err(error.into());
        }
    }
    for relation in relations {
        let names = vec![
            relation.source_relation_name.clone(),
            relation.replacement_relation_name.clone(),
        ];
        let statement = format!(
            "LOCK TABLE {}, {} IN SHARE MODE NOWAIT",
            quoted_identifier(&names[0])?,
            quoted_identifier(&names[1])?,
        );
        if let Err(error) = sqlx::query(&statement).execute(&mut *connection).await {
            rollback_swap_savepoint(&mut *connection).await?;
            if is_lock_not_available(&error) {
                return Ok(Some(TranscodeSwapBusy {
                    job_id,
                    lock_mode: SwapLockMode::Leaves,
                    relation_names: names,
                }));
            }
            return Err(error.into());
        }
    }
    sqlx::query("RELEASE SAVEPOINT horsies_transcode_swap_locks")
        .execute(connection)
        .await?;
    Ok(None)
}

async fn rollback_swap_savepoint(connection: &mut PgConnection) -> Result<(), TranscodeError> {
    sqlx::query("ROLLBACK TO SAVEPOINT horsies_transcode_swap_locks")
        .execute(&mut *connection)
        .await?;
    sqlx::query("RELEASE SAVEPOINT horsies_transcode_swap_locks")
        .execute(connection)
        .await?;
    Ok(())
}

fn is_lock_not_available(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .as_deref()
        == Some("55P03")
}

#[derive(FromRow)]
struct BlockerRow {
    pid: i32,
    state: Option<String>,
    transaction_age_seconds: Option<f64>,
    wait_event: Option<String>,
    query: Option<String>,
    relation_name: String,
    held_lock_mode: String,
    granted: bool,
}

async fn capture_swap_blockers(
    connection: &mut PgConnection,
    lock_mode: SwapLockMode,
    relation_names: &[String],
) -> Result<Vec<SwapBlocker>, TranscodeError> {
    sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(&mut *connection)
        .await?;
    let rows: Vec<BlockerRow> = sqlx::query_as(&format!(
        r#"
        WITH requested AS (
            SELECT relation_name, to_regclass(relation_name)::oid AS relation_oid
            FROM unnest($1::text[]) AS names(relation_name)
        )
        SELECT locks.pid, activity.state,
               EXTRACT(EPOCH FROM clock_timestamp() - activity.xact_start)::double precision
                   AS transaction_age_seconds,
               activity.wait_event,
               LEFT(activity.query, {BLOCKER_QUERY_TRUNCATION_CHARS}) AS query,
               requested.relation_name,
               locks.mode AS held_lock_mode,
               locks.granted
        FROM requested
        JOIN pg_locks AS locks
          ON locks.locktype = 'relation' AND locks.relation = requested.relation_oid
        JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
        WHERE locks.pid <> pg_backend_pid() AND locks.granted
          AND ($2::text = 'ACCESS_EXCLUSIVE'
               OR locks.mode = ANY($3::text[]))
        ORDER BY locks.pid, requested.relation_name, locks.mode
        "#
    ))
    .bind(relation_names)
    .bind(lock_mode.as_str())
    .bind(
        &[
            "RowExclusiveLock",
            "ShareUpdateExclusiveLock",
            "ShareRowExclusiveLock",
            "ExclusiveLock",
            "AccessExclusiveLock",
        ][..],
    )
    .fetch_all(connection)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| SwapBlocker {
            pid: row.pid,
            state: row.state,
            transaction_age_seconds: row.transaction_age_seconds,
            wait_event: row.wait_event,
            query: row.query,
            relation_name: row.relation_name,
            held_lock_mode: row.held_lock_mode,
            granted: row.granted,
        })
        .collect())
}
