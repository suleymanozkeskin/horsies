//! Strict row types for the migration-owned transcode state program.

use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use super::outcomes::{ArchiveComponent, TranscodeJobState};
use super::TranscodeError;

pub const TRANSCODE_JOBS: &str = "horsies_archive_replacement_jobs";
pub const TRANSCODE_RELATIONS: &str = "horsies_archive_replacement_relations";
pub const TRANSCODE_BATCHES: &str = "horsies_archive_replacement_batches";
pub const TRANSCODE_MUTATION_FUNCTION: &str = "horsies_archive_replacement_note_mutation";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeJobRow {
    pub job_id: Uuid,
    pub maintenance_session_id: Uuid,
    pub component: ArchiveComponent,
    pub source_version: i16,
    pub target_version: i16,
    pub source_codec: String,
    pub target_codec: String,
    pub state: TranscodeJobState,
    pub transformed_rows: i64,
    pub copied_rows_total: i64,
    pub copied_rows_completed: i64,
    pub relation_count: i64,
    pub start_lsn: String,
    pub wal_bytes: Option<i64>,
}

#[derive(FromRow)]
struct RawJobRow {
    job_id: Uuid,
    maintenance_session_id: Uuid,
    component: String,
    source_version: i16,
    target_version: i16,
    source_codec: String,
    target_codec: String,
    state: String,
    transformed_rows: i64,
    copied_rows_total: i64,
    copied_rows_completed: i64,
    relation_count: i64,
    start_lsn: String,
    wal_bytes: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct TranscodeRelationRow {
    pub job_id: Uuid,
    pub relation_ordinal: i32,
    pub source_relation_oid: i64,
    pub source_relation_name: String,
    pub parent_relation_oid: i64,
    pub parent_relation_name: String,
    pub partition_bound: String,
    pub partition_constraint: String,
    pub replacement_relation_name: String,
    pub replacement_relation_oid: Option<i64>,
    pub backup_relation_name: String,
    pub state: String,
    pub row_count: i64,
    pub transformed_rows: i64,
    pub rows_copied: i64,
    pub last_source_ctid: Option<String>,
    pub source_mutation_generation: i64,
    pub replacement_mutation_generation: i64,
    pub verified_source_generation: Option<i64>,
    pub verified_replacement_generation: Option<i64>,
    pub verified_source_filenode: Option<i64>,
    pub verified_replacement_filenode: Option<i64>,
    pub verified_source_schema_signature: Option<String>,
    pub verified_replacement_schema_signature: Option<String>,
}

impl TranscodeRelationRow {
    pub fn parsed_state(&self) -> Result<TranscodeJobState, TranscodeError> {
        TranscodeJobState::parse(&self.state).ok_or_else(|| {
            TranscodeError::contract(format!(
                "unknown replacement relation state {:?}",
                self.state
            ))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationVerificationToken {
    pub source_generation: i64,
    pub replacement_generation: i64,
    pub source_filenode: i64,
    pub replacement_filenode: i64,
    pub source_schema_signature: String,
    pub replacement_schema_signature: String,
}

pub async fn lock_job(
    connection: &mut PgConnection,
    job_id: Uuid,
) -> Result<TranscodeJobRow, TranscodeError> {
    let raw: Option<RawJobRow> = sqlx::query_as(&format!(
        "SELECT jobs.job_id, jobs.maintenance_session_id, jobs.component,
                jobs.source_version, jobs.target_version, jobs.source_codec,
                jobs.target_codec, jobs.state, jobs.transformed_rows,
                jobs.copied_rows_total, jobs.copied_rows_completed,
                (SELECT count(*) FROM {TRANSCODE_RELATIONS}
                 WHERE job_id = jobs.job_id) AS relation_count,
                jobs.start_lsn::text AS start_lsn, jobs.wal_bytes
         FROM {TRANSCODE_JOBS} AS jobs
         WHERE jobs.job_id = $1 FOR UPDATE"
    ))
    .bind(job_id)
    .fetch_optional(connection)
    .await?;
    let raw = raw.ok_or_else(|| TranscodeError::state("unknown replacement transcode job"))?;
    let component = ArchiveComponent::parse(&raw.component).ok_or_else(|| {
        TranscodeError::contract(format!("unknown archive component {:?}", raw.component))
    })?;
    let state = TranscodeJobState::parse(&raw.state).ok_or_else(|| {
        TranscodeError::contract(format!("unknown replacement job state {:?}", raw.state))
    })?;
    Ok(TranscodeJobRow {
        job_id: raw.job_id,
        maintenance_session_id: raw.maintenance_session_id,
        component,
        source_version: raw.source_version,
        target_version: raw.target_version,
        source_codec: raw.source_codec,
        target_codec: raw.target_codec,
        state,
        transformed_rows: raw.transformed_rows,
        copied_rows_total: raw.copied_rows_total,
        copied_rows_completed: raw.copied_rows_completed,
        relation_count: raw.relation_count,
        start_lsn: raw.start_lsn,
        wal_bytes: raw.wal_bytes,
    })
}

pub async fn job_relations(
    connection: &mut PgConnection,
    job_id: Uuid,
) -> Result<Vec<TranscodeRelationRow>, TranscodeError> {
    Ok(sqlx::query_as(&format!(
        "SELECT job_id, relation_ordinal, source_relation_oid,
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
         WHERE job_id = $1 ORDER BY relation_ordinal"
    ))
    .bind(job_id)
    .fetch_all(connection)
    .await?)
}
