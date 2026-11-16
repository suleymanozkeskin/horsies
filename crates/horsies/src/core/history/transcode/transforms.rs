//! Single-owner SQL renderers for component copy and verification.

use crate::core::history::commands::is_safe_identifier;

use super::outcomes::ArchiveComponent;
use super::TranscodeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComponentColumns {
    pub version: &'static str,
    pub codec: &'static str,
    pub payload: &'static str,
    pub presence_predicate: &'static str,
    pub metadata_only: bool,
}

pub const fn component_columns(component: ArchiveComponent) -> ComponentColumns {
    match component {
        ArchiveComponent::HistoryRow => ComponentColumns {
            version: "history_schema_version",
            codec: "CASE history_schema_version WHEN 1 THEN 'row-v1' WHEN 2 THEN 'row-v2' END",
            payload: "NULL::bytea",
            presence_predicate: "TRUE",
            metadata_only: true,
        },
        ArchiveComponent::Result => ComponentColumns {
            version: "result_envelope_version",
            codec: "result_codec",
            payload: "COALESCE(result_payload, prior_result_payload)",
            presence_predicate: "result_payload IS NOT NULL OR prior_result_payload IS NOT NULL",
            metadata_only: false,
        },
        ArchiveComponent::Attempts => ComponentColumns {
            version: "attempt_archive_version",
            codec: "attempt_snapshot_codec",
            payload: "attempt_snapshot",
            presence_predicate: "attempt_snapshot IS NOT NULL",
            metadata_only: false,
        },
        ArchiveComponent::RerunInput => ComponentColumns {
            version: "rerun_input_version",
            codec: "rerun_input_codec",
            payload: "rerun_input_inline",
            presence_predicate: "rerun_input_disposition IN ('INLINE', 'REFERENCE')",
            metadata_only: false,
        },
    }
}

pub fn quoted_identifier(value: &str) -> Result<String, TranscodeError> {
    if !is_safe_identifier(value) {
        return Err(TranscodeError::InvalidArgument(format!(
            "unsafe PostgreSQL identifier {value:?}"
        )));
    }
    Ok(format!("\"{value}\""))
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn column_list(columns: &[String]) -> Result<String, TranscodeError> {
    columns
        .iter()
        .map(|column| quoted_identifier(column))
        .collect::<Result<Vec<_>, _>>()
        .map(|columns| columns.join(", "))
}

fn encoded_payload_name(payload_column: &str) -> String {
    format!("archive_target_{payload_column}")
}

pub fn component_source_condition(
    component: ArchiveComponent,
    alias: &str,
    source_version: i16,
    source_codec: &str,
) -> String {
    if component == ArchiveComponent::HistoryRow {
        return format!("{alias}.history_schema_version = {source_version}");
    }
    let columns = component_columns(component);
    let presence = match component {
        ArchiveComponent::HistoryRow => unreachable!("handled above"),
        ArchiveComponent::Result => {
            format!(
                "{alias}.result_payload IS NOT NULL OR {alias}.prior_result_payload IS NOT NULL"
            )
        }
        ArchiveComponent::Attempts => format!("{alias}.attempt_snapshot IS NOT NULL"),
        ArchiveComponent::RerunInput => {
            format!("{alias}.rerun_input_disposition IN ('INLINE', 'REFERENCE')")
        }
    };
    format!(
        "{alias}.{} = {source_version} AND {alias}.{} = {} AND ({presence})",
        columns.version,
        columns.codec,
        sql_literal(source_codec)
    )
}

pub fn encoded_source_select(
    component: ArchiveComponent,
    alias: &str,
    source_version: i16,
    source_codec: &str,
    forward: bool,
) -> Result<String, TranscodeError> {
    let condition = component_source_condition(component, alias, source_version, source_codec);
    let payload_columns: &[&str] = match component {
        ArchiveComponent::HistoryRow => return Ok(format!("{alias}.*")),
        ArchiveComponent::Result => &["result_payload", "prior_result_payload"],
        ArchiveComponent::Attempts => &["attempt_snapshot"],
        ArchiveComponent::RerunInput => &["rerun_input_inline"],
    };
    let mut encoded = vec![format!("{alias}.*")];
    for payload_column in payload_columns {
        let source = format!("{alias}.{}", quoted_identifier(payload_column)?);
        let transformed = if forward {
            format!("decode('4832', 'hex') || {source}")
        } else {
            format!("substring({source} FROM 3)")
        };
        encoded.push(format!(
            "CASE WHEN {condition} AND {source} IS NOT NULL THEN {transformed} \
             ELSE {source} END AS {}",
            quoted_identifier(&encoded_payload_name(payload_column))?
        ));
    }
    Ok(encoded.join(", "))
}

pub fn transformed_select(
    columns: &[String],
    component: ArchiveComponent,
    source_version: i16,
    source_codec: &str,
    target_version: i16,
    target_codec: &str,
    alias: &str,
) -> Result<String, TranscodeError> {
    let condition = component_source_condition(component, alias, source_version, source_codec);
    let mut expressions = std::collections::BTreeMap::new();
    for column in columns {
        expressions.insert(
            column.as_str(),
            format!("{alias}.{}", quoted_identifier(column)?),
        );
    }
    match component {
        ArchiveComponent::HistoryRow => {
            expressions.insert(
                "history_schema_version",
                format!(
                    "CASE WHEN {condition} THEN {target_version} ELSE {alias}.history_schema_version END"
                ),
            );
        }
        ArchiveComponent::Result => apply_payload_transform(
            &mut expressions,
            &condition,
            alias,
            "result_envelope_version",
            "result_codec",
            &["result_payload", "prior_result_payload"],
            "result_digest",
            target_version,
            target_codec,
        )?,
        ArchiveComponent::Attempts => apply_payload_transform(
            &mut expressions,
            &condition,
            alias,
            "attempt_archive_version",
            "attempt_snapshot_codec",
            &["attempt_snapshot"],
            "attempt_snapshot_digest",
            target_version,
            target_codec,
        )?,
        ArchiveComponent::RerunInput => apply_payload_transform(
            &mut expressions,
            &condition,
            alias,
            "rerun_input_version",
            "rerun_input_codec",
            &["rerun_input_inline"],
            "rerun_input_digest",
            target_version,
            target_codec,
        )?,
    }
    columns
        .iter()
        .map(|column| {
            expressions
                .get(column.as_str())
                .cloned()
                .ok_or_else(|| TranscodeError::contract(format!("missing projection for {column}")))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|columns| columns.join(", "))
}

#[allow(clippy::too_many_arguments)]
fn apply_payload_transform<'a>(
    expressions: &mut std::collections::BTreeMap<&'a str, String>,
    condition: &str,
    alias: &str,
    version_column: &'a str,
    codec_column: &'a str,
    payload_columns: &[&'a str],
    digest_column: &'a str,
    target_version: i16,
    target_codec: &str,
) -> Result<(), TranscodeError> {
    expressions.insert(
        version_column,
        format!(
            "CASE WHEN {condition} THEN {target_version} ELSE {alias}.{} END",
            quoted_identifier(version_column)?
        ),
    );
    expressions.insert(
        codec_column,
        format!(
            "CASE WHEN {condition} THEN {} ELSE {alias}.{} END",
            sql_literal(target_codec),
            quoted_identifier(codec_column)?
        ),
    );
    let mut transformed_payloads = Vec::new();
    for payload_column in payload_columns {
        let transformed = format!(
            "{alias}.{}",
            quoted_identifier(&encoded_payload_name(payload_column))?
        );
        expressions.insert(payload_column, transformed.clone());
        transformed_payloads.push(transformed);
    }
    let payload = match transformed_payloads.as_slice() {
        [payload] => payload.clone(),
        payloads => format!("COALESCE({})", payloads.join(", ")),
    };
    expressions.insert(
        digest_column,
        format!(
            "CASE WHEN {condition} AND {payload} IS NOT NULL THEN sha256({payload}) \
             ELSE {alias}.{} END",
            quoted_identifier(digest_column)?
        ),
    );
    Ok(())
}

fn job_suffix(job_id: uuid::Uuid) -> String {
    job_id.simple().to_string()[..12].to_owned()
}

pub fn replacement_bound_name(job_id: uuid::Uuid, ordinal: i32) -> String {
    format!("archive_replacement_bound_{}_{ordinal}", job_suffix(job_id))
}

pub fn replacement_index_name(job_id: uuid::Uuid, ordinal: i32) -> String {
    format!("archive_replacement_id_{}_{ordinal}", job_suffix(job_id))
}

pub fn replacement_ordering_index_name(job_id: uuid::Uuid, ordinal: i32) -> String {
    format!("archive_replacement_enq_{}_{ordinal}", job_suffix(job_id))
}

pub fn replacement_relation_name(job_id: uuid::Uuid, ordinal: i32) -> String {
    format!("archive_replacement_{}_{ordinal}", job_suffix(job_id))
}

pub fn backup_relation_name(job_id: uuid::Uuid, ordinal: i32) -> String {
    format!("archive_replaced_{}_{ordinal}", job_suffix(job_id))
}
