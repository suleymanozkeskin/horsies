//! Keyset preparation of enqueue-time facts for every legacy live-table row.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgConnection};

use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::history::errors::HistoryError;
use crate::core::history::identity::fingerprint::{EnqueueCommandV1, COMMAND_FINGERPRINT_VERSION};
use crate::core::history::names::LIVE_TASKS;
use crate::core::history::rerun::input_envelope::{
    canonical_json_bytes, INPUT_ENVELOPE_CODEC, INPUT_ENVELOPE_CONTENT_TYPE,
    INPUT_ENVELOPE_INLINE_MAX_BYTES, INPUT_ENVELOPE_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparationCursor {
    after_id: Option<String>,
}

impl PreparationCursor {
    pub fn start() -> Self {
        Self { after_id: None }
    }

    pub fn after_id(&self) -> Option<&str> {
        self.after_id.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparationOutcome {
    Batch {
        rows_prepared: usize,
        live_rows_prepared: usize,
        inline_rows: usize,
        over_bound_rows: usize,
        policy_declined_rows: usize,
        decode_failed_rows: usize,
        cursor: PreparationCursor,
    },
    Complete {
        rows_prepared: i64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum PreparationError {
    #[error("preparation batch size must be positive")]
    InvalidBatchSize,
    #[error("legacy command fingerprint failed: {0}")]
    Fingerprint(String),
    #[error("legacy input envelope encoding failed: {0}")]
    Envelope(String),
    #[error(transparent)]
    History(#[from] HistoryError),
}

#[derive(Debug, FromRow)]
struct LegacyRow {
    task_id: String,
    status: String,
    task_name: String,
    queue_name: String,
    priority: i32,
    args: Option<String>,
    kwargs: Option<String>,
    task_options: Option<String>,
    good_until: Option<DateTime<Utc>>,
    retention_class_key: Option<String>,
    retain_rerun_input: Option<bool>,
}

#[derive(Debug)]
struct PreparedRow {
    task_id: String,
    fingerprint: Vec<u8>,
    input_digest: Option<Vec<u8>>,
    retention_class_key: String,
    retain: bool,
    disposition: String,
    version: Option<i16>,
    codec: Option<String>,
    content_type: Option<String>,
    digest: Option<Vec<u8>>,
    inline: Option<Vec<u8>>,
    decode_failed: bool,
}

fn python_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value
            .as_f64()
            .map_or_else(|| value.to_string() != "0", |value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn decode_json(value: Option<&str>) -> Result<Value, serde_json::Error> {
    match value {
        Some(value) => serde_json::from_str(value),
        None => Ok(Value::Null),
    }
}

fn prepare_one(row: LegacyRow, retain_default: bool) -> Result<PreparedRow, PreparationError> {
    let retain = row.retain_rerun_input.unwrap_or(retain_default);
    let retention_class_key = row
        .retention_class_key
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| FOREVER_CLASS_KEY.to_owned());
    let fingerprint = EnqueueCommandV1::new(
        &row.task_name,
        &row.queue_name,
        row.priority,
        row.args.clone(),
        row.kwargs.clone(),
        row.good_until,
        None,
        row.task_options.clone(),
        &retention_class_key,
        retain,
        None,
        None,
    )
    .and_then(|command| command.fingerprint())
    .map_err(|error| PreparationError::Fingerprint(error.to_string()))?
    .to_vec();

    let decoded = (|| {
        let args = decode_json(row.args.as_deref())?;
        let kwargs = decode_json(row.kwargs.as_deref())?;
        let options = decode_json(row.task_options.as_deref())?;
        let mut content = Map::new();
        content.insert(
            "args".to_owned(),
            if python_truthy(&args) {
                args
            } else {
                Value::Array(Vec::new())
            },
        );
        content.insert(
            "kwargs".to_owned(),
            if python_truthy(&kwargs) {
                kwargs
            } else {
                Value::Object(Map::new())
            },
        );
        content.insert("options".to_owned(), options);
        canonical_json_bytes(&Value::Object(content))
    })();

    let payload = match decoded {
        Ok(payload) => payload,
        Err(_) => {
            return Ok(PreparedRow {
                task_id: row.task_id,
                fingerprint,
                input_digest: None,
                retention_class_key,
                retain,
                disposition: "DECLINED_BY_POLICY".to_owned(),
                version: None,
                codec: None,
                content_type: None,
                digest: None,
                inline: None,
                decode_failed: true,
            });
        }
    };
    let digest = Sha256::digest(&payload).to_vec();
    let (disposition, envelope) = if !retain {
        ("DECLINED_BY_POLICY", false)
    } else if payload.len() > INPUT_ENVELOPE_INLINE_MAX_BYTES {
        ("OVER_BOUND", false)
    } else {
        ("INLINE", true)
    };
    Ok(PreparedRow {
        task_id: row.task_id,
        fingerprint,
        input_digest: Some(digest.clone()),
        retention_class_key,
        retain,
        disposition: disposition.to_owned(),
        version: envelope.then_some(INPUT_ENVELOPE_VERSION),
        codec: envelope.then(|| INPUT_ENVELOPE_CODEC.to_owned()),
        content_type: envelope.then(|| INPUT_ENVELOPE_CONTENT_TYPE.to_owned()),
        digest: envelope.then_some(digest),
        inline: envelope.then_some(payload),
        decode_failed: false,
    })
}

pub async fn prepare_legacy_batch(
    connection: &mut PgConnection,
    retain_default: bool,
    batch_size: i64,
    cursor: &PreparationCursor,
) -> Result<PreparationOutcome, PreparationError> {
    if batch_size <= 0 {
        return Err(PreparationError::InvalidBatchSize);
    }
    let select = format!(
        "SELECT id::text AS task_id, status, task_name, queue_name, priority,
                args, kwargs, task_options, good_until,
                retention_class_key, retain_rerun_input
         FROM {LIVE_TASKS}
         WHERE prepared_rerun_input_disposition IS NULL {}
         ORDER BY id LIMIT $1",
        if cursor.after_id.is_some() {
            "AND id::text > $2"
        } else {
            ""
        }
    );
    let rows: Vec<LegacyRow> = match cursor.after_id.as_deref() {
        Some(after_id) => sqlx::query_as(&select)
            .bind(batch_size)
            .bind(after_id)
            .fetch_all(&mut *connection)
            .await
            .map_err(HistoryError::from)?,
        None => sqlx::query_as(&select)
            .bind(batch_size)
            .fetch_all(&mut *connection)
            .await
            .map_err(HistoryError::from)?,
    };
    if rows.is_empty() {
        let total = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {LIVE_TASKS} \
             WHERE prepared_rerun_input_disposition IS NOT NULL"
        ))
        .fetch_one(connection)
        .await
        .map_err(HistoryError::from)?;
        return Ok(PreparationOutcome::Complete {
            rows_prepared: total,
        });
    }
    let live_rows_prepared = rows
        .iter()
        .filter(|row| matches!(row.status.as_str(), "PENDING" | "CLAIMED" | "RUNNING"))
        .count();
    let prepared: Vec<PreparedRow> = rows
        .into_iter()
        .map(|row| prepare_one(row, retain_default))
        .collect::<Result<_, _>>()?;

    let task_ids: Vec<String> = prepared.iter().map(|row| row.task_id.clone()).collect();
    let fingerprints: Vec<Vec<u8>> = prepared.iter().map(|row| row.fingerprint.clone()).collect();
    let input_digests: Vec<Option<Vec<u8>>> = prepared
        .iter()
        .map(|row| row.input_digest.clone())
        .collect();
    let class_keys: Vec<String> = prepared
        .iter()
        .map(|row| row.retention_class_key.clone())
        .collect();
    let retains: Vec<bool> = prepared.iter().map(|row| row.retain).collect();
    let dispositions: Vec<String> = prepared.iter().map(|row| row.disposition.clone()).collect();
    let versions: Vec<Option<i16>> = prepared.iter().map(|row| row.version).collect();
    let codecs: Vec<Option<String>> = prepared.iter().map(|row| row.codec.clone()).collect();
    let content_types: Vec<Option<String>> = prepared
        .iter()
        .map(|row| row.content_type.clone())
        .collect();
    let digests: Vec<Option<Vec<u8>>> = prepared.iter().map(|row| row.digest.clone()).collect();
    let inline: Vec<Option<Vec<u8>>> = prepared.iter().map(|row| row.inline.clone()).collect();
    let update = format!(
        "UPDATE {LIVE_TASKS} AS t SET
             command_fingerprint_version = {COMMAND_FINGERPRINT_VERSION},
             command_fingerprint = p.fingerprint,
             input_digest = p.input_digest,
             retention_class_key = p.retention_class_key,
             retain_rerun_input = p.retain,
             prepared_rerun_input_disposition = p.disposition,
             prepared_rerun_input_version = p.version,
             prepared_rerun_input_codec = p.codec,
             prepared_rerun_input_content_type = p.content_type,
             prepared_rerun_input_digest = p.digest,
             prepared_rerun_input_inline = p.inline_payload
         FROM UNNEST(
             $1::text[], $2::bytea[], $3::bytea[], $4::text[], $5::boolean[],
             $6::text[], $7::smallint[], $8::text[], $9::text[], $10::bytea[],
             $11::bytea[]
         ) AS p(task_id, fingerprint, input_digest, retention_class_key, retain,
                disposition, version, codec, content_type, digest, inline_payload)
         WHERE t.id::text = p.task_id"
    );
    let updated = sqlx::query(&update)
        .bind(&task_ids)
        .bind(&fingerprints)
        .bind(&input_digests)
        .bind(&class_keys)
        .bind(&retains)
        .bind(&dispositions)
        .bind(&versions)
        .bind(&codecs)
        .bind(&content_types)
        .bind(&digests)
        .bind(&inline)
        .execute(connection)
        .await
        .map_err(HistoryError::from)?;
    if updated.rows_affected() != prepared.len() as u64 {
        return Err(PreparationError::History(HistoryError::contract(format!(
            "preparation updated {} of {} selected rows",
            updated.rows_affected(),
            prepared.len()
        ))));
    }
    Ok(PreparationOutcome::Batch {
        rows_prepared: prepared.len(),
        live_rows_prepared,
        inline_rows: prepared
            .iter()
            .filter(|row| row.disposition == "INLINE")
            .count(),
        over_bound_rows: prepared
            .iter()
            .filter(|row| row.disposition == "OVER_BOUND")
            .count(),
        policy_declined_rows: prepared
            .iter()
            .filter(|row| row.disposition == "DECLINED_BY_POLICY" && !row.decode_failed)
            .count(),
        decode_failed_rows: prepared.iter().filter(|row| row.decode_failed).count(),
        cursor: PreparationCursor {
            after_id: task_ids.last().cloned(),
        },
    })
}
