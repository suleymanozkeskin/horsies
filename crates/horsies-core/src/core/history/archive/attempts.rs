//! Canonical 12-field attempt-snapshot codec, version 1.

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::versions::{
    archive_digest, corrupt, validate_envelope_contract, verify_payload_digest, ArchiveDecodeError,
    ArchiveDomain, ARCHIVE_VERSION_1, JSON_CONTENT_TYPE, JSON_UTF8_CODEC,
};

pub const ATTEMPT_FIELD_COUNT: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    attempt: i32,
    outcome: String,
    will_retry: bool,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    error_code: Option<String>,
    error_message: Option<String>,
    failed_reason: Option<String>,
    worker_id: Option<String>,
    worker_hostname: Option<String>,
    worker_pid: Option<i32>,
    worker_process_name: Option<String>,
}

impl AttemptRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempt: i32,
        outcome: impl Into<String>,
        will_retry: bool,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
        error_code: Option<String>,
        error_message: Option<String>,
        failed_reason: Option<String>,
        worker_id: Option<String>,
        worker_hostname: Option<String>,
        worker_pid: Option<i32>,
        worker_process_name: Option<String>,
    ) -> Result<Self, &'static str> {
        let outcome = outcome.into();
        if attempt < 1 {
            return Err("attempt numbers start at 1");
        }
        if outcome.is_empty() {
            return Err("attempt outcome must be non-empty");
        }
        Ok(Self {
            attempt,
            outcome,
            will_retry,
            started_at,
            finished_at,
            error_code,
            error_message,
            failed_reason,
            worker_id,
            worker_hostname,
            worker_pid,
            worker_process_name,
        })
    }

    pub fn attempt(&self) -> i32 {
        self.attempt
    }

    pub fn outcome(&self) -> &str {
        &self.outcome
    }

    pub fn will_retry(&self) -> bool {
        self.will_retry
    }

    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    pub fn finished_at(&self) -> DateTime<Utc> {
        self.finished_at
    }

    pub fn error_code(&self) -> Option<&str> {
        self.error_code.as_deref()
    }

    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    pub fn failed_reason(&self) -> Option<&str> {
        self.failed_reason.as_deref()
    }

    pub fn worker_id(&self) -> Option<&str> {
        self.worker_id.as_deref()
    }

    pub fn worker_hostname(&self) -> Option<&str> {
        self.worker_hostname.as_deref()
    }

    pub fn worker_pid(&self) -> Option<i32> {
        self.worker_pid
    }

    pub fn worker_process_name(&self) -> Option<&str> {
        self.worker_process_name.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAttemptSnapshot {
    pub version: i16,
    pub codec: &'static str,
    pub content_type: &'static str,
    pub payload: Vec<u8>,
    pub digest: [u8; 32],
}

pub fn encode_attempt_snapshot(
    attempts: &[AttemptRecord],
) -> Result<StoredAttemptSnapshot, ArchiveDecodeError> {
    for (index, attempt) in attempts.iter().enumerate() {
        if attempt.attempt as usize != index + 1 {
            return Err(corrupt(
                ArchiveDomain::Attempts,
                "attempts must be ordered and contiguous from 1",
            ));
        }
    }
    let rows: Vec<Value> = attempts.iter().map(positional_row).collect();
    let payload = serde_json::to_vec(&rows)
        .map_err(|error| corrupt(ArchiveDomain::Attempts, error.to_string()))?;
    Ok(StoredAttemptSnapshot {
        version: ARCHIVE_VERSION_1,
        codec: JSON_UTF8_CODEC,
        content_type: JSON_CONTENT_TYPE,
        digest: archive_digest(&payload),
        payload,
    })
}

pub fn decode_attempt_snapshot(
    version: i16,
    codec: &str,
    content_type: &str,
    payload: &[u8],
    digest: &[u8],
) -> Result<Vec<AttemptRecord>, ArchiveDecodeError> {
    validate_envelope_contract(ArchiveDomain::Attempts, version, codec, content_type)?;
    verify_payload_digest(ArchiveDomain::Attempts, payload, digest)?;
    let parsed: Value = serde_json::from_slice(payload)
        .map_err(|_| corrupt(ArchiveDomain::Attempts, "JSONDecodeError"))?;
    let rows = parsed
        .as_array()
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, "expected_array"))?;
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        records.push(decode_positional_row(row)?);
    }
    if records
        .iter()
        .enumerate()
        .any(|(index, record)| record.attempt as usize != index + 1)
    {
        return Err(corrupt(ArchiveDomain::Attempts, "non_contiguous_attempts"));
    }
    Ok(records)
}

fn positional_row(record: &AttemptRecord) -> Value {
    Value::Array(vec![
        Value::from(record.attempt),
        Value::from(record.outcome.clone()),
        Value::from(record.will_retry),
        Value::from(record.started_at.timestamp_micros()),
        Value::from(record.finished_at.timestamp_micros()),
        optional_string(&record.error_code),
        optional_string(&record.error_message),
        optional_string(&record.failed_reason),
        optional_string(&record.worker_id),
        optional_string(&record.worker_hostname),
        record.worker_pid.map(Value::from).unwrap_or(Value::Null),
        optional_string(&record.worker_process_name),
    ])
}

fn decode_positional_row(value: &Value) -> Result<AttemptRecord, ArchiveDecodeError> {
    let row = value
        .as_array()
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, "expected_positional_row"))?;
    if row.len() != ATTEMPT_FIELD_COUNT {
        return Err(corrupt(ArchiveDomain::Attempts, "wrong_field_count"));
    }
    let attempt = plain_i64(&row[0], "invalid_attempt_number")?;
    if attempt < 1 || attempt > i32::MAX as i64 {
        return Err(corrupt(ArchiveDomain::Attempts, "invalid_attempt_number"));
    }
    let outcome = row[1]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, "invalid_outcome"))?;
    let will_retry = row[2]
        .as_bool()
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, "invalid_will_retry"))?;
    let started_at = decode_epoch_us(&row[3], "invalid_started_at")?;
    let finished_at = decode_epoch_us(&row[4], "invalid_finished_at")?;
    let worker_pid = match &row[10] {
        Value::Null => None,
        value => {
            let value = plain_i64(value, "invalid_worker_pid")?;
            Some(
                i32::try_from(value)
                    .map_err(|_| corrupt(ArchiveDomain::Attempts, "invalid_worker_pid"))?,
            )
        }
    };
    AttemptRecord::new(
        attempt as i32,
        outcome,
        will_retry,
        started_at,
        finished_at,
        optional_text(&row[5], "invalid_text_field_5")?,
        optional_text(&row[6], "invalid_text_field_6")?,
        optional_text(&row[7], "invalid_text_field_7")?,
        optional_text(&row[8], "invalid_text_field_8")?,
        optional_text(&row[9], "invalid_text_field_9")?,
        worker_pid,
        optional_text(&row[11], "invalid_worker_process_name")?,
    )
    .map_err(|detail| corrupt(ArchiveDomain::Attempts, detail))
}

fn optional_string(value: &Option<String>) -> Value {
    value.clone().map(Value::from).unwrap_or(Value::Null)
}

fn optional_text(
    value: &Value,
    detail: &'static str,
) -> Result<Option<String>, ArchiveDecodeError> {
    match value {
        Value::Null => Ok(None),
        Value::String(value) => Ok(Some(value.clone())),
        _ => Err(corrupt(ArchiveDomain::Attempts, detail)),
    }
}

fn plain_i64(value: &Value, detail: &'static str) -> Result<i64, ArchiveDecodeError> {
    value
        .as_i64()
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, detail))
}

fn decode_epoch_us(
    value: &Value,
    detail: &'static str,
) -> Result<DateTime<Utc>, ArchiveDecodeError> {
    DateTime::from_timestamp_micros(plain_i64(value, detail)?)
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, detail))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_payload(payload: &[u8]) -> Result<Vec<AttemptRecord>, ArchiveDecodeError> {
        decode_attempt_snapshot(
            1,
            JSON_UTF8_CODEC,
            JSON_CONTENT_TYPE,
            payload,
            &archive_digest(payload),
        )
    }

    fn corrupt_detail(payload: &[u8]) -> String {
        match decode_payload(payload).unwrap_err() {
            ArchiveDecodeError::Corrupt { detail, .. } => detail,
            other => panic!("expected corrupt value, got {other:?}"),
        }
    }

    #[test]
    fn malformed_attempt_shapes_fail_with_pinned_details() {
        for (payload, detail) in [
            (b"not json".as_slice(), "JSONDecodeError"),
            (b"{}".as_slice(), "expected_array"),
            (b"[{}]".as_slice(), "expected_positional_row"),
            (b"[[1,\"OK\",true]]".as_slice(), "wrong_field_count"),
        ] {
            assert_eq!(corrupt_detail(payload), detail);
        }
    }

    #[test]
    fn attempt_fields_and_contiguity_fail_closed() {
        let base =
            serde_json::json!([[1, "OK", false, 0, 1, null, null, null, null, null, null, null]]);
        let cases = [
            (0, serde_json::json!(0), "invalid_attempt_number"),
            (0, serde_json::json!(true), "invalid_attempt_number"),
            (1, serde_json::json!(""), "invalid_outcome"),
            (2, serde_json::json!(1), "invalid_will_retry"),
            (3, serde_json::json!(1.5), "invalid_started_at"),
            (4, Value::Null, "invalid_finished_at"),
            (5, serde_json::json!(7), "invalid_text_field_5"),
            (10, serde_json::json!(true), "invalid_worker_pid"),
            (11, serde_json::json!(4), "invalid_worker_process_name"),
        ];
        for (index, replacement, detail) in cases {
            let mut mutated = base.clone();
            mutated[0][index] = replacement;
            assert_eq!(
                corrupt_detail(&serde_json::to_vec(&mutated).unwrap()),
                detail
            );
        }
        let non_contiguous = serde_json::to_vec(&serde_json::json!([
            [1, "OK", false, 0, 1, null, null, null, null, null, null, null],
            [3, "OK", false, 2, 3, null, null, null, null, null, null, null]
        ]))
        .unwrap();
        assert_eq!(corrupt_detail(&non_contiguous), "non_contiguous_attempts");
    }

    #[test]
    fn envelope_contract_precedes_digest_validation() {
        let payload = b"[]";
        assert!(matches!(
            decode_attempt_snapshot(9, JSON_UTF8_CODEC, JSON_CONTENT_TYPE, payload, &[0; 32]),
            Err(ArchiveDecodeError::UnknownVersion { version: 9, .. })
        ));
        assert!(matches!(
            decode_attempt_snapshot(
                1,
                "cbor",
                JSON_CONTENT_TYPE,
                payload,
                &archive_digest(payload)
            ),
            Err(ArchiveDecodeError::UnknownCodec { .. })
        ));
        assert!(matches!(
            decode_attempt_snapshot(
                1,
                JSON_UTF8_CODEC,
                "text/plain",
                payload,
                &archive_digest(payload)
            ),
            Err(ArchiveDecodeError::UnknownContentType { .. })
        ));
    }

    #[test]
    fn invalid_attempt_records_cannot_be_constructed() {
        let started = DateTime::from_timestamp(0, 0).unwrap();
        let finished = DateTime::from_timestamp(1, 0).unwrap();
        assert!(AttemptRecord::new(
            0, "OK", false, started, finished, None, None, None, None, None, None, None,
        )
        .is_err());
        assert!(AttemptRecord::new(
            1, "", false, started, finished, None, None, None, None, None, None, None,
        )
        .is_err());
    }
}
