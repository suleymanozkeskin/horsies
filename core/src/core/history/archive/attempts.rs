//! The attempt archive: the attempts of one task as history stores them.
//!
//! Two versions, each one JSON payload (`json-utf8`, `application/json`)
//! with a SHA-256 digest:
//! - Version 1: an array of attempts, each an array of
//!   [`ATTEMPT_FIELD_COUNT`] values. It holds no phase record and no open
//!   attempt.
//! - Version 2: an array of two items, `[attempts, open]`. Each attempt is an
//!   array of [`ATTEMPT_V2_FIELD_COUNT`] values: the values of version 1,
//!   then the phase record (`phase_record_version`, `enqueued_at`,
//!   `claimed_at`, `attempt_claims`, `first_claimed_at`, `placed_at`,
//!   `finalizing_at`, `recorded_by`, `last_seen_at`). `open` is `null` when
//!   the last attempt ended with its row, or `[attempt_claims,
//!   first_claimed_at, finalizing_at]` of the attempt that has no row.
//!
//! Times are epoch microseconds; a missing value is JSON `null`. The SQL
//! encoder of version 2 (`horsies_encode_attempt_archive`, migration 1001)
//! writes the same bytes as [`encode_attempt_archive`] for the same values.

use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::versions::{
    archive_digest, corrupt, validate_envelope_contract, verify_payload_digest, ArchiveDecodeError,
    ArchiveDomain, ARCHIVE_VERSION_1, ARCHIVE_VERSION_2, JSON_CONTENT_TYPE, JSON_UTF8_CODEC,
};

/// The values of one attempt in version 1, and the first values of one
/// attempt in version 2.
pub const ATTEMPT_FIELD_COUNT: usize = 12;

/// The values of one attempt in version 2: the values of version 1, then
/// the [`PHASE_FIELD_COUNT`] values of the phase record.
pub const ATTEMPT_V2_FIELD_COUNT: usize = ATTEMPT_FIELD_COUNT + PHASE_FIELD_COUNT;

/// The values of the phase record of one attempt in version 2.
pub const PHASE_FIELD_COUNT: usize = 9;

/// The form of the phase record that migration 1001 writes
/// (`horsies_task_attempts.phase_record_version`).
pub const PHASE_RECORD_VERSION_1: i64 = 1;

/// The stored text of [`RecordedBy::Worker`].
const RECORDED_BY_WORKER: &str = "WORKER";
/// The stored text of [`RecordedBy::Recovery`].
const RECORDED_BY_RECOVERY: &str = "RECOVERY";

/// Whether an attempt row has a phase record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseRecord {
    /// The attempt row was written before migration 1001, or the archive is
    /// version 1: the row has no phase record.
    NotCaptured,
    /// The phase record of the attempt.
    Captured(AttemptPhases),
}

/// The phase times of one recorded attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptPhases {
    enqueued_at: DateTime<Utc>,
    claimed_at: DateTime<Utc>,
    claims: ClaimRecord,
    finalizing: Finalizing,
    recorded_by: RecordedBy,
}

impl AttemptPhases {
    pub fn new(
        enqueued_at: DateTime<Utc>,
        claimed_at: DateTime<Utc>,
        claims: ClaimRecord,
        finalizing: Finalizing,
        recorded_by: RecordedBy,
    ) -> Self {
        Self {
            enqueued_at,
            claimed_at,
            claims,
            finalizing,
            recorded_by,
        }
    }

    /// The due time of the attempt.
    pub fn enqueued_at(&self) -> DateTime<Utc> {
        self.enqueued_at
    }

    /// The claim generation that ran the attempt.
    pub fn claimed_at(&self) -> DateTime<Utc> {
        self.claimed_at
    }

    pub fn claims(&self) -> ClaimRecord {
        self.claims
    }

    pub fn finalizing(&self) -> Finalizing {
        self.finalizing
    }

    pub fn recorded_by(&self) -> RecordedBy {
        self.recorded_by
    }
}

/// The claims of one recorded attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimRecord {
    /// The attempt began before migration 1001: its claims were not counted.
    NotCounted,
    /// `count` claims ran for the attempt; the first was at
    /// `first_claimed_at`.
    Counted {
        count: NonZeroU32,
        first_claimed_at: DateTime<Utc>,
    },
}

/// When a child of the worker took the task of the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// A child took the task at `at`: the claim generation plus the slot
    /// wait that the worker measured.
    Placed { at: DateTime<Utc> },
    /// The writer of the attempt row did not observe a placement.
    NotObserved,
}

/// The finalizing marker of the claim that ran an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finalizing {
    /// The child marked the end of the body at `at`.
    Marked { at: DateTime<Utc> },
    /// No marker was written.
    NotMarked,
}

/// Who wrote the attempt row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedBy {
    /// The worker wrote the end of the attempt, with the placement it
    /// observed.
    Worker { placement: Placement },
    /// Stale RUNNING recovery wrote the attempt row. `last_seen_at` is the
    /// last time the attempt was known alive: the greatest of its start, its
    /// finalizing marker and its newest runner beat. Recovery observes no
    /// placement.
    Recovery { last_seen_at: DateTime<Utc> },
}

/// The attempt of a task that has no attempt row, as the archive keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAttempt {
    /// The last attempt ended with its row: no attempt is open.
    Ended,
    /// The attempt `retry_count + 1` has no row: a cancel, an expiry or a
    /// pause abandon ended it, or it still runs (a live read).
    Open(OpenAttemptFacts),
    /// The archive is version 1, which does not store the open attempt.
    NotStored,
}

/// The claim facts of an open attempt, from the task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenAttemptFacts {
    claims: OpenClaims,
    finalizing: Finalizing,
}

impl OpenAttemptFacts {
    pub fn new(claims: OpenClaims, finalizing: Finalizing) -> Self {
        Self { claims, finalizing }
    }

    pub fn claims(&self) -> OpenClaims {
        self.claims
    }

    pub fn finalizing(&self) -> Finalizing {
        self.finalizing
    }
}

/// The claims of an open attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenClaims {
    /// The attempt began before migration 1001: its claims were not counted.
    NotCounted,
    /// The attempt was never claimed.
    NotClaimed,
    /// `count` claims ran for the attempt; the first was at
    /// `first_claimed_at`.
    Counted {
        count: NonZeroU32,
        first_claimed_at: DateTime<Utc>,
    },
}

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
    phases: PhaseRecord,
}

impl AttemptRecord {
    /// An attempt with no phase record ([`PhaseRecord::NotCaptured`]): the
    /// form of version 1. [`AttemptRecord::with_phases`] gives the record
    /// its phases.
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
            phases: PhaseRecord::NotCaptured,
        })
    }

    /// The same attempt with the phase record `phases`.
    #[must_use]
    pub fn with_phases(self, phases: PhaseRecord) -> Self {
        Self { phases, ..self }
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

    pub fn phases(&self) -> PhaseRecord {
        self.phases
    }
}

/// The decoded archive of one task: its attempts in ascending order, and
/// its open attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptArchive {
    attempts: Vec<AttemptRecord>,
    open: OpenAttempt,
}

impl AttemptArchive {
    pub fn attempts(&self) -> &[AttemptRecord] {
        &self.attempts
    }

    pub fn open(&self) -> OpenAttempt {
        self.open
    }

    /// The attempts, without the open attempt.
    pub fn into_attempts(self) -> Vec<AttemptRecord> {
        self.attempts
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

/// Encodes `attempts` in version 1.
///
/// Errors: `Corrupt` when the attempts are not contiguous from 1, or when
/// one holds a phase record, which version 1 cannot store.
pub fn encode_attempt_snapshot(
    attempts: &[AttemptRecord],
) -> Result<StoredAttemptSnapshot, ArchiveDecodeError> {
    check_contiguous_for_encode(attempts)?;
    if attempts
        .iter()
        .any(|attempt| matches!(attempt.phases, PhaseRecord::Captured(_)))
    {
        return Err(corrupt(
            ArchiveDomain::Attempts,
            "version 1 stores no phase record",
        ));
    }
    let rows: Vec<Value> = attempts
        .iter()
        .map(|attempt| Value::Array(version_1_fields(attempt)))
        .collect();
    stored(ARCHIVE_VERSION_1, &Value::Array(rows))
}

/// Encodes `attempts` and `open` in version 2, the form of
/// `horsies_encode_attempt_archive`.
///
/// Errors: `Corrupt` when the attempts are not contiguous from 1, or when
/// `open` is [`OpenAttempt::NotStored`], the state of version 1 only.
pub fn encode_attempt_archive(
    attempts: &[AttemptRecord],
    open: &OpenAttempt,
) -> Result<StoredAttemptSnapshot, ArchiveDecodeError> {
    check_contiguous_for_encode(attempts)?;
    let open = match open {
        OpenAttempt::Ended => Value::Null,
        OpenAttempt::Open(facts) => open_attempt_value(facts),
        OpenAttempt::NotStored => {
            return Err(corrupt(
                ArchiveDomain::Attempts,
                "version 2 stores the open attempt",
            ))
        }
    };
    let rows: Vec<Value> = attempts
        .iter()
        .map(|attempt| {
            let mut fields = version_1_fields(attempt);
            fields.extend(phase_fields(&attempt.phases));
            Value::Array(fields)
        })
        .collect();
    stored(
        ARCHIVE_VERSION_2,
        &Value::Array(vec![Value::Array(rows), open]),
    )
}

fn check_contiguous_for_encode(attempts: &[AttemptRecord]) -> Result<(), ArchiveDecodeError> {
    match attempts
        .iter()
        .enumerate()
        .all(|(index, attempt)| attempt.attempt as usize == index + 1)
    {
        true => Ok(()),
        false => Err(corrupt(
            ArchiveDomain::Attempts,
            "attempts must be ordered and contiguous from 1",
        )),
    }
}

fn stored(version: i16, value: &Value) -> Result<StoredAttemptSnapshot, ArchiveDecodeError> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| corrupt(ArchiveDomain::Attempts, error.to_string()))?;
    Ok(StoredAttemptSnapshot {
        version,
        codec: JSON_UTF8_CODEC,
        content_type: JSON_CONTENT_TYPE,
        digest: archive_digest(&payload),
        payload,
    })
}

/// The attempts of a stored archive of either version, without the open
/// attempt ([`decode_attempt_archive`]).
pub fn decode_attempt_snapshot(
    version: i16,
    codec: &str,
    content_type: &str,
    payload: &[u8],
    digest: &[u8],
) -> Result<Vec<AttemptRecord>, ArchiveDecodeError> {
    decode_attempt_archive(version, codec, content_type, payload, digest)
        .map(AttemptArchive::into_attempts)
}

/// Decodes a stored archive: the envelope first (a retained version, the
/// codec, the content type), then the digest, then the payload of its
/// version. A version-1 archive decodes with every attempt
/// [`PhaseRecord::NotCaptured`] and the open attempt
/// [`OpenAttempt::NotStored`].
///
/// Errors: `UnknownVersion`, `UnknownCodec`, `UnknownContentType`,
/// `DigestMismatch`, or `Corrupt` with the detail of the first value that
/// does not read.
pub fn decode_attempt_archive(
    version: i16,
    codec: &str,
    content_type: &str,
    payload: &[u8],
    digest: &[u8],
) -> Result<AttemptArchive, ArchiveDecodeError> {
    validate_envelope_contract(ArchiveDomain::Attempts, version, codec, content_type)?;
    verify_payload_digest(ArchiveDomain::Attempts, payload, digest)?;
    decode_payload(version, payload)
}

/// The attempts of a live task, without the open attempt
/// ([`decode_live_attempt_archive`]).
pub fn decode_live_attempt_snapshot(
    version: i16,
    payload: &[u8],
) -> Result<Vec<AttemptRecord>, ArchiveDecodeError> {
    decode_live_attempt_archive(version, payload).map(AttemptArchive::into_attempts)
}

/// Decodes the archive of a live task, which the database encodes at read
/// time in the form that a move stores (`horsies_encode_attempt_archive`).
/// A live encoding has no stored digest, so there is none to check; the
/// envelope version is checked as in [`decode_attempt_archive`].
pub fn decode_live_attempt_archive(
    version: i16,
    payload: &[u8],
) -> Result<AttemptArchive, ArchiveDecodeError> {
    validate_envelope_contract(
        ArchiveDomain::Attempts,
        version,
        JSON_UTF8_CODEC,
        JSON_CONTENT_TYPE,
    )?;
    decode_payload(version, payload)
}

/// The payload of `version`, a retained version.
fn decode_payload(version: i16, payload: &[u8]) -> Result<AttemptArchive, ArchiveDecodeError> {
    let parsed: Value = serde_json::from_slice(payload)
        .map_err(|_| corrupt(ArchiveDomain::Attempts, "JSONDecodeError"))?;
    match version {
        ARCHIVE_VERSION_1 => Ok(AttemptArchive {
            attempts: decode_attempt_rows(&parsed, ATTEMPT_FIELD_COUNT)?,
            open: OpenAttempt::NotStored,
        }),
        ARCHIVE_VERSION_2 => decode_version_2(&parsed),
        other => Err(ArchiveDecodeError::UnknownVersion {
            domain: ArchiveDomain::Attempts,
            version: other,
        }),
    }
}

/// `[attempts, open]` of version 2.
fn decode_version_2(parsed: &Value) -> Result<AttemptArchive, ArchiveDecodeError> {
    let items = parsed
        .as_array()
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, "expected_array"))?;
    let [attempts, open] = items.as_slice() else {
        return Err(corrupt(ArchiveDomain::Attempts, "wrong_item_count"));
    };
    Ok(AttemptArchive {
        attempts: decode_attempt_rows(attempts, ATTEMPT_V2_FIELD_COUNT)?,
        open: decode_open_attempt(open)?,
    })
}

/// The attempt records of an array of rows of `field_count` values each
/// (version 1: [`ATTEMPT_FIELD_COUNT`]; version 2:
/// [`ATTEMPT_V2_FIELD_COUNT`]), contiguous from 1.
fn decode_attempt_rows(
    rows: &Value,
    field_count: usize,
) -> Result<Vec<AttemptRecord>, ArchiveDecodeError> {
    let rows = rows
        .as_array()
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, "expected_array"))?;
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        records.push(decode_positional_row(row, field_count)?);
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

fn version_1_fields(record: &AttemptRecord) -> Vec<Value> {
    vec![
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
    ]
}

/// The [`PHASE_FIELD_COUNT`] values of a phase record in version 2.
fn phase_fields(phases: &PhaseRecord) -> [Value; PHASE_FIELD_COUNT] {
    let phases = match phases {
        PhaseRecord::NotCaptured => return std::array::from_fn(|_| Value::Null),
        PhaseRecord::Captured(phases) => phases,
    };
    let (claims, first_claimed_at) = match phases.claims {
        ClaimRecord::NotCounted => (Value::Null, Value::Null),
        ClaimRecord::Counted {
            count,
            first_claimed_at,
        } => (Value::from(count.get()), micros(first_claimed_at)),
    };
    let (placed_at, recorded_by, last_seen_at) = match phases.recorded_by {
        RecordedBy::Worker { placement } => (
            match placement {
                Placement::Placed { at } => micros(at),
                Placement::NotObserved => Value::Null,
            },
            RECORDED_BY_WORKER,
            Value::Null,
        ),
        RecordedBy::Recovery { last_seen_at } => {
            (Value::Null, RECORDED_BY_RECOVERY, micros(last_seen_at))
        }
    };
    [
        Value::from(PHASE_RECORD_VERSION_1),
        micros(phases.enqueued_at),
        micros(phases.claimed_at),
        claims,
        first_claimed_at,
        placed_at,
        finalizing_value(phases.finalizing),
        Value::from(recorded_by),
        last_seen_at,
    ]
}

/// `[attempt_claims, first_claimed_at, finalizing_at]` of an open attempt.
fn open_attempt_value(facts: &OpenAttemptFacts) -> Value {
    let (claims, first_claimed_at) = match facts.claims {
        OpenClaims::NotCounted => (Value::Null, Value::Null),
        OpenClaims::NotClaimed => (Value::from(0_u32), Value::Null),
        OpenClaims::Counted {
            count,
            first_claimed_at,
        } => (Value::from(count.get()), micros(first_claimed_at)),
    };
    Value::Array(vec![
        claims,
        first_claimed_at,
        finalizing_value(facts.finalizing),
    ])
}

fn finalizing_value(finalizing: Finalizing) -> Value {
    match finalizing {
        Finalizing::Marked { at } => micros(at),
        Finalizing::NotMarked => Value::Null,
    }
}

fn micros(at: DateTime<Utc>) -> Value {
    Value::from(at.timestamp_micros())
}

/// One attempt row of `field_count` values: the values of version 1, then,
/// in version 2, the phase record.
fn decode_positional_row(
    value: &Value,
    field_count: usize,
) -> Result<AttemptRecord, ArchiveDecodeError> {
    let row = value
        .as_array()
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, "expected_positional_row"))?;
    if row.len() != field_count {
        return Err(corrupt(ArchiveDomain::Attempts, "wrong_field_count"));
    }
    let (version_1, phase) = row.split_at(ATTEMPT_FIELD_COUNT);
    let record = decode_version_1_fields(version_1)?;
    match phase.is_empty() {
        true => Ok(record),
        false => Ok(record.with_phases(decode_phase_record(phase)?)),
    }
}

fn decode_version_1_fields(row: &[Value]) -> Result<AttemptRecord, ArchiveDecodeError> {
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

/// The [`PHASE_FIELD_COUNT`] values of a version-2 attempt: no phase record
/// (every value `null`), or a record of [`PHASE_RECORD_VERSION_1`].
fn decode_phase_record(fields: &[Value]) -> Result<PhaseRecord, ArchiveDecodeError> {
    let [version, enqueued_at, claimed_at, claims, first_claimed_at, placed_at, finalizing_at, recorded_by, last_seen_at] =
        fields
    else {
        return Err(corrupt(ArchiveDomain::Attempts, "wrong_field_count"));
    };
    if version.is_null() {
        return match fields.iter().all(Value::is_null) {
            true => Ok(PhaseRecord::NotCaptured),
            false => Err(corrupt(
                ArchiveDomain::Attempts,
                "phase_values_without_version",
            )),
        };
    }
    if plain_i64(version, "invalid_phase_record_version")? != PHASE_RECORD_VERSION_1 {
        return Err(corrupt(
            ArchiveDomain::Attempts,
            "unknown_phase_record_version",
        ));
    }
    let placement = match placed_at {
        Value::Null => Placement::NotObserved,
        value => Placement::Placed {
            at: decode_epoch_us(value, "invalid_placed_at")?,
        },
    };
    let recorded_by = match (recorded_by.as_str(), placement, last_seen_at) {
        (Some(RECORDED_BY_WORKER), placement, Value::Null) => RecordedBy::Worker { placement },
        (Some(RECORDED_BY_RECOVERY), Placement::NotObserved, last_seen_at) => {
            RecordedBy::Recovery {
                last_seen_at: decode_epoch_us(last_seen_at, "invalid_last_seen_at")?,
            }
        }
        (Some(RECORDED_BY_WORKER | RECORDED_BY_RECOVERY), _, _) => {
            return Err(corrupt(ArchiveDomain::Attempts, "recorded_by_shape"))
        }
        (Some(_) | None, _, _) => {
            return Err(corrupt(ArchiveDomain::Attempts, "invalid_recorded_by"))
        }
    };
    Ok(PhaseRecord::Captured(AttemptPhases {
        enqueued_at: decode_epoch_us(enqueued_at, "invalid_enqueued_at")?,
        claimed_at: decode_epoch_us(claimed_at, "invalid_claimed_at")?,
        claims: match (claims, first_claimed_at) {
            (Value::Null, Value::Null) => ClaimRecord::NotCounted,
            (count, first_claimed_at) => ClaimRecord::Counted {
                count: claim_count(count, "invalid_attempt_claims")?,
                first_claimed_at: decode_epoch_us(first_claimed_at, "invalid_first_claimed_at")?,
            },
        },
        finalizing: decode_finalizing(finalizing_at)?,
        recorded_by,
    }))
}

/// The open attempt of version 2: `null`, or `[attempt_claims,
/// first_claimed_at, finalizing_at]`.
fn decode_open_attempt(value: &Value) -> Result<OpenAttempt, ArchiveDecodeError> {
    let fields = match value {
        Value::Null => return Ok(OpenAttempt::Ended),
        Value::Array(fields) => fields,
        _ => return Err(corrupt(ArchiveDomain::Attempts, "invalid_open_attempt")),
    };
    let [claims, first_claimed_at, finalizing_at] = fields.as_slice() else {
        return Err(corrupt(
            ArchiveDomain::Attempts,
            "wrong_open_attempt_field_count",
        ));
    };
    let claims = match (claims, first_claimed_at) {
        (Value::Null, Value::Null) => OpenClaims::NotCounted,
        (count, Value::Null) if count.as_i64() == Some(0) => OpenClaims::NotClaimed,
        (count, first_claimed_at) => OpenClaims::Counted {
            count: claim_count(count, "invalid_open_attempt_claims")?,
            first_claimed_at: decode_epoch_us(first_claimed_at, "invalid_open_first_claimed_at")?,
        },
    };
    Ok(OpenAttempt::Open(OpenAttemptFacts {
        claims,
        finalizing: decode_finalizing(finalizing_at)?,
    }))
}

fn decode_finalizing(value: &Value) -> Result<Finalizing, ArchiveDecodeError> {
    match value {
        Value::Null => Ok(Finalizing::NotMarked),
        value => Ok(Finalizing::Marked {
            at: decode_epoch_us(value, "invalid_finalizing_at")?,
        }),
    }
}

/// A claim count of 1 or more.
fn claim_count(value: &Value, detail: &'static str) -> Result<NonZeroU32, ArchiveDecodeError> {
    value
        .as_u64()
        .and_then(|count| u32::try_from(count).ok())
        .and_then(NonZeroU32::new)
        .ok_or_else(|| corrupt(ArchiveDomain::Attempts, detail))
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

    fn at(micros: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(micros).unwrap()
    }

    fn count(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
    }

    fn attempt(number: i32) -> AttemptRecord {
        AttemptRecord::new(
            number,
            "FAILED",
            true,
            at(1_000),
            at(2_000),
            Some("E".to_owned()),
            None,
            None,
            Some("w1".to_owned()),
            Some("host".to_owned()),
            Some(7),
            Some("proc".to_owned()),
        )
        .unwrap()
    }

    fn worker_phases(placement: Placement, finalizing: Finalizing) -> PhaseRecord {
        PhaseRecord::Captured(AttemptPhases::new(
            at(100),
            at(300),
            ClaimRecord::Counted {
                count: count(2),
                first_claimed_at: at(200),
            },
            finalizing,
            RecordedBy::Worker { placement },
        ))
    }

    fn decode_v2(payload: &[u8]) -> Result<AttemptArchive, ArchiveDecodeError> {
        decode_attempt_archive(
            ARCHIVE_VERSION_2,
            JSON_UTF8_CODEC,
            JSON_CONTENT_TYPE,
            payload,
            &archive_digest(payload),
        )
    }

    fn corrupt_v2_detail(payload: &serde_json::Value) -> String {
        match decode_v2(&serde_json::to_vec(payload).unwrap()).unwrap_err() {
            ArchiveDecodeError::Corrupt { detail, .. } => detail,
            other => panic!("expected corrupt value, got {other:?}"),
        }
    }

    /// The 21 values of a version-2 attempt with the phase values `phase`.
    fn v2_row(phase: &serde_json::Value) -> serde_json::Value {
        let mut row = serde_json::json!([
            1, "FAILED", true, 1000, 2000, "E", null, null, "w1", "host", 7, "proc"
        ]);
        row.as_array_mut()
            .unwrap()
            .extend(phase.as_array().unwrap().iter().cloned());
        row
    }

    /// The bytes of version 2: the form that `horsies_encode_attempt_archive`
    /// writes, with no space between tokens.
    #[test]
    fn version_two_encodes_the_pinned_bytes() {
        let attempts = [
            attempt(1).with_phases(worker_phases(
                Placement::Placed { at: at(350) },
                Finalizing::Marked { at: at(1_500) },
            )),
            attempt(2).with_phases(PhaseRecord::Captured(AttemptPhases::new(
                at(2_100),
                at(2_200),
                ClaimRecord::NotCounted,
                Finalizing::NotMarked,
                RecordedBy::Recovery {
                    last_seen_at: at(2_400),
                },
            ))),
            attempt(3),
        ];
        let open = OpenAttempt::Open(OpenAttemptFacts::new(
            OpenClaims::Counted {
                count: count(1),
                first_claimed_at: at(3_000),
            },
            Finalizing::NotMarked,
        ));
        let encoded = encode_attempt_archive(&attempts, &open).unwrap();
        let row = |number: i32| {
            format!(r#"{number},"FAILED",true,1000,2000,"E",null,null,"w1","host",7,"proc""#)
        };
        let (first, second, third) = (row(1), row(2), row(3));
        let expected = format!(
            r#"[[[{first},1,100,300,2,200,350,1500,"WORKER",null],[{second},1,2100,2200,null,null,null,null,"RECOVERY",2400],[{third},null,null,null,null,null,null,null,null,null]],[1,3000,null]]"#
        );
        assert_eq!(
            String::from_utf8(encoded.payload.clone()).unwrap(),
            expected
        );
        assert_eq!(encoded.version, ARCHIVE_VERSION_2);
        assert_eq!(encoded.digest, archive_digest(&encoded.payload));
    }

    /// Each named state of each phase value and of the open attempt
    /// survives an encode and a decode of version 2.
    #[test]
    fn version_two_keeps_each_named_state() {
        let phase_records = [
            PhaseRecord::NotCaptured,
            worker_phases(Placement::Placed { at: at(350) }, Finalizing::NotMarked),
            worker_phases(Placement::NotObserved, Finalizing::Marked { at: at(400) }),
            PhaseRecord::Captured(AttemptPhases::new(
                at(100),
                at(300),
                ClaimRecord::NotCounted,
                Finalizing::NotMarked,
                RecordedBy::Recovery {
                    last_seen_at: at(900),
                },
            )),
        ];
        let opens = [
            OpenAttempt::Ended,
            OpenAttempt::Open(OpenAttemptFacts::new(
                OpenClaims::NotCounted,
                Finalizing::NotMarked,
            )),
            OpenAttempt::Open(OpenAttemptFacts::new(
                OpenClaims::NotClaimed,
                Finalizing::NotMarked,
            )),
            OpenAttempt::Open(OpenAttemptFacts::new(
                OpenClaims::Counted {
                    count: count(3),
                    first_claimed_at: at(10),
                },
                Finalizing::Marked { at: at(20) },
            )),
        ];
        for phases in phase_records {
            for open in opens {
                let attempts = vec![attempt(1).with_phases(phases)];
                let encoded = encode_attempt_archive(&attempts, &open).unwrap();
                let decoded = decode_v2(&encoded.payload).unwrap();
                assert_eq!(decoded.attempts(), attempts.as_slice());
                assert_eq!(decoded.open(), open);
            }
        }
    }

    /// Version 1 decodes with no phase record and no open attempt, through
    /// the stored and the live reads.
    #[test]
    fn version_one_decodes_as_not_captured_and_not_stored() {
        let encoded = encode_attempt_snapshot(&[attempt(1), attempt(2)]).unwrap();
        let archive = decode_attempt_archive(
            ARCHIVE_VERSION_1,
            JSON_UTF8_CODEC,
            JSON_CONTENT_TYPE,
            &encoded.payload,
            &encoded.digest,
        )
        .unwrap();
        assert_eq!(archive.open(), OpenAttempt::NotStored);
        assert!(archive
            .attempts()
            .iter()
            .all(|record| record.phases() == PhaseRecord::NotCaptured));
        let live = decode_live_attempt_archive(ARCHIVE_VERSION_1, &encoded.payload).unwrap();
        assert_eq!(live, archive);
    }

    /// The live read decodes version 2 with no digest.
    #[test]
    fn the_live_read_decodes_version_two() {
        let attempts =
            vec![attempt(1)
                .with_phases(worker_phases(Placement::NotObserved, Finalizing::NotMarked))];
        let encoded = encode_attempt_archive(&attempts, &OpenAttempt::Ended).unwrap();
        assert_eq!(
            decode_live_attempt_snapshot(ARCHIVE_VERSION_2, &encoded.payload).unwrap(),
            attempts
        );
    }

    /// Version 1 cannot store a phase record, and version 2 always stores
    /// the open attempt: each encode refuses the other's state.
    #[test]
    fn each_version_refuses_a_state_it_cannot_store() {
        let captured =
            attempt(1).with_phases(worker_phases(Placement::NotObserved, Finalizing::NotMarked));
        assert!(matches!(
            encode_attempt_snapshot(&[captured]),
            Err(ArchiveDecodeError::Corrupt { .. })
        ));
        assert!(matches!(
            encode_attempt_archive(&[attempt(1)], &OpenAttempt::NotStored),
            Err(ArchiveDecodeError::Corrupt { .. })
        ));
        assert!(matches!(
            encode_attempt_archive(&[attempt(2)], &OpenAttempt::Ended),
            Err(ArchiveDecodeError::Corrupt { .. })
        ));
    }

    /// Each corrupt version-2 payload fails with its own detail.
    #[test]
    fn corrupt_version_two_payloads_fail_with_pinned_details() {
        let none = serde_json::json!([null, null, null, null, null, null, null, null, null]);
        let worker = |placed: serde_json::Value, last_seen: serde_json::Value| {
            serde_json::json!([1, 100, 300, 2, 200, placed, null, "WORKER", last_seen])
        };
        let recovery = |placed: serde_json::Value| {
            serde_json::json!([1, 100, 300, null, null, placed, null, "RECOVERY", 900])
        };
        let second_v2_row = {
            let mut second = v2_row(&none);
            second[0] = serde_json::json!(3);
            second
        };
        let cases = [
            (serde_json::json!({}), "expected_array"),
            (serde_json::json!([[]]), "wrong_item_count"),
            (serde_json::json!([[], null, null]), "wrong_item_count"),
            (serde_json::json!([{}, null]), "expected_array"),
            (
                serde_json::json!([
                    [[1, "FAILED", true, 1000, 2000, "E", null, null, "w1", "host", 7, "proc"]],
                    null
                ]),
                "wrong_field_count",
            ),
            (
                serde_json::json!([[v2_row(&none), second_v2_row.clone()], null]),
                "non_contiguous_attempts",
            ),
            (
                serde_json::json!([
                    [v2_row(&serde_json::json!([
                        null, 100, null, null, null, null, null, null, null
                    ]))],
                    null
                ]),
                "phase_values_without_version",
            ),
            (
                serde_json::json!([
                    [v2_row(&serde_json::json!([
                        2, 100, 300, null, null, null, null, "WORKER", null
                    ]))],
                    null
                ]),
                "unknown_phase_record_version",
            ),
            (
                serde_json::json!([
                    [v2_row(&worker(
                        serde_json::json!(null),
                        serde_json::json!(5)
                    ))],
                    null
                ]),
                "recorded_by_shape",
            ),
            (
                serde_json::json!([[v2_row(&recovery(serde_json::json!(350)))], null]),
                "recorded_by_shape",
            ),
            (
                serde_json::json!([
                    [v2_row(&serde_json::json!([
                        1, 100, 300, null, null, null, null, "CANCEL", null
                    ]))],
                    null
                ]),
                "invalid_recorded_by",
            ),
            (
                serde_json::json!([
                    [v2_row(&serde_json::json!([
                        1, 100, 300, 0, 200, null, null, "WORKER", null
                    ]))],
                    null
                ]),
                "invalid_attempt_claims",
            ),
            (
                serde_json::json!([
                    [v2_row(&serde_json::json!([
                        1, 100, 300, 2, null, null, null, "WORKER", null
                    ]))],
                    null
                ]),
                "invalid_first_claimed_at",
            ),
            (
                serde_json::json!([
                    [v2_row(&serde_json::json!([
                        1, null, 300, null, null, null, null, "WORKER", null
                    ]))],
                    null
                ]),
                "invalid_enqueued_at",
            ),
            (serde_json::json!([[], 7]), "invalid_open_attempt"),
            (
                serde_json::json!([[], [0, null]]),
                "wrong_open_attempt_field_count",
            ),
            (
                serde_json::json!([[], [0, 5, null]]),
                "invalid_open_attempt_claims",
            ),
            (
                serde_json::json!([[], [null, 5, null]]),
                "invalid_open_attempt_claims",
            ),
            (
                serde_json::json!([[], [2, null, null]]),
                "invalid_open_first_claimed_at",
            ),
            (
                serde_json::json!([[], [null, null, "x"]]),
                "invalid_finalizing_at",
            ),
        ];
        for (payload, detail) in cases {
            assert_eq!(corrupt_v2_detail(&payload), detail, "{payload}");
        }
    }

    /// A version-2 payload with a wrong digest, and a version the domain does
    /// not retain, fail before the payload is read.
    #[test]
    fn version_two_checks_the_digest_and_refuses_version_three() {
        let payload = br#"[[],null]"#;
        assert!(matches!(
            decode_attempt_archive(
                ARCHIVE_VERSION_2,
                JSON_UTF8_CODEC,
                JSON_CONTENT_TYPE,
                payload,
                &[0; 32]
            ),
            Err(ArchiveDecodeError::DigestMismatch { .. })
        ));
        assert!(matches!(
            decode_attempt_archive(
                3,
                JSON_UTF8_CODEC,
                JSON_CONTENT_TYPE,
                payload,
                &archive_digest(payload)
            ),
            Err(ArchiveDecodeError::UnknownVersion { version: 3, .. })
        ));
        assert!(matches!(
            decode_live_attempt_archive(3, payload),
            Err(ArchiveDecodeError::UnknownVersion { version: 3, .. })
        ));
    }
}
