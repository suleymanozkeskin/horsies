//! Canonical v27 facts prepared for every live-task enqueue.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::archive::rerun_input::RerunInputDisposition;
use super::ddl::classes::resolve_retention_class_key;
use super::identity::fingerprint::{
    EnqueueCommandV1, FingerprintError, COMMAND_FINGERPRINT_VERSION,
};
use super::identity::keys::{IdempotencyKeyError, ScopedIdempotencyKey};
use super::rerun::input_envelope::{
    encode_input_envelope_v1, INPUT_ENVELOPE_CODEC, INPUT_ENVELOPE_CONTENT_TYPE,
    INPUT_ENVELOPE_INLINE_MAX_BYTES, INPUT_ENVELOPE_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueInputEligibility {
    Ordinary,
    NeverEligible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEnqueueFacts {
    pub command_fingerprint_version: i16,
    pub command_fingerprint: [u8; 32],
    pub retention_class_key: String,
    pub input_digest: [u8; 32],
    pub idempotency_key_digest: Option<[u8; 32]>,
    pub retain_rerun_input: bool,
    pub prepared_rerun_input_disposition: RerunInputDisposition,
    pub prepared_rerun_input_version: Option<i16>,
    pub prepared_rerun_input_codec: Option<&'static str>,
    pub prepared_rerun_input_content_type: Option<&'static str>,
    pub prepared_rerun_input_digest: Option<[u8; 32]>,
    pub prepared_rerun_input_inline: Option<Vec<u8>>,
    pub prepared_rerun_input_reference: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueuePreparationError {
    #[error("{field} JSON is corrupt: {source}")]
    Json {
        field: &'static str,
        source: serde_json::Error,
    },
    #[error("args JSON must encode positional values")]
    InvalidArgs,
    #[error("kwargs JSON must encode an object")]
    InvalidKwargs,
    #[error("task_options JSON must encode an object")]
    InvalidTaskOptions,
    #[error("input envelope serialization failed: {0}")]
    Envelope(#[from] serde_json::Error),
    #[error("command fingerprint failed: {0}")]
    Fingerprint(#[from] FingerprintError),
    #[error("idempotency key is invalid: {0}")]
    Idempotency(#[from] IdempotencyKeyError),
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_enqueue_facts(
    task_name: &str,
    queue_name: &str,
    priority: i32,
    args_json: Option<&str>,
    kwargs_json: Option<&str>,
    good_until: Option<DateTime<Utc>>,
    enqueue_delay_seconds: Option<i64>,
    task_options_json: Option<&str>,
    retention_class_key: Option<&str>,
    retain_rerun_input: bool,
    idempotency_key: Option<&str>,
    eligibility: EnqueueInputEligibility,
) -> Result<PreparedEnqueueFacts, EnqueuePreparationError> {
    prepare_enqueue_facts_with_lineage(
        task_name,
        queue_name,
        priority,
        args_json,
        kwargs_json,
        good_until,
        enqueue_delay_seconds,
        task_options_json,
        retention_class_key,
        retain_rerun_input,
        idempotency_key,
        eligibility,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_enqueue_facts_with_lineage(
    task_name: &str,
    queue_name: &str,
    priority: i32,
    args_json: Option<&str>,
    kwargs_json: Option<&str>,
    good_until: Option<DateTime<Utc>>,
    enqueue_delay_seconds: Option<i64>,
    task_options_json: Option<&str>,
    retention_class_key: Option<&str>,
    retain_rerun_input: bool,
    idempotency_key: Option<&str>,
    eligibility: EnqueueInputEligibility,
    rerun_of_task_id: Option<Uuid>,
    rerun_root_task_id: Option<Uuid>,
) -> Result<PreparedEnqueueFacts, EnqueuePreparationError> {
    let args = parse_args(args_json)?;
    let kwargs = parse_object(
        "kwargs",
        kwargs_json,
        EnqueuePreparationError::InvalidKwargs,
    )?;
    let options = parse_optional_object(task_options_json)?;
    let payload = encode_input_envelope_v1(&args, &kwargs, options.as_ref())?;
    let input_digest: [u8; 32] = Sha256::digest(&payload).into();
    let retention_class_key = resolve_retention_class_key(retention_class_key).to_owned();
    let command = EnqueueCommandV1::new(
        task_name,
        queue_name,
        priority,
        args_json.map(str::to_owned),
        kwargs_json.map(str::to_owned),
        good_until,
        enqueue_delay_seconds,
        task_options_json.map(str::to_owned),
        &retention_class_key,
        retain_rerun_input,
        rerun_of_task_id,
        rerun_root_task_id,
    )?;
    let idempotency_key_digest = idempotency_key
        .map(|key| ScopedIdempotencyKey::new(task_name, key).map(|scoped| scoped.digest()))
        .transpose()?;

    let (disposition, version, codec, content_type, digest, inline) = match eligibility {
        EnqueueInputEligibility::NeverEligible => (
            RerunInputDisposition::NeverEligible,
            None,
            None,
            None,
            None,
            None,
        ),
        EnqueueInputEligibility::Ordinary if !retain_rerun_input => (
            RerunInputDisposition::DeclinedByPolicy,
            None,
            None,
            None,
            None,
            None,
        ),
        EnqueueInputEligibility::Ordinary if payload.len() > INPUT_ENVELOPE_INLINE_MAX_BYTES => (
            RerunInputDisposition::OverBound,
            None,
            None,
            None,
            None,
            None,
        ),
        EnqueueInputEligibility::Ordinary => (
            RerunInputDisposition::Inline,
            Some(INPUT_ENVELOPE_VERSION),
            Some(INPUT_ENVELOPE_CODEC),
            Some(INPUT_ENVELOPE_CONTENT_TYPE),
            Some(input_digest),
            Some(payload),
        ),
    };

    Ok(PreparedEnqueueFacts {
        command_fingerprint_version: COMMAND_FINGERPRINT_VERSION,
        command_fingerprint: command.fingerprint()?,
        retention_class_key,
        input_digest,
        idempotency_key_digest,
        retain_rerun_input,
        prepared_rerun_input_disposition: disposition,
        prepared_rerun_input_version: version,
        prepared_rerun_input_codec: codec,
        prepared_rerun_input_content_type: content_type,
        prepared_rerun_input_digest: digest,
        prepared_rerun_input_inline: inline,
        prepared_rerun_input_reference: None,
    })
}

fn parse_args(value: Option<&str>) -> Result<Vec<Value>, EnqueuePreparationError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let parsed: Value =
        serde_json::from_str(value).map_err(|source| EnqueuePreparationError::Json {
            field: "args",
            source,
        })?;
    Ok(match parsed {
        Value::Array(values) => values,
        Value::Null => Vec::new(),
        scalar @ (Value::Bool(_) | Value::Number(_) | Value::String(_)) => vec![scalar],
        Value::Object(_) => return Err(EnqueuePreparationError::InvalidArgs),
    })
}

fn parse_object(
    field: &'static str,
    value: Option<&str>,
    shape_error: EnqueuePreparationError,
) -> Result<Map<String, Value>, EnqueuePreparationError> {
    let Some(value) = value else {
        return Ok(Map::new());
    };
    let parsed: Value = serde_json::from_str(value)
        .map_err(|source| EnqueuePreparationError::Json { field, source })?;
    match parsed {
        Value::Object(object) => Ok(object),
        Value::Null => Ok(Map::new()),
        _ => Err(shape_error),
    }
}

fn parse_optional_object(
    value: Option<&str>,
) -> Result<Option<Map<String, Value>>, EnqueuePreparationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let parsed: Value =
        serde_json::from_str(value).map_err(|source| EnqueuePreparationError::Json {
            field: "task_options",
            source,
        })?;
    match parsed {
        Value::Object(object) => Ok(Some(object)),
        Value::Null => Ok(None),
        _ => Err(EnqueuePreparationError::InvalidTaskOptions),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_facts_pin_inline_declined_over_bound_and_forever() {
        let inline = prepare_enqueue_facts(
            "task",
            "default",
            50,
            Some("[1]"),
            Some("{\"z\":1}"),
            None,
            None,
            Some("{}"),
            Some("standard_30d"),
            true,
            Some("request-1"),
            EnqueueInputEligibility::Ordinary,
        )
        .unwrap();
        assert_eq!(inline.prepared_rerun_input_disposition.as_str(), "INLINE");
        assert_eq!(inline.command_fingerprint_version, 1);
        assert_eq!(inline.retention_class_key, "standard_30d");
        assert_eq!(inline.command_fingerprint.len(), 32);
        assert_eq!(
            inline.input_digest,
            inline.prepared_rerun_input_digest.unwrap()
        );
        assert!(inline.idempotency_key_digest.is_some());

        let declined = prepare_enqueue_facts(
            "task",
            "default",
            50,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            EnqueueInputEligibility::Ordinary,
        )
        .unwrap();
        assert_eq!(declined.retention_class_key, "forever");
        assert_eq!(
            declined.prepared_rerun_input_disposition,
            RerunInputDisposition::DeclinedByPolicy
        );
        assert!(declined.prepared_rerun_input_inline.is_none());

        let oversized_args =
            serde_json::to_string(&vec!["x".repeat(INPUT_ENVELOPE_INLINE_MAX_BYTES)]).unwrap();
        let over_bound = prepare_enqueue_facts(
            "task",
            "default",
            50,
            Some(&oversized_args),
            None,
            None,
            None,
            None,
            Some("standard_30d"),
            true,
            None,
            EnqueueInputEligibility::Ordinary,
        )
        .unwrap();
        assert_eq!(
            over_bound.prepared_rerun_input_disposition,
            RerunInputDisposition::OverBound
        );
        assert!(over_bound.prepared_rerun_input_version.is_none());
        assert!(over_bound.prepared_rerun_input_digest.is_none());
        assert!(over_bound.prepared_rerun_input_inline.is_none());
    }

    #[test]
    fn workflow_facts_are_never_eligible_but_still_fingerprinted() {
        let facts = prepare_enqueue_facts(
            "node",
            "bulk",
            10,
            Some("[1]"),
            None,
            None,
            None,
            None,
            Some("q_bulk_7d"),
            false,
            None,
            EnqueueInputEligibility::NeverEligible,
        )
        .unwrap();
        assert_eq!(
            facts.prepared_rerun_input_disposition,
            RerunInputDisposition::NeverEligible
        );
        assert_eq!(facts.command_fingerprint.len(), 32);
        assert_eq!(facts.input_digest.len(), 32);
    }
}
