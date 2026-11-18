//! Result-envelope codec, version 1.

use crate::core::codec::check_json_bytes;

use super::versions::{
    archive_digest, corrupt, validate_envelope_contract, verify_payload_digest, ArchiveDecodeError,
    ArchiveDomain, ARCHIVE_VERSION_1, JSON_CONTENT_TYPE, JSON_UTF8_CODEC,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredResultEnvelope {
    pub version: i16,
    pub codec: &'static str,
    pub content_type: &'static str,
    pub payload: Vec<u8>,
    pub digest: [u8; 32],
}

pub fn encode_result_envelope(
    result_json: &str,
) -> Result<StoredResultEnvelope, serde_json::Error> {
    let payload = result_json.as_bytes().to_vec();
    // A syntax check: the value is skipped, not built, so no number is
    // converted and an integer of any width passes as it is stored.
    serde_json::from_slice::<serde::de::IgnoredAny>(&payload)?;
    Ok(StoredResultEnvelope {
        version: ARCHIVE_VERSION_1,
        codec: JSON_UTF8_CODEC,
        content_type: JSON_CONTENT_TYPE,
        digest: archive_digest(&payload),
        payload,
    })
}

/// Decode one stored result payload to its JSON value (Python
/// `decode_result_envelope`, `core/history/archive/results.py:28-51`), for
/// the history vector tests. A `serde_json::Value` holds an integer outside
/// the `i64` and `u64` range as an `f64`, so no production path reads a
/// result this way: the reads take [`decode_result_envelope_text`]. Pure.
#[cfg(test)]
pub(crate) fn decode_result_envelope(
    version: i16,
    codec: &str,
    content_type: &str,
    payload: &[u8],
    digest: &[u8],
) -> Result<serde_json::Value, ArchiveDecodeError> {
    validate_envelope_contract(ArchiveDomain::Result, version, codec, content_type)?;
    verify_payload_digest(ArchiveDomain::Result, payload, digest)?;
    crate::core::codec::from_json_bytes::<serde_json::Value>(payload)
        .map_err(|_| corrupt(ArchiveDomain::Result, "JSONDecodeError"))
}

/// The text of one stored result payload, checked as Python's
/// `decode_result_envelope` checks it (`core/history/archive/results.py:28-51`),
/// without building a value. Pure.
///
/// Checks the envelope version, codec and content type, then the digest,
/// then the payload under the strict read rule of a stored text
/// ([`check_json_bytes`]): an empty or non-UTF-8 payload, a repeated key,
/// `NaN`, a float out of the binary64 range, an integer of more than
/// [`crate::core::codec::MAX_INT_DIGITS`] digits and nesting past
/// [`crate::core::codec::MAX_JSON_DEPTH`] levels are refused. The text keeps
/// every digit, so a reader decodes the value that Python stored.
///
/// Errors: the [`ArchiveDecodeError`] of the envelope or the digest check;
/// [`ArchiveDecodeError::Corrupt`] with the detail `JSONDecodeError` for a
/// payload that the strict read rule refuses.
pub fn decode_result_envelope_text(
    version: i16,
    codec: &str,
    content_type: &str,
    payload: &[u8],
    digest: &[u8],
) -> Result<String, ArchiveDecodeError> {
    validate_envelope_contract(ArchiveDomain::Result, version, codec, content_type)?;
    verify_payload_digest(ArchiveDomain::Result, payload, digest)?;
    check_json_bytes(payload).map_err(|_| corrupt(ArchiveDomain::Result, "JSONDecodeError"))?;
    String::from_utf8(payload.to_vec())
        .map_err(|_| corrupt(ArchiveDomain::Result, "JSONDecodeError"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultPayloadSelection<'a> {
    Canonical(&'a [u8]),
    AdministrativePrior(&'a [u8]),
    None,
}

pub fn select_result_payload<'a>(
    canonical: Option<&'a [u8]>,
    prior: Option<&'a [u8]>,
) -> Result<ResultPayloadSelection<'a>, ArchiveDecodeError> {
    match (canonical, prior) {
        (None, None) => Ok(ResultPayloadSelection::None),
        (Some(payload), None) => Ok(ResultPayloadSelection::Canonical(payload)),
        (None, Some(payload)) => Ok(ResultPayloadSelection::AdministrativePrior(payload)),
        (Some(_), Some(_)) => Err(corrupt(
            ArchiveDomain::Result,
            "canonical_and_prior_both_present",
        )),
    }
}
