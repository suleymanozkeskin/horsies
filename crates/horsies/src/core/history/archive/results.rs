//! Result-envelope codec, version 1.

use serde_json::Value;

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
    serde_json::from_slice::<Value>(&payload)?;
    Ok(StoredResultEnvelope {
        version: ARCHIVE_VERSION_1,
        codec: JSON_UTF8_CODEC,
        content_type: JSON_CONTENT_TYPE,
        digest: archive_digest(&payload),
        payload,
    })
}

pub fn decode_result_envelope(
    version: i16,
    codec: &str,
    content_type: &str,
    payload: &[u8],
    digest: &[u8],
) -> Result<Value, ArchiveDecodeError> {
    validate_envelope_contract(ArchiveDomain::Result, version, codec, content_type)?;
    verify_payload_digest(ArchiveDomain::Result, payload, digest)?;
    serde_json::from_slice(payload).map_err(|_| corrupt(ArchiveDomain::Result, "JSONDecodeError"))
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
