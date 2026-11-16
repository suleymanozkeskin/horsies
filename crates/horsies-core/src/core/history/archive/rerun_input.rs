//! Exhaustive stored rerun-input envelope.

use super::versions::{
    archive_digest, corrupt, validate_envelope_contract, verify_payload_digest, ArchiveDecodeError,
    ArchiveDomain, ARCHIVE_VERSION_1, DIGEST_LENGTH_BYTES, JSON_CONTENT_TYPE, JSON_UTF8_CODEC,
};

pub const RERUN_INPUT_INLINE_MAX_BYTES: usize = 65_536;
pub const RERUN_INPUT_REFERENCE_MAX_BYTES: usize = 2_048;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerunInputDisposition {
    Inline,
    Reference,
    DeclinedByPolicy,
    OverBound,
    NeverEligible,
}

impl RerunInputDisposition {
    pub const ALL: [Self; 5] = [
        Self::Inline,
        Self::Reference,
        Self::DeclinedByPolicy,
        Self::OverBound,
        Self::NeverEligible,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "INLINE",
            Self::Reference => "REFERENCE",
            Self::DeclinedByPolicy => "DECLINED_BY_POLICY",
            Self::OverBound => "OVER_BOUND",
            Self::NeverEligible => "NEVER_ELIGIBLE",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "INLINE" => Some(Self::Inline),
            "REFERENCE" => Some(Self::Reference),
            "DECLINED_BY_POLICY" => Some(Self::DeclinedByPolicy),
            "OVER_BOUND" => Some(Self::OverBound),
            "NEVER_ELIGIBLE" => Some(Self::NeverEligible),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_disposition_set_and_unavailability_mapping_are_exact() {
        assert_eq!(
            RerunInputDisposition::ALL.map(RerunInputDisposition::as_str),
            [
                "INLINE",
                "REFERENCE",
                "DECLINED_BY_POLICY",
                "OVER_BOUND",
                "NEVER_ELIGIBLE"
            ]
        );
        for (unavailability, expected) in [
            (
                RerunInputUnavailability::DeclinedByPolicy,
                RerunInputDisposition::DeclinedByPolicy,
            ),
            (
                RerunInputUnavailability::OverBound,
                RerunInputDisposition::OverBound,
            ),
            (
                RerunInputUnavailability::NeverEligible,
                RerunInputDisposition::NeverEligible,
            ),
        ] {
            assert_eq!(
                disposition_of(&store_unavailable_rerun_input(unavailability)),
                expected
            );
        }
    }

    #[test]
    fn storage_bounds_are_utf8_byte_exact() {
        assert!(store_inline_rerun_input(&vec![0; RERUN_INPUT_INLINE_MAX_BYTES]).is_ok());
        assert!(store_inline_rerun_input(&vec![0; RERUN_INPUT_INLINE_MAX_BYTES + 1]).is_err());
        assert!(store_referenced_rerun_input("", &[0; 32]).is_err());
        assert!(store_referenced_rerun_input(&"ü".repeat(1_024), &[0; 32]).is_ok());
        assert!(store_referenced_rerun_input(&"ü".repeat(1_025), &[0; 32]).is_err());
        assert!(store_referenced_rerun_input("sha256:abc", b"short").is_err());
    }

    #[test]
    fn available_and_unavailable_column_shapes_fail_closed() {
        let payload = b"{}";
        let digest = archive_digest(payload);
        assert!(matches!(
            decode_rerun_input("INLINE", Some(1), Some(JSON_UTF8_CODEC), Some(JSON_CONTENT_TYPE), None, Some(payload), None),
            Err(ArchiveDecodeError::Corrupt { ref detail, .. }) if detail == "invalid_inline_envelope"
        ));
        assert!(matches!(
            decode_rerun_input("REFERENCE", Some(1), Some(JSON_UTF8_CODEC), Some(JSON_CONTENT_TYPE), Some(&digest), Some(payload), Some("ref")),
            Err(ArchiveDecodeError::Corrupt { ref detail, .. }) if detail == "invalid_reference_envelope"
        ));
        assert!(matches!(
            decode_rerun_input("DECLINED_BY_POLICY", Some(1), None, None, None, None, None),
            Err(ArchiveDecodeError::Corrupt { ref detail, .. }) if detail == "unavailable_with_envelope_fields"
        ));
        assert!(matches!(
            decode_rerun_input(
                "INLINE",
                Some(3),
                Some(JSON_UTF8_CODEC),
                Some(JSON_CONTENT_TYPE),
                Some(&digest),
                Some(payload),
                None
            ),
            Err(ArchiveDecodeError::UnknownVersion { version: 3, .. })
        ));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerunInputUnavailability {
    DeclinedByPolicy,
    OverBound,
    NeverEligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerunInputUnavailableReason {
    DeclinedByPolicy,
    OverBound,
    NeverEligible,
    MissingObject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredRerunInput {
    Inline {
        version: i16,
        codec: &'static str,
        content_type: &'static str,
        payload: Vec<u8>,
        digest: [u8; 32],
    },
    Reference {
        version: i16,
        codec: &'static str,
        content_type: &'static str,
        reference: String,
        digest: [u8; 32],
    },
    Unavailable(RerunInputUnavailability),
}

pub fn store_inline_rerun_input(payload: &[u8]) -> Result<StoredRerunInput, ArchiveDecodeError> {
    if payload.len() > RERUN_INPUT_INLINE_MAX_BYTES {
        return Err(corrupt(
            ArchiveDomain::RerunInput,
            format!("inline_over_bound:{}", payload.len()),
        ));
    }
    Ok(StoredRerunInput::Inline {
        version: ARCHIVE_VERSION_1,
        codec: JSON_UTF8_CODEC,
        content_type: JSON_CONTENT_TYPE,
        payload: payload.to_vec(),
        digest: archive_digest(payload),
    })
}

pub fn store_referenced_rerun_input(
    reference: &str,
    digest: &[u8],
) -> Result<StoredRerunInput, ArchiveDecodeError> {
    if reference.is_empty() || reference.len() > RERUN_INPUT_REFERENCE_MAX_BYTES {
        return Err(corrupt(ArchiveDomain::RerunInput, "invalid_reference"));
    }
    let digest: [u8; DIGEST_LENGTH_BYTES] = digest
        .try_into()
        .map_err(|_| corrupt(ArchiveDomain::RerunInput, "invalid_reference_digest"))?;
    Ok(StoredRerunInput::Reference {
        version: ARCHIVE_VERSION_1,
        codec: JSON_UTF8_CODEC,
        content_type: JSON_CONTENT_TYPE,
        reference: reference.to_owned(),
        digest,
    })
}

pub fn store_unavailable_rerun_input(unavailability: RerunInputUnavailability) -> StoredRerunInput {
    StoredRerunInput::Unavailable(unavailability)
}

pub fn disposition_of(stored: &StoredRerunInput) -> RerunInputDisposition {
    match stored {
        StoredRerunInput::Inline { .. } => RerunInputDisposition::Inline,
        StoredRerunInput::Reference { .. } => RerunInputDisposition::Reference,
        StoredRerunInput::Unavailable(RerunInputUnavailability::DeclinedByPolicy) => {
            RerunInputDisposition::DeclinedByPolicy
        }
        StoredRerunInput::Unavailable(RerunInputUnavailability::OverBound) => {
            RerunInputDisposition::OverBound
        }
        StoredRerunInput::Unavailable(RerunInputUnavailability::NeverEligible) => {
            RerunInputDisposition::NeverEligible
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedRerunInput {
    Inline { payload: Vec<u8>, digest: [u8; 32] },
    Reference { reference: String, digest: [u8; 32] },
    Unavailable { reason: RerunInputUnavailableReason },
}

#[allow(clippy::too_many_arguments)]
pub fn decode_rerun_input(
    disposition: &str,
    version: Option<i16>,
    codec: Option<&str>,
    content_type: Option<&str>,
    digest: Option<&[u8]>,
    inline_payload: Option<&[u8]>,
    reference: Option<&str>,
) -> Result<DecodedRerunInput, ArchiveDecodeError> {
    let disposition = RerunInputDisposition::parse(disposition)
        .ok_or_else(|| corrupt(ArchiveDomain::RerunInput, "unknown_disposition"))?;
    match disposition {
        RerunInputDisposition::DeclinedByPolicy
        | RerunInputDisposition::OverBound
        | RerunInputDisposition::NeverEligible => {
            if version.is_some()
                || codec.is_some()
                || content_type.is_some()
                || digest.is_some()
                || inline_payload.is_some()
                || reference.is_some()
            {
                return Err(corrupt(
                    ArchiveDomain::RerunInput,
                    "unavailable_with_envelope_fields",
                ));
            }
            let reason = match disposition {
                RerunInputDisposition::DeclinedByPolicy => {
                    RerunInputUnavailableReason::DeclinedByPolicy
                }
                RerunInputDisposition::OverBound => RerunInputUnavailableReason::OverBound,
                RerunInputDisposition::NeverEligible => RerunInputUnavailableReason::NeverEligible,
                RerunInputDisposition::Inline | RerunInputDisposition::Reference => unreachable!(),
            };
            Ok(DecodedRerunInput::Unavailable { reason })
        }
        RerunInputDisposition::Inline => {
            let (version, codec, content_type, digest, payload) = match (
                version,
                codec,
                content_type,
                digest,
                inline_payload,
                reference,
            ) {
                (
                    Some(version),
                    Some(codec),
                    Some(content_type),
                    Some(digest),
                    Some(payload),
                    None,
                ) => (version, codec, content_type, digest, payload),
                _ => {
                    return Err(corrupt(
                        ArchiveDomain::RerunInput,
                        "invalid_inline_envelope",
                    ));
                }
            };
            validate_envelope_contract(ArchiveDomain::RerunInput, version, codec, content_type)?;
            if payload.len() > RERUN_INPUT_INLINE_MAX_BYTES {
                return Err(corrupt(ArchiveDomain::RerunInput, "inline_over_bound"));
            }
            verify_payload_digest(ArchiveDomain::RerunInput, payload, digest)?;
            Ok(DecodedRerunInput::Inline {
                payload: payload.to_vec(),
                digest: digest
                    .try_into()
                    .map_err(|_| corrupt(ArchiveDomain::RerunInput, "invalid_inline_digest"))?,
            })
        }
        RerunInputDisposition::Reference => {
            let (version, codec, content_type, digest, reference) = match (
                version,
                codec,
                content_type,
                digest,
                inline_payload,
                reference,
            ) {
                (
                    Some(version),
                    Some(codec),
                    Some(content_type),
                    Some(digest),
                    None,
                    Some(reference),
                ) if !reference.is_empty() => (version, codec, content_type, digest, reference),
                _ => {
                    return Err(corrupt(
                        ArchiveDomain::RerunInput,
                        "invalid_reference_envelope",
                    ));
                }
            };
            validate_envelope_contract(ArchiveDomain::RerunInput, version, codec, content_type)?;
            let digest = digest
                .try_into()
                .map_err(|_| corrupt(ArchiveDomain::RerunInput, "invalid_reference_digest"))?;
            Ok(DecodedRerunInput::Reference {
                reference: reference.to_owned(),
                digest,
            })
        }
    }
}
