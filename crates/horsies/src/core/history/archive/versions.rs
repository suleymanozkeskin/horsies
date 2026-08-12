//! Independent archive-version domains and typed decode failures.

use sha2::{Digest, Sha256};

pub const ARCHIVE_VERSION_1: i16 = 1;
pub const JSON_UTF8_CODEC: &str = "json-utf8";
pub const JSON_CONTENT_TYPE: &str = "application/json";
pub const DIGEST_LENGTH_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveDomain {
    HistoryRow,
    Result,
    Attempts,
    RerunInput,
}

impl ArchiveDomain {
    pub const ALL: [Self; 4] = [
        Self::HistoryRow,
        Self::Result,
        Self::Attempts,
        Self::RerunInput,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveDecodeError {
    #[error("unknown {domain:?} archive version {version}")]
    UnknownVersion { domain: ArchiveDomain, version: i16 },
    #[error("unknown {domain:?} archive codec {codec:?}")]
    UnknownCodec {
        domain: ArchiveDomain,
        codec: String,
    },
    #[error("unknown {domain:?} archive content type {content_type:?}")]
    UnknownContentType {
        domain: ArchiveDomain,
        content_type: String,
    },
    #[error("corrupt {domain:?} archive value: {detail}")]
    Corrupt {
        domain: ArchiveDomain,
        detail: String,
    },
    #[error("{domain:?} archive digest mismatch")]
    DigestMismatch { domain: ArchiveDomain },
}

pub fn archive_digest(payload: &[u8]) -> [u8; DIGEST_LENGTH_BYTES] {
    Sha256::digest(payload).into()
}

pub fn decode_history_row_version(version: i16) -> Result<i16, ArchiveDecodeError> {
    if version == ARCHIVE_VERSION_1 {
        Ok(version)
    } else {
        Err(ArchiveDecodeError::UnknownVersion {
            domain: ArchiveDomain::HistoryRow,
            version,
        })
    }
}

pub fn validate_envelope_contract(
    domain: ArchiveDomain,
    version: i16,
    codec: &str,
    content_type: &str,
) -> Result<(), ArchiveDecodeError> {
    if version != ARCHIVE_VERSION_1 {
        return Err(ArchiveDecodeError::UnknownVersion { domain, version });
    }
    if codec != JSON_UTF8_CODEC {
        return Err(ArchiveDecodeError::UnknownCodec {
            domain,
            codec: codec.to_owned(),
        });
    }
    if content_type != JSON_CONTENT_TYPE {
        return Err(ArchiveDecodeError::UnknownContentType {
            domain,
            content_type: content_type.to_owned(),
        });
    }
    Ok(())
}

pub fn verify_payload_digest(
    domain: ArchiveDomain,
    payload: &[u8],
    digest: &[u8],
) -> Result<(), ArchiveDecodeError> {
    if archive_digest(payload).as_slice() == digest {
        Ok(())
    } else {
        Err(ArchiveDecodeError::DigestMismatch { domain })
    }
}

pub fn corrupt(domain: ArchiveDomain, detail: impl Into<String>) -> ArchiveDecodeError {
    ArchiveDecodeError::Corrupt {
        domain,
        detail: detail.into(),
    }
}
