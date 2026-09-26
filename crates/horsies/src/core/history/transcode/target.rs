//! Checked transcode target: the version and codec that a transcode stamps on
//! history rows.
//!
//! Migration 0064 dropped the history table's single-column CHECKs. Transcode
//! is the one history writer that takes its version and codec from operator
//! input, so the bounds of those columns are checked here, once, when the job
//! is planned.

/// Lowest archive version a history row can carry.
pub const MIN_ARCHIVE_VERSION: i16 = 1;
/// Byte bounds of the history codec columns.
pub const MIN_CODEC_BYTES: usize = 1;
pub const MAX_CODEC_BYTES: usize = 64;

/// A transcode target inside the history column bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeTarget {
    version: i16,
    codec: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TranscodeTargetRejection {
    #[error("target version {version} is below {MIN_ARCHIVE_VERSION}")]
    VersionBelowMinimum { version: i16 },
    #[error("target codec is {bytes} bytes; allowed {MIN_CODEC_BYTES} to {MAX_CODEC_BYTES}")]
    CodecLengthOutOfBounds { bytes: usize },
}

impl TranscodeTarget {
    /// Checks the version and the codec byte length. Does not check that the
    /// decoder can read the target.
    pub fn parse(version: i16, codec: &str) -> Result<Self, TranscodeTargetRejection> {
        if version < MIN_ARCHIVE_VERSION {
            return Err(TranscodeTargetRejection::VersionBelowMinimum { version });
        }
        let bytes = codec.len();
        if !(MIN_CODEC_BYTES..=MAX_CODEC_BYTES).contains(&bytes) {
            return Err(TranscodeTargetRejection::CodecLengthOutOfBounds { bytes });
        }
        Ok(Self {
            version,
            codec: codec.to_owned(),
        })
    }

    pub fn version(&self) -> i16 {
        self.version
    }

    pub fn codec(&self) -> &str {
        &self.codec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_inside_the_bounds_is_accepted() {
        let target = TranscodeTarget::parse(2, "row-v2").unwrap();
        assert_eq!(target.version(), 2);
        assert_eq!(target.codec(), "row-v2");
        assert!(TranscodeTarget::parse(MIN_ARCHIVE_VERSION, &"c".repeat(MAX_CODEC_BYTES)).is_ok());
    }

    #[test]
    fn version_below_one_is_rejected() {
        assert_eq!(
            TranscodeTarget::parse(0, "json-utf8"),
            Err(TranscodeTargetRejection::VersionBelowMinimum { version: 0 })
        );
    }

    #[test]
    fn codec_outside_one_to_sixty_four_bytes_is_rejected() {
        assert_eq!(
            TranscodeTarget::parse(1, ""),
            Err(TranscodeTargetRejection::CodecLengthOutOfBounds { bytes: 0 })
        );
        assert_eq!(
            TranscodeTarget::parse(1, &"c".repeat(MAX_CODEC_BYTES + 1)),
            Err(TranscodeTargetRejection::CodecLengthOutOfBounds { bytes: 65 })
        );
        // 22 characters of 3 bytes each: 66 bytes.
        assert_eq!(
            TranscodeTarget::parse(1, &"€".repeat(22)),
            Err(TranscodeTargetRejection::CodecLengthOutOfBounds { bytes: 66 })
        );
    }
}
