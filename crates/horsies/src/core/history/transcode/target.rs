//! Checked transcode targets.
//!
//! `TranscodeTarget` holds a version and codec inside the history column
//! bounds; migration 0064 moved those bounds from the history table here. A
//! job row carries it. `DecodableTarget` adds the rule that a decoder reads
//! the target: `plan_transcode` accepts only a `DecodableTarget`, parsed
//! against a named `DecoderSet`.

use crate::core::history::archive::versions::{
    ARCHIVE_VERSION_1, HISTORY_ROW_V1_CODEC, JSON_UTF8_CODEC,
};

use super::outcomes::ArchiveComponent;

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

/// The component versions and codecs that one decoder reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecoderSet {
    entries: Vec<(ArchiveComponent, i16, String)>,
}

impl DecoderSet {
    /// What this binary's decoder reads, from the decoder's own constants.
    pub fn current() -> Self {
        Self::new(ArchiveComponent::ALL.map(|component| match component {
            ArchiveComponent::HistoryRow => (component, ARCHIVE_VERSION_1, HISTORY_ROW_V1_CODEC),
            ArchiveComponent::Result
            | ArchiveComponent::Attempts
            | ArchiveComponent::RerunInput => (component, ARCHIVE_VERSION_1, JSON_UTF8_CODEC),
        }))
    }

    /// A set of the given entries. A caller other than `current` names the
    /// decoder it stands for.
    pub fn new<'a>(entries: impl IntoIterator<Item = (ArchiveComponent, i16, &'a str)>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(component, version, codec)| (component, version, codec.to_owned()))
                .collect(),
        }
    }

    pub fn reads(&self, component: ArchiveComponent, version: i16, codec: &str) -> bool {
        self.entries
            .iter()
            .any(|(entry_component, entry_version, entry_codec)| {
                *entry_component == component && *entry_version == version && entry_codec == codec
            })
    }
}

/// A transcode target inside the history bounds that the decoder set reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodableTarget {
    component: ArchiveComponent,
    target: TranscodeTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodableTargetRejection {
    #[error(transparent)]
    OutOfBounds(#[from] TranscodeTargetRejection),
    #[error("the decoder does not read {component} version {version} codec {codec:?}")]
    NotDecodable {
        component: &'static str,
        version: i16,
        codec: String,
    },
}

impl DecodableTarget {
    /// Checks the history bounds, then membership in `decoders`.
    pub fn parse(
        component: ArchiveComponent,
        version: i16,
        codec: &str,
        decoders: &DecoderSet,
    ) -> Result<Self, DecodableTargetRejection> {
        let target = TranscodeTarget::parse(version, codec)?;
        if !decoders.reads(component, version, codec) {
            return Err(DecodableTargetRejection::NotDecodable {
                component: component.as_str(),
                version,
                codec: codec.to_owned(),
            });
        }
        Ok(Self { component, target })
    }

    pub fn component(&self) -> ArchiveComponent {
        self.component
    }

    pub fn target(&self) -> &TranscodeTarget {
        &self.target
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

    fn synthetic() -> DecoderSet {
        DecoderSet::new([
            (ArchiveComponent::Result, 1, "json-utf8"),
            (ArchiveComponent::Result, 2, "framed-v2"),
        ])
    }

    #[test]
    fn decodable_target_accepts_a_member_of_the_set() {
        let target =
            DecodableTarget::parse(ArchiveComponent::Result, 2, "framed-v2", &synthetic()).unwrap();
        assert_eq!(target.component(), ArchiveComponent::Result);
        assert_eq!(
            target.target(),
            &TranscodeTarget::parse(2, "framed-v2").unwrap()
        );
    }

    #[test]
    fn decodable_target_rejects_a_target_outside_the_bounds() {
        assert_eq!(
            DecodableTarget::parse(ArchiveComponent::Result, 0, "json-utf8", &synthetic()),
            Err(DecodableTargetRejection::OutOfBounds(
                TranscodeTargetRejection::VersionBelowMinimum { version: 0 }
            ))
        );
        assert_eq!(
            DecodableTarget::parse(ArchiveComponent::Result, 2, "", &synthetic()),
            Err(DecodableTargetRejection::OutOfBounds(
                TranscodeTargetRejection::CodecLengthOutOfBounds { bytes: 0 }
            ))
        );
    }

    #[test]
    fn decodable_target_rejects_a_target_the_decoder_does_not_read() {
        assert_eq!(
            DecodableTarget::parse(ArchiveComponent::Attempts, 2, "framed-v2", &synthetic()),
            Err(DecodableTargetRejection::NotDecodable {
                component: ArchiveComponent::Attempts.as_str(),
                version: 2,
                codec: "framed-v2".to_owned(),
            })
        );
        assert!(DecodableTarget::parse(
            ArchiveComponent::Result,
            2,
            "framed-v2",
            &DecoderSet::current()
        )
        .is_err());
    }

    #[test]
    fn current_decoder_set_matches_the_decoder_and_the_history_bounds() {
        use crate::core::history::archive::versions::{
            decode_history_row_version, validate_envelope_contract, ArchiveDomain,
            JSON_CONTENT_TYPE,
        };
        use crate::core::history::transcode::transforms::component_columns;
        let current = DecoderSet::current();
        for component in ArchiveComponent::ALL {
            let entries: Vec<_> = current
                .entries
                .iter()
                .filter(|(entry, _, _)| *entry == component)
                .collect();
            assert_eq!(entries.len(), 1, "{component:?}");
            let (_, version, codec) = entries[0];
            assert!(TranscodeTarget::parse(*version, codec).is_ok());
            match component {
                ArchiveComponent::HistoryRow => {
                    assert_eq!(decode_history_row_version(*version), Ok(*version));
                    assert!(component_columns(component)
                        .codec
                        .contains(&format!("WHEN {version} THEN '{codec}'")));
                }
                ArchiveComponent::Result => {
                    validate_envelope_contract(
                        ArchiveDomain::Result,
                        *version,
                        codec,
                        JSON_CONTENT_TYPE,
                    )
                    .unwrap();
                }
                ArchiveComponent::Attempts => {
                    validate_envelope_contract(
                        ArchiveDomain::Attempts,
                        *version,
                        codec,
                        JSON_CONTENT_TYPE,
                    )
                    .unwrap();
                }
                ArchiveComponent::RerunInput => {
                    validate_envelope_contract(
                        ArchiveDomain::RerunInput,
                        *version,
                        codec,
                        JSON_CONTENT_TYPE,
                    )
                    .unwrap();
                }
            }
        }
    }
}
