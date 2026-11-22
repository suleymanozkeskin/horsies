//! Retained archive decoders by independent domain.

use super::versions::ArchiveDomain;

const RETAINED_HISTORY_ROW_VERSIONS: &[i16] = &[1];
const RETAINED_RESULT_VERSIONS: &[i16] = &[1];
const RETAINED_ATTEMPT_VERSIONS: &[i16] = &[1, 2];
const RETAINED_RERUN_INPUT_VERSIONS: &[i16] = &[1];

pub fn retained_versions(domain: ArchiveDomain) -> &'static [i16] {
    match domain {
        ArchiveDomain::HistoryRow => RETAINED_HISTORY_ROW_VERSIONS,
        ArchiveDomain::Result => RETAINED_RESULT_VERSIONS,
        ArchiveDomain::Attempts => RETAINED_ATTEMPT_VERSIONS,
        ArchiveDomain::RerunInput => RETAINED_RERUN_INPUT_VERSIONS,
    }
}

pub fn is_retained(domain: ArchiveDomain, version: i16) -> bool {
    retained_versions(domain).contains(&version)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The attempt archive retains versions 1 and 2; every other domain
    /// retains version 1 alone.
    #[test]
    fn each_archive_domain_retains_its_versions() {
        assert_eq!(ArchiveDomain::ALL.len(), 4);
        for domain in ArchiveDomain::ALL {
            let expected: &[i16] = match domain {
                ArchiveDomain::Attempts => &[1, 2],
                ArchiveDomain::HistoryRow | ArchiveDomain::Result | ArchiveDomain::RerunInput => {
                    &[1]
                }
            };
            assert_eq!(retained_versions(domain), expected);
            assert!(is_retained(domain, 1));
            assert_eq!(is_retained(domain, 2), expected.contains(&2));
            assert!(!is_retained(domain, 3));
        }
    }
}
