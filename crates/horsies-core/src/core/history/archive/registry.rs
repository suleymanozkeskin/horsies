//! Retained archive decoders by independent domain.

use super::versions::ArchiveDomain;

const RETAINED_HISTORY_ROW_VERSIONS: &[i16] = &[1];
const RETAINED_RESULT_VERSIONS: &[i16] = &[1];
const RETAINED_ATTEMPT_VERSIONS: &[i16] = &[1];
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

    #[test]
    fn every_archive_domain_retains_exactly_version_one() {
        assert_eq!(ArchiveDomain::ALL.len(), 4);
        for domain in ArchiveDomain::ALL {
            assert_eq!(retained_versions(domain), &[1]);
            assert!(is_retained(domain, 1));
            assert!(!is_retained(domain, 2));
        }
    }
}
