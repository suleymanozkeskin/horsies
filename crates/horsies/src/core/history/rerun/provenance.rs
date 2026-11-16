//! Closed field provenance for a rerun enqueue.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldProvenance {
    NewIdentity,
    ReplayedFromSource,
    ReplayedViaEnvelope,
    Lineage,
    CallerExplicit,
    ResolvedAtEnqueue,
    FreshRuntimeState,
}

pub const RERUN_FIELD_PROVENANCE: &[(&str, FieldProvenance)] = &[
    ("id", FieldProvenance::NewIdentity),
    ("created_at", FieldProvenance::NewIdentity),
    ("enqueued_at", FieldProvenance::NewIdentity),
    ("sent_at", FieldProvenance::NewIdentity),
    ("updated_at", FieldProvenance::NewIdentity),
    ("task_name", FieldProvenance::ReplayedFromSource),
    ("queue_name", FieldProvenance::ReplayedFromSource),
    ("priority", FieldProvenance::ReplayedFromSource),
    ("max_retries", FieldProvenance::ReplayedFromSource),
    ("is_workflow_task", FieldProvenance::ReplayedFromSource),
    ("input_digest", FieldProvenance::ReplayedFromSource),
    ("args", FieldProvenance::ReplayedViaEnvelope),
    ("kwargs", FieldProvenance::ReplayedViaEnvelope),
    ("task_options", FieldProvenance::ReplayedViaEnvelope),
    ("rerun_of_task_id", FieldProvenance::Lineage),
    ("rerun_root_task_id", FieldProvenance::Lineage),
    ("good_until", FieldProvenance::CallerExplicit),
    ("retention_class_key", FieldProvenance::ResolvedAtEnqueue),
    ("retain_rerun_input", FieldProvenance::ResolvedAtEnqueue),
    (
        "prepared_rerun_input_disposition",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    (
        "prepared_rerun_input_version",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    (
        "prepared_rerun_input_codec",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    (
        "prepared_rerun_input_content_type",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    (
        "prepared_rerun_input_digest",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    (
        "prepared_rerun_input_inline",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    (
        "prepared_rerun_input_reference",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    ("idempotency_key_digest", FieldProvenance::ResolvedAtEnqueue),
    (
        "command_fingerprint_version",
        FieldProvenance::ResolvedAtEnqueue,
    ),
    ("command_fingerprint", FieldProvenance::ResolvedAtEnqueue),
    ("enqueue_sha", FieldProvenance::ResolvedAtEnqueue),
    ("status", FieldProvenance::FreshRuntimeState),
    ("retry_count", FieldProvenance::FreshRuntimeState),
    ("next_retry_at", FieldProvenance::FreshRuntimeState),
    ("claimed", FieldProvenance::FreshRuntimeState),
    ("claimed_at", FieldProvenance::FreshRuntimeState),
    ("claimed_by_worker_id", FieldProvenance::FreshRuntimeState),
    ("claim_expires_at", FieldProvenance::FreshRuntimeState),
    ("started_at", FieldProvenance::FreshRuntimeState),
    ("completed_at", FieldProvenance::FreshRuntimeState),
    ("failed_at", FieldProvenance::FreshRuntimeState),
    ("terminal_at", FieldProvenance::FreshRuntimeState),
    ("result", FieldProvenance::FreshRuntimeState),
    ("failed_reason", FieldProvenance::FreshRuntimeState),
    ("error_code", FieldProvenance::FreshRuntimeState),
    ("finalizing_at", FieldProvenance::FreshRuntimeState),
    (
        "finalizing_by_worker_id",
        FieldProvenance::FreshRuntimeState,
    ),
    ("worker_pid", FieldProvenance::FreshRuntimeState),
    ("worker_hostname", FieldProvenance::FreshRuntimeState),
    ("worker_process_name", FieldProvenance::FreshRuntimeState),
];

pub fn field_provenance(field: &str) -> Option<FieldProvenance> {
    RERUN_FIELD_PROVENANCE
        .iter()
        .find_map(|(name, provenance)| (*name == field).then_some(*provenance))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_table_has_no_duplicates_and_all_seven_sources_are_used() {
        let mut names = std::collections::HashSet::new();
        let mut sources = std::collections::HashSet::new();
        for (name, source) in RERUN_FIELD_PROVENANCE {
            assert!(names.insert(*name), "duplicate field {name}");
            sources.insert(*source);
            assert_eq!(field_provenance(name), Some(*source));
        }
        assert_eq!(sources.len(), 7);
        assert_eq!(
            field_provenance("rerun_of_task_id"),
            Some(FieldProvenance::Lineage)
        );
        assert_eq!(
            field_provenance("rerun_root_task_id"),
            Some(FieldProvenance::Lineage)
        );
        let caller_explicit: Vec<_> = RERUN_FIELD_PROVENANCE
            .iter()
            .filter_map(|(name, provenance)| {
                (*provenance == FieldProvenance::CallerExplicit).then_some(*name)
            })
            .collect();
        assert_eq!(caller_explicit, ["good_until"]);
    }
}
