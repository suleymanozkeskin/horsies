//! Single-column rules for `horsies_task_history` rows.
//!
//! Migration 0064 dropped these CHECK constraints from the history table: the
//! database re-parsed each constraint expression on every history INSERT. The
//! Horsies writers produce values inside these rules. The cutover relocation,
//! which copies legacy rows, validates them with this list before it inserts.
//! Each predicate is the constraint text as `pg_get_constraintdef` renders it.

/// One dropped constraint: its name and its predicate over one history column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryColumnRule {
    pub name: &'static str,
    pub predicate: &'static str,
}

/// The 24 single-column rules, ordered by name. Migration 0064 names the same set.
pub const HISTORY_COLUMN_RULES: [HistoryColumnRule; 24] = [
    HistoryColumnRule {
        name: "horsies_task_history_attempt_archive_version_check",
        predicate: "(attempt_archive_version > 0)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_attempt_snapshot_codec_check",
        predicate: "((octet_length((attempt_snapshot_codec)::text) >= 1) AND (octet_length((attempt_snapshot_codec)::text) <= 64))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_attempt_snapshot_content_type_check",
        predicate: "((octet_length((attempt_snapshot_content_type)::text) >= 1) AND (octet_length((attempt_snapshot_content_type)::text) <= 255))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_attempt_snapshot_digest_check",
        predicate: "(octet_length(attempt_snapshot_digest) = 32)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_command_fingerprint_check",
        predicate: "(octet_length(command_fingerprint) = 32)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_command_fingerprint_version_check",
        predicate: "(command_fingerprint_version > 0)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_history_schema_version_check",
        predicate: "(history_schema_version > 0)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_input_digest_check",
        predicate: "((input_digest IS NULL) OR (octet_length(input_digest) = 32))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_max_retries_check",
        predicate: "(max_retries >= 0)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_priority_check",
        predicate: "((priority >= 1) AND (priority <= 100))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_rerun_input_codec_check",
        predicate: "((rerun_input_codec IS NULL) OR ((octet_length((rerun_input_codec)::text) >= 1) AND (octet_length((rerun_input_codec)::text) <= 64)))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_rerun_input_content_type_check",
        predicate: "((rerun_input_content_type IS NULL) OR ((octet_length((rerun_input_content_type)::text) >= 1) AND (octet_length((rerun_input_content_type)::text) <= 255)))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_rerun_input_digest_check",
        predicate: "((rerun_input_digest IS NULL) OR (octet_length(rerun_input_digest) = 32))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_rerun_input_disposition_check",
        predicate: "((rerun_input_disposition)::text = ANY ((ARRAY['INLINE'::character varying, 'REFERENCE'::character varying, 'DECLINED_BY_POLICY'::character varying, 'OVER_BOUND'::character varying, 'NEVER_ELIGIBLE'::character varying])::text[]))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_rerun_input_inline_check",
        predicate: "((rerun_input_inline IS NULL) OR (octet_length(rerun_input_inline) <= 65536))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_rerun_input_reference_check",
        predicate: "((rerun_input_reference IS NULL) OR ((octet_length((rerun_input_reference)::text) >= 1) AND (octet_length((rerun_input_reference)::text) <= 2048)))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_rerun_input_version_check",
        predicate: "((rerun_input_version IS NULL) OR (rerun_input_version > 0))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_result_codec_check",
        predicate: "((octet_length((result_codec)::text) >= 1) AND (octet_length((result_codec)::text) <= 64))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_result_content_type_check",
        predicate: "((octet_length((result_content_type)::text) >= 1) AND (octet_length((result_content_type)::text) <= 255))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_result_digest_check",
        predicate: "((result_digest IS NULL) OR (octet_length(result_digest) = 32))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_result_envelope_version_check",
        predicate: "(result_envelope_version > 0)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_retry_count_check",
        predicate: "(retry_count >= 0)",
    },
    HistoryColumnRule {
        name: "horsies_task_history_status_check",
        predicate: "(status = ANY (ARRAY['COMPLETED'::text, 'FAILED'::text, 'CANCELLED'::text, 'EXPIRED'::text]))",
    },
    HistoryColumnRule {
        name: "horsies_task_history_terminalization_kind_check",
        predicate: "((terminalization_kind)::text = ANY ((ARRAY['COMPLETE_LOCKED'::character varying, 'COMPLETE_FUSED'::character varying, 'FAIL_RUNNING'::character varying, 'FAIL_STALE'::character varying, 'EXPIRE_CLAIMED'::character varying, 'EXPIRE_PENDING'::character varying, 'CANCEL_ADMIN'::character varying, 'CANCEL_ORPHAN'::character varying, 'CANCEL_ORPHAN_SWEEP'::character varying, 'PAUSE_ABANDON_CLAIM'::character varying, 'PAUSE_ABANDON_CLAIM_BATCH'::character varying, 'PAUSE_ABANDON_WORKFLOW'::character varying, 'WORKFLOW_CANCEL_CLAIM'::character varying, 'WORKFLOW_CANCEL_CLAIM_BATCH'::character varying, 'WORKFLOW_CANCEL_WORKFLOW'::character varying, 'LEGACY_TERMINAL'::character varying])::text[]))",
    },
];
