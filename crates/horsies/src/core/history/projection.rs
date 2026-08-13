//! Single ordered authority for immutable task-history inserts.

use std::borrow::Cow;

use crate::core::history::archive::rerun_input::RerunInputDisposition;
use crate::core::history::ddl::classes::FOREVER_CLASS_KEY;
use crate::core::lifecycle::TerminalizationKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HistoryProjectionColumn {
    TaskId,
    TaskName,
    QueueName,
    Priority,
    CommandFingerprintVersion,
    CommandFingerprint,
    Status,
    TerminalizationKind,
    TerminalAt,
    RetentionAnchorAt,
    RetentionClassKey,
    SentAt,
    EnqueuedAt,
    ClaimedAt,
    StartedAt,
    CreatedAt,
    GoodUntil,
    ResultEnvelopeVersion,
    ResultCodec,
    ResultContentType,
    ResultPayload,
    PriorResultPayload,
    ResultDigest,
    ErrorCode,
    FinalFailedReason,
    RetryCount,
    MaxRetries,
    LastClaimedWorkerId,
    LastWorkerHostname,
    LastWorkerPid,
    LastWorkerProcessName,
    InputDigest,
    RerunOfTaskId,
    RerunRootTaskId,
    WorkflowId,
    IsWorkflowTask,
    HistorySchemaVersion,
    AttemptArchiveVersion,
    AttemptSnapshotCodec,
    AttemptSnapshotContentType,
    AttemptSnapshot,
    AttemptSnapshotDigest,
    RerunInputDisposition,
    RerunInputVersion,
    RerunInputCodec,
    RerunInputContentType,
    RerunInputDigest,
    RerunInputInline,
    RerunInputReference,
}

impl HistoryProjectionColumn {
    pub const ALL: [Self; 49] = [
        Self::TaskId,
        Self::TaskName,
        Self::QueueName,
        Self::Priority,
        Self::CommandFingerprintVersion,
        Self::CommandFingerprint,
        Self::Status,
        Self::TerminalizationKind,
        Self::TerminalAt,
        Self::RetentionAnchorAt,
        Self::RetentionClassKey,
        Self::SentAt,
        Self::EnqueuedAt,
        Self::ClaimedAt,
        Self::StartedAt,
        Self::CreatedAt,
        Self::GoodUntil,
        Self::ResultEnvelopeVersion,
        Self::ResultCodec,
        Self::ResultContentType,
        Self::ResultPayload,
        Self::PriorResultPayload,
        Self::ResultDigest,
        Self::ErrorCode,
        Self::FinalFailedReason,
        Self::RetryCount,
        Self::MaxRetries,
        Self::LastClaimedWorkerId,
        Self::LastWorkerHostname,
        Self::LastWorkerPid,
        Self::LastWorkerProcessName,
        Self::InputDigest,
        Self::RerunOfTaskId,
        Self::RerunRootTaskId,
        Self::WorkflowId,
        Self::IsWorkflowTask,
        Self::HistorySchemaVersion,
        Self::AttemptArchiveVersion,
        Self::AttemptSnapshotCodec,
        Self::AttemptSnapshotContentType,
        Self::AttemptSnapshot,
        Self::AttemptSnapshotDigest,
        Self::RerunInputDisposition,
        Self::RerunInputVersion,
        Self::RerunInputCodec,
        Self::RerunInputContentType,
        Self::RerunInputDigest,
        Self::RerunInputInline,
        Self::RerunInputReference,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TaskId => "task_id",
            Self::TaskName => "task_name",
            Self::QueueName => "queue_name",
            Self::Priority => "priority",
            Self::CommandFingerprintVersion => "command_fingerprint_version",
            Self::CommandFingerprint => "command_fingerprint",
            Self::Status => "status",
            Self::TerminalizationKind => "terminalization_kind",
            Self::TerminalAt => "terminal_at",
            Self::RetentionAnchorAt => "retention_anchor_at",
            Self::RetentionClassKey => "retention_class_key",
            Self::SentAt => "sent_at",
            Self::EnqueuedAt => "enqueued_at",
            Self::ClaimedAt => "claimed_at",
            Self::StartedAt => "started_at",
            Self::CreatedAt => "created_at",
            Self::GoodUntil => "good_until",
            Self::ResultEnvelopeVersion => "result_envelope_version",
            Self::ResultCodec => "result_codec",
            Self::ResultContentType => "result_content_type",
            Self::ResultPayload => "result_payload",
            Self::PriorResultPayload => "prior_result_payload",
            Self::ResultDigest => "result_digest",
            Self::ErrorCode => "error_code",
            Self::FinalFailedReason => "final_failed_reason",
            Self::RetryCount => "retry_count",
            Self::MaxRetries => "max_retries",
            Self::LastClaimedWorkerId => "last_claimed_worker_id",
            Self::LastWorkerHostname => "last_worker_hostname",
            Self::LastWorkerPid => "last_worker_pid",
            Self::LastWorkerProcessName => "last_worker_process_name",
            Self::InputDigest => "input_digest",
            Self::RerunOfTaskId => "rerun_of_task_id",
            Self::RerunRootTaskId => "rerun_root_task_id",
            Self::WorkflowId => "workflow_id",
            Self::IsWorkflowTask => "is_workflow_task",
            Self::HistorySchemaVersion => "history_schema_version",
            Self::AttemptArchiveVersion => "attempt_archive_version",
            Self::AttemptSnapshotCodec => "attempt_snapshot_codec",
            Self::AttemptSnapshotContentType => "attempt_snapshot_content_type",
            Self::AttemptSnapshot => "attempt_snapshot",
            Self::AttemptSnapshotDigest => "attempt_snapshot_digest",
            Self::RerunInputDisposition => "rerun_input_disposition",
            Self::RerunInputVersion => "rerun_input_version",
            Self::RerunInputCodec => "rerun_input_codec",
            Self::RerunInputContentType => "rerun_input_content_type",
            Self::RerunInputDigest => "rerun_input_digest",
            Self::RerunInputInline => "rerun_input_inline",
            Self::RerunInputReference => "rerun_input_reference",
        }
    }

    fn relocation_expression(self) -> Cow<'static, str> {
        match self {
            Self::TaskId => "CAST(t.id AS uuid)".into(),
            Self::TaskName => "t.task_name".into(),
            Self::QueueName => "t.queue_name".into(),
            Self::Priority => "t.priority".into(),
            Self::CommandFingerprintVersion => "t.command_fingerprint_version".into(),
            Self::CommandFingerprint => "t.command_fingerprint".into(),
            Self::Status => "t.status".into(),
            Self::TerminalizationKind => format!(
                "COALESCE(t.terminalization_kind, '{}')",
                TerminalizationKind::LegacyTerminal.as_str()
            )
            .into(),
            Self::TerminalAt | Self::RetentionAnchorAt => "t.terminal_at".into(),
            Self::RetentionClassKey => {
                format!("COALESCE(t.retention_class_key, '{FOREVER_CLASS_KEY}')").into()
            }
            Self::SentAt => "t.sent_at".into(),
            Self::EnqueuedAt => "t.enqueued_at".into(),
            Self::ClaimedAt => "t.claimed_at".into(),
            Self::StartedAt => "t.started_at".into(),
            Self::CreatedAt => "t.created_at".into(),
            Self::GoodUntil => "t.good_until".into(),
            Self::ResultEnvelopeVersion | Self::HistorySchemaVersion | Self::AttemptArchiveVersion => {
                "1".into()
            }
            Self::ResultCodec | Self::AttemptSnapshotCodec => "'json-utf8'".into(),
            Self::ResultContentType | Self::AttemptSnapshotContentType => {
                "'application/json'".into()
            }
            Self::ResultPayload => format!(
                "CASE WHEN t.terminalization_kind = '{}' THEN NULL ELSE (CASE WHEN t.result IS NULL THEN NULL ELSE convert_to(t.result, 'UTF8') END) END",
                TerminalizationKind::CancelAdmin.as_str()
            )
            .into(),
            Self::PriorResultPayload => format!(
                "CASE WHEN t.terminalization_kind = '{}' THEN (CASE WHEN t.result IS NULL THEN NULL ELSE convert_to(t.result, 'UTF8') END) END",
                TerminalizationKind::CancelAdmin.as_str()
            )
            .into(),
            Self::ResultDigest => format!(
                "CASE WHEN t.terminalization_kind = '{}' THEN CASE WHEN t.result IS NULL THEN NULL ELSE sha256(convert_to(t.result, 'UTF8')) END WHEN t.result IS NULL THEN NULL ELSE sha256(convert_to(t.result, 'UTF8')) END",
                TerminalizationKind::CancelAdmin.as_str()
            )
            .into(),
            Self::ErrorCode => "t.error_code".into(),
            Self::FinalFailedReason => {
                "CASE WHEN t.status IN ('FAILED', 'EXPIRED') THEN last_attempt.failed_reason END"
                    .into()
            }
            Self::RetryCount => "t.retry_count".into(),
            Self::MaxRetries => "t.max_retries".into(),
            Self::LastClaimedWorkerId => "t.claimed_by_worker_id".into(),
            Self::LastWorkerHostname => "t.worker_hostname".into(),
            Self::LastWorkerPid => "t.worker_pid".into(),
            Self::LastWorkerProcessName => "t.worker_process_name".into(),
            Self::InputDigest => "t.input_digest".into(),
            Self::RerunOfTaskId => "t.rerun_of_task_id".into(),
            Self::RerunRootTaskId => "t.rerun_root_task_id".into(),
            Self::WorkflowId => {
                "CASE WHEN t.is_workflow_task THEN CAST(node.workflow_id AS uuid) END".into()
            }
            Self::IsWorkflowTask => "t.is_workflow_task".into(),
            Self::AttemptSnapshot => "horsies_encode_task_attempts(CAST(t.id AS uuid))".into(),
            Self::AttemptSnapshotDigest => {
                "sha256(horsies_encode_task_attempts(CAST(t.id AS uuid)))".into()
            }
            Self::RerunInputDisposition => "d.disposition".into(),
            Self::RerunInputVersion => rerun_carriage_expression("version").into(),
            Self::RerunInputCodec => rerun_carriage_expression("codec").into(),
            Self::RerunInputContentType => rerun_carriage_expression("content_type").into(),
            Self::RerunInputDigest => rerun_carriage_expression("digest").into(),
            Self::RerunInputInline => rerun_carriage_expression("inline").into(),
            Self::RerunInputReference => rerun_carriage_expression("reference").into(),
        }
    }
}

fn rerun_carriage_expression(field: &str) -> String {
    format!(
        "CASE WHEN d.disposition IN ('{}', '{}')\n             THEN t.prepared_rerun_input_{field} END",
        RerunInputDisposition::Inline.as_str(),
        RerunInputDisposition::Reference.as_str(),
    )
}

pub fn relocation_disposition_case_expression() -> String {
    format!(
        "CASE\n            WHEN t.is_workflow_task OR t.status = 'COMPLETED' THEN '{}'\n            WHEN NOT t.retain_rerun_input THEN '{}'\n            ELSE t.prepared_rerun_input_disposition\n        END",
        RerunInputDisposition::NeverEligible.as_str(),
        RerunInputDisposition::DeclinedByPolicy.as_str(),
    )
}

pub fn render_relocation_insert_sql(task_ids_expression: &str) -> String {
    let columns = HistoryProjectionColumn::ALL
        .iter()
        .map(|column| column.as_str())
        .collect::<Vec<_>>()
        .join(",\n        ");
    let values = HistoryProjectionColumn::ALL
        .iter()
        .map(|column| column.relocation_expression())
        .collect::<Vec<_>>()
        .join(",\n        ");
    let disposition = relocation_disposition_case_expression();
    format!(
        "INSERT INTO horsies_task_history (\n        {columns}\n    )\n    SELECT\n        {values}\n    FROM horsies_tasks t\n    LEFT JOIN LATERAL (\n        SELECT wt.workflow_id\n        FROM horsies_workflow_tasks wt\n        WHERE wt.task_id = t.id\n        ORDER BY wt.id\n        LIMIT 1\n    ) node ON TRUE\n    LEFT JOIN LATERAL (\n        SELECT a.failed_reason\n        FROM horsies_task_attempts a\n        WHERE a.task_id = CAST(t.id AS uuid)\n        ORDER BY a.attempt DESC\n        LIMIT 1\n    ) last_attempt ON TRUE\n    CROSS JOIN LATERAL (\n        SELECT {disposition} AS disposition\n    ) d\n    WHERE t.id::text = ANY({task_ids_expression})"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const PYTHON_AUTHORITIES: &str =
        include_str!("../../../tests/fixtures/task_history/python-v052-authorities.json");
    const PYTHON_RELOCATION_SQL: &str =
        include_str!("../../../tests/fixtures/task_history/python-v052-relocation.sql");
    const FRESH_CUTOVER_MIGRATION: &str =
        include_str!("../../../migrations/0041_task_history_fresh_cutover.sql");

    fn normalize_bind(sql: &str) -> String {
        sql.replace("ANY(CAST(:task_ids AS text[]))", "ANY($1::text[])")
    }

    #[test]
    fn projection_ladder_carriage_and_all_installed_writers_share_the_authority() {
        let authorities: serde_json::Value = serde_json::from_str(PYTHON_AUTHORITIES).unwrap();
        let expected_columns: Vec<&str> = authorities["history_projection_columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|column| column.as_str().unwrap())
            .collect();
        assert_eq!(
            HistoryProjectionColumn::ALL
                .map(HistoryProjectionColumn::as_str)
                .as_slice(),
            expected_columns
        );
        assert_eq!(
            authorities["disposition_ladder"]["rungs"],
            serde_json::json!([
                [
                    "{row}.is_workflow_task OR {status} = 'COMPLETED'",
                    "NEVER_ELIGIBLE"
                ],
                ["NOT {row}.retain_rerun_input", "DECLINED_BY_POLICY"],
            ])
        );
        assert_eq!(
            authorities["disposition_ladder"]["fallback"],
            "{row}.prepared_rerun_input_disposition"
        );
        let rendered = render_relocation_insert_sql("$1::text[]");
        assert_eq!(rendered, normalize_bind(PYTHON_RELOCATION_SQL.trim_end()));
        assert_eq!(
            rendered
                .matches("d.disposition IN ('INLINE', 'REFERENCE')")
                .count(),
            6
        );

        let columns = format!(
            "INSERT INTO horsies_task_history (\n        {}\n    )",
            expected_columns.join(",\n        ")
        );
        let functions: Vec<&str> = FRESH_CUTOVER_MIGRATION
            .split("$horsies_p1_sql$")
            .enumerate()
            .filter_map(|(index, statement)| {
                (index % 2 == 1 && statement.contains("INSERT INTO horsies_task_history"))
                    .then_some(statement)
            })
            .collect();
        assert_eq!(functions.len(), 7);
        let mut carriage_counts = Vec::new();
        for function in functions {
            assert!(function.contains(&columns), "writer projection drifted");
            carriage_counts.push(
                function
                    .matches("d.disposition IN ('INLINE', 'REFERENCE')")
                    .count(),
            );
        }
        carriage_counts.sort_unstable();
        assert_eq!(carriage_counts, [0, 6, 6, 6, 6, 6, 6]);
    }
}
