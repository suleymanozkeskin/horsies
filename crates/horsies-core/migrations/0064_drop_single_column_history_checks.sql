-- Drop the 24 single-column CHECK constraints of horsies_task_history.
-- PostgreSQL rebuilds every CHECK expression from its stored text on each
-- INSERT into the table. The Horsies writers produce values inside these
-- rules. The cutover relocation, which copies legacy rows, validates the same
-- predicates before it inserts: HISTORY_COLUMN_RULES in
-- horsies/src/core/history/column_rules.rs holds the same 24 names.
-- The cross-column CHECK constraints stay.
--
-- The migration refuses a database whose single-column CHECK set on the
-- parent differs from these 24 names.
DO $migration$
DECLARE
    v_expected text[] := ARRAY[
        'horsies_task_history_attempt_archive_version_check',
        'horsies_task_history_attempt_snapshot_codec_check',
        'horsies_task_history_attempt_snapshot_content_type_check',
        'horsies_task_history_attempt_snapshot_digest_check',
        'horsies_task_history_command_fingerprint_check',
        'horsies_task_history_command_fingerprint_version_check',
        'horsies_task_history_history_schema_version_check',
        'horsies_task_history_input_digest_check',
        'horsies_task_history_max_retries_check',
        'horsies_task_history_priority_check',
        'horsies_task_history_rerun_input_codec_check',
        'horsies_task_history_rerun_input_content_type_check',
        'horsies_task_history_rerun_input_digest_check',
        'horsies_task_history_rerun_input_disposition_check',
        'horsies_task_history_rerun_input_inline_check',
        'horsies_task_history_rerun_input_reference_check',
        'horsies_task_history_rerun_input_version_check',
        'horsies_task_history_result_codec_check',
        'horsies_task_history_result_content_type_check',
        'horsies_task_history_result_digest_check',
        'horsies_task_history_result_envelope_version_check',
        'horsies_task_history_retry_count_check',
        'horsies_task_history_status_check',
        'horsies_task_history_terminalization_kind_check'
    ];
    v_found text[];
BEGIN
    SELECT COALESCE(array_agg(conname::text ORDER BY conname), ARRAY[]::text[])
      INTO v_found
      FROM pg_constraint
     WHERE conrelid = 'horsies_task_history'::regclass
       AND contype = 'c'
       AND cardinality(conkey) = 1;
    IF v_found IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION
            'horsies_task_history single-column CHECK set differs: expected %, found %',
            v_expected, v_found;
    END IF;
END
$migration$;

ALTER TABLE horsies_task_history
    DROP CONSTRAINT horsies_task_history_attempt_archive_version_check,
    DROP CONSTRAINT horsies_task_history_attempt_snapshot_codec_check,
    DROP CONSTRAINT horsies_task_history_attempt_snapshot_content_type_check,
    DROP CONSTRAINT horsies_task_history_attempt_snapshot_digest_check,
    DROP CONSTRAINT horsies_task_history_command_fingerprint_check,
    DROP CONSTRAINT horsies_task_history_command_fingerprint_version_check,
    DROP CONSTRAINT horsies_task_history_history_schema_version_check,
    DROP CONSTRAINT horsies_task_history_input_digest_check,
    DROP CONSTRAINT horsies_task_history_max_retries_check,
    DROP CONSTRAINT horsies_task_history_priority_check,
    DROP CONSTRAINT horsies_task_history_rerun_input_codec_check,
    DROP CONSTRAINT horsies_task_history_rerun_input_content_type_check,
    DROP CONSTRAINT horsies_task_history_rerun_input_digest_check,
    DROP CONSTRAINT horsies_task_history_rerun_input_disposition_check,
    DROP CONSTRAINT horsies_task_history_rerun_input_inline_check,
    DROP CONSTRAINT horsies_task_history_rerun_input_reference_check,
    DROP CONSTRAINT horsies_task_history_rerun_input_version_check,
    DROP CONSTRAINT horsies_task_history_result_codec_check,
    DROP CONSTRAINT horsies_task_history_result_content_type_check,
    DROP CONSTRAINT horsies_task_history_result_digest_check,
    DROP CONSTRAINT horsies_task_history_result_envelope_version_check,
    DROP CONSTRAINT horsies_task_history_retry_count_check,
    DROP CONSTRAINT horsies_task_history_status_check,
    DROP CONSTRAINT horsies_task_history_terminalization_kind_check;
