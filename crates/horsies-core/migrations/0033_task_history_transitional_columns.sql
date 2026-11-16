-- Schema v27: nullable, check-free live cutover columns.
ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS command_fingerprint_version smallint,
    ADD COLUMN IF NOT EXISTS command_fingerprint bytea,
    ADD COLUMN IF NOT EXISTS retention_class_key varchar(64),
    ADD COLUMN IF NOT EXISTS input_digest bytea,
    ADD COLUMN IF NOT EXISTS rerun_of_task_id uuid,
    ADD COLUMN IF NOT EXISTS rerun_root_task_id uuid,
    ADD COLUMN IF NOT EXISTS idempotency_key_digest bytea,
    ADD COLUMN IF NOT EXISTS retain_rerun_input boolean,
    ADD COLUMN IF NOT EXISTS prepared_rerun_input_disposition varchar(32),
    ADD COLUMN IF NOT EXISTS prepared_rerun_input_version smallint,
    ADD COLUMN IF NOT EXISTS prepared_rerun_input_codec varchar(64),
    ADD COLUMN IF NOT EXISTS prepared_rerun_input_content_type varchar(255),
    ADD COLUMN IF NOT EXISTS prepared_rerun_input_digest bytea,
    ADD COLUMN IF NOT EXISTS prepared_rerun_input_inline bytea,
    ADD COLUMN IF NOT EXISTS prepared_rerun_input_reference varchar(2048);
