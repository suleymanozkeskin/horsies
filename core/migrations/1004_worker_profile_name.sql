-- horsies schema migration 1004: the name of the worker profile in each worker snapshot.
--
-- The install applies this text after migration 1003 (core/migrations/1003_attempt_cancelled.sql), in one READ
-- COMMITTED transaction with its bookkeeping row. A database that applied an older text of migration 1004 is refused
-- by the checksum.
--
-- Changes:
-- - horsies_worker_states: profile_name keeps the name of the worker profile that the worker runs: the --profile value
--   of `horsies worker`, the profile of WorkerStart in the Rust client. The worker writes it with each snapshot
--   (INSERT_WORKER_STATE_SQL, core/src/worker/worker_state.rs).
--
-- Rows written before this migration keep NULL in the new column, and the migration writes no value into them. NULL
-- has one meaning: the worker that wrote the row did not record its profile name (a worker of an earlier build). The
-- readers give it as SnapshotProfile::NotRecorded (Rust) and ProfileNotRecorded (Python).
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control.
--
-- Locks: the ALTER TABLE takes ACCESS EXCLUSIVE on horsies_worker_states until the migration commits. The new column
-- is nullable with no default, so the table is not rewritten; the new CHECK reads the table once.

------------------------------------------------------------------------------------------------------------------------
-- Worker profile name (horsies_worker_states)
------------------------------------------------------------------------------------------------------------------------

-- profile_name: the name of the profile in AppConfig.worker_profiles. A name has 1 to 64 characters: letters, digits,
-- '_', '.' and '-', and the first character is not '.' or '-' (is_valid_profile_name,
-- core/src/core/config/worker_profile.rs). The worker refuses a name that breaks this rule before it writes a snapshot.
-- NULL: a row that a worker of an earlier build wrote.
-- No index: the reads take the row by worker and snapshot time.
ALTER TABLE horsies_worker_states
    ADD COLUMN profile_name character varying(64),
    ADD CONSTRAINT horsies_worker_states_profile_name_shape CHECK (
        profile_name IS NULL OR profile_name ~ '^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$'
    );
