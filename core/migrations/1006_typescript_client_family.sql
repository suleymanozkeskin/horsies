-- horsies schema migration 1006: the client family TYPESCRIPT.
--
-- The install applies this text after the migration before it in the chain (MIGRATION_SOURCES,
-- core/src/broker/migrations.rs), in one READ COMMITTED transaction with its bookkeeping row. A database that applied
-- an older text of migration 1006 is refused by the checksum.
--
-- Changes:
-- - horsies_client_family: the CHECK horsies_client_family_family_known admits 'TYPESCRIPT' in addition to 'PYTHON' and
--   'RUST'. The core reads the value as ClientFamily::TypeScript (core/src/broker/client_family.rs). A handle writes only
--   into a database of its own family, so a Python or a Rust handle refuses a TYPESCRIPT database with
--   ClientFamilyMismatch, as it refuses a database of the other one.
--
-- The migration does not change the family row, and the Python and Rust clients never write 'TYPESCRIPT'.
--
-- After this migration the CHECK exists with the predicate below, whatever the database held before: the baseline
-- CHECK, no CHECK of this name (dropped out of band), or a CHECK of this name with another predicate. A family row that
-- the new predicate refuses makes the ADD CONSTRAINT fail (23514); the transaction rolls back and nothing changes.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control.
--
-- Locks: the ALTER TABLE takes ACCESS EXCLUSIVE on horsies_client_family until the migration commits. The table holds
-- one row at most, so the new CHECK reads one row.

------------------------------------------------------------------------------------------------------------------------
-- Client family values (horsies_client_family)
------------------------------------------------------------------------------------------------------------------------

-- family: the client library that installed the database. PYTHON: the Python client. RUST: the Rust client.
-- TYPESCRIPT: the TypeScript client.
ALTER TABLE horsies_client_family
    DROP CONSTRAINT IF EXISTS horsies_client_family_family_known,
    ADD CONSTRAINT horsies_client_family_family_known CHECK (family IN ('PYTHON', 'RUST', 'TYPESCRIPT'));
