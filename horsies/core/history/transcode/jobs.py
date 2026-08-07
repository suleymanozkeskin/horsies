"""Durable state for transcode jobs: jobs, relations, batches, guards.

Three tables carry a job across process restarts: the job row (with a
partial unique index enforcing at most ONE non-complete job — the
single-active rule is schema, not convention), the per-relation rows
(carrying the verification token's stored half and the mutation
generations), and the committed-batch ledger. A statement trigger on
every source and replacement relation bumps the owning relation row's
mutation generation, so any write between verification and swap moves
the token and the swap refuses.

Job identifiers are native uuid, and the maintenance foreign key
targets the REAL maintenance-sessions table from the gate module — the
prototype's singleton stand-ins resolved to that existing mechanism.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from ..maintenance.gate import ARCHIVE_MAINTENANCE_SESSIONS

TRANSCODE_JOBS: Final = 'horsies_archive_replacement_jobs'
TRANSCODE_RELATIONS: Final = 'horsies_archive_replacement_relations'
TRANSCODE_BATCHES: Final = 'horsies_archive_replacement_batches'
TRANSCODE_MUTATION_FUNCTION: Final = 'horsies_archive_replacement_note_mutation'

TRANSCODE_JOBS_DDL = f"""
CREATE TABLE {TRANSCODE_JOBS} (
    job_id uuid PRIMARY KEY,
    maintenance_session_id uuid NOT NULL
        REFERENCES {ARCHIVE_MAINTENANCE_SESSIONS}(session_id),
    component text NOT NULL CHECK (
        component IN ('HISTORY_ROW', 'RESULT', 'ATTEMPTS', 'RERUN_INPUT')
    ),
    source_version smallint NOT NULL,
    target_version smallint NOT NULL,
    source_codec text NOT NULL,
    target_codec text NOT NULL,
    state text NOT NULL CHECK (
        state IN (
            'PLANNED', 'COPYING', 'COPIED',
            'VERIFIED', 'SWAPPED', 'COMPLETE'
        )
    ),
    transformed_rows bigint NOT NULL CHECK (transformed_rows >= 0),
    copied_rows_total bigint NOT NULL CHECK (copied_rows_total >= 0),
    copied_rows_completed bigint NOT NULL CHECK (
        copied_rows_completed >= 0
        AND copied_rows_completed <= copied_rows_total
    ),
    payload_rows bigint NOT NULL CHECK (payload_rows >= 0),
    payload_bytes_before bigint NOT NULL CHECK (payload_bytes_before >= 0),
    projected_payload_bytes bigint NOT NULL CHECK (
        projected_payload_bytes >= 0
    ),
    affected_relation_bytes bigint NOT NULL CHECK (
        affected_relation_bytes >= 0
    ),
    started_at timestamptz NOT NULL,
    last_batch_at timestamptz,
    copied_at timestamptz,
    verified_at timestamptz,
    swapped_at timestamptz,
    completed_at timestamptz,
    start_lsn pg_lsn NOT NULL,
    wal_bytes bigint CHECK (wal_bytes IS NULL OR wal_bytes >= 0),
    CHECK ((state = 'COMPLETE') = (completed_at IS NOT NULL)),
    CHECK ((state = 'COMPLETE') = (wal_bytes IS NOT NULL))
)
"""

TRANSCODE_SINGLE_ACTIVE_INDEX_DDL = f"""
CREATE UNIQUE INDEX {TRANSCODE_JOBS}_single_active_idx
    ON {TRANSCODE_JOBS} ((1))
    WHERE state <> 'COMPLETE'
"""

TRANSCODE_RELATIONS_DDL = f"""
CREATE TABLE {TRANSCODE_RELATIONS} (
    job_id uuid NOT NULL REFERENCES {TRANSCODE_JOBS}(job_id),
    relation_ordinal integer NOT NULL CHECK (relation_ordinal > 0),
    source_relation_oid bigint NOT NULL,
    source_relation_name text NOT NULL,
    parent_relation_oid bigint NOT NULL,
    parent_relation_name text NOT NULL,
    partition_bound text NOT NULL,
    partition_constraint text NOT NULL,
    replacement_relation_name text NOT NULL,
    replacement_relation_oid bigint,
    backup_relation_name text NOT NULL,
    state text NOT NULL CHECK (
        state IN (
            'PLANNED', 'COPYING', 'COPIED',
            'VERIFIED', 'SWAPPED', 'COMPLETE'
        )
    ),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    transformed_rows bigint NOT NULL CHECK (transformed_rows >= 0),
    rows_copied bigint NOT NULL CHECK (
        rows_copied >= 0 AND rows_copied <= row_count
    ),
    relation_bytes bigint NOT NULL CHECK (relation_bytes >= 0),
    last_source_ctid tid,
    source_mutation_generation bigint NOT NULL DEFAULT 0
        CHECK (source_mutation_generation >= 0),
    replacement_mutation_generation bigint NOT NULL DEFAULT 0
        CHECK (replacement_mutation_generation >= 0),
    verified_source_generation bigint CHECK (
        verified_source_generation IS NULL
        OR verified_source_generation >= 0
    ),
    verified_replacement_generation bigint CHECK (
        verified_replacement_generation IS NULL
        OR verified_replacement_generation >= 0
    ),
    verified_source_filenode bigint,
    verified_replacement_filenode bigint,
    verified_source_schema_signature text,
    verified_replacement_schema_signature text,
    prepared_at timestamptz,
    copied_at timestamptz,
    verified_at timestamptz,
    swapped_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY (job_id, relation_ordinal),
    UNIQUE (job_id, source_relation_name),
    UNIQUE (job_id, replacement_relation_name),
    UNIQUE (job_id, backup_relation_name)
)
"""

TRANSCODE_BATCHES_DDL = f"""
CREATE TABLE {TRANSCODE_BATCHES} (
    job_id uuid NOT NULL REFERENCES {TRANSCODE_JOBS}(job_id),
    batch_number integer NOT NULL CHECK (batch_number > 0),
    relation_ordinal integer NOT NULL,
    rows_copied integer NOT NULL CHECK (rows_copied > 0),
    committed_at timestamptz NOT NULL,
    PRIMARY KEY (job_id, batch_number),
    FOREIGN KEY (job_id, relation_ordinal)
        REFERENCES {TRANSCODE_RELATIONS}(job_id, relation_ordinal)
)
"""

TRANSCODE_MUTATION_FUNCTION_DDL = f"""
CREATE FUNCTION {TRANSCODE_MUTATION_FUNCTION}()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    changed_rows integer;
BEGIN
    UPDATE {TRANSCODE_RELATIONS}
    SET source_mutation_generation =
            source_mutation_generation
            + CASE WHEN source_relation_oid = TG_RELID
                   THEN 1 ELSE 0 END,
        replacement_mutation_generation =
            replacement_mutation_generation
            + CASE WHEN replacement_relation_oid = TG_RELID
                   THEN 1 ELSE 0 END
    WHERE state <> 'COMPLETE'
      AND (
            source_relation_oid = TG_RELID
            OR replacement_relation_oid = TG_RELID
          );
    GET DIAGNOSTICS changed_rows = ROW_COUNT;
    IF changed_rows <> 1 THEN
        RAISE EXCEPTION
            'archive replacement mutation guard has % owners for %',
            changed_rows, TG_RELID;
    END IF;
    RETURN NULL;
END
$function$
"""


def job_state_fragments() -> tuple[str, ...]:
    """The durable-state DDL, in installation order."""
    return (
        TRANSCODE_JOBS_DDL,
        TRANSCODE_SINGLE_ACTIVE_INDEX_DDL,
        TRANSCODE_RELATIONS_DDL,
        TRANSCODE_BATCHES_DDL,
        TRANSCODE_MUTATION_FUNCTION_DDL,
    )


@dataclass(frozen=True, slots=True)
class TranscodeJobRow:
    """One job row, decoded fail-closed."""

    job_id: str
    maintenance_session_id: str
    component: str
    source_version: int
    target_version: int
    source_codec: str
    target_codec: str
    state: str
    transformed_rows: int
    copied_rows_total: int
    copied_rows_completed: int
    relation_count: int
    start_lsn: str
    wal_bytes: int | None


@dataclass(frozen=True, slots=True)
class TranscodeRelationRow:
    """One relation row, decoded fail-closed."""

    job_id: str
    relation_ordinal: int
    source_relation_oid: int
    source_relation_name: str
    parent_relation_oid: int
    parent_relation_name: str
    partition_bound: str
    partition_constraint: str
    replacement_relation_name: str
    replacement_relation_oid: int | None
    backup_relation_name: str
    state: str
    row_count: int
    transformed_rows: int
    rows_copied: int
    last_source_ctid: str | None
    source_mutation_generation: int
    replacement_mutation_generation: int
    verified_source_generation: int | None
    verified_replacement_generation: int | None
    verified_source_filenode: int | None
    verified_replacement_filenode: int | None
    verified_source_schema_signature: str | None
    verified_replacement_schema_signature: str | None


@dataclass(frozen=True, slots=True)
class RelationVerificationToken:
    """The six-field identity captured at verification, re-checked at
    swap: generations, filenodes, and UTC-pinned schema signatures for
    both sides."""

    source_generation: int
    replacement_generation: int
    source_filenode: int
    replacement_filenode: int
    source_schema_signature: str
    replacement_schema_signature: str


def decode_relation_row(mapping: Any) -> TranscodeRelationRow:
    """Decode one relation row from a RowMapping."""
    return TranscodeRelationRow(
        job_id=str(mapping['job_id']),
        relation_ordinal=int(mapping['relation_ordinal']),
        source_relation_oid=int(mapping['source_relation_oid']),
        source_relation_name=str(mapping['source_relation_name']),
        parent_relation_oid=int(mapping['parent_relation_oid']),
        parent_relation_name=str(mapping['parent_relation_name']),
        partition_bound=str(mapping['partition_bound']),
        partition_constraint=str(mapping['partition_constraint']),
        replacement_relation_name=str(mapping['replacement_relation_name']),
        replacement_relation_oid=(
            int(mapping['replacement_relation_oid'])
            if mapping['replacement_relation_oid'] is not None
            else None
        ),
        backup_relation_name=str(mapping['backup_relation_name']),
        state=str(mapping['state']),
        row_count=int(mapping['row_count']),
        transformed_rows=int(mapping['transformed_rows']),
        rows_copied=int(mapping['rows_copied']),
        last_source_ctid=(
            str(mapping['last_source_ctid'])
            if mapping['last_source_ctid'] is not None
            else None
        ),
        source_mutation_generation=int(
            mapping['source_mutation_generation']
        ),
        replacement_mutation_generation=int(
            mapping['replacement_mutation_generation']
        ),
        verified_source_generation=(
            int(mapping['verified_source_generation'])
            if mapping['verified_source_generation'] is not None
            else None
        ),
        verified_replacement_generation=(
            int(mapping['verified_replacement_generation'])
            if mapping['verified_replacement_generation'] is not None
            else None
        ),
        verified_source_filenode=(
            int(mapping['verified_source_filenode'])
            if mapping['verified_source_filenode'] is not None
            else None
        ),
        verified_replacement_filenode=(
            int(mapping['verified_replacement_filenode'])
            if mapping['verified_replacement_filenode'] is not None
            else None
        ),
        verified_source_schema_signature=(
            str(mapping['verified_source_schema_signature'])
            if mapping['verified_source_schema_signature'] is not None
            else None
        ),
        verified_replacement_schema_signature=(
            str(mapping['verified_replacement_schema_signature'])
            if mapping['verified_replacement_schema_signature'] is not None
            else None
        ),
    )
