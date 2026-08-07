"""Paired terminalization measurement for the bounded inline rerun-input envelope.

The declared comparison is one live task carrying a prepared rerun-input
envelope, terminalized two ways on the same server: the pre-consolidation
same-row terminal update, and the direct live-to-history transition that copies
the prepared envelope into final history and removes the live row.

Both sides run on structurally identical live tables inside one disposable
schema. Two tables rather than one keeps candidate rows from sharing identifiers
with baseline rows; replaying the deployed index set onto both keeps the
comparison from crediting the candidate for indexes it merely does not have.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from random import Random
from shutil import disk_usage
from time import perf_counter
from uuid import uuid4
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.history.terminalization.live_cutover import (
    cutover_column_definitions,
)
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.schemas.terminalization import OUTCOME_COLUMNS


def _drop_inherited_transitional_columns(relation: str) -> str:
    """Restore the qualified v26 clone shape after LIKE horsies_tasks."""
    drops = ',\n    '.join(
        f'DROP COLUMN IF EXISTS {name}'
        for name, _ in cutover_column_definitions()
    )
    return f'ALTER TABLE {relation}\n    {drops}'
from tests.perf.counters import Counts
from tests.perf.statistics import (
    Budget,
    Comparison,
    Verdict,
    compare,
    percentile_ms,
    worst,
)
from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_conditions,
)
from tests.task_history_prototypes.qualification_io import (
    AtomicEvidenceWriter,
    QualificationProgress,
    QualificationProgressReporter,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.transcode import (
    install_archive_transcode_prototype,
)


class RerunTerminalizationError(Exception):
    """A condition the measurement requires was not met."""


class BaselineRelationError(RerunTerminalizationError):
    """The baseline statement template could not be bound to a relation."""


class IndexReplayError(RerunTerminalizationError):
    """The deployed task-table index set could not be reproduced per side."""


class PreparedEnvelopeError(RerunTerminalizationError):
    """A prepared rerun-input envelope violates its declared bound."""


# The inclusive inline bound under test. An envelope above it is not a smaller
# measurement of the same thing — it is a different storage disposition.
INLINE_BOUND_BYTES = 65_536

_RELATION_TOKEN = '{relation}'
_SOURCE_RELATION = 'horsies_tasks'
_IDENTIFIER = re.compile(r'^[a-z_][a-z0-9_]*$')

# Copied verbatim from tests/perf/legacy_terminalization_sql.py's
# MARK_TASK_FAILED_SQL, with the single relation name replaced by a format
# token. That module states its statements are retained only as performance
# controls, so this collector copies the text rather than importing and
# rewriting it. tests/unit/test_rerun_terminalization_baseline_copy.py asserts
# this template still renders to the original character for character, so the
# copy cannot drift away from the statement it claims to be.
BASELINE_TERMINAL_FAILURE_TEMPLATE = """
    UPDATE {relation}
    SET status='FAILED',
        failed_at = NOW(),
        result = :result_json,
        error_code = :error_code,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
    RETURNING id
"""


class PairedSide(StrEnum):
    """Which implementation a block of observations measures."""

    BASELINE = 'baseline'
    CANDIDATE = 'candidate'


class PayloadShape(StrEnum):
    """Whether the envelope bytes compress, which decides retained bytes."""

    COMPRESSIBLE = 'compressible'
    INCOMPRESSIBLE = 'incompressible'


@dataclass(frozen=True, slots=True)
class SideRelations:
    """The relations one side of the comparison owns."""

    schema: PrototypeSchema
    side: PairedSide

    @property
    def live_tasks(self) -> str:
        return f'{self.schema.sql}."{self.side.value}_live_tasks"'

    @property
    def live_attempts(self) -> str:
        return f'{self.schema.sql}."{self.side.value}_live_attempts"'

    @property
    def live_tasks_name(self) -> str:
        return f'{self.side.value}_live_tasks'

    @property
    def live_attempts_name(self) -> str:
        return f'{self.side.value}_live_attempts'


def baseline_statement_text(*, relation: str) -> str:
    """Render the copied baseline statement against one relation.

    Rendering rather than string-replacing at the call site keeps exactly one
    substitution point, which is what the drift test can then pin.
    """
    if _RELATION_TOKEN not in BASELINE_TERMINAL_FAILURE_TEMPLATE:
        raise BaselineRelationError(
            'the baseline template lost its relation token'
        )
    if not relation.strip():
        raise BaselineRelationError('baseline relation must be non-empty')
    return BASELINE_TERMINAL_FAILURE_TEMPLATE.replace(_RELATION_TOKEN, relation)


def source_baseline_relation() -> str:
    """The relation the copied statement was frozen against."""
    return _SOURCE_RELATION


def validate_prepared_envelope(payload: bytes) -> None:
    """Reject an envelope the inline disposition would not have accepted.

    The bound decision happens at enqueue in the ratified contract, so a
    terminalization measurement that silently accepted an over-bound value
    would be measuring a disposition that cannot occur.
    """
    if not payload:
        raise PreparedEnvelopeError('prepared rerun input must be non-empty')
    if len(payload) > INLINE_BOUND_BYTES:
        raise PreparedEnvelopeError(
            f'prepared rerun input is {len(payload)} bytes, above the '
            f'{INLINE_BOUND_BYTES}-byte inline bound'
        )


async def deployed_task_index_definitions(
    connection: AsyncConnection,
) -> tuple[str, ...]:
    """Every secondary index actually present on the deployed task table.

    Read from the catalog rather than transcribed from the migration program:
    a migration that dropped or repredicated an index would otherwise leave the
    measurement replaying an index set the server does not have, and the paired
    comparison would silently stop being paired.
    """
    rows = (
        await connection.execute(
            text(
                """
                SELECT i.indexdef AS definition,
                       i.indexname AS name
                FROM pg_indexes AS i
                WHERE i.tablename = :relation
                  AND i.schemaname = current_schema()
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_constraint AS c
                      JOIN pg_class AS r ON r.oid = c.conindid
                      WHERE c.contype IN ('p', 'u')
                        AND r.relname = i.indexname
                  )
                ORDER BY i.indexname
                """
            ),
            {'relation': _SOURCE_RELATION},
        )
    ).all()
    definitions = tuple(str(row.definition) for row in rows)
    if not definitions:
        raise IndexReplayError(
            f'no secondary indexes found on {_SOURCE_RELATION}; the paired '
            'live tables would not carry the deployed index set'
        )
    return definitions


def rebind_index_definition(
    definition: str,
    *,
    relations: SideRelations,
) -> str:
    """Point one deployed index definition at a side's live table.

    Both the index name and the table it indexes have to move: two sides in one
    schema cannot share an index name, and an index left pointing at the
    deployed table would be measured by neither side.
    """
    match = re.match(
        r'^CREATE (UNIQUE )?INDEX (?P<name>\S+) ON '
        r'(?P<qualified>\S+) (?P<remainder>.+)$',
        definition,
        flags=re.DOTALL,
    )
    if match is None:
        raise IndexReplayError(
            f'could not parse deployed index definition: {definition!r}'
        )
    unique = 'UNIQUE ' if definition.startswith('CREATE UNIQUE') else ''
    source_name = match.group('name')
    if _IDENTIFIER.fullmatch(source_name) is None:
        raise IndexReplayError(
            f'deployed index name is not a plain identifier: {source_name!r}'
        )
    replayed_name = f'{relations.side.value}_{source_name}'
    if len(replayed_name.encode('utf-8')) > 63:
        raise IndexReplayError(
            f'replayed index name {replayed_name!r} exceeds the 63-byte '
            'PostgreSQL identifier bound'
        )
    remainder = match.group('remainder')
    return (
        f'CREATE {unique}INDEX "{replayed_name}" '
        f'ON {relations.live_tasks} {remainder}'
    )


class RerunInputDisposition(StrEnum):
    """The exhaustive stored discriminant for retained rerun input.

    Ratified vocabulary. The two available dispositions carry envelope fields;
    the three unavailable ones require every envelope field to be null.
    Eligibility is classified before policy, so a completed or workflow-backing
    request is never merely declined.
    """

    INLINE = 'INLINE'
    REFERENCE = 'REFERENCE'
    DECLINED_BY_POLICY = 'DECLINED_BY_POLICY'
    OVER_BOUND = 'OVER_BOUND'
    NEVER_ELIGIBLE = 'NEVER_ELIGIBLE'

    @property
    def carries_envelope(self) -> bool:
        match self:
            case (
                RerunInputDisposition.INLINE | RerunInputDisposition.REFERENCE
            ):
                return True
            case (
                RerunInputDisposition.DECLINED_BY_POLICY
                | RerunInputDisposition.OVER_BOUND
                | RerunInputDisposition.NEVER_ELIGIBLE
            ):
                return False


# The history projection under measurement predates the ratified discriminant
# and still names the column `rerun_input_form`, admitting NULL, 'INLINE', or
# 'REFERENCE'. The live side below uses the ratified five-value vocabulary,
# and the transition maps it onto the stored column: the two available
# dispositions keep their exact stored value, and the three unavailable ones
# collapse to NULL with every envelope field null.
#
# For this gate that mapping is byte-identical, because the measured
# disposition is INLINE and stores the same six characters under either name.
# The shapes differ only on rows that carry no envelope, which this gate does
# not measure. Measuring on the existing column therefore keeps the separately
# qualified storage evidence valid at its own head instead of forcing a rerun.
_UNAVAILABLE_STORED_FORM = None


def stored_history_form(
    disposition: RerunInputDisposition,
) -> str | None:
    """Map a ratified live disposition onto the measured history column."""
    match disposition:
        case RerunInputDisposition.INLINE:
            return RerunInputDisposition.INLINE.value
        case RerunInputDisposition.REFERENCE:
            return RerunInputDisposition.REFERENCE.value
        case (
            RerunInputDisposition.DECLINED_BY_POLICY
            | RerunInputDisposition.OVER_BOUND
            | RerunInputDisposition.NEVER_ELIGIBLE
        ):
            return _UNAVAILABLE_STORED_FORM


# --------------------------------------------------------------------------
# Disposable schema for the paired comparison.
#
# Authored here rather than reused from
# tests/task_history_prototypes/terminalization.py because that module's
# transition carries no rerun-input envelope: it has no envelope columns on its
# live table (terminalization.py:37-48) and its history insert omits every
# rerun_input_* column (terminalization.py:326-343). Measuring it unchanged
# would measure a terminalization that writes no envelope.
#
# The structure below follows that module's transition step for step —
# availability probe, advisory lock, locked read, identity check, attempt
# snapshot, single history insert, live deletes, post-commit notification —
# with envelope carriage added. The shared module is not edited, so the
# separately qualified storage and terminalization evidence keeps its authority
# at its own head.
# --------------------------------------------------------------------------

_LIVE_ENVELOPE_COLUMNS = """
    ADD COLUMN retention_class_key text NOT NULL
        REFERENCES {namespace}.retention_classes(class_key),
    ADD COLUMN rerun_of_task_id varchar(36),
    ADD COLUMN rerun_root_task_id varchar(36),
    ADD COLUMN input_digest bytea,
    ADD COLUMN retain_rerun_input boolean NOT NULL,
    ADD COLUMN prepared_rerun_input_disposition varchar(32) NOT NULL,
    ADD COLUMN prepared_rerun_input_version smallint,
    ADD COLUMN prepared_rerun_input_codec varchar(64),
    ADD COLUMN prepared_rerun_input_content_type varchar(255),
    ADD COLUMN prepared_rerun_input_digest bytea,
    ADD COLUMN prepared_rerun_input_inline bytea,
    ADD COLUMN prepared_rerun_input_reference varchar(2048),
    ADD CHECK (input_digest IS NULL OR octet_length(input_digest) = 32),
    ADD CHECK (
        prepared_rerun_input_disposition IN (
            'INLINE', 'REFERENCE',
            'DECLINED_BY_POLICY', 'OVER_BOUND', 'NEVER_ELIGIBLE'
        )
    ),
    ADD CHECK (
        (prepared_rerun_input_disposition = 'INLINE'
            AND prepared_rerun_input_version IS NOT NULL
            AND prepared_rerun_input_codec IS NOT NULL
            AND prepared_rerun_input_content_type IS NOT NULL
            AND prepared_rerun_input_digest IS NOT NULL
            AND prepared_rerun_input_inline IS NOT NULL
            AND prepared_rerun_input_reference IS NULL)
        OR (prepared_rerun_input_disposition = 'REFERENCE'
            AND prepared_rerun_input_version IS NOT NULL
            AND prepared_rerun_input_codec IS NOT NULL
            AND prepared_rerun_input_content_type IS NOT NULL
            AND prepared_rerun_input_digest IS NOT NULL
            AND prepared_rerun_input_inline IS NULL
            AND prepared_rerun_input_reference IS NOT NULL)
        OR (prepared_rerun_input_disposition IN (
                'DECLINED_BY_POLICY', 'OVER_BOUND', 'NEVER_ELIGIBLE')
            AND prepared_rerun_input_version IS NULL
            AND prepared_rerun_input_codec IS NULL
            AND prepared_rerun_input_content_type IS NULL
            AND prepared_rerun_input_digest IS NULL
            AND prepared_rerun_input_inline IS NULL
            AND prepared_rerun_input_reference IS NULL)
    ),
    ADD CHECK (
        prepared_rerun_input_inline IS NULL
        OR octet_length(prepared_rerun_input_inline) <= {inline_bound}
    ),
    ADD CHECK (
        prepared_rerun_input_disposition <> 'DECLINED_BY_POLICY'
        OR retain_rerun_input IS FALSE
    ),
    ADD CHECK (
        prepared_rerun_input_disposition NOT IN ('INLINE', 'REFERENCE')
        OR retain_rerun_input IS TRUE
    )
"""


def _live_table_statements(relations: SideRelations) -> tuple[str, ...]:
    """One side's live task and attempt relations.

    Both sides get the same statements, so any cost difference the measurement
    reports comes from the transition rather than from the tables it runs on.
    """
    namespace = relations.schema.sql
    envelope = _LIVE_ENVELOPE_COLUMNS.format(
        namespace=namespace,
        inline_bound=INLINE_BOUND_BYTES,
    )
    return (
        f"""
        CREATE TABLE {relations.live_tasks} (
            LIKE horsies_tasks
                INCLUDING DEFAULTS
                INCLUDING GENERATED
                INCLUDING IDENTITY
                INCLUDING STORAGE
        )
        """,
        # The v27 chain adds transitional cutover columns to the cloned
        # source; the qualified collector base is the v26 column set,
        # and the envelope ALTER below adds its own shapes. Restore the
        # qualified clone shape from the one shape authority.
        _drop_inherited_transitional_columns(relations.live_tasks),
        f'ALTER TABLE {relations.live_tasks} ADD PRIMARY KEY (id)',
        f'ALTER TABLE {relations.live_tasks} {envelope}',
        f"""
        CREATE TABLE {relations.live_attempts} (
            LIKE horsies_task_attempts
                INCLUDING DEFAULTS
                INCLUDING GENERATED
                INCLUDING IDENTITY
                INCLUDING STORAGE
        )
        """,
        f'ALTER TABLE {relations.live_attempts} ADD PRIMARY KEY (id)',
        f"""
        CREATE UNIQUE INDEX "{relations.side.value}_live_attempts_task_idx"
            ON {relations.live_attempts} (task_id, attempt)
        """,
    )


def _outcome_type_statement(relations: SideRelations) -> str:
    """The wire row shape, taken from the production vocabulary."""
    columns = ',\n            '.join(
        f'{name} {kind}' for name, kind in OUTCOME_COLUMNS
    )
    return f"""
    CREATE TYPE {relations.schema.sql}.rerun_terminalization_outcome AS (
            {columns}
    )
    """


def _attempt_snapshot_statement(relations: SideRelations) -> str:
    """Positional attempt codec v1 over one side's live attempts.

    Field order is part of the version. Copied from
    tests/task_history_prototypes/terminalization.py:136-179 with the relation
    rebound; the encoding itself is unchanged so the snapshot bytes this gate
    writes are the qualified ones.
    """
    return f"""
    CREATE FUNCTION {relations.schema.sql}.encode_candidate_attempts(
        p_task_id varchar
    )
    RETURNS bytea
    LANGUAGE sql
    STABLE
    STRICT
    AS $function$
        SELECT convert_to(
            '[' || COALESCE(
                string_agg(
                    '[' || to_jsonb(a.attempt)::text || ',' ||
                    to_jsonb(a.outcome)::text || ',' ||
                    to_jsonb(a.will_retry)::text || ',' ||
                    to_jsonb(
                        floor(
                            extract(epoch FROM a.started_at) * 1000000
                        )::bigint
                    )::text || ',' ||
                    to_jsonb(
                        floor(
                            extract(epoch FROM a.finished_at) * 1000000
                        )::bigint
                    )::text || ',' ||
                    COALESCE(to_jsonb(a.error_code)::text, 'null') || ',' ||
                    COALESCE(to_jsonb(a.error_message)::text, 'null') || ',' ||
                    COALESCE(to_jsonb(a.failed_reason)::text, 'null') || ',' ||
                    COALESCE(to_jsonb(a.worker_id)::text, 'null') || ',' ||
                    COALESCE(to_jsonb(a.worker_hostname)::text, 'null') || ',' ||
                    COALESCE(to_jsonb(a.worker_pid)::text, 'null') || ',' ||
                    COALESCE(
                        to_jsonb(a.worker_process_name)::text,
                        'null'
                    ) || ']',
                    ',' ORDER BY a.attempt
                ),
                ''
            ) || ']',
            'UTF8'
        )
        FROM {relations.live_attempts} AS a
        WHERE a.task_id = p_task_id
    $function$
    """


def _move_statement(
    relations: SideRelations,
    *,
    duplicate_envelope: bool,
) -> str:
    """The direct live-to-history transition, carrying the prepared envelope.

    Transaction structure, in order, following
    tests/task_history_prototypes/terminalization.py:182-396: archive
    availability probe, per-task advisory lock, locked read of the live row,
    liveness guard, identity-uniqueness guard, attempt snapshot, one history
    insert, live attempt and task deletes, then the post-commit notification.

    Envelope handling is the addition. Eligibility is classified before policy:
    a workflow backing request or a completed request is never eligible, an
    otherwise eligible request whose enqueue-time snapshot declined retention is
    declined, and only an available disposition carries envelope fields. The
    prepared value is copied, never re-encoded — terminalization does not
    reserialize task input.

    `duplicate_envelope` exists for the detection control: it stores the
    envelope bytes a second time, in the result payload, which the retained-byte
    rule forbids. The schema permits that write — a failed task may carry a
    result — so only the harness can catch it, which is the point of a
    detection control. It is never true in a measured cell.
    """
    namespace = relations.schema.sql
    stored_result = (
        'COALESCE(v_envelope_inline, v_result_payload)'
        if duplicate_envelope
        else 'v_result_payload'
    )
    return f"""
    CREATE FUNCTION {namespace}.move_candidate_task_to_history(
        p_task_id varchar,
        p_terminal_at timestamptz,
        p_result text,
        p_error_code text,
        p_failed_reason text
    ) RETURNS void
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_task {relations.live_tasks}%ROWTYPE;
        v_attempt_snapshot bytea;
        v_result_payload bytea;
        v_stored_form varchar(16);
        v_envelope_version smallint;
        v_envelope_codec varchar(64);
        v_envelope_content_type varchar(255);
        v_envelope_digest bytea;
        v_envelope_inline bytea;
        v_envelope_reference varchar(2048);
        v_history_rows bigint;
        v_deleted_rows bigint;
    BEGIN
        PERFORM {namespace}.assert_archive_available();
        PERFORM pg_advisory_xact_lock(hashtextextended(p_task_id, 731));

        SELECT * INTO STRICT v_task
        FROM {relations.live_tasks}
        WHERE id = p_task_id
        FOR UPDATE;

        IF v_task.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING') THEN
            RAISE EXCEPTION 'live task has non-live status %', v_task.status;
        END IF;
        IF p_terminal_at IS NULL THEN
            RAISE EXCEPTION 'terminal timestamp is required';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM {namespace}.history_aggregate
            WHERE task_id = p_task_id
        ) THEN
            RAISE EXCEPTION 'task identity exists in multiple locations';
        END IF;

        -- Eligibility precedes policy. A never-eligible request is not
        -- "declined": the two are different public reasons, and collapsing
        -- them would misreport why the input is unavailable.
        IF v_task.is_workflow_task THEN
            v_stored_form := NULL;
        ELSIF NOT v_task.retain_rerun_input THEN
            v_stored_form := NULL;
        ELSIF v_task.prepared_rerun_input_disposition IN (
            'INLINE', 'REFERENCE'
        ) THEN
            v_stored_form := v_task.prepared_rerun_input_disposition;
        ELSE
            v_stored_form := NULL;
        END IF;

        IF v_stored_form IS NULL THEN
            v_envelope_version := NULL;
            v_envelope_codec := NULL;
            v_envelope_content_type := NULL;
            v_envelope_digest := NULL;
            v_envelope_inline := NULL;
            v_envelope_reference := NULL;
        ELSE
            -- Copied, not re-encoded. The bytes and the digest are the ones
            -- enqueue decided; terminalization never reserializes input.
            v_envelope_version := v_task.prepared_rerun_input_version;
            v_envelope_codec := v_task.prepared_rerun_input_codec;
            v_envelope_content_type :=
                v_task.prepared_rerun_input_content_type;
            v_envelope_digest := v_task.prepared_rerun_input_digest;
            v_envelope_inline := v_task.prepared_rerun_input_inline;
            v_envelope_reference := v_task.prepared_rerun_input_reference;
        END IF;

        v_attempt_snapshot := {namespace}.encode_candidate_attempts(p_task_id);
        v_result_payload := CASE
            WHEN p_result IS NULL THEN NULL
            ELSE convert_to(p_result, 'UTF8')
        END;

        INSERT INTO {namespace}.history_aggregate (
            task_id, task_name, queue_name, priority,
            command_fingerprint_version, command_fingerprint, status,
            terminalization_kind, terminal_at, retention_anchor_at,
            retention_class_key, sent_at, enqueued_at, claimed_at,
            started_at, created_at, good_until,
            result_envelope_version, result_codec, result_content_type,
            result_payload, result_digest, error_code, final_failed_reason,
            prior_result_payload, retry_count, max_retries,
            last_claimed_worker_id, last_worker_hostname,
            last_worker_pid, last_worker_process_name,
            rerun_of_task_id, rerun_root_task_id, input_digest,
            rerun_input_version, rerun_input_codec,
            rerun_input_content_type, rerun_input_form,
            rerun_input_digest, rerun_input_inline, rerun_input_reference,
            workflow_id, is_workflow_task,
            history_schema_version, attempt_archive_version,
            attempt_snapshot_codec, attempt_snapshot_content_type,
            attempt_snapshot, attempt_snapshot_digest
        ) VALUES (
            v_task.id, v_task.task_name, v_task.queue_name, v_task.priority,
            1, decode(v_task.enqueue_sha, 'hex'),
            'FAILED', '{TerminalizationKind.FAIL_RUNNING.value}',
            p_terminal_at, p_terminal_at, v_task.retention_class_key,
            v_task.sent_at, v_task.enqueued_at, v_task.claimed_at,
            v_task.started_at, v_task.created_at, v_task.good_until,
            1, 'json-utf8', 'application/json', {stored_result},
            CASE WHEN {stored_result} IS NULL
                 THEN NULL ELSE sha256({stored_result}) END,
            p_error_code, p_failed_reason,
            NULL::bytea, v_task.retry_count, v_task.max_retries,
            v_task.claimed_by_worker_id, v_task.worker_hostname,
            v_task.worker_pid, v_task.worker_process_name,
            v_task.rerun_of_task_id, v_task.rerun_root_task_id,
            v_task.input_digest,
            v_envelope_version, v_envelope_codec,
            v_envelope_content_type, v_stored_form,
            v_envelope_digest, v_envelope_inline, v_envelope_reference,
            NULL, v_task.is_workflow_task,
            1, 1, 'json-utf8', 'application/json',
            v_attempt_snapshot, sha256(v_attempt_snapshot)
        );
        GET DIAGNOSTICS v_history_rows = ROW_COUNT;
        IF v_history_rows <> 1 THEN
            RAISE EXCEPTION 'terminal history insert did not affect one row';
        END IF;

        DELETE FROM {relations.live_attempts} WHERE task_id = p_task_id;
        DELETE FROM {relations.live_tasks} WHERE id = p_task_id;
        GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;
        IF v_deleted_rows <> 1 THEN
            RAISE EXCEPTION 'live task delete did not affect one row';
        END IF;

        PERFORM pg_notify('task_done', p_task_id);
    END
    $function$
    """


def _candidate_fail_statement(relations: SideRelations) -> str:
    """The measured candidate entry point: one client statement, one commit.

    The guard mirrors the shipped locked-failure fence. A miss raises rather
    than classifying, because every observation in this gate is seeded to hit
    and a miss is a harness fault rather than a measurable outcome; production's
    miss classifier is out of scope here and lives in
    horsies/core/schemas/terminalization.py.
    """
    namespace = relations.schema.sql
    return f"""
    CREATE FUNCTION {namespace}.candidate_fail_locked_task(
        p_task_id varchar,
        p_worker_id text,
        p_result text,
        p_error_code text,
        p_failed_reason text
    ) RETURNS SETOF {namespace}.rerun_terminalization_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_claimed_at timestamptz;
        v_terminal_at timestamptz;
    BEGIN
        SELECT claimed_at INTO v_claimed_at
        FROM {relations.live_tasks}
        WHERE id = p_task_id
          AND status = 'RUNNING'
          AND claimed_by_worker_id = CAST(p_worker_id AS varchar)
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'seeded observation did not match its fence'
                USING ERRCODE = 'no_data_found';
        END IF;

        v_terminal_at := NOW();
        PERFORM {namespace}.move_candidate_task_to_history(
            p_task_id, v_terminal_at, p_result, p_error_code, p_failed_reason
        );
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'APPLIED'::text,
            v_terminal_at,
            '{TerminalizationKind.FAIL_RUNNING.value}'::text,
            'RUNNING'::text, CAST(p_worker_id AS varchar), v_claimed_at,
            NULL::text, NULL::jsonb;
    END
    $function$
    """


def _availability_statement(schema: PrototypeSchema) -> str:
    """The archive-availability probe the transition runs before it writes.

    Copied from tests/task_history_prototypes/terminalization.py:100-121. It
    reads the gate and maintenance relations owned by
    install_archive_transcode_prototype (transcode.py:1068-1076). It stays
    inside the measured transaction: it is work the shipping transition does,
    and removing it to shorten the candidate would flatter the candidate.
    """
    return f"""
    CREATE FUNCTION {schema.sql}.assert_archive_available()
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        PERFORM singleton
        FROM {schema.sql}.archive_access_gate
        WHERE singleton IS TRUE
        FOR SHARE;
        IF EXISTS (
            SELECT 1
            FROM {schema.sql}.archive_maintenance_sessions
            WHERE ended_at IS NULL
        ) THEN
            RAISE EXCEPTION 'archive maintenance is active'
                USING ERRCODE = 'object_in_use';
        END IF;
    END
    $function$
    """


async def _covering_finite_leaf(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    leaf_name: str,
) -> bool:
    """Whether the finite leaf for the run date already exists.

    The shared installer creates one dated leaf. When a run lands on that same
    date the leaf is already present, and creating it again would fail on
    overlapping bounds rather than on anything meaningful.
    """
    return bool(
        (
            await connection.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_class AS c
                        JOIN pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE n.nspname = :schema AND c.relname = :leaf
                    )
                    """
                ),
                {'schema': schema.name, 'leaf': leaf_name},
            )
        ).scalar_one()
    )


async def ensure_run_date_finite_leaf(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> str:
    """Attach a finite daily leaf covering the database's current date.

    The transition stamps `terminal_at := NOW()`, and the ratified default
    retention class is finite, so without a leaf covering the run date the
    measured insert has nowhere to route. Routing to forever instead — the
    shortcut the terminalization integration tests take — would measure a
    different retention class from the one the default selects.

    The date comes from the database clock rather than the host clock: the
    partition bound and the value being routed must be decided by the same
    clock, or a run near midnight can create a leaf that its own rows miss.

    Both the date and the bounds are pinned to UTC. A bare date literal is
    resolved in the session's timezone, so on a server west or east of UTC the
    leaf would start at a different instant than its name claims and would
    overlap the neighbouring day's leaf — the shared fixture states its bounds
    as explicit UTC instants, and these have to tile against those exactly.
    """
    run_date = (
        await connection.execute(
            text("SELECT (now() AT TIME ZONE 'UTC')::date AS run_date")
        )
    ).scalar_one()
    suffix = run_date.strftime('%Y_%m_%d')
    leaf_name = f'history_aggregate_finite_{suffix}'
    if await _covering_finite_leaf(connection, schema, leaf_name=leaf_name):
        return leaf_name
    lower_bound = f'{run_date.isoformat()}T00:00:00Z'
    upper_bound = f'{(run_date + timedelta(days=1)).isoformat()}T00:00:00Z'
    await connection.execute(
        text(
            f"""
            CREATE TABLE {schema.sql}."{leaf_name}"
                PARTITION OF {schema.sql}.history_aggregate_finite
                FOR VALUES FROM ('{lower_bound}')
                TO ('{upper_bound}')
            """
        )
    )
    await connection.execute(
        text(
            f'CREATE INDEX "{leaf_name}_id_idx" '
            f'ON {schema.sql}."{leaf_name}" (task_id)'
        )
    )
    return leaf_name


@dataclass(frozen=True, slots=True)
class InstalledComparison:
    """What one installation produced, recorded so the artifact can name it."""

    schema: PrototypeSchema
    baseline: SideRelations
    candidate: SideRelations
    finite_leaf: str
    replayed_indexes: tuple[str, ...]
    duplicate_envelope: bool


async def install_rerun_terminalization_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    duplicate_envelope: bool = False,
) -> InstalledComparison:
    """Install both sides of the paired comparison into one disposable schema.

    Requires install_archive_candidates and install_archive_transcode_prototype
    to have run first: the first owns the qualified history projection this
    measurement writes into, the second owns the archive gate relations the
    availability probe reads. Neither is modified.
    """
    baseline = SideRelations(schema=schema, side=PairedSide.BASELINE)
    candidate = SideRelations(schema=schema, side=PairedSide.CANDIDATE)

    index_definitions = await deployed_task_index_definitions(connection)
    replayed: list[str] = []
    for relations in (baseline, candidate):
        for statement in _live_table_statements(relations):
            await connection.execute(text(statement))
        for definition in index_definitions:
            replayed_statement = rebind_index_definition(
                definition,
                relations=relations,
            )
            await connection.execute(text(replayed_statement))
            replayed.append(replayed_statement)

    finite_leaf = await ensure_run_date_finite_leaf(connection, schema)

    for statement in (
        _availability_statement(schema),
        _outcome_type_statement(candidate),
        _attempt_snapshot_statement(candidate),
        _move_statement(candidate, duplicate_envelope=duplicate_envelope),
        _candidate_fail_statement(candidate),
    ):
        await connection.execute(text(statement))

    return InstalledComparison(
        schema=schema,
        baseline=baseline,
        candidate=candidate,
        finite_leaf=finite_leaf,
        replayed_indexes=tuple(replayed),
        duplicate_envelope=duplicate_envelope,
    )


# --------------------------------------------------------------------------
# Post-commit structural assertions.
#
# These are exact requirements, not sampled ones: a violation fails the cell
# whatever the latency said. They run after commit because what they assert is
# a property of committed state, and asserting inside the measured transaction
# would put the assertion's own cost into the measurement.
# --------------------------------------------------------------------------


class StructuralViolation(StrEnum):
    """What a committed transition got wrong, if anything."""

    LIVE_TASK_REMAINS = 'live_task_remains'
    LIVE_ATTEMPTS_REMAIN = 'live_attempts_remain'
    HISTORY_ROW_MISSING = 'history_row_missing'
    ENVELOPE_MISSING = 'envelope_missing'
    ENVELOPE_DIGEST_MISMATCH = 'envelope_digest_mismatch'
    ENVELOPE_COPIED_MORE_THAN_ONCE = 'envelope_copied_more_than_once'
    WRONG_RETENTION_CLASS = 'wrong_retention_class'


@dataclass(frozen=True, slots=True)
class StructuralOutcome:
    """The committed shape one candidate observation produced."""

    task_id: str
    violations: tuple[StructuralViolation, ...]

    @property
    def passed(self) -> bool:
        return not self.violations


async def assert_candidate_structure(
    connection: AsyncConnection,
    installed: InstalledComparison,
    *,
    task_id: str,
    expected_envelope: bytes,
    expected_retention_class: str,
) -> StructuralOutcome:
    """Check one committed candidate transition against its exact obligations.

    The envelope-copy count compares stored bytes rather than trusting the
    disposition column: a candidate that wrote the same payload into a second
    column would still report one envelope while retaining two.
    """
    row = (
        await connection.execute(
            text(
                f"""
                SELECT
                    h.rerun_input_inline AS envelope,
                    h.rerun_input_digest AS envelope_digest,
                    h.retention_class_key AS retention_class,
                    (h.result_payload IS NOT NULL
                        AND h.result_payload = :envelope) AS result_duplicates,
                    (h.prior_result_payload IS NOT NULL
                        AND h.prior_result_payload = :envelope)
                        AS prior_duplicates,
                    (h.attempt_snapshot = :envelope) AS attempt_duplicates
                FROM {installed.schema.sql}.history_aggregate AS h
                WHERE h.task_id = :task_id
                """
            ),
            {'task_id': task_id, 'envelope': expected_envelope},
        )
    ).one_or_none()

    live_tasks_remaining = (
        await connection.execute(
            text(
                f'SELECT COUNT(*) FROM {installed.candidate.live_tasks} '
                'WHERE id = :task_id'
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    live_attempts_remaining = (
        await connection.execute(
            text(
                f'SELECT COUNT(*) FROM {installed.candidate.live_attempts} '
                'WHERE task_id = :task_id'
            ),
            {'task_id': task_id},
        )
    ).scalar_one()

    violations: list[StructuralViolation] = []
    if live_tasks_remaining:
        violations.append(StructuralViolation.LIVE_TASK_REMAINS)
    if live_attempts_remaining:
        violations.append(StructuralViolation.LIVE_ATTEMPTS_REMAIN)
    if row is None:
        violations.append(StructuralViolation.HISTORY_ROW_MISSING)
        return StructuralOutcome(task_id=task_id, violations=tuple(violations))

    stored_envelope = row.envelope
    if stored_envelope is None:
        violations.append(StructuralViolation.ENVELOPE_MISSING)
    elif bytes(stored_envelope) != expected_envelope:
        violations.append(StructuralViolation.ENVELOPE_MISSING)
    elif (
        row.envelope_digest is None
        or bytes(row.envelope_digest) != sha256(expected_envelope).digest()
    ):
        violations.append(StructuralViolation.ENVELOPE_DIGEST_MISMATCH)

    extra_copies = sum(
        1
        for duplicated in (
            row.result_duplicates,
            row.prior_duplicates,
            row.attempt_duplicates,
        )
        if duplicated
    )
    if extra_copies:
        violations.append(StructuralViolation.ENVELOPE_COPIED_MORE_THAN_ONCE)

    if row.retention_class != expected_retention_class:
        violations.append(StructuralViolation.WRONG_RETENTION_CLASS)

    return StructuralOutcome(task_id=task_id, violations=tuple(violations))


# --------------------------------------------------------------------------
# Server-side counts.
#
# The statements below are the ones tests/perf/counters.py issues, restated
# because that module's probe is synchronous and this collector is not. They
# are pinned against their originals by
# tests/unit/test_rerun_terminalization_counter_sql.py, so the restatement
# cannot drift from the counting contract it reuses. The `Counts` shape itself
# is imported rather than redeclared.
# --------------------------------------------------------------------------

COUNTER_RESET_SQL = 'SELECT pg_stat_statements_reset()'

COUNTER_READ_SQL = """
    SELECT
        COALESCE(SUM(calls) FILTER (WHERE toplevel), 0)      AS client_statements,
        COALESCE(SUM(calls) FILTER (WHERE NOT toplevel), 0)  AS nested_statements,
        COALESCE(SUM(rows) FILTER (WHERE toplevel), 0)       AS client_rows,
        COALESCE(SUM(rows) FILTER (WHERE NOT toplevel), 0)   AS nested_rows,
        COALESCE(SUM(wal_records) FILTER (WHERE toplevel), 0) AS wal_records,
        COALESCE(SUM(wal_bytes) FILTER (WHERE toplevel), 0)  AS wal_bytes,
        COALESCE(SUM(wal_fpi) FILTER (WHERE toplevel), 0)    AS wal_fpi
    FROM pg_stat_statements
    WHERE query NOT LIKE '%pg_stat%'
      AND query NOT LIKE '%pg_snapshot%'
"""

COUNTER_WRITE_TRANSACTIONS_SQL = (
    'SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint'
)


class AsyncCounterProbe:
    """Brackets one measured block and reports what the server counted."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._connection = connection
        self._transactions_at_start: int | None = None

    async def begin(self) -> None:
        await self._connection.execute(text(COUNTER_RESET_SQL))
        self._transactions_at_start = await self._read_write_transactions()

    async def finish(self, *, terminal_rows: int) -> Counts:
        if self._transactions_at_start is None:
            raise RerunTerminalizationError('probe finished before it began')
        write_transactions = (
            await self._read_write_transactions() - self._transactions_at_start
        )
        row = (
            await self._connection.execute(text(COUNTER_READ_SQL))
        ).one()
        self._transactions_at_start = None
        return Counts(
            client_statements=int(row.client_statements),
            nested_statements=int(row.nested_statements),
            client_rows=int(row.client_rows),
            nested_rows=int(row.nested_rows),
            terminal_rows=terminal_rows,
            wal_records=int(row.wal_records),
            wal_bytes=int(row.wal_bytes),
            wal_fpi=int(row.wal_fpi),
            write_transactions=write_transactions,
        )

    async def _read_write_transactions(self) -> int:
        return int(
            (
                await self._connection.execute(
                    text(COUNTER_WRITE_TRANSACTIONS_SQL)
                )
            ).scalar_one()
        )


class CounterAvailability(StrEnum):
    """Whether this server can count statements, and if not, why not."""

    AVAILABLE = 'available'
    NOT_PRELOADED = 'environment_lacks_pg_stat_statements_preload'
    EXTENSION_ABSENT = 'environment_lacks_pg_stat_statements_extension'


class StatementCountersUnavailableError(RerunTerminalizationError):
    """This server cannot count statements, with the reason named."""


@dataclass(frozen=True, slots=True)
class StatementCounterPrerequisite:
    """What the server reports about its statement-counting prerequisites."""

    availability: CounterAvailability
    shared_preload_libraries: str
    extension_present: bool
    track: str
    reason: str

    @property
    def usable(self) -> bool:
        return self.availability is CounterAvailability.AVAILABLE


def classify_statement_counters(
    *,
    shared_preload_libraries: str,
    extension_present: bool,
) -> CounterAvailability:
    """Decide whether statement counting can work here.

    Preload is checked first and independently of the extension, because
    `CREATE EXTENSION` succeeds on a server that never loaded the library and
    the failure then surfaces later as an error from the view itself. A bench
    without the preload has to read as a missing prerequisite, not as a set of
    unexplained failures in whatever happened to query it first.
    """
    loaded = {
        library.strip()
        for library in shared_preload_libraries.split(',')
        if library.strip()
    }
    if 'pg_stat_statements' not in loaded:
        return CounterAvailability.NOT_PRELOADED
    if not extension_present:
        return CounterAvailability.EXTENSION_ABSENT
    return CounterAvailability.AVAILABLE


async def read_statement_counter_prerequisite(
    connection: AsyncConnection,
) -> StatementCounterPrerequisite:
    """Report whether this server can count statements, and why not if it cannot."""
    row = (
        await connection.execute(
            text(
                """
                SELECT
                    current_setting('shared_preload_libraries')
                        AS shared_preload_libraries,
                    COALESCE(
                        current_setting('pg_stat_statements.track', true),
                        'unavailable'
                    ) AS track,
                    EXISTS (
                        SELECT 1 FROM pg_extension
                        WHERE extname = 'pg_stat_statements'
                    ) AS extension_present
                """
            )
        )
    ).one()
    preload = str(row.shared_preload_libraries)
    extension_present = bool(row.extension_present)
    availability = classify_statement_counters(
        shared_preload_libraries=preload,
        extension_present=extension_present,
    )
    match availability:
        case CounterAvailability.AVAILABLE:
            reason = 'statement counters available'
        case CounterAvailability.NOT_PRELOADED:
            reason = (
                'pg_stat_statements is not in shared_preload_libraries on this '
                f'server (reports {preload!r}); statement, transaction and WAL '
                'counts cannot be collected here'
            )
        case CounterAvailability.EXTENSION_ABSENT:
            reason = (
                'pg_stat_statements is preloaded but the extension is not '
                'created in this database'
            )
    return StatementCounterPrerequisite(
        availability=availability,
        shared_preload_libraries=preload,
        extension_present=extension_present,
        track=str(row.track),
        reason=reason,
    )


async def install_statement_counters(
    connection: AsyncConnection,
) -> StatementCounterPrerequisite:
    """Make the statement view usable, or refuse with the reason it cannot be.

    Creating the extension is attempted only when the library is loaded: doing
    it the other way round produces a database that looks equipped and fails at
    the first read.
    """
    prerequisite = await read_statement_counter_prerequisite(connection)
    match prerequisite.availability:
        case CounterAvailability.AVAILABLE:
            return prerequisite
        case CounterAvailability.NOT_PRELOADED:
            raise StatementCountersUnavailableError(prerequisite.reason)
        case CounterAvailability.EXTENSION_ABSENT:
            await connection.execute(
                text('CREATE EXTENSION IF NOT EXISTS pg_stat_statements')
            )
            await connection.commit()
            confirmed = await read_statement_counter_prerequisite(connection)
            if not confirmed.usable:
                raise StatementCountersUnavailableError(confirmed.reason)
            return confirmed


# --------------------------------------------------------------------------
# Declared budgets.
#
# Single-row terminalization at a 200-byte result and 1-4 attempts: p50 within
# the greater of +25% or 1.00 ms, p99 within the greater of +30% or 3.00 ms,
# the p95 statement-to-commit lock envelope within the greater of +30% or
# 3.00 ms and at most 25 ms absolute through a 64 KiB result, and WAL per
# terminal task within the greater of +50% or 4 KiB.
#
# The fractions are named constants rather than literals inside the comparison
# so the detection control can tighten one deliberately and the artifact can
# report the tightened value it was tightened to.
# --------------------------------------------------------------------------

P50_BUDGET = Budget(fraction=1.25, floor_ms=1.00)
P99_BUDGET = Budget(fraction=1.30, floor_ms=3.00)
LOCK_P95_BUDGET = Budget(fraction=1.30, floor_ms=3.00)
ABSOLUTE_LOCK_P95_LIMIT_MS = 25.0
WAL_FRACTION = 0.50
WAL_FLOOR_BYTES = 4096

# Envelope-carrying terminalization is judged on copy honesty rather than on a
# share of the baseline. The baseline's same-row update never rewrites an
# unchanged out-of-line value, so it pays nothing for the envelope; the
# candidate moves the envelope into history and pays for all of it. A share of
# a baseline that never carried the payload is not a bound on carrying it once.
#
# The candidate may therefore write one copy of the prepared envelope plus a
# declared overhead. The coefficient is exactly 1.0: a second copy is the thing
# this bound exists to reject, and the structural one-copy assertion is its
# counted twin.
ENVELOPE_WAL_COEFFICIENT = 1.0

# Derived, not assumed. Residue is WAL delta per task minus prepared envelope
# bytes, measured on both supported majors in paired-micro mode at 200
# observations per side:
#
#   envelope  chunks   residue att=1   residue att=4
#      4,096       3       476 / 478       968 / 970
#      8,192       5             865            1,357
#     16,384       9   1,614 / 1,616    2,107 / 2,109
#     32,768      17           3,122            3,614
#     65,536      33   6,138 / 6,140    6,631 / 6,634
#
# (PostgreSQL 14 / PostgreSQL 16 where both were measured; three repetitions at
# 65,536 on PostgreSQL 16 spread two bytes.)
#
# The residue is NOT size-invariant. It tracks TOAST chunk count at
# 188.7 bytes per chunk, chunks = ceil(bytes / 1996), with a constant
# 492-493 byte increment from one attempt row to four. Fitting the endpoints of
# the attempt-1 series gives residue = 188.73 * chunks - 88, which reproduces
# the measured 6,140 at 33 chunks exactly.
#
# The floor below is therefore declared AT the 65,536-byte shape and the worst
# declared attempt depth, where the measured maximum across both majors is
# 6,634 bytes. Eight kibibytes covers that with roughly 23% headroom while
# remaining an order of magnitude below the 65,536 bytes a second envelope copy
# would add, so the bound still rejects what it exists to reject. A different
# inline bound must RE-DERIVE this floor from the per-chunk model above rather
# than inherit it.
ENVELOPE_WAL_OVERHEAD_FLOOR_BYTES = 8_192
ENVELOPE_WAL_DERIVED_AT_BYTES = 65_536
ENVELOPE_WAL_WORST_MEASURED_RESIDUE_BYTES = 6_634

_DEFAULT_BLOCK_SIZE = 100

MEASURED_WORKER_ID = 'rerun-gate-worker'
MEASURED_ERROR_CODE = 'RERUN_GATE_FAILURE'
FINITE_RETENTION_CLASS = 'finite_30d_v1'


class WalBoundKind(StrEnum):
    """Which declared WAL bound a cell was judged against."""

    ENVELOPE_COPY = 'envelope_copy'
    BASELINE_SHARE = 'baseline_share'


@dataclass(frozen=True, slots=True)
class WalComparison:
    """WAL bytes per terminal task, judged against its declared allowance."""

    bound_kind: WalBoundKind
    baseline_bytes_per_task: float
    candidate_bytes_per_task: float
    delta_bytes_per_task: float
    prepared_envelope_bytes: int
    coefficient: float
    overhead_floor_bytes: int
    fraction: float
    floor_bytes: int
    limit_bytes: float
    verdict: Verdict

    @property
    def deliberately_tightened(self) -> bool:
        """Whether this comparison ran under something other than the budget."""
        match self.bound_kind:
            case WalBoundKind.ENVELOPE_COPY:
                return (
                    self.coefficient != ENVELOPE_WAL_COEFFICIENT
                    or self.overhead_floor_bytes
                    != ENVELOPE_WAL_OVERHEAD_FLOOR_BYTES
                )
            case WalBoundKind.BASELINE_SHARE:
                return (
                    self.fraction != WAL_FRACTION
                    or self.floor_bytes != WAL_FLOOR_BYTES
                )


@dataclass(frozen=True, slots=True)
class AbsoluteLockEnvelope:
    """The candidate's own p95 lock envelope against its absolute ceiling.

    Measured as statement start through commit return. That is an upper bound
    on the interval the transition actually holds its locks, since it also
    contains the client round trips; a candidate that passes here has not been
    flattered by the measurement.
    """

    candidate_p95_ms: float
    limit_ms: float
    verdict: Verdict


@dataclass(frozen=True, slots=True)
class ExactCountOutcome:
    """Statement and transaction counts, which admit no confidence interval."""

    side: PairedSide
    observations: int
    client_statements: int
    write_transactions: int
    violations: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.violations


@dataclass(frozen=True, slots=True)
class SideMeasurement:
    """One side's timings and what the server counted while they ran."""

    side: PairedSide
    samples_ms: tuple[float, ...]
    counts: Counts


def _payload_of(size: int, shape: PayloadShape, *, seed: int) -> bytes:
    """A JSON envelope of an exact size that does or does not compress."""
    if size < 8:
        raise PreparedEnvelopeError('envelope size must be at least 8 bytes')
    body_size = size - 8
    match shape:
        case PayloadShape.COMPRESSIBLE:
            body = 'x' * body_size
        case PayloadShape.INCOMPRESSIBLE:
            alphabet = (
                '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                'abcdefghijklmnopqrstuvwxyz'
            )
            generator = Random(seed)
            body = ''.join(generator.choices(alphabet, k=body_size))
    payload = f'{{"v":"{body}"}}'.encode()
    if len(payload) != size:
        raise PreparedEnvelopeError(
            'envelope generator did not preserve the requested size'
        )
    return payload


async def seed_side(
    connection: AsyncConnection,
    comparison: InstalledComparison,
    *,
    side: PairedSide,
    task_ids: list[str],
    envelope: bytes,
    attempts_per_task: int,
) -> None:
    """Seed one side's live rows, outside the measured statement and WAL window.

    Seeding runs before the counter probe opens, so its own statements and WAL
    are attributed to nobody. Both sides receive the same row shape and the same
    prepared envelope; only the transition under measurement differs.
    """
    validate_prepared_envelope(envelope)
    relations = (
        comparison.baseline
        if side is PairedSide.BASELINE
        else comparison.candidate
    )
    digest = sha256(envelope).digest()
    await connection.execute(
        text(
            f"""
            INSERT INTO {relations.live_tasks} (
                id, task_name, queue_name, priority, status,
                args, kwargs, enqueue_sha, is_workflow_task,
                claimed, claimed_by_worker_id, claimed_at, started_at,
                retention_class_key, input_digest,
                retain_rerun_input, prepared_rerun_input_disposition,
                prepared_rerun_input_version, prepared_rerun_input_codec,
                prepared_rerun_input_content_type,
                prepared_rerun_input_digest, prepared_rerun_input_inline
            )
            SELECT
                candidate_id, 'prototype.rerun', 'default', 100, 'RUNNING',
                '[]', '{{}}', repeat('a', 64), FALSE,
                TRUE, :worker, NOW(), NOW(),
                :retention_class, :digest,
                TRUE, 'INLINE', 1, 'json-utf8', 'application/json',
                :digest, :envelope
            FROM unnest(CAST(:task_ids AS varchar[])) AS candidate_id
            """
        ),
        {
            'worker': MEASURED_WORKER_ID,
            'retention_class': FINITE_RETENTION_CLASS,
            'digest': digest,
            'envelope': envelope,
            'task_ids': task_ids,
        },
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {relations.live_attempts} (
                task_id, attempt, outcome, will_retry,
                started_at, finished_at, worker_id
            )
            SELECT candidate_id, attempt_number, 'FAILED', FALSE,
                   NOW(), NOW(), :worker
            FROM unnest(CAST(:task_ids AS varchar[])) AS candidate_id
            CROSS JOIN generate_series(1, :attempts) AS attempt_number
            """
        ),
        {
            'worker': MEASURED_WORKER_ID,
            'task_ids': task_ids,
            'attempts': attempts_per_task,
        },
    )
    await connection.commit()


async def _run_block(
    connection: AsyncConnection,
    comparison: InstalledComparison,
    *,
    side: PairedSide,
    task_ids: list[str],
    result_json: str,
) -> list[float]:
    """One block of observations, each timed statement start through commit."""
    samples: list[float] = []
    match side:
        case PairedSide.BASELINE:
            statement = text(
                baseline_statement_text(
                    relation=comparison.baseline.live_tasks
                )
            )
            for task_id in task_ids:
                started = perf_counter()
                await connection.execute(
                    statement,
                    {
                        'id': task_id,
                        'wid': MEASURED_WORKER_ID,
                        'result_json': result_json,
                        'error_code': MEASURED_ERROR_CODE,
                    },
                )
                await connection.commit()
                samples.append((perf_counter() - started) * 1000.0)
        case PairedSide.CANDIDATE:
            statement = text(
                f'SELECT * FROM {comparison.schema.sql}'
                '.candidate_fail_locked_task('
                ':task_id, :worker, :result, :error_code, NULL)'
            )
            for task_id in task_ids:
                started = perf_counter()
                await connection.execute(
                    statement,
                    {
                        'task_id': task_id,
                        'worker': MEASURED_WORKER_ID,
                        'result': result_json,
                        'error_code': MEASURED_ERROR_CODE,
                    },
                )
                await connection.commit()
                samples.append((perf_counter() - started) * 1000.0)
    return samples


def _exact_counts(
    side: PairedSide,
    counts: Counts,
    *,
    observations: int,
) -> ExactCountOutcome:
    """Judge the counts the operation contract fixes exactly.

    Sampling noise does not create count tolerance: one client statement and
    one write transaction per operation is a contract, not a target. Write
    transactions are counted from a global identity sequence, so background
    work can only inflate them; a count below the observation count means the
    measured operation did not commit every time it claimed to.
    """
    violations: list[str] = []
    if counts.client_statements != observations:
        violations.append(
            f'{side.value} client statements {counts.client_statements} != '
            f'{observations}'
        )
    if counts.write_transactions < observations:
        violations.append(
            f'{side.value} write transactions {counts.write_transactions} < '
            f'{observations}'
        )
    return ExactCountOutcome(
        side=side,
        observations=observations,
        client_statements=counts.client_statements,
        write_transactions=counts.write_transactions,
        violations=tuple(violations),
    )


def compare_wal(
    *,
    baseline: Counts,
    candidate: Counts,
    prepared_envelope_bytes: int,
    coefficient: float = ENVELOPE_WAL_COEFFICIENT,
    overhead_floor_bytes: int = ENVELOPE_WAL_OVERHEAD_FLOOR_BYTES,
    fraction: float = WAL_FRACTION,
    floor_bytes: int = WAL_FLOOR_BYTES,
) -> WalComparison:
    """Judge WAL per terminal task against the bound its payload selects.

    A cell carrying a prepared envelope is judged on copy honesty: one copy of
    the prepared bytes plus a derived overhead. A cell carrying none keeps the
    original share-of-baseline bound, which is meaningful there because both
    sides write the same payload.

    Every coefficient, floor, fraction and limit travels in the result. A
    detection control tightens one deliberately, and a control whose tightened
    value is not recorded is an anecdote rather than something reproducible.
    """
    if prepared_envelope_bytes < 0:
        raise RerunTerminalizationError(
            'prepared envelope bytes cannot be negative'
        )
    baseline_per_task = baseline.wal_bytes_per_row
    candidate_per_task = candidate.wal_bytes_per_row
    delta = candidate_per_task - baseline_per_task
    if prepared_envelope_bytes > 0:
        bound_kind = WalBoundKind.ENVELOPE_COPY
        limit = prepared_envelope_bytes * coefficient + overhead_floor_bytes
    else:
        bound_kind = WalBoundKind.BASELINE_SHARE
        limit = max(baseline_per_task * fraction, float(floor_bytes))
    return WalComparison(
        bound_kind=bound_kind,
        baseline_bytes_per_task=baseline_per_task,
        candidate_bytes_per_task=candidate_per_task,
        delta_bytes_per_task=delta,
        prepared_envelope_bytes=prepared_envelope_bytes,
        coefficient=coefficient,
        overhead_floor_bytes=overhead_floor_bytes,
        fraction=fraction,
        floor_bytes=floor_bytes,
        limit_bytes=limit,
        verdict=Verdict.PASS if delta <= limit else Verdict.FAIL,
    )


def judge_absolute_lock_envelope(
    candidate_samples: list[float],
    *,
    limit_ms: float = ABSOLUTE_LOCK_P95_LIMIT_MS,
) -> AbsoluteLockEnvelope:
    """The candidate's own p95 envelope against its absolute ceiling."""
    observed = percentile_ms(candidate_samples, 95.0)
    return AbsoluteLockEnvelope(
        candidate_p95_ms=observed,
        limit_ms=limit_ms,
        verdict=Verdict.PASS if observed <= limit_ms else Verdict.FAIL,
    )


@dataclass(frozen=True, slots=True)
class CellResult:
    """One (payload shape, attempt depth) verdict and everything behind it."""

    payload_shape: PayloadShape
    attempts_per_task: int
    envelope_bytes: int
    observations_per_side: int
    block_size: int
    resamples: int
    seed: int
    baseline: SideMeasurement
    candidate: SideMeasurement
    comparisons: tuple[Comparison, ...]
    absolute_lock_envelope: AbsoluteLockEnvelope
    wal: WalComparison
    exact_counts: tuple[ExactCountOutcome, ...]
    structural: StructuralOutcome
    verdict: Verdict


def _total_counts(blocks: list[Counts]) -> Counts:
    """Sum one side's per-block counts into the side's total."""
    return Counts(
        client_statements=sum(b.client_statements for b in blocks),
        nested_statements=sum(b.nested_statements for b in blocks),
        client_rows=sum(b.client_rows for b in blocks),
        nested_rows=sum(b.nested_rows for b in blocks),
        terminal_rows=sum(b.terminal_rows for b in blocks),
        wal_records=sum(b.wal_records for b in blocks),
        wal_bytes=sum(b.wal_bytes for b in blocks),
        wal_fpi=sum(b.wal_fpi for b in blocks),
        write_transactions=sum(b.write_transactions for b in blocks),
    )


async def _measure_interleaved(
    connection: AsyncConnection,
    comparison: InstalledComparison,
    *,
    envelope: bytes,
    attempts_per_task: int,
    observations: int,
    block_size: int,
    result_json: str,
) -> tuple[SideMeasurement, SideMeasurement, str]:
    """Alternate blocks between the two sides until both are complete.

    Interleaving rather than running one side and then the other: a machine
    that drifts — a checkpoint, a busier host, a cache that filled — would
    otherwise donate the drift entirely to whichever side ran second. One
    warm-up block per side is run and discarded before counting starts, so a
    cold cache is not attributed to the side that happened to go first.

    Returns both sides plus one candidate task id retained for the post-commit
    structural check.
    """
    blocks = -(-observations // block_size)
    ids = {
        side: [
            f'{side.value[:4]}-{uuid4()}'[:36]
            for _ in range((blocks + 1) * block_size)
        ]
        for side in PairedSide
    }
    for side in PairedSide:
        await seed_side(
            connection,
            comparison,
            side=side,
            task_ids=ids[side],
            envelope=envelope,
            attempts_per_task=attempts_per_task,
        )

    cursors = {side: 0 for side in PairedSide}

    # Warm-up: one block per side, discarded.
    for side in PairedSide:
        warm = ids[side][cursors[side] : cursors[side] + block_size]
        cursors[side] += block_size
        await _run_block(
            connection,
            comparison,
            side=side,
            task_ids=warm,
            result_json=result_json,
        )

    samples: dict[PairedSide, list[float]] = {side: [] for side in PairedSide}
    blocks_counted: dict[PairedSide, list[Counts]] = {
        side: [] for side in PairedSide
    }
    # The statement view is server-wide and the probe resets it, so a block is
    # the unit of attribution: bracket each block, and the counts belong to the
    # side that just ran. Resetting once and reading at the end would hand each
    # side the sum of both, which reads as two sides that write identical WAL.
    probe = AsyncCounterProbe(connection)
    retained_candidate_id = ''

    for _ in range(blocks):
        for side in PairedSide:
            block_ids = ids[side][cursors[side] : cursors[side] + block_size]
            cursors[side] += block_size
            if side is PairedSide.CANDIDATE and not retained_candidate_id:
                retained_candidate_id = block_ids[0]
            await probe.begin()
            block_samples = await _run_block(
                connection,
                comparison,
                side=side,
                task_ids=block_ids,
                result_json=result_json,
            )
            samples[side] += block_samples
            blocks_counted[side].append(
                await probe.finish(terminal_rows=len(block_samples))
            )

    return (
        SideMeasurement(
            side=PairedSide.BASELINE,
            samples_ms=tuple(samples[PairedSide.BASELINE]),
            counts=_total_counts(blocks_counted[PairedSide.BASELINE]),
        ),
        SideMeasurement(
            side=PairedSide.CANDIDATE,
            samples_ms=tuple(samples[PairedSide.CANDIDATE]),
            counts=_total_counts(blocks_counted[PairedSide.CANDIDATE]),
        ),
        retained_candidate_id,
    )


async def measure_cell(
    connection: AsyncConnection,
    comparison: InstalledComparison,
    *,
    payload_shape: PayloadShape,
    attempts_per_task: int,
    observations: int,
    block_size: int = _DEFAULT_BLOCK_SIZE,
    resamples: int,
    seed: int,
    envelope_bytes: int = INLINE_BOUND_BYTES,
    result_bytes: int = 200,
    wal_coefficient: float = ENVELOPE_WAL_COEFFICIENT,
    wal_overhead_floor_bytes: int = ENVELOPE_WAL_OVERHEAD_FLOOR_BYTES,
) -> CellResult:
    """Measure one (payload shape, attempt depth) cell and judge every budget.

    A cell is an independent verdict. Compressible and incompressible envelopes
    are not averaged with one another and neither are attempt depths, because a
    budget met on one shape says nothing about the other.

    The exact requirements are judged alongside the sampled ones and outrank
    them: a cell that met every latency budget while writing the envelope twice,
    leaving the live row behind, or issuing a second client statement has failed
    regardless of what the intervals said.
    """
    if observations <= 0:
        raise RerunTerminalizationError('observations must be positive')
    if block_size <= 0:
        raise RerunTerminalizationError('block size must be positive')
    if resamples <= 0:
        raise RerunTerminalizationError('bootstrap resamples must be positive')
    if attempts_per_task <= 0:
        raise RerunTerminalizationError('attempts per task must be positive')

    envelope = _payload_of(envelope_bytes, payload_shape, seed=seed)
    result_json = _payload_of(
        result_bytes,
        PayloadShape.COMPRESSIBLE,
        seed=seed + 1,
    ).decode()

    baseline, candidate, retained_id = await _measure_interleaved(
        connection,
        comparison,
        envelope=envelope,
        attempts_per_task=attempts_per_task,
        observations=observations,
        block_size=block_size,
        result_json=result_json,
    )

    comparisons = tuple(
        compare(
            baseline=list(baseline.samples_ms),
            candidate=list(candidate.samples_ms),
            percentile=percentile,
            budget=budget,
            resamples=resamples,
            seed=seed,
        )
        for percentile, budget in (
            (50.0, P50_BUDGET),
            (95.0, LOCK_P95_BUDGET),
            (99.0, P99_BUDGET),
        )
    )
    absolute_lock = judge_absolute_lock_envelope(list(candidate.samples_ms))
    wal = compare_wal(
        baseline=baseline.counts,
        candidate=candidate.counts,
        prepared_envelope_bytes=envelope_bytes,
        coefficient=wal_coefficient,
        overhead_floor_bytes=wal_overhead_floor_bytes,
    )
    exact_counts = (
        _exact_counts(
            PairedSide.BASELINE,
            baseline.counts,
            observations=len(baseline.samples_ms),
        ),
        _exact_counts(
            PairedSide.CANDIDATE,
            candidate.counts,
            observations=len(candidate.samples_ms),
        ),
    )
    structural = await assert_candidate_structure(
        connection,
        comparison,
        task_id=retained_id,
        expected_envelope=envelope,
        expected_retention_class=FINITE_RETENTION_CLASS,
    )

    exact_failed = (
        not structural.passed
        or any(not outcome.passed for outcome in exact_counts)
    )
    sampled_verdict = worst(
        [
            *(comparison_result.verdict for comparison_result in comparisons),
            absolute_lock.verdict,
            wal.verdict,
        ]
    )

    return CellResult(
        payload_shape=payload_shape,
        attempts_per_task=attempts_per_task,
        envelope_bytes=envelope_bytes,
        observations_per_side=len(candidate.samples_ms),
        block_size=block_size,
        resamples=resamples,
        seed=seed,
        baseline=baseline,
        candidate=candidate,
        comparisons=comparisons,
        absolute_lock_envelope=absolute_lock,
        wal=wal,
        exact_counts=exact_counts,
        structural=structural,
        verdict=Verdict.FAIL if exact_failed else sampled_verdict,
    )


# --------------------------------------------------------------------------
# Capacity preflight.
#
# Incompressible envelopes at the inline bound do not compress, so a cell's
# footprint is predictable and large. Stating the declared peak before the run
# and refusing to start without room turns an out-of-space death partway
# through a multi-hour cell into a sentence the operator can act on.
# --------------------------------------------------------------------------

# Row overhead beyond the envelope: the live tuple and its replayed indexes,
# the attempt rows, and the history tuple. Deliberately generous; the point is
# a refusal that is never surprised, not a tight estimate.
_PER_ROW_OVERHEAD_BYTES = 4_096


class InsufficientDiskError(RerunTerminalizationError):
    """The declared peak footprint does not fit in the free space available."""


@dataclass(frozen=True, slots=True)
class CapacityDeclaration:
    """What one cell will occupy, stated before it runs."""

    rows_per_side: int
    envelope_bytes: int
    live_bytes: int
    history_bytes: int
    declared_peak_bytes: int
    free_bytes: int
    sufficient: bool


def declare_cell_capacity(
    *,
    observations: int,
    block_size: int,
    envelope_bytes: int,
    free_bytes: int,
) -> CapacityDeclaration:
    """State one cell's peak footprint and whether it fits.

    Both sides are seeded before measurement and the candidate's rows are then
    copied into history, so the peak holds two seeded sides plus one history
    copy at once.
    """
    blocks = -(-observations // block_size)
    rows_per_side = (blocks + 1) * block_size
    per_row = envelope_bytes + _PER_ROW_OVERHEAD_BYTES
    live_bytes = 2 * rows_per_side * per_row
    history_bytes = rows_per_side * per_row
    declared_peak = live_bytes + history_bytes
    return CapacityDeclaration(
        rows_per_side=rows_per_side,
        envelope_bytes=envelope_bytes,
        live_bytes=live_bytes,
        history_bytes=history_bytes,
        declared_peak_bytes=declared_peak,
        free_bytes=free_bytes,
        sufficient=free_bytes >= declared_peak,
    )


def preflight_capacity(
    *,
    observations: int,
    block_size: int,
    envelope_bytes: int,
    data_path: Path,
) -> CapacityDeclaration:
    """Refuse to start a cell the filesystem cannot hold.

    PostgreSQL exposes file sizes but not filesystem free space, so the check
    is made where the bytes actually land and the caller names that path. On a
    containerised bench the server writes through the host's storage driver, so
    the runner's own filesystem is the right one to measure; a bench on separate
    storage must be given that path instead.
    """
    free_bytes = disk_usage(data_path).free
    declaration = declare_cell_capacity(
        observations=observations,
        block_size=block_size,
        envelope_bytes=envelope_bytes,
        free_bytes=free_bytes,
    )
    if not declaration.sufficient:
        raise InsufficientDiskError(
            'declared peak footprint '
            f'{declaration.declared_peak_bytes} bytes for {observations} '
            f'observations per side at {envelope_bytes}-byte envelopes '
            f'exceeds {free_bytes} bytes free on {data_path}'
        )
    return declaration


GATE_OBSERVATIONS_PER_SIDE = 10_000
GATE_BLOCK_SIZE = 100
GATE_BOOTSTRAP_RESAMPLES = 1_000

ATTEMPT_DEPTHS: tuple[int, ...] = (1, 4)


@dataclass(frozen=True, slots=True)
class RerunTerminalizationEvidence:
    """Every cell's verdict and the conditions all of them ran under."""

    conditions: EvidenceConditions
    workload: dict[str, int | str]
    capacity: CapacityDeclaration
    cells: tuple[CellResult, ...]
    verdict: Verdict


def _validate_gate_authority(
    run_kind: EvidenceRunKind,
    *,
    observations: int,
    block_size: int,
    resamples: int,
) -> None:
    """A gate run declares its sampling before it starts, or it is not a gate."""
    if run_kind is not EvidenceRunKind.GATE:
        return
    if observations < GATE_OBSERVATIONS_PER_SIDE:
        raise RerunTerminalizationError(
            'gate evidence requires at least '
            f'{GATE_OBSERVATIONS_PER_SIDE} observations per side'
        )
    if block_size != GATE_BLOCK_SIZE:
        raise RerunTerminalizationError(
            f'gate evidence alternates in blocks of {GATE_BLOCK_SIZE}'
        )
    if resamples < GATE_BOOTSTRAP_RESAMPLES:
        raise RerunTerminalizationError(
            'gate evidence requires at least '
            f'{GATE_BOOTSTRAP_RESAMPLES} bootstrap resamples'
        )


async def collect_rerun_terminalization_evidence(
    engine: AsyncEngine,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    observations: int = GATE_OBSERVATIONS_PER_SIDE,
    block_size: int = GATE_BLOCK_SIZE,
    resamples: int = GATE_BOOTSTRAP_RESAMPLES,
    seed: int,
    envelope_bytes: int = INLINE_BOUND_BYTES,
    result_bytes: int = 200,
    data_path: Path,
    checkpoint_path: Path | None = None,
    progress: QualificationProgressReporter | None = None,
) -> RerunTerminalizationEvidence:
    """Measure every payload shape and attempt depth as an independent cell.

    Cells run strictly one at a time, each in its own disposable schema which is
    dropped before the next begins. Two cells sharing a bench would compete for
    cache and disk, and the second would be measuring the first.
    """
    _validate_gate_authority(
        run_kind,
        observations=observations,
        block_size=block_size,
        resamples=resamples,
    )
    capacity = preflight_capacity(
        observations=observations,
        block_size=block_size,
        envelope_bytes=envelope_bytes,
        data_path=data_path,
    )

    cells: list[CellResult] = []
    writer = AtomicEvidenceWriter(checkpoint_path)
    reporter = progress or QualificationProgressReporter()
    planned = [
        (shape, depth) for shape in PayloadShape for depth in ATTEMPT_DEPTHS
    ]

    async with engine.connect() as connection:
        conditions = await collect_conditions(
            connection,
            commit=commit,
            run_kind=run_kind,
            server_image=server_image,
            host_description=host_description,
            storage_description=storage_description,
            demo_quiesced=demo_quiesced,
            cache_posture=(
                'one warm-up block per side discarded before counting; '
                'each cell in a fresh disposable schema'
            ),
            prepared_posture=(
                'one client statement per observation, parameterized, '
                'reused across the block'
            ),
        )
        counters_prerequisite = await install_statement_counters(connection)

    workload: dict[str, int | str] = {
        'observations_per_side': observations,
        'block_size': block_size,
        'bootstrap_resamples': resamples,
        'seed': seed,
        'envelope_bytes': envelope_bytes,
        'result_bytes': result_bytes,
        'attempt_depths': ','.join(str(depth) for depth in ATTEMPT_DEPTHS),
        'cells_planned': len(planned),
        'retention_class': FINITE_RETENTION_CLASS,
        'terminalization_kind': TerminalizationKind.FAIL_RUNNING.value,
        'rerun_input_disposition': RerunInputDisposition.INLINE.value,
        'declared_peak_bytes': capacity.declared_peak_bytes,
        'shared_preload_libraries': (
            counters_prerequisite.shared_preload_libraries
        ),
        'pg_stat_statements_track': counters_prerequisite.track,
        'lock_envelope_definition': (
            'statement-start-through-commit-return; an upper bound on the '
            'interval the transition holds its locks'
        ),
    }

    for index, (shape, depth) in enumerate(planned, start=1):
        reporter.emit(
            QualificationProgress(
                scenario='rerun-input-terminalization',
                phase='cell',
                status='started',
                category=shape.value,
                cell_index=index,
                cell_total=len(planned),
                observation_target=observations,
            )
        )
        schema = PrototypeSchema(f'rerun_gate_{uuid4().hex[:10]}')
        async with engine.connect() as connection:
            await install_archive_candidates(connection, schema)
            await install_archive_transcode_prototype(connection, schema)
            comparison = await install_rerun_terminalization_prototype(
                connection,
                schema,
            )
            await connection.commit()
            try:
                cell = await measure_cell(
                    connection,
                    comparison,
                    payload_shape=shape,
                    attempts_per_task=depth,
                    observations=observations,
                    block_size=block_size,
                    resamples=resamples,
                    seed=seed + index,
                    envelope_bytes=envelope_bytes,
                    result_bytes=result_bytes,
                )
            finally:
                await connection.rollback()
                await remove_archive_candidates(connection, schema)
                await connection.commit()
        cells.append(cell)
        # Flushed before the next cell starts, so an interruption keeps every
        # finalized cell rather than losing the whole run.
        writer.write(
            RerunTerminalizationEvidence(
                conditions=conditions,
                workload=workload,
                capacity=capacity,
                cells=tuple(cells),
                verdict=worst([result.verdict for result in cells]),
            )
        )
        reporter.emit(
            QualificationProgress(
                scenario='rerun-input-terminalization',
                phase='cell',
                status=cell.verdict.value.lower(),
                category=shape.value,
                cell_index=index,
                cell_total=len(planned),
                observations=cell.observations_per_side,
            )
        )

    return RerunTerminalizationEvidence(
        conditions=conditions,
        workload=workload,
        capacity=capacity,
        cells=tuple(cells),
        verdict=worst([result.verdict for result in cells]),
    )
