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
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.schemas.terminalization import OUTCOME_COLUMNS
from tests.task_history_prototypes.schema import PrototypeSchema


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
    """
    run_date = (
        await connection.execute(
            text("SELECT (date_trunc('day', now()))::date AS run_date")
        )
    ).scalar_one()
    suffix = run_date.strftime('%Y_%m_%d')
    leaf_name = f'history_aggregate_finite_{suffix}'
    if await _covering_finite_leaf(connection, schema, leaf_name=leaf_name):
        return leaf_name
    await connection.execute(
        text(
            f"""
            CREATE TABLE {schema.sql}."{leaf_name}"
                PARTITION OF {schema.sql}.history_aggregate_finite
                FOR VALUES FROM ('{run_date.isoformat()}')
                TO ('{(run_date + timedelta(days=1)).isoformat()}')
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
