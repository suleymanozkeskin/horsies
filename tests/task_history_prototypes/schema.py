"""Disposable PostgreSQL objects for archive-shape comparisons."""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.schemas.migrations import SCHEMA_VERSION


_IDENTIFIER = re.compile(r'^[a-z][a-z0-9_]{0,62}$')
# v27 adds the transitional live cutover columns: nullable, check-free,
# a catalog-only ALTER the prototype program neither reads nor writes
# (every prototype statement names its columns). The base moves; the
# qualified behavior does not. Any future base move re-litigates this
# guard the same way — explicitly, never by widening it to a range.
_EXPECTED_BASE_SCHEMA_VERSION = 27


@dataclass(frozen=True, slots=True)
class PrototypeSchema:
    name: str

    def __post_init__(self) -> None:
        if _IDENTIFIER.fullmatch(self.name) is None:
            raise ValueError('prototype schema must be a safe PostgreSQL identifier')

    @property
    def sql(self) -> str:
        return f'"{self.name}"'


async def install_archive_candidates(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    if SCHEMA_VERSION != _EXPECTED_BASE_SCHEMA_VERSION:
        raise RuntimeError(
            'task-history prototypes require the published schema v27 program'
        )
    stored_version = (
        await connection.execute(
            text('SELECT COALESCE(MAX(version), 0) FROM horsies_schema_version')
        )
    ).scalar_one()
    if stored_version != _EXPECTED_BASE_SCHEMA_VERSION:
        raise RuntimeError(
            'task-history prototypes require a disposable database at schema v26'
        )

    for statement in _archive_candidate_manifest(schema):
        await connection.execute(text(statement))


async def remove_archive_candidates(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    await connection.execute(text(f'DROP SCHEMA IF EXISTS {schema.sql} CASCADE'))
    remains = (
        await connection.execute(
            text('SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :name)'),
            {'name': schema.name},
        )
    ).scalar_one()
    if remains:
        raise RuntimeError('prototype schema cleanup left catalog objects behind')


def _archive_candidate_manifest(schema: PrototypeSchema) -> tuple[str, ...]:
    namespace = schema.sql
    terminalization_kinds = ', '.join(
        f"'{kind.value}'" for kind in TerminalizationKind
    )
    common_columns = _common_history_columns(namespace, terminalization_kinds)
    return (
        f'CREATE SCHEMA {namespace}',
        f"""
        CREATE TABLE {namespace}.retention_classes (
            class_key varchar(64) PRIMARY KEY,
            duration interval,
            partition_interval interval,
            CHECK (
                (duration IS NULL AND partition_interval IS NULL)
                OR (duration > interval '0' AND partition_interval > interval '0')
            )
        )
        """,
        f"""
        INSERT INTO {namespace}.retention_classes
            (class_key, duration, partition_interval)
        VALUES
            ('finite_30d_v1', interval '30 days', interval '1 day'),
            ('forever', NULL, NULL)
        """,
        f"""
        CREATE TABLE {namespace}.history_aggregate (
            {common_columns},
            attempt_archive_version smallint NOT NULL,
            attempt_snapshot_codec varchar(64) NOT NULL,
            attempt_snapshot_content_type varchar(255) NOT NULL,
            attempt_snapshot bytea NOT NULL,
            attempt_snapshot_digest bytea NOT NULL,
            CHECK (attempt_archive_version > 0),
            CHECK (octet_length(attempt_snapshot_codec) BETWEEN 1 AND 64),
            CHECK (
                octet_length(attempt_snapshot_content_type) BETWEEN 1 AND 255
            ),
            CHECK (octet_length(attempt_snapshot_digest) = 32)
        ) PARTITION BY LIST (retention_class_key)
        """,
        *_history_partitions(namespace, 'history_aggregate'),
        f"""
        CREATE TABLE {namespace}.history_copartitioned (
            {common_columns},
            attempt_archive_version smallint NOT NULL
        ) PARTITION BY LIST (retention_class_key)
        """,
        *_history_partitions(namespace, 'history_copartitioned'),
        f"""
        CREATE TABLE {namespace}.attempts_copartitioned (
            task_id varchar(36) NOT NULL,
            retention_class_key text NOT NULL,
            retention_anchor_at timestamptz NOT NULL,
            attempt_archive_version smallint NOT NULL,
            attempt integer NOT NULL CHECK (attempt > 0),
            outcome text NOT NULL CHECK (
                outcome IN ('COMPLETED', 'FAILED', 'WORKER_FAILURE')
            ),
            will_retry boolean NOT NULL,
            started_at timestamptz NOT NULL,
            finished_at timestamptz NOT NULL,
            error_code text,
            error_message text,
            failed_reason text,
            worker_id text,
            worker_hostname text,
            worker_pid integer,
            worker_process_name text,
            UNIQUE (task_id, retention_class_key, retention_anchor_at, attempt)
        ) PARTITION BY LIST (retention_class_key)
        """,
        *_attempt_partitions(namespace),
    )


def _common_history_columns(
    namespace: str,
    terminalization_kinds: str,
) -> str:
    return f"""
        task_id varchar(36) NOT NULL,
        task_name varchar(255) NOT NULL,
        queue_name varchar(100) NOT NULL,
        priority integer NOT NULL CHECK (priority BETWEEN 1 AND 100),
        command_fingerprint_version smallint NOT NULL
            CHECK (command_fingerprint_version > 0),
        command_fingerprint bytea NOT NULL
            CHECK (octet_length(command_fingerprint) = 32),
        status text NOT NULL CHECK (
            status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
        ),
        terminalization_kind varchar(32) NOT NULL CHECK (
            terminalization_kind IN ({terminalization_kinds})
        ),
        terminal_at timestamptz NOT NULL,
        retention_anchor_at timestamptz NOT NULL,
        retention_class_key varchar(64) NOT NULL
            REFERENCES {namespace}.retention_classes(class_key),
        sent_at timestamptz,
        enqueued_at timestamptz NOT NULL,
        claimed_at timestamptz,
        started_at timestamptz,
        created_at timestamptz NOT NULL,
        good_until timestamptz,
        retry_count integer NOT NULL CHECK (retry_count >= 0),
        max_retries integer NOT NULL CHECK (max_retries >= 0),
        last_claimed_worker_id varchar(255),
        last_worker_hostname varchar(255),
        last_worker_pid integer,
        last_worker_process_name varchar(255),
        result_envelope_version smallint NOT NULL
            CHECK (result_envelope_version > 0),
        result_codec varchar(64) NOT NULL,
        result_content_type varchar(255) NOT NULL,
        result_payload bytea,
        result_digest bytea,
        error_code text,
        final_failed_reason text,
        prior_result_payload bytea,
        rerun_of_task_id varchar(36),
        rerun_root_task_id varchar(36),
        input_digest bytea,
        rerun_input_version smallint,
        rerun_input_codec varchar(64),
        rerun_input_content_type varchar(255),
        rerun_input_form varchar(16),
        rerun_input_digest bytea,
        rerun_input_inline bytea,
        rerun_input_reference varchar(2048),
        workflow_id varchar(36),
        is_workflow_task boolean NOT NULL,
        history_schema_version smallint NOT NULL
            CHECK (history_schema_version > 0),
        CHECK (retention_anchor_at = terminal_at),
        CHECK (octet_length(result_codec) BETWEEN 1 AND 64),
        CHECK (octet_length(result_content_type) BETWEEN 1 AND 255),
        CHECK (result_digest IS NULL OR octet_length(result_digest) = 32),
        CHECK (input_digest IS NULL OR octet_length(input_digest) = 32),
        CHECK (rerun_input_digest IS NULL OR octet_length(rerun_input_digest) = 32),
        CHECK (
            rerun_input_reference IS NULL
            OR octet_length(rerun_input_reference) BETWEEN 1 AND 2048
        ),
        CHECK (
            (rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL)
            OR (rerun_of_task_id IS NOT NULL AND rerun_root_task_id IS NOT NULL)
        ),
        CHECK (
            (rerun_input_form IS NULL
                AND rerun_input_version IS NULL
                AND rerun_input_codec IS NULL
                AND rerun_input_content_type IS NULL
                AND rerun_input_digest IS NULL
                AND rerun_input_inline IS NULL
                AND rerun_input_reference IS NULL)
            OR (rerun_input_form = 'INLINE'
                AND rerun_input_version IS NOT NULL
                AND rerun_input_codec IS NOT NULL
                AND rerun_input_content_type IS NOT NULL
                AND rerun_input_digest IS NOT NULL
                AND rerun_input_inline IS NOT NULL
                AND rerun_input_reference IS NULL)
            OR (rerun_input_form = 'REFERENCE'
                AND rerun_input_version IS NOT NULL
                AND rerun_input_codec IS NOT NULL
                AND rerun_input_content_type IS NOT NULL
                AND rerun_input_digest IS NOT NULL
                AND rerun_input_inline IS NULL
                AND rerun_input_reference IS NOT NULL)
        ),
        CHECK (rerun_input_version IS NULL OR rerun_input_version > 0),
        CHECK (
            rerun_input_codec IS NULL
            OR octet_length(rerun_input_codec) BETWEEN 1 AND 64
        ),
        CHECK (
            rerun_input_content_type IS NULL
            OR octet_length(rerun_input_content_type) BETWEEN 1 AND 255
        ),
        CHECK (status <> 'COMPLETED' OR rerun_input_form IS NULL),
        CHECK (NOT is_workflow_task OR rerun_input_form IS NULL),
        CHECK (
            terminalization_kind <> 'CANCEL_ADMIN'
            OR result_payload IS NULL
        ),
        CHECK (
            prior_result_payload IS NULL
            OR terminalization_kind = 'CANCEL_ADMIN'
        ),
        CHECK (
            result_payload IS NULL OR prior_result_payload IS NULL
        ),
        CHECK (
            prior_result_payload IS NULL OR result_digest IS NOT NULL
        )
    """


def _history_partitions(namespace: str, parent: str) -> tuple[str, ...]:
    finite = f'{parent}_finite'
    return (
        f"""
        CREATE TABLE {namespace}.{finite}
            PARTITION OF {namespace}.{parent}
            FOR VALUES IN ('finite_30d_v1')
            PARTITION BY RANGE (retention_anchor_at)
        """,
        f"""
        CREATE TABLE {namespace}.{finite}_2026_08_05
            PARTITION OF {namespace}.{finite}
            FOR VALUES FROM ('2026-08-05T00:00:00Z')
            TO ('2026-08-06T00:00:00Z')
        """,
        f"""
        CREATE TABLE {namespace}.{parent}_forever
            PARTITION OF {namespace}.{parent}
            FOR VALUES IN ('forever')
        """,
        f'CREATE INDEX {parent}_finite_2026_08_05_id_idx ON '
        f'{namespace}.{finite}_2026_08_05 (task_id)',
        f'CREATE INDEX {parent}_forever_id_idx ON '
        f'{namespace}.{parent}_forever (task_id)',
    )


def _attempt_partitions(namespace: str) -> tuple[str, ...]:
    return (
        f"""
        CREATE TABLE {namespace}.attempts_copartitioned_finite
            PARTITION OF {namespace}.attempts_copartitioned
            FOR VALUES IN ('finite_30d_v1')
            PARTITION BY RANGE (retention_anchor_at)
        """,
        f"""
        CREATE TABLE {namespace}.attempts_copartitioned_finite_2026_08_05
            PARTITION OF {namespace}.attempts_copartitioned_finite
            FOR VALUES FROM ('2026-08-05T00:00:00Z')
            TO ('2026-08-06T00:00:00Z')
        """,
        f"""
        CREATE TABLE {namespace}.attempts_copartitioned_forever
            PARTITION OF {namespace}.attempts_copartitioned
            FOR VALUES IN ('forever')
        """,
        f"""
        CREATE INDEX attempts_copartitioned_finite_2026_08_05_task_idx
            ON {namespace}.attempts_copartitioned_finite_2026_08_05 (task_id, attempt)
        """,
        f"""
        CREATE INDEX attempts_copartitioned_forever_task_idx
            ON {namespace}.attempts_copartitioned_forever (task_id, attempt)
        """,
    )
