"""Disposable database programs for three task-identity candidates."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.identity import (
    CANDIDATE_IDEMPOTENCY_WINDOW_MAX,
)
from tests.task_history_prototypes.schema import PrototypeSchema


_BASE_LOOKUP_LEAVES = 2
_MAX_LOOKUP_LEAVES = 512
_IDENTIFIER = re.compile(r'^[a-z][a-z0-9_]{0,62}$')
_STAGED_LOOKUP_PREFIXES = frozenset({'no_directory', 'key_registry'})
_CANDIDATE_WINDOW_MAX_SECONDS = int(
    CANDIDATE_IDEMPOTENCY_WINDOW_MAX.total_seconds()
)


@dataclass(frozen=True, slots=True)
class FiniteLookupLeaf:
    """One generated finite-history probe and its terminal-time bounds."""

    relation_name: str
    lower_bound: datetime
    upper_bound: datetime

    def __post_init__(self) -> None:
        if _IDENTIFIER.fullmatch(self.relation_name) is None:
            raise ValueError(
                f'invalid staged lookup leaf name: {self.relation_name!r}'
            )
        if self.lower_bound.tzinfo is None or self.upper_bound.tzinfo is None:
            raise ValueError('finite lookup leaf bounds must be timezone-aware')
        if self.lower_bound >= self.upper_bound:
            raise ValueError(
                'finite lookup leaf lower bound must precede its upper bound'
            )


async def install_identity_candidates(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    for statement in _identity_candidate_manifest(schema):
        await connection.execute(text(statement))


async def extend_identity_history_leaves(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    target_leaf_count: int,
) -> None:
    if not _BASE_LOOKUP_LEAVES <= target_leaf_count <= _MAX_LOOKUP_LEAVES:
        raise ValueError(
            f'target leaf count must be between {_BASE_LOOKUP_LEAVES} '
            f'and {_MAX_LOOKUP_LEAVES}'
        )
    for year in range(2028, 2026 + target_leaf_count):
        for prefix in ('no_directory', 'key_registry', 'combined'):
            await connection.execute(
                text(
                    f"""
                    CREATE TABLE {schema.sql}.{prefix}_history_finite_{year}
                        PARTITION OF {schema.sql}.{prefix}_history_finite
                        FOR VALUES FROM ('{year}-01-01T00:00:00Z')
                        TO ('{year + 1}-01-01T00:00:00Z')
                    """
                )
            )
            await connection.execute(
                text(
                    f"""
                    CREATE INDEX {prefix}_history_finite_{year}_task_idx
                        ON {schema.sql}.{prefix}_history_finite_{year} (task_id)
                    """
                )
            )
            if prefix == 'no_directory':
                await connection.execute(
                    text(
                        f"""
                        CREATE INDEX no_directory_history_finite_{year}_key_idx
                            ON {schema.sql}.no_directory_history_finite_{year}
                                (idempotency_key_digest, idempotency_expires_at)
                            WHERE idempotency_key_digest IS NOT NULL
                        """
                    )
                )


async def install_staged_lookup_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    prefix: Literal['no_directory', 'key_registry'],
    newest_first_leaf_names: Sequence[str],
) -> None:
    """Install a one-call lookup with static, catalog-generated leaf probes."""
    await connection.execute(
        text(
            render_staged_lookup_prototype(
                schema.sql,
                prefix,
                newest_first_leaf_names,
            )
        )
    )


async def install_uuid7_staged_lookup_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    prefix: Literal['no_directory', 'key_registry'],
    oldest_first_leaves: Sequence[FiniteLookupLeaf],
    maximum_request_lifetime: timedelta | None,
) -> None:
    """Install a staged lookup with UUIDv7 time pruning and v4 fallback."""
    await connection.execute(
        text(
            render_uuid7_staged_lookup_prototype(
                schema.sql,
                prefix,
                oldest_first_leaves,
                maximum_request_lifetime=maximum_request_lifetime,
            )
        )
    )


def _identity_candidate_manifest(schema: PrototypeSchema) -> tuple[str, ...]:
    namespace = schema.sql
    statements: list[str] = [
        f"""
        CREATE TYPE {namespace}.enqueue_outcome_kind AS ENUM (
            'APPLIED', 'REPLAY', 'CONFLICT'
        )
        """,
        f"""
        CREATE TYPE {namespace}.enqueue_outcome AS (
            outcome {namespace}.enqueue_outcome_kind,
            task_id varchar(36),
            observed_fingerprint_version smallint
        )
        """,
        f"""
        CREATE TYPE {namespace}.task_lookup AS (
            found boolean,
            location text,
            task_id varchar(36),
            fingerprint_version smallint,
            command_fingerprint bytea
        )
        """,
    ]
    for prefix in ('no_directory', 'key_registry', 'combined'):
        statements.extend(_authoritative_task_tables(namespace, prefix))
    statements.extend(
        [
            f"""
            CREATE UNIQUE INDEX no_directory_live_key_idx
                ON {namespace}.no_directory_live (idempotency_key_digest)
                WHERE idempotency_key_digest IS NOT NULL
            """,
            f"""
            CREATE INDEX no_directory_history_finite_2026_key_idx
                ON {namespace}.no_directory_history_finite_2026
                    (idempotency_key_digest, idempotency_expires_at)
                WHERE idempotency_key_digest IS NOT NULL
            """,
            f"""
            CREATE INDEX no_directory_history_finite_2027_key_idx
                ON {namespace}.no_directory_history_finite_2027
                    (idempotency_key_digest, idempotency_expires_at)
                WHERE idempotency_key_digest IS NOT NULL
            """,
            f"""
            CREATE INDEX no_directory_history_forever_key_idx
                ON {namespace}.no_directory_history_forever
                    (idempotency_key_digest, idempotency_expires_at)
                WHERE idempotency_key_digest IS NOT NULL
            """,
            f"""
            CREATE TABLE {namespace}.key_reservations (
                idempotency_key_digest bytea PRIMARY KEY,
                key_scope_version smallint NOT NULL,
                fingerprint_version smallint NOT NULL,
                command_fingerprint bytea NOT NULL,
                task_id varchar(36) NOT NULL,
                disposition text NOT NULL CHECK (
                    disposition IN ('LIVE', 'TERMINAL')
                ),
                reservation_window interval NOT NULL CHECK (
                    reservation_window > interval '0'
                ),
                expires_at timestamptz NOT NULL,
                CHECK (octet_length(idempotency_key_digest) = 32),
                CHECK (octet_length(command_fingerprint) = 32)
            )
            """,
            f"""
            CREATE INDEX key_reservations_expiry_idx
                ON {namespace}.key_reservations (expires_at)
                WHERE disposition = 'TERMINAL'
            """,
            f"""
            CREATE TABLE {namespace}.combined_registry (
                task_id varchar(36) PRIMARY KEY,
                idempotency_key_digest bytea,
                key_scope_version smallint,
                fingerprint_version smallint NOT NULL,
                command_fingerprint bytea NOT NULL,
                location text NOT NULL CHECK (location IN ('LIVE', 'HISTORY')),
                retention_class_key text NOT NULL,
                retention_anchor_at timestamptz,
                key_window interval,
                key_expires_at timestamptz,
                CHECK (
                    (idempotency_key_digest IS NULL
                        AND key_scope_version IS NULL
                        AND key_window IS NULL
                        AND key_expires_at IS NULL)
                    OR (octet_length(idempotency_key_digest) = 32
                        AND key_scope_version IS NOT NULL
                        AND key_window > interval '0'
                        AND key_expires_at IS NOT NULL)
                ),
                CHECK (octet_length(command_fingerprint) = 32),
                CHECK (
                    (location = 'LIVE' AND retention_anchor_at IS NULL)
                    OR (location = 'HISTORY' AND retention_anchor_at IS NOT NULL)
                )
            )
            """,
            f"""
            CREATE UNIQUE INDEX combined_registry_key_idx
                ON {namespace}.combined_registry (idempotency_key_digest)
                WHERE idempotency_key_digest IS NOT NULL
            """,
            _no_directory_enqueue(namespace),
            _key_registry_enqueue(namespace),
            _combined_enqueue(namespace),
            _terminalize_function(namespace, 'no_directory', registry='none'),
            _terminalize_function(namespace, 'key_registry', registry='key'),
            _terminalize_function(namespace, 'combined', registry='combined'),
            _cleanup_key_reservations_function(namespace),
            _cleanup_combined_reservations_function(namespace),
            _fanout_lookup_function(namespace, 'no_directory'),
            _fanout_lookup_function(namespace, 'key_registry'),
            _combined_lookup_function(namespace),
        ]
    )
    return tuple(statements)


def _authoritative_task_tables(namespace: str, prefix: str) -> tuple[str, ...]:
    key_columns = (
        """
        idempotency_key_digest bytea,
        key_scope_version smallint,
        idempotency_window interval,
        idempotency_expires_at timestamptz,
        """
        if prefix == 'no_directory'
        else ''
    )
    key_checks = (
        """
        , CHECK (
            (idempotency_key_digest IS NULL
                AND key_scope_version IS NULL
                AND idempotency_window IS NULL
                AND idempotency_expires_at IS NULL)
            OR (octet_length(idempotency_key_digest) = 32
                AND key_scope_version IS NOT NULL
                AND idempotency_window > interval '0'
                AND idempotency_expires_at IS NOT NULL)
        )
        """
        if prefix == 'no_directory'
        else ''
    )
    return (
        f"""
        CREATE TABLE {namespace}.{prefix}_live (
            task_id varchar(36) PRIMARY KEY,
            task_name text NOT NULL,
            fingerprint_version smallint NOT NULL,
            command_fingerprint bytea NOT NULL,
            {key_columns}
            retention_class_key text NOT NULL,
            created_at timestamptz NOT NULL,
            CHECK (octet_length(command_fingerprint) = 32)
            {key_checks}
        )
        """,
        f"""
        CREATE TABLE {namespace}.{prefix}_history (
            task_id varchar(36) NOT NULL,
            task_name text NOT NULL,
            fingerprint_version smallint NOT NULL,
            command_fingerprint bytea NOT NULL,
            {key_columns}
            retention_class_key text NOT NULL,
            terminal_at timestamptz NOT NULL,
            CHECK (octet_length(command_fingerprint) = 32)
            {key_checks}
        ) PARTITION BY LIST (retention_class_key)
        """,
        f"""
        CREATE TABLE {namespace}.{prefix}_history_finite
            PARTITION OF {namespace}.{prefix}_history
            FOR VALUES IN ('finite_30d_v1')
            PARTITION BY RANGE (terminal_at)
        """,
        f"""
        CREATE TABLE {namespace}.{prefix}_history_finite_2026
            PARTITION OF {namespace}.{prefix}_history_finite
            FOR VALUES FROM ('2026-01-01T00:00:00Z')
            TO ('2027-01-01T00:00:00Z')
        """,
        f"""
        CREATE TABLE {namespace}.{prefix}_history_finite_2027
            PARTITION OF {namespace}.{prefix}_history_finite
            FOR VALUES FROM ('2027-01-01T00:00:00Z')
            TO ('2028-01-01T00:00:00Z')
        """,
        f"""
        CREATE TABLE {namespace}.{prefix}_history_forever
            PARTITION OF {namespace}.{prefix}_history
            FOR VALUES IN ('forever')
        """,
        f"""
        CREATE INDEX {prefix}_history_finite_2026_task_idx
            ON {namespace}.{prefix}_history_finite_2026 (task_id)
        """,
        f"""
        CREATE INDEX {prefix}_history_finite_2027_task_idx
            ON {namespace}.{prefix}_history_finite_2027 (task_id)
        """,
        f"""
        CREATE INDEX {prefix}_history_forever_task_idx
            ON {namespace}.{prefix}_history_forever (task_id)
        """,
    )


def _lock_sql() -> str:
    return """
        FOR v_lock_key IN
            SELECT DISTINCT lock_key
            FROM unnest(ARRAY[
                hashtextextended(p_task_id, 731),
                CASE WHEN p_key_digest IS NULL THEN NULL
                     ELSE hashtextextended(encode(p_key_digest, 'hex'), 947)
                END
            ]) AS requested(lock_key)
            WHERE lock_key IS NOT NULL
            ORDER BY lock_key
        LOOP
            PERFORM pg_advisory_xact_lock(v_lock_key);
        END LOOP;
    """


def _function_header(namespace: str, name: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.{name}(
        p_task_id varchar(36),
        p_task_name text,
        p_key_digest bytea,
        p_key_scope_version smallint,
        p_key_window interval,
        p_fingerprint_version smallint,
        p_fingerprint bytea,
        p_retention_class_key text
    ) RETURNS {namespace}.enqueue_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_lock_key bigint;
        v_task_id varchar(36);
        v_fingerprint_version smallint;
        v_fingerprint bytea;
        v_key_digest bytea;
    BEGIN
        IF octet_length(p_fingerprint) <> 32 THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'fingerprint must be 32 bytes';
        END IF;
        IF (p_key_digest IS NULL) <> (p_key_scope_version IS NULL)
           OR (p_key_digest IS NULL) <> (p_key_window IS NULL) THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'key digest, scope version, and window must be present together';
        END IF;
        IF p_key_digest IS NOT NULL AND octet_length(p_key_digest) <> 32 THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'key digest must be 32 bytes';
        END IF;
        IF p_key_window IS NOT NULL AND p_key_window <= interval '0' THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'idempotency window must be positive';
        END IF;
        IF p_key_window IS NOT NULL
           AND p_key_window > make_interval(secs => {_CANDIDATE_WINDOW_MAX_SECONDS})
        THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'idempotency window must not exceed 30 days';
        END IF;
        {_lock_sql()}
    """


def _classify_sql(namespace: str) -> str:
    return f"""
        IF v_fingerprint = p_fingerprint
           AND v_fingerprint_version = p_fingerprint_version
           AND v_key_digest IS NOT DISTINCT FROM p_key_digest THEN
            RETURN ROW('REPLAY', v_task_id, v_fingerprint_version)
                ::{namespace}.enqueue_outcome;
        END IF;
        RETURN ROW('CONFLICT', v_task_id, v_fingerprint_version)
            ::{namespace}.enqueue_outcome;
    """


def _no_directory_enqueue(namespace: str) -> str:
    return (
        _function_header(namespace, 'enqueue_no_directory')
        + f"""
        SELECT found.task_id, found.fingerprint_version,
               found.command_fingerprint, found.idempotency_key_digest
        INTO v_task_id, v_fingerprint_version, v_fingerprint, v_key_digest
        FROM (
            SELECT task_id, fingerprint_version, command_fingerprint,
                   idempotency_key_digest, 0 AS location_order
            FROM {namespace}.no_directory_live
            WHERE task_id = p_task_id
            UNION ALL
            SELECT task_id, fingerprint_version, command_fingerprint,
                   idempotency_key_digest, 1 AS location_order
            FROM {namespace}.no_directory_history
            WHERE task_id = p_task_id
        ) AS found
        ORDER BY found.location_order
        LIMIT 1;
        IF FOUND THEN
            {_classify_sql(namespace)}
        END IF;

        IF p_key_digest IS NOT NULL THEN
            SELECT found.task_id, found.fingerprint_version,
                   found.command_fingerprint, found.idempotency_key_digest
            INTO v_task_id, v_fingerprint_version, v_fingerprint, v_key_digest
            FROM (
                SELECT task_id, fingerprint_version, command_fingerprint,
                       idempotency_key_digest, 0 AS location_order
                FROM {namespace}.no_directory_live
                WHERE idempotency_key_digest = p_key_digest
                UNION ALL
                SELECT task_id, fingerprint_version, command_fingerprint,
                       idempotency_key_digest, 1 AS location_order
                FROM {namespace}.no_directory_history
                WHERE idempotency_key_digest = p_key_digest
                  AND idempotency_expires_at > statement_timestamp()
            ) AS found
            ORDER BY found.location_order
            LIMIT 1;
            IF FOUND THEN
                {_classify_sql(namespace)}
            END IF;
        END IF;

        INSERT INTO {namespace}.no_directory_live (
            task_id, task_name, fingerprint_version, command_fingerprint,
            idempotency_key_digest, key_scope_version,
            idempotency_window, idempotency_expires_at,
            retention_class_key, created_at
        ) VALUES (
            p_task_id, p_task_name, p_fingerprint_version, p_fingerprint,
            p_key_digest, p_key_scope_version,
            p_key_window,
            CASE WHEN p_key_digest IS NULL THEN NULL
                 ELSE 'infinity'::timestamptz END,
            p_retention_class_key, statement_timestamp()
        );
        RETURN ROW('APPLIED', p_task_id, NULL)::{namespace}.enqueue_outcome;
    END
    $function$
    """
    )


def _key_registry_enqueue(namespace: str) -> str:
    return (
        _function_header(namespace, 'enqueue_key_registry')
        + f"""
        SELECT found.task_id, found.fingerprint_version,
               found.command_fingerprint, found.idempotency_key_digest
        INTO v_task_id, v_fingerprint_version, v_fingerprint, v_key_digest
        FROM (
            SELECT task.task_id, task.fingerprint_version,
                   task.command_fingerprint,
                   reservation.idempotency_key_digest, 0 AS location_order
            FROM {namespace}.key_registry_live AS task
            LEFT JOIN {namespace}.key_reservations AS reservation
              ON reservation.task_id = task.task_id
            WHERE task.task_id = p_task_id
            UNION ALL
            SELECT task.task_id, task.fingerprint_version,
                   task.command_fingerprint,
                   reservation.idempotency_key_digest, 1 AS location_order
            FROM {namespace}.key_registry_history AS task
            LEFT JOIN {namespace}.key_reservations AS reservation
              ON reservation.task_id = task.task_id
            WHERE task.task_id = p_task_id
        ) AS found
        ORDER BY found.location_order
        LIMIT 1;
        IF FOUND THEN
            {_classify_sql(namespace)}
        END IF;

        IF p_key_digest IS NOT NULL THEN
            SELECT task_id, fingerprint_version, command_fingerprint,
                   idempotency_key_digest
            INTO v_task_id, v_fingerprint_version, v_fingerprint, v_key_digest
            FROM {namespace}.key_reservations
            WHERE idempotency_key_digest = p_key_digest
              AND (disposition = 'LIVE' OR expires_at > statement_timestamp());
            IF FOUND THEN
                {_classify_sql(namespace)}
            END IF;
            DELETE FROM {namespace}.key_reservations
            WHERE idempotency_key_digest = p_key_digest
              AND disposition = 'TERMINAL'
              AND expires_at <= statement_timestamp();
        END IF;

        INSERT INTO {namespace}.key_registry_live (
            task_id, task_name, fingerprint_version, command_fingerprint,
            retention_class_key, created_at
        ) VALUES (
            p_task_id, p_task_name, p_fingerprint_version, p_fingerprint,
            p_retention_class_key, statement_timestamp()
        );
        IF p_key_digest IS NOT NULL THEN
            INSERT INTO {namespace}.key_reservations (
                idempotency_key_digest, key_scope_version,
                fingerprint_version, command_fingerprint, task_id,
                disposition, reservation_window, expires_at
            ) VALUES (
                p_key_digest, p_key_scope_version, p_fingerprint_version,
                p_fingerprint, p_task_id, 'LIVE', p_key_window,
                'infinity'::timestamptz
            );
        END IF;
        RETURN ROW('APPLIED', p_task_id, NULL)::{namespace}.enqueue_outcome;
    END
    $function$
    """
    )


def _combined_enqueue(namespace: str) -> str:
    return (
        _function_header(namespace, 'enqueue_combined_registry')
        + f"""
        SELECT task_id, fingerprint_version, command_fingerprint,
               idempotency_key_digest
        INTO v_task_id, v_fingerprint_version, v_fingerprint, v_key_digest
        FROM {namespace}.combined_registry
        WHERE task_id = p_task_id;
        IF FOUND THEN
            {_classify_sql(namespace)}
        END IF;

        IF p_key_digest IS NOT NULL THEN
            SELECT task_id, fingerprint_version, command_fingerprint,
                   idempotency_key_digest
            INTO v_task_id, v_fingerprint_version, v_fingerprint, v_key_digest
            FROM {namespace}.combined_registry
            WHERE idempotency_key_digest = p_key_digest
              AND (location = 'LIVE' OR key_expires_at > statement_timestamp());
            IF FOUND THEN
                {_classify_sql(namespace)}
            END IF;
            UPDATE {namespace}.combined_registry
            SET idempotency_key_digest = NULL,
                key_scope_version = NULL,
                key_window = NULL,
                key_expires_at = NULL
            WHERE idempotency_key_digest = p_key_digest
              AND location = 'HISTORY'
              AND key_expires_at <= statement_timestamp();
        END IF;

        INSERT INTO {namespace}.combined_live (
            task_id, task_name, fingerprint_version, command_fingerprint,
            retention_class_key, created_at
        ) VALUES (
            p_task_id, p_task_name, p_fingerprint_version, p_fingerprint,
            p_retention_class_key, statement_timestamp()
        );
        INSERT INTO {namespace}.combined_registry (
            task_id, idempotency_key_digest, key_scope_version,
            fingerprint_version, command_fingerprint, location,
            retention_class_key, retention_anchor_at, key_window,
            key_expires_at
        ) VALUES (
            p_task_id, p_key_digest, p_key_scope_version,
            p_fingerprint_version, p_fingerprint, 'LIVE',
            p_retention_class_key, NULL, p_key_window,
            CASE WHEN p_key_digest IS NULL THEN NULL
                 ELSE 'infinity'::timestamptz END
        );
        RETURN ROW('APPLIED', p_task_id, NULL)::{namespace}.enqueue_outcome;
    END
    $function$
    """
    )


def _terminalize_function(namespace: str, prefix: str, *, registry: str) -> str:
    no_directory_columns = (
        'idempotency_key_digest, key_scope_version, idempotency_window, '
        'idempotency_expires_at, '
        if registry == 'none'
        else ''
    )
    no_directory_values = (
        'moved.idempotency_key_digest, moved.key_scope_version, '
        'moved.idempotency_window, '
        'CASE WHEN moved.idempotency_key_digest IS NULL THEN NULL '
        'ELSE p_terminal_at + moved.idempotency_window END, '
        if registry == 'none'
        else ''
    )
    if registry == 'key':
        final_statement = f"""
        UPDATE {namespace}.key_reservations
        SET disposition = 'TERMINAL',
            expires_at = p_terminal_at + reservation_window
        WHERE task_id = p_task_id;
        """
    elif registry == 'combined':
        final_statement = f"""
        UPDATE {namespace}.combined_registry
        SET location = 'HISTORY',
            retention_anchor_at = p_terminal_at,
            key_expires_at = CASE
                WHEN idempotency_key_digest IS NULL THEN NULL
                ELSE p_terminal_at + key_window
            END
        WHERE task_id = p_task_id;
        """
    else:
        final_statement = ''
    return f"""
    CREATE FUNCTION {namespace}.terminalize_{prefix}(
        p_task_id varchar(36),
        p_terminal_at timestamptz
    ) RETURNS boolean
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_moved boolean;
    BEGIN
        IF p_terminal_at IS NULL THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'terminal instant must be non-null';
        END IF;
        WITH moved AS (
            DELETE FROM {namespace}.{prefix}_live
            WHERE task_id = p_task_id
            RETURNING *
        ), inserted AS (
            INSERT INTO {namespace}.{prefix}_history (
                task_id, task_name, fingerprint_version,
                command_fingerprint, {no_directory_columns}
                retention_class_key, terminal_at
            )
            SELECT moved.task_id, moved.task_name, moved.fingerprint_version,
                   moved.command_fingerprint, {no_directory_values}
                   moved.retention_class_key, p_terminal_at
            FROM moved
            RETURNING TRUE
        )
        SELECT COALESCE(bool_or(TRUE), FALSE) INTO v_moved FROM inserted;
        {final_statement}
        RETURN v_moved;
    END
    $function$
    """


def _cleanup_key_reservations_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.cleanup_key_reservations(p_batch_size integer)
    RETURNS TABLE(task_id varchar(36))
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'batch size must be a positive integer';
        END IF;
        RETURN QUERY
        WITH targets AS MATERIALIZED (
            SELECT reservation.idempotency_key_digest
            FROM {namespace}.key_reservations AS reservation
            WHERE reservation.disposition = 'TERMINAL'
              AND reservation.expires_at <= statement_timestamp()
            ORDER BY reservation.expires_at,
                     reservation.idempotency_key_digest
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM {namespace}.key_reservations AS reservation
        USING targets
        WHERE reservation.idempotency_key_digest =
              targets.idempotency_key_digest
        RETURNING reservation.task_id;
    END
    $function$
    """


def _cleanup_combined_reservations_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.cleanup_combined_reservations(
        p_batch_size integer
    ) RETURNS TABLE(task_id varchar(36))
    LANGUAGE plpgsql
    AS $function$
    BEGIN
        IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'batch size must be a positive integer';
        END IF;
        RETURN QUERY
        WITH targets AS MATERIALIZED (
            SELECT registry.task_id
            FROM {namespace}.combined_registry AS registry
            WHERE registry.location = 'HISTORY'
              AND registry.idempotency_key_digest IS NOT NULL
              AND registry.key_expires_at <= statement_timestamp()
            ORDER BY registry.key_expires_at, registry.task_id
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {namespace}.combined_registry AS registry
        SET idempotency_key_digest = NULL,
            key_scope_version = NULL,
            key_window = NULL,
            key_expires_at = NULL
        FROM targets
        WHERE registry.task_id = targets.task_id
        RETURNING registry.task_id;
    END
    $function$
    """


def _fanout_lookup_function(namespace: str, prefix: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.lookup_{prefix}(p_task_id varchar(36))
    RETURNS {namespace}.task_lookup
    LANGUAGE sql
    STABLE
    AS $function$
        SELECT COALESCE(
            (
                SELECT ROW(
                    TRUE,
                    located.location,
                    located.task_id,
                    located.fingerprint_version,
                    located.command_fingerprint
                )::{namespace}.task_lookup
                FROM (
                    SELECT 'LIVE'::text AS location, task_id,
                           fingerprint_version, command_fingerprint,
                           0 AS location_order
                    FROM {namespace}.{prefix}_live
                    WHERE task_id = p_task_id
                    UNION ALL
                    SELECT 'HISTORY'::text AS location, task_id,
                           fingerprint_version, command_fingerprint,
                           1 AS location_order
                    FROM {namespace}.{prefix}_history
                    WHERE task_id = p_task_id
                ) AS located
                ORDER BY located.location_order
                LIMIT 1
            ),
            ROW(FALSE, NULL, NULL, NULL, NULL)::{namespace}.task_lookup
        )
    $function$
    """


def render_staged_lookup_prototype(
    namespace: str,
    prefix: str,
    newest_first_leaf_names: Sequence[str],
) -> str:
    if prefix not in _STAGED_LOOKUP_PREFIXES:
        raise ValueError('staged lookup requires a non-directory candidate')
    if not newest_first_leaf_names:
        raise ValueError('staged lookup requires at least one finite leaf')
    leaf_names = tuple(newest_first_leaf_names)
    if len(set(leaf_names)) != len(leaf_names):
        raise ValueError('staged lookup finite leaves must be distinct')
    expected_leaf_prefix = f'{prefix}_history_finite_'
    for leaf_name in leaf_names:
        if (
            _IDENTIFIER.fullmatch(leaf_name) is None
            or not leaf_name.startswith(expected_leaf_prefix)
        ):
            raise ValueError(f'invalid staged lookup leaf name: {leaf_name!r}')

    finite_probes = '\n'.join(
        _staged_relation_probe(
            namespace,
            leaf_name,
            location='HISTORY',
        )
        for leaf_name in leaf_names
    )
    return f"""
    CREATE OR REPLACE FUNCTION {namespace}.lookup_{prefix}_staged(
        p_task_id varchar(36)
    )
    RETURNS {namespace}.task_lookup
    LANGUAGE plpgsql
    STABLE
    AS $function$
    DECLARE
        v_task_id varchar(36);
        v_fingerprint_version smallint;
        v_fingerprint bytea;
    BEGIN
        {_staged_relation_probe(namespace, f'{prefix}_live', location='LIVE')}
        {_staged_relation_probe(
            namespace,
            f'{prefix}_history_forever',
            location='HISTORY',
        )}
        {finite_probes}
        RETURN ROW(FALSE, NULL, NULL, NULL, NULL)
            ::{namespace}.task_lookup;
    END
    $function$
    """


def render_uuid7_staged_lookup_prototype(
    namespace: str,
    prefix: str,
    oldest_first_leaves: Sequence[FiniteLookupLeaf],
    *,
    maximum_request_lifetime: timedelta | None,
) -> str:
    """Render birth-time-pruned finite probes with a complete legacy walk."""
    leaves = _validate_uuid7_leaf_manifest(prefix, oldest_first_leaves)
    if (
        maximum_request_lifetime is not None
        and maximum_request_lifetime <= timedelta(0)
    ):
        raise ValueError('maximum request lifetime must be positive')

    latest_assignment = 'v_latest_terminal_at := NULL;'
    bounded_purge = ''
    latest_guard = ''
    if maximum_request_lifetime is not None:
        lifetime_seconds = maximum_request_lifetime.total_seconds()
        latest_assignment = (
            'v_latest_terminal_at := v_birth_at '
            f'+ make_interval(secs => {lifetime_seconds!r});'
        )
        oldest_lower = _timestamp_literal(leaves[0].lower_bound)
        bounded_purge = f"""
            IF v_latest_terminal_at < {oldest_lower} THEN
                RETURN ROW(FALSE, NULL, NULL, NULL, NULL)
                    ::{namespace}.task_lookup;
            END IF;
        """
        latest_guard = (
            ' AND v_latest_terminal_at >= '
            '{lower_bound}'
        )

    uuid7_probes = '\n'.join(
        _uuid7_guarded_probe(
            namespace,
            leaf,
            latest_guard=latest_guard,
        )
        for leaf in leaves
    )
    legacy_probes = '\n'.join(
        _staged_relation_probe(
            namespace,
            leaf.relation_name,
            location='HISTORY',
        )
        for leaf in reversed(leaves)
    )
    return f"""
    CREATE OR REPLACE FUNCTION {namespace}.lookup_{prefix}_uuid7_staged(
        p_task_id varchar(36)
    )
    RETURNS {namespace}.task_lookup
    LANGUAGE plpgsql
    STABLE
    AS $function$
    DECLARE
        v_task_uuid uuid;
        v_uuid_bytes bytea;
        v_birth_milliseconds bigint;
        v_birth_at timestamptz;
        v_latest_terminal_at timestamptz;
        v_task_id varchar(36);
        v_fingerprint_version smallint;
        v_fingerprint bytea;
    BEGIN
        {_staged_relation_probe(namespace, f'{prefix}_live', location='LIVE')}
        {_staged_relation_probe(
            namespace,
            f'{prefix}_history_forever',
            location='HISTORY',
        )}

        BEGIN
            v_task_uuid := p_task_id::uuid;
        EXCEPTION WHEN invalid_text_representation THEN
            RETURN ROW(FALSE, NULL, NULL, NULL, NULL)
                ::{namespace}.task_lookup;
        END;
        v_uuid_bytes := uuid_send(v_task_uuid);
        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7
           AND (get_byte(v_uuid_bytes, 8) & 192) = 128
        THEN
            v_birth_milliseconds :=
                (get_byte(v_uuid_bytes, 0)::bigint << 40)
                | (get_byte(v_uuid_bytes, 1)::bigint << 32)
                | (get_byte(v_uuid_bytes, 2)::bigint << 24)
                | (get_byte(v_uuid_bytes, 3)::bigint << 16)
                | (get_byte(v_uuid_bytes, 4)::bigint << 8)
                | get_byte(v_uuid_bytes, 5)::bigint;
            v_birth_at := to_timestamp(
                v_birth_milliseconds::double precision / 1000.0
            );
            {latest_assignment}
            {bounded_purge}
            {uuid7_probes}
        ELSE
            {legacy_probes}
        END IF;
        RETURN ROW(FALSE, NULL, NULL, NULL, NULL)
            ::{namespace}.task_lookup;
    END
    $function$
    """


def _validate_uuid7_leaf_manifest(
    prefix: str,
    oldest_first_leaves: Sequence[FiniteLookupLeaf],
) -> tuple[FiniteLookupLeaf, ...]:
    if prefix not in _STAGED_LOOKUP_PREFIXES:
        raise ValueError(
            'UUIDv7 staged lookup requires a non-directory candidate'
        )
    leaves = tuple(oldest_first_leaves)
    if not leaves:
        raise ValueError(
            'UUIDv7 staged lookup requires at least one finite leaf'
        )
    if len({leaf.relation_name for leaf in leaves}) != len(leaves):
        raise ValueError('UUIDv7 staged lookup finite leaves must be distinct')
    expected_prefix = f'{prefix}_history_finite_'
    for index, leaf in enumerate(leaves):
        if not leaf.relation_name.startswith(expected_prefix):
            raise ValueError(
                f'invalid UUIDv7 staged lookup leaf name: '
                f'{leaf.relation_name!r}'
            )
        if index > 0 and leaves[index - 1].upper_bound != leaf.lower_bound:
            raise ValueError(
                'UUIDv7 staged lookup leaf bounds must be contiguous and '
                'oldest-first'
            )
    return leaves


def _uuid7_guarded_probe(
    namespace: str,
    leaf: FiniteLookupLeaf,
    *,
    latest_guard: str,
) -> str:
    lower_bound = _timestamp_literal(leaf.lower_bound)
    upper_bound = _timestamp_literal(leaf.upper_bound)
    formatted_latest_guard = latest_guard.format(lower_bound=lower_bound)
    return f"""
        IF v_birth_at < {upper_bound}{formatted_latest_guard} THEN
            {_staged_relation_probe(
                namespace,
                leaf.relation_name,
                location='HISTORY',
            )}
        END IF;
    """


def _timestamp_literal(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    rendered = utc_value.isoformat().replace('+00:00', 'Z')
    return f"TIMESTAMPTZ '{rendered}'"


def _staged_relation_probe(
    namespace: str,
    relation: str,
    *,
    location: Literal['LIVE', 'HISTORY'],
) -> str:
    return f"""
        SELECT task_id, fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM {namespace}.{relation}
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, '{location}', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::{namespace}.task_lookup;
        END IF;
    """


def _combined_lookup_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.lookup_combined_registry(p_task_id varchar(36))
    RETURNS {namespace}.task_lookup
    LANGUAGE plpgsql
    STABLE
    AS $function$
    DECLARE
        v_location text;
        v_anchor timestamptz;
        v_retention_class_key text;
        v_fingerprint_version smallint;
        v_fingerprint bytea;
    BEGIN
        SELECT location, retention_anchor_at, retention_class_key,
               fingerprint_version,
               command_fingerprint
        INTO v_location, v_anchor, v_retention_class_key,
             v_fingerprint_version, v_fingerprint
        FROM {namespace}.combined_registry
        WHERE task_id = p_task_id;
        IF NOT FOUND THEN
            RETURN ROW(FALSE, NULL, NULL, NULL, NULL)
                ::{namespace}.task_lookup;
        END IF;

        CASE v_location
            WHEN 'LIVE' THEN
                PERFORM 1 FROM {namespace}.combined_live
                WHERE task_id = p_task_id;
            WHEN 'HISTORY' THEN
                PERFORM 1 FROM {namespace}.combined_history
                WHERE task_id = p_task_id
                  AND retention_class_key = v_retention_class_key
                  AND terminal_at = v_anchor;
            ELSE
                RAISE EXCEPTION 'unrecognized combined-registry location: %',
                    v_location;
        END CASE;
        IF NOT FOUND THEN
            RETURN ROW(FALSE, v_location, p_task_id,
                       v_fingerprint_version, v_fingerprint)
                ::{namespace}.task_lookup;
        END IF;
        RETURN ROW(TRUE, v_location, p_task_id,
                   v_fingerprint_version, v_fingerprint)
            ::{namespace}.task_lookup;
    END
    $function$
    """
