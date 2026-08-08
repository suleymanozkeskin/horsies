"""Disposable phase-2 pending and quarantine program."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.schema import PrototypeSchema


async def install_workflow_recovery_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    for statement in _workflow_recovery_manifest(schema):
        await connection.execute(text(statement))


def _workflow_recovery_manifest(schema: PrototypeSchema) -> tuple[str, ...]:
    namespace = schema.sql
    return (
        f"""
        CREATE TABLE {namespace}.history_aggregate_finite_2026_06_01
            PARTITION OF {namespace}.history_aggregate_finite
            FOR VALUES FROM ('2026-06-01T00:00:00Z')
            TO ('2026-06-02T00:00:00Z')
        """,
        f"""
        CREATE INDEX history_aggregate_finite_2026_06_01_id_idx
            ON {namespace}.history_aggregate_finite_2026_06_01 (task_id)
        """,
        f"""
        CREATE TYPE {namespace}.recovery_source_kind AS ENUM (
            'HISTORY', 'QUARANTINE'
        )
        """,
        f"""
        CREATE TYPE {namespace}.quarantine_outcome AS ENUM (
            'REPOINTED', 'PENDING_ABSENT', 'ALREADY_QUARANTINED',
            'TOO_YOUNG', 'NOT_DETACHABLE', 'LOCATOR_CONFLICT',
            'SOURCE_ABSENT', 'INTEGRITY_CONFLICT'
        )
        """,
        f"""
        CREATE TYPE {namespace}.phase2_disposition AS ENUM (
            'APPLIED_TO_NODE', 'ALREADY_APPLIED',
            'SUPERSEDED_BY_WORKFLOW_TERMINAL', 'SOURCE_STATE_CONFLICT',
            'PENDING_ABSENT'
        )
        """,
        f"""
        CREATE TABLE {namespace}.phase2_workflows (
            workflow_id uuid PRIMARY KEY,
            status text NOT NULL CHECK (
                status IN ('RUNNING', 'PAUSED', 'COMPLETED', 'FAILED', 'CANCELLED')
            )
        )
        """,
        f"""
        CREATE TABLE {namespace}.phase2_nodes (
            id bigserial NOT NULL UNIQUE,
            workflow_id uuid NOT NULL,
            node_id text NOT NULL,
            task_id uuid UNIQUE,
            status text NOT NULL CHECK (
                status IN (
                    'ENQUEUED', 'READY', 'PENDING', 'RUNNING',
                    'COMPLETED', 'FAILED', 'CANCELLED', 'SKIPPED'
                )
            ),
            result_payload bytea,
            result_digest bytea,
            phase2_generation uuid,
            requires_parent_propagation boolean NOT NULL,
            PRIMARY KEY (workflow_id, node_id),
            UNIQUE (id, workflow_id),
            FOREIGN KEY (workflow_id)
                REFERENCES {namespace}.phase2_workflows(workflow_id),
            CHECK (result_digest IS NULL OR octet_length(result_digest) = 32)
        )
        """,
        f"""
        CREATE TABLE {namespace}.phase2_parent_responsibilities (
            workflow_id uuid NOT NULL,
            node_id text NOT NULL,
            phase2_generation uuid NOT NULL,
            created_at timestamptz NOT NULL,
            PRIMARY KEY (workflow_id, node_id, phase2_generation)
        )
        """,
        f"""
        CREATE TABLE {namespace}.workflow_phase2_quarantine (
            task_id uuid PRIMARY KEY,
            workflow_id uuid NOT NULL,
            node_id text NOT NULL,
            task_name text NOT NULL,
            terminal_status text NOT NULL CHECK (
                terminal_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
            ),
            terminalization_kind text NOT NULL,
            terminal_at timestamptz NOT NULL,
            history_schema_version smallint NOT NULL,
            result_envelope_version smallint NOT NULL,
            result_codec text NOT NULL,
            result_payload bytea NOT NULL,
            result_digest bytea NOT NULL,
            source_history_class text NOT NULL,
            source_history_anchor timestamptz NOT NULL,
            quarantine_reason text NOT NULL,
            quarantined_at timestamptz NOT NULL,
            CHECK (octet_length(result_digest) = 32)
        )
        """,
        f"""
        CREATE TABLE {namespace}.workflow_phase2_pending (
            task_id uuid PRIMARY KEY,
            workflow_id uuid NOT NULL,
            workflow_node_row_id bigint NOT NULL,
            terminal_status text NOT NULL CHECK (
                terminal_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
            ),
            terminal_at timestamptz NOT NULL,
            terminalization_kind varchar(32) NOT NULL
                CHECK (octet_length(terminalization_kind) <= 32),
            recovery_source {namespace}.recovery_source_kind NOT NULL,
            history_class varchar(64)
                CHECK (octet_length(history_class) <= 64),
            history_anchor timestamptz,
            history_schema_version smallint NOT NULL,
            result_digest bytea NOT NULL,
            quarantine_task_id uuid
                REFERENCES {namespace}.workflow_phase2_quarantine(task_id),
            phase2_generation uuid NOT NULL,
            created_at timestamptz NOT NULL,
            attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            last_attempt_at timestamptz,
            last_failure_class varchar(64)
                CHECK (octet_length(last_failure_class) <= 64),
            CHECK (octet_length(result_digest) = 32),
            FOREIGN KEY (workflow_node_row_id, workflow_id)
                REFERENCES {namespace}.phase2_nodes(id, workflow_id),
            CHECK (
                (recovery_source = 'HISTORY'
                    AND history_class IS NOT NULL
                    AND history_anchor IS NOT NULL
                    AND quarantine_task_id IS NULL)
                OR (recovery_source = 'QUARANTINE'
                    AND history_class IS NULL
                    AND history_anchor IS NULL
                    AND quarantine_task_id = task_id)
            )
        )
        """,
        f"""
        CREATE INDEX phase2_pending_oldest_idx
            ON {namespace}.workflow_phase2_pending (created_at, task_id)
        """,
        f"""
        CREATE INDEX phase2_pending_workflow_node_idx
            ON {namespace}.workflow_phase2_pending (workflow_node_row_id)
        """,
        f"""
        CREATE INDEX phase2_pending_history_locator_idx
            ON {namespace}.workflow_phase2_pending
                (history_class, history_anchor, task_id)
            WHERE recovery_source = 'HISTORY'
        """,
        f"""
        CREATE INDEX phase2_pending_failure_idx
            ON {namespace}.workflow_phase2_pending
                (last_failure_class, last_attempt_at)
            WHERE last_failure_class IS NOT NULL
        """,
        _archive_value_is_valid_function(namespace),
        _quarantine_function(namespace),
        _phase2_function(namespace),
    )


def _archive_value_is_valid_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.archive_value_is_valid(
        p_version smallint,
        p_codec text,
        p_payload bytea,
        p_digest bytea
    ) RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    AS $function$
    BEGIN
        IF p_version <> 1
           OR p_codec <> 'json-utf8'
           OR sha256(p_payload) <> p_digest THEN
            RETURN FALSE;
        END IF;
        PERFORM convert_from(p_payload, 'UTF8')::jsonb;
        RETURN TRUE;
    EXCEPTION
        WHEN character_not_in_repertoire
          OR untranslatable_character
          OR invalid_text_representation THEN
            RETURN FALSE;
    END
    $function$
    """


def _quarantine_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.quarantine_phase2_pending(
        p_task_id uuid,
        p_leaf_lower timestamptz,
        p_leaf_upper timestamptz,
        p_detach_horizon interval,
        p_reason text
    ) RETURNS {namespace}.quarantine_outcome
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_pending {namespace}.workflow_phase2_pending%ROWTYPE;
        v_history {namespace}.history_aggregate%ROWTYPE;
        v_node_id text;
        v_duration interval;
    BEGIN
        IF p_leaf_lower IS NULL OR p_leaf_upper IS NULL
           OR p_leaf_lower >= p_leaf_upper THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'leaf bounds must be non-null and increasing';
        END IF;
        IF p_detach_horizon <= interval '0' THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'detach horizon must be positive';
        END IF;
        IF p_reason IS NULL OR p_reason = '' THEN
            RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
                MESSAGE = 'quarantine reason must be non-empty';
        END IF;

        SELECT * INTO v_pending
        FROM {namespace}.workflow_phase2_pending
        WHERE task_id = p_task_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RETURN 'PENDING_ABSENT';
        END IF;
        IF v_pending.recovery_source = 'QUARANTINE' THEN
            RETURN 'ALREADY_QUARANTINED';
        END IF;
        SELECT node_id INTO v_node_id
        FROM {namespace}.phase2_nodes
        WHERE id = v_pending.workflow_node_row_id
          AND workflow_id = v_pending.workflow_id;
        IF NOT FOUND THEN
            RETURN 'SOURCE_ABSENT';
        END IF;
        IF v_pending.created_at + p_detach_horizon > statement_timestamp() THEN
            RETURN 'TOO_YOUNG';
        END IF;
        IF v_pending.history_anchor < p_leaf_lower
           OR v_pending.history_anchor >= p_leaf_upper THEN
            RETURN 'LOCATOR_CONFLICT';
        END IF;

        SELECT duration INTO v_duration
        FROM {namespace}.retention_classes
        WHERE class_key = v_pending.history_class;
        IF NOT FOUND OR v_duration IS NULL
           OR p_leaf_upper + v_duration > statement_timestamp() THEN
            RETURN 'NOT_DETACHABLE';
        END IF;

        SELECT * INTO v_history
        FROM {namespace}.history_aggregate
        WHERE task_id = p_task_id
          AND retention_class_key = v_pending.history_class
          AND retention_anchor_at = v_pending.history_anchor
        FOR UPDATE;
        IF NOT FOUND THEN
            RETURN 'SOURCE_ABSENT';
        END IF;
        IF v_history.history_schema_version
                IS DISTINCT FROM v_pending.history_schema_version
           OR v_history.history_schema_version IS DISTINCT FROM 1
           OR v_history.result_digest IS DISTINCT FROM v_pending.result_digest
           OR {namespace}.archive_value_is_valid(
                v_history.result_envelope_version,
                v_history.result_codec,
                v_history.result_payload,
                v_history.result_digest
              ) IS NOT TRUE THEN
            UPDATE {namespace}.workflow_phase2_pending
            SET attempt_count = attempt_count + 1,
                last_attempt_at = statement_timestamp(),
                last_failure_class = 'SOURCE_INTEGRITY'
            WHERE task_id = p_task_id;
            RETURN 'INTEGRITY_CONFLICT';
        END IF;

        INSERT INTO {namespace}.workflow_phase2_quarantine (
            task_id, workflow_id, node_id, task_name, terminal_status,
            terminalization_kind, terminal_at, history_schema_version,
            result_envelope_version, result_codec, result_payload,
            result_digest, source_history_class, source_history_anchor,
            quarantine_reason, quarantined_at
        ) VALUES (
            v_pending.task_id, v_pending.workflow_id, v_node_id,
            v_history.task_name, v_history.status,
            v_history.terminalization_kind, v_history.terminal_at,
            v_history.history_schema_version,
            v_history.result_envelope_version, v_history.result_codec,
            v_history.result_payload, v_history.result_digest,
            v_history.retention_class_key, v_history.retention_anchor_at,
            p_reason, statement_timestamp()
        );

        IF NOT EXISTS (
            SELECT 1 FROM {namespace}.workflow_phase2_quarantine
            WHERE task_id = v_pending.task_id
              AND result_digest = v_pending.result_digest
              AND history_schema_version = v_pending.history_schema_version
        ) THEN
            RAISE EXCEPTION 'quarantine copy verification failed';
        END IF;

        UPDATE {namespace}.workflow_phase2_pending
        SET recovery_source = 'QUARANTINE',
            history_class = NULL,
            history_anchor = NULL,
            quarantine_task_id = task_id
        WHERE task_id = v_pending.task_id;
        RETURN 'REPOINTED';
    END
    $function$
    """


def _phase2_function(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.apply_phase2(
        p_task_id uuid,
        p_phase2_generation uuid
    ) RETURNS {namespace}.phase2_disposition
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        v_pending {namespace}.workflow_phase2_pending%ROWTYPE;
        v_workflow_status text;
        v_node {namespace}.phase2_nodes%ROWTYPE;
        v_result_payload bytea;
        v_result_digest bytea;
        v_result_version smallint;
        v_result_codec text;
        v_history_schema_version smallint;
        v_terminal_status text;
        v_quarantine_task_id uuid;
    BEGIN
        SELECT * INTO v_pending
        FROM {namespace}.workflow_phase2_pending
        WHERE task_id = p_task_id;
        IF NOT FOUND THEN
            IF EXISTS (
                SELECT 1 FROM {namespace}.phase2_nodes
                WHERE task_id = p_task_id
                  AND phase2_generation = p_phase2_generation
            ) THEN
                RETURN 'ALREADY_APPLIED';
            END IF;
            RETURN 'PENDING_ABSENT';
        END IF;

        SELECT status INTO v_workflow_status
        FROM {namespace}.phase2_workflows
        WHERE workflow_id = v_pending.workflow_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RETURN 'SOURCE_STATE_CONFLICT';
        END IF;
        SELECT * INTO v_node
        FROM {namespace}.phase2_nodes
        WHERE id = v_pending.workflow_node_row_id
          AND workflow_id = v_pending.workflow_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RETURN 'SOURCE_STATE_CONFLICT';
        END IF;
        SELECT * INTO v_pending
        FROM {namespace}.workflow_phase2_pending
        WHERE task_id = p_task_id
        FOR UPDATE;
        IF NOT FOUND
           OR v_pending.phase2_generation <> p_phase2_generation
           OR v_pending.workflow_id <> v_node.workflow_id THEN
            RETURN 'SOURCE_STATE_CONFLICT';
        END IF;

        CASE v_pending.recovery_source
            WHEN 'HISTORY' THEN
                SELECT result_payload, result_digest, result_envelope_version,
                       result_codec, history_schema_version, status
                INTO v_result_payload, v_result_digest, v_result_version,
                     v_result_codec, v_history_schema_version, v_terminal_status
                FROM {namespace}.history_aggregate
                WHERE task_id = v_pending.task_id
                  AND retention_class_key = v_pending.history_class
                  AND retention_anchor_at = v_pending.history_anchor;
            WHEN 'QUARANTINE' THEN
                SELECT result_payload, result_digest, result_envelope_version,
                       result_codec, history_schema_version, terminal_status,
                       task_id
                INTO v_result_payload, v_result_digest, v_result_version,
                     v_result_codec, v_history_schema_version,
                     v_terminal_status, v_quarantine_task_id
                FROM {namespace}.workflow_phase2_quarantine
                WHERE task_id = v_pending.quarantine_task_id
                FOR UPDATE;
            ELSE
                RAISE EXCEPTION 'unrecognized recovery source: %',
                    v_pending.recovery_source;
        END CASE;
        IF NOT FOUND
           OR v_result_digest IS DISTINCT FROM v_pending.result_digest
           OR v_history_schema_version
                IS DISTINCT FROM v_pending.history_schema_version
           OR v_history_schema_version IS DISTINCT FROM 1
           OR {namespace}.archive_value_is_valid(
                v_result_version, v_result_codec,
                v_result_payload, v_result_digest
              ) IS NOT TRUE THEN
            UPDATE {namespace}.workflow_phase2_pending
            SET attempt_count = attempt_count + 1,
                last_attempt_at = statement_timestamp(),
                last_failure_class = 'SOURCE_INTEGRITY'
            WHERE task_id = p_task_id;
            RETURN 'SOURCE_STATE_CONFLICT';
        END IF;

        IF v_node.phase2_generation = p_phase2_generation
           AND v_node.result_digest = v_result_digest
           AND v_node.status IN ('COMPLETED', 'FAILED') THEN
            DELETE FROM {namespace}.workflow_phase2_pending
            WHERE task_id = p_task_id;
            IF v_quarantine_task_id IS NOT NULL THEN
                DELETE FROM {namespace}.workflow_phase2_quarantine
                WHERE task_id = v_quarantine_task_id;
            END IF;
            RETURN 'ALREADY_APPLIED';
        END IF;

        IF v_workflow_status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
            DELETE FROM {namespace}.workflow_phase2_pending
            WHERE task_id = p_task_id;
            IF v_quarantine_task_id IS NOT NULL THEN
                DELETE FROM {namespace}.workflow_phase2_quarantine
                WHERE task_id = v_quarantine_task_id;
            END IF;
            RETURN 'SUPERSEDED_BY_WORKFLOW_TERMINAL';
        END IF;
        IF v_node.status <> 'RUNNING' THEN
            RETURN 'SOURCE_STATE_CONFLICT';
        END IF;

        UPDATE {namespace}.phase2_nodes
        SET status = CASE WHEN v_terminal_status = 'COMPLETED'
                          THEN 'COMPLETED' ELSE 'FAILED' END,
            result_payload = v_result_payload,
            result_digest = v_result_digest,
            phase2_generation = p_phase2_generation
        WHERE id = v_pending.workflow_node_row_id;
        IF v_node.requires_parent_propagation THEN
            INSERT INTO {namespace}.phase2_parent_responsibilities (
                workflow_id, node_id, phase2_generation, created_at
            ) VALUES (
                v_node.workflow_id, v_node.node_id,
                p_phase2_generation, statement_timestamp()
            ) ON CONFLICT DO NOTHING;
        END IF;
        DELETE FROM {namespace}.workflow_phase2_pending
        WHERE task_id = p_task_id;
        IF v_quarantine_task_id IS NOT NULL THEN
            DELETE FROM {namespace}.workflow_phase2_quarantine
            WHERE task_id = v_quarantine_task_id;
        END IF;
        RETURN 'APPLIED_TO_NODE';
    END
    $function$
    """
