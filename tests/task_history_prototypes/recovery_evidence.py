"""Physical-size evidence for workflow phase-2 locator candidates."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.archive import archive_digest
from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_conditions,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.workflow_schema import (
    install_workflow_recovery_prototype,
)


@dataclass(frozen=True, slots=True)
class PendingLocatorEvidence:
    conditions: EvidenceConditions
    byte_budget: int
    wide_locator_bytes: int
    compact_history_locator_bytes: int
    compact_quarantine_locator_bytes: int
    compact_candidate_passed: bool


async def collect_pending_locator_evidence(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
) -> PendingLocatorEvidence:
    conditions = await collect_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture='not applicable; exact row-size evidence',
        prepared_posture='one insert per locator shape',
    )
    schema = PrototypeSchema(f'history_pending_evidence_{uuid4().hex[:8]}')
    await install_archive_candidates(connection, schema)
    await install_workflow_recovery_prototype(connection, schema)
    await connection.commit()
    try:
        wide_bytes = await _measure_wide_candidate(connection, schema)
        compact_history, compact_quarantine = await _measure_compact_candidate(
            connection,
            schema,
        )
        byte_budget = 512
        return PendingLocatorEvidence(
            conditions=conditions,
            byte_budget=byte_budget,
            wide_locator_bytes=wide_bytes,
            compact_history_locator_bytes=compact_history,
            compact_quarantine_locator_bytes=compact_quarantine,
            compact_candidate_passed=(
                max(compact_history, compact_quarantine) <= byte_budget
            ),
        )
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()


async def _measure_wide_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> int:
    await connection.execute(
        text(
            f"""
            CREATE TABLE {schema.sql}.wide_pending_candidate (
                task_id uuid PRIMARY KEY,
                workflow_id uuid NOT NULL,
                node_id varchar(255) NOT NULL,
                task_name varchar(255) NOT NULL,
                terminal_status text NOT NULL,
                terminal_at timestamptz NOT NULL,
                terminalization_kind text NOT NULL,
                recovery_source text NOT NULL,
                history_class text,
                history_anchor timestamptz,
                history_schema_version smallint NOT NULL,
                result_digest bytea NOT NULL,
                quarantine_task_id uuid,
                phase2_generation uuid NOT NULL,
                created_at timestamptz NOT NULL,
                attempt_count integer NOT NULL,
                last_attempt_at timestamptz,
                last_failure_class text,
                last_failure_detail text
            )
            """
        )
    )
    task_id = str(uuid4())
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.wide_pending_candidate VALUES (
                :task_id, :workflow_id, repeat('n', 255), repeat('t', 255),
                'COMPLETED', statement_timestamp(), repeat('k', 32),
                'HISTORY', repeat('c', 64), statement_timestamp(), 32767,
                :digest, NULL, :generation, statement_timestamp(),
                2147483647, statement_timestamp(), repeat('f', 64),
                repeat('d', 1024)
            )
            """
        ),
        {
            'task_id': task_id,
            'workflow_id': str(uuid4()),
            'generation': str(uuid4()),
            'digest': archive_digest(b'wide'),
        },
    )
    return (
        await connection.execute(
            text(
                f"""
                SELECT pg_column_size(candidate)
                FROM {schema.sql}.wide_pending_candidate AS candidate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()


async def _measure_compact_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> tuple[int, int]:
    workflow_id = str(uuid4())
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.phase2_workflows (workflow_id, status)
            VALUES (:workflow_id, 'RUNNING')
            """
        ),
        {'workflow_id': workflow_id},
    )
    task_ids = (str(uuid4()), str(uuid4()))
    for index, task_id in enumerate(task_ids, start=1):
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.phase2_nodes (
                    workflow_id, node_id, task_id, status,
                    requires_parent_propagation
                ) VALUES (
                    :workflow_id, :node_id, :task_id, 'RUNNING', FALSE
                )
                """
            ),
            {
                'workflow_id': workflow_id,
                'node_id': f'node-{index}',
                'task_id': task_id,
            },
        )

    quarantine_payload = b'{"ok":true}'
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.workflow_phase2_quarantine (
                task_id, workflow_id, node_id, task_name, terminal_status,
                terminalization_kind, terminal_at, history_schema_version,
                result_envelope_version, result_codec, result_payload,
                result_digest, source_history_class, source_history_anchor,
                quarantine_reason, quarantined_at
            ) VALUES (
                :task_id, :workflow_id, 'node-2', 'prototype.task',
                'COMPLETED', 'COMPLETE_LOCKED', statement_timestamp(),
                1, 1, 'json-utf8', :payload, :digest, 'finite_30d_v1',
                statement_timestamp(), 'size evidence', statement_timestamp()
            )
            """
        ),
        {
            'task_id': task_ids[1],
            'workflow_id': workflow_id,
            'payload': quarantine_payload,
            'digest': archive_digest(quarantine_payload),
        },
    )
    for task_id, source in zip(task_ids, ('HISTORY', 'QUARANTINE'), strict=True):
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.workflow_phase2_pending (
                    task_id, workflow_id, workflow_node_row_id, terminal_status,
                    terminal_at, terminalization_kind, recovery_source,
                    history_class, history_anchor, history_schema_version,
                    result_digest, quarantine_task_id, phase2_generation,
                    created_at, attempt_count, last_attempt_at,
                    last_failure_class
                )
                SELECT CAST(:task_id AS uuid), node.workflow_id, node.id,
                       'COMPLETED',
                       statement_timestamp(),
                       repeat('k', 32), CAST(:source AS {schema.sql}.recovery_source_kind),
                       CASE WHEN :source = 'HISTORY' THEN repeat('é', 32) END,
                       CASE WHEN :source = 'HISTORY'
                            THEN statement_timestamp() END,
                       32767, :digest,
                       CASE WHEN :source = 'QUARANTINE'
                            THEN CAST(:task_id AS uuid) END,
                       CAST(:generation AS uuid), statement_timestamp(),
                       2147483647,
                       statement_timestamp(), repeat('é', 32)
                FROM {schema.sql}.phase2_nodes AS node
                WHERE node.task_id = CAST(:task_id AS uuid)
                """
            ),
            {
                'task_id': task_id,
                'source': source,
                'digest': archive_digest(source.encode()),
                'generation': str(uuid4()),
            },
        )
    sizes = (
        await connection.execute(
            text(
                f"""
                SELECT recovery_source::text, pg_column_size(pending) AS bytes
                FROM {schema.sql}.workflow_phase2_pending AS pending
                ORDER BY recovery_source::text
                """
            )
        )
    ).all()
    by_source = {row.recovery_source: row.bytes for row in sizes}
    return by_source['HISTORY'], by_source['QUARANTINE']
