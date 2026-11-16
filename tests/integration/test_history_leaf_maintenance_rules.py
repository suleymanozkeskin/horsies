"""History leaf maintenance against the baseline schema: pending blockers and DDL roles.

A HISTORY pending row pins the leaf that holds its source row: the detach of
that leaf is refused until the quarantine copy repoints the row. After the
repoint the leaf detaches and drops, and phase 2 reads the copy.

Leaf DDL needs CREATE on the schema and ownership of the class parent. A role
with table privileges only (a worker) cannot create a leaf, and the health
pass names the missing privilege; a role with both (an operator, or the role
that installed the schema) can.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Final
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CollectPartitionHealth,
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    LeafBounds,
    LeafLifecycle,
    LeafRef,
)
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.outcomes import (
    LeafCreated,
    LeafDetached,
    LeafDropped,
    LeafPendingBlocked,
    MissingDdlPrivilege,
)
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.health import collect_partition_health
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_expired_leaf,
    drop_detached_leaf,
)
from horsies.core.history.partitions.publication import UnpublishedLoader
from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    create_workflow,
    day_bounds,
    frozen_history_row,
    link_workflow_node,
    register_class,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

UTC = timezone.utc
CLASS_KEY: Final = 'standard_30d'
EXPIRED_DAYS_AGO: Final = 40
MAINTENANCE_CONNECTIONS: Final = 2

history_schema = task_history_schema_fixture('task_history_it_leaf_rules')


def leaf_ref(parent_name: str, day: datetime) -> LeafRef:
    lower, upper = day_bounds(day)
    return LeafRef(
        leaf_name=daily_leaf_name(parent_name, lower),
        class_key=CLASS_KEY,
        bounds=LeafBounds(lower=lower, upper=upper),
    )


async def seed_pinned_history_row(
    connection: AsyncConnection, ref: LeafRef
) -> str:
    """A workflow task's history row in `ref` and the HISTORY pending row of its node."""
    task_id = str(uuid4())
    terminal_at = ref.bounds.lower + timedelta(hours=1)
    row = frozen_history_row(
        task_id=task_id, class_key=CLASS_KEY, terminal_at=terminal_at
    )
    row.update(
        terminalization_kind='COMPLETE_LOCKED',
        is_workflow_task=True,
        command_fingerprint_version=None,
        command_fingerprint=None,
    )
    await connection.execute(text(INSERT_HISTORY_ROW_SQL), row)
    workflow_id = await create_workflow(connection, status='RUNNING')
    node_row_id = await link_workflow_node(
        connection, task_id, workflow_id=workflow_id, node_status='RUNNING'
    )
    await connection.execute(
        text(
            """
            INSERT INTO horsies_workflow_phase2_pending (
                task_id, workflow_id, workflow_node_row_id,
                terminalization_kind, recovery_source, history_class,
                history_anchor, history_schema_version, result_digest,
                quarantine_task_id, created_at, attempt_count,
                last_attempt_at, last_failure_class
            ) VALUES (
                CAST(:task_id AS uuid), CAST(:workflow_id AS uuid),
                CAST(:node_row_id AS uuid),
                'COMPLETE_LOCKED', 'HISTORY', :class_key,
                :anchor, 1, :digest,
                NULL, statement_timestamp(), 0,
                NULL, NULL
            )
            """
        ),
        {
            'task_id': task_id,
            'workflow_id': workflow_id,
            'node_row_id': node_row_id,
            'class_key': CLASS_KEY,
            'anchor': terminal_at,
            'digest': sha256(b'{}').digest(),
        },
    )
    return task_id


async def detach(schema: HistorySchema, ref: LeafRef) -> object:
    return await detach_expired_leaf(
        PartitionMaintenanceDatabase(
            schema.engine, connection_capacity=MAINTENANCE_CONNECTIONS
        ),
        DetachExpiredHistoryLeaf(leaf=ref, statement_timeout_ms=None),
        UnpublishedLoader(),
    )


async def test_a_quarantine_repoint_releases_the_leaf_that_a_pending_row_pins(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        parent = await register_class(connection, CLASS_KEY)
        ref = leaf_ref(
            parent, datetime.now(UTC) - timedelta(days=EXPIRED_DAYS_AGO)
        )
        created = await create_daily_leaf(
            connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
        )
        assert isinstance(created, LeafCreated)
        task_id = await seed_pinned_history_row(connection, ref)

    refused = await detach(history_schema, ref)
    assert isinstance(refused, LeafPendingBlocked)
    assert refused.blocker_count == 1

    async with history_schema.engine.begin() as connection:
        verdict = (
            await connection.execute(
                text(
                    'SELECT verdict FROM horsies_phase2_quarantine_one('
                    'CAST(:task_id AS uuid))'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()
        assert verdict == 'REPOINTED'

    assert isinstance(await detach(history_schema, ref), LeafDetached)
    async with history_schema.engine.begin() as connection:
        dropped = await drop_detached_leaf(
            connection,
            DropDetachedHistoryLeaf(leaf=ref, lifecycle=LeafLifecycle.RETENTION),
            UnpublishedLoader(),
        )
        assert dropped == LeafDropped(leaf_name=ref.leaf_name)

    async with history_schema.engine.begin() as connection:
        disposition = (
            await connection.execute(
                text(
                    'SELECT disposition, node_status FROM horsies_phase2_consume('
                    "CAST(:task_id AS uuid), 'COMPLETED')"
                ),
                {'task_id': task_id},
            )
        ).one()
        assert tuple(disposition) == ('APPLIED_TO_NODE', 'COMPLETED')
        remaining = (
            await connection.execute(
                text(
                    'SELECT '
                    '(SELECT count(*) FROM horsies_workflow_phase2_pending), '
                    '(SELECT count(*) FROM horsies_workflow_phase2_quarantine)'
                )
            )
        ).one()
        assert tuple(remaining) == (0, 0)


async def test_a_drop_keeps_an_out_of_band_detached_leaf_that_a_pending_row_pins(
    history_schema: HistorySchema,
) -> None:
    """A leaf detached outside the manager is still pinned by its pending row."""
    async with history_schema.engine.begin() as connection:
        parent = await register_class(connection, CLASS_KEY)
        ref = leaf_ref(
            parent, datetime.now(UTC) - timedelta(days=EXPIRED_DAYS_AGO)
        )
        created = await create_daily_leaf(
            connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
        )
        assert isinstance(created, LeafCreated)
        await seed_pinned_history_row(connection, ref)
        await connection.execute(
            text(f'ALTER TABLE {parent} DETACH PARTITION {ref.leaf_name}')
        )

    async with history_schema.engine.begin() as connection:
        refused = await drop_detached_leaf(
            connection,
            DropDetachedHistoryLeaf(leaf=ref, lifecycle=LeafLifecycle.RETENTION),
            UnpublishedLoader(),
        )
        assert isinstance(refused, LeafPendingBlocked)
        assert refused.blocker_count == 1
        relation = (
            await connection.execute(
                text('SELECT to_regclass(:leaf_name) IS NOT NULL'),
                {'leaf_name': ref.leaf_name},
            )
        ).scalar_one()
        assert relation is True


def privilege_faults(faults: tuple[object, ...]) -> list[MissingDdlPrivilege]:
    return [fault for fault in faults if isinstance(fault, MissingDdlPrivilege)]


async def test_leaf_ddl_needs_schema_create_and_parent_ownership(
    history_schema: HistorySchema,
) -> None:
    """The installer and an operator manage leaves; a worker cannot."""
    suffix = uuid4().hex[:12]
    worker_role = f'it_history_worker_{suffix}'
    operator_role = f'it_history_operator_{suffix}'
    schema = history_schema.schema_name
    tomorrow = datetime.now(UTC) + timedelta(days=1)

    async with history_schema.engine.begin() as connection:
        parent = await register_class(connection, CLASS_KEY)
        installer = await collect_partition_health(
            connection,
            CollectPartitionHealth(class_key=CLASS_KEY, application_managed=True),
        )
        assert privilege_faults(installer.faults) == []
        for role in (worker_role, operator_role):
            await connection.execute(text(f'CREATE ROLE {role} NOLOGIN'))
            await connection.execute(
                text(f'GRANT USAGE ON SCHEMA {schema} TO {role}')
            )
            await connection.execute(
                text(
                    'GRANT SELECT, INSERT, UPDATE, DELETE '
                    f'ON ALL TABLES IN SCHEMA {schema} TO {role}'
                )
            )
        await connection.execute(
            text(f'GRANT CREATE ON SCHEMA {schema} TO {operator_role}')
        )
        await connection.execute(
            text(f'ALTER TABLE {parent} OWNER TO {operator_role}')
        )
    try:
        async with history_schema.engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE {worker_role}'))
            worker = await collect_partition_health(
                connection,
                CollectPartitionHealth(
                    class_key=CLASS_KEY, application_managed=True
                ),
            )
            assert privilege_faults(worker.faults) == [
                MissingDdlPrivilege(schema_create=False, owns_parent=False)
            ]
            with pytest.raises(DBAPIError, match='permission denied|must be owner'):
                async with connection.begin_nested():
                    await create_daily_leaf(
                        connection,
                        CreateDailyHistoryLeaf(leaf=leaf_ref(parent, tomorrow)),
                        UnpublishedLoader(),
                    )

        async with history_schema.engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE {operator_role}'))
            operator = await collect_partition_health(
                connection,
                CollectPartitionHealth(
                    class_key=CLASS_KEY, application_managed=True
                ),
            )
            assert privilege_faults(operator.faults) == []
            created = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=leaf_ref(parent, tomorrow)),
                UnpublishedLoader(),
            )
            assert isinstance(created, LeafCreated)
    finally:
        async with history_schema.engine.begin() as connection:
            for role in (worker_role, operator_role):
                await connection.execute(
                    text(f'REASSIGN OWNED BY {role} TO current_user')
                )
                await connection.execute(text(f'DROP OWNED BY {role}'))
                await connection.execute(text(f'DROP ROLE {role}'))
