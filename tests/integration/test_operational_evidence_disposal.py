"""Disposing the evidence schema must never destroy the evidence.

The operational qualification returned its evidence from inside a `try` and
cleaned up in `finally`. A single `DROP SCHEMA ... CASCADE` over a 512-leaf
schema exhausts the lock table at production-shaped settings, so the cleanup
raised, and the raise replaced a completed return value with an exception: the
measurements had run, a verdict had been computed, and the evidence was
destroyed on the way out. Both majors, identically, after four minutes of work.

These checks pin the two halves of the fix — a disposal that survives the
partition count, and a disposal that cannot propagate a failure whatever the
cause.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes import operational_evidence
from tests.task_history_prototypes.operational_evidence import (
    dispose_evidence_schema,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_CLASS_KEY = 'finite_30d_v1'
# Enough leaves that batching is exercised several times over without paying
# for the full qualification shape; the 512-leaf proof is the redispatched run.
_LEAVES = 96
_BATCH = 32


async def _seed_leaves(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    count: int,
) -> None:
    origin = datetime(2026, 9, 1, tzinfo=timezone.utc)
    async with engine.connect() as connection:
        for offset in range(count):
            lower = origin + timedelta(days=offset)
            upper = lower + timedelta(days=1)
            name = f'history_aggregate_finite_{lower:%Y_%m_%d}'
            await connection.execute(
                text(
                    f"""
                    CREATE TABLE {schema.sql}."{name}"
                        PARTITION OF {schema.sql}.history_aggregate_finite
                        FOR VALUES FROM ('{lower.isoformat()}')
                        TO ('{upper.isoformat()}')
                    """
                )
            )
            await connection.execute(
                text(
                    f'CREATE INDEX "{name}_id_idx" '
                    f'ON {schema.sql}."{name}" (task_id)'
                )
            )
        await connection.commit()


async def _schema_exists(engine: AsyncEngine, name: str) -> bool:
    async with engine.connect() as connection:
        return bool(
            (
                await connection.execute(
                    text(
                        'SELECT EXISTS (SELECT 1 FROM pg_namespace '
                        'WHERE nspname = :name)'
                    ),
                    {'name': name},
                )
            ).scalar_one()
        )


class TestBoundedDisposal:
    async def test_many_leaf_schema_is_disposed_in_batches(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    ) -> None:
        schema = PrototypeSchema(f'disposal_{uuid4().hex[:10]}')
        async with engine.connect() as connection:
            await install_archive_candidates(connection, schema)
            await connection.commit()
        await _seed_leaves(engine, schema, count=_LEAVES)

        outcome = await dispose_evidence_schema(
            engine,
            schema,
            batch_size=_BATCH,
        )

        assert outcome.clean
        assert outcome.schema_dropped
        assert outcome.warning is None
        assert outcome.leaves_dropped >= _LEAVES
        # Batching is the point: one statement over every relation is what
        # exhausted the lock table.
        assert outcome.batches > 1
        assert not await _schema_exists(engine, schema.name)

    async def test_absent_schema_disposes_without_raising(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    ) -> None:
        outcome = await dispose_evidence_schema(
            engine,
            PrototypeSchema(f'never_created_{uuid4().hex[:8]}'),
        )

        assert outcome.clean
        assert outcome.leaves_dropped == 0


class TestDisposalNeverPropagates:
    """Whatever goes wrong in cleanup, the caller still gets an outcome."""

    async def test_failure_is_reported_not_raised(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def explode(*_args: object, **_kwargs: object) -> tuple[str, ...]:
            raise RuntimeError('lock table exhausted')

        monkeypatch.setattr(
            operational_evidence,
            '_leaf_partitions',
            explode,
        )

        outcome = await dispose_evidence_schema(
            engine,
            PrototypeSchema(f'disposal_{uuid4().hex[:10]}'),
        )

        assert not outcome.clean
        assert not outcome.schema_dropped
        assert outcome.warning is not None
        # The artifact has to say what went wrong, not merely that something did.
        assert 'RuntimeError' in outcome.warning
        assert 'lock table exhausted' in outcome.warning
