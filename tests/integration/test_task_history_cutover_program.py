"""Stage-2 installation and stage-3 drain on the real migrated schema.

The stage-2 set must install cleanly on the exact shape the production
migration chain leaves — that is the whole point of assembling it by
import — and R2 must tear it down exactly, with a reinstall proving
the drop list left nothing that blocks a second pass. Drain
verification is read-only and typed: in-flight work names itself.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.cutover.drain import (
    DrainBlocked,
    DrainVerified,
    verify_drained,
)
from horsies.core.history.cutover.identity import (
    normalize_attempt_identity,
)
from horsies.core.history.cutover.program import (
    ProgramsRefused,
    install_programs,
    uninstall_programs,
)
from horsies.core.models.broker import PostgresConfig
from tests.integration.test_task_history_relocation import (
    insert_legacy_task,
)
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]


async def _prepare_db(url: str) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()


class TestProgramInstallation:
    @pytest.mark.asyncio
    async def test_installs_tears_down_and_reinstalls(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                # Before normalization the installer refuses, typed — the
                # same invariant the tighten enforces, at this door: a
                # varchar attempts identity would otherwise surface later
                # as a raw operator-mismatch error naming a type instead
                # of the omission.
                refused = await install_programs(connection)
                assert isinstance(refused, ProgramsRefused)
                assert 'identity' in refused.reasons[0]

                await normalize_attempt_identity(connection)
                installed = await install_programs(connection)
                assert isinstance(installed, int)
                assert installed > 0
                move_present = (
                    await connection.execute(
                        text(
                            "SELECT to_regproc("
                            "'horsies_move_task_to_history') IS NOT NULL"
                        )
                    )
                ).scalar_one()
                assert bool(move_present)
                removed = await uninstall_programs(connection)
                assert removed > 0
                move_present = (
                    await connection.execute(
                        text(
                            "SELECT to_regproc("
                            "'horsies_move_task_to_history') IS NOT NULL"
                        )
                    )
                ).scalar_one()
                assert not bool(move_present)
                # R2 left nothing behind that blocks a second pass.
                second_pass = await install_programs(connection)
                assert isinstance(second_pass, int)
                assert second_pass == installed
        finally:
            await engine.dispose()


class TestDrainVerification:
    @pytest.mark.asyncio
    async def test_in_flight_work_names_itself(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare_db(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                running = await insert_legacy_task(
                    connection, status='RUNNING', kind=None
                )
                blocked = await verify_drained(connection)
                assert isinstance(blocked, DrainBlocked)
                assert blocked.running_rows == 1
                assert blocked.claimed_rows == 0

                await connection.execute(
                    text('DELETE FROM horsies_tasks WHERE id = :t'),
                    {'t': running},
                )
                await insert_legacy_task(
                    connection, status='PENDING', kind=None
                )
                verified = await verify_drained(connection)
                assert isinstance(verified, DrainVerified)
                assert verified.pending_rows == 1
        finally:
            await engine.dispose()
