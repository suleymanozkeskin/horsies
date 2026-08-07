"""The whole offline program, in ratified order, on the real chain.

One database installed by the production migration chain runs every
stage end to end: preflight, drain verification, program replacement,
identity normalization, envelope preparation, relocation, tighten,
validation. The tighten gate is proven to refuse — wrong confirmation
phrase, terminal rows still live — before the pass that crosses it,
and after the crossing the validation report attests the frozen
posture on concrete catalog facts.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.cutover.drain import DrainVerified, verify_drained
from horsies.core.history.cutover.identity import (
    normalize_attempt_identity,
)
from horsies.core.history.cutover.preflight import (
    RelocationCoefficients,
    run_preflight,
)
from horsies.core.history.cutover.program import install_programs
from horsies.core.history.cutover.relocation import (
    RELOCATION_LEDGER_DDL,
    RelocationComplete,
)
from horsies.core.history.cutover.tighten import (
    TightenComplete,
    TightenRefused,
    confirmation_phrase,
    tighten_to_frozen,
)
from horsies.core.history.cutover.validation import (
    CutoverValidated,
    validate_cutover,
)
from horsies.core.models.broker import PostgresConfig
from tests.integration.task_history_harness import prepare_move_storage
from tests.integration.test_task_history_preparation import run_preparation_to_complete
from tests.integration.test_task_history_relocation import (
    CLASS_KEY,
    insert_legacy_task,
    relocate_all,
)
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]

COEFFICIENTS = RelocationCoefficients(
    seconds_per_million_rows=120.0, fixed_seconds=30.0
)


@pytest.mark.asyncio
async def test_the_offline_program_end_to_end(
    make_database: MakeDatabase,
) -> None:
    url = await make_database()
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()

    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            # The legacy population a deployment brings to the cutover.
            await prepare_move_storage(connection, CLASS_KEY)
            await connection.execute(text(RELOCATION_LEDGER_DDL))
            terminal_completed = await insert_legacy_task(
                connection,
                status='COMPLETED',
                kind='COMPLETE_LOCKED',
                result='{"ok": true}',
                disposition=None,
                retain=None,
                args_json='[1]',
            )
            await insert_legacy_task(
                connection,
                status='FAILED',
                kind=None,
                disposition=None,
                retain=None,
                args_json='[2]',
                attempts=(('FAILED', 'the recorded reason'),),
            )
            live_pending = await insert_legacy_task(
                connection, status='PENDING', kind=None
            )

            # Stage 0: preflight sees the work.
            plan = await run_preflight(
                connection, coefficients=COEFFICIENTS
            )
            assert plan.terminal_live_rows == 2

            # Stage 2: drain verified (nothing claimed or running).
            drained = await verify_drained(connection)
            assert isinstance(drained, DrainVerified)

            # Stage 3: program replacement; stage-4 prerequisites.
            await normalize_attempt_identity(connection)
            await install_programs(connection)

            # Stage 5 gate BEFORE stage 4: terminal rows still live and
            # the phrase is wrong — both refusals name themselves.
            refused = await tighten_to_frozen(
                connection,
                backup_label='backup-2026-08-07',
                operator_confirmation='yes',
            )
            assert isinstance(refused, TightenRefused)
            assert any(
                'confirmation' in reason for reason in refused.reasons
            )
            assert any(
                'terminal rows remain' in reason
                for reason in refused.reasons
            )

            # Stage 4: preparation, then relocation.
            await run_preparation_to_complete(connection, retain_default=True)
            relocated = await relocate_all(connection)
            assert isinstance(relocated, RelocationComplete)
            assert relocated.rows_relocated == 2

            # Stage 5: the point of no return, correctly confirmed.
            tightened = await tighten_to_frozen(
                connection,
                backup_label='backup-2026-08-07',
                operator_confirmation=confirmation_phrase(
                    'backup-2026-08-07'
                ),
            )
            assert isinstance(tightened, TightenComplete), tightened

            # Stage 6: validation attests the frozen posture.
            validated = await validate_cutover(connection)
            assert isinstance(validated, CutoverValidated), validated
            assert validated.history_rows == 2

            # The surviving live row rides the frozen shape.
            survivor = (
                await connection.execute(
                    text(
                        'SELECT id, status FROM horsies_tasks '
                        'WHERE id = CAST(:t AS uuid)'
                    ),
                    {'t': live_pending},
                )
            ).one()
            assert survivor.status == 'PENDING'
            history_row = (
                await connection.execute(
                    text(
                        'SELECT status FROM horsies_task_history '
                        'WHERE task_id = CAST(:t AS uuid)'
                    ),
                    {'t': terminal_completed},
                )
            ).one()
            assert history_row.status == 'COMPLETED'
    finally:
        await engine.dispose()
