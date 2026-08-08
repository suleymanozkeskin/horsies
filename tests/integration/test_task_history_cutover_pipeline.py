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
    demote_to_upgraded_world,
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
    seconds_per_million_rows=120.0,
    fixed_seconds=30.0,
    preparation_seconds_per_million_rows=0.0,
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
            # The legacy population a deployment brings to the cutover
            # is only reachable in the world that deployment is in: the
            # fresh chain now installs the cutover's END state, so the
            # upgraded world is reinstated before anything legacy is
            # written. Without it the live-only status domain refuses
            # the very rows the program exists to relocate.
            await demote_to_upgraded_world(connection)
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


@pytest.mark.asyncio
async def test_the_gate_names_unparseable_identities(
    make_database: MakeDatabase,
) -> None:
    """No foreign key ever policed FORMAT on the identity columns; the
    gate verifies every column the conversion will cast and names the
    offenders BEFORE the point of no return."""
    url = await make_database()
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await normalize_attempt_identity(connection)
            await connection.execute(text(RELOCATION_LEDGER_DDL))
            # The gate exists for UPGRADED databases, whose identity
            # columns are varchar until the tighten converts them; a
            # fresh install is uuid from birth and cannot even store a
            # malformed value. Reproduce the upgraded shape.
            await connection.execute(
                text(
                    'ALTER TABLE horsies_workflows '
                    'ALTER COLUMN root_workflow_id TYPE varchar(36)'
                )
            )
            await connection.execute(
                text(
                    'INSERT INTO horsies_workflows '
                    '(id, name, status, on_error, depth, '
                    'root_workflow_id, created_at, updated_at, sent_at) '
                    "VALUES ('11111111-1111-1111-1111-111111111111', "
                    "'legacy-wf', 'COMPLETED', 'fail', 0, "
                    "'not-a-uuid', "
                    'statement_timestamp(), statement_timestamp(), '
                    'statement_timestamp())'
                )
            )
            refused = await tighten_to_frozen(
                connection,
                backup_label='backup-x',
                operator_confirmation=confirmation_phrase('backup-x'),
            )
            assert isinstance(refused, TightenRefused)
            assert any(
                'root_workflow_id' in reason and 'parse' in reason
                for reason in refused.reasons
            ), refused.reasons
    finally:
        await engine.dispose()
