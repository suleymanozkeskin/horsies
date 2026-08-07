"""The cutover relocation against the real migrated schema.

Every case runs on a database installed by the production migration
chain — the exact pre-cutover shape a deployment is in — with legacy
terminal rows written the way 0.4.x left them: terminal statuses on
the live table, terminalization_kind recorded post-v19 or NULL before
it, attempts in the live attempts table, and the transitional envelope
columns populated as the preparation backfill leaves them.

Proven here: recorded provenance carries and absent provenance takes
LEGACY_TERMINAL, never a guess; the recorded administrative-cancel
rows take the ruled result-swap projection; FAILED/EXPIRED rows take
the last attempt's recorded reason; the idempotence probe skips
identities already in history; workflow-backing rows relocate WITHOUT
minting phase-2 pending; and the omission of live-path guards and
effects is structural — the rendered program contains no gate probe
and no notify.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.cutover.identity import (
    normalize_attempt_identity,
)
from horsies.core.history.cutover.relocation import (
    RELOCATION_LEDGER_DDL,
    RelocationBatch,
    RelocationComplete,
    relocation_insert_sql,
    relocate_terminal_batch,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.history.terminalization.move import ATTEMPT_ENCODER_DDL
from tests.integration.task_history_harness import prepare_move_storage
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_relocation'


async def _prepare(url: str) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()


async def install_program_state(connection: AsyncConnection) -> None:
    await prepare_move_storage(connection, CLASS_KEY)
    await connection.execute(text(RELOCATION_LEDGER_DDL))
    await normalize_attempt_identity(connection)
    # Stage-2 program the relocation consumes: the one attempt encoder.
    await connection.execute(text(ATTEMPT_ENCODER_DDL))


async def insert_legacy_task(
    connection: AsyncConnection,
    *,
    status: str,
    kind: str | None,
    result: str | None = None,
    error_code: str | None = None,
    is_workflow_task: bool = False,
    attempts: tuple[tuple[str, str | None], ...] = (),
    disposition: str | None = 'NEVER_ELIGIBLE',
    args_json: str | None = None,
    kwargs_json: str | None = None,
    task_options: str | None = None,
    retain: bool | None = False,
) -> str:
    """One row as 0.4.x left it, with the transitional columns in their
    post-backfill state."""
    task_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    terminal = status not in ('PENDING', 'CLAIMED', 'RUNNING')
    await connection.execute(
        text(
            """
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, status, result,
                args, kwargs, task_options,
                enqueued_at, created_at, started_at, claimed_at,
                terminal_at, terminalization_kind,
                retry_count, max_retries, error_code,
                claimed_by_worker_id, worker_hostname, worker_pid,
                worker_process_name, is_workflow_task, enqueue_sha,
                command_fingerprint_version, command_fingerprint,
                retention_class_key, retain_rerun_input,
                prepared_rerun_input_disposition
            ) VALUES (
                CAST(:task_id AS uuid), 'legacy.task', 'default', 50,
                :status, :result,
                :args_json, :kwargs_json, :task_options,
                :enqueued_at, :enqueued_at, :started_at, :started_at,
                :terminal_at, :kind,
                0, 0, :error_code,
                'legacy-worker', 'legacy-host', 4242, 'legacy-proc',
                :is_workflow_task, :enqueue_sha,
                1, :fingerprint,
                :class_key, :retain, :disposition
            )
            """
        ),
        {
            'task_id': task_id,
            'status': status,
            'result': result,
            'enqueued_at': now - timedelta(hours=2),
            'started_at': now - timedelta(hours=1),
            'terminal_at': (
                now - timedelta(minutes=30) if terminal else None
            ),
            'kind': kind,
            'error_code': error_code,
            'is_workflow_task': is_workflow_task,
            'enqueue_sha': 'a' * 64,
            'fingerprint': uuid.uuid4().bytes + uuid.uuid4().bytes,
            'class_key': CLASS_KEY,
            'retain': retain,
            'disposition': disposition,
            'args_json': args_json,
            'kwargs_json': kwargs_json,
            'task_options': task_options,
        },
    )
    for attempt_number, (outcome, failed_reason) in enumerate(
        attempts, start=1
    ):
        await connection.execute(
            text(
                'INSERT INTO horsies_task_attempts '
                '(task_id, attempt, outcome, will_retry, started_at, '
                'finished_at, failed_reason) VALUES '
                '(CAST(:task_id AS uuid), :attempt, :outcome, FALSE, '
                'statement_timestamp(), statement_timestamp(), :reason)'
            ),
            {
                'task_id': task_id,
                'attempt': attempt_number,
                'outcome': outcome,
                'reason': failed_reason,
            },
        )
    return task_id


async def relocate_all(
    connection: AsyncConnection, *, batch_size: int = 2
) -> RelocationComplete:
    while True:
        outcome = await relocate_terminal_batch(
            connection, batch_size=batch_size
        )
        if isinstance(outcome, RelocationComplete):
            return outcome
        assert isinstance(outcome, RelocationBatch)


class TestStructuralOmissions:
    def test_the_rendered_program_omits_guards_and_effects(self) -> None:
        rendered = relocation_insert_sql()
        assert 'pg_notify' not in rendered
        assert 'horsies_assert_archive_available' not in rendered
        assert 'pg_advisory' not in rendered
        assert 'horsies_workflow_phase2_pending' not in rendered
        assert 'key_reservation' not in rendered


class TestRelocation:
    @pytest.mark.asyncio
    async def test_mixed_population_relocates_faithfully(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                completed = await insert_legacy_task(
                    connection,
                    status='COMPLETED',
                    kind='COMPLETE_LOCKED',
                    result='{"ok": true}',
                )
                failed = await insert_legacy_task(
                    connection,
                    status='FAILED',
                    kind=None,
                    result='{"error": {"code": "BOOM"}}',
                    error_code='BOOM',
                    attempts=(
                        ('FAILED', 'first blew up'),
                        ('FAILED', 'terminal reason'),
                    ),
                )
                cancelled = await insert_legacy_task(
                    connection,
                    status='CANCELLED',
                    kind='CANCEL_ADMIN',
                    result='{"partial": 1}',
                )
                expired = await insert_legacy_task(
                    connection, status='EXPIRED', kind=None
                )
                live = await insert_legacy_task(
                    connection, status='PENDING', kind=None
                )

                outcome = await relocate_all(connection)
                assert outcome.rows_relocated == 4
                assert outcome.batches_committed == 2

                rows = {
                    str(row.task_id): row
                    for row in (
                        await connection.execute(
                            text(
                                'SELECT task_id, status, '
                                'terminalization_kind, result_payload, '
                                'prior_result_payload, '
                                'final_failed_reason, error_code '
                                'FROM horsies_task_history'
                            )
                        )
                    ).all()
                }
                assert set(rows) == {completed, failed, cancelled, expired}

                assert (
                    rows[completed].terminalization_kind
                    == 'COMPLETE_LOCKED'
                )
                assert rows[completed].final_failed_reason is None

                assert (
                    rows[failed].terminalization_kind == 'LEGACY_TERMINAL'
                )
                assert (
                    rows[failed].final_failed_reason == 'terminal reason'
                )
                assert rows[failed].error_code == 'BOOM'

                assert (
                    rows[cancelled].terminalization_kind == 'CANCEL_ADMIN'
                )
                assert rows[cancelled].result_payload is None
                assert (
                    bytes(rows[cancelled].prior_result_payload)
                    == b'{"partial": 1}'
                )

                assert (
                    rows[expired].terminalization_kind == 'LEGACY_TERMINAL'
                )

                remaining = (
                    await connection.execute(
                        text(
                            'SELECT id, status FROM horsies_tasks'
                        )
                    )
                ).all()
                assert [(str(r.id), r.status) for r in remaining] == [
                    (live, 'PENDING')
                ]
                orphan_attempts = (
                    await connection.execute(
                        text('SELECT count(*) FROM horsies_task_attempts')
                    )
                ).scalar_one()
                assert int(orphan_attempts) == 0

                ledger = (
                    await connection.execute(
                        text(
                            'SELECT rows_relocated, legacy_kind_rows '
                            'FROM horsies_cutover_relocation_ledger '
                            'ORDER BY batch_number'
                        )
                    )
                ).all()
                assert [(r.rows_relocated) for r in ledger] == [2, 2]
                assert sum(r.legacy_kind_rows for r in ledger) == 2
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_probe_skips_identities_already_in_history(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                relocated = await insert_legacy_task(
                    connection, status='COMPLETED', kind='COMPLETE_LOCKED'
                )
                first = await relocate_all(connection)
                assert first.rows_relocated == 1
                # The same identity terminal on live again — the state a
                # resumed run sees after a crash between insert and
                # delete. The probe must skip it, never double-insert.
                await insert_legacy_task(
                    connection, status='COMPLETED', kind='COMPLETE_LOCKED'
                )
                await connection.execute(
                    text(
                        'UPDATE horsies_tasks SET id = CAST(:t AS uuid) '
                        "WHERE task_name = 'legacy.task'"
                    ),
                    {'t': relocated},
                )
                resumed = await relocate_terminal_batch(
                    connection, batch_size=10
                )
                assert isinstance(resumed, RelocationComplete)
                history_count = (
                    await connection.execute(
                        text(
                            'SELECT count(*) FROM horsies_task_history '
                            'WHERE task_id = CAST(:t AS uuid)'
                        ),
                        {'t': relocated},
                    )
                ).scalar_one()
                assert int(history_count) == 1
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_workflow_row_relocates_without_minting_pending(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _prepare(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await install_program_state(connection)
                task_id = await insert_legacy_task(
                    connection,
                    status='COMPLETED',
                    kind=None,
                    result='{"ok": true}',
                    is_workflow_task=True,
                )
                workflow_id = str(uuid.uuid4())
                await connection.execute(
                    text(
                        'INSERT INTO horsies_workflows '
                        '(id, name, status, on_error, depth, created_at, '
                        'updated_at, sent_at) '
                        "VALUES (:w, 'legacy-wf', 'COMPLETED', 'fail', 0, "
                        'statement_timestamp(), statement_timestamp(), '
                        'statement_timestamp())'
                    ),
                    {'w': workflow_id},
                )
                await connection.execute(
                    text(
                        'INSERT INTO horsies_workflow_tasks '
                        '(id, workflow_id, task_id, task_index, task_name, '
                        'queue_name, priority, dependencies, '
                        'allow_failed_deps, join_type, status, '
                        'is_subworkflow, created_at) VALUES '
                        "(:n, :w, :t, 0, 'legacy.task', 'default', 50, "
                        "'{}', FALSE, 'all', 'COMPLETED', FALSE, "
                        'statement_timestamp())'
                    ),
                    {
                        'n': str(uuid.uuid4()),
                        'w': workflow_id,
                        't': task_id,
                    },
                )
                outcome = await relocate_all(connection)
                assert outcome.rows_relocated == 1
                row = (
                    await connection.execute(
                        text(
                            'SELECT workflow_id, is_workflow_task '
                            'FROM horsies_task_history '
                            'WHERE task_id = CAST(:t AS uuid)'
                        ),
                        {'t': task_id},
                    )
                ).one()
                assert str(row.workflow_id) == workflow_id
                assert row.is_workflow_task is True
                pending = (
                    await connection.execute(
                        text(
                            'SELECT count(*) FROM '
                            'horsies_workflow_phase2_pending'
                        )
                    )
                ).scalar_one()
                assert int(pending) == 0
        finally:
            await engine.dispose()
