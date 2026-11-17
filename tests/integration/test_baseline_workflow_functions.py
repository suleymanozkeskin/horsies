"""The workflow engine functions of the baseline (0005_workflow_functions.sql).

The single-session cases are SQL (baseline_workflow_functions.sql, with the
helpers of baseline_workflow_fixtures.sql); this file runs them in one rolled
back transaction. The two-session cases check the lock order: one session holds
a step open, a second session runs a conflicting step and waits on a lock, the
first commits, and the second finishes with the expected outcome and no
deadlock error. Each test builds its own database (``isolated_test_database``).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import psycopg
import pytest
import pytest_asyncio

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

_FIXTURES_SQL: Final = Path(__file__).with_name('baseline_workflow_fixtures.sql')
_CASES_SQL: Final = Path(__file__).with_name('baseline_workflow_functions.sql')

_LOCK_WAIT_POLLS: Final = 200
"""Polls of the waiting session's wait event before the case fails."""

_LOCK_WAIT_POLL_INTERVAL_S: Final = 0.025

_CONTENDER_TIMEOUT_S: Final = 10.0
"""Bound on the second session after the first committed."""

_WAIT_EVENT_SQL: Final = 'SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s'


def _migration() -> BaselineMigration:
    return load_baseline_migration()


@pytest_asyncio.fixture
async def workflow_database() -> AsyncGenerator[str]:
    """The conninfo of a fresh database with the baseline and the fixture helpers committed."""
    migration = _migration()
    async with isolated_test_database(DB_URL) as url:
        conninfo = to_psycopg_url(url)
        connection = await psycopg.AsyncConnection.connect(conninfo)
        try:
            # No parameters: psycopg sends each text with the simple query
            # protocol, which runs every statement of it.
            async with connection.transaction():
                await connection.execute(migration.sql.encode('utf-8'))
                await connection.execute(
                    _FIXTURES_SQL.read_text(encoding='utf-8').encode('utf-8')
                )
        finally:
            await connection.close()
        yield conninfo


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_function_cases_pass(workflow_database: str) -> None:
    connection = await psycopg.AsyncConnection.connect(workflow_database)
    try:
        async with connection.transaction(force_rollback=True):
            await connection.execute(
                _CASES_SQL.read_text(encoding='utf-8').encode('utf-8')
            )
    finally:
        await connection.close()


@dataclass(frozen=True, slots=True)
class LockOrderCase:
    """One two-session case. ``hold`` runs in an open transaction of the first
    session; ``contend`` runs in the second session while the first holds its
    locks; ``check`` runs after both. Each of the three is one statement."""

    name: str
    setup: str
    hold: str
    held: tuple[object, ...]
    contend: str
    contended: tuple[object, ...]
    check: str | None
    checked: tuple[object, ...] | None


_ROOT_START: Final = "SELECT count(*) FROM wft_start1('{id}', 'fail', '{nodes}')"

_CLAIMED_AT: Final = '2030-01-01 00:00:00+00'
"""The claim generation of the claimed task of the pause cases."""

_AUDIT_TOKEN: Final = '00000000-0000-7000-8000-00000000a0ee'

_PAUSED_CLAIMED: Final = (
    "SELECT count(*) FROM wft_start1('{id}', 'fail', '[{{\"idx\":0}}]')"
    "; SELECT horsies_workflow_pause('{id}')"
    f"; SELECT wft_claim(wft_task_of('{{id}}', 0), '{_CLAIMED_AT}')"
)
"""A one-node workflow, PAUSED, whose task a worker claimed after the pause."""

_RESUME: Final = (
    "SELECT disposition FROM horsies_workflow_resume('{id}') WHERE row_kind = 'OUTCOME'"
)

_TASK_AND_NODE: Final = (
    "SELECT t.status || ',' || n.status FROM horsies_workflow_tasks n "
    "JOIN horsies_tasks t ON t.id = n.task_id WHERE n.workflow_id = '{id}'"
)

LOCK_ORDER_CASES: Final = (
    LockOrderCase(
        name='a second start with the same id waits for the first, then sees EXISTS',
        setup='SELECT 1',
        hold=(
            "SELECT disposition FROM wft_start1('00000000-0000-7000-8000-00000000a001', 'fail', "
            "'[{\"idx\":0}]') WHERE row_kind = 'OUTCOME'"
        ),
        held=('STARTED',),
        contend=(
            "SELECT disposition, detail FROM wft_start1('00000000-0000-7000-8000-00000000a001', 'fail', "
            "'[{\"idx\":0}]') WHERE row_kind = 'OUTCOME'"
        ),
        contended=('EXISTS', 'wf'),
        check=None,
        checked=None,
    ),
    LockOrderCase(
        name='a cancel waits for a phase 2 that holds the workflow row, then cancels the tree',
        setup=(
            _ROOT_START.format(
                id='00000000-0000-7000-8000-00000000a011',
                nodes='[{"idx":0},{"idx":1},{"idx":2,"deps":[0]}]',
            )
            + "; SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a011', 0), wft_ok('1'))"
        ),
        hold=(
            'SELECT disposition FROM horsies_workflow_node_finish('
            "wft_task_of('00000000-0000-7000-8000-00000000a011', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"
        ),
        held=('APPLIED',),
        contend=(
            'SELECT outcome, cardinality(cancelled_task_ids) '
            "FROM horsies_workflow_cancel('00000000-0000-7000-8000-00000000a011')"
        ),
        contended=('CANCELLED', 2),
        check=(
            "SELECT string_agg(task_index || '=' || status, ',' ORDER BY task_index) FROM horsies_workflow_tasks "
            "WHERE workflow_id = '00000000-0000-7000-8000-00000000a011'"
        ),
        checked=('0=COMPLETED,1=SKIPPED,2=SKIPPED',),
    ),
    LockOrderCase(
        name='a root pause waits for a child phase 2 that pauses the child and its child, in (depth, id) order',
        setup=(
            'SELECT count(*) FROM wft_start('
            '\'[{"id":"00000000-0000-7000-8000-00000000a021","on_error":"fail"},'
            '{"id":"00000000-0000-7000-8000-00000000a022","on_error":"pause",'
            '"parent":"00000000-0000-7000-8000-00000000a021","parent_index":0,"depth":1},'
            '{"id":"00000000-0000-7000-8000-00000000a023","on_error":"fail",'
            '"parent":"00000000-0000-7000-8000-00000000a022","parent_index":1,"depth":2}]\', '
            '\'[{"idx":0,"sub":true},{"wf":"00000000-0000-7000-8000-00000000a022","idx":0},'
            '{"wf":"00000000-0000-7000-8000-00000000a022","idx":1,"sub":true},'
            '{"wf":"00000000-0000-7000-8000-00000000a023","idx":0}]\')'
            "; SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a022', 0), wft_err('USER'))"
        ),
        hold=(
            'SELECT disposition FROM horsies_workflow_node_finish('
            "wft_task_of('00000000-0000-7000-8000-00000000a022', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"
        ),
        held=('APPLIED',),
        contend=(
            'SELECT outcome, cardinality(paused_workflow_ids) '
            "FROM horsies_workflow_pause('00000000-0000-7000-8000-00000000a021')"
        ),
        contended=('PAUSED', 1),
        check=(
            "SELECT string_agg(depth || '=' || status, ',' ORDER BY depth) FROM horsies_workflows "
            "WHERE root_workflow_id = '00000000-0000-7000-8000-00000000a021'"
        ),
        checked=('0=PAUSED,1=PAUSED,2=PAUSED',),
    ),
    LockOrderCase(
        name='a phase 2 waits for the raise record on the workflow row, then applies the record',
        setup=(
            _ROOT_START.format(
                id='00000000-0000-7000-8000-00000000a031', nodes='[{"idx":0}]'
            )
            + "; SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a031', 0), wft_ok('1'))"
        ),
        hold=(
            'SELECT outcome FROM horsies_workflow_phase2_record_raise('
            "wft_task_of('00000000-0000-7000-8000-00000000a031', 0), 25, '{}', '{}')"
        ),
        held=('COUNTED',),
        contend=(
            'SELECT disposition FROM horsies_workflow_node_finish('
            "wft_task_of('00000000-0000-7000-8000-00000000a031', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"
        ),
        contended=('APPLIED',),
        check=None,
        checked=None,
    ),
    LockOrderCase(
        name='a child phase 2 waits for a pause that holds the child row, then promotes nothing',
        setup=(
            'SELECT count(*) FROM wft_start('
            '\'[{"id":"00000000-0000-7000-8000-00000000a041","on_error":"fail"},'
            '{"id":"00000000-0000-7000-8000-00000000a042","on_error":"fail",'
            '"parent":"00000000-0000-7000-8000-00000000a041","parent_index":0,"depth":1}]\', '
            '\'[{"idx":0,"sub":true},{"wf":"00000000-0000-7000-8000-00000000a042","idx":0},'
            '{"wf":"00000000-0000-7000-8000-00000000a042","idx":1,"deps":[0]}]\')'
            "; SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a042', 0), wft_ok('1'))"
        ),
        hold="SELECT outcome FROM horsies_workflow_pause('00000000-0000-7000-8000-00000000a041')",
        held=('PAUSED',),
        contend=(
            'SELECT disposition FROM horsies_workflow_node_finish('
            "wft_task_of('00000000-0000-7000-8000-00000000a042', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"
        ),
        contended=('APPLIED',),
        check=(
            "SELECT string_agg(task_index || '=' || status, ',' ORDER BY task_index) FROM horsies_workflow_tasks "
            "WHERE workflow_id = '00000000-0000-7000-8000-00000000a042'"
        ),
        checked=('0=COMPLETED,1=PENDING',),
    ),
    LockOrderCase(
        name='a claim-time settle waits for a resume that holds the workflow row, then dispatches the task',
        setup=_PAUSED_CLAIMED.format(id='00000000-0000-7000-8000-00000000a051'),
        hold=_RESUME.format(id='00000000-0000-7000-8000-00000000a051'),
        held=('RESUMED',),
        contend=(
            'SELECT disposition FROM horsies_workflow_claimed_tasks_settle('
            "ARRAY[wft_task_of('00000000-0000-7000-8000-00000000a051', 0)], "
            f"ARRAY['{_CLAIMED_AT}'::timestamptz], 'worker-1')"
        ),
        contended=('DISPATCH',),
        check=_TASK_AND_NODE.format(id='00000000-0000-7000-8000-00000000a051'),
        checked=('CLAIMED,ENQUEUED',),
    ),
    LockOrderCase(
        name='a stop before start waits for a resume that holds the workflow row, then reports RESUMED',
        setup=_PAUSED_CLAIMED.format(id='00000000-0000-7000-8000-00000000a061'),
        hold=_RESUME.format(id='00000000-0000-7000-8000-00000000a061'),
        held=('RESUMED',),
        contend=(
            'SELECT outcome FROM horsies_workflow_task_stop_before_start('
            "wft_task_of('00000000-0000-7000-8000-00000000a061', 0), 'worker-1', "
            f"'{_CLAIMED_AT}', '{{}}', TRUE)"
        ),
        contended=('RESUMED',),
        check=_TASK_AND_NODE.format(id='00000000-0000-7000-8000-00000000a061'),
        checked=('CLAIMED,ENQUEUED',),
    ),
    LockOrderCase(
        name='a paused expiry waits for a resume that holds the workflow row, then expires nothing',
        setup=(
            _ROOT_START.format(id='00000000-0000-7000-8000-00000000a071', nodes='[{"idx":0}]')
            + "; SELECT horsies_workflow_pause('00000000-0000-7000-8000-00000000a071')"
            + "; UPDATE horsies_workflows SET updated_at = now() - interval '2 hours'"
            + " WHERE id = '00000000-0000-7000-8000-00000000a071'"
        ),
        hold=_RESUME.format(id='00000000-0000-7000-8000-00000000a071'),
        held=('RESUMED',),
        contend=(
            'SELECT outcome, observed_status FROM horsies_workflow_expire_paused('
            "'00000000-0000-7000-8000-00000000a071', 3600000000, '{}')"
        ),
        contended=('NOT_EXPIRED', 'RUNNING'),
        check=_TASK_AND_NODE.format(id='00000000-0000-7000-8000-00000000a071'),
        checked=('PENDING,ENQUEUED',),
    ),
    LockOrderCase(
        name='a pause waits for a phase 2 and its child start in one transaction, then pauses the new child',
        setup=(
            _ROOT_START.format(
                id='00000000-0000-7000-8000-00000000a081',
                nodes='[{"idx":0},{"idx":1,"deps":[0],"sub":true}]',
            )
            + "; SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a081', 0), wft_ok('1'))"
        ),
        hold=(
            "SELECT wft_finish_with_child('00000000-0000-7000-8000-00000000a081', 0, "
            "'00000000-0000-7000-8000-00000000a082')"
        ),
        held=('STARTED',),
        contend=(
            'SELECT outcome, cardinality(paused_workflow_ids) '
            "FROM horsies_workflow_pause('00000000-0000-7000-8000-00000000a081')"
        ),
        contended=('PAUSED', 2),
        check=(
            "SELECT string_agg(depth || '=' || status, ',' ORDER BY depth) FROM horsies_workflows "
            "WHERE root_workflow_id = '00000000-0000-7000-8000-00000000a081'"
        ),
        checked=('0=PAUSED,1=PAUSED',),
    ),
    LockOrderCase(
        name='a recovery completion waits for a phase 2 that completes the workflow, then changes nothing',
        setup=(
            _ROOT_START.format(id='00000000-0000-7000-8000-00000000a091', nodes='[{"idx":0}]')
            + "; SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a091', 0), wft_ok('1'))"
            + '; '
            + _ROOT_START.format(id='00000000-0000-7000-8000-00000000a092', nodes='[{"idx":0}]')
            + "; UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('2'), completed_at = now()"
            + " WHERE workflow_id = '00000000-0000-7000-8000-00000000a092'"
            + '; SELECT outcome FROM horsies_workflow_recovery_audit_page('
            + f"1000, '{_AUDIT_TOKEN}', 300000)"
        ),
        hold=(
            'SELECT disposition FROM horsies_workflow_node_finish('
            "wft_task_of('00000000-0000-7000-8000-00000000a091', 0), TRUE, 'NONE') WHERE row_kind = 'OUTCOME'"
        ),
        held=('APPLIED',),
        contend=(
            'SELECT outcome, workflow_status FROM horsies_workflow_recover_completion('
            f"'00000000-0000-7000-8000-00000000a091', '{_AUDIT_TOKEN}', 300000)"
        ),
        contended=('UNCHANGED', 'COMPLETED'),
        check=None,
        checked=None,
    ),
    LockOrderCase(
        name='a recovery raise record waits for a phase 2 on the workflow row, then counts',
        setup=(
            _ROOT_START.format(
                id='00000000-0000-7000-8000-00000000a0a1', nodes='[{"idx":0},{"idx":1,"deps":[0]}]'
            )
            + "; SELECT wft_phase1(wft_task_of('00000000-0000-7000-8000-00000000a0a1', 0), wft_ok('1'))"
        ),
        hold=(
            'SELECT disposition FROM horsies_workflow_node_finish('
            "wft_task_of('00000000-0000-7000-8000-00000000a0a1', 0), TRUE, 'READ') WHERE row_kind = 'OUTCOME'"
        ),
        held=('APPLIED',),
        contend=(
            'SELECT outcome, failed_passes FROM horsies_workflow_recovery_record_raise('
            "'00000000-0000-7000-8000-00000000a0a1', '00000000-0000-7000-8000-00000000a0f1', "
            "'pending_terminal_deps', 1, 'KeyError', 'boom', 25)"
        ),
        contended=('COUNTED', 1),
        check=None,
        checked=None,
    ),
)


async def _one_row(
    connection: psycopg.AsyncConnection[tuple[object, ...]],
    statement: str,
) -> tuple[object, ...]:
    cursor = await connection.execute(statement.encode('utf-8'))
    rows = await cursor.fetchall()
    assert len(rows) == 1, f'{statement!r} returned {len(rows)} rows'
    return rows[0]


async def _wait_until_blocked(
    observer: psycopg.AsyncConnection[tuple[object, ...]],
    backend_pid: int,
) -> None:
    for _ in range(_LOCK_WAIT_POLLS):
        cursor = await observer.execute(_WAIT_EVENT_SQL, (backend_pid,))
        row = await cursor.fetchone()
        if row is not None and row[0] == 'Lock':
            return
        await asyncio.sleep(_LOCK_WAIT_POLL_INTERVAL_S)
    pytest.fail(f'session {backend_pid} never waited on a lock')


@pytest.mark.parametrize(
    'case', LOCK_ORDER_CASES, ids=[case.name for case in LOCK_ORDER_CASES]
)
@pytest.mark.asyncio(loop_scope='function')
async def test_lock_order(workflow_database: str, case: LockOrderCase) -> None:
    setup = await psycopg.AsyncConnection.connect(workflow_database, autocommit=True)
    holder = await psycopg.AsyncConnection.connect(workflow_database)
    contender = await psycopg.AsyncConnection.connect(
        workflow_database, autocommit=True
    )
    observer = await psycopg.AsyncConnection.connect(workflow_database, autocommit=True)
    try:
        await setup.execute(case.setup.encode('utf-8'))
        assert await _one_row(holder, case.hold) == case.held
        contended = asyncio.create_task(_one_row(contender, case.contend))
        await _wait_until_blocked(observer, contender.info.backend_pid)
        await holder.commit()
        assert (
            await asyncio.wait_for(contended, timeout=_CONTENDER_TIMEOUT_S)
            == case.contended
        )
        match (case.check, case.checked):
            case (None, None):
                pass
            case (str() as check, tuple() as checked):
                assert await _one_row(observer, check) == checked
            case _:
                pytest.fail(f'case {case.name!r} sets one of check and checked')
    finally:
        for connection in (setup, holder, contender, observer):
            await connection.close()
