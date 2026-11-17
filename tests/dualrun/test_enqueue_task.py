# pyright: reportPrivateUsage=false
"""Dual-run of the enqueue switch: the insert, conflict read and key path against ``horsies_enqueue_task``.

Reference: ``tests/dualrun/reference/enqueue.py``. Candidate:
``PostgresBroker.enqueue_async`` with its send kind. The seed holds live rows
that earlier sends wrote (unkeyed, keyed, a workflow task with no
fingerprint) and tasks that finished. The sends cover a fresh row of each
start kind, a fresh keyed row, repeats and mismatches of a live row, a key
replay, a key conflict, a key on a row bound to another key, a key bound to
an unbound row, and sends to the finished tasks.

Each side reports, per send, its answer and the live rows of the sent id
after it. One difference is deliberate: a ``REPEAT`` send of a task that
finished reads history and answers SAME_COMMAND or PAYLOAD_MISMATCH, with no
live row. The old statements inserted a second live row, which ran the task
again. The reports pin it; the reference then deletes those second rows, so
the snapshots must be equal in everything else.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Final, assert_never
from uuid import UUID

import psycopg
import pytest

from horsies.core.brokers.enqueue_task import EnqueueRow, SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerEnqueueError,
    BrokerEnqueueFailed,
    BrokerEventLoopLimit,
    BrokerIdempotencyKeyConflict,
    BrokerInvalidInput,
    BrokerInvalidTaskOptions,
    BrokerPayloadMismatch,
)
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import enqueue as reference
from tests.dualrun.seeds import SeedCoverage, SeedStatus, TaskSeed, WorkflowLink, insert_workflow_task
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_KEY_WINDOW: Final = timedelta(hours=24)
_COMMAND: Final = '{"n": 1}'
_OTHER_COMMAND: Final = '{"n": 2}'


@dataclass(frozen=True, slots=True)
class _Send:
    """One send: its case name, the row it carries, and its kind."""

    case: str
    row: EnqueueRow
    kind: SendKind
    idempotency_key: str | None
    enqueued_at: datetime | None
    enqueue_delay_seconds: int | None


@dataclass(frozen=True, slots=True)
class _Seed:
    sends: tuple[_Send, ...]
    names_by_id: Mapping[UUID, str]
    """The case name that first used each task id."""
    repeats_of_finished: tuple[UUID, ...]
    """The ids of the finished tasks that a ``REPEAT`` send targets."""


def _send(
    case: str,
    task_name: str,
    task_id: UUID,
    sent_at: datetime,
    kind: SendKind,
    *,
    kwargs_json: str = _COMMAND,
    idempotency_key: str | None = None,
    enqueued_at: datetime | None = None,
    enqueue_delay_seconds: int | None = None,
) -> _Send:
    prepared = PostgresBroker._prepare_enqueue(
        task_id=task_id,
        task_name=task_name,
        queue_name='default',
        priority=50,
        input_json=kwargs_json,
        sent_at=sent_at,
        enqueued_at=enqueued_at,
        enqueue_delay_seconds=enqueue_delay_seconds,
        good_until=None,
        task_options=None,
        retention_class_key=DEFAULT_RETENTION_CLASS_KEY,
        retain_rerun_input=True,
        idempotency_key=idempotency_key,
        key_window=_KEY_WINDOW,
    )
    match prepared:
        case Ok(value):
            return _Send(case, value.row, kind, idempotency_key, enqueued_at, enqueue_delay_seconds)
        case Err(refused):
            raise AssertionError(f'the send of {case} was refused: {refused}')


def _database_now(connection: Connection) -> datetime:
    match connection.execute('SELECT now()').fetchone():
        case (datetime() as now,):
            return now
        case other:
            raise AssertionError(f'now() returned {other!r}')


def _cancel(connection: Connection, task_id: UUID) -> None:
    match connection.execute(
        'SELECT outcome FROM horsies_cancel_task(%s::uuid, FALSE)', (task_id,)
    ).fetchall():
        case [('APPLIED',)]:
            pass
        case other:
            raise AssertionError(f'the seed cancel of {task_id} returned {other!r}')


def _seed(connection: Connection) -> _Seed:
    now = _database_now(connection)
    sent_at = now - timedelta(hours=1)
    other_sent_at = now - timedelta(hours=2)
    ids = {
        name: mint_task_id()
        for name in (
            'enqueue.fresh',
            'enqueue.fresh_delayed',
            'enqueue.fresh_at',
            'enqueue.fresh_keyed',
            'enqueue.live',
            'enqueue.bind',
            'enqueue.keyed',
            'enqueue.keyed_replay',
            'enqueue.keyed_conflict',
            'history.first',
            'history.repeat',
            'history.repeat_other',
        )
    }
    first, repeat = SendKind.FIRST, SendKind.REPEAT

    def at(
        name: str,
        kind: SendKind = first,
        *,
        kwargs_json: str = _COMMAND,
        idempotency_key: str | None = None,
        enqueued_at: datetime | None = None,
        enqueue_delay_seconds: int | None = None,
    ) -> _Send:
        return _send(
            name, name, ids[name], sent_at, kind,
            kwargs_json=kwargs_json,
            idempotency_key=idempotency_key,
            enqueued_at=enqueued_at,
            enqueue_delay_seconds=enqueue_delay_seconds,
        )

    # Rows that earlier sends wrote, with the old statements.
    for written in (
        at('enqueue.live'),
        at('enqueue.bind'),
        at('enqueue.keyed', idempotency_key='key-1'),
        at('history.first'),
        at('history.repeat'),
        at('history.repeat_other'),
    ):
        answer = reference.send(connection, written.row)
        assert answer.outcome == 'INSERTED', f'the seed send of {written.case}: {answer}'
    for finished in ('history.first', 'history.repeat', 'history.repeat_other'):
        _cancel(connection, ids[finished])
    workflow = insert_workflow_task(
        connection,
        TaskSeed(task_name='enqueue.workflow', status=SeedStatus.PENDING),
        WorkflowLink(workflow_status='RUNNING', node_status='ENQUEUED'),
    )
    workflow_id = workflow.task_id

    live, keyed = ids['enqueue.live'], ids['enqueue.keyed']
    sends = (
        at('enqueue.fresh'),
        at('enqueue.fresh_delayed', enqueue_delay_seconds=60),
        at('enqueue.fresh_at', enqueued_at=now + timedelta(minutes=10)),
        at('enqueue.fresh_keyed', idempotency_key='key-2'),
        replace(at('enqueue.live'), case='live.same'),
        replace(at('enqueue.live', repeat), case='live.repeat'),
        _send('live.other_sent_at', 'enqueue.live', live, other_sent_at, first),
        _send(
            'live.keyed_other_sent_at', 'enqueue.live', live, other_sent_at, first,
            idempotency_key='key-4',
        ),
        _send(
            'keyed.replay', 'enqueue.keyed', ids['enqueue.keyed_replay'], sent_at, first,
            idempotency_key='key-1',
        ),
        _send(
            'keyed.conflict', 'enqueue.keyed', ids['enqueue.keyed_conflict'], sent_at, first,
            kwargs_json=_OTHER_COMMAND, idempotency_key='key-1',
        ),
        _send('keyed.other_key', 'enqueue.keyed', keyed, sent_at, first, idempotency_key='key-5'),
        replace(at('enqueue.bind', idempotency_key='key-3'), case='bind.key'),
        _send('workflow.unkeyed', 'enqueue.workflow', workflow_id, sent_at, first),
        at('history.first'),
        at('history.repeat', repeat),
        at('history.repeat_other', repeat, kwargs_json=_OTHER_COMMAND),
    )
    names_by_id = {task_id: name for name, task_id in ids.items()} | {
        workflow_id: 'enqueue.workflow'
    }
    return _Seed(
        sends=sends,
        names_by_id=names_by_id,
        repeats_of_finished=(ids['history.repeat'], ids['history.repeat_other']),
    )


_LIVE_ROWS_SQL: Final = 'SELECT count(*) FROM horsies_tasks WHERE id = %s::uuid'


def _live_rows(connection: Connection, task_id: UUID) -> int:
    match connection.execute(_LIVE_ROWS_SQL, (task_id,)).fetchone():
        case (int() as count,):
            return count
        case other:
            raise AssertionError(f'the live row count returned {other!r}')


def _reference_label(answer: reference.SendAnswer, seed: _Seed) -> str:
    holder = seed.names_by_id[answer.holder]
    match answer.outcome:
        case 'INSERTED' | 'SAME_COMMAND' | 'KEY_REPLAY':
            return f'ok {holder}'
        case 'KEY_CONFLICT':
            return f'key conflict, owner {holder}'
        case 'PAYLOAD_MISMATCH':
            return 'payload mismatch'
        case 'CONFLICT_UNVERIFIABLE':
            return 'enqueue failed'
        case other:
            raise AssertionError(f'unknown outcome {other!r}')


def _reference_sends(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    """The old statements per send; then the delete of the second live rows of
    the finished tasks, which the candidate does not write."""
    with psycopg.connect(database.url, autocommit=True) as connection:
        report: dict[str, str] = {}
        for each in seed.sends:
            label = _reference_label(reference.send(connection, each.row), seed)
            report[each.case] = f'{label}; live rows {_live_rows(connection, each.row.task_id)}'
        _ = connection.execute(
            'DELETE FROM horsies_tasks WHERE id = ANY(%s::uuid[])',
            (list(seed.repeats_of_finished),),
        )
        return report


def _candidate_label(result: Result[UUID, BrokerEnqueueError], seed: _Seed) -> str:
    match result:
        case Ok(task_id):
            return f'ok {seed.names_by_id[task_id]}'
        case Err(error):
            pass
    match error:
        case BrokerIdempotencyKeyConflict(owner_task_id=owner):
            return f'key conflict, owner {seed.names_by_id[owner]}'
        case BrokerPayloadMismatch():
            return 'payload mismatch'
        case BrokerEnqueueFailed():
            return 'enqueue failed'
        case BrokerInvalidTaskOptions() | BrokerInvalidInput() | BrokerEventLoopLimit():
            raise AssertionError(f'the send was refused: {error}')
        case _ as unreachable:
            assert_never(unreachable)


async def _candidate_sends_async(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    report: dict[str, str] = {}
    async with open_broker(database) as broker:
        for each in seed.sends:
            result = await broker.enqueue_async(
                each.row.task_name,
                each.row.queue_name,
                task_id=each.row.task_id,
                send_kind=each.kind,
                input_json=each.row.stored_input,
                priority=each.row.priority,
                sent_at=each.row.sent_at,
                enqueued_at=each.enqueued_at,
                enqueue_delay_seconds=each.enqueue_delay_seconds,
                good_until=each.row.good_until,
                task_options=each.row.task_options,
                retention_class_key=DEFAULT_RETENTION_CLASS_KEY,
                retain_rerun_input=each.row.retain_rerun_input,
                idempotency_key=each.idempotency_key,
            )
            with psycopg.connect(database.url, autocommit=True) as connection:
                live = _live_rows(connection, each.row.task_id)
            report[each.case] = f'{_candidate_label(result, seed)}; live rows {live}'
    return report


def _candidate_sends(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    return asyncio.run(_candidate_sends_async(database, seed))


def _ok(
    result: Result[OperationDualRun[dict[str, str], dict[str, str]], SelfCheckStepFailed],
) -> OperationDualRun[dict[str, str], dict[str, str]]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_enqueue_writes_and_answers_what_the_insert_and_key_path_did(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='insert, conflict read and key path', act=_reference_sends),
            OperationSide(name='horsies_enqueue_task', act=_candidate_sends),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {
        'enqueue.fresh': 'ok enqueue.fresh; live rows 1',
        'enqueue.fresh_delayed': 'ok enqueue.fresh_delayed; live rows 1',
        'enqueue.fresh_at': 'ok enqueue.fresh_at; live rows 1',
        'enqueue.fresh_keyed': 'ok enqueue.fresh_keyed; live rows 1',
        'live.same': 'ok enqueue.live; live rows 1',
        'live.repeat': 'ok enqueue.live; live rows 1',
        'live.other_sent_at': 'payload mismatch; live rows 1',
        'live.keyed_other_sent_at': 'payload mismatch; live rows 1',
        'keyed.replay': 'ok enqueue.keyed; live rows 0',
        'keyed.conflict': 'key conflict, owner enqueue.keyed; live rows 0',
        'keyed.other_key': 'payload mismatch; live rows 1',
        'bind.key': 'ok enqueue.bind; live rows 1',
        'workflow.unkeyed': 'payload mismatch; live rows 1',
        'history.first': 'ok history.first; live rows 1',
        'history.repeat': 'ok history.repeat; live rows 1',
        'history.repeat_other': 'ok history.repeat_other; live rows 1',
    }
    # The deliberate difference: a repeat of a finished task writes no row.
    assert run.candidate.report == run.reference.report | {
        'history.repeat': 'ok history.repeat; live rows 0',
        'history.repeat_other': 'payload mismatch; live rows 0',
    }
