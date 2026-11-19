"""The notification section of the capture against a real database."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import TypeVar

import psycopg
import pytest

from horsies.core.types.result import Err, Ok, Result
from tests.parity.snapshot.capture import (
    Notification,
    capture_with_notifications,
    collect_notifications,
    open_listener,
    read_database_clock,
)
from tests.parity.snapshot.catalog import NOTIFICATIONS
from tests.parity.snapshot.compare import ExtraRow, compare_snapshots
from tests.parity.snapshot.databases import DualRunDatabase, install_baseline_schema
from tests.parity.snapshot.normalize import NormalizedSnapshot
from tests.parity.snapshot.values import make_run_window

pytestmark = [pytest.mark.integration]

T = TypeVar('T')
E = TypeVar('E')

MakeDatabase = Callable[[], DualRunDatabase]


def _ok(result: Result[T, E], subject: str) -> T:
    match result:
        case Ok(value):
            return value
        case Err(error):
            pytest.fail(f'{subject}: {error}')


def _installed(make: MakeDatabase) -> DualRunDatabase:
    database = make()
    _ok(install_baseline_schema(database), f'install the baseline into {database.name}')
    return database


def _notify(
    connection: psycopg.Connection[tuple[object, ...]], channel: str, payload: str
) -> None:
    _ = connection.execute(
        'SELECT pg_notify(%(channel)s, %(payload)s)',
        {'channel': channel, 'payload': payload},
    ).fetchall()


def test_collect_reads_the_committed_notifications_up_to_the_marker(
    empty_database: MakeDatabase,
) -> None:
    database = _installed(empty_database)
    listener = _ok(
        open_listener(database.url, ('task_done', 'task_queue_default')), 'listen'
    )
    try:
        with (
            psycopg.connect(database.url) as writer,
            psycopg.connect(database.url) as sender,
        ):
            with writer.transaction():
                _notify(writer, 'task_done', 'first')
            with writer.transaction(force_rollback=True):
                _notify(writer, 'task_done', 'rolled back')
            with writer.transaction():
                _notify(writer, 'task_queue_default', 'new')
                _notify(writer, 'workflow_done', 'not listened')
            collected = collect_notifications(listener, sender)
            with writer.transaction():
                _notify(writer, 'task_done', 'after the marker')
            later = collect_notifications(listener, sender)
    finally:
        listener.connection.close()

    assert collected == Ok(
        (
            Notification(channel='task_done', payload='first'),
            Notification(channel='task_queue_default', payload='new'),
        )
    )
    assert later == Ok((Notification(channel='task_done', payload='after the marker'),))


def _captured(
    database: DualRunDatabase, notifications: tuple[Notification, ...]
) -> NormalizedSnapshot:
    with psycopg.connect(database.url) as connection:
        start = _ok(read_database_clock(connection), 'read the run start')
        end = _ok(read_database_clock(connection), 'read the run end')
        window = _ok(make_run_window(start, end), 'make the run window')
        return _ok(
            capture_with_notifications(connection, notifications, window), 'capture'
        )


def _sent(task_id: str, workflow_id: str) -> tuple[Notification, ...]:
    return (
        Notification(channel='task_new', payload=task_id),
        Notification(channel='task_queue_default', payload='new'),
        Notification(channel='horsies_workflow_status', payload=workflow_id),
        Notification(channel='task_done', payload=task_id),
    )


def test_notifications_with_other_ids_capture_equal_and_one_more_differs(
    empty_database: MakeDatabase,
) -> None:
    reference = _installed(empty_database)
    candidate = _installed(empty_database)
    reference_sent = _sent(str(uuid.uuid4()), str(uuid.uuid4()))
    candidate_sent = _sent(str(uuid.uuid4()), str(uuid.uuid4()))
    wake = Notification(channel='task_queue_default', payload='capacity')

    left = _captured(reference, reference_sent)
    right = _captured(candidate, candidate_sent)
    more = _captured(candidate, (*candidate_sent, wake))

    assert compare_snapshots(left, right).entries == ()
    extra = compare_snapshots(left, more).entries
    assert len(extra) == 1
    assert isinstance(extra[0], ExtraRow)
    assert extra[0].table == NOTIFICATIONS.name
