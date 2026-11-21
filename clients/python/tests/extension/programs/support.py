"""Shared parts of the extension programs: the manifest of a handle, the
send request of a test task, a held lock on the task table, the log record
collectors and the bounded waits.

Importing this module imports ``horsies._core``. A case that measures the
process before that import imports this module after its measurement.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final
from urllib.parse import urlsplit, urlunsplit

import psutil
import psycopg
from pydantic import SecretStr

from horsies._catalog.manifest import SchemaMode, dump_producer_manifest
from horsies._core import (
    EnqueueRequest,
    IdMintRefused,
    NoTraceCarrier,
    StartNow,
    mint_task_id,
)
from horsies.core.history.identity.keys import IDEMPOTENCY_WINDOW_DEFAULT
from horsies.core.manifest import to_producer_manifest
from horsies.core.models.broker import PostgresConfig
from horsies.core.utils.url import to_psycopg_url
from tests.extension.programs.report import DATABASE_URL_VARIABLE, Report

TASK_NAME: Final = 'tests.extension.task'
"""The task name of every send of the extension cases. No worker serves it."""

TASK_QUEUE: Final = 'tests_extension'
"""The queue of every send of the extension cases. No worker serves it."""

POLL_INTERVAL_S: Final = 0.02
"""The pause between two checks of a bounded wait."""

LOCK_WAIT_BOUND_S: Final = 10.0
"""The longest wait for a backend to wait on the held task table lock."""

RECORD_WAIT_BOUND_S: Final = 10.0
"""The longest wait for a log record or a row that a core call writes."""

WAITING_BACKENDS_SQL: Final = """
    SELECT a.pid FROM pg_stat_activity a
    WHERE a.datname = current_database()
      AND a.pid <> pg_backend_pid()
      AND EXISTS (
          SELECT 1 FROM pg_locks l
          WHERE l.pid = a.pid
            AND NOT l.granted
            AND l.relation = 'horsies_tasks'::regclass
      )
"""
"""The backends of this database that wait for a lock on ``horsies_tasks``."""


def database_url() -> str:
    """The URL of the case's database, from ``DATABASE_URL_VARIABLE``.

    Raises:
        RuntimeError: the variable is not set. The test sets it for every case
            that needs a database, so this is a defect of the test.
    """
    url = os.environ.get(DATABASE_URL_VARIABLE, '')
    match url:
        case '':
            raise RuntimeError(f'{DATABASE_URL_VARIABLE} is not set: this case needs a database')
        case _:
            return url


def manifest_json(url: str, *, schema: SchemaMode, echo: bool) -> str:
    """The JSON text of the producer manifest of a handle for ``url``, with
    the default pool of ``PostgresConfig``. ``echo`` turns the statement log
    on. Pure."""
    config = PostgresConfig(database_url=SecretStr(url), echo=echo)
    manifest = to_producer_manifest(
        config,
        schema=schema,
        idempotency_reservation_window=IDEMPOTENCY_WINDOW_DEFAULT,
    )
    return dump_producer_manifest(manifest)


def address_url(url: str) -> str:
    """``url`` with its host replaced by one IP address of the host (IPv4
    first), so that the core's connect needs no host name lookup and no
    thread of the runtime's blocking pool. Python resolves the name here, on
    the calling thread.

    Raises:
        RuntimeError: the URL has no host, or the host has no address.
    """
    parts = urlsplit(url)
    host = parts.hostname
    match host:
        case None:
            raise RuntimeError('the database URL has no host')
        case _:
            pass
    port = parts.port
    found = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    ipv4 = [entry[4][0] for entry in found if entry[0] == socket.AF_INET]
    ipv6 = [entry[4][0] for entry in found if entry[0] == socket.AF_INET6]
    match (ipv4, ipv6):
        case ([address, *_], _):
            host_text = str(address)
        case ([], [address, *_]):
            host_text = f'[{address}]'
        case _:
            raise RuntimeError(f'the database host {host!r} has no address')
    user_part, _at, _host_part = parts.netloc.rpartition('@')
    netloc = host_text if port is None else f'{host_text}:{port}'
    if user_part:
        netloc = f'{user_part}@{netloc}'
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def os_thread_count() -> int:
    """The number of OS threads of this process."""
    return psutil.Process().num_threads()


def minted_task_id() -> uuid.UUID:
    """A task id from the core's UUIDv7 mint, as the Python client sends.

    Raises:
        RuntimeError: the core refused the mint.
    """
    match mint_task_id():
        case IdMintRefused(cause=cause):
            raise RuntimeError(f'the core refused a task id mint: {cause}')
        case uuid.UUID() as task_id:
            return task_id


def send_request(task_id: uuid.UUID) -> EnqueueRequest:
    """The first send of ``task_id`` as a task of ``TASK_NAME`` on
    ``TASK_QUEUE``: no input, no options, no key, the ``forever`` retention
    class, starting now."""
    return EnqueueRequest(
        task_id=task_id,
        task_name=TASK_NAME,
        queue_name=TASK_QUEUE,
        priority=100,
        input_json='{}',
        sent_at=datetime.now(UTC),
        start=StartNow(),
        good_until=None,
        task_options_json=None,
        idempotency_key=None,
        retention_class_key='forever',
        rerun_input='APP_DEFAULT',
        send_kind='FIRST',
        trace=NoTraceCarrier(),
    )


def task_row_exists(url: str, task_id: uuid.UUID) -> bool:
    """Whether ``horsies_tasks`` holds ``task_id``. Opens one connection."""
    with psycopg.connect(to_psycopg_url(url), autocommit=True) as connection:
        row = connection.execute(
            'SELECT count(*) FROM horsies_tasks WHERE id = %s',
            (task_id,),
        ).fetchone()
    match row:
        case (int() as count,):
            return count == 1
        case _:
            raise AssertionError(f'a count query gave {row!r}')


def delete_tasks(url: str, task_ids: Iterable[uuid.UUID]) -> None:
    """Delete the rows of ``task_ids`` from ``horsies_tasks``, so that the
    case leaves no task in the shared test database. Opens one connection."""
    with psycopg.connect(to_psycopg_url(url), autocommit=True) as connection:
        _ = connection.execute(
            'DELETE FROM horsies_tasks WHERE id = ANY(%s)',
            (list(task_ids),),
        )


class TasksTableLock:
    """An ACCESS EXCLUSIVE lock on ``horsies_tasks``, held by a connection of
    its own from ``take`` to ``release``. A core statement that reads or
    writes the table waits for it, so a case can keep a core call in flight
    for as long as it needs.

    A second connection reads which backends wait for the lock. Each
    connection is used by one thread at a time.
    """

    def __init__(self, url: str) -> None:
        self._holder = psycopg.connect(to_psycopg_url(url))
        self._observer = psycopg.connect(to_psycopg_url(url), autocommit=True)

    def take(self) -> None:
        """Take the lock in a transaction that stays open."""
        _ = self._holder.execute('LOCK TABLE horsies_tasks IN ACCESS EXCLUSIVE MODE')

    def release(self) -> None:
        """End the transaction, and with it the lock."""
        self._holder.rollback()

    def waiting_backends(self) -> frozenset[int]:
        """The process ids of the backends that wait for a lock on the table."""
        rows = self._observer.execute(WAITING_BACKENDS_SQL).fetchall()
        return frozenset(int(row[0]) for row in rows)

    def close(self) -> None:
        """Close both connections; a held lock ends with them."""
        self._holder.close()
        self._observer.close()


@dataclass(frozen=True)
class Waiting:
    """The backend ``pid`` waits for the task table lock."""

    pid: int


@dataclass(frozen=True)
class NoWaiter:
    """No new backend waited for the lock within ``bound_s``."""

    bound_s: float


type WaiterSeen = Waiting | NoWaiter


def wait_for_new_waiter(lock: TasksTableLock, known: frozenset[int]) -> WaiterSeen:
    """Wait up to ``LOCK_WAIT_BOUND_S`` for a backend outside ``known`` to
    wait for the lock."""
    deadline = time.monotonic() + LOCK_WAIT_BOUND_S
    while True:
        new = lock.waiting_backends() - known
        if new:
            return Waiting(pid=min(new))
        if time.monotonic() >= deadline:
            return NoWaiter(bound_s=LOCK_WAIT_BOUND_S)
        time.sleep(POLL_INTERVAL_S)


async def wait_for_new_waiter_async(lock: TasksTableLock, known: frozenset[int]) -> WaiterSeen:
    """``wait_for_new_waiter`` for a coroutine: the loop runs between checks."""
    deadline = time.monotonic() + LOCK_WAIT_BOUND_S
    while True:
        new = lock.waiting_backends() - known
        if new:
            return Waiting(pid=min(new))
        if time.monotonic() >= deadline:
            return NoWaiter(bound_s=LOCK_WAIT_BOUND_S)
        await asyncio.sleep(POLL_INTERVAL_S)


def waiter_report(seen: WaiterSeen) -> Report:
    """The report of one wait for a waiting backend."""
    match seen:
        case Waiting(pid=pid):
            return {'waited': True, 'pid': pid}
        case NoWaiter(bound_s=bound_s):
            return {'waited': False, 'bound_s': bound_s}


def wait_until(condition: Callable[[], bool], bound_s: float) -> bool:
    """Check ``condition`` every ``POLL_INTERVAL_S`` until it holds or
    ``bound_s`` passes; whether it held."""
    deadline = time.monotonic() + bound_s
    while True:
        if condition():
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(POLL_INTERVAL_S)


async def wait_until_async(condition: Callable[[], bool], bound_s: float) -> bool:
    """``wait_until`` for a coroutine: the loop runs between checks."""
    deadline = time.monotonic() + bound_s
    while True:
        if condition():
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(POLL_INTERVAL_S)


@dataclass(frozen=True)
class SeenRecord:
    """One log record that a collector handled, with the time it handled it."""

    logger: str
    level: str
    message: str
    created: float
    thread_name: str
    handled_at: float
    statement: str | None
    """The ``statement`` field of a statement record; None on other records."""

    def to_report(self) -> Report:
        return {
            'logger': self.logger,
            'level': self.level,
            'message': self.message,
            'created': self.created,
            'thread_name': self.thread_name,
            'handled_at': self.handled_at,
            'statement': self.statement,
        }


def seen_record(record: logging.LogRecord) -> SeenRecord:
    """The facts of ``record`` that the cases check, handled now."""
    statement: object = getattr(record, 'statement', None)
    return SeenRecord(
        logger=record.name,
        level=record.levelname,
        message=record.getMessage(),
        created=record.created,
        thread_name=str(record.threadName),
        handled_at=time.time(),
        statement=statement if isinstance(statement, str) else None,
    )


class RecordCollector(logging.Handler):
    """Keeps every record it handles. Safe to read from another thread."""

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self._seen: list[SeenRecord] = []
        self._seen_lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        seen = seen_record(record)
        with self._seen_lock:
            self._seen.append(seen)

    def records(self) -> list[SeenRecord]:
        """The records handled so far, in order."""
        with self._seen_lock:
            return list(self._seen)

    def messages(self) -> list[str]:
        """The messages of the records handled so far, in order."""
        return [record.message for record in self.records()]


def collect_records(logger_name: str, level: int) -> RecordCollector:
    """Set ``level`` on the logger ``logger_name`` and add a collector to it.
    A case calls it before its first core call: the core reads the levels of
    the ``horsies`` loggers when it starts."""
    collector = RecordCollector()
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.addHandler(collector)
    return collector
