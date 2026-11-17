"""Enqueue: one send in one statement, ``horsies_enqueue_task``.

``enqueue_task_params`` gives the parameters of ``ENQUEUE_TASK_SQL`` for one
checked send (``EnqueueRow``) and its ``SendKind``. The function writes one
PENDING row, or answers what the row that holds the task id or the key holds:

- an unkeyed send inserts the row; a task id that a live row holds answers
  SAME_COMMAND when the stored command fingerprint and ``sent_at`` are the
  sent ones, else PAYLOAD_MISMATCH;
- a keyed send claims the key reservation first: a key that another send
  holds answers KEY_REPLAY (same command) or KEY_CONFLICT, with the id that
  holds the key; a keyed send on a live row of the same command binds its key
  to the row, and a row bound to another key is PAYLOAD_MISMATCH;
- a ``REPEAT`` send reads history first, under the task advisory lock: a task
  that finished answers SAME_COMMAND or PAYLOAD_MISMATCH by the same
  comparison, and never runs a second time.

A send is a first run, or the rerun of a finished task
(``horsies.core.history.rerun.operations.rerun_task``), which names its source
and its root. ``enqueue_result`` maps the outcome to the result of
``PostgresBroker.enqueue_async``. A send runs alone in autocommit; a rerun runs
in its caller's transaction. A refusal writes nothing and leaves no key
reservation of this send.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Final, assert_never
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

from horsies.core.brokers.result_types import (
    BrokerEnqueueFailed,
    BrokerIdempotencyKeyConflict,
    BrokerPayloadMismatch,
)
from horsies.core.history.identity.fingerprint import COMMAND_FINGERPRINT_VERSION
from horsies.core.models.tasks import Execution
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import uuid_column_value

ENQUEUE_TASK_SQL = text("""
    SELECT outcome, task_id FROM horsies_enqueue_task(
        CAST(:task_id AS uuid),
        :task_name,
        :queue_name,
        CAST(:priority AS INTEGER),
        :input,
        :task_options,
        :execution,
        :retention_class_key,
        CAST(:retain_rerun_input AS BOOLEAN),
        CAST(:max_retries AS INTEGER),
        CAST(:good_until AS TIMESTAMPTZ),
        CAST(:sent_at AS TIMESTAMPTZ),
        :start_kind,
        CAST(:start_at AS TIMESTAMPTZ),
        CAST(:start_delay_s AS INTEGER),
        CAST(:fingerprint_version AS SMALLINT),
        CAST(:fingerprint AS BYTEA),
        :lineage_kind,
        CAST(:rerun_of_task_id AS uuid),
        CAST(:rerun_root_task_id AS uuid),
        :key_kind,
        CAST(:key_digest AS BYTEA),
        CAST(:key_scope_version AS SMALLINT),
        CAST(:key_window_us AS BIGINT) * INTERVAL '1 microsecond',
        :send_kind
    )
""")

ENQUEUE_TASK_DRIVER_SQL: Final[str] = ENQUEUE_TASK_SQL.compile(
    dialect=PGDialect_psycopg(),
).string
"""``ENQUEUE_TASK_SQL`` as the psycopg dialect sends it (``%(name)s``
binds): the statement of a sync send on the caller's thread."""


_HOLDER_COLUMN: Final = 'horsies_enqueue_task.task_id'
"""The column of the id that holds a key, as ``uuid_column_value`` names it."""


class SendKind(Enum):
    """Whether a send is the first send of its task id or a repeat of it."""

    FIRST = 'FIRST'
    """The first send of a task id: the function does not read history."""

    REPEAT = 'REPEAT'
    """A send retry: the same task id, command and ``sent_at`` again. The
    function reads history first, so a task that finished answers with its
    id and does not run again."""


@dataclass(frozen=True, slots=True)
class StartNow:
    """The task is due at the database clock's now."""


@dataclass(frozen=True, slots=True)
class StartAt:
    """The task is due at ``at``."""

    at: datetime


@dataclass(frozen=True, slots=True)
class StartAfterDelay:
    """The task is due ``delay_s`` whole seconds after the database clock's now."""

    delay_s: int


type EnqueueStart = StartNow | StartAt | StartAfterDelay


@dataclass(frozen=True, slots=True)
class Unkeyed:
    """The send has no idempotency key."""


@dataclass(frozen=True, slots=True)
class Keyed:
    """The send reserves its scoped key digest for ``window`` after its task
    finishes. The window is sent as its count of microseconds and made into a
    time ``interval`` by the statement: exact, and with no day field, whose
    addition would depend on the session time zone."""

    digest: bytes
    scope_version: int
    window: timedelta


type EnqueueKey = Unkeyed | Keyed


@dataclass(frozen=True, slots=True)
class FirstRun:
    """The send is the first run of its command: the row has no lineage."""


@dataclass(frozen=True, slots=True)
class RerunOf:
    """The send reruns the finished task ``source_task_id``; ``root_task_id``
    is the first task of the lineage (the source itself when the source is no
    rerun)."""

    source_task_id: UUID
    root_task_id: UUID


type EnqueueLineage = FirstRun | RerunOf


@dataclass(frozen=True, slots=True)
class EnqueueRow:
    """The checked values of one send, as the row stores them."""

    task_id: UUID
    task_name: str
    queue_name: str
    priority: int
    stored_input: str
    task_options: str | None
    execution: Execution
    retention_class_key: str
    retain_rerun_input: bool
    max_retries: int
    good_until: datetime | None
    sent_at: datetime
    start: EnqueueStart
    command_fingerprint: bytes
    key: EnqueueKey
    lineage: EnqueueLineage


def _start_params(start: EnqueueStart) -> dict[str, Any]:
    match start:
        case StartNow():
            return {'start_kind': 'NOW', 'start_at': None, 'start_delay_s': None}
        case StartAt(at=at):
            return {'start_kind': 'AT', 'start_at': at, 'start_delay_s': None}
        case StartAfterDelay(delay_s=delay_s):
            return {'start_kind': 'AFTER_DELAY', 'start_at': None, 'start_delay_s': delay_s}
        case _ as unreachable:
            assert_never(unreachable)


def _key_params(key: EnqueueKey) -> dict[str, Any]:
    match key:
        case Unkeyed():
            return {
                'key_kind': 'UNKEYED',
                'key_digest': None,
                'key_scope_version': None,
                'key_window_us': None,
            }
        case Keyed(digest=digest, scope_version=scope_version, window=window):
            return {
                'key_kind': 'KEYED',
                'key_digest': digest,
                'key_scope_version': scope_version,
                'key_window_us': window // timedelta(microseconds=1),
            }
        case _ as unreachable:
            assert_never(unreachable)


def _lineage_params(lineage: EnqueueLineage) -> dict[str, Any]:
    match lineage:
        case FirstRun():
            return {
                'lineage_kind': 'FIRST_RUN',
                'rerun_of_task_id': None,
                'rerun_root_task_id': None,
            }
        case RerunOf(source_task_id=source_task_id, root_task_id=root_task_id):
            return {
                'lineage_kind': 'RERUN',
                'rerun_of_task_id': str(source_task_id),
                'rerun_root_task_id': str(root_task_id),
            }
        case _ as unreachable:
            assert_never(unreachable)


def enqueue_task_params(row: EnqueueRow, send_kind: SendKind) -> dict[str, Any]:
    """The parameters of ``ENQUEUE_TASK_SQL``, one entry per bind. Pure."""
    return {
        'task_id': str(row.task_id),
        'task_name': row.task_name,
        'queue_name': row.queue_name,
        'priority': row.priority,
        'input': row.stored_input,
        'task_options': row.task_options,
        'execution': row.execution.value,
        'retention_class_key': row.retention_class_key,
        'retain_rerun_input': row.retain_rerun_input,
        'max_retries': row.max_retries,
        'good_until': row.good_until,
        'sent_at': row.sent_at,
        **_start_params(row.start),
        'fingerprint_version': COMMAND_FINGERPRINT_VERSION,
        'fingerprint': row.command_fingerprint,
        **_lineage_params(row.lineage),
        **_key_params(row.key),
        'send_kind': send_kind.value,
    }


@dataclass(frozen=True, slots=True)
class Inserted:
    """The send wrote its row."""


@dataclass(frozen=True, slots=True)
class SameCommand:
    """The task id holds this send: a live row, or a task that finished (a
    ``REPEAT`` send). A keyed send bound its key to the live row."""


@dataclass(frozen=True, slots=True)
class KeyReplay:
    """The key is held by ``task_id`` with the same command."""

    task_id: UUID


@dataclass(frozen=True, slots=True)
class KeyConflict:
    """The key is held by ``owner_task_id`` with another command."""

    owner_task_id: UUID


@dataclass(frozen=True, slots=True)
class PayloadMismatch:
    """The task id holds another command or another ``sent_at``, or its live
    row is bound to another key."""


@dataclass(frozen=True, slots=True)
class ConflictUnverifiable:
    """The task id was taken, and its row moved to history before the
    comparison read it."""


type EnqueueOutcome = (
    Inserted | SameCommand | KeyReplay | KeyConflict | PayloadMismatch | ConflictUnverifiable
)


def read_enqueue_outcome(rows: Sequence[Sequence[Any]]) -> EnqueueOutcome:
    """The outcome of ``ENQUEUE_TASK_SQL`` from its rows. The function returns
    one row; anything else is a defect of the function (``AssertionError``).
    The id column arrives as ``UUID`` or as text, by driver setting. Pure."""
    match rows:
        case [(str() as outcome, UUID() | str() as task_id)]:
            pass
        case _:
            raise AssertionError(
                f'horsies_enqueue_task returned rows outside its contract: {list(rows)!r}',
            )
    match outcome:
        case 'INSERTED':
            return Inserted()
        case 'SAME_COMMAND':
            return SameCommand()
        case 'KEY_REPLAY':
            return KeyReplay(task_id=uuid_column_value(task_id, column=_HOLDER_COLUMN))
        case 'KEY_CONFLICT':
            return KeyConflict(
                owner_task_id=uuid_column_value(task_id, column=_HOLDER_COLUMN),
            )
        case 'PAYLOAD_MISMATCH':
            return PayloadMismatch()
        case 'CONFLICT_UNVERIFIABLE':
            return ConflictUnverifiable()
        case _:
            raise AssertionError(f'horsies_enqueue_task returned outcome {outcome!r}')


def enqueue_result(
    outcome: EnqueueOutcome,
    *,
    task_id: UUID,
    task_name: str,
) -> Result[
    UUID,
    BrokerEnqueueFailed | BrokerPayloadMismatch | BrokerIdempotencyKeyConflict,
]:
    """The result of ``enqueue_async`` for ``outcome``. Pure.

    ``Ok`` names the task that holds the send: the sent id, or the id that
    holds the key for ``KeyReplay``.
    """
    match outcome:
        case Inserted() | SameCommand():
            return Ok(task_id)
        case KeyReplay(task_id=holder):
            return Ok(holder)
        case KeyConflict(owner_task_id=owner):
            return Err(
                BrokerIdempotencyKeyConflict(
                    message=(
                        f'idempotency key for {task_name} is reserved by task '
                        f'{owner} with a different command'
                    ),
                    owner_task_id=owner,
                )
            )
        case PayloadMismatch():
            return Err(
                BrokerPayloadMismatch(
                    message=(
                        f'task_id {task_id} for {task_name} already holds another '
                        'command, another sent_at, or another idempotency key'
                    ),
                )
            )
        case ConflictUnverifiable():
            return Err(
                BrokerEnqueueFailed(
                    message=(
                        f'task_id {task_id} conflict detected but the row moved '
                        f'before verification for {task_name}; cannot verify '
                        'payload identity'
                    ),
                    retryable=False,
                    exception=NoCause(),
                )
            )
        case _ as unreachable:
            assert_never(unreachable)
