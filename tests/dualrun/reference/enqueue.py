"""Enqueue, before ``horsies_enqueue_task``.

``PostgresBroker.enqueue_async`` (``horsies/core/brokers/postgres.py``) sent:

- an unkeyed send: ``ENQUEUE_INSERT_SQL`` in autocommit; a taken id then the
  read of the stored send (``_verify_enqueue_conflict``) in a second
  statement;
- a keyed send, in one session transaction: the key claim
  (``claim_key_reservation``), ``ENQUEUE_INSERT_SQL``, and on a taken id the
  locked read of the row and, for the same command on an unbound row, the
  binding of the key (``_bind_key_after_task_id_conflict``). A refusal
  rolled the transaction back.

Every send was the same: there was no send kind, and a resend of a task
that had moved to history inserted a new row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final, LiteralString, assert_never

import psycopg

from horsies.core.brokers.enqueue_task import (
    EnqueueRow,
    Keyed,
    StartAfterDelay,
    StartAt,
    StartNow,
    Unkeyed,
)
from horsies.core.history.identity.fingerprint import COMMAND_FINGERPRINT_VERSION
from tests.dualrun.capture import Connection

ENQUEUE_INSERT_SQL: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        idempotency_key_digest, id, task_name, queue_name, priority,
        input, status, sent_at, enqueued_at, good_until,
        retry_count, max_retries, task_options, execution,
        is_workflow_task, created_at, updated_at,
        command_fingerprint_version, command_fingerprint,
        retention_class_key, retain_rerun_input
    ) VALUES (
        %(idempotency_key_digest)s, %(id)s, %(task_name)s, %(queue_name)s, %(priority)s,
        %(input)s, 'PENDING', %(sent_at)s,
        COALESCE(
            %(enqueued_at)s,
            NOW() + COALESCE(%(enqueue_delay)s, INTERVAL '0 seconds')
        ),
        %(good_until)s,
        0, %(max_retries)s, %(task_options)s, %(execution)s,
        FALSE, NOW(), NOW(),
        %(command_fingerprint_version)s, %(command_fingerprint)s,
        %(retention_class_key)s, %(retain_rerun_input)s
    )
    ON CONFLICT (id) DO NOTHING
    RETURNING id
"""

READ_STORED_SEND_SQL: Final[LiteralString] = """
    SELECT command_fingerprint_version, command_fingerprint, sent_at
    FROM horsies_tasks
    WHERE id = %(id)s
"""

CLAIM_KEY_RESERVATION_SQL: Final[LiteralString] = """
    SELECT outcome, task_id, observed_fingerprint_version
    FROM horsies_key_reservation_claim(
        %(key_digest)s, CAST(%(key_scope_version)s AS smallint),
        make_interval(secs => %(window_seconds)s),
        CAST(%(fingerprint_version)s AS smallint), %(fingerprint)s,
        CAST(%(task_id)s AS uuid)
    )
"""

LOCK_STORED_SEND_SQL: Final[LiteralString] = """
    SELECT idempotency_key_digest, command_fingerprint_version, command_fingerprint, sent_at
    FROM horsies_tasks
    WHERE id = %(id)s
    FOR UPDATE
"""

BIND_KEY_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET idempotency_key_digest = %(key_digest)s
    WHERE id = CAST(%(task_id)s AS uuid)
"""


@dataclass(frozen=True, slots=True)
class SendAnswer:
    """What one send answered, in the outcome names of ``horsies_enqueue_task``.

    ``holder`` is the id that holds the key for KEY_REPLAY and KEY_CONFLICT,
    else the sent id.
    """

    outcome: str
    holder: str


def _insert_params(row: EnqueueRow) -> dict[str, Any]:
    match row.start:
        case StartNow():
            enqueued_at, delay = None, None
        case StartAt(at=at):
            enqueued_at, delay = at, None
        case StartAfterDelay(delay_s=delay_s):
            enqueued_at, delay = None, timedelta(seconds=delay_s)
        case _ as unreachable:
            assert_never(unreachable)
    match row.key:
        case Unkeyed():
            digest = None
        case Keyed(digest=key_digest):
            digest = key_digest
        case _ as unreachable:
            assert_never(unreachable)
    return {
        'idempotency_key_digest': digest,
        'id': row.task_id,
        'task_name': row.task_name,
        'queue_name': row.queue_name,
        'priority': row.priority,
        'input': row.stored_input,
        'sent_at': row.sent_at,
        'enqueued_at': enqueued_at,
        'enqueue_delay': delay,
        'good_until': row.good_until,
        'max_retries': row.max_retries,
        'task_options': row.task_options,
        'execution': row.execution.value,
        'command_fingerprint_version': COMMAND_FINGERPRINT_VERSION,
        'command_fingerprint': row.command_fingerprint,
        'retention_class_key': row.retention_class_key,
        'retain_rerun_input': row.retain_rerun_input,
    }


def _repeats(stored: tuple[Any, ...], row: EnqueueRow) -> bool:
    """``_repeats_stored_send``: the stored version-1 fingerprint and ``sent_at`` are the sent ones."""
    version, fingerprint, sent_at = stored
    return (
        version == COMMAND_FINGERPRINT_VERSION
        and fingerprint is not None
        and bytes(fingerprint) == row.command_fingerprint
        and sent_at == row.sent_at
    )


def _send_unkeyed(connection: Connection, row: EnqueueRow) -> SendAnswer:
    inserted = connection.execute(ENQUEUE_INSERT_SQL, _insert_params(row)).fetchone()
    if inserted is not None:
        return SendAnswer('INSERTED', row.task_id)
    stored = connection.execute(READ_STORED_SEND_SQL, {'id': row.task_id}).fetchone()
    if stored is None:
        return SendAnswer('CONFLICT_UNVERIFIABLE', row.task_id)
    if _repeats(stored, row):
        return SendAnswer('SAME_COMMAND', row.task_id)
    return SendAnswer('PAYLOAD_MISMATCH', row.task_id)


def _send_keyed_in_transaction(connection: Connection, row: EnqueueRow, key: Keyed) -> SendAnswer:
    """The keyed send. Raises ``psycopg.Rollback`` after it set the answer of a refusal."""
    claim = connection.execute(
        CLAIM_KEY_RESERVATION_SQL,
        {
            'key_digest': key.digest,
            'key_scope_version': key.scope_version,
            'window_seconds': key.window_s,
            'fingerprint_version': COMMAND_FINGERPRINT_VERSION,
            'fingerprint': row.command_fingerprint,
            'task_id': row.task_id,
        },
    ).fetchone()
    match claim:
        case ('APPLIED', _, _):
            pass
        case ('REPLAY', holder, _):
            return SendAnswer('KEY_REPLAY', str(holder))
        case ('CONFLICT', owner, _):
            return SendAnswer('KEY_CONFLICT', str(owner))
        case _:
            raise AssertionError(f'the key claim returned {claim!r}')
    inserted = connection.execute(ENQUEUE_INSERT_SQL, _insert_params(row)).fetchone()
    if inserted is not None:
        return SendAnswer('INSERTED', row.task_id)
    match connection.execute(LOCK_STORED_SEND_SQL, {'id': row.task_id}).fetchone():
        case None:
            return SendAnswer('CONFLICT_UNVERIFIABLE', row.task_id)
        case (bytes() | None as bound_digest, *send):
            pass
        case stored:
            raise AssertionError(f'the locked read returned {stored!r}')
    if not _repeats(tuple(send), row):
        return SendAnswer('PAYLOAD_MISMATCH', row.task_id)
    match bound_digest:
        case None:
            _ = connection.execute(BIND_KEY_SQL, {'key_digest': key.digest, 'task_id': row.task_id})
            return SendAnswer('SAME_COMMAND', row.task_id)
        case bytes() if bound_digest == key.digest:
            return SendAnswer('SAME_COMMAND', row.task_id)
        case bytes():
            return SendAnswer('PAYLOAD_MISMATCH', row.task_id)


def _send_keyed(connection: Connection, row: EnqueueRow, key: Keyed) -> SendAnswer:
    """One transaction, committed only for INSERTED and SAME_COMMAND."""
    answers: list[SendAnswer] = []
    with connection.transaction():
        answers.append(_send_keyed_in_transaction(connection, row, key))
        match answers[0].outcome:
            case 'INSERTED' | 'SAME_COMMAND':
                pass
            case _:
                raise psycopg.Rollback()
    (answer,) = answers
    return answer


def send(connection: Connection, row: EnqueueRow) -> SendAnswer:
    """One send as ``enqueue_async`` sent it, on an autocommit connection."""
    match row.key:
        case Unkeyed():
            return _send_unkeyed(connection, row)
        case Keyed() as key:
            return _send_keyed(connection, row, key)
        case _ as unreachable:
            assert_never(unreachable)
