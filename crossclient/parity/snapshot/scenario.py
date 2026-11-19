"""One logical database state, written with fresh ids and the database clock.

Two databases that each run ``write_scenario`` hold the same logical state
with different ids, different times, and (with ``RowOrder.REVERSED``) a
different physical row order. The state covers every rule kind of the
catalog: labelled ids and references across tables, ranked and windowed
clock values, anchored offsets, ignored samples and a history row read
through its partition parent.

Timeline, in milliseconds after the base time read from the database:
0 workflow start; 10-30 task ``extract`` runs and moves to history; 40
task ``load`` is claimed; 50-70 task ``send_invoice`` fails attempt 1 and
waits for a retry; 80 a rerun of ``extract`` is sent; 90-120 worker,
recovery and schedule bookkeeping.
"""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Final, LiteralString

import psycopg

from horsies.core.types.result import Err, Ok, Result
from crossclient.parity.snapshot.capture import Connection

Params = dict[str, object]

_LEASE: Final = timedelta(seconds=30)
_RETRY_DELAY: Final = timedelta(seconds=5)
_RECOVERY_CLAIM_TTL: Final = timedelta(seconds=10)
_DEADLINE: Final = timedelta(hours=2)
_SCHEDULE_AHEAD: Final = timedelta(hours=1)
_HOST: Final = 'host-a'
_PID: Final = 4242
_PROCESS: Final = 'horsies-worker-1'
_EXTRACT_INPUT: Final = '{"source":"s3://bucket/a"}'


class RowOrder(Enum):
    """Physical insert order of the rows of each table."""

    AS_LISTED = 'as listed'
    REVERSED = 'reversed'


@dataclass(frozen=True, slots=True)
class ScenarioVariant:
    """What one database run writes. ``invoice_max_retries`` is the one field a test can change."""

    row_order: RowOrder
    invoice_max_retries: int


@dataclass(frozen=True, slots=True)
class ScenarioFacts:
    """The raw ids and base time one run used."""

    workflow_id: uuid.UUID
    task_ids: tuple[uuid.UUID, ...]
    worker_id: str
    base_time: datetime


@dataclass(frozen=True, slots=True)
class ScenarioWriteFailed:
    """A scenario statement raised a database error. The transaction is rolled back: nothing was written."""

    cause: psycopg.Error


@dataclass(frozen=True, slots=True)
class _Ids:
    workflow: uuid.UUID
    extract_node: uuid.UUID
    load_node: uuid.UUID
    extract: uuid.UUID
    load: uuid.UUID
    invoice: uuid.UUID
    rerun: uuid.UUID
    worker: str
    recovery_pass: uuid.UUID
    recovery_claim: uuid.UUID


def _new_ids() -> _Ids:
    return _Ids(
        workflow=uuid.uuid4(),
        extract_node=uuid.uuid4(),
        load_node=uuid.uuid4(),
        extract=uuid.uuid4(),
        load=uuid.uuid4(),
        invoice=uuid.uuid4(),
        rerun=uuid.uuid4(),
        worker=f'worker-{uuid.uuid4().hex}',
        recovery_pass=uuid.uuid4(),
        recovery_claim=uuid.uuid4(),
    )


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


_WORKFLOW_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result,
        error, parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at,
        created_at, started_at, completed_at, updated_at
    ) VALUES (
        %(id)s, 'nightly_etl', 'etl.v1', 'RUNNING', 'fail', 1, NULL, NULL,
        NULL, NULL, NULL, %(id)s, 0, %(at)s,
        %(at)s, %(at)s, NULL, %(at)s
    )
"""

_NODE_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
        dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type, min_success,
        task_options, max_retries, execution, good_until, retention_class_key, status, task_id,
        is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key,
        sub_workflow_params, sub_workflow_summary, result, error, created_at, started_at,
        completed_at
    ) VALUES ('NONE',
        %(id)s, %(workflow_id)s, %(task_index)s, %(task_name)s, %(task_name)s, %(input)s,
        'default', 50, %(dependencies)s::integer[], %(args_from)s::jsonb, NULL, FALSE, 'all', NULL,
        NULL, 0, 'ISOLATED', %(good_until)s, 'forever', %(status)s, %(task_id)s,
        FALSE, NULL, NULL, NULL,
        NULL, NULL, %(result)s, NULL, %(created_at)s, %(started_at)s,
        %(completed_at)s
    )
"""


def _node_rows(ids: _Ids, at: dict[int, datetime], base: datetime) -> list[Params]:
    return [
        {
            'id': ids.extract_node,
            'workflow_id': ids.workflow,
            'task_index': 0,
            'task_name': 'extract',
            'input': _EXTRACT_INPUT,
            'dependencies': [],
            'args_from': None,
            'good_until': None,
            'status': 'COMPLETED',
            'task_id': ids.extract,
            'result': '{"ok":3}',
            'created_at': at[0],
            'started_at': at[20],
            'completed_at': at[30],
        },
        {
            'id': ids.load_node,
            'workflow_id': ids.workflow,
            'task_index': 1,
            'task_name': 'load',
            'input': '{}',
            'dependencies': [0],
            'args_from': '{"rows": 0}',
            'good_until': base + _DEADLINE,
            'status': 'ENQUEUED',
            'task_id': ids.load,
            'result': None,
            'created_at': at[0],
            'started_at': None,
            'completed_at': None,
        },
    ]


_TASK_SQL: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key,
        sent_at, created_at, command_fingerprint_version, command_fingerprint,
        idempotency_key_digest, status, enqueued_at, claimed_at, claimed_by_worker_id,
        claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id, worker_pid,
        worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        %(id)s, %(task_name)s, %(queue_name)s, %(priority)s, %(input)s, NULL, %(execution)s,
        'forever', %(sent_at)s, %(sent_at)s, %(fingerprint_version)s, %(fingerprint)s,
        %(key_digest)s, %(status)s, %(enqueued_at)s, %(claimed_at)s, %(claimed_by)s,
        %(claim_expires_at)s, NULL, NULL, NULL, NULL,
        NULL, NULL, %(retry_count)s, %(max_retries)s, %(next_retry_at)s, %(good_until)s,
        %(is_workflow_task)s, %(updated_at)s, %(retain_rerun_input)s, %(rerun_of)s, %(rerun_of)s
    )
"""


def _task_row(
    task_id: uuid.UUID, name: str, sent_at: datetime, **fields: object
) -> Params:
    row: Params = {
        'id': task_id,
        'task_name': name,
        'queue_name': 'default',
        'priority': 50,
        'input': '{}',
        'execution': 'ISOLATED',
        'sent_at': sent_at,
        'fingerprint_version': 1,
        'fingerprint': _digest(task_id.bytes),
        'key_digest': None,
        'status': 'PENDING',
        'enqueued_at': sent_at,
        'claimed_at': None,
        'claimed_by': None,
        'claim_expires_at': None,
        'retry_count': 0,
        'max_retries': 0,
        'next_retry_at': None,
        'good_until': None,
        'is_workflow_task': False,
        'updated_at': sent_at,
        'retain_rerun_input': False,
        'rerun_of': None,
    }
    row.update(fields)
    return row


def _task_rows(
    ids: _Ids, at: dict[int, datetime], base: datetime, variant: ScenarioVariant
) -> list[Params]:
    retry_at = at[70] + _RETRY_DELAY
    return [
        _task_row(
            ids.load,
            'load',
            at[0],
            fingerprint_version=None,
            fingerprint=None,
            status='CLAIMED',
            claimed_at=at[40],
            claimed_by=ids.worker,
            claim_expires_at=at[40] + _LEASE,
            good_until=base + _DEADLINE,
            is_workflow_task=True,
            updated_at=at[40],
        ),
        _task_row(
            ids.invoice,
            'send_invoice',
            at[50],
            queue_name='billing',
            priority=10,
            input='{"invoice":42}',
            execution='SHARED_ASYNC',
            key_digest=_digest(b'invoice-42'),
            enqueued_at=retry_at,
            retry_count=1,
            max_retries=variant.invoice_max_retries,
            next_retry_at=retry_at,
            updated_at=at[70],
            retain_rerun_input=True,
        ),
        _task_row(
            ids.rerun, 'extract', at[80], input=_EXTRACT_INPUT, rerun_of=ids.extract
        ),
    ]


_ATTEMPT_SQL: Final[LiteralString] = """
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message,
        failed_reason, worker_id, worker_hostname, worker_pid, worker_process_name
    ) VALUES (
        %(task_id)s, 1, 'FAILED', TRUE, %(started_at)s, %(finished_at)s, 'TIMEOUT',
        'upstream timed out', NULL, %(worker)s, %(host)s, %(pid)s, %(process)s
    )
"""

_HISTORY_SQL: Final[LiteralString] = """
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority, command_fingerprint_version, command_fingerprint,
        retention_class_key, is_workflow_task, workflow_id, rerun_of_task_id, rerun_root_task_id,
        sent_at, created_at, enqueued_at, claimed_at, started_at, good_until, terminal_at, status,
        terminalization_kind, retry_count, max_retries, last_claimed_worker_id,
        last_worker_hostname, last_worker_pid, last_worker_process_name, error_code,
        final_failed_reason, result_envelope_version, result_payload, result_digest,
        history_schema_version, attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
        rerun_input_disposition, rerun_input_version, rerun_input_digest, rerun_input_inline
    ) VALUES (
        %(task_id)s, 'extract', 'default', 50, NULL, NULL,
        'forever', TRUE, %(workflow_id)s, NULL, NULL,
        %(sent_at)s, %(sent_at)s, %(sent_at)s, %(claimed_at)s, %(started_at)s, NULL,
        %(terminal_at)s, 'COMPLETED',
        'COMPLETE_LOCKED', 0, 0, %(worker)s,
        %(host)s, %(pid)s, %(process)s, NULL,
        NULL, 1, %(payload)s, %(payload_digest)s,
        1, 1, %(snapshot)s, %(snapshot_digest)s,
        'NEVER_ELIGIBLE', NULL, NULL, NULL
    )
"""


def _epoch_microseconds(at: datetime) -> int:
    return round(at.timestamp() * 1_000_000)


def _history_row(ids: _Ids, at: dict[int, datetime]) -> Params:
    payload = b'{"ok":3}'
    # Attempt archive version 1: one 12-field array per attempt.
    attempts = [
        [
            1,
            'COMPLETED',
            False,
            _epoch_microseconds(at[20]),
            _epoch_microseconds(at[30]),
            None,
            None,
            None,
            ids.worker,
            _HOST,
            _PID,
            _PROCESS,
        ],
    ]
    snapshot = json.dumps(attempts, separators=(',', ':')).encode()
    return {
        'task_id': ids.extract,
        'workflow_id': ids.workflow,
        'sent_at': at[0],
        'claimed_at': at[10],
        'started_at': at[20],
        'terminal_at': at[30],
        'worker': ids.worker,
        'host': _HOST,
        'pid': _PID,
        'process': _PROCESS,
        'payload': payload,
        'payload_digest': _digest(payload),
        'snapshot': snapshot,
        'snapshot_digest': _digest(snapshot),
    }


_WORKER_STATE_SQL: Final[LiteralString] = """
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid, processes, max_claim_batch, max_claim_per_worker,
        cluster_wide_cap, queues, queue_priorities, queue_max_concurrency, recovery_config,
        tasks_running, tasks_claimed, memory_usage_mb, memory_percent, cpu_percent,
        children_memory_mb, worker_started_at, slots_down, class_capacity, telemetry
    ) VALUES (
        %(worker)s, %(snapshot_at)s, %(host)s, %(pid)s, 2, 4, 8,
        NULL, ARRAY['default', 'billing']::varchar(100)[], '{"default": 1, "billing": 2}', '{}',
        '{"enabled": true}', 0, 1, %(memory_mb)s, %(memory_percent)s, %(cpu_percent)s,
        NULL, %(started_at)s, '[]', '{}', jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
    )
"""

_RECOVERY_FAILURE_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflow_recovery_failures (
        workflow_id, failed_passes, last_pass_id, first_failed_at, last_failed_at, last_case,
        last_task_index, last_error_type
    ) VALUES (
        %(workflow_id)s, 1, %(pass_id)s, %(at)s, %(at)s, 'terminal_workflow_running',
        NULL, 'OperationalError'
    )
"""

_CURSOR_SQL: Final[LiteralString] = """
    UPDATE horsies_recovery_scan_cursors
    SET last_created_at = %(last_created_at)s,
        last_id = %(last_id)s,
        cycle_upper_created_at = %(last_created_at)s,
        cycle_upper_id = %(last_id)s,
        claim_token = %(claim_token)s,
        claim_expires_at = %(claim_expires_at)s,
        completed_cycles = %(completed_cycles)s,
        last_scan_rows = 1,
        last_candidate_rows = %(candidate_rows)s,
        last_scan_at = %(scan_at)s
    WHERE scan_name = %(scan_name)s
"""


def _cursor_rows(ids: _Ids, at: dict[int, datetime]) -> list[Params]:
    return [
        {
            'scan_name': 'running_workflows',
            'last_created_at': None,
            'last_id': None,
            'claim_token': ids.recovery_claim,
            'claim_expires_at': at[110] + _RECOVERY_CLAIM_TTL,
            'completed_cycles': 1,
            'candidate_rows': 1,
            'scan_at': at[110],
        },
        {
            'scan_name': 'orphan_workflow_tasks',
            'last_created_at': at[0],
            'last_id': ids.load,
            'claim_token': None,
            'claim_expires_at': None,
            'completed_cycles': 0,
            'candidate_rows': 0,
            'scan_at': at[110],
        },
    ]


_RESERVATION_SQL: Final[LiteralString] = """
    INSERT INTO horsies_key_reservations (
        idempotency_key_digest, key_scope_version, fingerprint_version, command_fingerprint,
        task_id, disposition, reservation_window, expires_at
    ) VALUES (
        %(key_digest)s, 1, 1, %(fingerprint)s, %(task_id)s, 'LIVE', interval '1 day', NULL
    )
"""

_SCHEDULE_SQL: Final[LiteralString] = """
    INSERT INTO horsies_schedule_state (
        schedule_name, next_run_at, last_run_at, last_task_id, run_count, config_hash, updated_at
    ) VALUES (
        'hourly_report', %(next_run_at)s, NULL, NULL, 0, %(config_hash)s, %(at)s
    )
"""

_BASE_TIME_SQL: Final[LiteralString] = 'SELECT clock_timestamp()'


def _ordered(rows: list[Params], order: RowOrder) -> list[Params]:
    match order:
        case RowOrder.AS_LISTED:
            return rows
        case RowOrder.REVERSED:
            return list(reversed(rows))


def _base_time(connection: Connection) -> datetime:
    match connection.execute(_BASE_TIME_SQL).fetchone():
        case (datetime() as now,):
            return now
        case record:
            raise AssertionError(
                f'the base time read returned {record!r}, not one timestamp'
            )


def _write(connection: Connection, ids: _Ids, variant: ScenarioVariant) -> datetime:
    base = _base_time(connection)
    at = {offset: base + timedelta(milliseconds=offset) for offset in range(0, 121, 10)}
    order = variant.row_order
    _ = connection.execute(_WORKFLOW_SQL, {'id': ids.workflow, 'at': at[0]})
    with connection.cursor() as cursor:
        cursor.executemany(_NODE_SQL, _ordered(_node_rows(ids, at, base), order))
        cursor.executemany(
            _TASK_SQL, _ordered(_task_rows(ids, at, base, variant), order)
        )
        cursor.executemany(_CURSOR_SQL, _ordered(_cursor_rows(ids, at), order))
    host = {'worker': ids.worker, 'host': _HOST, 'pid': _PID, 'process': _PROCESS}
    attempt = {'task_id': ids.invoice, 'started_at': at[60], 'finished_at': at[70]}
    _ = connection.execute(_ATTEMPT_SQL, attempt | host)
    _ = connection.execute(_HISTORY_SQL, _history_row(ids, at))
    sample = {
        'snapshot_at': at[90],
        'started_at': at[0],
        'memory_mb': random.uniform(50, 500),
        'memory_percent': random.uniform(0, 100),
        'cpu_percent': random.uniform(0, 100),
    }
    _ = connection.execute(_WORKER_STATE_SQL, sample | host)
    failure = {'workflow_id': ids.workflow, 'pass_id': ids.recovery_pass, 'at': at[100]}
    _ = connection.execute(_RECOVERY_FAILURE_SQL, failure)
    reservation = {
        'key_digest': _digest(b'invoice-42'),
        'fingerprint': _digest(ids.invoice.bytes),
        'task_id': ids.invoice,
    }
    _ = connection.execute(_RESERVATION_SQL, reservation)
    schedule = {
        'next_run_at': base + _SCHEDULE_AHEAD,
        'config_hash': _digest(b'hourly_report:v1'),
        'at': at[120],
    }
    _ = connection.execute(_SCHEDULE_SQL, schedule)
    return base


def write_scenario(
    connection: Connection, variant: ScenarioVariant
) -> Result[ScenarioFacts, ScenarioWriteFailed]:
    """Write the scenario in one transaction, with new ids and the database clock as base time."""
    ids = _new_ids()
    try:
        with connection.transaction():
            return Ok(_facts(ids, _write(connection, ids, variant)))
        raise AssertionError('the scenario transaction block swallowed an exception')
    except psycopg.Error as exc:
        return Err(ScenarioWriteFailed(cause=exc))


def _facts(ids: _Ids, base: datetime) -> ScenarioFacts:
    return ScenarioFacts(
        workflow_id=ids.workflow,
        task_ids=(ids.extract, ids.load, ids.invoice, ids.rerun),
        worker_id=ids.worker,
        base_time=base,
    )
