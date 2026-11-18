"""The rerun operation against real PostgreSQL: the ruled matrix.

A failed plain task reruns end to end — new identity, atomic lineage,
source record byte-untouched, the new PENDING row written strictly by
the provenance table with the source's input and options texts, so its
own move stores the source's envelope bytes. Every typed refusal is
exercised: live source, absent source, both ineligibilities, input
unavailable with its disposition, corrupt envelope failing closed. Keyed
rerun rides the real reservation registry: replay returns the committed
rerun, and a key still reserved by the source conflicts by contract.
Policy resolution at enqueue is proven by flipping the effective class
between source and rerun.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from horsies._core import CoreContractViolation
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.periods import period_class_key
from horsies.core.history.identity.keys import ScopedIdempotencyKey
from horsies.core.history.reads.detail import TaskAbsenceReason
from horsies.core.history.identity.reservations import (
    ReservationApplied,
    claim_key_reservation,
)
from horsies.core.history.rerun.operations import (
    NotEligibleReason,
    RerunEnqueued,
    RerunEnqueuePolicy,
    RerunInputCorrupt,
    RerunInputUnavailable,
    RerunKeyConflict,
    RerunKeyReplay,
    RerunNotEligible,
    RerunSourceAbsent,
    RerunSourceCorrupt,
    RerunSourceLive,
    RerunTask,
    rerun_task,
)
from horsies.core.types.status import TaskStatus

from tests.broker_resources_support import current_resources
from tests.integration.history_seeding import stored_history_status
from tests.integration.monitoring_seeding import insert_rows
from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)
from tests.integration.terminal_writes import fail_running_task, task_failure

from tests.integration.test_monitoring_queries import make_task
from tests.rerun_envelope_support import frame_rerun_envelope

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
# A rerun policy names a period, so its classes are period classes, each
# registered with the period its key stands for.
CLASS_DAYS = 11
CLASS_KEY = period_class_key(CLASS_DAYS)
SECOND_DAYS = 12
SECOND_CLASS = period_class_key(SECOND_DAYS)
NEVER_REGISTERED_DAYS = 13
WORKER = 'worker-rerun-1'
SOURCE_INPUT = '{"k":"v"}'
SOURCE_OPTIONS = '{"timeout_ms":5}'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_rerun'
)


@pytest_asyncio.fixture()
async def python_family(terminalization_schema: HistorySchema) -> None:
    """The family row that the install of a Python broker records. The
    rerun runs on the core, which writes only into a database of its own
    family; this module's schema comes from the baseline SQL, which records
    no family."""
    async with terminalization_schema.engine.begin() as connection:
        _ = await connection.execute(
            text(
                'INSERT INTO horsies_client_family (only_row, family, recorded_at) '
                "VALUES (TRUE, 'PYTHON', now())"
            )
        )


def policy(
    *,
    days: int = CLASS_DAYS,
    retain: bool = True,
) -> RerunEnqueuePolicy:
    return RerunEnqueuePolicy(
        retention=timedelta(days=days),
        retain_rerun_input=retain,
        reservation_window=timedelta(hours=24),
    )


async def seed_failed_source(
    connection: AsyncConnection,
    *,
    input_text: str = SOURCE_INPUT,
    task_options: str | None = SOURCE_OPTIONS,
) -> str:
    """A failed task whose move retained its input: an INLINE envelope."""
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        worker=WORKER,
        retain=True,
        input_text=input_text,
        task_options=task_options,
    )
    await fail_running(connection, task_id)
    return task_id


async def fail_running(
    connection: AsyncConnection,
    task_id: str,
    *,
    result_json: str = '{"error":{"code":"BOOM"}}',
    error_code: str = 'BOOM',
    failed_reason: str = 'exploded',
) -> None:
    """The worker's failure of the RUNNING task of ``WORKER``."""
    outcome = await fail_running_task(
        connection,
        task_id,
        worker_id=WORKER,
        failure=task_failure(
            result_json=result_json,
            error_code=error_code,
            failed_reason=failed_reason,
        ),
    )
    assert outcome == 'APPLIED'


async def live_row(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_tasks '
                'WHERE id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


async def history_row_bytes(
    connection: AsyncConnection, task_id: str
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_task_history '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


@pytest.mark.usefixtures('python_family')
class TestRerunEndToEnd:
    @pytest.mark.asyncio
    async def test_failed_task_reruns_by_the_provenance_table(
        self, terminalization_schema: HistorySchema
    ) -> None:
        deadline = datetime.now(UTC) + timedelta(hours=2)
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(connection)
            before = await history_row_bytes(connection, source)

            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=deadline),
                policy(),
            )
            assert isinstance(outcome, RerunEnqueued)
            assert outcome.source_task_id == UUID(source)
            assert outcome.rerun_root_task_id == UUID(source)
            assert outcome.new_task_id != UUID(source)

            row = await live_row(connection, str(outcome.new_task_id))
            # Replayed from the source row or its carried envelope.
            assert row.task_name == 'it.move'
            assert row.queue_name == 'default'
            assert row.priority == 50
            assert row.input == SOURCE_INPUT
            assert row.task_options == SOURCE_OPTIONS
            # Options written before the execution class have no key.
            assert row.execution == 'ISOLATED'
            assert row.max_retries == 0
            assert row.is_workflow_task is False
            # Lineage, atomic pair.
            assert str(row.rerun_of_task_id) == source
            assert str(row.rerun_root_task_id) == source
            # Caller-explicit deadline.
            assert row.good_until == deadline
            # Fresh runtime state.
            assert row.status == 'PENDING'
            assert row.retry_count == 0
            assert row.claimed_by_worker_id is None
            # Policy resolved at enqueue.
            assert row.retention_class_key == CLASS_KEY
            assert row.retain_rerun_input is True

            after = await history_row_bytes(connection, source)
            assert before == after

            # The rerun's own move stores the source's envelope bytes. The
            # row runs under WORKER with the claim and process facts of its
            # start.
            await connection.execute(
                text(
                    'UPDATE horsies_tasks '
                    "SET status = 'RUNNING', "
                    'claimed_by_worker_id = :worker, claimed_at = NOW(), '
                    'started_at = NOW(), worker_pid = 4242, '
                    "worker_hostname = 'it-host', worker_process_name = 'it-proc' "
                    'WHERE id = CAST(:task_id AS uuid)'
                ),
                {'task_id': str(outcome.new_task_id), 'worker': WORKER},
            )
            await fail_running(connection, str(outcome.new_task_id))
            rerun_history = await history_row_bytes(
                connection, str(outcome.new_task_id)
            )
            envelope = frame_rerun_envelope(SOURCE_INPUT, SOURCE_OPTIONS)
            assert bytes(before.rerun_input_inline) == envelope
            assert bytes(rerun_history.rerun_input_inline) == envelope
            assert bytes(rerun_history.rerun_input_digest) == (
                sha256(envelope).digest()
            )

    @pytest.mark.asyncio
    async def test_a_rerun_keeps_the_execution_class_of_the_request(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(
                connection,
                task_options=(
                    '{"timeout_ms":1000,"execution":"SHARED_THREAD",'
                    '"cancel_grace_ms":null}'
                ),
            )

            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(),
            )

            assert isinstance(outcome, RerunEnqueued)
            row = await live_row(connection, str(outcome.new_task_id))
            assert row.execution == 'SHARED_THREAD'

    @pytest.mark.asyncio
    async def test_rerun_of_a_rerun_keeps_the_root(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(connection)
            first = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(),
            )
            assert isinstance(first, RerunEnqueued)
            # Fail the first rerun for real, then rerun it. The row runs
            # under WORKER with the claim and process facts of its start.
            await connection.execute(
                text(
                    'UPDATE horsies_tasks '
                    "SET status = 'RUNNING', "
                    'claimed_by_worker_id = :worker, claimed_at = NOW(), '
                    'started_at = NOW(), worker_pid = 4242, '
                    "worker_hostname = 'it-host', worker_process_name = 'it-proc' "
                    'WHERE id = CAST(:task_id AS uuid)'
                ),
                {'task_id': str(first.new_task_id), 'worker': WORKER},
            )
            await fail_running(
                connection,
                str(first.new_task_id),
                result_json='{}',
                error_code='X',
                failed_reason='x',
            )
            second = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=first.new_task_id, deadline=None
                ),
                policy(),
            )
            assert isinstance(second, RerunEnqueued)
            assert second.source_task_id == first.new_task_id
            assert second.rerun_root_task_id == UUID(source)

    @pytest.mark.asyncio
    async def test_policy_resolves_current_class_at_enqueue(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            await prepare_move_storage(connection, SECOND_CLASS, duration_days=SECOND_DAYS)
            source = await seed_failed_source(connection)
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(days=SECOND_DAYS),
            )
            assert isinstance(outcome, RerunEnqueued)
            row = await live_row(connection, str(outcome.new_task_id))
            assert row.retention_class_key == SECOND_CLASS

    @pytest.mark.asyncio
    async def test_unregistered_class_raises_before_any_write(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """A typo'd class must cost a refused call, not a new task that
        runs to completion and then has no history partition to land in."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(connection)
            before = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()
            with pytest.raises(ValueError, match='is not registered'):
                await rerun_task(
                    connection,
                    RerunTask(source_task_id=UUID(source), deadline=None),
                    policy(days=NEVER_REGISTERED_DAYS),
                )
            after = (
                await connection.execute(
                    text('SELECT count(*) FROM horsies_tasks')
                )
            ).scalar_one()
            assert after == before, 'a refused rerun must write nothing'

    @pytest.mark.asyncio
    async def test_current_policy_can_decline_retention(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(connection)
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(retain=False),
            )
            assert isinstance(outcome, RerunEnqueued)
            row = await live_row(connection, str(outcome.new_task_id))
            assert row.retain_rerun_input is False
            # The input still carries.
            assert row.input == SOURCE_INPUT


@pytest.mark.usefixtures('python_family')
class TestTypedRefusals:
    @pytest.mark.asyncio
    async def test_an_invalid_stored_execution_is_input_corrupt(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(
                connection,
                input_text='{}',
                task_options='{"execution":"PARALLEL"}',
            )
            count_tasks = text('SELECT count(*) FROM horsies_tasks')
            before = (await connection.execute(count_tasks)).scalar_one()

            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(),
            )

            assert isinstance(outcome, RerunInputCorrupt)
            assert outcome.task_id == UUID(source)
            assert 'stored task options: execution is not one of' in outcome.detail
            assert (await connection.execute(count_tasks)).scalar_one() == before

    @pytest.mark.asyncio
    async def test_live_source_refuses(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            live = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(live), deadline=None),
                policy(),
            )
            assert outcome == RerunSourceLive(task_id=UUID(live))

    @pytest.mark.asyncio
    async def test_absent_source_classifies(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            ghost = str(uuid4())
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(ghost), deadline=None),
                policy(),
            )
            assert isinstance(outcome, RerunSourceAbsent)
            assert outcome.task_id == UUID(ghost)
            assert outcome.reason is TaskAbsenceReason.NOT_CLASSIFIED

    @pytest.mark.asyncio
    async def test_completed_source_is_not_eligible(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            done = (
                await connection.execute(
                    text(
                        'SELECT outcome FROM horsies_complete_task_fused('
                        'CAST(:task_id AS uuid), :worker, '
                        'COALESCE((SELECT claimed_at FROM horsies_tasks WHERE id = CAST(:task_id AS uuid)), NOW()), '
                        ':result, TRUE)'
                    ),
                    {
                        'task_id': task_id,
                        'worker': WORKER,
                        'result': '{"ok":true}',
                    },
                )
            ).one()
            assert done.outcome == 'APPLIED'
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(task_id), deadline=None),
                policy(),
            )
            assert outcome == RerunNotEligible(
                task_id=UUID(task_id),
                reason=NotEligibleReason.COMPLETED_SOURCE,
            )

    @pytest.mark.asyncio
    async def test_workflow_backing_task_is_not_eligible(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            task_id = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                is_workflow_task=True,
            )
            workflow_id = await create_workflow(
                connection, status='RUNNING'
            )
            await link_workflow_node(
                connection,
                task_id,
                workflow_id=workflow_id,
                node_status='RUNNING',
            )
            await fail_running(
                connection,
                task_id,
                result_json='{}',
                error_code='X',
                failed_reason='x',
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(task_id), deadline=None),
                policy(),
            )
            assert outcome == RerunNotEligible(
                task_id=UUID(task_id),
                reason=NotEligibleReason.WORKFLOW_TASK,
            )

    @pytest.mark.asyncio
    async def test_declined_input_is_unavailable_with_disposition(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await insert_live_task(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                retain=False,
            )
            await fail_running(
                connection,
                source,
                result_json='{}',
                error_code='X',
                failed_reason='x',
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(),
            )
            assert outcome == RerunInputUnavailable(
                task_id=UUID(source),
                disposition='DECLINED_BY_POLICY',
            )

    @pytest.mark.asyncio
    async def test_corrupt_envelope_fails_closed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(connection)
            await connection.execute(
                text(
                    'UPDATE horsies_task_history '
                    'SET rerun_input_inline = :bad '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'bad': b'tampered', 'task_id': source},
            )
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(),
            )
            assert isinstance(outcome, RerunInputCorrupt)
            assert 'digest' in outcome.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('input_text', 'detail'),
        [
            ('{"x":Infinity}', 'Non-standard JSON constant'),
            ('{"x":1e9999}', 'finite binary64'),
            ('{"x":"\\ud800"}', 'lone surrogate'),
        ],
    )
    async def test_refused_value_in_a_stored_envelope_fails_closed(
        self,
        terminalization_schema: HistorySchema,
        input_text: str,
        detail: str,
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(
                connection, input_text=input_text, task_options=None
            )
            count_tasks = text('SELECT count(*) FROM horsies_tasks')
            tasks_before = (await connection.execute(count_tasks)).scalar_one()
            outcome = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(),
            )
            assert isinstance(outcome, RerunInputCorrupt)
            assert detail in outcome.detail
            tasks_after = (await connection.execute(count_tasks)).scalar_one()
            assert tasks_after == tasks_before


@pytest.mark.usefixtures('python_family')
class TestKeyedRerun:
    @pytest.mark.asyncio
    async def test_replay_returns_the_committed_rerun(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(connection)
            first = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=UUID(source),
                    deadline=None,
                    caller_key='retry-batch-7',
                ),
                policy(),
            )
            assert isinstance(first, RerunEnqueued)
            replay = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=UUID(source),
                    deadline=None,
                    caller_key='retry-batch-7',
                ),
                policy(),
            )
            assert replay == RerunKeyReplay(
                existing_task_id=first.new_task_id
            )
            count = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE rerun_of_task_id = CAST(:source AS uuid)'
                    ),
                    {'source': source},
                )
            ).scalar_one()
            assert count == 1

    @pytest.mark.asyncio
    async def test_key_reserved_by_the_source_conflicts_by_contract(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The frozen fingerprint covers rerun lineage, so the source's
        own reservation cannot alias its rerun — the conflict is the
        contract working."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
            source = await seed_failed_source(connection)
            scoped = ScopedIdempotencyKey(
                task_name='it.move', key='original-key'
            )
            claimed = await claim_key_reservation(
                connection,
                key_digest=scoped.digest,
                key_scope_version=1,
                reservation_window=timedelta(days=1),
                fingerprint_version=1,
                fingerprint=sha256(b'the original command').digest(),
                task_id=UUID(source),
            )
            assert isinstance(claimed, ReservationApplied)
            outcome = await rerun_task(
                connection,
                RerunTask(
                    source_task_id=UUID(source),
                    deadline=None,
                    caller_key='original-key',
                ),
                policy(),
            )
            assert outcome == RerunKeyConflict(
                task_id=UUID(source), reserved_by_task_id=UUID(source)
            )
            new_rows = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE rerun_of_task_id IS NOT NULL'
                    )
                )
            ).scalar_one()
            assert new_rows == 0


class TestStoredSourceDoesNotDecode:
    """Stored data that does not parse is an outcome with its cause.

    The history parent has no status CHECK, so this runs on the broker
    database, where an out-of-band write can store a status that is not a
    ``TaskStatus`` member.
    """

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_unknown_stored_status_is_source_corrupt(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        source = make_task(status=TaskStatus.FAILED)
        await insert_rows(session, source)
        count_tasks = text('SELECT count(*) FROM horsies_tasks')

        # The status column holds 9 characters; the value is not a member.
        async with stored_history_status(session, source.id, 'UNKNOWN'):
            async with current_resources(broker).engine.begin() as connection:
                tasks_before = (await connection.execute(count_tasks)).scalar_one()
                outcome = await rerun_task(
                    connection,
                    RerunTask(source_task_id=UUID(source.id), deadline=None),
                    RerunEnqueuePolicy(
                        retention=timedelta(days=30),
                        retain_rerun_input=True,
                        reservation_window=timedelta(hours=24),
                    ),
                )
                tasks_after = (await connection.execute(count_tasks)).scalar_one()

        assert isinstance(outcome, RerunSourceCorrupt)
        assert outcome.task_id == UUID(source.id)
        assert isinstance(outcome.exception, HistoryContractError)
        assert "'UNKNOWN'" in str(outcome.exception)
        assert tasks_after == tasks_before


@pytest.mark.asyncio
async def test_a_family_row_that_holds_no_client_family_raises_a_contract_violation(
    terminalization_schema: HistorySchema,
) -> None:
    """A family value outside PYTHON and RUST is corrupt stored data.

    The test writes it past the CHECK ``horsies_client_family_family_known``,
    which it drops first. The core answers ``RerunUnknownStoredFamily`` with
    the stored value, and ``rerun_task`` raises its ``CoreContractViolation``
    (subject ``rerun_task``) before any write.
    """
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY, duration_days=CLASS_DAYS)
        source = await seed_failed_source(connection)
        _ = await connection.execute(
            text(
                'ALTER TABLE horsies_client_family '
                'DROP CONSTRAINT horsies_client_family_family_known'
            )
        )
        _ = await connection.execute(
            text(
                'INSERT INTO horsies_client_family (only_row, family, recorded_at) '
                "VALUES (TRUE, 'GO', now())"
            )
        )
        count_tasks = text('SELECT count(*) FROM horsies_tasks')
        before = (await connection.execute(count_tasks)).scalar_one()

        with pytest.raises(CoreContractViolation) as raised:
            _ = await rerun_task(
                connection,
                RerunTask(source_task_id=UUID(source), deadline=None),
                policy(),
            )

        assert raised.value.subject == 'rerun_task'
        assert 'horsies_client_family.family holds "GO"' in raised.value.message
        assert (await connection.execute(count_tasks)).scalar_one() == before
