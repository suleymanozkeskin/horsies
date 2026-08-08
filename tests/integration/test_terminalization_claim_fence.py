"""Claim-generation fences on the workflow pause/cancel terminalizers.

Six writers moved a task to a terminal status without rejecting a stale claim
generation: the two post-claim guard batches, the workflow-pause API batch, the
two child pre-start branches, and the workflow-cancel batch. Reading their
callers splits them in two.

The four with a real staleness window — the two post-claim batches and the two
child branches — now fence on worker plus claim generation. The race: a
worker's lease lapses,
the reaper requeues the task, the same worker re-claims it. The row is CLAIMED
by that worker again, so a (status, worker) guard cannot tell the generations
apart and terminalizes the new claim.

The other two — the workflow-pause API batch and the workflow-cancel batch —
have no staleness window: their callers hold row locks over exactly the rows
they update. They gained a live workflow-status join instead, which must not
change what they cancel. Both directions are pinned here because only the pair
distinguishes the correct semantics from the generation fence that would have
broken them.

One test proves the fence is armed rather than merely tolerant: a NULL
generation disables it per task, so a dispatch path that stopped carrying
``claimed_at`` would silently restore the old behavior while every test that
pre-arms generations kept passing.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.lifecycle.commands import (
    AbandonNodesOfPausedWorkflows,
    CancelNodesOfCancelledWorkflow,
)
from horsies.core.lifecycle.persistence import apply_batch_async
from horsies.core.worker.child_runner import _handle_workflow_stop_before_start
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration]

_WORKER_ID = 'fence-worker-1'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(engine: AsyncEngine) -> Worker:
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)
    worker.worker_instance_id = _WORKER_ID
    return worker


async def _insert_claimed_task(
    session: AsyncSession,
    *,
    claimed_at: datetime | None,
    worker_id: str | None = _WORKER_ID,
    status: str = 'CLAIMED',
) -> str:
    """Insert one task row at a known claim generation."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='fence_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, claimed_at, claimed_by_worker_id, enqueue_sha,
                 is_workflow_task,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'fence_test', 'default', 100, '[]', '{}',
                 :status, :sent_at, NOW(), NOW(), TRUE, 0,
                 0, :claimed_at, :worker_id, :enqueue_sha, TRUE,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'status': status,
            'sent_at': sent_at,
            'claimed_at': claimed_at,
            'worker_id': worker_id,
            'enqueue_sha': sha,
        },
    )
    return task_id


async def _link_to_workflow(
    session: AsyncSession,
    task_id: str,
    *,
    wf_status: str,
    wt_status: str = 'ENQUEUED',
) -> str:
    wf_id = str(uuid.uuid4())
    wt_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:id, 'fence_wf', :status, 'FAIL', 0, :id,
                 NOW(), NOW(), NOW(), NOW())
        """),
        {'id': wf_id, 'status': wf_status},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES
                (:id, :wf_id, 0, 'node_0', 'fence_test', '[]', '{}',
                 'default', 100, '{}', FALSE, 'all',
                 FALSE, :wt_status, :task_id, NOW())
        """),
        {
            'id': wt_id,
            'wf_id': wf_id,
            'task_id': task_id,
            'wt_status': wt_status,
        },
    )
    return wf_id


async def _status_of(session: AsyncSession, task_id: str) -> str:
    row = (
        await session.execute(
            text(
                'SELECT status FROM itest_task_rows '
                'WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'task {task_id} vanished'
    return str(row[0])


async def _terminalization_kind_of(
    session: AsyncSession,
    task_id: str,
) -> str | None:
    return (
        await session.execute(
            text(
                'SELECT terminalization_kind FROM itest_task_rows '
                'WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).scalar_one()


async def _workflow_node_state(
    session: AsyncSession,
    workflow_id: str,
) -> tuple[str, str | None]:
    row = (
        await session.execute(
            text(
                'SELECT status, task_id FROM horsies_workflow_tasks '
                'WHERE workflow_id = :workflow_id'
            ),
            {'workflow_id': workflow_id},
        )
    ).one()
    return str(row.status), row.task_id


def _dispatch_rows(*pairs: tuple[str, datetime | None]) -> list[dict[str, Any]]:
    """Dispatch-shaped rows carrying an explicit claim generation."""
    return [
        {
            'id': task_id,
            'task_name': 'fence_test',
            'args': '[]',
            'kwargs': '{}',
            'is_workflow_task': True,
            'claimed_at': claimed_at,
        }
        for task_id, claimed_at in pairs
    ]


def _generations() -> tuple[datetime, datetime]:
    """(dispatched generation, later re-claim generation)."""
    first = datetime.now(timezone.utc) - timedelta(minutes=5)
    return first, first + timedelta(minutes=1)


# ---------------------------------------------------------------------------
# Paused-workflow abandon, post-claim guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_t02_stale_generation_does_not_abandon_reclaimed_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A re-claimed row survives an abandon issued for the old generation."""
    dispatched, reclaimed = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    await _link_to_workflow(session, task_id, wf_status='PAUSED')
    await session.commit()

    worker = _make_worker(engine)
    result = await worker._filter_nonrunnable_workflow_tasks(
        _dispatch_rows((task_id, dispatched)),
    )

    assert await _status_of(session, task_id) == 'CLAIMED'
    # The guard still refuses to dispatch it; it simply does not terminalize
    # a claim generation it does not own.
    assert result == []


@pytest.mark.asyncio(loop_scope='function')
async def test_t02_matching_generation_abandons_the_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The abandon still fires for the generation it was dispatched from."""
    dispatched, _ = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=dispatched)
    await _link_to_workflow(session, task_id, wf_status='PAUSED')
    await session.commit()

    worker = _make_worker(engine)
    await worker._filter_nonrunnable_workflow_tasks(
        _dispatch_rows((task_id, dispatched)),
    )

    assert await _status_of(session, task_id) == 'CANCELLED'


# ---------------------------------------------------------------------------
# Cancelled-workflow cancel, post-claim guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_t03_stale_generation_does_not_cancel_reclaimed_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    dispatched, reclaimed = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    await _link_to_workflow(session, task_id, wf_status='CANCELLED')
    await session.commit()

    worker = _make_worker(engine)
    await worker._filter_nonrunnable_workflow_tasks(
        _dispatch_rows((task_id, dispatched)),
    )

    assert await _status_of(session, task_id) == 'CLAIMED'


@pytest.mark.asyncio(loop_scope='function')
async def test_t03_matching_generation_cancels_the_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    dispatched, _ = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=dispatched)
    await _link_to_workflow(session, task_id, wf_status='CANCELLED')
    await session.commit()

    worker = _make_worker(engine)
    await worker._filter_nonrunnable_workflow_tasks(
        _dispatch_rows((task_id, dispatched)),
    )

    assert await _status_of(session, task_id) == 'CANCELLED'


@pytest.mark.asyncio(loop_scope='function')
async def test_batch_fences_each_task_on_its_own_generation(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """One batch can span claim transactions, so the fence is pairwise.

    A scalar generation would either spare both rows or terminalize both.
    """
    dispatched, reclaimed = _generations()
    fresh_id = await _insert_claimed_task(session, claimed_at=dispatched)
    stale_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    await _link_to_workflow(session, fresh_id, wf_status='PAUSED')
    await _link_to_workflow(session, stale_id, wf_status='PAUSED')
    await session.commit()

    worker = _make_worker(engine)
    await worker._filter_nonrunnable_workflow_tasks(
        _dispatch_rows((fresh_id, dispatched), (stale_id, dispatched)),
    )

    assert await _status_of(session, fresh_id) == 'CANCELLED'
    assert await _status_of(session, stale_id) == 'CLAIMED'


# ---------------------------------------------------------------------------
# The fence must be armed, not merely tolerant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_real_claim_path_arms_the_fence(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Rows from the production claim path carry a usable generation.

    A NULL generation disables the fence for that task, so a dispatch path
    that stopped reporting claimed_at would silently restore the unfenced
    behavior while every test that pre-arms generations kept passing. This
    claims through the real statement, then proves the fence engages on the
    generation that claim produced.
    """
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='fence_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries, enqueue_sha,
                 is_workflow_task,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'fence_test', 'default', 100, '[]', '{}',
                 'PENDING', :sent_at, NOW(), NOW(), NOW(),
                 FALSE, 0, 0, :enqueue_sha, TRUE,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {'id': task_id, 'sent_at': sent_at, 'enqueue_sha': sha},
    )
    await _link_to_workflow(session, task_id, wf_status='PAUSED')
    await session.commit()

    worker = _make_worker(engine)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    async with sf() as claim_session:
        rows = await worker._claim_batch_locked(claim_session, 'default', 10)
        await claim_session.commit()

    claimed = [row for row in rows if row['id'] == task_id]
    assert claimed, 'the claim statement did not return the seeded task'
    assert claimed[0].get('claimed_at') is not None, (
        'the claim path stopped reporting claimed_at; every fence built on it '
        'silently degrades to unfenced'
    )

    # Simulate the reaper requeue plus re-claim by the same worker.
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET claimed_at = claimed_at + INTERVAL '1 minute'
            WHERE id = :id
        """),
        {'id': task_id},
    )
    await session.commit()

    await worker._filter_nonrunnable_workflow_tasks(claimed)

    assert await _status_of(session, task_id) == 'CLAIMED', (
        'the abandon terminalized a generation it did not own, so the fence '
        'was not armed by the real claim path'
    )


# ---------------------------------------------------------------------------
# Child pre-start branches, raw psycopg
# ---------------------------------------------------------------------------


def _psycopg_dsn(db_url: str) -> str:
    return db_url.replace('postgresql+psycopg://', 'postgresql://')


@pytest.mark.parametrize(
    (
        'wf_status',
        'terminal_expected',
        'kind_expected',
        'stale_node_status',
        'stale_node_detached',
    ),
    [
        (
            'PAUSED',
            'CANCELLED',
            'PAUSE_ABANDON_CLAIM',
            'READY',
            True,
        ),
        (
            'CANCELLED',
            'CANCELLED',
            'WORKFLOW_CANCEL_CLAIM',
            'SKIPPED',
            False,
        ),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_child_prestart_fences_on_claim_generation(
    db_url: str,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    wf_status: str,
    terminal_expected: str,
    kind_expected: str,
    stale_node_status: str,
    stale_node_detached: bool,
) -> None:
    """T10/T11 refuse a generation the child was not dispatched from."""
    dispatched, reclaimed = _generations()
    stale_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    fresh_id = await _insert_claimed_task(session, claimed_at=dispatched)
    stale_workflow_id = await _link_to_workflow(
        session,
        stale_id,
        wf_status=wf_status,
    )
    await _link_to_workflow(session, fresh_id, wf_status=wf_status)
    await session.commit()

    with psycopg.connect(_psycopg_dsn(db_url)) as conn:
        with conn.cursor() as cursor:
            _handle_workflow_stop_before_start(
                cursor, conn, stale_id, wf_status, _WORKER_ID, dispatched,
            )
            _handle_workflow_stop_before_start(
                cursor, conn, fresh_id, wf_status, _WORKER_ID, dispatched,
            )

    await session.commit()
    assert await _status_of(session, stale_id) == 'CLAIMED'
    assert await _status_of(session, fresh_id) == terminal_expected
    assert await _terminalization_kind_of(session, stale_id) is None
    assert await _terminalization_kind_of(session, fresh_id) == kind_expected
    observed_node_status, observed_node_task_id = await _workflow_node_state(
        session,
        stale_workflow_id,
    )
    assert observed_node_status == stale_node_status
    assert (observed_node_task_id is None) is stale_node_detached


@pytest.mark.asyncio(loop_scope='function')
async def test_child_prestart_still_cancels_requeued_pending_row(
    db_url: str,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """PENDING carries no claim, so the fence must not gate it.

    A row the reaper requeued between dispatch and this check has no
    claimed_by_worker_id. Gating it on the worker would turn the fence into a
    skip, leaving a task of a CANCELLED workflow live.
    """
    dispatched, _ = _generations()
    task_id = await _insert_claimed_task(
        session, claimed_at=None, worker_id=None, status='PENDING',
    )
    await _link_to_workflow(session, task_id, wf_status='CANCELLED')
    await session.commit()

    with psycopg.connect(_psycopg_dsn(db_url)) as conn:
        with conn.cursor() as cursor:
            _handle_workflow_stop_before_start(
                cursor, conn, task_id, 'CANCELLED', _WORKER_ID, dispatched,
            )

    await session.commit()
    assert await _status_of(session, task_id) == 'CANCELLED'


# ---------------------------------------------------------------------------
# Workflow-API batches: live workflow-status guard, both directions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_t09_skips_tasks_whose_workflow_resumed(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A resumed workflow's claims survive a batch computed while paused."""
    _, reclaimed = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    wf_id = await _link_to_workflow(session, task_id, wf_status='PAUSED')
    await session.commit()

    await session.execute(
        text("UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = :id"),
        {'id': wf_id},
    )
    await apply_batch_async(
        await session.connection(),
        AbandonNodesOfPausedWorkflows(workflow_ids=(wf_id,)),
    )
    await session.commit()

    assert await _status_of(session, task_id) == 'CLAIMED'


@pytest.mark.asyncio(loop_scope='function')
async def test_t09_abandons_a_claim_taken_while_still_paused(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """While the workflow is paused, whatever claim exists is abandoned.

    This is the semantics a claim-generation fence would have broken: the
    claim that slipped in after the pause is exactly the one the batch exists
    to catch.
    """
    _, reclaimed = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    wf_id = await _link_to_workflow(session, task_id, wf_status='PAUSED')
    await session.commit()

    await apply_batch_async(
        await session.connection(),
        AbandonNodesOfPausedWorkflows(workflow_ids=(wf_id,)),
    )
    await session.commit()

    assert await _status_of(session, task_id) == 'CANCELLED'


@pytest.mark.asyncio(loop_scope='function')
async def test_t16_skips_tasks_whose_workflow_is_not_cancelled(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The enqueued-not-started cancel requires a live CANCELLED workflow."""
    _, reclaimed = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    wf_id = await _link_to_workflow(session, task_id, wf_status='RUNNING')
    await session.commit()

    await apply_batch_async(
        await session.connection(),
        CancelNodesOfCancelledWorkflow(workflow_ids=(wf_id,)),
    )
    await session.commit()

    assert await _status_of(session, task_id) == 'CLAIMED'


@pytest.mark.asyncio(loop_scope='function')
async def test_t16_cancels_whatever_claim_exists_under_a_cancelled_workflow(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    _, reclaimed = _generations()
    task_id = await _insert_claimed_task(session, claimed_at=reclaimed)
    wf_id = await _link_to_workflow(session, task_id, wf_status='CANCELLED')
    await session.commit()

    await apply_batch_async(
        await session.connection(),
        CancelNodesOfCancelledWorkflow(workflow_ids=(wf_id,)),
    )
    await session.commit()

    assert await _status_of(session, task_id) == 'CANCELLED'
