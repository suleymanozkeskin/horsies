"""The command vocabulary, checked against the writers it has to describe.

Two properties matter and neither is visible by reading:

- the union covers every structurally distinct writer and no more, so a new
  writer forces a new variant and a redundant variant fails to justify itself;
- dispatch over the union is exhaustive, which the type checker proves by
  rejecting a match that omits a variant, and these tests confirm at runtime.

The correspondence is asserted against the matrix rather than restated here, so
the two cannot drift.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import get_args

import pytest

from horsies.core.lifecycle.commands import (
    AbandonNodesOfPausedWorkflows,
    AbandonOwnedNode,
    AbandonOwnedNodes,
    CancelLockedTask,
    CancelNodesOfCancelledWorkflow,
    CancelOrphanedTasks,
    CancelOwnedNode,
    CancelOwnedNodes,
    CancelOwnedOrphan,
    CompleteLockedTask,
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
    FailLockedTask,
    FailStaleTask,
    TerminalizationCommand,
    fence_of,
    target_status,
)
from horsies.core.lifecycle.fences import (
    CallerHoldsRowLock,
    OwnedClaim,
    OwnedClaimBatch,
    PriorLockedRead,
    WorkerOwned,
)
from horsies.core.types.status import TaskStatus
from tests.lifecycle_matrix import MATRIX

pytestmark = [pytest.mark.unit]

_GENERATION = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
_RESULT = '{"ok": null}'
_OWNED = OwnedClaim(worker_id='w1', claimed_at=_GENERATION)
_BATCH = OwnedClaimBatch(worker_id='w1', claim_generations=(('t1', _GENERATION),))
_LOCKED_READ = PriorLockedRead(worker_id='w1')
_WORKER = WorkerOwned(worker_id='w1')

# One instance of every variant. Anything missing here fails the count check
# below rather than silently going unexercised by the dispatch tests.
ONE_OF_EACH: tuple[TerminalizationCommand, ...] = (
    CompleteLockedTask(task_id='t1', fence=_LOCKED_READ, result_json=_RESULT),
    CompleteTaskFused(
        task_id='t1',
        fence=_OWNED,
        result_json=_RESULT,
        notify_channel='task_queue_default',
        notify_payload='capacity:t1',
    ),
    FailLockedTask(
        task_id='t1',
        fence=_LOCKED_READ,
        result_json=_RESULT,
        error_code='TASK_EXCEPTION',
        failed_reason=None,
    ),
    FailStaleTask(
        task_id='t1',
        stale_after_ms=60_000,
        finalizing_stale_after_ms=60_000,
        result_json=_RESULT,
        error_code='WORKER_CRASHED',
        failed_reason='Worker process crashed',
    ),
    ExpireOwnedClaim(
        task_id='t1',
        fence=_WORKER,
        result_json=_RESULT,
        error_code='TASK_EXPIRED',
    ),
    ExpirePendingTasks(
        batch_size=100, result_json=_RESULT, error_code='TASK_EXPIRED',
    ),
    CancelLockedTask(
        task_id='t1',
        fence=CallerHoldsRowLock(),
        permitted_source_statuses=(TaskStatus.PENDING, TaskStatus.CLAIMED),
    ),
    CancelOwnedOrphan(task_id='t1', fence=_OWNED),
    CancelOrphanedTasks(batch_size=100),
    AbandonOwnedNode(task_id='t1', fence=_OWNED),
    AbandonOwnedNodes(fence=_BATCH),
    AbandonNodesOfPausedWorkflows(workflow_ids=('wf1',)),
    CancelOwnedNode(
        task_id='t1', fence=_OWNED, accepts_requeued_pending=True,
    ),
    CancelOwnedNodes(fence=_BATCH),
    CancelNodesOfCancelledWorkflow(workflow_ids=('wf1',)),
)


def _matrix_signature_count() -> int:
    return len({
        (row.fence, row.shape, row.coupled_write, row.target_status, row.guards)
        for row in MATRIX
    })


class TestUnionMatchesTheWriters:
    def test_variant_count_equals_distinct_writer_signatures(self) -> None:
        """A new structurally distinct writer must force a new variant."""
        variants = get_args(TerminalizationCommand.__value__)
        assert len(variants) == _matrix_signature_count(), (
            f'{len(variants)} command variants against '
            f'{_matrix_signature_count()} distinct writer signatures'
        )

    def test_one_instance_per_variant_is_exercised(self) -> None:
        variants = get_args(TerminalizationCommand.__value__)
        assert {type(c) for c in ONE_OF_EACH} == set(variants)

    def test_target_statuses_cover_the_writers(self) -> None:
        """Every status the writers produce is produced by some command."""
        from_commands = {target_status(c).value for c in ONE_OF_EACH}
        from_writers = {row.target_status for row in MATRIX}
        assert from_commands == from_writers


class TestDispatchIsExhaustive:
    @pytest.mark.parametrize('command', ONE_OF_EACH, ids=lambda c: type(c).__name__)
    def test_target_status_returns_for_every_variant(
        self,
        command: TerminalizationCommand,
    ) -> None:
        """No variant falls through; the checker rejects an omitted case."""
        assert target_status(command).is_terminal

    @pytest.mark.parametrize('command', ONE_OF_EACH, ids=lambda c: type(c).__name__)
    def test_fence_of_returns_for_every_variant(
        self,
        command: TerminalizationCommand,
    ) -> None:
        fence_of(command)


class TestIllegalStatesAreUnrepresentable:
    """The properties the vocabulary exists to provide.

    These are compile-time guarantees; the assertions here pin the structure
    that produces them so a refactor cannot quietly give it up.
    """

    def test_batch_fences_belong_only_to_batch_commands(self) -> None:
        """A batch fence cannot be attached to a single-task command."""
        for command in ONE_OF_EACH:
            fence = fence_of(command)
            if isinstance(fence, OwnedClaimBatch):
                assert not hasattr(command, 'task_id'), type(command).__name__

    def test_single_task_commands_do_not_carry_a_batch(self) -> None:
        for command in ONE_OF_EACH:
            if hasattr(command, 'task_id'):
                assert not isinstance(fence_of(command), OwnedClaimBatch)

    def test_workflow_scoped_commands_carry_no_task_ids(self) -> None:
        """They select whole workflows; a task id list could disagree."""
        for command in ONE_OF_EACH:
            if hasattr(command, 'workflow_ids'):
                assert not hasattr(command, 'task_id'), type(command).__name__
                assert fence_of(command) is None, type(command).__name__

    def test_required_workflow_status_is_implied_by_the_variant(self) -> None:
        """No command names the workflow status its guard requires.

        Carrying it as data would let a pause command verify a cancellation —
        the same defect a disposition field would introduce, one level down in
        the fence. The absence of this test is what let that survive review
        once already, so it is asserted rather than argued.
        """
        for command in ONE_OF_EACH:
            fields = {f.name for f in dataclasses.fields(command)}
            assert 'required_workflow_status' not in fields
            assert 'required_node_statuses' not in fields
            assert 'workflow_status' not in fields

    def test_node_disposition_is_implied_by_the_variant(self) -> None:
        """No command carries a disposition field that could contradict it.

        Pause readies a node, cancellation skips it. Expressing that as a field
        would permit a cancelled workflow readying nodes for a resume that can
        never come, so it is expressed as variant identity instead.
        """
        for command in ONE_OF_EACH:
            fields = {f.name for f in dataclasses.fields(command)}
            assert 'disposition' not in fields, type(command).__name__
            assert 'node_status' not in fields, type(command).__name__

    def test_no_variant_carries_an_unaccounted_field(self) -> None:
        """Every field on every variant is one of these roles, or the test fails.

        Named forbidden fields only catch mistakes already made once. This
        reads the fields the dataclasses actually have, so the next field
        nobody thought about has to be justified here before it can ship —
        which is how a free status field survived review the first time.
        """
        permitted = {
            # what it acts on
            'task_id', 'workflow_ids', 'batch_size',
            # what proves it may
            'fence', 'permitted_source_statuses', 'accepts_requeued_pending',
            'stale_after_ms', 'finalizing_stale_after_ms',
            # what it records
            'result_json', 'error_code', 'failed_reason',
            # what it wakes
            'notify_channel', 'notify_payload',
        }
        for command in ONE_OF_EACH:
            fields = {f.name for f in dataclasses.fields(command)}
            assert fields <= permitted, (
                f'{type(command).__name__} carries unaccounted '
                f'{sorted(fields - permitted)}'
            )

    def test_every_command_is_frozen(self) -> None:
        """A command is a record of a decision already made, not a builder."""
        for command in ONE_OF_EACH:
            assert dataclasses.is_dataclass(command)
            first_field = dataclasses.fields(command)[0]
            with pytest.raises(dataclasses.FrozenInstanceError):
                setattr(command, first_field.name, getattr(command, first_field.name))


class TestClaimBatchPreconditions:
    """A batch fence is only meaningful if each task appears once."""

    def test_duplicate_task_id_is_rejected_at_construction(self) -> None:
        """Two generations for one row make the fence ambiguous.

        Caught where the batch is built rather than discovered as a row count
        that does not match expectations.
        """
        with pytest.raises(ValueError, match='duplicate task id'):
            OwnedClaimBatch(
                worker_id='w1',
                claim_generations=(('t1', _GENERATION), ('t1', None)),
            )

    def test_distinct_task_ids_are_accepted(self) -> None:
        batch = OwnedClaimBatch(
            worker_id='w1',
            claim_generations=(('t1', _GENERATION), ('t2', None)),
        )
        assert batch.task_ids() == ('t1', 't2')
        assert batch.generations() == (_GENERATION, None)


class TestDiscoveryBatchPreconditions:
    """The bound is load-bearing: a batch that does not bound is a caller error."""

    @pytest.mark.parametrize('batch_size', [0, -1])
    def test_a_non_positive_expiry_bound_is_rejected(
        self, batch_size: int,
    ) -> None:
        with pytest.raises(ValueError, match='positive integer'):
            ExpirePendingTasks(
                batch_size=batch_size, result_json='{}', error_code='TASK_EXPIRED',
            )

    @pytest.mark.parametrize('batch_size', [0, -1])
    def test_a_non_positive_orphan_sweep_bound_is_rejected(
        self, batch_size: int,
    ) -> None:
        with pytest.raises(ValueError, match='positive integer'):
            CancelOrphanedTasks(batch_size=batch_size)

    def test_positive_bounds_construct(self) -> None:
        assert ExpirePendingTasks(
            batch_size=1, result_json='{}', error_code='TASK_EXPIRED',
        ).batch_size == 1
        assert CancelOrphanedTasks(batch_size=500).batch_size == 500


class TestFenceCoverageMatchesTheWriters:
    """Every ownership model the writers use has exactly one fence type."""

    def test_fence_kinds_used_match_the_writer_fence_kinds(self) -> None:
        from tests.lifecycle_matrix import Fence

        used = {
            type(fence_of(c)).__name__ if fence_of(c) is not None else None
            for c in ONE_OF_EACH
        }
        expected = {
            Fence.CALLER_ROW_LOCK: 'CallerHoldsRowLock',
            Fence.PRIOR_LOCKED_SELECT: 'PriorLockedRead',
            Fence.WORKER: 'WorkerOwned',
            Fence.WORKER_AND_GENERATION: 'OwnedClaim',
            Fence.WORKER_AND_GENERATION_PAIRWISE: 'OwnedClaimBatch',
            Fence.NONE: None,
        }
        from_matrix = {expected[row.fence] for row in MATRIX}
        assert used == from_matrix
