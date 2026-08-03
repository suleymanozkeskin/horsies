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
    TerminalResultPayload,
    TerminalizationCommand,
    fence_of,
    target_status,
)
from horsies.core.lifecycle.fences import (
    CallerHoldsRowLock,
    OwnedClaim,
    OwnedClaimBatch,
    WorkflowStateUnderLock,
)
from tests.lifecycle_matrix import MATRIX

pytestmark = [pytest.mark.unit]

_GENERATION = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
_PAYLOAD = TerminalResultPayload(
    result_json='{}', error_code='CODE', failed_reason=None,
)
_OWNED = OwnedClaim(worker_id='w1', claimed_at=_GENERATION)
_BATCH = OwnedClaimBatch(worker_id='w1', claim_generations=(('t1', _GENERATION),))
_PAUSED = WorkflowStateUnderLock(
    workflow_ids=('wf1',),
    required_workflow_status='PAUSED',
    required_node_statuses=('ENQUEUED', 'RUNNING'),
)
_CANCELLED_WF = WorkflowStateUnderLock(
    workflow_ids=('wf1',),
    required_workflow_status='CANCELLED',
    required_node_statuses=('ENQUEUED',),
)

# One instance of every variant. Anything missing here fails the count check
# below rather than silently going unexercised by the dispatch tests.
ONE_OF_EACH: tuple[TerminalizationCommand, ...] = (
    CompleteLockedTask(task_id='t1', worker_id='w1', payload=_PAYLOAD),
    CompleteTaskFused(
        task_id='t1',
        fence=_OWNED,
        payload=_PAYLOAD,
        notify_channel='task_queue_default',
        notify_payload='capacity:t1',
    ),
    FailLockedTask(task_id='t1', worker_id='w1', payload=_PAYLOAD),
    FailStaleTask(
        task_id='t1',
        stale_after_seconds=60,
        finalizing_stale_after_seconds=60,
        payload=_PAYLOAD,
    ),
    ExpireOwnedClaim(task_id='t1', worker_id='w1', payload=_PAYLOAD),
    ExpirePendingTasks(batch_size=100, payload=_PAYLOAD),
    CancelLockedTask(
        task_id='t1',
        fence=CallerHoldsRowLock(),
        permitted_source_statuses=('PENDING', 'CLAIMED'),
        payload=_PAYLOAD,
    ),
    CancelOwnedOrphan(task_id='t1', fence=_OWNED, payload=_PAYLOAD),
    CancelOrphanedTasks(batch_size=100, payload=_PAYLOAD),
    AbandonOwnedNode(task_id='t1', fence=_OWNED, payload=_PAYLOAD),
    AbandonOwnedNodes(fence=_BATCH, payload=_PAYLOAD),
    AbandonNodesOfPausedWorkflows(fence=_PAUSED, payload=_PAYLOAD),
    CancelOwnedNode(
        task_id='t1',
        fence=_OWNED,
        accepts_requeued_pending=True,
        payload=_PAYLOAD,
    ),
    CancelOwnedNodes(fence=_BATCH, payload=_PAYLOAD),
    CancelNodesOfCancelledWorkflow(fence=_CANCELLED_WF, payload=_PAYLOAD),
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
        from_commands = {target_status(c) for c in ONE_OF_EACH}
        from_writers = {row.target_status for row in MATRIX}
        assert from_commands == from_writers


class TestDispatchIsExhaustive:
    @pytest.mark.parametrize('command', ONE_OF_EACH, ids=lambda c: type(c).__name__)
    def test_target_status_returns_for_every_variant(
        self,
        command: TerminalizationCommand,
    ) -> None:
        """No variant falls through; the checker rejects an omitted case."""
        assert target_status(command) in {
            'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED',
        }

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
        """The guard describes its targets; a separate id list could disagree."""
        for command in ONE_OF_EACH:
            if isinstance(fence_of(command), WorkflowStateUnderLock):
                assert not hasattr(command, 'task_id'), type(command).__name__

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

    def test_every_command_is_frozen(self) -> None:
        """A command is a record of a decision already made, not a builder."""
        for command in ONE_OF_EACH:
            assert dataclasses.is_dataclass(command)
            with pytest.raises(dataclasses.FrozenInstanceError):
                setattr(command, 'payload', _PAYLOAD)
