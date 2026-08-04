"""Adapter-side enforcement of the id-keyed batch wire contract."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from horsies.core.lifecycle.commands import AbandonOwnedNodes
from horsies.core.lifecycle.fences import OwnedClaimBatch
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import Applied, ObservedTaskState
from horsies.core.lifecycle.persistence import (
    _reconstruct_id_keyed_batch,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.types.status import TaskStatus

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)


def _command() -> AbandonOwnedNodes:
    return AbandonOwnedNodes(
        fence=OwnedClaimBatch(
            worker_id='worker',
            claim_generations=(('one', _NOW), ('two', _NOW)),
        ),
    )


def _outcome(task_id: str, ordinality: int | None) -> Applied:
    return Applied(
        task_id=task_id,
        ordinality=ordinality,
        terminal_at=_NOW,
        kind=TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH,
        observed=ObservedTaskState(
            status=TaskStatus.CLAIMED,
            worker_id='worker',
            claimed_at=_NOW,
        ),
    )


def test_reconstructs_by_ordinal_instead_of_result_order() -> None:
    outcomes = _reconstruct_id_keyed_batch(
        [_outcome('two', 2), _outcome('one', 1)],
        expected_count=2,
        command=_command(),
    )

    assert [outcome.task_id for outcome in outcomes] == ['one', 'two']


@pytest.mark.parametrize(
    ('outcomes', 'message'),
    [
        ([_outcome('one', None), _outcome('two', 2)], 'without ordinality'),
        ([_outcome('one', 1), _outcome('two', 1)], 'duplicate ordinality'),
        ([_outcome('one', 1)], 'ordinal set does not match'),
        ([_outcome('one', 1), _outcome('two', 3)], 'ordinal set does not match'),
    ],
)
def test_rejects_an_invalid_ordinal_contract(
    outcomes: list[Applied],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        _reconstruct_id_keyed_batch(
            outcomes,
            expected_count=2,
            command=_command(),
        )
