"""How the core case of a pair with a decided difference reports its judgement.

A difference that the decided fact explains is an expected failure that
names the fact, its decision and the difference. A difference with an entry
that the fact does not explain fails the pair. No database: the capture and
the judgement are put in place by hand.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final, cast

import pytest

from horsies.core.types.result import Ok
from tests.dualrun.capture import NotificationListener
from tests.dualrun.compare import MissingRow, SnapshotDiff
from tests.dualrun.normalize import NormalizedSnapshot
from tests.dualrun.operations import plugin
from tests.dualrun.operations.lifecycle import LIFECYCLE_DECIDED_DIFFERENCES
from tests.dualrun.operations.plugin import (
    PairCase,
    _after_core_body,  # pyright: ignore[reportPrivateUsage]
    _references,  # pyright: ignore[reportPrivateUsage]
)
from tests.dualrun.operations.rebinding import CallCounts
from tests.dualrun.operations.table import Implementation, PairCompare
from tests.dualrun import pair_outcomes
from tests.dualrun.pair_outcomes import (
    DecidedDifferenceSeen,
    DualRunMismatch,
    PairJudgement,
    PairKey,
    ReferenceCaptured,
)
from tests.dualrun.values import Cell, ExactValue, RunWindow

pytestmark = [pytest.mark.unit]

(CLOSE,) = LIFECYCLE_DECIDED_DIFFERENCES
START: Final = datetime(2026, 11, 18, 10, 5, tzinfo=UTC)
WINDOW: Final = RunWindow(start=START, end=START + timedelta(minutes=1))
EMPTY: Final = NormalizedSnapshot(tables=(), ambiguities=())
DIFF: Final = SnapshotDiff(
    entries=(
        MissingRow(
            table='horsies_tasks',
            identity=(Cell(column='id', value=ExactValue(text='task#3')),),
            cells=(Cell(column='status', value=ExactValue(text='RUNNING')),),
        ),
    ),
)


@dataclass
class _Config:
    """The part of ``pytest.Config`` that the pair records use."""

    stash: pytest.Stash = field(default_factory=pytest.Stash)


def _core_case(
    judgement: PairJudgement, compare: PairCompare, monkeypatch: pytest.MonkeyPatch
) -> tuple[pytest.Config, PairCase]:
    """A core case whose capture gives an empty snapshot and whose judgement is ``judgement``."""

    def captured(case: PairCase) -> Ok[tuple[NormalizedSnapshot, RunWindow]]:
        return Ok((EMPTY, WINDOW))

    def judged(*arguments: object) -> PairJudgement:
        return judgement

    monkeypatch.setattr(plugin, '_capture', captured)
    monkeypatch.setattr(pair_outcomes, 'judge_pair', judged)
    config = cast(pytest.Config, _Config())
    key = PairKey(case_name=CLOSE.node_id)
    _references(config)[key] = ReferenceCaptured(snapshot=EMPTY, window=WINDOW)
    case = PairCase(
        key=key,
        implementation=Implementation.CORE,
        counts=CallCounts(),
        url='not a database',
        start=START,
        listener=cast(NotificationListener, None),
        row_limit=10,
        compare=compare,
    )
    return config, case


def test_a_difference_that_the_decided_fact_explains_is_an_expected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, case = _core_case(
        DecidedDifferenceSeen(diff=DIFF, decided=CLOSE), CLOSE, monkeypatch
    )

    with pytest.raises(pytest.xfail.Exception) as expected:
        _after_core_body(config, case)

    text = str(expected.value)
    assert text.startswith(
        f'DecidedDifference RECEIVED_WRITE_RUNS_TO_ITS_END ({CLOSE.fact.value}). '
        f'Decision: {CLOSE.decision}.\n'
    )
    assert "horsies_tasks [id='task#3']: row only in the reference" in text


def test_a_difference_that_the_decided_fact_does_not_explain_fails_the_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, case = _core_case(DualRunMismatch(diff=DIFF), CLOSE, monkeypatch)

    with pytest.raises(pytest.fail.Exception) as failed:
        _after_core_body(config, case)

    assert not isinstance(failed.value, pytest.xfail.Exception)
    assert str(failed.value).startswith(
        'DualRunMismatch under the decided difference RECEIVED_WRITE_RUNS_TO_ITS_END, '
        'with entries that it does not explain:\n'
    )
