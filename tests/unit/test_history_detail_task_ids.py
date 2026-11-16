"""The history detail read gives its task ids as ``uuid.UUID``.

A connection that horsies builds reads uuid columns as text; a connection
that a caller passes in (``rerun_task``) can read them as ``uuid.UUID``.
Both give the same detail.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

import pytest

from horsies.core.history.reads.detail import (
    _decode_history_detail,  # pyright: ignore[reportPrivateUsage]
)

from tests.unit.test_history_detail_status_parse import (
    _history_row,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = [pytest.mark.unit]

TASK_ID = UUID('2d3e4f50-6172-4c83-8d94-e5f60718293a')
RERUN_OF_TASK_ID = UUID('0198c0de-0000-7000-8000-000000000001')
RERUN_ROOT_TASK_ID = UUID('0198c0de-0000-7000-8000-000000000002')


def _as_text(value: UUID) -> object:
    return str(value)


def _as_uuid(value: UUID) -> object:
    return value


@pytest.mark.parametrize('presentation', [_as_text, _as_uuid], ids=['text', 'uuid'])
def test_task_and_lineage_ids_decode_to_uuid(
    presentation: Callable[[UUID], object],
) -> None:
    row = _history_row('FAILED')
    row.task_id = presentation(TASK_ID)
    row.rerun_of_task_id = presentation(RERUN_OF_TASK_ID)
    row.rerun_root_task_id = presentation(RERUN_ROOT_TASK_ID)

    detail = _decode_history_detail(row)

    assert detail.task_id == TASK_ID
    assert detail.rerun_of_task_id == RERUN_OF_TASK_ID
    assert detail.rerun_root_task_id == RERUN_ROOT_TASK_ID


def test_a_row_without_lineage_has_no_lineage_ids() -> None:
    detail = _decode_history_detail(_history_row('FAILED'))

    assert detail.task_id == TASK_ID
    assert detail.rerun_of_task_id is None
    assert detail.rerun_root_task_id is None
