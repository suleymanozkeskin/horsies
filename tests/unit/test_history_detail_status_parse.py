"""The history detail read parses the stored status one time, into ``TaskStatus``.

Schema v49 drops the status CHECK of the history parent, so a value that is
not a member can be stored out of band. The decode raises
``HistoryContractError`` for it, before any reader sees the row.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.detail import (
    _decode_history_detail,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.types.status import TaskStatus

from tests.attempt_snapshot_support import encode_attempt_snapshot

pytestmark = [pytest.mark.unit]

TASK_ID = '2d3e4f50-6172-4c83-8d94-e5f60718293a'
NOW = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)


def _history_row(status: str) -> SimpleNamespace:
    """The columns of one staged detail row, with an empty attempt snapshot."""
    snapshot = encode_attempt_snapshot(())
    return SimpleNamespace(
        task_id=TASK_ID,
        task_name='report',
        queue_name='default',
        priority=100,
        status=status,
        terminalization_kind='FAIL_RUNNING',
        terminal_at=NOW,
        retention_class_key='default',
        enqueued_at=NOW,
        created_at=NOW,
        sent_at=None,
        claimed_at=None,
        started_at=None,
        good_until=None,
        retry_count=0,
        max_retries=0,
        last_claimed_worker_id=None,
        last_worker_hostname=None,
        last_worker_pid=None,
        result_envelope_version=1,
        result_codec='json-utf8',
        result_content_type='application/json',
        result_payload=None,
        prior_result_payload=None,
        result_digest=None,
        error_code=None,
        final_failed_reason=None,
        rerun_of_task_id=None,
        rerun_root_task_id=None,
        workflow_id=None,
        is_workflow_task=False,
        rerun_input_disposition='DECLINED_BY_POLICY',
        rerun_input_version=None,
        rerun_input_codec=None,
        rerun_input_content_type=None,
        rerun_input_digest=None,
        rerun_input_inline=None,
        rerun_input_reference=None,
        attempt_archive_version=snapshot.version,
        attempt_snapshot_codec=snapshot.codec,
        attempt_snapshot_content_type=snapshot.content_type,
        attempt_snapshot=snapshot.payload,
        attempt_snapshot_digest=snapshot.digest,
    )


@pytest.mark.parametrize('status', list(TaskStatus), ids=lambda status: status.value)
def test_a_member_status_decodes_to_the_member(status: TaskStatus) -> None:
    detail = _decode_history_detail(_history_row(status.value))

    assert detail.status is status
    assert detail.attempts == ()


def test_a_status_outside_the_enum_raises_a_contract_error() -> None:
    with pytest.raises(HistoryContractError) as raised:
        _decode_history_detail(_history_row('NOT_A_STATUS'))

    assert str(raised.value) == (
        f"history row {TASK_ID} has status 'NOT_A_STATUS', which is not a "
        'task status'
    )
