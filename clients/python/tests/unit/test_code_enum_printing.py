"""A built-in code enum prints its code text: `str(member)` and
`f"{member}"` give the wire text, one member per generated family."""

from __future__ import annotations

from enum import StrEnum

import pytest

from horsies._catalog.broker import DatabaseErrorCode
from horsies._catalog.error_codes import HorsiesErrorCode
from horsies._catalog.history import RerunErrorCode, TaskAbsenceReason
from horsies._catalog.monitoring import (
    MonitoringQueryErrorCode,
    MonitoringQueryRefusalCode,
    TaskActionErrorCode,
)
from horsies._catalog.status import (
    TaskAttemptOutcome,
    WorkflowNodeStatus,
    WorkflowStatus,
)
from horsies._catalog.task_codes import (
    ContractCode,
    OperationalCode,
    OutcomeCode,
    RetrievalCode,
)
from horsies._catalog.task_send import SeatRelayCode, TaskSendErrorCode
from horsies._catalog.workflow_handle import WorkflowHandleErrorCode
from horsies._catalog.workflow_start import WorkflowStartErrorCode

pytestmark = [pytest.mark.unit]

TEXT_FAMILIES: list[type[StrEnum]] = [
    OperationalCode,
    ContractCode,
    RetrievalCode,
    OutcomeCode,
    HorsiesErrorCode,
    DatabaseErrorCode,
    TaskSendErrorCode,
    SeatRelayCode,
    WorkflowStartErrorCode,
    WorkflowHandleErrorCode,
    MonitoringQueryErrorCode,
    TaskActionErrorCode,
    MonitoringQueryRefusalCode,
    RerunErrorCode,
    TaskAbsenceReason,
    TaskAttemptOutcome,
    WorkflowStatus,
    WorkflowNodeStatus,
]


@pytest.mark.parametrize('family', TEXT_FAMILIES, ids=lambda family: family.__name__)
def test_a_member_prints_its_code_text(family: type[StrEnum]) -> None:
    member = next(iter(family))
    assert str(member) == member.value
    assert f'{member}' == member.value
    assert member == member.value

