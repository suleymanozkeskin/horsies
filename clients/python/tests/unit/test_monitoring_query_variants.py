"""The monitoring query error variants.

``MonitoringQueryError`` is a union with one frozen variant per
``MonitoringQueryErrorCode`` member. A variant's repr hides the message and
the cause, and only a database failure can be retryable. The reads run in
the core; the production facades are tested on a mocked core handle in
``tests/unit/test_monitoring_reads_in_core.py``.
"""

from __future__ import annotations

import pytest

from horsies.monitoring.errors import (
    MonitoringQueryDatabaseOperationFailed,
    MonitoringQueryError,
    MonitoringQueryErrorCode,
    MonitoringQueryHistoryContractViolation,
)
from tests.union_members import union_members

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-91af'
CAUSE_MARKER = 'cause-marker-91af'


def _every_variant(cause: BaseException) -> list[MonitoringQueryError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    return [
        MonitoringQueryDatabaseOperationFailed(message=message, retryable=True, cause=cause),
        MonitoringQueryHistoryContractViolation(message=message, cause=cause),
    ]


def _variant_id(error: MonitoringQueryError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in union_members(MonitoringQueryError)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(MonitoringQueryErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(union_members(MonitoringQueryError))

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id,
    )
    def test_repr_hides_message_and_cause(self, error: MonitoringQueryError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_only_a_database_failure_can_be_retryable(
        self, error: MonitoringQueryError,
    ) -> None:
        match error:
            case MonitoringQueryDatabaseOperationFailed():
                assert error.retryable is True
            case _:
                assert error.retryable is False


