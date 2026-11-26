"""Unit tests for the monitoring query API's derivation helpers.

Covers:
- ``none_if_blank`` empty-text normalization
- ``as_utc`` awareness coercion
- ``elapsed_seconds`` / ``duration_seconds`` duration semantics (exact seconds), including the
  open-span rule
- ``categorize_error_code`` taxonomy mapping and its drift guards
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
)
from horsies.monitoring import (
    ErrorCategory,
    as_utc,
    categorize_error_code,
    duration_seconds,
    elapsed_seconds,
    none_if_blank,
)

pytestmark = [pytest.mark.unit]

UTC = timezone.utc


class TestNoneIfBlank:
    """Empty and whitespace-only text normalizes to None."""

    @pytest.mark.parametrize(
        'value,expected',
        [
            (None, None),
            ('', None),
            ('   ', None),
            ('\t\n', None),
            ('code', 'code'),
            ('  code  ', 'code'),
        ],
    )
    def test_normalizes(self, value: str | None, expected: str | None) -> None:
        assert none_if_blank(value) == expected


class TestAsUtc:
    """Naive datetimes are read as UTC; aware datetimes pass through."""

    def test_naive_becomes_utc(self) -> None:
        naive = datetime(2026, 7, 26, 12, 0, 0)

        assert as_utc(naive) == datetime(2026, 7, 26, 12, 0, 0, tzinfo=UTC)

    def test_aware_is_unchanged(self) -> None:
        aware = datetime(2026, 7, 26, 12, 0, 0, tzinfo=timezone(timedelta(hours=3)))

        assert as_utc(aware) is aware

    def test_mixed_awareness_subtraction_does_not_raise(self) -> None:
        naive_start = datetime(2026, 7, 26, 12, 0, 0)
        aware_end = datetime(2026, 7, 26, 12, 0, 30, tzinfo=UTC)

        assert elapsed_seconds(naive_start, aware_end) == 30.0


class TestElapsedSeconds:
    """Seconds between two instants, exact to the microsecond."""

    def test_none_start_is_none(self) -> None:
        assert elapsed_seconds(None, datetime.now(UTC)) is None

    def test_whole_seconds_are_a_float(self) -> None:
        start = datetime(2026, 7, 26, 12, 0, 0, tzinfo=UTC)
        end = start + timedelta(seconds=90)

        measured = elapsed_seconds(start, end)

        assert isinstance(measured, float)
        assert measured == 90.0

    def test_fractional_seconds_keep_their_microseconds(self) -> None:
        start = datetime(2026, 7, 26, 12, 0, 0, tzinfo=UTC)
        end = start + timedelta(seconds=1, microseconds=999_999)

        assert elapsed_seconds(start, end) == 1.999999

    def test_end_before_start_is_negative(self) -> None:
        start = datetime(2026, 7, 26, 12, 0, 0, tzinfo=UTC)
        end = start - timedelta(seconds=1, microseconds=500_000)

        assert elapsed_seconds(start, end) == -1.5

    def test_missing_end_counts_to_now(self) -> None:
        start = datetime.now(UTC) - timedelta(seconds=20)

        measured = elapsed_seconds(start, None)

        assert measured is not None
        assert 20 <= measured <= 120


class TestDurationSeconds:
    """An open span counts up only when the caller says it is still live."""

    def test_none_start_is_none(self) -> None:
        assert duration_seconds(None, datetime.now(UTC), live=True) is None

    def test_closed_span_ignores_live(self) -> None:
        start = datetime(2026, 7, 26, 12, 0, 0, tzinfo=UTC)
        end = start + timedelta(seconds=5)

        assert duration_seconds(start, end, live=False) == 5.0
        assert duration_seconds(start, end, live=True) == 5.0

    def test_open_span_when_live_counts_up(self) -> None:
        start = datetime.now(UTC) - timedelta(seconds=15)

        measured = duration_seconds(start, None, live=True)

        assert measured is not None
        assert 15 <= measured <= 120

    def test_open_span_when_not_live_is_none(self) -> None:
        start = datetime.now(UTC) - timedelta(seconds=15)

        assert duration_seconds(start, None, live=False) is None


class TestCategorizeErrorCode:
    """Error codes resolve to the family their enum declares."""

    @pytest.mark.parametrize(
        'code,expected',
        [
            (None, None),
            ('', None),
            ('   ', None),
        ],
    )
    def test_absent_code_has_no_category(
        self, code: str | None, expected: None
    ) -> None:
        assert categorize_error_code(code) is expected

    @pytest.mark.parametrize(
        'family,expected',
        [
            (OperationalErrorCode, ErrorCategory.OPERATIONAL),
            (ContractCode, ErrorCategory.CONTRACT),
            (RetrievalCode, ErrorCategory.RETRIEVAL),
            (OutcomeCode, ErrorCategory.OUTCOME),
        ],
    )
    def test_every_builtin_member_maps_to_its_family(
        self,
        family: type[OperationalErrorCode]
        | type[ContractCode]
        | type[RetrievalCode]
        | type[OutcomeCode],
        expected: ErrorCategory,
    ) -> None:
        for member in family:
            assert categorize_error_code(member.value) is expected

    def test_unknown_code_is_domain(self) -> None:
        assert categorize_error_code('MY_BUSINESS_ERROR') is ErrorCategory.DOMAIN

    def test_former_hydration_code_is_domain(self) -> None:
        """``PYDANTIC_HYDRATION_ERROR`` is not a built-in code, so a
        stored row with it is a domain error, not a contract error."""
        assert (
            categorize_error_code('PYDANTIC_HYDRATION_ERROR')
            is ErrorCategory.DOMAIN
        )

    def test_whitespace_is_stripped_before_lookup(self) -> None:
        assert categorize_error_code('  TASK_CANCELLED  ') is ErrorCategory.OUTCOME

    def test_builtin_families_do_not_collide(self) -> None:
        """A value shared by two families would silently take one category."""
        values = [
            member.value
            for family in (
                OperationalErrorCode,
                ContractCode,
                RetrievalCode,
                OutcomeCode,
            )
            for member in family
        ]

        assert len(values) == len(set(values))
