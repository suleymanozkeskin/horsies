"""Shape pins for the phase-2 attempt bound and preparation's reach.

The runtime behavior (attempts climbing, the quarantine transition, the
pipeline crossing with genuinely legacy rows) is proven by integration
suites; these pins hold the structural facts those suites assume: the
bound's config validation refuses out-of-range values by name, discovery
excludes rows at the bound, preparation selects without a status filter
and stamps the class it resolved, and a class-less row resolves to the
forever key — the same authority the relocation coalesce imports.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from pydantic import ValidationError

from horsies.core.history.cutover.preparation import (
    _prepare_one,
    prepare_legacy_batch,
)
from horsies.core.history.ddl.tables import FOREVER_CLASS_KEY
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.workflows.phase2_recovery import DISCOVER_PENDING_SQL

pytestmark = [pytest.mark.unit]


class TestQuarantineBoundConfig:
    @pytest.mark.parametrize('value', [2, 1_001])
    def test_out_of_range_bounds_are_refused(self, value: int) -> None:
        with pytest.raises(
            ValidationError, match='phase2_quarantine_after_attempts'
        ):
            RecoveryConfig(
                **cast(
                    'dict[str, Any]',
                    {'phase2_quarantine_after_attempts': value},
                )
            )

    def test_the_default_holds_the_derived_value(self) -> None:
        assert RecoveryConfig().phase2_quarantine_after_attempts == 25


class TestDiscoveryExcludesTheBound:
    def test_discovery_carries_the_attempt_predicate(self) -> None:
        """A row at the bound is not selected, so an unresolvable row
        costs a bounded number of passes even when its quarantine
        transition refused."""
        rendered = str(DISCOVER_PENDING_SQL)
        assert 'attempt_count <' in rendered


class TestPreparationReach:
    def test_selection_has_no_status_filter(self) -> None:
        """Preparation reaches every unprepared row, live rows included:
        the tighten's entry check counts unprepared rows over the whole
        table, and drain deliberately lets pending rows survive."""
        source = inspect.getsource(prepare_legacy_batch)
        assert 'status NOT IN' not in source
        assert 'prepared_rerun_input_disposition IS NULL' in source

    def test_the_update_stamps_the_resolved_class(self) -> None:
        source = inspect.getsource(prepare_legacy_batch)
        assert 'retention_class_key = :retention_class_key' in source


@dataclass(frozen=True, slots=True)
class _StubRow:
    id: str
    task_name: str
    queue_name: str
    priority: int
    args: str | None
    kwargs: str | None
    task_options: str | None
    good_until: datetime | None
    retention_class_key: str | None
    retain_rerun_input: bool | None


def _row(retention_class_key: str | None) -> _StubRow:
    return _StubRow(
        id='11111111-2222-3333-4444-555555555555',
        task_name='legacy.task',
        queue_name='default',
        priority=50,
        args='[1]',
        kwargs=None,
        task_options=None,
        good_until=datetime(2026, 1, 1, tzinfo=UTC),
        retention_class_key=retention_class_key,
        retain_rerun_input=None,
    )


class TestClassResolution:
    def test_a_classless_row_resolves_to_forever(self) -> None:
        """No recorded policy means no deletion policy applied — the
        forever key, imported from the same authority the relocation
        projection coalesces with, so the two stages cannot disagree."""
        prepared = _prepare_one(_row(None), retain_default=False)
        assert prepared.retention_class_key == FOREVER_CLASS_KEY

    def test_a_classed_row_keeps_its_class(self) -> None:
        prepared = _prepare_one(_row('standard_30d'), retain_default=False)
        assert prepared.retention_class_key == 'standard_30d'
