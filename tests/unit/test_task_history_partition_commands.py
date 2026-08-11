"""The partition maintenance command vocabulary and its constructors.

Commands validate at construction, so an executor may rely on every command
it receives being well-formed. These tests pin that property from both
sides: well-formed values construct, and each documented rejection actually
rejects. Field enumeration keeps the union honest — a new field or variant
has to be accounted for here before it can ship.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from typing import get_args

import pytest

from horsies.core.history.commands import (
    CollectPartitionHealth,
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    EnsureLeafCoverage,
    FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf,
    LeafBounds,
    LeafRef,
    PartitionMaintenanceCommand,
    is_safe_identifier,
)
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    leaf_id_index_name,
)

pytestmark = [pytest.mark.unit]


UTC = timezone.utc
LOWER = datetime(2026, 8, 6, tzinfo=UTC)
UPPER = datetime(2026, 8, 7, tzinfo=UTC)


def make_leaf_ref(**overrides: object) -> LeafRef:
    values: dict[str, object] = {
        'leaf_name': 'horsies_task_history_finite_30d_2026_08_06',
        'class_key': 'finite_30d_v1',
        'bounds': LeafBounds(lower=LOWER, upper=UPPER),
    }
    values.update(overrides)
    return LeafRef(**values)  # type: ignore[arg-type]


class TestSafeIdentifier:
    def test_accepts_lowercase_snake_names(self) -> None:
        assert is_safe_identifier('horsies_task_history_2026_08_06')

    @pytest.mark.parametrize(
        'name',
        [
            '',
            'Upper',
            '1leading_digit',
            'has-hyphen',
            'has space',
            'semi;colon',
            'quoted"name',
            'a' * 64,
        ],
    )
    def test_rejects_unsafe_names(self, name: str) -> None:
        assert not is_safe_identifier(name)

    def test_accepts_maximum_length_identifier(self) -> None:
        assert is_safe_identifier('a' * 63)


class TestLeafBounds:
    def test_constructs_aware_increasing_bounds(self) -> None:
        bounds = LeafBounds(lower=LOWER, upper=UPPER)
        assert bounds.spans_one_day

    def test_rejects_naive_lower(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            LeafBounds(lower=LOWER.replace(tzinfo=None), upper=UPPER)

    def test_rejects_naive_upper(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            LeafBounds(lower=LOWER, upper=UPPER.replace(tzinfo=None))

    def test_rejects_equal_bounds(self) -> None:
        with pytest.raises(ValueError, match='increasing'):
            LeafBounds(lower=LOWER, upper=LOWER)

    def test_rejects_inverted_bounds(self) -> None:
        with pytest.raises(ValueError, match='increasing'):
            LeafBounds(lower=UPPER, upper=LOWER)

    def test_two_day_span_is_not_daily(self) -> None:
        bounds = LeafBounds(lower=LOWER, upper=LOWER + timedelta(days=2))
        assert not bounds.spans_one_day


class TestLeafRef:
    def test_constructs_with_safe_names(self) -> None:
        leaf = make_leaf_ref()
        assert leaf.class_key == 'finite_30d_v1'

    def test_rejects_unsafe_leaf_name(self) -> None:
        with pytest.raises(ValueError, match='safe PostgreSQL identifier'):
            make_leaf_ref(leaf_name='drop table; --')

    def test_rejects_empty_class_key(self) -> None:
        with pytest.raises(ValueError, match='non-empty'):
            make_leaf_ref(class_key='')


class TestCreateDailyHistoryLeaf:
    def test_accepts_one_day_bounds(self) -> None:
        command = CreateDailyHistoryLeaf(leaf=make_leaf_ref())
        assert command.leaf.bounds.spans_one_day

    def test_rejects_multi_day_bounds(self) -> None:
        wide = make_leaf_ref(
            bounds=LeafBounds(lower=LOWER, upper=LOWER + timedelta(days=2)),
        )
        with pytest.raises(ValueError, match='exactly one day'):
            CreateDailyHistoryLeaf(leaf=wide)


class TestEnsureLeafCoverage:
    def test_accepts_horizon_at_floor(self) -> None:
        command = EnsureLeafCoverage(class_key='finite_30d_v1', horizon_days=2)
        assert command.horizon_days == 2

    @pytest.mark.parametrize('horizon', [1, 0, -1])
    def test_rejects_horizon_below_health_floor(self, horizon: int) -> None:
        with pytest.raises(ValueError, match='red line'):
            EnsureLeafCoverage(class_key='finite_30d_v1', horizon_days=horizon)

    def test_rejects_empty_class_key(self) -> None:
        with pytest.raises(ValueError, match='non-empty'):
            EnsureLeafCoverage(class_key='', horizon_days=3)


class TestDetachExpiredHistoryLeaf:
    def test_accepts_absent_timeout(self) -> None:
        command = DetachExpiredHistoryLeaf(
            leaf=make_leaf_ref(),
            quarantine_horizon=None,
            statement_timeout_ms=None,
        )
        assert command.statement_timeout_ms is None

    def test_accepts_positive_timeout(self) -> None:
        command = DetachExpiredHistoryLeaf(
            leaf=make_leaf_ref(),
            quarantine_horizon=None,
            statement_timeout_ms=5_000,
        )
        assert command.statement_timeout_ms == 5_000

    @pytest.mark.parametrize('timeout_ms', [0, -1])
    def test_rejects_non_positive_timeout(self, timeout_ms: int) -> None:
        with pytest.raises(ValueError, match='positive'):
            DetachExpiredHistoryLeaf(
                leaf=make_leaf_ref(),
                quarantine_horizon=None,
                statement_timeout_ms=timeout_ms,
            )

    @pytest.mark.parametrize(
        'field_name', ['quarantine_horizon', 'statement_timeout_ms']
    )
    def test_field_has_no_default(self, field_name: str) -> None:
        # Every call site states its detach posture explicitly; a silent
        # default would let existing callers change behavior without
        # review. statement_timeout_ms joined this rule after two call
        # sites took its None default and waited on a lock unbounded
        # while holding the cluster-wide maintenance gate -- an omission
        # that read as "nothing to say" rather than as a choice.
        for field in dataclasses.fields(DetachExpiredHistoryLeaf):
            if field.name == field_name:
                assert field.default is dataclasses.MISSING
                return
        raise AssertionError(f'{field_name} field is missing')

    def test_finalize_timeout_has_no_default(self) -> None:
        # A blocked FINALIZE blocks every future detach on the parent,
        # so unbounded is the most expensive default in the family.
        for field in dataclasses.fields(FinalizeInterruptedLeafDetach):
            if field.name == 'statement_timeout_ms':
                assert field.default is dataclasses.MISSING
                return
        raise AssertionError('statement_timeout_ms field is missing')

    @pytest.mark.parametrize('timeout_ms', [0, -1])
    def test_finalize_rejects_non_positive_timeout(
        self, timeout_ms: int
    ) -> None:
        with pytest.raises(ValueError, match='positive'):
            FinalizeInterruptedLeafDetach(
                leaf=make_leaf_ref(), statement_timeout_ms=timeout_ms
            )

    def test_accepts_positive_horizon(self) -> None:
        command = DetachExpiredHistoryLeaf(
            leaf=make_leaf_ref(),
            quarantine_horizon=timedelta(days=7),
            statement_timeout_ms=None,
        )
        assert command.quarantine_horizon == timedelta(days=7)

    @pytest.mark.parametrize(
        'horizon', [timedelta(0), timedelta(seconds=-1)]
    )
    def test_rejects_non_positive_horizon(self, horizon: object) -> None:
        with pytest.raises(ValueError, match='horizon must be positive'):
            DetachExpiredHistoryLeaf(
                leaf=make_leaf_ref(),
                quarantine_horizon=horizon,  # type: ignore[arg-type]
                statement_timeout_ms=None,
            )


class TestCollectPartitionHealth:
    def test_requires_explicit_privilege_mode(self) -> None:
        fields = {
            field.name for field in dataclasses.fields(CollectPartitionHealth)
        }
        assert fields == {'class_key', 'application_managed'}
        for field in dataclasses.fields(CollectPartitionHealth):
            assert field.default is dataclasses.MISSING

    def test_rejects_empty_class_key(self) -> None:
        with pytest.raises(ValueError, match='non-empty'):
            CollectPartitionHealth(class_key='', application_managed=True)


class TestCommandUnion:
    """Every variant is accounted for; a new one must justify itself here."""

    def test_union_members_are_exactly_the_maintenance_operations(self) -> None:
        assert set(get_args(PartitionMaintenanceCommand.__value__)) == {
            InspectHistoryLeaf,
            CreateDailyHistoryLeaf,
            EnsureLeafCoverage,
            DetachExpiredHistoryLeaf,
            FinalizeInterruptedLeafDetach,
            DropDetachedHistoryLeaf,
            CollectPartitionHealth,
        }

    def test_every_variant_is_frozen(self) -> None:
        for variant in get_args(PartitionMaintenanceCommand.__value__):
            params = getattr(variant, '__dataclass_params__')
            assert params.frozen, f'{variant.__name__} must be frozen'

    def test_every_field_is_accounted_for(self) -> None:
        permitted: dict[type, set[str]] = {
            InspectHistoryLeaf: {'leaf'},
            CreateDailyHistoryLeaf: {'leaf'},
            EnsureLeafCoverage: {'class_key', 'horizon_days'},
            DetachExpiredHistoryLeaf: {
                'leaf',
                'quarantine_horizon',
                'statement_timeout_ms',
            },
            FinalizeInterruptedLeafDetach: {
                'leaf',
                'statement_timeout_ms',
            },
            DropDetachedHistoryLeaf: {'leaf'},
            CollectPartitionHealth: {'class_key', 'application_managed'},
        }
        for variant in get_args(PartitionMaintenanceCommand.__value__):
            actual = {field.name for field in dataclasses.fields(variant)}
            assert actual == permitted[variant], (
                f'{variant.__name__} carries unaccounted fields: '
                f'{actual ^ permitted[variant]}'
            )


class TestDerivedNames:
    def test_daily_leaf_name_embeds_the_day(self) -> None:
        name = daily_leaf_name('horsies_task_history_finite_30d', LOWER)
        assert name == 'horsies_task_history_finite_30d_2026_08_06'

    def test_daily_leaf_name_rejects_overlong_result(self) -> None:
        with pytest.raises(ValueError, match='safe identifier'):
            daily_leaf_name('p' * 60, LOWER)

    def test_id_index_name_derives_from_leaf(self) -> None:
        assert (
            leaf_id_index_name('horsies_task_history_finite_30d_2026_08_06')
            == 'horsies_task_history_finite_30d_2026_08_06_task_idx'
        )


class TestFinalizeAppliesItsTimeout:
    """Carrying the field is not the same as using it.

    Adding `statement_timeout_ms` to the command would look like a fix
    while the executor ignored it, so this reads the executor's own
    source for the two statements that make the timeout real: reading
    the prior value, and setting the new one. A FINALIZE that waits
    unbounded blocks every future detach on its parent, so the failure
    this guards is the expensive one in the family.
    """

    def test_finalize_reads_and_sets_the_statement_timeout(self) -> None:
        import inspect as _inspect

        from horsies.core.history.partitions.manager import (
            finalize_interrupted_detach,
        )

        source = _inspect.getsource(finalize_interrupted_detach)
        assert 'command.statement_timeout_ms' in source, (
            'the finalize executor never reads the timeout it is given'
        )
        assert "SHOW statement_timeout" in source, (
            'the prior timeout is not captured, so it cannot be restored'
        )
        assert source.count("set_config('statement_timeout'") == 2, (
            'the timeout must be both applied and restored'
        )
