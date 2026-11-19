"""Unit tests for the comparison of the public API gate (tests/public_api/compare.py).

One test per difference variant and per malformed-document outcome.
"""

from __future__ import annotations

import pytest

from tests.public_api import compare
from tests.public_api.compare import (
    DiagnosticAdded,
    DiagnosticRemoved,
    Difference,
    DocumentMalformed,
    EntryAdded,
    EntryChanged,
    EntryRemoved,
    Err,
    JsonValue,
    MemberAdded,
    Ok,
    ScoreLowered,
    ScoreRaised,
    blocks,
    compare_snapshots,
    compare_views,
    entry_differences,
    render,
    value_changes,
)

pytestmark = pytest.mark.unit


def _snapshot(entries: dict[str, JsonValue], reachable: dict[str, JsonValue] | None = None) -> JsonValue:
    return {'format': 1, 'entries': entries, 'reachable': reachable or {}}


def _view(score: float, diagnostics: list[JsonValue]) -> JsonValue:
    return {'format': 1, 'completeness_score': score, 'diagnostics': diagnostics}


def _differences(baseline: JsonValue, current: JsonValue) -> list[Difference]:
    match compare_snapshots(baseline, current):
        case Ok(differences):
            return differences
        case Err(malformed):
            raise AssertionError(f'unexpected malformed document: {malformed}')


def _view_differences(baseline: JsonValue, current: JsonValue) -> list[Difference]:
    match compare_views(baseline, current):
        case Ok(differences):
            return differences
        case Err(malformed):
            raise AssertionError(f'unexpected malformed document: {malformed}')


def _function(
    kind: str = 'function',
    returns: str = 'None',
    defined_as: str = 'horsies.core.a.f',
) -> JsonValue:
    return {
        'kind': kind,
        'defined_as': defined_as,
        'signature': {'async': 'sync', 'parameters': [{'name': 'x', 'annotation': 'int'}], 'returns': returns},
    }


FUNCTION = _function()


class TestBlocks:
    @pytest.mark.parametrize(
        ('difference', 'blocking'),
        [
            (EntryRemoved('entries', 'horsies:A', 'gone'), True),
            (EntryChanged('entries', 'horsies:A', ('x',)), True),
            (ScoreLowered(0.9, 0.8), True),
            (DiagnosticAdded('d'), True),
            (EntryAdded('entries', 'horsies:A'), False),
            (MemberAdded('entries', 'horsies:A', 'm'), False),
            (ScoreRaised(0.8, 0.9), False),
            (DiagnosticRemoved('d'), False),
        ],
    )
    def test_the_rule_of_each_variant(self, difference: Difference, blocking: bool) -> None:
        assert blocks(difference) is blocking


class TestValueChanges:
    def test_equal_values_have_no_change(self) -> None:
        assert value_changes(FUNCTION, FUNCTION, 'k') == []

    def test_a_changed_leaf_names_its_path(self) -> None:
        assert value_changes(FUNCTION, _function(returns='int'), 'k') == ['k.signature.returns: "None" -> "int"']

    def test_defined_as_is_not_compared(self) -> None:
        assert value_changes(FUNCTION, _function(defined_as='horsies._catalog.a.f'), 'k') == []

    def test_a_removed_and_an_added_key(self) -> None:
        assert value_changes({'a': 1}, {'b': 1}, 'k') == ['k.a: removed', 'k.b: added 1']

    def test_a_list_of_another_length_differs_as_a_whole(self) -> None:
        assert value_changes([1], [1, 2], 'k') == ['k: [1] -> [1, 2]']

    def test_a_list_of_the_same_length_differs_per_item(self) -> None:
        assert value_changes([1, 2], [1, 3], 'k') == ['k[1]: 2 -> 3']

    def test_equal_numbers_of_another_type_differ(self) -> None:
        assert value_changes(1, True, 'k') == ['k: 1 -> true']

    def test_a_nesting_past_the_bound_is_a_change(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(compare, 'MAX_COMPARE_DEPTH', 1)
        assert value_changes({'a': {'b': {'c': 1}}}, {'a': {'b': {'c': 1}}}, 'k') == [
            'k.a.b: nests deeper than 1 levels'
        ]


class TestEntryDifferences:
    @staticmethod
    def _class(members: dict[str, JsonValue]) -> JsonValue:
        return {'kind': 'class', 'mro': ['object'], 'members': members}

    def test_a_new_member_is_member_added(self) -> None:
        differences = entry_differences('entries', 'horsies:A', self._class({}), self._class({'m': FUNCTION}))
        assert differences == [MemberAdded('entries', 'horsies:A', 'm')]

    def test_a_removed_member_is_a_change(self) -> None:
        differences = entry_differences('entries', 'horsies:A', self._class({'m': FUNCTION}), self._class({}))
        assert differences == [EntryChanged('entries', 'horsies:A', ('horsies:A.members.m: removed',))]

    def test_a_changed_member_names_its_path(self) -> None:
        differences = entry_differences(
            'entries', 'horsies:A', self._class({'m': FUNCTION}), self._class({'m': _function(kind='method')})
        )
        assert differences == [
            EntryChanged('entries', 'horsies:A', ('horsies:A.members.m.kind: "function" -> "method"',))
        ]

    def test_a_change_outside_the_members(self) -> None:
        old = self._class({})
        new: JsonValue = {'kind': 'class', 'mro': ['Base', 'object'], 'members': {}}
        differences = entry_differences('entries', 'horsies:A', old, new)
        assert differences == [
            EntryChanged('entries', 'horsies:A', ('horsies:A.mro: ["object"] -> ["Base", "object"]',))
        ]


class TestCompareSnapshots:
    def test_a_new_entry_is_entry_added(self) -> None:
        assert _differences(_snapshot({}), _snapshot({'horsies:f': FUNCTION})) == [
            EntryAdded('entries', 'horsies:f')
        ]

    def test_a_missing_entry_is_entry_removed(self) -> None:
        assert _differences(_snapshot({}, {'horsies.x.C': FUNCTION}), _snapshot({})) == [
            EntryRemoved('reachable', 'horsies.x.C', 'not in the snapshot')
        ]

    def test_an_absent_entry_is_entry_removed_with_its_cause(self) -> None:
        absent: JsonValue = {'kind': 'absent', 'cause': 'module horsies has no name f'}
        assert _differences(_snapshot({'horsies:f': FUNCTION}), _snapshot({'horsies:f': absent})) == [
            EntryRemoved('entries', 'horsies:f', 'module horsies has no name f')
        ]

    def test_an_entry_absent_in_the_baseline_is_entry_added(self) -> None:
        absent: JsonValue = {'kind': 'absent', 'cause': 'module horsies has no name f'}
        assert _differences(_snapshot({'horsies:f': absent}), _snapshot({'horsies:f': FUNCTION})) == [
            EntryAdded('entries', 'horsies:f')
        ]

    def test_an_entry_absent_in_both_has_no_difference(self) -> None:
        absent: JsonValue = {'kind': 'absent', 'cause': 'module horsies.web does not import'}
        assert _differences(_snapshot({'horsies.web': absent}), _snapshot({'horsies.web': absent})) == []

    def test_a_changed_entry_is_entry_changed(self) -> None:
        changed = _function(kind='value')
        assert _differences(_snapshot({'horsies:f': FUNCTION}), _snapshot({'horsies:f': changed})) == [
            EntryChanged('entries', 'horsies:f', ('horsies:f.kind: "function" -> "value"',))
        ]

    def test_a_document_without_the_format_is_malformed(self) -> None:
        assert compare_snapshots({'entries': {}}, _snapshot({})) == Err(
            DocumentMalformed('baseline snapshot', 'no "format": 1')
        )

    def test_a_document_without_a_section_is_malformed(self) -> None:
        assert compare_snapshots(_snapshot({}), {'format': 1, 'entries': {}}) == Err(
            DocumentMalformed('current snapshot', 'no "reachable" object')
        )


class TestCompareViews:
    def test_equal_views_have_no_difference(self) -> None:
        assert _view_differences(_view(0.9, ['d']), _view(0.9, ['d'])) == []

    def test_a_lower_score(self) -> None:
        assert _view_differences(_view(0.9, []), _view(0.8, [])) == [ScoreLowered(0.9, 0.8)]

    def test_a_higher_score(self) -> None:
        assert _view_differences(_view(0.8, []), _view(0.9, [])) == [ScoreRaised(0.8, 0.9)]

    def test_a_new_and_a_gone_diagnostic(self) -> None:
        assert _view_differences(_view(0.9, ['old']), _view(0.9, ['new'])) == [
            DiagnosticAdded('new'),
            DiagnosticRemoved('old'),
        ]

    def test_a_view_without_a_score_is_malformed(self) -> None:
        assert compare_views({'format': 1, 'diagnostics': []}, _view(0.9, [])) == Err(
            DocumentMalformed(
                'baseline pyright view', 'no "format": 1, "completeness_score" and "diagnostics"'
            )
        )

    def test_a_diagnostic_that_is_not_text_is_malformed(self) -> None:
        assert compare_views(_view(0.9, []), _view(0.9, [1])) == Err(
            DocumentMalformed('current pyright view', 'diagnostic 1 is not a string')
        )


def test_render_lists_the_blocking_differences_first() -> None:
    report = render([EntryAdded('entries', 'horsies:B'), EntryRemoved('entries', 'horsies:A', 'gone')])
    assert report.index('entries horsies:A: removed (gone)') < report.index('entries horsies:B: added')
    assert report.startswith('1 blocking differences:')
