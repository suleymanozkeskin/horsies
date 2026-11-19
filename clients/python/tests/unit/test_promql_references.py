"""The PromQL reference scanner (``tests/promql_references.py``)."""

from __future__ import annotations

import pytest

from tests.promql_references import (
    MatchOperator,
    Matcher,
    References,
    Selector,
    references,
)


def _series(expression: str) -> list[str]:
    return [selector.series for selector in references(expression, 'e').selectors]


def _failure(expression: str) -> str:
    with pytest.raises(pytest.fail.Exception) as failed:
        references(expression, 'expr')
    return str(failed.value)


def test_a_selector_gives_its_series_and_each_matcher() -> None:
    found = references('x_total{a="1", b!="2", c=~"3|4", d!~"5"}', 'e')

    assert found == References(
        selectors=(
            Selector(
                series='x_total',
                matchers=(
                    Matcher('a', MatchOperator.EQUAL, '1'),
                    Matcher('b', MatchOperator.NOT_EQUAL, '2'),
                    Matcher('c', MatchOperator.MATCHES, '3|4'),
                    Matcher('d', MatchOperator.NOT_MATCHES, '5'),
                ),
            ),
        ),
        grouping_labels=frozenset(),
    )


def test_a_matcher_value_keeps_its_commas_and_escaped_quotes() -> None:
    (selector,) = references(r'x{a="1,2", b="say \"hi\"",}', 'e').selectors

    assert [matcher.value for matcher in selector.matchers] == ['1,2', r'say \"hi\"']


def test_function_and_aggregation_names_are_not_series() -> None:
    assert _series('sum(rate(x_total[5m])) / count(max(y) by (a))') == ['x_total', 'y']


def test_aggregation_labels_are_grouping_labels() -> None:
    found = references('sum by (a, b) (x) + max without (c) (y) + sum(z) by (d)', 'e')

    assert found.grouping_labels == frozenset({'a', 'b', 'c', 'd'})


def test_vector_matching_labels_are_grouping_labels() -> None:
    found = references(
        'x * on (a) group_left (b) y / ignoring (c) group_right z and on() w', 'e'
    )

    assert found.grouping_labels == frozenset({'a', 'b', 'c'})
    assert _series('x * on (a) group_left y') == ['x', 'y']


def test_ranges_subqueries_numbers_and_offsets_are_skipped() -> None:
    expression = 'rate(x[$__rate_interval]) > 0.95 and max_over_time(y[1h:5m] offset 1h30m) < 1e3'

    assert _series(expression) == ['x', 'y']


def test_string_literals_are_skipped() -> None:
    assert _series('label_replace(x, "dst", "$1", \'src\', `(.*)`)') == ['x']


def test_grafana_variables_and_comments_are_skipped() -> None:
    assert _series('x > $threshold + ${limit} # y{a="b"}\n or z') == ['x', 'z']


def test_an_unclosed_brace_fails() -> None:
    assert "no '}' for this '{'" in _failure('x{a="1"')


def test_an_unclosed_bracket_fails() -> None:
    assert "no ']' for this '['" in _failure('rate(x[5m)')


def test_a_selector_without_a_series_name_fails() -> None:
    assert 'a selector without a metric name' in _failure('{__name__="x"}')


def test_a_matcher_that_is_not_label_op_value_fails() -> None:
    assert 'is not a matcher label op "value"' in _failure('x{a}')


def test_an_unclosed_string_fails() -> None:
    assert 'an unclosed string literal' in _failure('label_replace(x, "dst)')


def test_a_dollar_that_starts_no_variable_fails() -> None:
    assert 'a "$" that starts no Grafana variable' in _failure('x > $')


def test_a_grouping_list_with_a_text_that_is_not_a_label_fails() -> None:
    assert '\'"a"\' is not a label name' in _failure('sum by ("a") (x)')
