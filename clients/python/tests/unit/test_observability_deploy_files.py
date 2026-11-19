"""The Grafana dashboard and the Prometheus alert rules of ``deploy/`` name
only series, labels and label values of the metric catalog
(``core/src/telemetry/catalog_snapshot.txt``).

A series is the Prometheus name of a counter or a gauge, or the ``_bucket``,
``_sum`` or ``_count`` series of a histogram (``le`` is a label of
``_bucket`` only). A label in a matcher is a label of the metric of its
selector. A label of ``by``, ``on`` and the other label lists, of a legend
(``{{label}}``) or of an annotation (``$labels.label``) is a label of a
metric of the expression. A matcher value of a closed label set is in the
set; a regex value is a ``|`` list of such values, or holds a Grafana
variable.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from tests.metric_catalog import (
    HISTOGRAM_BUCKET_LABEL,
    CatalogMetric,
    ClosedValues,
    OpenValues,
    read_catalog,
)
from tests.observability_deploy import (
    alert_cases,
    alert_rules,
    dashboard,
    dashboard_queries,
    panels,
    variable_queries,
)
from tests.promql_references import MatchOperator, Matcher, references

GRAFANA_SCHEMA_VERSION: Final = 42
"""``LATEST_VERSION`` of the dashboard schema in Grafana 12.2 to 13.2
(``apps/dashboard/pkg/migration/schemaversion/migrations.go``). Grafana
migrates an older version when it loads the dashboard, and cannot read a
newer one."""

DATASOURCE_VARIABLE: Final = '${datasource}'

ALERT_NAMES: Final = frozenset(
    {
        'HorsiesOldestPendingTaskTooOld',
        'HorsiesTaskFailureRatioHigh',
        'HorsiesNoLiveWorker',
        'HorsiesUnservedClass',
        'HorsiesEventsDropped',
        'HorsiesTelemetryDown',
        'HorsiesFleetQueryFailed',
    }
)

_LITERAL_ALTERNATION: Final = re.compile(r'[A-Za-z0-9_]*(\|[A-Za-z0-9_]*)*')
_LEGEND_LABEL: Final = re.compile(r'\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}')
_ANNOTATION_LABEL: Final = re.compile(r'\$labels\.([A-Za-z_][A-Za-z0-9_]*)')


@dataclass(frozen=True)
class CatalogSeries:
    """A series name of the catalog and the labels that it has."""

    metric: CatalogMetric
    labels: frozenset[str]


def _catalog_series() -> dict[str, CatalogSeries]:
    """Each series name of the catalog."""
    series: dict[str, CatalogSeries] = {}
    for metric in read_catalog():
        for name in metric.series_names():
            extra = (
                frozenset({HISTOGRAM_BUCKET_LABEL})
                if name.endswith('_bucket')
                else frozenset[str]()
            )
            series[name] = CatalogSeries(
                metric=metric, labels=metric.label_names() | extra
            )
    return series


CATALOG_SERIES: Final = _catalog_series()


def _value_problems(where: str, metric: CatalogMetric, matcher: Matcher) -> list[str]:
    """The problems of the value of one matcher of ``metric``. A regex that
    holds a Grafana variable names no value."""
    (label,) = [label for label in metric.labels if label.name == matcher.label]
    regex = (MatchOperator.MATCHES, MatchOperator.NOT_MATCHES)
    match (label.values, matcher.operator):
        case (OpenValues(), _):
            return []
        case (ClosedValues(), operator) if operator in regex and '$' in matcher.value:
            return []
        case (ClosedValues(), operator) if (
            operator in regex and _LITERAL_ALTERNATION.fullmatch(matcher.value) is None
        ):
            return [
                f'{where}: {matcher.label} regex {matcher.value!r} is not a | list of values'
            ]
        case (
            ClosedValues(values=values),
            MatchOperator.MATCHES | MatchOperator.NOT_MATCHES,
        ):
            candidates = matcher.value.split('|')
        case (
            ClosedValues(values=values),
            MatchOperator.EQUAL | MatchOperator.NOT_EQUAL,
        ):
            candidates = [matcher.value]
    return [
        f'{where}: {candidate!r} is not a value of {metric.prometheus_name} label {label.name}'
        for candidate in candidates
        if candidate not in values
    ]


def expression_labels(expression: str, where: str) -> frozenset[str]:
    """The labels of the catalog series that ``expression`` names."""
    found = references(expression, where)
    return frozenset[str]().union(
        *(
            CATALOG_SERIES[selector.series].labels
            for selector in found.selectors
            if selector.series in CATALOG_SERIES
        )
    )


def expression_problems(expression: str, where: str) -> list[str]:
    """Every series, label and value of ``expression`` that the catalog does
    not have."""
    found = references(expression, where)
    problems: list[str] = []
    if not found.selectors:
        problems.append(f'{where}: names no series')
    for selector in found.selectors:
        series = CATALOG_SERIES.get(selector.series)
        if series is None:
            problems.append(
                f'{where}: {selector.series} is not a series of the catalog'
            )
            continue
        for matcher in selector.matchers:
            if matcher.label not in series.labels:
                problems.append(
                    f'{where}: {selector.series} has no label {matcher.label}'
                )
                continue
            if matcher.label != HISTOGRAM_BUCKET_LABEL:
                problems.extend(_value_problems(where, series.metric, matcher))
    allowed = expression_labels(expression, where)
    problems.extend(
        f'{where}: grouping label {label} is not a label of its series'
        for label in sorted(found.grouping_labels - allowed)
    )
    return problems


def template_problems(
    where: str, expression: str, template: str, pattern: re.Pattern[str]
) -> list[str]:
    """Every label that ``template`` names and the series of ``expression``
    do not have."""
    allowed = expression_labels(expression, where)
    return [
        f'{where}: {template!r} names {label}, not a label of its series'
        for label in pattern.findall(template)
        if label not in allowed
    ]


def _all(problems: Iterable[list[str]]) -> list[str]:
    return [problem for group in problems for problem in group]


def test_every_dashboard_query_names_catalog_series_labels_and_values() -> None:
    queries = dashboard_queries()

    assert queries
    assert (
        _all(expression_problems(query.expression, query.where) for query in queries)
        == []
    )


def test_every_dashboard_legend_names_a_label_of_its_query() -> None:
    problems = _all(
        template_problems(query.where, query.expression, query.legend, _LEGEND_LABEL)
        for query in dashboard_queries()
    )

    assert problems == []


def test_every_dashboard_variable_names_a_catalog_series_and_its_label() -> None:
    variables = variable_queries()
    problems = _all(
        expression_problems(variable.selector, variable.where) for variable in variables
    )
    problems.extend(
        f'{variable.where}: {variable.label} is not a label of {variable.selector}'
        for variable in variables
        if variable.label not in expression_labels(variable.selector, variable.where)
    )

    assert [variable.label for variable in variables] == ['queue', 'task']
    assert problems == []


def test_the_dashboard_has_the_pinned_schema_version_unique_ids_and_the_datasource_variable() -> (
    None
):
    board = dashboard()
    found = panels(board)
    ids = [panel.get('id') for panel in found]
    queried = [panel for panel in found if panel.get('type') != 'row']

    assert board.get('schemaVersion') == GRAFANA_SCHEMA_VERSION
    assert board.get('uid') == 'horsies-overview'
    assert len(ids) == len(set(ids))
    assert all(
        panel.get('datasource') == {'type': 'prometheus', 'uid': DATASOURCE_VARIABLE}
        for panel in queried
    )


def test_every_alert_expression_names_catalog_series_labels_and_values() -> None:
    rules = alert_rules()

    assert {rule.name for rule in rules} == ALERT_NAMES
    assert len(rules) == len(ALERT_NAMES)
    assert _all(expression_problems(rule.expression, rule.name) for rule in rules) == []


def test_every_alert_annotation_names_a_label_of_its_expression() -> None:
    problems = _all(
        template_problems(
            f'{rule.name} {key}', rule.expression, template, _ANNOTATION_LABEL
        )
        for rule in alert_rules()
        for key, template in rule.annotations
    )

    assert problems == []


def test_every_alert_has_a_promtool_case_that_fires_and_one_that_stays_quiet() -> None:
    cases = alert_cases()
    firing = {case.alertname for case in cases if case.firing > 0}
    quiet = {case.alertname for case in cases if case.firing == 0}

    assert {case.alertname for case in cases} == ALERT_NAMES
    assert firing == ALERT_NAMES
    assert quiet == ALERT_NAMES


def test_the_check_refuses_names_labels_and_values_outside_the_catalog() -> None:
    expression = (
        'sum by (worker) (rate(horsies_task_attempt_total[5m]))'
        ' + sum by (le) (horsies_task_attempts_total{outcome="DONE", colour="red"})'
        ' + horsies_queue_tasks{status=~"PENDING|LATE"}'
        ' + horsies_queue_tasks{status=~"PEND.*"}'
        ' + horsies_fleet_workers{state!="serving"}'
    )

    assert expression_problems(expression, 'e') == [
        'e: horsies_task_attempt_total is not a series of the catalog',
        "e: 'DONE' is not a value of horsies_task_attempts_total label outcome",
        'e: horsies_task_attempts_total has no label colour',
        "e: 'LATE' is not a value of horsies_queue_tasks label status",
        "e: status regex 'PEND.*' is not a | list of values",
        'e: grouping label le is not a label of its series',
        'e: grouping label worker is not a label of its series',
    ]
    assert template_problems(
        'e', 'horsies_fleet_workers', '{{state}} {{queue}}', _LEGEND_LABEL
    ) == ["e: '{{state}} {{queue}}' names queue, not a label of its series"]
