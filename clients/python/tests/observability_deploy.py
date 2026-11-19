"""The Grafana dashboard and the Prometheus alert rules of ``deploy/``, read
as typed records.

- ``deploy/grafana/horsies-dashboard.json``: the queries of the panels and
  of the template variables.
- ``deploy/prometheus/horsies-alerts.yaml``: the alerting rules.
- ``deploy/prometheus/horsies-alerts.test.yaml``: the ``promtool test rules``
  cases of each alert.

A file that is larger than ``DEPLOY_FILE_MAX_BYTES`` or that does not have
the expected shape fails the test that reads it, with the place and the
cause.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

import pytest
import yaml

REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[3]
DASHBOARD: Final = REPOSITORY_ROOT / 'deploy' / 'grafana' / 'horsies-dashboard.json'
ALERT_RULES: Final = REPOSITORY_ROOT / 'deploy' / 'prometheus' / 'horsies-alerts.yaml'
ALERT_RULE_TESTS: Final = (
    REPOSITORY_ROOT / 'deploy' / 'prometheus' / 'horsies-alerts.test.yaml'
)
DEPLOY_FILE_MAX_BYTES: Final = 1_000_000
"""The largest deploy file that these readers read."""

_LABEL_VALUES_QUERY: Final = re.compile(
    r'^label_values\((?P<selector>.+),\s*(?P<label>[A-Za-z_][A-Za-z0-9_]*)\s*\)$'
)


@dataclass(frozen=True)
class DashboardQuery:
    """The PromQL query of one panel target."""

    where: str
    expression: str
    legend: str


@dataclass(frozen=True)
class VariableQuery:
    """A ``label_values(selector, label)`` query of a template variable."""

    where: str
    selector: str
    label: str


@dataclass(frozen=True)
class AlertRule:
    """One alerting rule."""

    name: str
    expression: str
    annotations: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class AlertCase:
    """One ``alert_rule_test`` entry of the promtool tests."""

    where: str
    alertname: str
    firing: int
    """The number of alerts that the entry expects to fire."""


def read_bounded(path: Path) -> str:
    """The text of ``path``. Fails the test when the file is larger than
    ``DEPLOY_FILE_MAX_BYTES``."""
    with path.open('rb') as source:
        data = source.read(DEPLOY_FILE_MAX_BYTES + 1)
    if len(data) > DEPLOY_FILE_MAX_BYTES:
        pytest.fail(f'{path}: larger than {DEPLOY_FILE_MAX_BYTES} bytes')
    return data.decode('utf-8')


def mapping(value: object, where: str) -> dict[str, object]:
    """``value`` as a mapping with text keys; fails the test otherwise."""
    if not isinstance(value, dict):
        pytest.fail(f'{where}: not a mapping: {value!r}')
    entries = cast(dict[object, object], value)
    return {str(key): entry for key, entry in entries.items()}


def sequence(value: object, where: str) -> list[object]:
    """``value`` as a list; fails the test otherwise."""
    if not isinstance(value, list):
        pytest.fail(f'{where}: not a list: {value!r}')
    return list(cast(list[object], value))


def text(value: object, where: str) -> str:
    """``value`` as a text; fails the test otherwise."""
    if not isinstance(value, str):
        pytest.fail(f'{where}: not a text: {value!r}')
    return value


def dashboard() -> dict[str, object]:
    """The dashboard JSON."""
    return mapping(json.loads(read_bounded(DASHBOARD)), str(DASHBOARD))


def panels(board: dict[str, object]) -> list[dict[str, object]]:
    """Every panel, the panels inside a collapsed row included."""
    found: list[dict[str, object]] = []
    for index, value in enumerate(sequence(board.get('panels', []), 'panels')):
        panel = mapping(value, f'panels[{index}]')
        found.append(panel)
        for inner, nested in enumerate(
            sequence(panel.get('panels', []), f'panels[{index}]')
        ):
            found.append(mapping(nested, f'panels[{index}].panels[{inner}]'))
    return found


def dashboard_queries() -> tuple[DashboardQuery, ...]:
    """The query of each target of each panel."""
    queries: list[DashboardQuery] = []
    for panel in panels(dashboard()):
        title = text(panel.get('title'), 'a panel title')
        for index, value in enumerate(sequence(panel.get('targets', []), title)):
            target = mapping(value, f'{title} targets[{index}]')
            where = f'panel {title!r} target {target.get("refId")}'
            queries.append(
                DashboardQuery(
                    where=where,
                    expression=text(target.get('expr'), f'{where} expr'),
                    legend=text(
                        target.get('legendFormat', ''), f'{where} legendFormat'
                    ),
                )
            )
    return tuple(queries)


def variable_queries() -> tuple[VariableQuery, ...]:
    """The ``label_values`` query of each query variable."""
    templating = mapping(dashboard().get('templating'), 'templating')
    queries: list[VariableQuery] = []
    for index, value in enumerate(sequence(templating.get('list'), 'templating.list')):
        variable = mapping(value, f'templating.list[{index}]')
        if variable.get('type') != 'query':
            continue
        where = f'variable {variable.get("name")!r}'
        definition = text(variable.get('query'), f'{where} query')
        if definition != variable.get('definition'):
            pytest.fail(
                f'{where}: definition {variable.get("definition")!r} is not {definition!r}'
            )
        found = _LABEL_VALUES_QUERY.match(definition)
        if found is None:
            pytest.fail(f'{where}: not label_values(selector, label): {definition!r}')
        queries.append(
            VariableQuery(
                where=where,
                selector=found.group('selector'),
                label=found.group('label'),
            )
        )
    return tuple(queries)


def alert_rules() -> tuple[AlertRule, ...]:
    """The alerting rules of every group, in file order."""
    document = mapping(yaml.safe_load(read_bounded(ALERT_RULES)), str(ALERT_RULES))
    rules: list[AlertRule] = []
    for group_index, group_value in enumerate(
        sequence(document.get('groups'), 'groups')
    ):
        group = mapping(group_value, f'groups[{group_index}]')
        for index, value in enumerate(
            sequence(group.get('rules'), f'groups[{group_index}].rules')
        ):
            where = f'groups[{group_index}].rules[{index}]'
            rule = mapping(value, where)
            annotations = mapping(rule.get('annotations', {}), f'{where}.annotations')
            rules.append(
                AlertRule(
                    name=text(rule.get('alert'), f'{where}.alert'),
                    expression=text(rule.get('expr'), f'{where}.expr'),
                    annotations=tuple(
                        (key, text(entry, f'{where}.annotations.{key}'))
                        for key, entry in annotations.items()
                    ),
                )
            )
    return tuple(rules)


def alert_cases() -> tuple[AlertCase, ...]:
    """Every ``alert_rule_test`` entry of the promtool tests."""
    document = mapping(
        yaml.safe_load(read_bounded(ALERT_RULE_TESTS)), str(ALERT_RULE_TESTS)
    )
    cases: list[AlertCase] = []
    for test_index, test_value in enumerate(sequence(document.get('tests'), 'tests')):
        test = mapping(test_value, f'tests[{test_index}]')
        name = text(test.get('name'), f'tests[{test_index}].name')
        for index, value in enumerate(sequence(test.get('alert_rule_test'), name)):
            where = f'{name}: alert_rule_test[{index}]'
            case = mapping(value, where)
            cases.append(
                AlertCase(
                    where=where,
                    alertname=text(case.get('alertname'), f'{where}.alertname'),
                    firing=len(sequence(case.get('exp_alerts'), f'{where}.exp_alerts')),
                )
            )
    return tuple(cases)
