"""Unit tests for the pyright view tool (tests/public_api/pyright_view.py).

pyright itself is replaced by a shell script that prints a fixed report, so
the tests run no type checker.
"""

from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from tests.public_api.pyright_view import (
    Diagnostic,
    Err,
    JsonValue,
    Ok,
    OutputUnwritable,
    PyrightFailed,
    PyrightNotRun,
    PyrightReport,
    PyrightSymbol,
    ReportMalformed,
    SnapshotUnreadable,
    ViewFailure,
    build_view,
    describe_failure,
    in_scope,
    parse_report,
    run_pyright,
    scope_prefixes,
    take_view,
)

pytestmark = pytest.mark.unit

SNAPSHOT: JsonValue = {
    'entries': {
        'horsies:Horsies': {'kind': 'class', 'defined_as': 'horsies.core.app.Horsies'},
        'horsies.web:gone': {'kind': 'absent', 'cause': 'x'},
    },
    'reachable': {'horsies.core.brokers.postgres.PostgresBroker': {'kind': 'class'}},
}

REPORT: JsonValue = {
    'version': '1.1.411',
    'typeCompleteness': {
        'completenessScore': 0.5,
        'symbols': [
            {
                'name': 'horsies.core.app.Horsies.__init__',
                'diagnostics': [{'severity': 'error', 'message': 'partially unknown'}],
            },
            {
                'name': 'horsies.core.models.app.AppConfig',
                'alternateNames': ['horsies.Horsies'],
                'diagnostics': [{'severity': 'warning', 'message': 'no docstring'}],
            },
            {'name': 'horsies.core.other.Thing', 'diagnostics': [{'severity': 'error', 'message': 'unknown'}]},
        ],
    },
}


def _fake_pyright(folder: Path, body: str) -> Path:
    script = folder / 'pyright'
    script.write_text(f'#!/bin/sh\n{body}\n', encoding='utf-8')
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


class TestScopePrefixes:
    def test_scope_paths_definitions_and_reachable_entries(self) -> None:
        assert scope_prefixes(Path('s.json'), SNAPSHOT) == Ok(
            frozenset(
                {
                    'horsies.Horsies',
                    'horsies.core.app.Horsies',
                    'horsies.core.brokers.postgres.PostgresBroker',
                }
            )
        )

    def test_a_snapshot_without_entries(self) -> None:
        assert scope_prefixes(Path('s.json'), {}) == Err(
            SnapshotUnreadable(Path('s.json'), 'no "entries" and "reachable" objects')
        )


class TestInScope:
    @pytest.mark.parametrize(
        ('name', 'expected'),
        [
            ('horsies.Horsies', True),
            ('horsies.Horsies.send', True),
            ('horsies.HorsiesConfig', False),
            ('horsies.core.other', False),
        ],
    )
    def test_a_scope_path_and_its_members(self, name: str, expected: bool) -> None:
        assert in_scope(name, frozenset({'horsies.Horsies'})) is expected


class TestParseReport:
    def test_a_report(self) -> None:
        match parse_report(json.dumps(REPORT)):
            case Ok(report):
                assert report.version == '1.1.411'
                assert report.completeness_score == 0.5
                assert report.symbols[1] == PyrightSymbol(
                    'horsies.core.models.app.AppConfig',
                    ('horsies.Horsies',),
                    (Diagnostic('horsies.core.models.app.AppConfig', 'warning', 'no docstring'),),
                )
            case Err(failure):
                raise AssertionError(failure)

    def test_text_that_is_not_json(self) -> None:
        match parse_report('{'):
            case Err(ReportMalformed(cause)):
                assert cause.startswith('not JSON')
            case other:
                raise AssertionError(other)

    def test_a_report_without_type_completeness(self) -> None:
        assert parse_report('{"version": "1"}') == Err(
            ReportMalformed('no version, typeCompleteness.completenessScore and typeCompleteness.symbols')
        )

    def test_a_symbol_without_diagnostics(self) -> None:
        text = json.dumps({'version': '1', 'typeCompleteness': {'completenessScore': 1, 'symbols': [{'name': 'a'}]}})
        assert parse_report(text) == Err(ReportMalformed('a symbol has no name and diagnostics list'))

    def test_alternate_names_that_are_not_text(self) -> None:
        symbol = {'name': 'a', 'alternateNames': [1], 'diagnostics': []}
        text = json.dumps({'version': '1', 'typeCompleteness': {'completenessScore': 1, 'symbols': [symbol]}})
        assert parse_report(text) == Err(ReportMalformed('the alternateNames of a are not a list of strings'))

    def test_a_diagnostic_without_a_message(self) -> None:
        symbol = {'name': 'a', 'diagnostics': [{'severity': 'error'}]}
        text = json.dumps({'version': '1', 'typeCompleteness': {'completenessScore': 1, 'symbols': [symbol]}})
        assert parse_report(text) == Err(ReportMalformed('a diagnostic of a has no severity and message'))


def test_the_view_keeps_the_diagnostics_in_scope() -> None:
    report = PyrightReport(
        '1.1.411',
        0.5,
        (
            PyrightSymbol('horsies.core.app.Horsies.x', (), (Diagnostic('horsies.core.app.Horsies.x', 'error', 'b'),)),
            PyrightSymbol('horsies.other', ('horsies.Horsies',), (Diagnostic('horsies.other', 'error', 'a'),)),
            PyrightSymbol('horsies.core.z', (), (Diagnostic('horsies.core.z', 'error', 'c'),)),
        ),
    )
    assert build_view(report, frozenset({'horsies.Horsies', 'horsies.core.app.Horsies'})) == {
        'format': 1,
        'pyright_version': '1.1.411',
        'completeness_score': 0.5,
        'diagnostics': ['horsies.core.app.Horsies.x | error | b', 'horsies.other | error | a'],
    }


class TestRunPyright:
    def test_the_report_of_a_run_that_found_errors(self, tmp_path: Path) -> None:
        pyright = _fake_pyright(tmp_path, 'echo "$VIRTUAL_ENV $1 $2"\nexit 1')
        python = tmp_path / 'venv' / 'bin' / 'python'
        assert run_pyright(pyright, python) == Ok(f'{tmp_path / "venv"} --verifytypes horsies\n')

    def test_a_run_that_ended_with_another_code(self, tmp_path: Path) -> None:
        pyright = _fake_pyright(tmp_path, 'echo broken >&2\nexit 3')
        assert run_pyright(pyright, tmp_path / 'python') == Err(PyrightFailed(3, 'broken\n'))

    def test_a_command_that_does_not_exist(self, tmp_path: Path) -> None:
        match run_pyright(tmp_path / 'no-pyright', tmp_path / 'python'):
            case Err(PyrightNotRun(cause)):
                assert cause.startswith('FileNotFoundError')
            case other:
                raise AssertionError(other)


class TestTakeView:
    def test_the_view_is_written(self, tmp_path: Path) -> None:
        report = tmp_path / 'report.json'
        report.write_text(json.dumps(REPORT), encoding='utf-8')
        pyright = _fake_pyright(tmp_path, f'cat "{report}"')
        snapshot = tmp_path / 'snapshot.json'
        snapshot.write_text(json.dumps(SNAPSHOT), encoding='utf-8')
        output = tmp_path / 'view.json'
        assert take_view(pyright, tmp_path / 'python', snapshot, output) == Ok(None)
        assert json.loads(output.read_text(encoding='utf-8'))['diagnostics'] == [
            'horsies.core.app.Horsies.__init__ | error | partially unknown',
            'horsies.core.models.app.AppConfig | warning | no docstring',
        ]

    def test_a_snapshot_that_cannot_be_read(self, tmp_path: Path) -> None:
        match take_view(tmp_path / 'pyright', tmp_path / 'python', tmp_path / 'none.json', tmp_path / 'v.json'):
            case Err(SnapshotUnreadable(path, _)):
                assert path == tmp_path / 'none.json'
            case other:
                raise AssertionError(other)


@pytest.mark.parametrize(
    ('failure', 'message'),
    [
        (SnapshotUnreadable(Path('s'), 'cause'), 'snapshot s cannot be read: cause'),
        (PyrightNotRun('cause'), 'pyright did not run: cause'),
        (PyrightFailed(3, 'tail'), 'pyright ended with exit code 3: tail'),
        (ReportMalformed('cause'), 'the pyright report is malformed: cause'),
        (OutputUnwritable(Path('o'), 'cause'), 'output o cannot be written: cause'),
    ],
)
def test_each_failure_names_its_subject(failure: ViewFailure, message: str) -> None:
    assert describe_failure(failure) == message
