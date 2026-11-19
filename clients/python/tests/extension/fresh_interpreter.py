"""Run one case of an extension program in a fresh interpreter, and read
the values of its report.

The test process never imports ``horsies._core`` for a case: the core state
is per process, so each case gets a new Python process. The process runs
``python -m tests.extension.programs.<program> <case> <report path>`` from
the repository root and writes one JSON object to the report path
(``programs/report.py``). A case that does not end within
``PROGRAM_TIMEOUT_S``, exits with a nonzero code, or writes no report fails
the test with the program's stdout and stderr.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final, NoReturn, cast

import pytest

from tests.extension.programs.report import DATABASE_URL_VARIABLE, JsonValue, Report

REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[2]
"""The folder that holds the ``tests`` package."""

PROGRAM_TIMEOUT_S: Final = 120.0
"""The longest time one case may run. Each wait inside a case has a smaller
bound of its own; this bound ends a case that hangs past all of them."""


@dataclass(frozen=True)
class CaseRun:
    """The report of one case and the output of its process."""

    report: Report
    stdout: str
    stderr: str


@dataclass(frozen=True)
class NoDatabase:
    """The case connects to no database."""


@dataclass(frozen=True)
class Database:
    """The case uses the database at ``url``."""

    url: str


type CaseDatabase = NoDatabase | Database


def run_case(program: str, case: str, report_dir: Path, database: CaseDatabase) -> CaseRun:
    """Run ``case`` of ``tests.extension.programs.<program>`` in a new Python
    process and return its report.

    The process inherits this process's environment. ``PYTHONPATH`` starts
    with the repository root, so the ``tests`` package imports; the database
    URL goes in ``DATABASE_URL_VARIABLE`` and is removed for ``NoDatabase``.
    Fails the test when the process does not end within
    ``PROGRAM_TIMEOUT_S``, exits with a nonzero code, or writes no report.
    """
    report_path = report_dir / f'{program}.{case}.json'
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.pathsep.join(
        [str(REPOSITORY_ROOT), *_paths_of(environment.get('PYTHONPATH', ''))],
    )
    match database:
        case NoDatabase():
            _ = environment.pop(DATABASE_URL_VARIABLE, None)
        case Database(url=url):
            environment[DATABASE_URL_VARIABLE] = url
    command = [
        sys.executable,
        '-m',
        f'tests.extension.programs.{program}',
        case,
        str(report_path),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=PROGRAM_TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired as expired:
        _fail(
            f'{program} {case} did not end within {PROGRAM_TIMEOUT_S} s\n'
            f'stdout:\n{_output_text(expired.stdout)}\n'
            f'stderr:\n{_output_text(expired.stderr)}',
        )
    output = f'stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}'
    if completed.returncode != 0:
        _fail(f'{program} {case} exited with {completed.returncode}\n{output}')
    if not report_path.is_file():
        _fail(f'{program} {case} wrote no report to {report_path}\n{output}')
    return CaseRun(
        report=_parsed_report(report_path.read_text(encoding='utf-8'), f'{program} {case}'),
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _fail(message: str) -> NoReturn:
    """Fail the test with ``message``. A ``raise``, so that every path after a
    failure is known to end here."""
    raise pytest.fail.Exception(message)


def _paths_of(value: str) -> list[str]:
    return [path for path in value.split(os.pathsep) if path]


def _output_text(output: str | bytes | None) -> str:
    match output:
        case None:
            return ''
        case str():
            return output
        case bytes():
            return output.decode('utf-8', errors='replace')


def _parsed_report(text: str, subject: str) -> Report:
    parsed: object = json.loads(text)
    match parsed:
        case dict():
            entries = cast('dict[object, object]', parsed)
            return {str(key): _json_value(value) for key, value in entries.items()}
        case _:
            _fail(f'the report of {subject} is not a JSON object: {text!r}')


def _json_value(value: object) -> JsonValue:
    match value:
        case None | bool() | int() | float() | str():
            return value
        case list():
            return [_json_value(item) for item in cast('list[object]', value)]
        case dict():
            entries = cast('dict[object, object]', value)
            return {str(key): _json_value(item) for key, item in entries.items()}
        case _:
            raise AssertionError(f'json.loads gave a value of type {type(value).__name__}')


def text_of(report: Report, key: str) -> str:
    """The text at ``key``; fails the test when it is missing or not text."""
    match _value_of(report, key):
        case str() as text:
            return text
        case other:
            _fail(f'report field {key!r} is not text: {other!r}')


def integer_of(report: Report, key: str) -> int:
    """The integer at ``key``; fails the test when it is missing or not an integer."""
    match _value_of(report, key):
        case bool() as other:
            _fail(f'report field {key!r} is not an integer: {other!r}')
        case int() as number:
            return number
        case other:
            _fail(f'report field {key!r} is not an integer: {other!r}')


def seconds_of(report: Report, key: str) -> float:
    """The number at ``key`` (seconds); fails the test when it is missing or not a number."""
    match _value_of(report, key):
        case bool() as other:
            _fail(f'report field {key!r} is not a number: {other!r}')
        case int() | float() as number:
            return float(number)
        case other:
            _fail(f'report field {key!r} is not a number: {other!r}')


def flag_of(report: Report, key: str) -> bool:
    """The boolean at ``key``; fails the test when it is missing or not a boolean."""
    match _value_of(report, key):
        case bool() as flag:
            return flag
        case other:
            _fail(f'report field {key!r} is not a boolean: {other!r}')


def texts_of(report: Report, key: str) -> list[str]:
    """The list of texts at ``key``; fails the test when it is missing or
    holds another value."""
    match _value_of(report, key):
        case list() as items:
            texts: list[str] = []
            for item in items:
                match item:
                    case str():
                        texts.append(item)
                    case _:
                        _fail(f'report field {key!r} holds a value that is not text: {item!r}')
            return texts
        case other:
            _fail(f'report field {key!r} is not a list: {other!r}')


def numbers_of(report: Report, key: str) -> list[float]:
    """The list of numbers at ``key``; fails the test when it is missing or
    holds another value."""
    match _value_of(report, key):
        case list() as items:
            numbers: list[float] = []
            for item in items:
                match item:
                    case bool():
                        _fail(f'report field {key!r} holds a value that is not a number: {item!r}')
                    case int() | float():
                        numbers.append(float(item))
                    case _:
                        _fail(f'report field {key!r} holds a value that is not a number: {item!r}')
            return numbers
        case other:
            _fail(f'report field {key!r} is not a list: {other!r}')


def report_at(report: Report, key: str) -> Report:
    """The JSON object at ``key``; fails the test when it is missing or not an object."""
    match _value_of(report, key):
        case dict() as nested:
            return nested
        case other:
            _fail(f'report field {key!r} is not an object: {other!r}')


def reports_of(report: Report, key: str) -> list[Report]:
    """The list of JSON objects at ``key``; fails the test when it is missing
    or holds another value."""
    match _value_of(report, key):
        case list() as items:
            nested: list[Report] = []
            for item in items:
                match item:
                    case dict():
                        nested.append(item)
                    case _:
                        _fail(f'report field {key!r} holds a value that is not an object: {item!r}')
            return nested
        case other:
            _fail(f'report field {key!r} is not a list: {other!r}')


def _value_of(report: Report, key: str) -> JsonValue:
    if key not in report:
        _fail(f'the report has no field {key!r}; fields: {sorted(report)}')
    return report[key]
