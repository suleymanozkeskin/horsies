"""The pyright view of the public API of the installed ``horsies`` package.

``pyright --verifytypes horsies --outputjson --ignoreexternal`` reports the
type completeness score of the package and diagnostics per symbol (a type
that is not fully known, a missing docstring). This tool keeps the score and
the diagnostics of the symbols in the scope of a runtime snapshot
(``snapshot.py``): each scope path, each definition that a scope path or a
reachable entry names, and their members.

``pyright --verifytypes`` resolves the package with the ``python`` that PATH
finds, so the tool runs pyright with the folder of ``--python`` first on PATH
and VIRTUAL_ENV set to its environment. Run it from a folder outside the
checkout, so no ``pyproject.toml`` or ``pyrightconfig.json`` applies::

    python -I tests/public_api/pyright_view.py --pyright PYRIGHT \\
        --python PYTHON --snapshot SNAPSHOT --output OUTPUT

Exit codes: 0 the view is written; 2 the snapshot cannot be read, pyright did
not run or ended with a code other than 0 or 1, its report is malformed, or
the output cannot be written (the message names which).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import typing
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, assert_never

type JsonValue = str | int | float | bool | None | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]

FORMAT_VERSION: Final = 1
PACKAGE: Final = 'horsies'
PYRIGHT_TIMEOUT_SECONDS: Final = 600
# pyright exits 0 with no error and 1 with errors; both write the report.
REPORT_EXIT_CODES: Final = frozenset({0, 1})
STDERR_TAIL_CHARS: Final = 2_000

EXIT_WRITTEN: Final = 0
EXIT_FAILED: Final = 2


@dataclass(frozen=True, slots=True)
class Ok[T]:
    value: T


@dataclass(frozen=True, slots=True)
class Err[E]:
    error: E


type Result[T, E] = Ok[T] | Err[E]


@dataclass(frozen=True, slots=True)
class SnapshotUnreadable:
    path: Path
    cause: str


@dataclass(frozen=True, slots=True)
class PyrightNotRun:
    cause: str


@dataclass(frozen=True, slots=True)
class PyrightFailed:
    exit_code: int
    stderr_tail: str


@dataclass(frozen=True, slots=True)
class ReportMalformed:
    cause: str


@dataclass(frozen=True, slots=True)
class OutputUnwritable:
    path: Path
    cause: str


type ViewFailure = SnapshotUnreadable | PyrightNotRun | PyrightFailed | ReportMalformed | OutputUnwritable


@dataclass(frozen=True, slots=True)
class Diagnostic:
    symbol: str
    severity: str
    message: str

    def text(self) -> str:
        return f'{self.symbol} | {self.severity} | {self.message}'


@dataclass(frozen=True, slots=True)
class PyrightSymbol:
    name: str
    alternate_names: tuple[str, ...]
    diagnostics: tuple[Diagnostic, ...]


@dataclass(frozen=True, slots=True)
class PyrightReport:
    version: str
    completeness_score: float
    symbols: tuple[PyrightSymbol, ...]


def scope_prefixes(path: Path, snapshot: JsonValue) -> Result[frozenset[str], SnapshotUnreadable]:
    """The dotted paths of a runtime snapshot: each scope path that is not
    absent, and the ``defined_as`` of each entry and reachable entry."""
    prefixes: set[str] = set()
    match snapshot:
        case {'entries': dict(entries), 'reachable': dict(reachable)}:
            pass
        case _:
            return Err(SnapshotUnreadable(path, 'no "entries" and "reachable" objects'))
    for key, entry in entries.items():
        match entry:
            case {'kind': 'absent'}:
                continue
            case {'defined_as': str(definition)}:
                prefixes.add(definition)
            case _:
                pass
        prefixes.add(key.replace(':', '.'))
    for key, entry in reachable.items():
        prefixes.add(key)
        match entry:
            case {'defined_as': str(definition)}:
                prefixes.add(definition)
            case _:
                pass
    return Ok(frozenset(prefixes))


def in_scope(name: str, prefixes: frozenset[str]) -> bool:
    """Whether ``name`` is a scope path or a member path under one."""
    parts = name.split('.')
    return any('.'.join(parts[:end]) in prefixes for end in range(1, len(parts) + 1))


def parse_diagnostic(symbol: str, value: JsonValue) -> Result[Diagnostic, ReportMalformed]:
    match value:
        case {'severity': str(severity), 'message': str(message)}:
            return Ok(Diagnostic(symbol, severity, message))
        case _:
            return Err(ReportMalformed(f'a diagnostic of {symbol} has no severity and message'))


def parse_symbol(value: JsonValue) -> Result[PyrightSymbol, ReportMalformed]:
    match value:
        case {'name': str(name), 'diagnostics': list(diagnostics)}:
            pass
        case _:
            return Err(ReportMalformed('a symbol has no name and diagnostics list'))
    alternates: list[str] = []
    match value.get('alternateNames'):
        case None:
            pass
        case list(names) if all(isinstance(alternate, str) for alternate in names):
            alternates.extend(str(alternate) for alternate in names)
        case _:
            return Err(ReportMalformed(f'the alternateNames of {name} are not a list of strings'))
    parsed: list[Diagnostic] = []
    for diagnostic in diagnostics:
        match parse_diagnostic(name, diagnostic):
            case Err() as failed:
                return failed
            case Ok(item):
                parsed.append(item)
    return Ok(PyrightSymbol(name, tuple(alternates), tuple(parsed)))


def parse_report(text: str) -> Result[PyrightReport, ReportMalformed]:
    """The version, score and symbols of a ``--verifytypes --outputjson`` report."""
    try:
        document: JsonValue = json.loads(text)
    except json.JSONDecodeError as exc:
        return Err(ReportMalformed(f'not JSON: {exc}'))
    match document:
        case {
            'version': str(version),
            'typeCompleteness': {'completenessScore': float() | int() as score, 'symbols': list(symbols)},
        }:
            pass
        case _:
            return Err(ReportMalformed('no version, typeCompleteness.completenessScore and typeCompleteness.symbols'))
    parsed: list[PyrightSymbol] = []
    for symbol in symbols:
        match parse_symbol(symbol):
            case Err() as failed:
                return failed
            case Ok(item):
                parsed.append(item)
    return Ok(PyrightReport(version, float(score), tuple(parsed)))


def build_view(report: PyrightReport, prefixes: frozenset[str]) -> JsonObject:
    """The score and the sorted diagnostics of the symbols in scope."""
    diagnostics = sorted(
        {
            diagnostic.text()
            for symbol in report.symbols
            if any(in_scope(name, prefixes) for name in (symbol.name, *symbol.alternate_names))
            for diagnostic in symbol.diagnostics
        }
    )
    return {
        'format': FORMAT_VERSION,
        'pyright_version': report.version,
        'completeness_score': report.completeness_score,
        'diagnostics': list(diagnostics),
    }


def run_pyright(pyright: Path, python: Path) -> Result[str, PyrightNotRun | PyrightFailed]:
    """The stdout of ``pyright --verifytypes`` for the environment of ``python``.

    It runs in the current folder, waits at most ``PYRIGHT_TIMEOUT_SECONDS``,
    and changes no file.
    """
    environment = dict(os.environ)
    environment['PATH'] = f'{python.parent}{os.pathsep}{environment.get("PATH", "")}'
    environment['VIRTUAL_ENV'] = str(python.parent.parent)
    command = [str(pyright), '--verifytypes', PACKAGE, '--outputjson', '--ignoreexternal']
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=environment,
            timeout=PYRIGHT_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Err(PyrightNotRun(f'{type(exc).__name__}: {exc}'))
    if completed.returncode not in REPORT_EXIT_CODES:
        return Err(PyrightFailed(completed.returncode, completed.stderr[-STDERR_TAIL_CHARS:]))
    return Ok(completed.stdout)


def read_snapshot(path: Path) -> Result[JsonValue, SnapshotUnreadable]:
    try:
        document: JsonValue = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return Err(SnapshotUnreadable(path, f'{type(exc).__name__}: {exc}'))
    return Ok(document)


def write_view(path: Path, view: JsonObject) -> Result[None, OutputUnwritable]:
    try:
        path.write_text(json.dumps(view, indent=1, sort_keys=True, ensure_ascii=False) + '\n', encoding='utf-8')
    except OSError as exc:
        return Err(OutputUnwritable(path, f'{type(exc).__name__}: {exc}'))
    return Ok(None)


def describe_failure(failure: ViewFailure) -> str:
    match failure:
        case SnapshotUnreadable(path, cause):
            return f'snapshot {path} cannot be read: {cause}'
        case PyrightNotRun(cause):
            return f'pyright did not run: {cause}'
        case PyrightFailed(exit_code, stderr_tail):
            return f'pyright ended with exit code {exit_code}: {stderr_tail}'
        case ReportMalformed(cause):
            return f'the pyright report is malformed: {cause}'
        case OutputUnwritable(path, cause):
            return f'output {path} cannot be written: {cause}'
        case _:
            assert_never(failure)


def take_view(pyright: Path, python: Path, snapshot: Path, output: Path) -> Result[None, ViewFailure]:
    """Read the snapshot, run pyright, and write the view of the snapshot's scope."""
    match read_snapshot(snapshot):
        case Err(unreadable):
            return Err(unreadable)
        case Ok(document):
            pass
    match scope_prefixes(snapshot, document):
        case Err(malformed_snapshot):
            return Err(malformed_snapshot)
        case Ok(prefixes):
            pass
    match run_pyright(pyright, python):
        case Err(not_run):
            return Err(not_run)
        case Ok(stdout):
            pass
    match parse_report(stdout):
        case Err(malformed_report):
            return Err(malformed_report)
        case Ok(report):
            pass
    match write_view(output, build_view(report, prefixes)):
        case Err(unwritable):
            return Err(unwritable)
        case Ok():
            return Ok(None)


def main(arguments: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description='Write the pyright view of the public API of the installed horsies.')
    parser.add_argument('--pyright', type=Path, required=True, help='the pyright command')
    parser.add_argument('--python', type=Path, required=True, help='the interpreter of the environment')
    parser.add_argument('--snapshot', type=Path, required=True, help='the runtime snapshot that gives the scope')
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args(arguments)
    match take_view(
        typing.cast(Path, options.pyright),
        typing.cast(Path, options.python),
        typing.cast(Path, options.snapshot),
        typing.cast(Path, options.output),
    ):
        case Err(failure):
            print(describe_failure(failure), file=sys.stderr)
            return EXIT_FAILED
        case Ok():
            return EXIT_WRITTEN


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
