"""Unit tests for the runner of the public API gate (tests/public_api/runner.py).

The run of both tools against the installed build is the gate test itself
(tests/public_api/test_public_api.py); these tests cover the failure exits.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.public_api.runner import (
    Err,
    Ok,
    OutputUnreadable,
    PyrightMissing,
    RunFailure,
    ToolFailed,
    describe_failure,
    read_json,
    run_tool,
    take_current_api,
)

pytestmark = pytest.mark.unit


def test_a_tool_that_exits_with_an_error(tmp_path: Path) -> None:
    tool = tmp_path / 'tool.py'
    tool.write_text('import sys\nprint("refused", file=sys.stderr)\nraise SystemExit(2)\n', encoding='utf-8')
    assert run_tool(tool, [], tmp_path) == Err(ToolFailed('tool.py', 'exit code 2: refused\n'))


def test_a_tool_that_succeeds(tmp_path: Path) -> None:
    tool = tmp_path / 'tool.py'
    tool.write_text('', encoding='utf-8')
    assert run_tool(tool, [], tmp_path) == Ok(None)


def test_an_output_that_is_not_json(tmp_path: Path) -> None:
    path = tmp_path / 'out.json'
    path.write_text('{', encoding='utf-8')
    match read_json(path):
        case Err(OutputUnreadable(failed_path, cause)):
            assert failed_path == path
            assert cause.startswith('JSONDecodeError')
        case other:
            raise AssertionError(other)


def test_no_pyright_on_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('PATH', str(tmp_path))
    assert take_current_api(tmp_path) == Err(PyrightMissing())


@pytest.mark.parametrize(
    ('failure', 'message'),
    [
        (PyrightMissing(), 'no pyright command on PATH; run inside the dev environment (uv run --no-sync)'),
        (ToolFailed('snapshot.py', 'cause'), 'snapshot.py failed: cause'),
        (OutputUnreadable(Path('o'), 'cause'), 'the tool output o cannot be read: cause'),
    ],
)
def test_each_failure_names_its_subject(failure: RunFailure, message: str) -> None:
    assert describe_failure(failure) == message
