"""The exit codes of the worker and its children, against the pages that list them."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from horsies.core.worker.child_main import (
    BOOT_FAILED_EXIT_CODE,
    STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
)
from horsies.core.worker.preload_main import REPARENT_FAILED_EXIT_CODE
from horsies.core.worker.runner_heartbeat import RUNNER_HEARTBEAT_LOST_EXIT_CODE

pytestmark = [pytest.mark.unit]

REPO = Path(__file__).resolve().parents[2]
MIGRATION_PAGE = REPO / 'website/src/content/docs/migrations/migration-to-0-7-0.md'
CLI_PAGES = (
    REPO / 'website/src/content/docs/cli.md',
    REPO / 'horsies/docs/cli.md',
)
# Codes that are new or changed in 0.7.0, with the process that exits.
CHANGED_IN_0_7_0 = {
    STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE: 'worker child',
    BOOT_FAILED_EXIT_CODE: 'worker child',
    REPARENT_FAILED_EXIT_CODE: 'worker child',
    RUNNER_HEARTBEAT_LOST_EXIT_CODE: '`horsies worker`',
}


def _section(text: str, heading: str) -> str:
    """The text under ``heading``, up to the next heading of the same level."""
    start = text.index(f'\n{heading}\n')
    end = text.find('\n## ', start + 1)
    return text[start : end if end != -1 else len(text)]


def _table_rows(section: str) -> dict[int, list[str]]:
    """The cells of each table row whose first cell is a number, by that number."""
    rows: dict[int, list[str]] = {}
    for line in section.splitlines():
        cells = [cell.strip() for cell in line.strip().strip('|').split('|')]
        if re.fullmatch(r'\d+', cells[0]):
            rows[int(cells[0])] = cells
    return rows


def test_the_new_codes_are_distinct() -> None:
    assert len(set(CHANGED_IN_0_7_0)) == len(CHANGED_IN_0_7_0)


@pytest.mark.parametrize(('code', 'process'), sorted(CHANGED_IN_0_7_0.items()))
def test_the_migration_table_lists_each_code_new_in_0_7_0(
    code: int,
    process: str,
) -> None:
    rows = _table_rows(_section(MIGRATION_PAGE.read_text(), '## Exit codes'))

    assert code in rows
    assert rows[code][1] == process


def test_the_migration_table_states_the_0_6_meaning_of_74() -> None:
    """In 0.6.x, 74 meant only a forked child that does not reparent."""
    rows = _table_rows(_section(MIGRATION_PAGE.read_text(), '## Exit codes'))

    assert 'reparent' in rows[STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE][3]
    assert rows[REPARENT_FAILED_EXIT_CODE][3] == 'Code 74'


@pytest.mark.parametrize(
    'page',
    CLI_PAGES,
    ids=lambda page: str(page.relative_to(REPO)),
)
def test_each_cli_page_lists_the_runner_heartbeat_exit(page: Path) -> None:
    rows = _table_rows(_section(page.read_text(), '## Exit Codes'))

    assert RUNNER_HEARTBEAT_LOST_EXIT_CODE in rows
    assert 'runner heartbeats' in rows[RUNNER_HEARTBEAT_LOST_EXIT_CODE][1]
