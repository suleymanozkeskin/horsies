"""The instrument a row's limits were written for, checked before the row runs.

A budget limit is a number about a declared mode. §2.1's paired-micro mode
requires `fsync`, `synchronous_commit`, `full_page_writes` and `autovacuum` all
off, explicit `ANALYZE` owning plan statistics, and the surrounding units
quiesced. A row measured with those at their defaults is measured true and
measured elsewhere: both sides still see the same server, so the paired delta is
still a paired delta, but every commit pays a disk sync the limit never
budgeted for and an autovacuum nobody scheduled competes with the harness for
the table.

**The mode is read from the server, not asserted by the harness.** A comment
saying the window was configured correctly is worth nothing the first time
somebody runs a cell against an instance that was reset overnight. The settings
are queried at cell time, compared against the row's declared mode, and
recorded in that cell's conditions so a reader can check the claim.

**Structure is part of the mode too.** The same section fixes the observation
count and block size, and those are what the interleave's guarantees are
computed over: a run in blocks of a thousand has a tenth of the alternations a
run in blocks of a hundred does.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final


class ModeConformanceError(Exception):
    """The instrument is not the one this row's limits belong to."""


class BenchMode(StrEnum):
    """The performance modes the budgets define."""

    PAIRED_MICRO = 'paired-micro'


# §2.1. Every one of these is at its PostgreSQL default in the opposite state,
# so an unconfigured instance fails this check rather than passing it by
# accident.
REQUIRED_SETTINGS: Final[Mapping[BenchMode, Mapping[str, str]]] = {
    BenchMode.PAIRED_MICRO: {
        'fsync': 'off',
        'synchronous_commit': 'off',
        'full_page_writes': 'off',
        'autovacuum': 'off',
    },
}

# §2.1 again: warm single-row paths use at least 10,000 observations per side
# in blocks of 100.
MIN_OBSERVATIONS_PER_SIDE: Final[Mapping[BenchMode, int]] = {
    BenchMode.PAIRED_MICRO: 10_000,
}
REQUIRED_BLOCK_SIZE: Final[Mapping[BenchMode, int]] = {
    BenchMode.PAIRED_MICRO: 100,
}

SETTINGS_QUERY: Final = """
SELECT name, setting FROM pg_settings WHERE name = ANY(%(names)s) ORDER BY name
"""

# Backends that are not this harness. The mode requires the application,
# worker, scheduler, reaper, monitoring and web units quiesced; anything else
# holding a connection is competing for the instrument.
QUIESCE_QUERY: Final = """
SELECT count(*) FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND backend_type = 'client backend'
  AND state <> 'idle'
"""


@dataclass(frozen=True, slots=True)
class ServerMode:
    """What the server reported about itself, at the moment a cell ran."""

    mode: BenchMode
    settings: Mapping[str, str]
    active_client_backends: int

    def as_conditions(self) -> dict[str, Any]:
        return {
            'mode': self.mode.value,
            'server_settings': dict(self.settings),
            'active_client_backends': self.active_client_backends,
        }


def settings_from_rows(rows: Sequence[Sequence[str]]) -> dict[str, str]:
    """Build the settings map from what the settings query returned."""
    settings: dict[str, str] = {}
    for row in rows:
        if len(row) != 2:
            raise ModeConformanceError(
                f'the settings query returned a row of {len(row)} columns, '
                'expected name and setting'
            )
        settings[str(row[0])] = str(row[1])
    return settings


def assert_mode_conformance(observed: ServerMode) -> None:
    """Refuse a cell whose instrument is not the one its limits assume.

    Checked at cell time rather than at window setup, because a window is not
    a guarantee: an instance can be reset, reloaded or replaced between the
    setup and the run, and a cell that trusted the setup would never notice.
    """
    required = REQUIRED_SETTINGS[observed.mode]
    missing = sorted(set(required) - set(observed.settings))
    if missing:
        raise ModeConformanceError(
            f'the server did not report {missing}; a setting nobody read '
            'cannot be shown to hold, and the mode rests on all of them'
        )
    wrong = {
        name: (observed.settings[name], expected)
        for name, expected in required.items()
        if observed.settings[name] != expected
    }
    if wrong:
        detail = ', '.join(
            f'{name} is {actual}, {observed.mode} requires {expected}'
            for name, (actual, expected) in sorted(wrong.items())
        )
        raise ModeConformanceError(
            f'the instrument is not in {observed.mode} mode ({detail}). The '
            'limits this row is judged against were authored for that mode, '
            'so a number taken here is measured true and measured elsewhere'
        )
    if observed.active_client_backends:
        raise ModeConformanceError(
            f'{observed.active_client_backends} other client backend(s) are '
            'active; the mode requires the surrounding units quiesced, and '
            'anything else working on this instance competes for it'
        )


def assert_structure_conformance(
    mode: BenchMode, *, observations_per_side: int, block_size: int
) -> None:
    """Refuse a run shaped differently from what the mode specifies.

    Block size is not a convenience. It is the unit the ordering alternates
    over, so a coarser block gives a run proportionally fewer alternations and
    a longer stretch in which the machine may move on one side alone.
    """
    minimum = MIN_OBSERVATIONS_PER_SIDE[mode]
    if observations_per_side < minimum:
        raise ModeConformanceError(
            f'{observations_per_side} observations per side, {mode} requires '
            f'at least {minimum}'
        )
    required_block = REQUIRED_BLOCK_SIZE[mode]
    if block_size != required_block:
        raise ModeConformanceError(
            f'blocks of {block_size}, {mode} specifies blocks of '
            f'{required_block}; block size is what the ordering alternates '
            'over, so a coarser block means proportionally fewer alternations'
        )


RESTORE_COMMAND_BLOCK: Final = """\
ALTER SYSTEM RESET fsync;
ALTER SYSTEM RESET synchronous_commit;
ALTER SYSTEM RESET full_page_writes;
ALTER SYSTEM RESET autovacuum;
SELECT pg_reload_conf();\
"""

# What the window costs the instance it borrows. Durability is relaxed for
# every database on the server, not only the disposable ones, so the window is
# declared with its restore rather than left to be remembered.
MODE_CONSEQUENCE: Final = (
    'paired-micro relaxes durability instance-wide: every database on this '
    'server runs without fsync, synchronous_commit, full_page_writes or '
    'autovacuum for the duration of the window, and the settings are restored '
    'by RESTORE_COMMAND_BLOCK when it closes'
)
