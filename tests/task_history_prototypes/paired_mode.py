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
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final, cast


class ModeConformanceError(Exception):
    """The instrument is not the one this row's limits belong to."""


class BenchMode(StrEnum):
    """The performance modes the budgets define.

    They are not variations on one instrument. Paired-micro strips durability
    so a latency delta is not dominated by disk sync; operational restores it
    because partition DDL, vacuum and WAL are the subject rather than the
    noise. A row measured in the wrong one is measured against limits written
    for the other.
    """

    PAIRED_MICRO = 'paired-micro'
    OPERATIONAL = 'operational'


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
    # Section 2.2, verbatim. Every value is the opposite of paired-micro's,
    # because the subject is different: partition DDL, long readers, vacuum and
    # freeze, sustained backlog, migration, transcode, disk, and
    # production-shaped WAL.
    BenchMode.OPERATIONAL: {
        'fsync': 'on',
        'synchronous_commit': 'on',
        'full_page_writes': 'on',
        'autovacuum': 'on',
    },
}

# Section 2.2 requires autovacuum on "with recorded thresholds and scale
# factors". A mode that turns autovacuum on without saying at what settings has
# declared nothing: the same workload behaves differently at a 0.2 scale factor
# than at 0.02, and the report would not say which it measured.
REQUIRED_RECORDED_PARAMETERS: Final[Mapping[BenchMode, tuple[str, ...]]] = {
    BenchMode.PAIRED_MICRO: (),
    BenchMode.OPERATIONAL: (
        'autovacuum_vacuum_threshold',
        'autovacuum_vacuum_scale_factor',
        'autovacuum_analyze_threshold',
        'autovacuum_analyze_scale_factor',
        'autovacuum_vacuum_insert_threshold',
        'autovacuum_vacuum_insert_scale_factor',
        'autovacuum_naptime',
        'autovacuum_max_workers',
        'autovacuum_vacuum_cost_delay',
        'autovacuum_vacuum_cost_limit',
        'checkpoint_timeout',
        'max_wal_size',
    ),
}

# §2.1 again: warm single-row paths use at least 10,000 observations per side
# in blocks of 100.
# Paired-micro only. Section 2.2 states no observation count or block size,
# because an operational row is not a pooled percentile over blocks — it is a
# set of intervals reported separately. A mode with no paired structure is
# recorded as None rather than as zero, so "no requirement" cannot be mistaken
# for "a requirement of nothing".
MIN_OBSERVATIONS_PER_SIDE: Final[Mapping[BenchMode, int | None]] = {
    BenchMode.PAIRED_MICRO: 10_000,
    BenchMode.OPERATIONAL: None,
}
REQUIRED_BLOCK_SIZE: Final[Mapping[BenchMode, int | None]] = {
    BenchMode.PAIRED_MICRO: 100,
    BenchMode.OPERATIONAL: None,
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
    """What the server reported about itself, at the moment a cell ran.

    ``recorded_parameters`` carries the values a mode requires reported but
    does not constrain — autovacuum's thresholds and scale factors above all.
    They are conditions, not requirements: the report must say what they were,
    and any value is permitted as long as it is stated.

    ``declared_competing_processes`` names what is deliberately NOT quiesced.
    Section 2.2's exception for a named reader, writer or maintenance process
    is an instrument setting like any other, so it is declared rather than
    merely permitted, and a run with unexplained activity is still refused.
    """

    mode: BenchMode
    settings: Mapping[str, str]
    active_client_backends: int
    recorded_parameters: Mapping[str, str] = field(
        default_factory=lambda: cast(Mapping[str, str], {})
    )
    declared_competing_processes: tuple[str, ...] = ()

    def as_conditions(self) -> dict[str, Any]:
        return {
            'mode': self.mode.value,
            'server_settings': dict(self.settings),
            'active_client_backends': self.active_client_backends,
            'recorded_parameters': dict(self.recorded_parameters),
            'declared_competing_processes': list(
                self.declared_competing_processes
            ),
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
    unrecorded = sorted(
        set(REQUIRED_RECORDED_PARAMETERS[observed.mode])
        - set(observed.recorded_parameters)
    )
    if unrecorded:
        raise ModeConformanceError(
            f'{observed.mode} requires these reported and they are absent: '
            f'{unrecorded}. The mode does not constrain their values, but a '
            'report that does not say what they were has not said which '
            'instrument it measured'
        )
    undeclared = observed.active_client_backends - len(
        observed.declared_competing_processes
    )
    if undeclared > 0:
        raise ModeConformanceError(
            f'{observed.active_client_backends} other client backend(s) are '
            f'active and {len(observed.declared_competing_processes)} are '
            'declared; the mode requires the surrounding units quiesced apart '
            'from processes named as under test, and undeclared activity '
            'competes for the instrument without appearing in the conditions'
        )


def assert_structure_conformance(
    mode: BenchMode, *, observations_per_side: int, block_size: int
) -> None:
    """Refuse a run shaped differently from what the mode specifies.

    Block size is not a convenience. It is the unit the ordering alternates
    over, so a coarser block gives a run proportionally fewer alternations and
    a longer stretch in which the machine may move on one side alone.

    A mode with no paired structure imposes none of this: an operational row
    reports intervals separately rather than pooling observations, so there is
    no per-side count for a minimum to apply to.
    """
    minimum = MIN_OBSERVATIONS_PER_SIDE[mode]
    if minimum is None:
        return
    if observations_per_side < minimum:
        raise ModeConformanceError(
            f'{observations_per_side} observations per side, {mode} requires '
            f'at least {minimum}'
        )
    required_block = REQUIRED_BLOCK_SIZE[mode]
    if required_block is None:
        return
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
