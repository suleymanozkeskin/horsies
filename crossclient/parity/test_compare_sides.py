"""The pure parts of the parity runner, with no database and no process:
the sides and their families, names, commands and programs, the scenarios
that each side offers, and the compare of two sides (`compare_sides`): the
planned difference, the judgement of the other snapshot differences by the
hour rule of the whole-run compare, and the report differences.

The test names spell the TypeScript side `ts`: the `rust-e2e` job runs this
file with `-k "not typescript"`, which deselects every test whose id holds
`typescript`."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import pytest

from crossclient.parity.snapshot.compare import AmbiguousRows, ColumnDifference, Side, SnapshotDiff
from crossclient.parity.snapshot.normalize import AmbiguousGroup, NormalizedSnapshot, NormalizedTable
from crossclient.parity.snapshot.values import ExactValue, RunWindow
from crossclient.parity.snapshot.worker_run import (
    WHOLE_RUN_CATALOG,
    ClockBoundaryStraddle,
    DualRunMismatch,
    PairEqual,
)
from crossclient.parity.runner import (
    BUN_BINARY_ENV,
    NATIVE_ADDON_BUILD,
    NATIVE_ADDON_PATH_ENV,
    NODE_BINARY_ENV,
    REFERENCE,
    REPO_ROOT,
    RUST_BINARY_ENV,
    RUST_BUILD,
    TYPESCRIPT_ENTRY_DEFAULT,
    TYPESCRIPT_ENTRY_ENV,
    AttemptReport,
    Family,
    JsRuntime,
    NotOffered,
    Offered,
    ProgramMissing,
    ProgramPresent,
    PythonSide,
    ReportEntry,
    ReportKey,
    RustSide,
    SideCommands,
    SideRun,
    TaskReport,
    TypeScriptSide,
    compare_sides,
    family_of,
    side_commands,
    side_name,
    side_program,
    scenario_offered,
    typescript_entry,
)
from crossclient.parity.runner import Side as ParitySide
from crossclient.parity.scenarios import SCENARIOS, Scenario, scenario_named

pytestmark = [pytest.mark.unit]

ONE_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 12, 5, tzinfo=UTC),
    end=datetime(2026, 11, 18, 12, 9, tzinfo=UTC),
)
ACROSS_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 12, 59, 48, tzinfo=UTC),
    end=datetime(2026, 11, 18, 13, 1, 2, tzinfo=UTC),
)
"""The run span of a Python side across 13:00Z."""
NEXT_HOUR: Final = RunWindow(
    start=datetime(2026, 11, 18, 13, 1, 4, tzinfo=UTC),
    end=datetime(2026, 11, 18, 13, 2, 3, tzinfo=UTC),
)
"""A span in the hour after `ONE_HOUR`; it does not cross an hour."""

TIE: Final = AmbiguousGroup(table='horsies_tasks', order_key=(), row_count=2)
"""An unplanned difference: two task rows of one side tie on their order."""

COMPLETED: Final = TaskReport(
    status='COMPLETED',
    error_code=None,
    retry_count=0,
    attempts=(AttemptReport(attempt=1, outcome='COMPLETED', error_code=None),),
)

NODE_SIDE: Final = TypeScriptSide(runtime=JsRuntime.NODE)
BUN_SIDE: Final = TypeScriptSide(runtime=JsRuntime.BUN)

THREAD_SCENARIOS: Final = ('shared_thread_outcomes', 'shared_thread_timeout', 'several_workers')
"""The scenarios with a profile of SHARED_THREAD children in the parity app."""


def _snapshot(family: str, ambiguities: tuple[AmbiguousGroup, ...]) -> NormalizedSnapshot:
    """A snapshot of the whole-run catalog whose only row is the client
    family row of `family`."""
    return NormalizedSnapshot(
        tables=tuple(
            NormalizedTable(
                rules=table,
                rows=(
                    (ExactValue(text='true'), ExactValue(text=family), ExactValue(text='t')),
                )
                if table.name == 'horsies_client_family'
                else (),
            )
            for table in WHOLE_RUN_CATALOG.tables
        ),
        ambiguities=ambiguities,
    )


def _side(
    side: ParitySide,
    span: RunWindow,
    *,
    ambiguities: tuple[AmbiguousGroup, ...] = (),
    report: dict[ReportKey, ReportEntry] | None = None,
) -> SideRun:
    """The run of `side` whose family row holds the family of `side`."""
    return SideRun(
        side=side,
        snapshot=_snapshot(family_of(side).value, ambiguities),
        report={} if report is None else report,
        span=span,
    )


def _side_with_family(side: ParitySide, family: Family) -> SideRun:
    """The run of `side` whose family row holds `family`."""
    return SideRun(side=side, snapshot=_snapshot(family.value, ()), report={}, span=ONE_HOUR)


# ---- The sides ----


def test_each_side_has_its_family_and_name() -> None:
    sides: tuple[ParitySide, ...] = (PythonSide(), RustSide(), NODE_SIDE, BUN_SIDE)

    assert [family_of(side) for side in sides] == [
        Family.PYTHON, Family.RUST, Family.TYPESCRIPT, Family.TYPESCRIPT,
    ]
    assert [side_name(side) for side in sides] == [
        'python', 'rust', 'typescript-node', 'typescript-bun',
    ]


def test_the_rust_side_runs_its_binary_with_each_command() -> None:
    commands = side_commands(RustSide(), {RUST_BINARY_ENV: '/opt/horsies-parity'})

    assert commands == SideCommands(
        worker=['/opt/horsies-parity', 'worker'],
        send=['/opt/horsies-parity', 'send'],
        scheduler=['/opt/horsies-parity', 'scheduler'],
    )


def test_a_ts_side_runs_its_runtime_with_the_entry_and_each_command() -> None:
    entry = str(REPO_ROOT / TYPESCRIPT_ENTRY_DEFAULT)

    assert side_commands(NODE_SIDE, {}) == SideCommands(
        worker=['node', entry, 'worker'], send=['node', entry, 'send'], scheduler=['node', entry, 'scheduler'],
    )
    assert side_commands(BUN_SIDE, {BUN_BINARY_ENV: '/opt/bun'}).worker == ['/opt/bun', entry, 'worker']
    assert side_commands(NODE_SIDE, {NODE_BINARY_ENV: '/opt/node'}).send == ['/opt/node', entry, 'send']


def test_the_ts_entry_is_against_the_repository_root_unless_absolute(tmp_path: Path) -> None:
    assert typescript_entry({}) == REPO_ROOT / TYPESCRIPT_ENTRY_DEFAULT
    assert typescript_entry({TYPESCRIPT_ENTRY_ENV: 'other/main.ts'}) == REPO_ROOT / 'other' / 'main.ts'
    assert typescript_entry({TYPESCRIPT_ENTRY_ENV: str(tmp_path / 'main.ts')}) == tmp_path / 'main.ts'


# ---- The programs of a side ----


def test_the_python_side_has_its_program() -> None:
    assert side_program(REFERENCE, {}) == ProgramPresent()


def test_a_missing_rust_binary_fails_with_the_build_command(tmp_path: Path) -> None:
    missing = tmp_path / 'horsies-parity'

    match side_program(RustSide(), {RUST_BINARY_ENV: str(missing)}):
        case ProgramMissing(hint=hint):
            assert str(missing) in hint
            assert RUST_BUILD in hint
        case ProgramPresent():
            pytest.fail('a missing Rust binary is present')


def test_a_built_rust_binary_is_present(tmp_path: Path) -> None:
    binary = tmp_path / 'horsies-parity'
    binary.write_bytes(b'')

    assert side_program(RustSide(), {RUST_BINARY_ENV: str(binary)}) == ProgramPresent()


def _ts_environment(tmp_path: Path) -> dict[str, str]:
    """The variables of a TypeScript side whose runtime, entry and addon
    exist: the runtime is this Python interpreter, a file that `PATH` lookup
    accepts."""
    entry = tmp_path / 'main.ts'
    entry.write_text('')
    addon = tmp_path / 'horsies.node'
    addon.write_bytes(b'')
    return {
        NODE_BINARY_ENV: sys.executable,
        BUN_BINARY_ENV: sys.executable,
        TYPESCRIPT_ENTRY_ENV: str(entry),
        NATIVE_ADDON_PATH_ENV: str(addon),
    }


def test_a_ts_side_with_its_runtime_entry_and_addon_is_present(tmp_path: Path) -> None:
    environment = _ts_environment(tmp_path)

    assert side_program(NODE_SIDE, environment) == ProgramPresent()
    assert side_program(BUN_SIDE, environment) == ProgramPresent()


@pytest.mark.parametrize(
    ('variable', 'value', 'named'),
    [
        (NODE_BINARY_ENV, 'no-such-runtime-program', NODE_BINARY_ENV),
        (TYPESCRIPT_ENTRY_ENV, '/no/such/main.ts', TYPESCRIPT_ENTRY_ENV),
        (NATIVE_ADDON_PATH_ENV, '/no/such/horsies.node', NATIVE_ADDON_BUILD),
        (NATIVE_ADDON_PATH_ENV, 'horsies.node', NATIVE_ADDON_BUILD),
    ],
    ids=['runtime', 'entry', 'addon', 'relative-addon'],
)
def test_a_missing_part_of_the_ts_node_side_fails_with_its_fix(
    tmp_path: Path, variable: str, value: str, named: str,
) -> None:
    environment = {**_ts_environment(tmp_path), variable: value}

    match side_program(NODE_SIDE, environment):
        case ProgramMissing(hint=hint):
            assert named in hint
            assert value in hint
        case ProgramPresent():
            pytest.fail(f'the side is present with {variable}={value}')


def test_an_unset_addon_variable_fails_with_the_build_command(tmp_path: Path) -> None:
    environment = _ts_environment(tmp_path)
    del environment[NATIVE_ADDON_PATH_ENV]

    match side_program(BUN_SIDE, environment):
        case ProgramMissing(hint=hint):
            assert NATIVE_ADDON_BUILD in hint
        case ProgramPresent():
            pytest.fail('the side is present with no addon')


# ---- The scenarios that a side offers ----


def test_the_reference_rust_and_node_sides_offer_every_scenario() -> None:
    for side in (REFERENCE, RustSide(), NODE_SIDE):
        assert [scenario_offered(side, scenario) for scenario in SCENARIOS] == [
            Offered() for _ in SCENARIOS
        ]


def test_bun_does_not_offer_a_scenario_with_thread_children() -> None:
    for name in THREAD_SCENARIOS:
        match scenario_offered(BUN_SIDE, scenario_named(name)):
            case NotOffered(reason=reason):
                assert reason.startswith(f'{name} runs the profile shared-thread-1x4 with SHARED_THREAD')
            case Offered():
                pytest.fail(f'Bun offers {name}')


def test_bun_offers_every_scenario_with_no_thread_children() -> None:
    others: list[Scenario] = [scenario for scenario in SCENARIOS if scenario.name not in THREAD_SCENARIOS]

    assert len(others) == len(SCENARIOS) - len(THREAD_SCENARIOS)
    assert [scenario_offered(BUN_SIDE, scenario) for scenario in others] == [Offered() for _ in others]


# ---- The compare ----


def test_the_planned_difference_alone_is_equal() -> None:
    result = compare_sides(_side(REFERENCE, ONE_HOUR), _side(RustSide(), NEXT_HOUR))

    assert result.snapshots == PairEqual()
    assert result.planned_seen
    assert result.report_differences == ()


def test_python_against_the_ts_family_is_the_planned_difference() -> None:
    for side in (NODE_SIDE, BUN_SIDE):
        result = compare_sides(_side(REFERENCE, ONE_HOUR), _side(side, NEXT_HOUR))

        assert result.snapshots == PairEqual()
        assert result.planned_seen


def test_a_rust_family_row_on_a_ts_side_is_an_unplanned_difference() -> None:
    result = compare_sides(_side(REFERENCE, ONE_HOUR), _side_with_family(NODE_SIDE, Family.RUST))

    assert not result.planned_seen
    match result.snapshots:
        case DualRunMismatch(
            diff=SnapshotDiff(
                entries=(
                    ColumnDifference(
                        table='horsies_client_family',
                        column='family',
                        reference=ExactValue(text='PYTHON'),
                        candidate=ExactValue(text='RUST'),
                    ),
                ),
            ),
        ):
            pass
        case other:
            pytest.fail(f'the judgement is {other!r}')


def test_a_ts_family_row_on_the_rust_side_is_an_unplanned_difference() -> None:
    result = compare_sides(
        _side(REFERENCE, ONE_HOUR), _side_with_family(RustSide(), Family.TYPESCRIPT),
    )

    assert not result.planned_seen
    assert isinstance(result.snapshots, DualRunMismatch)


def test_equal_client_family_rows_are_no_planned_difference() -> None:
    result = compare_sides(_side(REFERENCE, ONE_HOUR), _side_with_family(RustSide(), Family.PYTHON))

    assert result.snapshots == PairEqual()
    assert not result.planned_seen


def test_an_unplanned_difference_in_one_hour_is_a_mismatch() -> None:
    result = compare_sides(
        _side(REFERENCE, ONE_HOUR, ambiguities=(TIE,)),
        _side(RustSide(), ONE_HOUR),
    )

    assert result.snapshots == DualRunMismatch(
        diff=SnapshotDiff(
            entries=(
                AmbiguousRows(
                    side=Side.REFERENCE, table='horsies_tasks', order_key=(), row_count=2,
                ),
            ),
        ),
    )
    assert result.planned_seen


def test_an_unplanned_difference_across_an_hour_is_a_straddle_with_every_entry() -> None:
    result = compare_sides(
        _side(REFERENCE, ACROSS_HOUR, ambiguities=(TIE,)),
        _side(RustSide(), NEXT_HOUR),
    )

    assert result.snapshots == ClockBoundaryStraddle(
        diff=SnapshotDiff(
            entries=(
                AmbiguousRows(
                    side=Side.REFERENCE, table='horsies_tasks', order_key=(), row_count=2,
                ),
            ),
        ),
        reference_span=ACROSS_HOUR,
        candidate_span=NEXT_HOUR,
    )


def test_a_report_difference_names_the_key_and_both_sides() -> None:
    """The whole-run pairs had the same rule (`compare_reports`)."""
    retried = TaskReport(status=None, error_code=None, retry_count=None, attempts=())
    python_report: dict[ReportKey, ReportEntry] = {('task', 0): COMPLETED, ('task', 1): COMPLETED}
    candidate_report: dict[ReportKey, ReportEntry] = {('task', 0): COMPLETED, ('task', 1): retried}

    differing = compare_sides(
        _side(REFERENCE, ONE_HOUR, report=python_report),
        _side(NODE_SIDE, ONE_HOUR, report=candidate_report),
    )
    equal = compare_sides(
        _side(REFERENCE, ONE_HOUR, report=python_report),
        _side(NODE_SIDE, ONE_HOUR, report=dict(python_report)),
    )

    assert differing.report_differences == ((('task', 1), COMPLETED, retried),)
    assert equal.report_differences == ()
