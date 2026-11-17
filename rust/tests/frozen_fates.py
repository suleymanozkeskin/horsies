"""The fate of every test function of the frozen Rust client crate.

The frozen crate is `crates/horsies` at FREEZE_COMMIT, the last commit before
its engine was deleted (#1404). `frozen-crate-test-fates.tsv` holds one row
per test function of it: the file below `crates/horsies/`, the test as
`module::name`, the fate and its detail.

Fates:
- KEPT: the tip has a test of the same name; the detail names it.
- CHANGED: the tip has the test under another name, for the 0.8.0 rule; the
  detail names it.
- DELETED: the detail names the decision. A detail that holds NO_TWIN names
  a test with no tip test of its name: the check by name lists these.

Commands, from the repository root:
- `python rust/tests/frozen_fates.py build`: write the list from the frozen
  crate (read with `git` at FREEZE_COMMIT) and the tip tree.
- `python rust/tests/frozen_fates.py check`: check that each row has a known
  fate and that each KEPT and CHANGED row names a test of the tip; print the
  rows with no twin by name.

Exit status: 0 when the check passes, 1 with one line per problem.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final

FREEZE_COMMIT: Final = 'a21bd7b255a5ea7473acf3b632db728d46a0e3c9'
REPO_ROOT: Final = Path(__file__).resolve().parents[2]
LIST_PATH: Final = REPO_ROOT / 'rust' / 'tests' / 'frozen-crate-test-fates.tsv'
FROZEN_CRATE: Final = 'crates/horsies'
TIP_CRATES: Final = (('horsies-core', 'crates/horsies-core'), ('horsies', 'crates/horsies'))
NO_TWIN: Final = 'no tip test of this name'

HEADER: Final = (
    '# One fate for every test function of the frozen crate crates/horsies at '
    f'{FREEZE_COMMIT[:10]} (#1754). Built and checked by rust/tests/frozen_fates.py.\n'
    '# fate: KEPT (detail: the tip test of the same name), CHANGED (detail: the tip test '
    'under another name, for the 0.8.0 rule), DELETED (detail: the decision; "'
    f'{NO_TWIN}" marks a test with no twin by name).\n'
    'file\ttest\tfate\tdetail\n'
)

_ATTR: Final = re.compile(r'^\s*#\[(?:tokio::test|test|sqlx::test)\b')
_FN: Final = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_]\w*)')
_MOD: Final = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_]\w*)\s*\{')
_STRING: Final = re.compile(r'"(?:\\.|[^"\\])*"')
_CHAR: Final = re.compile(r"'(?:\\.|[^'\\])'")

# Tests that the tip holds under another name, for the 0.8.0 rule (one input
# value, the core's catalogs, the clean-slate history): frozen name -> tip
# test as `module::name` in horsies-core.
RENAMED: Final[dict[str, str]] = {
    'terminal_move_emits_task_done_without_a_monitoring_status_event': 'broker::monitoring_schema_tests::a_terminal_move_sends_task_done_and_the_status_event',
    'cancel_locked_refuses_workflow_tasks_and_never_overwrites_terminal': 'broker::terminalization_matrix::cancel_task_refuses_workflow_tasks_and_never_overwrites_terminal',
    'declarations_and_queue_mappings_fail_closed': 'core::config::retention::tests::periods_and_queue_mappings_fail_closed',
    'precedence_and_registrable_classes_are_exact': 'core::config::retention::tests::precedence_sendable_and_registrable_periods_are_exact',
    'command_union_carries_exactly_the_seven_variants': 'core::history::commands::tests::command_union_carries_exactly_the_six_variants',
    'partition_commands_reject_empty_health_class_and_invalid_timeouts': 'core::history::tests::partition_commands_reject_invalid_timeouts',
    'open_ended_start_and_predicate_render_one_pinned_text': 'core::history::partitions::catalog::open_ended_leaf_sql_tests::open_ended_predicate_renders_one_pinned_text',
    'manifest_excludes_absent_probes_but_birth_floor_keeps_the_complete_attached_set': 'core::history::partitions::tests::manifest_excludes_absent_probes_and_keeps_the_complete_attached_set',
    'guard_probe_set_is_standard_horizon_plus_the_open_ended_leaf': 'core::history::partitions::tests::guard_probe_set_is_standard_horizon_plus_the_static_forever_leaf',
    'fifteen_commands_fifteen_kinds': 'core::lifecycle::tests::eleven_commands_eleven_kinds',
    'display_codes': 'core::task::error::tests::display_codes_and_errors',
    'serialize_retry_options_emits_timeout_ms_alone': 'core::task::options::tests::stored_options_text_emits_timeout_ms_alone',
    'case_name_first_satisfied': 'core::workflow::policy::tests::evaluate_first_case_satisfied',
    'case_name_second_satisfied': 'core::workflow::policy::tests::evaluate_second_case_satisfied',
    'success_case_name_serde_round_trip': 'core::workflow::policy::tests::success_case_serde_round_trip',
    'validate_retry_rejects_internal_failed': 'core::workflow::start_types::tests::validate_retry_rejects_validation_failed',
    'broadcaster_uses_its_own_listener_and_batches_real_notify_bursts': 'web::tests::broadcaster_subscribes_on_the_broker_listener_and_batches_real_notify_bursts',
    'notify_worker_capacity_emits_queue_specific_signal_only': 'worker::worker::tests::capacity_wake_emits_queue_specific_signal_only',
    'merge_missing_dep_wraps_in_task_result_err': 'workflow_engine::args_merge::tests::merge_failed_dep_wraps_in_task_result_err',
    'merge_preserves_existing_kwargs': 'workflow_engine::args_merge::tests::merge_preserves_existing_input_fields',
    'merge_invalid_existing_kwargs_is_error': 'workflow_engine::args_merge::tests::merge_invalid_existing_input_is_error',
    'merge_deadline_round_trips_through_parse': 'workflow_engine::tests::merge_deadline_round_trips_through_the_reader',
    'recovery_rolls_back_one_failed_row_and_advances_the_next_candidate': 'workflow_engine::phase2_recovery::tests::recovery_rolls_back_one_failed_row_counts_its_raise_and_advances_the_next_candidate',
    'workflow_handle_get_fails_closed_on_malformed_expiry_error': 'workflow_engine::query::wait_tests::the_wait_returns_a_malformed_expiry_error_as_stored',
    'zero_scan_limit_has_undefined_completion_yield': 'workflow_engine::recovery::cap_tests::a_zero_page_size_is_refused_and_has_undefined_completion_yield',
    'get_workflow_result_no_timeout_returns_not_found_for_missing_workflow': 'workflow_engine::query::wait_tests::a_wait_with_no_timeout_returns_not_found_for_a_missing_workflow',
    'workflow_handle_get_returns_immediately_for_paused_workflow': 'workflow_engine::query::wait_tests::the_wait_returns_immediately_for_a_paused_workflow',
    'workflow_handle_get_preserves_structured_expiry_error': 'workflow_engine::query::wait_tests::the_wait_returns_a_structured_expiry_error_as_stored',
}

CLEAN_SLATE: Final = (
    'clean-slate baseline: no cutover, transcode or upgrade of an older database '
    '(decisions "Schema for 1.0" and "No shims in the clean slate", 2026-11-15)'
)
_CLEAN_SLATE_FILE: Final = re.compile(
    r'(cutover|transcode|migrations\.rs|terminalization_matrix/schema\.rs)'
)
_CLEAN_SLATE_NAME: Final = re.compile(
    r'(cutover|transcode|upgrade|legacy|migration_00|_v\d\d_|_v\d\d$|v\d\d_fixture)'
)


class Fate(Enum):
    KEPT = 'KEPT'
    CHANGED = 'CHANGED'
    DELETED = 'DELETED'


_FATES: Final = frozenset(fate.value for fate in Fate)


@dataclass(frozen=True, slots=True)
class TestFn:
    """One test function: its file below the crate, module path and name."""

    file: str
    module: str
    name: str

    @property
    def path(self) -> str:
        return f'{self.module}::{self.name}' if self.module else self.name


@dataclass(frozen=True, slots=True)
class GitFailed:
    """`git archive` or `tar` refused: the step and its output."""

    step: str
    output: str


@dataclass(frozen=True, slots=True)
class Row:
    file: str
    test: str
    fate: Fate
    detail: str


def _module_of(file: str) -> list[str]:
    match file.split('/'):
        case ['src', *rest]:
            parts = rest
            parts[-1] = parts[-1].removesuffix('.rs')
            return parts[:-1] if parts[-1] in ('mod', 'lib', 'main') else parts
        case _:
            return [file]


def _code(line: str) -> str:
    """The line without string and char literals and its line comment."""
    return _CHAR.sub("''", _STRING.sub('""', line)).split('//')[0]


def _tests_of_file(file: str, text: str) -> list[TestFn]:
    found: list[TestFn] = []
    base = _module_of(file)
    stack: list[tuple[str, int]] = []
    depth = 0
    after_attribute = False
    for line in text.split('\n'):
        opened = _MOD.match(line)
        if _ATTR.match(line):
            after_attribute = True
        function = _FN.match(line)
        if function is not None and after_attribute:
            module = '::'.join(base + [name for name, _depth in stack])
            found.append(TestFn(file=file, module=module, name=function.group(1)))
            after_attribute = False
        code = _code(line)
        if opened is not None:
            stack.append((opened.group(1), depth))
        depth += code.count('{') - code.count('}')
        while stack and depth <= stack[-1][1]:
            stack.pop()
    return found


def tests_of_crate(crate_dir: Path) -> list[TestFn]:
    """The test functions of the Rust files below `crate_dir`, by path."""
    found: list[TestFn] = []
    for path in sorted(crate_dir.rglob('*.rs')):
        file = path.relative_to(crate_dir).as_posix()
        if file.startswith('target/'):
            continue
        found.extend(_tests_of_file(file, path.read_text(encoding='utf-8')))
    return found


def _frozen_tests() -> list[TestFn] | GitFailed:
    """The test functions of the frozen crate, read from git history."""
    with tempfile.TemporaryDirectory(prefix='frozen-crate-') as folder:
        archive = subprocess.run(
            ['git', 'archive', FREEZE_COMMIT, FROZEN_CRATE],
            cwd=REPO_ROOT, capture_output=True, check=False,
        )
        if archive.returncode != 0:
            return GitFailed('git archive', archive.stderr.decode(errors='replace'))
        unpacked = subprocess.run(
            ['tar', '-x', '-C', folder], input=archive.stdout, capture_output=True, check=False,
        )
        if unpacked.returncode != 0:
            return GitFailed('tar', unpacked.stderr.decode(errors='replace'))
        return tests_of_crate(Path(folder) / FROZEN_CRATE)


def _tip_index() -> dict[str, list[tuple[str, TestFn]]]:
    """Each test name of the tip crates -> (crate, test)."""
    index: dict[str, list[tuple[str, TestFn]]] = defaultdict(list)
    for crate, folder in TIP_CRATES:
        for test in tests_of_crate(REPO_ROOT / folder):
            index[test.name].append((crate, test))
    return index


def _twin(test: TestFn, index: dict[str, list[tuple[str, TestFn]]]) -> str | None:
    """The tip test of the same name: the core file of the same path first."""
    candidates = index.get(test.name, [])
    ranked = sorted(
        candidates,
        key=lambda found: (found[1].file != test.file, found[0] != 'horsies-core', found[1].path),
    )
    if not ranked:
        return None
    crate, twin = ranked[0]
    return f'{crate} {twin.path}'


def _deleted_detail(test: TestFn) -> str:
    match test.file.split('/'):
        case _ if _CLEAN_SLATE_FILE.search(test.file) or _CLEAN_SLATE_NAME.search(test.name):
            return CLEAN_SLATE
        case (
            ['src', 'core', 'app.rs']
            | ['src', 'core', 'registry', 'workflow.rs']
            | ['src', 'core', 'task', 'macros.rs']
            | ['src', 'core', 'workflow', 'spec.rs' | 'node.rs' | 'sub_workflow.rs' | 'definition.rs']
            | ['src', 'workflow_engine', 'query.rs']
        ):
            return (
                'the typed API of the old Rust client left horsies-core (#1830, #1832, #1833, '
                f'#1839); the pull request that removed the test names its covering test; {NO_TWIN}'
            )
        case ['src', 'task.rs' | 'lib.rs' | 'workflow.rs']:
            return (
                'the frozen facade API is replaced by the 0.8.0 client API (build plan 1.2 and '
                f'its changelog list 2.16); {NO_TWIN}'
            )
        case ['src', 'worker', 'cli', *_]:
            return (
                'the frozen CLI is replaced by horsies::cli::run and the rewritten horsies '
                f'binary (build plan 1.2); {NO_TWIN}'
            )
        case ['src', 'broker' | 'worker' | 'workflow_engine' | 'monitoring', *_]:
            return f'deleted with the frozen engine; the core holds the newer engine (build plan 1.2); {NO_TWIN}'
        case ['src', 'web', *_]:
            return (
                'the frozen axum server is replaced by the mount over the core handler '
                f'(build plan 1.2); {NO_TWIN}'
            )
        case ['src', 'core', *_]:
            return (
                'deleted with the frozen copy of the shared types; the core copy is the newer '
                f'code (build plan 1.2); {NO_TWIN}'
            )
        case _:
            return f'deleted with the frozen crate (build plan 1.2); {NO_TWIN}'


def build_rows() -> list[Row] | GitFailed:
    frozen = _frozen_tests()
    if isinstance(frozen, GitFailed):
        return frozen
    index = _tip_index()
    rows: list[Row] = []
    for test in frozen:
        twin = _twin(test, index)
        renamed = RENAMED.get(test.name)
        match (twin, renamed):
            case (str() as found, _):
                rows.append(Row(test.file, test.path, Fate.KEPT, found))
            case (None, str() as other):
                rows.append(Row(test.file, test.path, Fate.CHANGED, f'horsies-core {other}'))
            case (None, None):
                rows.append(Row(test.file, test.path, Fate.DELETED, _deleted_detail(test)))
    return rows


def write_rows(rows: list[Row]) -> None:
    lines = [f'{row.file}\t{row.test}\t{row.fate.value}\t{row.detail}' for row in rows]
    _ = LIST_PATH.write_text(HEADER + '\n'.join(lines) + '\n', encoding='utf-8')


def read_rows() -> tuple[list[Row], list[str]]:
    rows: list[Row] = []
    problems: list[str] = []
    text = LIST_PATH.read_text(encoding='utf-8').split('\n')
    for number, line in enumerate(text, start=1):
        if not line or line.startswith('#') or line == 'file\ttest\tfate\tdetail':
            continue
        match line.split('\t'):
            case [file, test, fate, detail] if fate in _FATES:
                rows.append(Row(file, test, Fate(fate), detail))
            case _:
                problems.append(f'{LIST_PATH.name}:{number}: not a row with a known fate: {line!r}')
    return rows, problems


def check_rows(rows: list[Row]) -> list[str]:
    """The rows whose KEPT or CHANGED detail names no test of the tip."""
    known: set[str] = set()
    for crate, folder in TIP_CRATES:
        known.update(f'{crate} {test.path}' for test in tests_of_crate(REPO_ROOT / folder))
    return [
        f'{row.file} {row.test}: {row.fate.value} names {row.detail!r}, which the tip does not hold'
        for row in rows
        if row.fate in (Fate.KEPT, Fate.CHANGED) and row.detail not in known
    ]


def main(arguments: list[str]) -> int:
    match arguments:
        case ['build']:
            built = build_rows()
            if isinstance(built, GitFailed):
                print(f'{built.step} of {FREEZE_COMMIT} failed: {built.output}', file=sys.stderr)
                return 1
            rows = built
            write_rows(rows)
            counts = {fate: sum(1 for row in rows if row.fate is fate) for fate in Fate}
            print(', '.join(f'{fate.value}: {count}' for fate, count in counts.items()))
            return 0
        case ['check']:
            rows, problems = read_rows()
            problems.extend(check_rows(rows))
            no_twin = [row for row in rows if NO_TWIN in row.detail]
            for row in no_twin:
                print(f'no twin: {row.file} {row.test}')
            print(f'{len(rows)} rows, {len(no_twin)} with no twin by name')
            for problem in problems:
                print(problem, file=sys.stderr)
            return 1 if problems else 0
        case _:
            print('usage: frozen_fates.py build|check', file=sys.stderr)
            return 2


if __name__ == '__main__':
    os.chdir(REPO_ROOT)
    sys.exit(main(sys.argv[1:]))
