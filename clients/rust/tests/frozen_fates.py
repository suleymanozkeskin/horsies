"""The fate of every test function of the frozen Rust client crate.

The frozen crate is `crates/horsies` at FREEZE_COMMIT, the last commit before
its engine was deleted (#1404). `frozen-crate-test-fates.tsv` holds one row
per test function of it: the file below `clients/rust/horsies/`, the test as
`module::name`, the fate and its detail.

Fates and their details:
- KEPT: the tip has a test of the same name in `horsies-core` or the client;
  the detail is `<crate> <module::name>`.
- CHANGED: the tip has the test under another name, for the 0.8.0 rule
  (`RENAMED`); the detail is as for KEPT.
- COVERED: another tip test holds the same guarantee. The detail is one or
  more test references joined by ` + `, then ` -- ` and a note when the form
  of the guarantee changed. A test reference is `<repository path>::<name>`:
  for a Rust file the test function, with the inline modules above it when
  the name alone is not unique; for a Python file the pytest name
  (`Class::test` or `test`).
- PORTED: the test was ported into the tip; the detail is as for COVERED.
- DELETED: the guarantee no longer exists. The detail is the reason, then
  ` -- code: ` and one or more code references joined by ` + `: a
  repository path, or `<repository path>::<text>` where the file holds the
  text.

A row whose detail holds `UNACCOUNTED` has no fate yet: `build` writes it
for a frozen test with no tip test of its name and no recorded fate.

Commands, from the repository root:
- `python clients/rust/tests/frozen_fates.py build`: write the list from the frozen
  crate (read with `git` at FREEZE_COMMIT) and the tip tree. A COVERED,
  PORTED or DELETED row of the current list is kept as it is.
- `python clients/rust/tests/frozen_fates.py check`: check that each row has a known
  fate, that each test and code reference resolves in the tip, and that no
  row is unaccounted.

Exit status: 0 when the check passes, 1 with one line per problem.
"""

from __future__ import annotations

import ast
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
REPO_ROOT: Final = Path(__file__).resolve().parents[3]
LIST_PATH: Final = REPO_ROOT / 'clients' / 'rust' / 'tests' / 'frozen-crate-test-fates.tsv'
FROZEN_CRATE: Final = 'crates/horsies'
TIP_CRATES: Final = (('horsies-core', 'core'), ('horsies', 'clients/rust/horsies'))
UNACCOUNTED: Final = 'UNACCOUNTED: no tip test of this name and no recorded fate'

REFERENCE_JOIN: Final = ' + '
NOTE_MARK: Final = ' -- '
CODE_MARK: Final = ' -- code: '

HEADER: Final = (
    '# One fate for every test function of the frozen crate crates/horsies at '
    f'{FREEZE_COMMIT[:10]} (#1754). Built and checked by clients/rust/tests/frozen_fates.py.\n'
    '# fate: KEPT (detail: the tip test of the same name), CHANGED (detail: the tip test '
    'under another name, for the 0.8.0 rule), COVERED (detail: the tip tests that hold the '
    'same guarantee, then a note), PORTED (detail: the ported tests), DELETED (detail: why '
    'the guarantee no longer exists, then the code that shows it).\n'
    'file\ttest\tfate\tdetail\n'
)

_ATTR: Final = re.compile(r'^\s*#\[(?:tokio::test|test|sqlx::test)\b')
_FN: Final = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_]\w*)')
_MOD: Final = re.compile(r'^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_]\w*)\s*\{')
_CHAR: Final = re.compile(r"'(?:\\.|[^'\\])'")
_RAW_STRING_OPEN: Final = re.compile(r'b?r(#*)"')
_IDENTIFIER_CHAR: Final = re.compile(r'\w')

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
    'busy_coverage_gate_refuses_startup_when_current_heartbeat_is_absent': 'core::history::partitions::tests::busy_coverage_gate_makes_startup_wait_when_current_heartbeat_is_absent',
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
    'clean-slate baseline: the schema is one baseline (migration 1000); a database of an '
    'older horsies is refused, never cut over, transcoded, migrated or upgraded'
    f'{CODE_MARK}core/src/broker/migrations.rs::pub enum SchemaRefusal'
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
    COVERED = 'COVERED'
    PORTED = 'PORTED'
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


def is_recorded(row: Row) -> bool:
    """A fate that a person recorded, which `build` keeps: COVERED, PORTED,
    or DELETED with its code."""
    match row.fate:
        case Fate.COVERED | Fate.PORTED:
            return True
        case Fate.DELETED:
            return CODE_MARK in row.detail
        case Fate.KEPT | Fate.CHANGED:
            return False


def _module_of(file: str) -> list[str]:
    match file.split('/'):
        case ['src', *rest]:
            parts = rest
            parts[-1] = parts[-1].removesuffix('.rs')
            return parts[:-1] if parts[-1] in ('mod', 'lib', 'main') else parts
        case _:
            return [file]


def _literal_end(text: str, start: int) -> int | None:
    """The index after the string or char literal, or the comment, that
    starts at `start` of `text`, or None when no literal starts there. A
    string, a raw string (`r#"..."#`) and a block comment may span lines; the
    end of the text ends an open one."""
    if text.startswith('//', start):
        end = text.find('\n', start)
        return len(text) if end == -1 else end
    if text.startswith('/*', start):
        end = text.find('*/', start + 2)
        return len(text) if end == -1 else end + 2
    previous = text[start - 1] if start > 0 else ' '
    raw = _RAW_STRING_OPEN.match(text, start)
    if raw is not None and _IDENTIFIER_CHAR.match(previous) is None:
        closing = '"' + raw.group(1)
        end = text.find(closing, raw.end())
        return len(text) if end == -1 else end + len(closing)
    match text[start]:
        case '"':
            index = start + 1
            while index < len(text):
                match text[index]:
                    case '\\':
                        index += 2
                    case '"':
                        return index + 1
                    case _:
                        index += 1
            return len(text)
        case "'":
            char = _CHAR.match(text, start)
            return None if char is None else char.end()
        case _:
            return None


def _code_lines(text: str) -> list[str]:
    """The lines of `text` with the contents of string and char literals and
    of comments blanked, so that a brace counts only in code. The text keeps
    its lines: a literal that spans lines leaves its newlines."""
    kept: list[str] = []
    index = 0
    while index < len(text):
        end = _literal_end(text, index)
        match end:
            case None:
                kept.append(text[index])
                index += 1
            case int():
                kept.append(''.join('\n' if char == '\n' else ' ' for char in text[index:end]))
                index = end
    return ''.join(kept).split('\n')


def _inline_tests(text: str) -> list[tuple[list[str], str]]:
    """Each test function of a Rust file: its inline modules and its name."""
    found: list[tuple[list[str], str]] = []
    stack: list[tuple[str, int]] = []
    depth = 0
    after_attribute = False
    for code in _code_lines(text):
        opened = _MOD.match(code)
        if _ATTR.match(code):
            after_attribute = True
        function = _FN.match(code)
        if function is not None and after_attribute:
            found.append(([name for name, _depth in stack], function.group(1)))
            after_attribute = False
        if opened is not None:
            stack.append((opened.group(1), depth))
        depth += code.count('{') - code.count('}')
        while stack and depth <= stack[-1][1]:
            stack.pop()
    return found


def _tests_of_file(file: str, text: str) -> list[TestFn]:
    base = _module_of(file)
    return [
        TestFn(file=file, module='::'.join(base + modules), name=name)
        for modules, name in _inline_tests(text)
    ]


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


def _is_clean_slate(test: TestFn) -> bool:
    return bool(_CLEAN_SLATE_FILE.search(test.file) or _CLEAN_SLATE_NAME.search(test.name))


def build_rows(recorded: dict[tuple[str, str], Row]) -> list[Row] | GitFailed:
    """The list from the frozen crate and the tip. A row of `recorded` (the
    COVERED, PORTED and DELETED rows of the current list) is kept as it is."""
    frozen = _frozen_tests()
    if isinstance(frozen, GitFailed):
        return frozen
    index = _tip_index()
    rows: list[Row] = []
    for test in frozen:
        kept = recorded.get((test.file, test.path))
        twin = _twin(test, index)
        renamed = RENAMED.get(test.name)
        match (kept, twin, renamed):
            case (Row() as row, _, _):
                rows.append(row)
            case (None, str() as found, _):
                rows.append(Row(test.file, test.path, Fate.KEPT, found))
            case (None, None, str() as other):
                rows.append(Row(test.file, test.path, Fate.CHANGED, f'horsies-core {other}'))
            case (None, None, None) if _is_clean_slate(test):
                rows.append(Row(test.file, test.path, Fate.DELETED, CLEAN_SLATE))
            case (None, None, None):
                rows.append(Row(test.file, test.path, Fate.DELETED, UNACCOUNTED))
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


def _python_test_names(text: str) -> set[str]:
    """The pytest names of a Python file: `test` and `Class::test`."""
    names: set[str] = set()
    for node in ast.parse(text).body:
        match node:
            case ast.FunctionDef(name=name) | ast.AsyncFunctionDef(name=name):
                names.add(name)
            case ast.ClassDef(name=class_name, body=body):
                for member in body:
                    match member:
                        case ast.FunctionDef(name=name) | ast.AsyncFunctionDef(name=name):
                            names.add(f'{class_name}::{name}')
                        case _:
                            pass
            case _:
                pass
    return names


def _rust_test_names(text: str) -> set[str]:
    """The test names of a Rust file: each test function, alone and with
    each run of the inline modules above it."""
    names: set[str] = set()
    for modules, name in _inline_tests(text):
        for start in range(len(modules) + 1):
            names.add('::'.join([*modules[start:], name]))
    return names


class _Sources:
    """The tip files that the references read, each read once."""

    def __init__(self) -> None:
        self._texts: dict[str, str | None] = {}
        self._tests: dict[str, set[str]] = {}

    def text(self, path: str) -> str | None:
        if path not in self._texts:
            file = REPO_ROOT / path
            self._texts[path] = file.read_text(encoding='utf-8') if file.is_file() else None
        return self._texts[path]

    def tests(self, path: str) -> set[str] | None:
        text = self.text(path)
        if text is None:
            return None
        if path not in self._tests:
            match path.rsplit('.', 1)[-1]:
                case 'py':
                    self._tests[path] = _python_test_names(text)
                case 'rs':
                    self._tests[path] = _rust_test_names(text)
                case _:
                    self._tests[path] = set()
        return self._tests[path]


def _test_reference_problem(reference: str, sources: _Sources) -> str | None:
    """Why `reference` (`<path>::<name>`) names no test of the tip, or None."""
    path, separator, name = reference.partition('::')
    if not separator or not name:
        return f'{reference!r} is not <path>::<test>'
    tests = sources.tests(path)
    if tests is None:
        return f'{path} is not a file of the tip'
    if name not in tests:
        return f'{path} holds no test {name}'
    return None


def _code_reference_problem(reference: str, sources: _Sources) -> str | None:
    """Why `reference` (`<path>` or `<path>::<text>`) shows no code of the
    tip, or None."""
    path, separator, needle = reference.partition('::')
    target = REPO_ROOT / path
    if not separator:
        return None if target.exists() else f'{path} is not in the tip'
    text = sources.text(path)
    if text is None:
        return f'{path} is not a file of the tip'
    if needle not in text:
        return f'{path} does not hold {needle!r}'
    return None


def _detail_problems(row: Row, known: set[str], sources: _Sources) -> list[str]:
    """The problems of the detail of `row`."""
    match row.fate:
        case _ if UNACCOUNTED in row.detail:
            return ['no fate: give it COVERED, PORTED or DELETED']
        case Fate.KEPT | Fate.CHANGED:
            match row.detail in known:
                case True:
                    return []
                case False:
                    return [f'names {row.detail!r}, which the tip does not hold']
        case Fate.COVERED | Fate.PORTED:
            references, _mark, _note = row.detail.partition(NOTE_MARK)
            return [
                problem
                for reference in references.split(REFERENCE_JOIN)
                for problem in [_test_reference_problem(reference.strip(), sources)]
                if problem is not None
            ]
        case Fate.DELETED:
            reason, mark, cites = row.detail.partition(CODE_MARK)
            if not mark or not reason.strip() or not cites.strip():
                return ['a DELETED detail is <reason> -- code: <code reference>']
            return [
                problem
                for cite in cites.split(REFERENCE_JOIN)
                for problem in [_code_reference_problem(cite.strip(), sources)]
                if problem is not None
            ]


def check_rows(rows: list[Row]) -> list[str]:
    """One line per row whose fate is not accounted for, or whose detail
    names a test or code that the tip does not hold."""
    known: set[str] = set()
    for crate, folder in TIP_CRATES:
        known.update(f'{crate} {test.path}' for test in tests_of_crate(REPO_ROOT / folder))
    sources = _Sources()
    return [
        f'{row.file} {row.test}: {row.fate.value}: {problem}'
        for row in rows
        for problem in _detail_problems(row, known, sources)
    ]


def main(arguments: list[str]) -> int:
    match arguments:
        case ['build']:
            current, problems = read_rows()
            if problems:
                for problem in problems:
                    print(problem, file=sys.stderr)
                return 1
            recorded = {(row.file, row.test): row for row in current if is_recorded(row)}
            built = build_rows(recorded)
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
            counts = {fate: sum(1 for row in rows if row.fate is fate) for fate in Fate}
            print(f'{len(rows)} rows: ' + ', '.join(
                f'{fate.value} {count}' for fate, count in counts.items()
            ))
            for problem in problems:
                print(problem, file=sys.stderr)
            return 1 if problems else 0
        case _:
            print('usage: frozen_fates.py build|check', file=sys.stderr)
            return 2


if __name__ == '__main__':
    os.chdir(REPO_ROOT)
    sys.exit(main(sys.argv[1:]))
