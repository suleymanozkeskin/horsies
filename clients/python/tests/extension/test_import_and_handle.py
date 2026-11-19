"""Importing ``horsies._core`` and building a ``CoreHandle`` start nothing; a
refused manifest raises ``ManifestRefused``, which ``except Exception`` does
not catch."""

from __future__ import annotations

from pathlib import Path

from tests.extension.fresh_interpreter import (
    NoDatabase,
    integer_of,
    run_case,
    text_of,
    texts_of,
)

PROGRAM = 'import_and_handle'


def test_import_starts_no_runtime_and_no_thread(tmp_path: Path) -> None:
    report = run_case(PROGRAM, 'import_starts_nothing', tmp_path, NoDatabase()).report

    assert text_of(report, 'core_state') == 'not_started'
    assert integer_of(report, 'threads_after_import') == integer_of(report, 'threads_before_import')


def test_a_handle_build_starts_nothing(tmp_path: Path) -> None:
    report = run_case(PROGRAM, 'handle_starts_nothing', tmp_path, NoDatabase()).report

    assert text_of(report, 'core_state') == 'not_started'
    assert integer_of(report, 'threads_after_build') == integer_of(report, 'threads_before_build')
    assert integer_of(report, 'handle_manifest_version') == integer_of(
        report, 'sent_manifest_version',
    )


def test_a_refused_manifest_raises_manifest_refused_past_except_exception(
    tmp_path: Path,
) -> None:
    report = run_case(PROGRAM, 'refused_manifest', tmp_path, NoDatabase()).report

    assert text_of(report, 'caught_by') == 'ManifestRefused'
    refusal_classes = texts_of(report, 'refusal_classes')
    assert 'CoreDefect' in refusal_classes
    assert 'BaseException' in refusal_classes
    assert 'Exception' not in refusal_classes
    arguments = texts_of(report, 'arguments')
    assert len(arguments) == 2, arguments
    field_path, cause = arguments
    assert field_path == 'database.extra'
    # The cause names the class and the position of the error, never a value.
    assert cause.startswith('value does not fit the field at line '), cause
    assert text_of(report, 'core_state') == 'not_started'
