"""``horsies.core.types.core_failure``: the call rule and the retry rule of
a core boundary error, and the absence of the interim class
``CoreOperationFailed``.

A failure record of the core carries the core's class of the cause
(``horsies._core.CoreServerError`` and the other ``CoreFailure`` leaf
classes). The interim ``CoreOperationFailed`` and its subclass
``CoreLockNotAvailable`` are removed; nothing of the package defines or
names them again.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

import horsies
import horsies._core as core
from horsies.core.types.core_failure import call_refusal_or_raise, core_error_retryable

pytestmark = [pytest.mark.unit]

_REMOVED_CLASSES = ('CoreOperationFailed', 'CoreLockNotAvailable')


class TestTheInterimClassIsRemoved:
    @pytest.mark.parametrize('name', _REMOVED_CLASSES)
    def test_its_module_does_not_define_it(self, name: str) -> None:
        module = importlib.import_module('horsies.core.types.core_failure')

        assert not hasattr(module, name)

    @pytest.mark.parametrize('name', _REMOVED_CLASSES)
    def test_the_extension_module_does_not_define_it(self, name: str) -> None:
        assert not hasattr(core, name)

    def test_no_source_file_of_the_package_names_it(self) -> None:
        package_dir = Path(horsies.__file__).parent
        naming = [
            str(path.relative_to(package_dir))
            for path in sorted(package_dir.rglob('*.py*'))
            if path.suffix in ('.py', '.pyi')
            and any(name in path.read_text(encoding='utf-8') for name in _REMOVED_CLASSES)
        ]

        assert naming == []


class TestCoreErrorRetryable:
    @pytest.mark.parametrize(
        'error',
        [
            core.TooManyPendingCalls('CoreHandle.release_claims_async', 65_536),
            core.CoreCallTimedOut('CoreHandle.enqueue', 1800, 'outcome_unknown'),
        ],
    )
    def test_a_condition_that_clears_in_the_process_is_retryable(
        self, error: core.CoreError,
    ) -> None:
        assert core_error_retryable(error) is True

    @pytest.mark.parametrize(
        'error',
        [
            core.CoreStopped('core task', 'panic'),
            core.CoreStartFailed('the tokio runtime could not be built'),
            core.ForkedAfterCoreStart(100, 101, 'use spawn'),
        ],
    )
    def test_a_state_that_stays_for_the_process_is_not_retryable(
        self, error: core.CoreError,
    ) -> None:
        assert core_error_retryable(error) is False


class TestCallRefusalOrRaise:
    @pytest.mark.parametrize(
        'error',
        [
            core.TooManyPendingCalls('CoreHandle.task_info_async', 65_536),
            core.CoreCallTimedOut('CoreHandle.task_info', 1800, 'no_change'),
            core.CoreStartFailed('the tokio runtime could not be built'),
        ],
        ids=['too-many-pending-calls', 'call-timed-out', 'start-failed'],
    )
    def test_the_failure_of_one_call_is_returned(self, error: core.CoreError) -> None:
        assert call_refusal_or_raise(error) is error

    @pytest.mark.parametrize(
        'error',
        [
            core.ForkedAfterCoreStart(100, 101, 'use spawn'),
            core.CoreStopped('core task', 'panic'),
        ],
        ids=['forked', 'stopped'],
    )
    def test_a_state_of_the_process_is_raised_unchanged(self, error: core.CoreError) -> None:
        with pytest.raises(type(error)) as raised:
            _ = call_refusal_or_raise(error)

        assert raised.value is error

    def test_a_class_the_core_does_not_define_is_a_defect(self) -> None:
        unknown = core.CoreError('not a class of the core')

        with pytest.raises(AssertionError, match='CoreError: not a class of the core') as raised:
            _ = call_refusal_or_raise(unknown)

        assert raised.value.__cause__ is unknown
