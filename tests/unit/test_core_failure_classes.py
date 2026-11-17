"""The exception classes of a failed core operation in ``horsies._core``.

A failure record of the core carries one leaf class in ``exception``. Each
leaf class is built from Python with its fields, keeps them as read-only
attributes, and pickles.
"""

from __future__ import annotations

import pickle

import pytest

from horsies._core import (
    CoreConnectionError,
    CoreContractViolation,
    CoreDatabaseError,
    CoreFailure,
    CorePreconditionFailed,
    CoreServerError,
)


class TestCoreServerError:
    def test_keeps_its_subject_sqlstate_message_and_retry_rule(self) -> None:
        error = CoreServerError('release_claims', '55P03', 'could not obtain lock', True)

        assert error.subject == 'release_claims'
        assert error.sqlstate == '55P03'
        assert error.message == 'could not obtain lock'
        assert error.retryable is True
        assert str(error) == 'release_claims: could not obtain lock'

    def test_is_a_database_failure_and_an_exception(self) -> None:
        error = CoreServerError('release_claims', '23505', 'duplicate key', False)

        assert isinstance(error, CoreDatabaseError)
        assert isinstance(error, CoreFailure)
        assert isinstance(error, Exception)

    @pytest.mark.parametrize('sqlstate', ['', '5503', '55P031', '55 03'])
    def test_refuses_a_sqlstate_that_is_not_five_letters_or_digits(self, sqlstate: str) -> None:
        with pytest.raises(ValueError, match='sqlstate must be 5 ASCII letters or digits'):
            _ = CoreServerError('release_claims', sqlstate, 'refused', False)


class TestCoreConnectionError:
    def test_keeps_its_fields_and_is_a_database_failure(self) -> None:
        error = CoreConnectionError('claim_pass', 'connection lost', True)

        assert (error.subject, error.message, error.retryable) == (
            'claim_pass',
            'connection lost',
            True,
        )
        assert isinstance(error, CoreDatabaseError)
        assert not isinstance(error, CoreServerError)


class TestCoreContractViolation:
    def test_keeps_its_fields_and_is_not_a_database_failure(self) -> None:
        error = CoreContractViolation('release_claims', 'ordinality 3 has no answer')

        assert (error.subject, error.message) == ('release_claims', 'ordinality 3 has no answer')
        assert isinstance(error, CoreFailure)
        assert not isinstance(error, CoreDatabaseError)


class TestCorePreconditionFailed:
    def test_keeps_its_fields_and_is_not_a_database_failure(self) -> None:
        error = CorePreconditionFailed('rerun_task', 'database of another family', False)

        assert (error.subject, error.message, error.retryable) == (
            'rerun_task',
            'database of another family',
            False,
        )
        assert isinstance(error, CoreFailure)
        assert not isinstance(error, CoreDatabaseError)


class TestBaseClasses:
    @pytest.mark.parametrize('base', [CoreFailure, CoreDatabaseError])
    def test_a_base_class_has_no_constructor(self, base: type[Exception]) -> None:
        with pytest.raises(TypeError):
            _ = base('subject', 'message')


class TestPickle:
    @pytest.mark.parametrize(
        'error',
        [
            CoreServerError('task_info', '57014', 'statement timeout', True),
            CoreConnectionError('task_info', 'connection lost', True),
            CoreContractViolation('task_info', 'stored status does not parse'),
            CorePreconditionFailed('task_info', 'pool closed', False),
        ],
    )
    def test_a_round_trip_keeps_the_class_and_the_fields(self, error: CoreFailure) -> None:
        restored = pickle.loads(pickle.dumps(error))

        assert type(restored) is type(error)
        assert restored.args == error.args
        assert (restored.subject, restored.message) == (error.subject, error.message)
