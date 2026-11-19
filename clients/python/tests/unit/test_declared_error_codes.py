"""The error codes that a task declares: `@app.task(..., error_codes=...)`.

The decorator takes an `Enum` class (its member values) or a tuple of code
texts. It refuses a value of another form (HRS-102), a code that fails the
code-text rule (HRS-112), a built-in or reserved name (HRS-212) and a code
given twice (HRS-113), and it keeps the declared codes on the task
function. `horsies check` lists them.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.declared_error_codes import (
    CodeSetFormRefused,
    CodesRefused,
    CodeTextRefused,
    RepeatedCodeRefused,
    ReservedNameRefused,
    parse_declared_error_codes,
)
from horsies.core.errors import (
    ErrorCode,
    MultipleValidationErrors,
    TaskDefinitionError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok

pytestmark = [pytest.mark.unit]


class ChargeCode(Enum):
    CARD_DECLINED = 'CARD_DECLINED'
    RATE_LIMITED = 'RATE_LIMITED'
    LIMITED = 'RATE_LIMITED'


class EmptyCodes(Enum):
    pass


class LowerCaseCodes(Enum):
    DECLINED = 'declined'


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://user:pass@127.0.0.1:1/none'),
            ),
        ),
    )


def _declare(app: Horsies, error_codes: Any) -> None:
    @app.task('charge', error_codes=error_codes)
    def charge(*, amount: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=amount)

    del charge


def _refusals(value: object) -> tuple[object, ...]:
    match parse_declared_error_codes(value):
        case Err(CodesRefused(refusals=refusals)):
            return refusals
        case other:
            raise AssertionError(f'expected CodesRefused, got {other!r}')


class TestParse:
    def test_a_tuple_keeps_the_declaration_order(self) -> None:
        assert parse_declared_error_codes(('RATE_LIMITED', 'CARD_DECLINED')) == Ok(
            ('RATE_LIMITED', 'CARD_DECLINED'),
        )

    def test_an_enum_class_declares_its_member_values_with_each_alias_once(
        self,
    ) -> None:
        assert parse_declared_error_codes(ChargeCode) == Ok(
            ('CARD_DECLINED', 'RATE_LIMITED'),
        )

    def test_no_entry_declares_no_code(self) -> None:
        assert parse_declared_error_codes(()) == Ok(())
        assert parse_declared_error_codes(EmptyCodes) == Ok(())

    @pytest.mark.parametrize(
        'value',
        [['RATE_LIMITED'], 'RATE_LIMITED', {'RATE_LIMITED'}, None, ChargeCode.RATE_LIMITED],
    )
    def test_a_value_that_is_neither_an_enum_class_nor_a_tuple_is_refused(
        self,
        value: object,
    ) -> None:
        assert parse_declared_error_codes(value) == Err(
            CodeSetFormRefused(given=type(value).__name__),
        )

    @pytest.mark.parametrize(
        'entry',
        ['', 'rate_limited', '_RATE', 'RateLimitError', 'RATE-LIMITED', 7, None],
    )
    def test_an_entry_outside_the_code_text_rule_is_refused(
        self,
        entry: object,
    ) -> None:
        refusals = _refusals((entry,))
        assert len(refusals) == 1
        assert isinstance(refusals[0], CodeTextRefused)

    def test_an_enum_value_outside_the_code_text_rule_is_refused(self) -> None:
        refusals = _refusals(LowerCaseCodes)
        assert len(refusals) == 1
        assert isinstance(refusals[0], CodeTextRefused)
        assert "'declined'" in refusals[0].detail

    def test_a_built_in_name_is_refused(self) -> None:
        refusals = _refusals(('BROKER_ERROR',))
        assert len(refusals) == 1
        refusal = refusals[0]
        assert isinstance(refusal, ReservedNameRefused)
        assert refusal.code == 'BROKER_ERROR'
        assert 'OperationalErrorCode.BROKER_ERROR' in refusal.detail

    def test_a_reserved_name_is_refused(self) -> None:
        refusals = _refusals(('WORKFLOW_STOPPED',))
        assert len(refusals) == 1
        refusal = refusals[0]
        assert isinstance(refusal, ReservedNameRefused)
        assert refusal.code == 'WORKFLOW_STOPPED'

    def test_a_repeated_code_is_refused_once_per_repeat(self) -> None:
        assert _refusals(('A', 'B', 'A', 'A')) == (
            RepeatedCodeRefused(code='A'),
            RepeatedCodeRefused(code='A'),
        )

    def test_every_refused_entry_is_named_in_input_order(self) -> None:
        refusals = _refusals(('bad', 'BROKER_ERROR', 'bad', 'GOOD', 'GOOD'))
        assert [type(refusal) for refusal in refusals] == [
            CodeTextRefused,
            ReservedNameRefused,
            CodeTextRefused,
            RepeatedCodeRefused,
        ]


class TestDecorator:
    def test_a_task_declares_no_code_by_default(self) -> None:
        app = _app()

        @app.task('resize')
        def resize(*, width: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=width)

        assert resize.task_error_codes == ()

    def test_a_tuple_is_kept_on_the_task_function(self) -> None:
        app = _app()

        @app.task('charge', error_codes=('RATE_LIMITED', 'CARD_DECLINED'))
        def charge(*, amount: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=amount)

        assert charge.task_error_codes == ('RATE_LIMITED', 'CARD_DECLINED')
        assert app.tasks['charge'].task_error_codes == ('RATE_LIMITED', 'CARD_DECLINED')

    def test_an_enum_class_is_kept_as_its_values(self) -> None:
        app = _app()

        @app.task('charge', error_codes=ChargeCode)
        async def charge(*, amount: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=amount)

        assert charge.task_error_codes == ('CARD_DECLINED', 'RATE_LIMITED')

    def test_a_value_of_another_form_is_hrs_102(self) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), ['RATE_LIMITED'])
        assert raised.value.code is ErrorCode.TASK_INVALID_OPTIONS
        assert any('list' in note for note in raised.value.notes)

    def test_a_code_outside_the_code_text_rule_is_hrs_112(self) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), ('rate_limited',))
        assert raised.value.code is ErrorCode.TASK_INVALID_ERROR_CODE
        assert raised.value.code.value == 'HRS-112'
        assert "'rate_limited'" in raised.value.message
        assert "task 'charge'" in raised.value.notes

    @pytest.mark.parametrize('code', ['TASK_TIMEOUT', 'WORKFLOW_STOPPED'])
    def test_a_built_in_or_reserved_name_is_hrs_212(self, code: str) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), (code,))
        assert raised.value.code is ErrorCode.CHECK_RESERVED_CODE_COLLISION
        assert code in raised.value.message

    def test_a_repeated_code_is_hrs_113(self) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), ('RATE_LIMITED', 'RATE_LIMITED'))
        assert raised.value.code is ErrorCode.TASK_DUPLICATE_ERROR_CODE
        assert raised.value.code.value == 'HRS-113'

    def test_several_refused_codes_come_in_one_report(self) -> None:
        with pytest.raises(MultipleValidationErrors) as raised:
            _declare(_app(), ('bad', 'BROKER_ERROR', 'OK', 'OK'))
        codes = [error.code for error in raised.value.report.errors]
        assert codes == [
            ErrorCode.TASK_INVALID_ERROR_CODE,
            ErrorCode.CHECK_RESERVED_CODE_COLLISION,
            ErrorCode.TASK_DUPLICATE_ERROR_CODE,
        ]

    def test_a_refused_set_registers_nothing(self) -> None:
        app = _app()
        with pytest.raises(TaskDefinitionError):
            _declare(app, ('RATE_LIMITED', 'RATE_LIMITED'))
        assert 'charge' not in app.tasks

    def test_the_app_lists_the_declaring_tasks_in_name_order(self) -> None:
        app = _app()

        @app.task('send_mail', error_codes=('BOUNCED',))
        def send_mail(*, to: str) -> TaskResult[str, TaskError]:
            return TaskResult(ok=to)

        @app.task('charge', error_codes=ChargeCode)
        def charge(*, amount: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=amount)

        @app.task('resize')
        def resize(*, width: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=width)

        del send_mail, charge, resize
        assert list(app.declared_error_codes().items()) == [
            ('charge', ('CARD_DECLINED', 'RATE_LIMITED')),
            ('send_mail', ('BOUNCED',)),
        ]
