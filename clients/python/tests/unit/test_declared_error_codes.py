"""The error codes that a task declares: `@app.task(..., error_codes=...)`.

The decorator takes an `Enum` class (its member values) or a tuple of code
texts. It refuses a value of another form (HRS-102), a code that fails the
code-text rule (HRS-112), a built-in or reserved name (HRS-212) and a code
given twice (HRS-113), and it keeps the declared codes on the task
function, with the codes of the task's and the app's exception mappers.
`horsies check` lists them.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Any, cast

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.declared_error_codes import (
    CodeSetFormRefused,
    CodesRefused,
    CodeTextRefused,
    RepeatedCodeRefused,
    ReservedNameRefused,
    RetryEntriesRefused,
    RetryEntryNotAMember,
    RetryMemberNotDeclared,
    check_retry_codes,
    parse_declared_error_codes,
    with_mapper_codes,
)
from horsies.core.errors import (
    HorsiesErrorCode,
    MultipleValidationErrors,
    TaskDefinitionError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import OperationalCode, OutcomeCode, TaskError
from horsies.core.types.result import Err, Ok, Result

pytestmark = [pytest.mark.unit]


class ChargeCode(Enum):
    CARD_DECLINED = 'CARD_DECLINED'
    RATE_LIMITED = 'RATE_LIMITED'
    LIMITED = 'RATE_LIMITED'


class EmptyCodes(Enum):
    pass


class LowerCaseCodes(Enum):
    DECLINED = 'declined'


class OtherCode(Enum):
    RATE_LIMITED = 'RATE_LIMITED'


def _app(
    exception_mapper: Mapping[type[BaseException], str] | None = None,
) -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://user:pass@127.0.0.1:1/none'),
            ),
            exception_mapper=dict(exception_mapper or {}),
        ),
    )


def _declare(app: Horsies, error_codes: Any) -> None:
    @app.task('charge', error_codes=error_codes)
    def charge(*, amount: int) -> Result[int, TaskError]:
        return Ok(amount)

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
        refusals = _refusals(('DATABASE_ERROR',))
        assert len(refusals) == 1
        refusal = refusals[0]
        assert isinstance(refusal, ReservedNameRefused)
        assert refusal.code == 'DATABASE_ERROR'
        assert 'OperationalCode.DATABASE_ERROR' in refusal.detail

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
        refusals = _refusals(('bad', 'DATABASE_ERROR', 'bad', 'GOOD', 'GOOD'))
        assert [type(refusal) for refusal in refusals] == [
            CodeTextRefused,
            ReservedNameRefused,
            CodeTextRefused,
            RepeatedCodeRefused,
        ]


class TestWithMapperCodes:
    def test_the_declared_codes_come_first_then_each_mapper_in_entry_order(
        self,
    ) -> None:
        assert with_mapper_codes(
            ('CARD_DECLINED',),
            {TimeoutError: 'GATEWAY_SLOW', KeyError: 'MISSING_ACCOUNT'},
            {ValueError: 'BAD_AMOUNT'},
        ) == ('CARD_DECLINED', 'GATEWAY_SLOW', 'MISSING_ACCOUNT', 'BAD_AMOUNT')

    def test_a_code_comes_once_at_its_first_place(self) -> None:
        assert with_mapper_codes(
            ('GATEWAY_SLOW',),
            {TimeoutError: 'GATEWAY_SLOW', KeyError: 'BAD_AMOUNT'},
            {ValueError: 'BAD_AMOUNT', OSError: 'GATEWAY_SLOW'},
        ) == ('GATEWAY_SLOW', 'BAD_AMOUNT')

    def test_a_task_with_no_mapper_takes_the_app_mapper_codes(self) -> None:
        assert with_mapper_codes((), None, {ValueError: 'BAD_AMOUNT'}) == (
            'BAD_AMOUNT',
        )

    def test_no_declaration_and_no_mapper_entry_is_no_code(self) -> None:
        assert with_mapper_codes((), None, {}) == ()
        assert with_mapper_codes((), {}, {}) == ()

    @pytest.mark.parametrize(
        'value',
        ['TASK_TIMEOUT', 'UNHANDLED_EXCEPTION', 'WORKFLOW_STOPPED', 'timeout', '', 7],
    )
    def test_a_mapper_value_that_is_not_a_user_code_is_not_added(
        self,
        value: object,
    ) -> None:
        mapper = cast('Mapping[type[BaseException], str]', {TimeoutError: value})
        assert with_mapper_codes(('CARD_DECLINED',), mapper, mapper) == (
            'CARD_DECLINED',
        )

    def test_the_result_is_a_declared_set_that_the_parser_accepts(self) -> None:
        codes = with_mapper_codes(
            ('CARD_DECLINED',),
            {TimeoutError: 'GATEWAY_SLOW', KeyError: 'TASK_TIMEOUT'},
            {ValueError: 'CARD_DECLINED', OSError: 'WORKFLOW_STOPPED'},
        )
        assert parse_declared_error_codes(codes) == Ok(codes)



class TestCheckRetryCodes:
    def test_built_in_members_and_members_of_the_declared_enum_are_accepted(
        self,
    ) -> None:
        entries = [
            OperationalCode.WORKER_CRASHED,
            OutcomeCode.TASK_TIMEOUT,
            ChargeCode.RATE_LIMITED,
        ]
        assert check_retry_codes(entries, ChargeCode) == Ok(None)
        assert check_retry_codes([OutcomeCode.TASK_TIMEOUT], None) == Ok(None)

    def test_a_plain_text_is_not_a_member(self) -> None:
        assert check_retry_codes(['WORKER_CRASHED'], ChargeCode) == Err(
            RetryEntriesRefused(
                refusals=(
                    RetryEntryNotAMember(index=0, entry="'WORKER_CRASHED'", given='str'),
                ),
            ),
        )

    def test_a_member_of_another_enum_is_not_declared(self) -> None:
        assert check_retry_codes([OtherCode.RATE_LIMITED], ChargeCode) == Err(
            RetryEntriesRefused(
                refusals=(
                    RetryMemberNotDeclared(index=0, member='OtherCode.RATE_LIMITED'),
                ),
            ),
        )

    def test_with_no_declared_enum_a_custom_member_is_not_declared(self) -> None:
        assert check_retry_codes([ChargeCode.CARD_DECLINED], None) == Err(
            RetryEntriesRefused(
                refusals=(
                    RetryMemberNotDeclared(index=0, member='ChargeCode.CARD_DECLINED'),
                ),
            ),
        )

    def test_every_refused_entry_is_named_in_list_order(self) -> None:
        entries: list[object] = [
            7,
            OperationalCode.DATABASE_ERROR,
            OtherCode.RATE_LIMITED,
        ]
        assert check_retry_codes(entries, ChargeCode) == Err(
            RetryEntriesRefused(
                refusals=(
                    RetryEntryNotAMember(index=0, entry='7', given='int'),
                    RetryMemberNotDeclared(index=2, member='OtherCode.RATE_LIMITED'),
                ),
            ),
        )

class TestDecorator:
    def test_a_task_declares_no_code_by_default(self) -> None:
        app = _app()

        @app.task('resize')
        def resize(*, width: int) -> Result[int, TaskError]:
            return Ok(width)

        assert resize.task_error_codes == ()

    def test_a_tuple_is_kept_on_the_task_function(self) -> None:
        app = _app()

        @app.task('charge', error_codes=('RATE_LIMITED', 'CARD_DECLINED'))
        def charge(*, amount: int) -> Result[int, TaskError]:
            return Ok(amount)

        assert charge.task_error_codes == ('RATE_LIMITED', 'CARD_DECLINED')
        assert app.tasks['charge'].task_error_codes == ('RATE_LIMITED', 'CARD_DECLINED')

    def test_an_enum_class_is_kept_as_its_values(self) -> None:
        app = _app()

        @app.task('charge', error_codes=ChargeCode)
        async def charge(*, amount: int) -> Result[int, TaskError]:
            return Ok(amount)

        assert charge.task_error_codes == ('CARD_DECLINED', 'RATE_LIMITED')

    def test_a_value_of_another_form_is_hrs_102(self) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), ['RATE_LIMITED'])
        assert raised.value.code is HorsiesErrorCode.TASK_INVALID_OPTIONS
        assert any('list' in note for note in raised.value.notes)

    def test_a_code_outside_the_code_text_rule_is_hrs_112(self) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), ('rate_limited',))
        assert raised.value.code is HorsiesErrorCode.TASK_INVALID_ERROR_CODE
        assert raised.value.code.value == 'HRS-112'
        assert "'rate_limited'" in raised.value.message
        assert "task 'charge'" in raised.value.notes

    @pytest.mark.parametrize('code', ['TASK_TIMEOUT', 'WORKFLOW_STOPPED'])
    def test_a_built_in_or_reserved_name_is_hrs_212(self, code: str) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), (code,))
        assert raised.value.code is HorsiesErrorCode.CHECK_RESERVED_CODE_COLLISION
        assert code in raised.value.message

    def test_a_repeated_code_is_hrs_113(self) -> None:
        with pytest.raises(TaskDefinitionError) as raised:
            _declare(_app(), ('RATE_LIMITED', 'RATE_LIMITED'))
        assert raised.value.code is HorsiesErrorCode.TASK_DUPLICATE_ERROR_CODE
        assert raised.value.code.value == 'HRS-113'

    def test_several_refused_codes_come_in_one_report(self) -> None:
        with pytest.raises(MultipleValidationErrors) as raised:
            _declare(_app(), ('bad', 'DATABASE_ERROR', 'OK', 'OK'))
        codes = [error.code for error in raised.value.report.errors]
        assert codes == [
            HorsiesErrorCode.TASK_INVALID_ERROR_CODE,
            HorsiesErrorCode.CHECK_RESERVED_CODE_COLLISION,
            HorsiesErrorCode.TASK_DUPLICATE_ERROR_CODE,
        ]

    def test_a_refused_set_registers_nothing(self) -> None:
        app = _app()
        with pytest.raises(TaskDefinitionError):
            _declare(app, ('RATE_LIMITED', 'RATE_LIMITED'))
        assert 'charge' not in app.tasks

    def test_the_app_lists_the_declaring_tasks_in_name_order(self) -> None:
        app = _app()

        @app.task('send_mail', error_codes=('BOUNCED',))
        def send_mail(*, to: str) -> Result[str, TaskError]:
            return Ok(to)

        @app.task('charge', error_codes=ChargeCode)
        def charge(*, amount: int) -> Result[int, TaskError]:
            return Ok(amount)

        @app.task('resize')
        def resize(*, width: int) -> Result[int, TaskError]:
            return Ok(width)

        del send_mail, charge, resize
        assert list(app.declared_error_codes().items()) == [
            ('charge', ('CARD_DECLINED', 'RATE_LIMITED')),
            ('send_mail', ('BOUNCED',)),
        ]

    def test_the_mapper_codes_of_the_task_and_the_app_are_declared(self) -> None:
        app = _app({ValueError: 'BAD_AMOUNT'})

        @app.task(
            'charge',
            error_codes=('CARD_DECLINED',),
            exception_mapper={TimeoutError: 'GATEWAY_SLOW'},
        )
        def charge(*, amount: int) -> Result[int, TaskError]:
            return Ok(amount)

        assert charge.task_error_codes == (
            'CARD_DECLINED',
            'GATEWAY_SLOW',
            'BAD_AMOUNT',
        )

    def test_a_mapper_code_that_the_task_also_declares_is_not_a_repeat(
        self,
    ) -> None:
        app = _app({ValueError: 'CARD_DECLINED'})

        @app.task(
            'charge',
            error_codes=('CARD_DECLINED',),
            exception_mapper={TimeoutError: 'CARD_DECLINED'},
        )
        def charge(*, amount: int) -> Result[int, TaskError]:
            return Ok(amount)

        assert charge.task_error_codes == ('CARD_DECLINED',)

    def test_the_app_lists_a_task_whose_only_codes_come_from_a_mapper(
        self,
    ) -> None:
        app = _app({ValueError: 'BAD_AMOUNT'})

        @app.task('resize')
        def resize(*, width: int) -> Result[int, TaskError]:
            return Ok(width)

        del resize
        assert app.declared_error_codes() == {'resize': ('BAD_AMOUNT',)}
