"""Type pin: the ``error_codes`` parameter of ``@app.task()`` that a type
checker sees.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it.
``error_codes`` takes an ``Enum`` class or a tuple of strings with every
execution class, and the task function exposes the declared codes as
``task_error_codes: tuple[str, ...]``. Each wrong use below has an ignore
comment, and the config makes an unneeded ignore comment an error. So an
overload set that accepts one of these uses fails the check.
"""

from __future__ import annotations

from enum import Enum
from typing import assert_type

from horsies import Execution, Horsies, TaskError, TaskResult


class ChargeCode(Enum):
    CARD_DECLINED = 'CARD_DECLINED'
    RATE_LIMITED = 'RATE_LIMITED'


def register(app: Horsies, chosen: Execution) -> None:
    @app.task('pin_codes_enum', error_codes=ChargeCode)
    def charge(*, amount: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=amount)

    @app.task('pin_codes_tuple', error_codes=('BOUNCED', 'RATE_LIMITED'))
    async def send_mail(*, to: str) -> TaskResult[str, TaskError]:
        return TaskResult(ok=to)

    @app.task(
        'pin_codes_shared_async',
        execution=Execution.SHARED_ASYNC,
        timeout_ms=5_000,
        error_codes=ChargeCode,
    )
    async def shared_async(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task(
        'pin_codes_shared_thread',
        execution=Execution.SHARED_THREAD,
        timeout_ms=5_000,
        error_codes=('RATE_LIMITED',),
    )
    def shared_thread(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_codes_chosen', execution=chosen, timeout_ms=5_000, error_codes=())
    def chosen_at_run_time(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    assert_type(charge.task_error_codes, tuple[str, ...])
    assert_type(send_mail.task_error_codes, tuple[str, ...])
    del shared_async, shared_thread, chosen_at_run_time

    @app.task('pin_codes_list', error_codes=['RATE_LIMITED'])  # pyright: ignore[reportArgumentType]
    def list_of_codes(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_codes_text', error_codes='RATE_LIMITED')  # pyright: ignore[reportArgumentType]
    def one_text(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_codes_member', error_codes=ChargeCode.RATE_LIMITED)  # pyright: ignore[reportArgumentType]
    def enum_member(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    @app.task('pin_codes_ints', error_codes=(1, 2))  # pyright: ignore[reportArgumentType]
    def int_codes(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    del list_of_codes, one_text, enum_member, int_codes
