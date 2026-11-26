"""The execution class: its declaration rules, its stored form, its reads.

`@app.task(execution=...)` declares how a worker runs a task. The decorator
refuses a declaration that breaks a rule, with one error code per rule. The
stored task options carry the class and the resolved cancel grace, and every
insert path reads the row's `execution` column from them.
"""

from __future__ import annotations

import json
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies import Execution as PublicExecution
from horsies.core.app import Horsies
from horsies.core.codec.json_io import Json
from horsies.core.codec.task_options import (
    InvalidExecution,
    serialize_task_options,
    stored_execution,
)
from horsies.core.errors import ErrorCode, TaskDefinitionError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    DEFAULT_CANCEL_GRACE_MS,
    MAX_CANCEL_GRACE_MS,
    MIN_CANCEL_GRACE_MS,
    Execution,
    TaskError,
    TaskOptions,
)
from horsies.core.task_decorator import (
    CancelGraceOnSharedThread,
    CancelGraceOutOfRange,
    ExecutionRuleViolation,
    SharedAsyncOnDef,
    SharedThreadOnAsyncDef,
    SharedWithoutTimeout,
    TaskBodyKind,
    check_execution_rules,
    execution_rule_error,
)
from horsies.core.types.result import Err, Ok, Result

pytestmark = [pytest.mark.unit]


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr(
                    'postgresql://user:pass@127.0.0.1:1/none'
                ),
            ),
        ),
    )


def _stored(options_json: str | None) -> dict[str, Any]:
    assert options_json is not None
    loaded = json.loads(options_json)
    assert isinstance(loaded, dict)
    return cast('dict[str, Any]', loaded)


def _definition_error(declare: Any) -> TaskDefinitionError:
    """Run one declaration and return the definition error it raises."""
    with pytest.raises(TaskDefinitionError) as raised:
        declare(_app())
    return raised.value


class TestExport:
    def test_the_enum_is_exported_at_the_top_level(self) -> None:
        assert PublicExecution is Execution

    def test_the_members_are_the_three_classes(self) -> None:
        assert [member.value for member in Execution] == [
            'ISOLATED',
            'SHARED_ASYNC',
            'SHARED_THREAD',
        ]


class TestDeclarationsThatRegister:
    def test_the_default_class_is_isolated(self) -> None:
        app = _app()

        @app.task('plain_def')
        def plain_def(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        @app.task('plain_async')
        async def plain_async(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        for task in (plain_def, plain_async):
            stored = _stored(task.task_options_json)
            assert stored['execution'] == 'ISOLATED'
            assert stored['cancel_grace_ms'] is None

    def test_isolated_takes_both_kinds_without_a_timeout(self) -> None:
        app = _app()

        @app.task('iso_def', execution=Execution.ISOLATED)
        def iso_def(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        @app.task('iso_async', execution=Execution.ISOLATED)
        async def iso_async(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        assert {'iso_def', 'iso_async'} <= set(app.tasks.keys())
        assert _stored(iso_async.task_options_json)['timeout_ms'] is None
        del iso_def

    def test_shared_async_without_a_grace_stores_the_default(self) -> None:
        app = _app()

        @app.task('sa', execution=Execution.SHARED_ASYNC, timeout_ms=5_000)
        async def sa(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        stored = _stored(sa.task_options_json)
        assert stored['execution'] == 'SHARED_ASYNC'
        assert stored['cancel_grace_ms'] == DEFAULT_CANCEL_GRACE_MS

    @pytest.mark.parametrize('grace', [MIN_CANCEL_GRACE_MS, MAX_CANCEL_GRACE_MS])
    def test_shared_async_stores_a_declared_grace_at_the_bounds(
        self,
        grace: int,
    ) -> None:
        app = _app()

        @app.task(
            'sa_grace',
            execution=Execution.SHARED_ASYNC,
            timeout_ms=5_000,
            cancel_grace_ms=grace,
        )
        async def sa_grace(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        assert _stored(sa_grace.task_options_json)['cancel_grace_ms'] == grace

    @pytest.mark.parametrize('grace', [MIN_CANCEL_GRACE_MS, MAX_CANCEL_GRACE_MS])
    def test_isolated_stores_a_declared_grace_at_the_bounds(self, grace: int) -> None:
        """An isolated grace is the hard stop after an operator cancel."""
        app = _app()

        @app.task('iso_grace', cancel_grace_ms=grace)
        def iso_grace(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        @app.task('iso_grace_async', execution=Execution.ISOLATED, cancel_grace_ms=grace)
        async def iso_grace_async(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        for task in (iso_grace, iso_grace_async):
            stored = _stored(task.task_options_json)
            assert stored['execution'] == 'ISOLATED'
            assert stored['cancel_grace_ms'] == grace

    def test_shared_thread_stores_no_grace(self) -> None:
        app = _app()

        @app.task('st', execution=Execution.SHARED_THREAD, timeout_ms=5_000)
        def st(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        stored = _stored(st.task_options_json)
        assert stored['execution'] == 'SHARED_THREAD'
        assert stored['cancel_grace_ms'] is None


class TestDeclarationsThatAreRefused:
    def test_shared_async_on_a_def_function(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', execution=Execution.SHARED_ASYNC, timeout_ms=5_000)  # pyright: ignore[reportArgumentType]
            def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        error = _definition_error(declare)
        assert error.code is ErrorCode.TASK_SHARED_ASYNC_REQUIRES_ASYNC_DEF
        assert 'async def' in error.message

    def test_shared_thread_on_an_async_def_function(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', execution=Execution.SHARED_THREAD, timeout_ms=5_000)  # pyright: ignore[reportArgumentType]
            async def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        error = _definition_error(declare)
        assert error.code is ErrorCode.TASK_SHARED_THREAD_REQUIRES_DEF

    def test_shared_async_without_a_timeout(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', execution=Execution.SHARED_ASYNC)  # pyright: ignore[reportArgumentType]
            async def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        error = _definition_error(declare)
        assert error.code is ErrorCode.TASK_SHARED_EXECUTION_REQUIRES_TIMEOUT
        assert 'SHARED_ASYNC' in error.message

    def test_shared_thread_without_a_timeout(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', execution=Execution.SHARED_THREAD)  # pyright: ignore[reportArgumentType]
            def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        error = _definition_error(declare)
        assert error.code is ErrorCode.TASK_SHARED_EXECUTION_REQUIRES_TIMEOUT
        assert 'SHARED_THREAD' in error.message

    @pytest.mark.parametrize(
        'grace',
        [
            pytest.param(MIN_CANCEL_GRACE_MS - 1, id='min-1'),
            pytest.param(MAX_CANCEL_GRACE_MS + 1, id='max+1'),
        ],
    )
    def test_an_isolated_grace_out_of_range(self, grace: int) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', cancel_grace_ms=grace)
            def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        error = _definition_error(declare)
        assert error.code is ErrorCode.TASK_CANCEL_GRACE_OUT_OF_RANGE
        assert 'execution is ISOLATED' in error.notes
        assert 'no hard stop' in error.help_text

    def test_a_grace_on_a_shared_thread_task(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task(  # pyright: ignore[reportArgumentType, reportUntypedFunctionDecorator]
                'bad',
                execution=Execution.SHARED_THREAD,  # pyright: ignore[reportArgumentType]
                timeout_ms=5_000,
                cancel_grace_ms=1_000,
            )
            def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        error = _definition_error(declare)
        assert error.code is ErrorCode.TASK_CANCEL_GRACE_REQUIRES_SHARED_ASYNC
        assert 'execution is SHARED_THREAD' in error.notes

    @pytest.mark.parametrize(
        'grace',
        [
            pytest.param(MIN_CANCEL_GRACE_MS - 1, id='min-1'),
            pytest.param(MAX_CANCEL_GRACE_MS + 1, id='max+1'),
            pytest.param(0, id='zero'),
            pytest.param(-1, id='negative'),
            pytest.param(10**5000, id='5001-digits'),
        ],
    )
    def test_a_grace_out_of_range(self, grace: int) -> None:
        def declare(app: Horsies) -> None:
            @app.task(
                'bad',
                execution=Execution.SHARED_ASYNC,
                timeout_ms=5_000,
                cancel_grace_ms=grace,
            )
            async def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        error = _definition_error(declare)
        assert error.code is ErrorCode.TASK_CANCEL_GRACE_OUT_OF_RANGE
        text = str(error)
        assert str(MIN_CANCEL_GRACE_MS) in text
        assert str(MAX_CANCEL_GRACE_MS) in text

    def test_the_kind_rule_is_reported_before_the_timeout_rule(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', execution=Execution.SHARED_ASYNC)  # pyright: ignore[reportArgumentType]
            def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        assert (
            _definition_error(declare).code
            is ErrorCode.TASK_SHARED_ASYNC_REQUIRES_ASYNC_DEF
        )

    def test_an_async_generator_is_refused_before_the_class_rules(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', execution=Execution.SHARED_ASYNC, timeout_ms=5_000)  # pyright: ignore[reportArgumentType]
            async def bad(*, n: int):  # pyright: ignore[reportUnusedFunction]
                yield n

        assert _definition_error(declare).code is ErrorCode.TASK_INVALID_RETURN_TYPE

    def test_a_value_that_is_not_a_class_is_an_invalid_option(self) -> None:
        def declare(app: Horsies) -> None:
            @app.task('bad', execution='PARALLEL')  # pyright: ignore[reportArgumentType]
            def bad(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        assert _definition_error(declare).code is ErrorCode.TASK_INVALID_OPTIONS

    def test_a_refused_task_is_not_registered(self) -> None:
        app = _app()
        with pytest.raises(TaskDefinitionError):

            @app.task('refused', execution=Execution.SHARED_THREAD, timeout_ms=5_000)  # pyright: ignore[reportArgumentType]
            async def refused(*, n: int) -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
                return Ok(n)

        assert 'refused' not in app.tasks.keys()

    def test_check_reports_the_rule_code_from_a_module_import(
        self,
        tmp_path: Path,
    ) -> None:
        module = tmp_path / 'shared_async_on_def.py'
        module.write_text(
            textwrap.dedent(
                """
                from pydantic import SecretStr

                from horsies import AppConfig, Execution, Horsies, PostgresConfig
                from horsies import Ok, Result, TaskError

                app = Horsies(AppConfig(broker=PostgresConfig(
                    database_url=SecretStr(
                        'postgresql://user:pass@127.0.0.1:1/none',
                    ),
                )))


                @app.task('mod_bad', execution=Execution.SHARED_ASYNC, timeout_ms=5000)
                def mod_bad(*, n: int) -> Result[int, TaskError]:
                    return Ok(n)
                """,
            ),
        )
        app = _app()
        app.discover_tasks([str(module)])

        errors = app.check()

        assert [error.code for error in errors] == [
            ErrorCode.TASK_SHARED_ASYNC_REQUIRES_ASYNC_DEF,
        ]


VIOLATION_CODES: list[tuple[ExecutionRuleViolation, ErrorCode]] = [
    (SharedAsyncOnDef(), ErrorCode.TASK_SHARED_ASYNC_REQUIRES_ASYNC_DEF),
    (SharedThreadOnAsyncDef(), ErrorCode.TASK_SHARED_THREAD_REQUIRES_DEF),
    (
        SharedWithoutTimeout(execution=Execution.SHARED_ASYNC),
        ErrorCode.TASK_SHARED_EXECUTION_REQUIRES_TIMEOUT,
    ),
    (CancelGraceOnSharedThread(), ErrorCode.TASK_CANCEL_GRACE_REQUIRES_SHARED_ASYNC),
    (
        CancelGraceOutOfRange(execution=Execution.SHARED_ASYNC),
        ErrorCode.TASK_CANCEL_GRACE_OUT_OF_RANGE,
    ),
]


class TestRuleCatalog:
    @pytest.mark.parametrize(('violation', 'code'), VIOLATION_CODES)
    def test_each_violation_has_its_own_code(
        self,
        violation: ExecutionRuleViolation,
        code: ErrorCode,
    ) -> None:
        error = execution_rule_error(violation, task_name='t', location=None)
        assert error.code is code
        assert "task 't'" in error.notes
        assert error.help_text

    def test_the_codes_are_distinct(self) -> None:
        codes = [code for _, code in VIOLATION_CODES]
        assert len(set(codes)) == len(codes)

    @pytest.mark.parametrize(
        ('kind', 'options', 'expected'),
        [
            (TaskBodyKind.DEF, TaskOptions(task_name='t'), Ok(None)),
            (TaskBodyKind.ASYNC_DEF, TaskOptions(task_name='t'), Ok(None)),
            (
                TaskBodyKind.ASYNC_DEF,
                TaskOptions(
                    task_name='t',
                    execution=Execution.SHARED_ASYNC,
                    timeout_ms=1_000,
                ),
                Ok(None),
            ),
            (
                TaskBodyKind.DEF,
                TaskOptions(
                    task_name='t',
                    execution=Execution.SHARED_THREAD,
                    timeout_ms=1_000,
                ),
                Ok(None),
            ),
            (
                TaskBodyKind.ASYNC_DEF,
                TaskOptions(
                    task_name='t',
                    execution=Execution.SHARED_ASYNC,
                    timeout_ms=1_000,
                    cancel_grace_ms=MAX_CANCEL_GRACE_MS + 1,
                ),
                Err(CancelGraceOutOfRange(execution=Execution.SHARED_ASYNC)),
            ),
            (
                TaskBodyKind.DEF,
                TaskOptions(
                    task_name='t',
                    execution=Execution.SHARED_THREAD,
                    cancel_grace_ms=1_000,
                ),
                Err(SharedWithoutTimeout(execution=Execution.SHARED_THREAD)),
            ),
        ],
        ids=[
            'isolated_def',
            'isolated_async',
            'shared_async',
            'shared_thread',
            'grace_above',
            'timeout_before_grace',
        ],
    )
    def test_check_execution_rules(
        self,
        kind: TaskBodyKind,
        options: TaskOptions,
        expected: Ok[None] | Err[ExecutionRuleViolation],
    ) -> None:
        assert check_execution_rules(kind, options) == expected


class TestSerializedOptions:
    @pytest.mark.parametrize(
        ('options', 'execution', 'grace'),
        [
            (TaskOptions(task_name='t'), 'ISOLATED', None),
            (TaskOptions(task_name='t', cancel_grace_ms=500), 'ISOLATED', 500),
            (
                TaskOptions(
                    task_name='t',
                    execution=Execution.SHARED_ASYNC,
                    timeout_ms=1_000,
                ),
                'SHARED_ASYNC',
                DEFAULT_CANCEL_GRACE_MS,
            ),
            (
                TaskOptions(
                    task_name='t',
                    execution=Execution.SHARED_ASYNC,
                    timeout_ms=1_000,
                    cancel_grace_ms=250,
                ),
                'SHARED_ASYNC',
                250,
            ),
            (
                TaskOptions(
                    task_name='t',
                    execution=Execution.SHARED_THREAD,
                    timeout_ms=1_000,
                ),
                'SHARED_THREAD',
                None,
            ),
        ],
        ids=['isolated', 'isolated_grace', 'async_default', 'async_declared', 'thread'],
    )
    def test_the_class_and_the_resolved_grace_are_written(
        self,
        options: TaskOptions,
        execution: str,
        grace: int | None,
    ) -> None:
        written = serialize_task_options(options)
        assert isinstance(written, Ok)
        stored = _stored(written.ok_value)
        assert stored['execution'] == execution
        assert stored['cancel_grace_ms'] == grace

    def test_written_options_read_back_as_their_class(self) -> None:
        for execution, timeout in (
            (Execution.ISOLATED, None),
            (Execution.SHARED_ASYNC, 1_000),
            (Execution.SHARED_THREAD, 1_000),
        ):
            written = serialize_task_options(
                TaskOptions(task_name='t', execution=execution, timeout_ms=timeout),
            )
            assert isinstance(written, Ok)
            assert stored_execution(json.loads(written.ok_value)) == Ok(execution)


def _refused_detail(options: Json) -> str:
    outcome = stored_execution(options)
    assert isinstance(outcome, Err)
    assert isinstance(outcome.err_value, InvalidExecution)
    return outcome.err_value.detail


class TestStoredExecution:
    def test_no_options_store_isolated(self) -> None:
        assert stored_execution(None) == Ok(Execution.ISOLATED)

    def test_options_without_the_key_store_isolated(self) -> None:
        assert stored_execution({'timeout_ms': None}) == Ok(Execution.ISOLATED)

    @pytest.mark.parametrize(
        ('options', 'expected'),
        [
            ({'execution': 'ISOLATED', 'cancel_grace_ms': None}, Execution.ISOLATED),
            ({'execution': 'ISOLATED', 'cancel_grace_ms': 1_000}, Execution.ISOLATED),
            ({'cancel_grace_ms': MIN_CANCEL_GRACE_MS}, Execution.ISOLATED),
            ({'execution': 'SHARED_THREAD'}, Execution.SHARED_THREAD),
            (
                {'execution': 'SHARED_ASYNC', 'cancel_grace_ms': MIN_CANCEL_GRACE_MS},
                Execution.SHARED_ASYNC,
            ),
            (
                {'execution': 'SHARED_ASYNC', 'cancel_grace_ms': MAX_CANCEL_GRACE_MS},
                Execution.SHARED_ASYNC,
            ),
        ],
        ids=[
            'isolated',
            'isolated_grace',
            'no_class_key_grace',
            'thread_without_grace_key',
            'async_min',
            'async_max',
        ],
    )
    def test_a_valid_class_is_stored(
        self,
        options: Json,
        expected: Execution,
    ) -> None:
        assert stored_execution(options) == Ok(expected)

    def test_options_that_are_not_an_object_are_refused(self) -> None:
        assert 'not an object' in _refused_detail([])

    def test_an_unknown_class_is_refused(self) -> None:
        assert 'execution is not one of' in _refused_detail({'execution': 'PARALLEL'})

    def test_a_class_that_is_not_a_string_is_refused(self) -> None:
        assert 'not a string' in _refused_detail({'execution': None})

    @pytest.mark.parametrize(
        ('grace', 'detail'),
        [
            (None, 'absent for execution SHARED_ASYNC'),
            (True, 'a bool'),
            (MIN_CANCEL_GRACE_MS - 1, f'below {MIN_CANCEL_GRACE_MS}'),
            (MAX_CANCEL_GRACE_MS + 1, f'above {MAX_CANCEL_GRACE_MS}'),
            (10**5000, f'above {MAX_CANCEL_GRACE_MS}'),
            (250.0, 'a JSON float'),
        ],
        ids=['null', 'bool', 'below', 'above', '5001-digits', 'float'],
    )
    def test_a_shared_async_grace_that_does_not_fit_is_refused(
        self,
        grace: Json,
        detail: str,
    ) -> None:
        assert detail in _refused_detail(
            {'execution': 'SHARED_ASYNC', 'cancel_grace_ms': grace},
        )

    def test_a_shared_async_row_without_the_grace_key_is_refused(self) -> None:
        assert 'absent' in _refused_detail({'execution': 'SHARED_ASYNC'})

    def test_a_grace_on_a_shared_thread_row_is_refused(self) -> None:
        assert 'only SHARED_ASYNC and ISOLATED have one' in _refused_detail(
            {'execution': 'SHARED_THREAD', 'cancel_grace_ms': 1_000},
        )

    @pytest.mark.parametrize(
        ('grace', 'detail'),
        [
            (True, 'a bool'),
            (MIN_CANCEL_GRACE_MS - 1, f'below {MIN_CANCEL_GRACE_MS}'),
            (MAX_CANCEL_GRACE_MS + 1, f'above {MAX_CANCEL_GRACE_MS}'),
            (250.0, 'a JSON float'),
        ],
        ids=['bool', 'below', 'above', 'float'],
    )
    def test_an_isolated_grace_that_does_not_fit_is_refused(
        self,
        grace: Json,
        detail: str,
    ) -> None:
        assert detail in _refused_detail({'execution': 'ISOLATED', 'cancel_grace_ms': grace})


class TestPerSendOptions:
    def test_a_send_with_a_deadline_keeps_the_class_and_the_grace(self) -> None:
        app = _app()

        @app.task(
            'sa_send',
            execution=Execution.SHARED_ASYNC,
            timeout_ms=5_000,
            cancel_grace_ms=750,
        )
        async def sa_send(*, n: int) -> Result[int, TaskError]:
            return Ok(n)

        broker = MagicMock()
        broker.enqueue.return_value = Ok('task-1')
        deadline = datetime.now(timezone.utc) + timedelta(hours=1)
        with patch.object(app, 'get_broker', return_value=broker):
            sent = sa_send.with_options(good_until=deadline).send(n=1)

        assert isinstance(sent, Ok)
        stored = _stored(broker.enqueue.call_args.kwargs['task_options'])
        assert stored['execution'] == 'SHARED_ASYNC'
        assert stored['cancel_grace_ms'] == 750
        assert stored['good_until'] == deadline.isoformat()


