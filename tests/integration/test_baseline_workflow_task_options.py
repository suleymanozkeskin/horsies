"""The phase-2 step reads a promoted task node's stored task options as Python does.

Each case stores a task options text on the dependent node of a two-node
workflow, completes the root through the phase-2 step of the baseline
(``horsies_workflow_node_finish``, through the ``wft_finish`` fixture of
crates/horsies-core/src/sql_tests/cases/baseline_workflow_fixtures.sql), and
compares the outcome with Python's ``_build_enqueued_task_params`` for the
same text:

- Python refuses the options: the node is FAILED with no task, and its stored
  result is the error that ``_fail_enqueued_task`` stores (compared as JSON).
- Python reads them: the node is ENQUEUED with a task row. The task row's
  ``good_until`` is the value that the database reads from Python's
  ``good_until`` text, or the step raises a data exception when the database
  refuses that text. Deadlines are compared as their text in the session, so
  ``infinity`` compares too.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock

import psycopg
import pytest
import pytest_asyncio

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.task_options import serialize_task_options
from horsies.core.models.tasks import (
    Execution,
    RetryPolicy,
    TaskError,
    TaskOptions,
    TaskResult,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import to_psycopg_url
from horsies.core.workflows.engine import _build_enqueued_task_params  # pyright: ignore[reportPrivateUsage]
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

_FIXTURES_SQL: Final = (
    Path(__file__).parent.parent.parent
    / 'crates'
    / 'horsies-core'
    / 'src'
    / 'sql_tests'
    / 'cases'
    / 'baseline_workflow_fixtures.sql'
)

_WORKFLOW_ID: Final = '00000000-0000-7000-8000-00000000f001'
_DEPENDENT_INDEX: Final = 1

_START_SQL: Final = (
    "SELECT count(*) FROM wft_start1(%s, 'fail', '[{\"idx\":0},{\"idx\":1,\"deps\":[0]}]')"
)
_STORE_OPTIONS_SQL: Final = (
    'UPDATE horsies_workflow_tasks SET task_options = %s WHERE workflow_id = %s AND task_index = %s'
)
_FINISH_ROOT_SQL: Final = "SELECT wft_finish(%s, 0, wft_ok('1'))"
_DEPENDENT_SQL: Final = """
    SELECT n.status, n.task_id IS NOT NULL, n.result, t.good_until::text
    FROM horsies_workflow_tasks n
    LEFT JOIN horsies_tasks t ON t.id = n.task_id
    WHERE n.workflow_id = %s AND n.task_index = %s
"""


@dataclass(frozen=True, slots=True)
class NodeFailed:
    """The node is FAILED with no task. ``result`` is its stored result, parsed."""

    result: object


@dataclass(frozen=True, slots=True)
class NodeEnqueued:
    """The node is ENQUEUED with a task row. ``good_until`` is the text of its
    deadline in the session, or None for no deadline."""

    good_until: str | None


@dataclass(frozen=True, slots=True)
class StepRaised:
    """The step raised a data exception (SQLSTATE class 22) and wrote nothing."""


type PromotionOutcome = NodeFailed | NodeEnqueued | StepRaised


def _library_options(options: TaskOptions) -> str:
    match serialize_task_options(options):
        case Ok(text):
            return text
        case Err(error):
            raise AssertionError(f'task options of the corpus do not serialize: {error}')


_POLICY: Final = '"auto_retry_for": ["X"]'
"""The one required member of a retry policy."""

_READ_OR_REFUSED_CASES: Final = (
    # No options, and options that Python reads.
    pytest.param(None, id='no options'),
    pytest.param('{}', id='empty object'),
    pytest.param('{"retry_policy": null, "x": [1, {"y": "z"}]}', id='other members are not read'),
    pytest.param(_library_options(TaskOptions(task_name='t')), id='library options, defaults'),
    pytest.param(
        _library_options(
            TaskOptions(
                task_name='t',
                retry_policy=RetryPolicy.exponential(
                    30,
                    max_retries=5,
                    auto_retry_for=['RATE_LIMITED'],
                    max_delay_seconds=600,
                ),
                execution=Execution.SHARED_ASYNC,
                cancel_grace_ms=750,
                good_until=datetime(2031, 1, 1, tzinfo=timezone.utc),
                timeout_ms=5000,
            ),
        ),
        id='library options, exponential policy, shared async, deadline',
    ),
    pytest.param(
        _library_options(
            TaskOptions(
                task_name='t',
                retry_policy=RetryPolicy.fixed([1, 2], auto_retry_for=['TASK_TIMEOUT']),
                execution=Execution.SHARED_THREAD,
            ),
        ),
        id='library options, fixed policy, shared thread',
    ),
    pytest.param('{"retry_policy": {%s}}' % _POLICY, id='policy with defaults'),
    pytest.param(
        '{ "retry_policy" : { "intervals" : [ 1 , 86400 ] , "max_retries" : 2 , %s } }' % _POLICY,
        id='policy with whitespace',
    ),
    pytest.param(
        '{"retry_policy": {"max\\u005fretries": 1, "intervals": [5], %s}}' % _POLICY,
        id='escaped field name',
    ),
    pytest.param(
        '{"retry_policy": {"max_retries": 1, "intervals": [5], "auto_retry_for": ["X\\u005fY"]}}',
        id='escaped code',
    ),
    pytest.param(
        '{"retry_policy": {"max_retries": 3, "intervals": [1, 2, 3], %s, "max_delay_seconds": null}}' % _POLICY,
        id='null max_delay_seconds',
    ),
    pytest.param(
        '{"retry_policy": {"max_retries": 3, "intervals": [1, 2, 3], %s, '
        '"max_delay_seconds": 99999999999999999999999}}' % _POLICY,
        id='large max_delay_seconds',
    ),
    pytest.param('{"retry_policy": {"auto_retry_for": ["UNHANDLED_EXCEPTION"]}}', id='builtin code'),
    pytest.param('{"execution": "SHARED_ASYNC", "cancel_grace_ms": 100}', id='lowest grace'),
    pytest.param('{"execution": "SHARED_ASYNC", "cancel_grace_ms": 10000}', id='highest grace'),
    pytest.param('{"execution": "SHARED_THREAD", "cancel_grace_ms": null}', id='thread with null grace'),
    pytest.param('{"execution": "SHARED_\\u0041SYNC", "cancel_grace_ms": 1000}', id='escaped execution'),
    pytest.param('{"a": 1.7976931348623157e308}', id='largest double'),
    pytest.param('{"a": 1.7976931348623158e308}', id='float rounding to the largest double'),
    pytest.param('{"a": 1e-400}', id='float underflow to zero'),
    pytest.param('{"a": %s}' % ('9' * 4300), id='int of 4300 digits'),
    pytest.param('{"a": -%s}' % ('9' * 4300), id='negative int of 4300 digits'),
    pytest.param('{"a":' + '[' * 299 + ']' * 299 + '}', id='nesting of 300'),
    pytest.param('{"a": "\\ud83d\\ude00"}', id='surrogate pair'),
    pytest.param('{"a": "\\\\ud800"}', id='escaped backslash before u'),
    pytest.param('{"a": "x", "b": {"a": 1}}', id='same key in two objects'),
    # Text that does not parse. An empty text is not JSON; a node without
    # options stores NULL.
    pytest.param('', id='empty options'),
    pytest.param('not json', id='not json'),
    pytest.param('   ', id='whitespace'),
    pytest.param('{"a": 1,}', id='trailing comma'),
    pytest.param('{"a": NaN}', id='NaN'),
    pytest.param('{"a": Infinity}', id='Infinity'),
    pytest.param('{"a": 01}', id='leading zero'),
    pytest.param('{"a": 1.}', id='bare fraction point'),
    pytest.param('{"a": "\\x"}', id='bad escape'),
    pytest.param('{"a": "\\ud800"}', id='lone high surrogate'),
    pytest.param('{"a": "\\udc00"}', id='lone low surrogate'),
    pytest.param('{"a": "\t"}', id='raw control character'),
    pytest.param('{"a": 1, "a": 2}', id='repeated key'),
    pytest.param('{"a": 1, "\\u0061": 2}', id='repeated key through an escape'),
    pytest.param('{"retry_policy": {"x": 1, "x": 1}}', id='repeated key in a nested object'),
    pytest.param('{"a": [{"b": 1, "b": 1}]}', id='repeated key in an array item'),
    pytest.param('{"a": 1e400}', id='float rounding to infinity'),
    pytest.param('{"a": -1.797693134862315808e308}', id='negative float rounding to infinity'),
    pytest.param('{"a": %s}' % ('9' * 4301), id='int of 4301 digits'),
    pytest.param('{"a": -%s}' % ('9' * 4301), id='negative int of 4301 digits'),
    pytest.param('{"a":' + '[' * 300 + ']' * 300 + '}', id='nesting of 301'),
    pytest.param('null', id='JSON null'),
    # Options that are not an object.
    pytest.param('[1]', id='array'),
    pytest.param('"text"', id='string'),
    pytest.param('5', id='number'),
    pytest.param('true', id='boolean'),
    # A retry_policy that is not an object.
    pytest.param('{"retry_policy": 5}', id='policy int'),
    pytest.param('{"retry_policy": -0}', id='policy negative zero'),
    pytest.param('{"retry_policy": 5.0}', id='policy float'),
    pytest.param('{"retry_policy": 1e5}', id='policy exponent float'),
    pytest.param('{"retry_policy": true}', id='policy bool'),
    pytest.param('{"retry_policy": "x"}', id='policy str'),
    pytest.param('{"retry_policy": [1]}', id='policy list'),
    pytest.param('{"retry_policy": []}', id='policy empty list'),
    # Field errors of a retry_policy object.
    pytest.param('{"retry_policy": {}}', id='policy missing auto_retry_for'),
    *(
        pytest.param('{"retry_policy": {%s, "max_retries": %s}}' % (_POLICY, value), id=f'max_retries {value}')
        for value in ('0', '21', 'true', '3.0', '1e0', '"3"', 'null', '[1]', '-0', '10' * 15, '-' + '10' * 15)
    ),
    *(
        pytest.param('{"retry_policy": {%s, "intervals": %s}}' % (_POLICY, value), id=f'intervals {value[:30]}')
        for value in (
            '[]',
            '"abc"',
            '{"a": 1}',
            'null',
            '[' + ','.join(['1'] * 21) + ']',
            '[' + ','.join(['0'] * 25) + ']',
            '[0, ' + ','.join(['1'] * 20) + ']',
            '[0, -1, 86401, 1.5, true, "a", null, 1e2, [1], 86400, 1]',
            '[0]',
            '[-1]',
            '[1, 0]',
            '[1, 86401]',
            '[1, 90000]',
            '[86401]',
            '[86410]',
            '[90000]',
            '[100000]',
            '[1, 9999, 10000, 79999, 80000, 85999, 86000, 86399, 86400]',
            '[%s]' % ('9' * 40),
        )
    ),
    *(
        pytest.param(
            '{"retry_policy": {%s, "backoff_strategy": %s}}' % (_POLICY, value),
            id=f'backoff_strategy {value}',
        )
        for value in ('"linear"', '1', 'null', '"FIXED"', '"fixe\\u0064"')
    ),
    *(
        pytest.param('{"retry_policy": {%s, "jitter": %s}}' % (_POLICY, value), id=f'jitter {value}')
        for value in ('1', '0', '"true"', 'null', 'false')
    ),
    *(
        pytest.param(
            '{"retry_policy": {%s, "max_delay_seconds": %s}}' % (_POLICY, value),
            id=f'max_delay_seconds {value}',
        )
        for value in ('0', '-5', '"a"', 'true', '1.0', '[1]')
    ),
    *(
        pytest.param('{"retry_policy": {"auto_retry_for": %s}}' % value, id=f'auto_retry_for {value[:30]}')
        for value in (
            '[]',
            '"X"',
            'null',
            '{}',
            '[1, null, true, {"a": 1}]',
            '[[1]]',
            '[1.5]',
            '["X", 1]',
        )
    ),
    pytest.param('{"retry_policy": {%s, "extra": 1, "zzz": 2}}' % _POLICY, id='extra members'),
    pytest.param(
        '{"retry_policy": {%s}}' % ', '.join(f'"{key}": 1' for key in 'abcdefghijkl'),
        id='twelve extra members and a missing field',
    ),
    pytest.param('{"retry_policy": {"a.b": 1, "": 2, %s}}' % _POLICY, id='dotted and empty extra keys'),
    pytest.param(
        '{"retry_policy": {"zz": 1, "auto_retry_for": ["X", 1], "aa": 2, "max_retries": 0}}',
        id='field and extra errors in order',
    ),
    # Model validator errors of a retry_policy object.
    pytest.param('{"retry_policy": {"intervals": [5], %s}}' % _POLICY, id='fixed with one interval'),
    pytest.param('{"retry_policy": {"max_retries": 1, %s}}' % _POLICY, id='fixed with default intervals'),
    pytest.param(
        '{"retry_policy": {"max_retries": 2, "intervals": [1, 2], "backoff_strategy": "exponential", %s}}'
        % _POLICY,
        id='exponential with two intervals',
    ),
    pytest.param(
        '{"retry_policy": {"max_retries": 1, "intervals": [5], "backoff_strategy": "exponential", %s}}'
        % _POLICY,
        id='exponential with one interval',
    ),
    *(
        pytest.param(
            '{"retry_policy": {"max_retries": 3, "intervals": [1, 2, 3], "auto_retry_for": %s}}' % codes,
            id=f'codes {codes}',
        )
        for codes in (
            '["x"]',
            '["ValueError"]',
            '["FooError"]',
            '["FOOError"]',
            '["Exception"]',
            '[""]',
            '["WORKFLOW_STOPPED"]',
            '["OK", "WORKFLOW_STOPPED"]',
            '["\\u00c9"]',
            '["A_\\u00c9"]',
            '["X\\nY"]',
            '["ABC\\n"]',
            '["OK", "", "bad"]',
            '["OK", "bad", ""]',
        )
    ),
    pytest.param(
        '{"retry_policy": {"max_retries": 2, "intervals": [1, 2, 3], "auto_retry_for": ["X", "bad"]}}',
        id='strategy error before code error',
    ),
    # Execution errors.
    pytest.param('{"execution": "ISOLATED", "cancel_grace_ms": 5}', id='isolated with grace'),
    pytest.param('{"cancel_grace_ms": 5}', id='default class with grace'),
    pytest.param('{"execution": "SHARED_THREAD", "cancel_grace_ms": false}', id='thread with grace'),
    pytest.param('{"execution": "SHARED_ASYNC"}', id='shared async without grace'),
    pytest.param('{"execution": "SHARED_ASYNC", "cancel_grace_ms": null}', id='shared async with null grace'),
    *(
        pytest.param(
            '{"execution": "SHARED_ASYNC", "cancel_grace_ms": %s}' % value,
            id=f'shared async grace {value}',
        )
        for value in ('true', '5.0', '1e3', '0', '99', '10001', '99999999999', '"1000"', '[1000]', '{"a": 1}')
    ),
    *(
        pytest.param('{"execution": %s}' % value, id=f'execution {value}')
        for value in ('"bogus"', '"isolated"', '3', '1.5', 'null', 'true', '[]', '{}')
    ),
    pytest.param(
        '{"retry_policy": 5, "execution": "bogus"}',
        id='retry_policy refused before execution',
    ),
)

_GOOD_UNTIL_CASES: Final = (
    pytest.param('null', id='null'),
    pytest.param('"2031-01-01T00:00:00+00:00"', id='aware isoformat'),
    pytest.param('"2031-01-01T00:00:00Z"', id='Z suffix'),
    pytest.param('"2031-01-01T00:00:00.123456+02:00"', id='fraction and offset'),
    pytest.param('"2031-01-01 00:00:00"', id='no offset'),
    pytest.param('"infinity"', id='infinity'),
    pytest.param('"not-a-date"', id='text that is not a time'),
    pytest.param('""', id='empty text'),
    pytest.param('"2031-13-01T00:00:00Z"', id='month out of range'),
    pytest.param('12345', id='int'),
    pytest.param('1e5', id='float'),
    pytest.param('100000.5', id='float with fraction'),
    pytest.param('true', id='bool'),
    pytest.param('{"a": 1}', id='object'),
    pytest.param('[1]', id='array'),
)

_PYTHON_READS_POSTGRESQL_REFUSES: Final = (
    pytest.param('{"a": "\\u0000"}', id='escape of U+0000'),
    pytest.param('{"a": 1e-20000}', id='exponent outside numeric'),
)


def _migration() -> BaselineMigration:
    return load_baseline_migration()


@pytest_asyncio.fixture(scope='module', loop_scope='module')
async def workflow_database() -> AsyncGenerator[str]:
    """The conninfo of a fresh database with the baseline and the fixture helpers committed."""
    migration = _migration()
    async with isolated_test_database(DB_URL) as url:
        conninfo = to_psycopg_url(url)
        connection = await psycopg.AsyncConnection.connect(conninfo)
        try:
            # No parameters: psycopg sends each text with the simple query
            # protocol, which runs every statement of it.
            async with connection.transaction():
                await connection.execute(migration.sql.encode('utf-8'))
                await connection.execute(
                    _FIXTURES_SQL.read_text(encoding='utf-8').encode('utf-8')
                )
        finally:
            await connection.close()
        yield conninfo


@pytest_asyncio.fixture(loop_scope='module')
async def connection(
    workflow_database: str,
) -> AsyncGenerator[psycopg.AsyncConnection[tuple[Any, ...]]]:
    """A connection whose writes are rolled back after the test."""
    opened = await psycopg.AsyncConnection.connect(workflow_database)
    try:
        async with opened.transaction(force_rollback=True):
            yield opened
    finally:
        await opened.close()


async def _promote(
    connection: psycopg.AsyncConnection[tuple[Any, ...]],
    options: str | None,
) -> PromotionOutcome:
    """Store ``options`` on the dependent node, run the phase-2 step of the
    root, and read the dependent node."""
    await connection.execute(_START_SQL, (_WORKFLOW_ID,))
    await connection.execute(_STORE_OPTIONS_SQL, (options, _WORKFLOW_ID, _DEPENDENT_INDEX))
    try:
        async with connection.transaction():
            await connection.execute(_FINISH_ROOT_SQL, (_WORKFLOW_ID,))
    except psycopg.DataError:
        return StepRaised()
    cursor = await connection.execute(_DEPENDENT_SQL, (_WORKFLOW_ID, _DEPENDENT_INDEX))
    row = await cursor.fetchone()
    match row:
        case ('FAILED', False, str() as result, None):
            return NodeFailed(result=json.loads(result))
        case ('ENQUEUED', True, None, None):
            return NodeEnqueued(good_until=None)
        case ('ENQUEUED', True, None, str() as deadline):
            return NodeEnqueued(good_until=deadline)
        case _:
            pytest.fail(f'the dependent node is in a state that no outcome names: {row!r}')


async def _database_deadline(
    connection: psycopg.AsyncConnection[tuple[Any, ...]],
    good_until: str | None,
) -> PromotionOutcome:
    """The outcome of Python's insert of the ``good_until`` text: the database
    reads the text into the timestamp column, or refuses it."""
    match good_until:
        case None:
            return NodeEnqueued(good_until=None)
        case text:
            try:
                async with connection.transaction():
                    cursor = await connection.execute('SELECT %s::timestamptz::text', (text,))
                    row = await cursor.fetchone()
            except psycopg.DataError:
                return StepRaised()
            match row:
                case (str() as deadline,):
                    return NodeEnqueued(good_until=deadline)
                case _:
                    pytest.fail(f'the deadline cast returned no text: {row!r}')


async def _python_promotion(
    connection: psycopg.AsyncConnection[tuple[Any, ...]],
    options: str | None,
) -> PromotionOutcome:
    """Python's promotion of the dependent node for the stored ``options``."""
    built = await _build_enqueued_task_params(
        MagicMock(),
        _WORKFLOW_ID,
        _DEPENDENT_INDEX,
        task_name=f't{_DEPENDENT_INDEX}',
        node_input='{}',
        workflow_sent_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        queue_name='default',
        priority=50,
        args_from=None,
        workflow_ctx_from=None,
        task_options_str=options,
        all_dep_results={},
        all_dep_task_names=None,
        broker=None,
        all_dep_definition_keys=None,
    )
    match built:
        case Err(refusal):
            stored = serialize_error_payload(
                TaskResult(
                    err=TaskError(
                        error_code=refusal.error_code,
                        message=refusal.message,
                        data={'workflow_id': _WORKFLOW_ID, 'task_index': _DEPENDENT_INDEX},
                    ),
                ),
            )
            return NodeFailed(result=json.loads(stored))
        case Ok(params):
            return await _database_deadline(connection, params['good_until'])


@pytest.mark.asyncio(loop_scope='module')
@pytest.mark.parametrize('options', _READ_OR_REFUSED_CASES)
async def test_promotion_reads_stored_options_as_python(
    connection: psycopg.AsyncConnection[tuple[Any, ...]],
    options: str | None,
) -> None:
    expected = await _python_promotion(connection, options)
    assert await _promote(connection, options) == expected


@pytest.mark.asyncio(loop_scope='module')
@pytest.mark.parametrize('good_until', _GOOD_UNTIL_CASES)
async def test_promotion_mints_the_stored_deadline_as_python(
    connection: psycopg.AsyncConnection[tuple[Any, ...]],
    good_until: str,
) -> None:
    options = '{"good_until": %s}' % good_until
    expected = await _python_promotion(connection, options)
    assert await _promote(connection, options) == expected


@pytest.mark.asyncio(loop_scope='module')
@pytest.mark.parametrize('options', _PYTHON_READS_POSTGRESQL_REFUSES)
async def test_promotion_refuses_text_that_postgresql_cannot_hold(
    connection: psycopg.AsyncConnection[tuple[Any, ...]],
    options: str,
) -> None:
    # Python reads these options; PostgreSQL text cannot hold U+0000, and
    # numeric cannot hold the exponent, so the step fails the node as text
    # that does not parse.
    assert await _python_promotion(connection, options) == NodeEnqueued(good_until=None)
    corrupt = await _python_promotion(connection, 'not json')
    assert await _promote(connection, options) == corrupt
