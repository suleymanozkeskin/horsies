"""The task context of log records, and the JSON log format.

``TaskContextFilter`` puts the running task of the current context on each
record; ``JsonLogsFormatter`` writes one JSON object per record with fixed
keys; ``configure_logging`` installs both on the horsies handler by
``AppConfig.log_format``; the CLI applies the app's format once it has found
the app.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import SecretStr, ValidationError

from horsies.core.cli import check_command
from horsies.core.logging import (
    TASK_CONTEXT_FIELDS,
    ColoredFormatter,
    JsonLogsFormatter,
    NoRunningTask,
    NoSpanIds,
    RunningTask,
    SpanIds,
    TaskContextFilter,
    configure_logging,
    enter_running_task,
    leave_running_task,
    running_task,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.log_format import JsonLogs, TextLogs

pytestmark = [pytest.mark.unit]

_BROKER = PostgresConfig(database_url=SecretStr('postgresql://user:pass@127.0.0.1:1/none'))
TRACE_ID = '0af7651916cd43dd8448eb211c80319c'
SPAN_ID = 'b7ad6b7169203331'
JSON_KEYS = (
    'ts',
    'level',
    'logger',
    'message',
    'process',
    'task_id',
    'task_name',
    'attempt',
    'trace_id',
    'span_id',
    'exc',
)


def _record(message: str = 'hello', **extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name='horsies.worker',
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=None,
        exc_info=None,
    )
    for name, value in extra.items():
        setattr(record, name, value)
    return record


@pytest.fixture
def inside_a_task() -> Iterator[RunningTask]:
    task = RunningTask(
        task_id='task-1',
        task_name='resize',
        attempt=2,
        span=SpanIds(trace_id=TRACE_ID, span_id=SPAN_ID),
    )
    token = enter_running_task(task)
    yield task
    leave_running_task(token)


@pytest.fixture(autouse=True)
def restore_horsies_handlers() -> Iterator[None]:
    root = logging.getLogger('horsies')
    handlers = list(root.handlers)
    level = root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


class TestTheRunningTask:
    def test_no_task_runs_outside_a_body(self) -> None:
        assert running_task() == NoRunningTask()

    def test_leaving_restores_the_task_before(self) -> None:
        token = enter_running_task(RunningTask('t', 'n', 1, NoSpanIds()))
        leave_running_task(token)

        assert running_task() == NoRunningTask()


class TestTaskContextFilter:
    def test_a_record_in_a_task_with_a_span_gets_every_field(
        self,
        inside_a_task: RunningTask,
    ) -> None:
        record = _record()

        assert TaskContextFilter().filter(record) is True
        assert {name: record.__dict__[name] for name in TASK_CONTEXT_FIELDS} == {
            'task_id': 'task-1',
            'task_name': 'resize',
            'attempt': 2,
            'trace_id': TRACE_ID,
            'span_id': SPAN_ID,
        }

    def test_a_record_in_a_task_with_no_span_has_no_ids(self) -> None:
        token = enter_running_task(RunningTask('task-2', 'crop', 4, NoSpanIds()))
        try:
            record = _record()
            _ = TaskContextFilter().filter(record)
        finally:
            leave_running_task(token)

        assert {name: record.__dict__[name] for name in TASK_CONTEXT_FIELDS} == {
            'task_id': 'task-2',
            'task_name': 'crop',
            'attempt': 4,
            'trace_id': None,
            'span_id': None,
        }

    def test_a_record_outside_a_task_gets_each_field_as_none(self) -> None:
        record = _record()

        assert TaskContextFilter().filter(record) is True
        assert all(record.__dict__[name] is None for name in TASK_CONTEXT_FIELDS)

    def test_a_record_that_names_its_task_keeps_its_fields(
        self,
        inside_a_task: RunningTask,
    ) -> None:
        """A record of the core carries the task of its own event."""
        record = _record(task_id='core-task', trace_id='f' * 32, attempt=2)

        _ = TaskContextFilter().filter(record)

        assert record.__dict__['task_id'] == 'core-task'
        assert record.__dict__['trace_id'] == 'f' * 32
        assert record.__dict__['attempt'] == 2
        assert record.__dict__['task_name'] == 'resize'


class TestJsonLogsFormatter:
    def test_a_record_in_a_task_is_one_line_with_the_fixed_keys(
        self,
        inside_a_task: RunningTask,
    ) -> None:
        record = _record('started %s', attempt=3)
        record.args = ('work',)
        record.created = datetime(2026, 11, 19, 10, 0, 0, 123456, tzinfo=UTC).timestamp()
        _ = TaskContextFilter().filter(record)

        line = JsonLogsFormatter().format(record)

        assert '\n' not in line
        parsed = json.loads(line)
        assert tuple(parsed) == JSON_KEYS
        assert parsed == {
            'ts': '2026-11-19T10:00:00.123456+00:00',
            'level': 'INFO',
            'logger': 'horsies.worker',
            'message': 'started work',
            'process': record.process,
            'task_id': 'task-1',
            'task_name': 'resize',
            'attempt': 3,
            'trace_id': TRACE_ID,
            'span_id': SPAN_ID,
            'exc': None,
        }

    def test_a_record_with_no_context_has_null_context_keys(self) -> None:
        parsed = json.loads(JsonLogsFormatter().format(_record()))

        assert tuple(parsed) == JSON_KEYS
        assert [parsed[name] for name in JSON_KEYS[5:]] == [None] * 6

    def test_an_exception_is_its_traceback_text(self) -> None:
        try:
            raise ValueError('bad input')
        except ValueError:
            record = _record()
            record.exc_info = sys.exc_info()

        parsed = json.loads(JsonLogsFormatter().format(record))

        assert parsed['exc'].startswith('Traceback (most recent call last):')
        assert parsed['exc'].endswith('ValueError: bad input')

    def test_a_context_value_of_another_type_is_its_text(self) -> None:
        parsed = json.loads(JsonLogsFormatter().format(_record(task_id=('a', 1))))

        assert parsed['task_id'] == "('a', 1)"

    def test_a_character_outside_ascii_is_escaped(self) -> None:
        line = JsonLogsFormatter().format(_record('café \ud800'))

        assert line.isascii()
        assert json.loads(line)['message'] == 'café \ud800'


def _horsies_handler() -> logging.Handler:
    [handler] = logging.getLogger('horsies').handlers
    return handler


class TestConfigureLogging:
    def test_text_logs_use_the_colored_formatter_and_the_context_filter(self) -> None:
        configure_logging(logging.INFO)

        handler = _horsies_handler()
        assert isinstance(handler.formatter, ColoredFormatter)
        assert any(isinstance(f, TaskContextFilter) for f in handler.filters)

    def test_json_logs_use_the_json_formatter_and_the_context_filter(self) -> None:
        configure_logging(logging.INFO, log_format=JsonLogs())

        handler = _horsies_handler()
        assert isinstance(handler.formatter, JsonLogsFormatter)
        assert any(isinstance(f, TaskContextFilter) for f in handler.filters)


class TestTheSetting:
    def test_an_app_config_writes_text_by_default(self) -> None:
        assert AppConfig(broker=_BROKER).log_format == TextLogs()

    def test_json_logs_are_accepted_in_both_forms(self) -> None:
        assert AppConfig(broker=_BROKER, log_format=JsonLogs()).log_format == JsonLogs()
        assert AppConfig.model_validate(
            {'broker': _BROKER, 'log_format': {'kind': 'json_logs'}},
        ).log_format == JsonLogs()

    def test_another_kind_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            _ = AppConfig.model_validate(
                {'broker': _BROKER, 'log_format': {'kind': 'xml_logs'}},
            )


class TestTheCliUsesTheFormatOfTheApp:
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_check_logs_in_the_format_of_the_app_once_it_found_it(
        self,
        mock_discover: MagicMock,
        mock_logging: MagicMock,
    ) -> None:
        app = MagicMock()
        app.check.return_value = []
        app.list_tasks.return_value = []
        app.declared_error_codes.return_value = {}
        app.config = AppConfig(broker=_BROKER, log_format=JsonLogs())
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = argparse.Namespace(module='my.mod:app', module_pos=None, loglevel='INFO', live=False)

        with pytest.raises(SystemExit):
            check_command(args)

        assert mock_logging.call_args_list == [call('INFO'), call('INFO', JsonLogs())]
