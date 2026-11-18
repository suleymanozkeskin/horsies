"""``PostgresConfig.echo`` shows the core's statement log as SQLAlchemy's
``echo=True`` shows its statements.

``enable_statement_echo`` sets the ``horsies.sql`` logger to INFO and adds
one stdout handler per process, and only to a logger with no handler. A
broker built with ``echo=True`` calls it; a broker built with ``echo=False``
changes nothing. The brokers here open no connection: construction does no
I/O.

Each statement shows on stdout once in each logging configuration: with no
configuration, under ``configure_logging`` (the CLI and a worker child), and
with an application's own handler on ``horsies`` or the root logger, which
gets the record too.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from typing import Final

import pytest
from pydantic import SecretStr

from horsies._core import refresh_log_levels
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import (
    STATEMENT_ECHO_FORMAT,
    STATEMENT_LOGGER_NAME,
    configure_logging,
    enable_statement_echo,
)
from horsies.core.models.broker import PostgresConfig

pytestmark = pytest.mark.unit

# Construction opens no connection, so no server listens at this address.
UNUSED_DATABASE_URL: Final = 'postgresql://user:password@127.0.0.1:1/horsies_echo_unit'

HORSIES_LOGGER_NAME: Final = 'horsies'
ROOT_LOGGER_NAME: Final = ''
STATEMENT_TEXT: Final = 'SELECT 1'
# The end of the stdout line of STATEMENT_TEXT in STATEMENT_ECHO_FORMAT.
ECHOED_STATEMENT_END: Final = f' INFO horsies.sql {STATEMENT_TEXT}'


@pytest.fixture
def statement_logger() -> Iterator[logging.Logger]:
    """The ``horsies.sql`` logger with no handler and no level, restored to
    its earlier handlers, level and propagation after the test."""
    logger = logging.getLogger(STATEMENT_LOGGER_NAME)
    earlier_handlers = list(logger.handlers)
    earlier_level = logger.level
    earlier_propagate = logger.propagate
    logger.handlers = []
    logger.setLevel(logging.NOTSET)
    yield logger
    logger.handlers = earlier_handlers
    logger.setLevel(earlier_level)
    logger.propagate = earlier_propagate
    refresh_log_levels()


@pytest.fixture
def horsies_logger() -> Iterator[logging.Logger]:
    """The ``horsies`` logger as the library leaves it at import (one
    ``NullHandler``, no level), restored to its earlier handlers and level
    after the test. ``configure_logging`` replaces its handlers."""
    logger = logging.getLogger(HORSIES_LOGGER_NAME)
    earlier_handlers = list(logger.handlers)
    earlier_level = logger.level
    logger.handlers = [logging.NullHandler()]
    logger.setLevel(logging.NOTSET)
    yield logger
    logger.handlers = earlier_handlers
    logger.setLevel(earlier_level)
    refresh_log_levels()


class _RecordingHandler(logging.Handler):
    """A handler of the application: keeps every record it handles."""

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@contextmanager
def _attached(
    logger: logging.Logger,
    handler: logging.Handler,
) -> Generator[None, None, None]:
    """``handler`` on ``logger`` for the block, removed after it."""
    logger.addHandler(handler)
    try:
        yield
    finally:
        logger.removeHandler(handler)


def _configure_logging_as_the_cli() -> None:
    """The call of ``horsies worker`` at its default level."""
    configure_logging(logging.INFO)


def _the_one_line(text: str) -> str:
    match text.splitlines():
        case [line]:
            return line
        case lines:
            raise AssertionError(f'expected one line, got {lines!r}')


def _broker(*, echo: bool) -> PostgresBroker:
    return PostgresBroker(
        PostgresConfig(database_url=SecretStr(UNUSED_DATABASE_URL), echo=echo),
    )


def _the_one_stdout_handler(logger: logging.Logger) -> logging.Handler:
    """The one handler of ``logger``, which writes to ``sys.stdout``."""
    match logger.handlers:
        case [logging.StreamHandler() as handler]:
            assert handler.stream is sys.stdout
            return handler
        case other:
            raise AssertionError(f'expected one stdout handler, got {other!r}')


class TestEnableStatementEcho:
    def test_sets_info_and_adds_one_stdout_handler_with_sqlalchemy_format(
        self,
        statement_logger: logging.Logger,
    ) -> None:
        enable_statement_echo()

        assert statement_logger.level == logging.INFO
        handler = _the_one_stdout_handler(statement_logger)
        assert handler.formatter is not None
        assert handler.formatter._fmt == STATEMENT_ECHO_FORMAT  # pyright: ignore[reportPrivateUsage]
        assert STATEMENT_ECHO_FORMAT == '%(asctime)s %(levelname)s %(name)s %(message)s'

    def test_a_second_call_adds_no_second_handler(
        self,
        statement_logger: logging.Logger,
    ) -> None:
        enable_statement_echo()
        first = _the_one_stdout_handler(statement_logger)

        enable_statement_echo()

        assert statement_logger.handlers == [first]

    def test_a_logger_with_an_application_handler_gets_no_stdout_handler(
        self,
        statement_logger: logging.Logger,
    ) -> None:
        application_handler = logging.NullHandler()
        statement_logger.addHandler(application_handler)

        enable_statement_echo()

        assert statement_logger.handlers == [application_handler]
        assert statement_logger.level == logging.INFO

    def test_keeps_propagation(self, statement_logger: logging.Logger) -> None:
        enable_statement_echo()

        assert statement_logger.propagate is True

    def test_an_info_record_reaches_stdout(
        self,
        statement_logger: logging.Logger,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # capsys replaced sys.stdout before the test, so the handler that the
        # call adds writes to the captured stream.
        enable_statement_echo()

        statement_logger.info('SELECT 1')
        statement_logger.debug('a DEBUG record does not pass INFO')

        lines = capsys.readouterr().out.splitlines()
        match lines:
            case [line]:
                assert line.endswith(' INFO horsies.sql SELECT 1')
            case _:
                raise AssertionError(f'expected one stdout line, got {lines!r}')


class TestBrokerEcho:
    def test_a_broker_with_echo_enables_the_statement_echo(
        self,
        statement_logger: logging.Logger,
    ) -> None:
        _broker(echo=True)

        assert statement_logger.level == logging.INFO
        _the_one_stdout_handler(statement_logger)

    def test_two_brokers_with_echo_add_one_handler(
        self,
        statement_logger: logging.Logger,
    ) -> None:
        _broker(echo=True)
        first = _the_one_stdout_handler(statement_logger)

        _broker(echo=True)

        assert statement_logger.handlers == [first]

    def test_a_broker_without_echo_changes_nothing(
        self,
        statement_logger: logging.Logger,
    ) -> None:
        _broker(echo=False)

        assert statement_logger.handlers == []
        assert statement_logger.level == logging.NOTSET


class TestStatementOutputByLoggingConfiguration:
    """The stderr handler of ``configure_logging`` drops ``horsies.sql``
    records while ``horsies.sql`` holds the stdout handler of
    ``enable_statement_echo``. capsys replaced ``sys.stdout`` and
    ``sys.stderr`` before each test, so the handlers that the test adds write
    to the captured streams."""

    def test_no_logging_configuration_shows_a_statement_once_on_stdout(
        self,
        statement_logger: logging.Logger,
        horsies_logger: logging.Logger,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        enable_statement_echo()

        statement_logger.info(STATEMENT_TEXT)

        captured = capsys.readouterr()
        assert _the_one_line(captured.out).endswith(ECHOED_STATEMENT_END)
        assert captured.err == ''

    @pytest.mark.parametrize(
        'setup_steps',
        [
            pytest.param(
                (_configure_logging_as_the_cli, enable_statement_echo),
                id='configure_logging-then-echo',
            ),
            pytest.param(
                (enable_statement_echo, _configure_logging_as_the_cli),
                id='echo-then-configure_logging',
            ),
        ],
    )
    def test_the_cli_shows_a_statement_once_on_stdout(
        self,
        statement_logger: logging.Logger,
        horsies_logger: logging.Logger,
        capsys: pytest.CaptureFixture[str],
        setup_steps: tuple[Callable[[], None], ...],
    ) -> None:
        for step in setup_steps:
            step()

        statement_logger.info(STATEMENT_TEXT)

        captured = capsys.readouterr()
        assert _the_one_line(captured.out).endswith(ECHOED_STATEMENT_END)
        assert captured.err == ''

    def test_the_cli_shows_other_horsies_records_on_stderr(
        self,
        statement_logger: logging.Logger,
        horsies_logger: logging.Logger,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _configure_logging_as_the_cli()
        enable_statement_echo()

        logging.getLogger('horsies.worker').info('worker started')

        captured = capsys.readouterr()
        assert captured.out == ''
        assert 'worker started' in _the_one_line(captured.err)

    def test_the_cli_shows_a_statement_on_stderr_when_horsies_sql_holds_an_application_handler(
        self,
        statement_logger: logging.Logger,
        horsies_logger: logging.Logger,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        application_handler = _RecordingHandler()
        statement_logger.addHandler(application_handler)
        _configure_logging_as_the_cli()
        enable_statement_echo()

        statement_logger.info(STATEMENT_TEXT)

        captured = capsys.readouterr()
        assert captured.out == ''
        assert STATEMENT_TEXT in _the_one_line(captured.err)
        assert [record.getMessage() for record in application_handler.records] == [
            STATEMENT_TEXT,
        ]

    @pytest.mark.parametrize(
        'application_logger_name',
        [
            pytest.param(HORSIES_LOGGER_NAME, id='horsies'),
            pytest.param(ROOT_LOGGER_NAME, id='root'),
        ],
    )
    def test_an_application_handler_gets_a_statement_shown_once_on_stdout(
        self,
        statement_logger: logging.Logger,
        horsies_logger: logging.Logger,
        capsys: pytest.CaptureFixture[str],
        application_logger_name: str,
    ) -> None:
        application_handler = _RecordingHandler()
        with _attached(logging.getLogger(application_logger_name), application_handler):
            enable_statement_echo()

            statement_logger.info(STATEMENT_TEXT)

        captured = capsys.readouterr()
        assert _the_one_line(captured.out).endswith(ECHOED_STATEMENT_END)
        assert captured.err == ''
        assert [
            (record.name, record.getMessage()) for record in application_handler.records
        ] == [(STATEMENT_LOGGER_NAME, STATEMENT_TEXT)]
