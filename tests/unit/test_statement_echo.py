"""``PostgresConfig.echo`` shows the core's statement log as SQLAlchemy's
``echo=True`` shows its statements.

``enable_statement_echo`` sets the ``horsies.sql`` logger to INFO and adds
one stdout handler per process, and only to a logger with no handler. A
broker built with ``echo=True`` calls it; a broker built with ``echo=False``
changes nothing. The brokers here open no connection: construction does no
I/O.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from typing import Final

import pytest
from pydantic import SecretStr

from horsies._core import refresh_log_levels
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import (
    STATEMENT_ECHO_FORMAT,
    STATEMENT_LOGGER_NAME,
    enable_statement_echo,
)
from horsies.core.models.broker import PostgresConfig

pytestmark = pytest.mark.unit

# Construction opens no connection, so no server listens at this address.
UNUSED_DATABASE_URL: Final = 'postgresql://user:password@127.0.0.1:1/horsies_echo_unit'


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
