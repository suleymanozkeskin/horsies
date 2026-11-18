"""A broker built with ``echo=True`` logs the statements of its core on the
``horsies.sql`` logger at INFO.

The test enters at the broker (``PostgresBroker`` with ``echo=True``) and
reads the records on the Python logger: the manifest's ``echo``, the core's
statement log, the log bridge's route and level, and the logger's level that
the broker sets. Records reach Python from the bridge's forwarder thread
after the event, so the test waits for them up to a bound.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Iterator
from typing import Final

import pytest
from pydantic import SecretStr

from horsies._core import refresh_log_levels
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import STATEMENT_LOGGER_NAME
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import is_ok

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

# The statement that the pending expiry pass sends, and the SQL function it
# calls.
EXPIRE_STATEMENT_NAME: Final = 'expire_pending_tasks'
EXPIRE_FUNCTION_NAME: Final = 'horsies_expire_pending_tasks'
RECORD_POLLS: Final = 100
RECORD_POLL_S: Final = 0.05


class _RecordingHandler(logging.Handler):
    """Keeps every record it handles. The forwarder thread calls ``emit``."""

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self._kept: list[logging.LogRecord] = []
        self._kept_lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        with self._kept_lock:
            self._kept.append(record)

    def records(self) -> list[logging.LogRecord]:
        with self._kept_lock:
            return list(self._kept)


@pytest.fixture
def statement_records() -> Iterator[_RecordingHandler]:
    """A recording handler as the only handler of ``horsies.sql``. The
    logger has a handler, so the echo broker adds no stdout handler. The
    earlier handlers and level come back after the test."""
    logger = logging.getLogger(STATEMENT_LOGGER_NAME)
    earlier_handlers = list(logger.handlers)
    earlier_level = logger.level
    recorder = _RecordingHandler()
    logger.handlers = [recorder]
    logger.setLevel(logging.NOTSET)
    yield recorder
    logger.handlers = earlier_handlers
    logger.setLevel(earlier_level)
    refresh_log_levels()


async def _expire_statement_records(
    recorder: _RecordingHandler,
) -> list[logging.LogRecord]:
    """The records of the expiry statement, once at least one arrived; an
    empty list when none arrived within the bound."""
    for _poll in range(RECORD_POLLS):
        found = [
            record
            for record in recorder.records()
            if getattr(record, 'statement', None) == EXPIRE_STATEMENT_NAME
        ]
        match found:
            case []:
                await asyncio.sleep(RECORD_POLL_S)
            case _:
                return found
    return []


async def test_a_broker_with_echo_logs_its_core_statements_at_info_on_horsies_sql(
    broker: PostgresBroker,
    db_url: str,
    statement_records: _RecordingHandler,
) -> None:
    # `broker` installed the schema; the echo broker uses it as it is.
    echo_broker = PostgresBroker(
        PostgresConfig(database_url=SecretStr(db_url), echo=True),
        assume_initialized=True,
    )
    try:
        expired = await echo_broker.expire_pending_tasks()
        assert is_ok(expired), expired
        records = await _expire_statement_records(statement_records)
    finally:
        closed = await echo_broker.close_async()
        assert is_ok(closed), closed

    assert records, (
        f'no {EXPIRE_STATEMENT_NAME} record on {STATEMENT_LOGGER_NAME} within '
        f'{RECORD_POLLS * RECORD_POLL_S} s'
    )
    for record in records:
        assert record.name == STATEMENT_LOGGER_NAME
        assert record.levelno == logging.INFO
        assert EXPIRE_FUNCTION_NAME in record.getMessage()
    assert logging.getLogger(STATEMENT_LOGGER_NAME).handlers == [statement_records]
