"""Unit tests for horsies logging module."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator

import pytest

from horsies.core.logging import ColoredFormatter, configure_logging, get_logger

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _clean_horsies_root_logger() -> Iterator[None]:
    """Restore the horsies root logger to NullHandler-only after each test."""
    root = logging.getLogger('horsies')
    original_handlers = list(root.handlers)
    original_level = root.level
    yield
    root.handlers = original_handlers
    root.setLevel(original_level)


# ---------------------------------------------------------------------------
# get_logger
# ---------------------------------------------------------------------------


class TestGetLogger:
    """get_logger returns a bare logger under the horsies namespace."""

    def test_returns_namespaced_logger(self) -> None:
        logger = get_logger('broker')
        assert logger.name == 'horsies.broker'

    def test_no_handlers_attached(self) -> None:
        name = f'test_{uuid.uuid4().hex[:8]}'
        logger = get_logger(name)
        assert logger.handlers == []

    def test_propagate_is_true(self) -> None:
        name = f'test_{uuid.uuid4().hex[:8]}'
        logger = get_logger(name)
        assert logger.propagate is True

    def test_same_logger_returned_on_repeated_calls(self) -> None:
        name = f'test_{uuid.uuid4().hex[:8]}'
        first = get_logger(name)
        second = get_logger(name)
        assert first is second


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    """configure_logging sets up the horsies root logger for CLI use."""

    def test_attaches_handler_to_root(self) -> None:
        configure_logging(logging.INFO)
        root = logging.getLogger('horsies')
        non_null = [h for h in root.handlers if not isinstance(h, logging.NullHandler)]
        assert len(non_null) == 1

    def test_handler_uses_colored_formatter(self) -> None:
        configure_logging(logging.INFO)
        root = logging.getLogger('horsies')
        non_null = [h for h in root.handlers if not isinstance(h, logging.NullHandler)]
        assert isinstance(non_null[0].formatter, ColoredFormatter)

    def test_sets_level_on_root(self) -> None:
        configure_logging(logging.DEBUG)
        root = logging.getLogger('horsies')
        assert root.level == logging.DEBUG

    def test_handler_writes_to_stderr(self) -> None:
        import sys

        configure_logging(logging.INFO)
        root = logging.getLogger('horsies')
        non_null = [h for h in root.handlers if not isinstance(h, logging.NullHandler)]
        assert isinstance(non_null[0], logging.StreamHandler)
        assert non_null[0].stream is sys.stderr

    def test_clears_previous_handlers(self) -> None:
        configure_logging(logging.INFO)
        configure_logging(logging.DEBUG)
        root = logging.getLogger('horsies')
        # Should have exactly one handler (the new one), not two
        assert len(root.handlers) == 1

    def test_child_logger_propagates_to_configured_root(self) -> None:
        configure_logging(logging.WARNING)
        child = get_logger(f'test_{uuid.uuid4().hex[:8]}')
        # Child has no handlers but effective level comes from parent
        assert child.getEffectiveLevel() == logging.WARNING


# ---------------------------------------------------------------------------
# ColoredFormatter
# ---------------------------------------------------------------------------


class TestColoredFormatter:
    """ColoredFormatter produces the expected tabular colored output."""

    def test_format_contains_component_and_level(self) -> None:
        formatter = ColoredFormatter()
        record = logging.LogRecord(
            name='horsies.broker',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='test message',
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        assert '[broker]' in output
        assert '[INFO]' in output
        assert 'test message' in output

    def test_format_includes_exception_info(self) -> None:
        formatter = ColoredFormatter()
        try:
            raise ValueError('boom')
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name='horsies.worker',
            level=logging.ERROR,
            pathname='',
            lineno=0,
            msg='failure',
            args=(),
            exc_info=exc_info,
        )
        output = formatter.format(record)
        assert 'ValueError' in output
        assert 'boom' in output
