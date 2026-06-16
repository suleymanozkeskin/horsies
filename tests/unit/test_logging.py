"""Unit tests for horsies logging module."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator

import pytest

import horsies.core.logging as logging_mod
from horsies.core.logging import (
    ColoredFormatter,
    _should_use_color,
    configure_logging,
    get_logger,
)

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


# ---------------------------------------------------------------------------
# Color gating
# ---------------------------------------------------------------------------


class _FakeStream:
    def __init__(self, isatty: bool) -> None:
        self._isatty = isatty

    def isatty(self) -> bool:
        return self._isatty


def _record() -> logging.LogRecord:
    return logging.LogRecord(
        'horsies.worker', logging.INFO, '', 0, 'hello', (), None
    )


class TestShouldUseColor:
    """_should_use_color: TTY by default, NO_COLOR/FORCE_COLOR overrides."""

    def _clear_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv('FORCE_COLOR', raising=False)
        monkeypatch.delenv('NO_COLOR', raising=False)

    def test_tty_enables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_env(monkeypatch)
        assert _should_use_color(_FakeStream(isatty=True)) is True

    def test_non_tty_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_env(monkeypatch)
        assert _should_use_color(_FakeStream(isatty=False)) is False

    def test_stream_without_isatty_disables(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._clear_env(monkeypatch)
        assert _should_use_color(object()) is False

    def test_no_color_overrides_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_env(monkeypatch)
        monkeypatch.setenv('NO_COLOR', '1')
        assert _should_use_color(_FakeStream(isatty=True)) is False

    def test_no_color_empty_value_still_disables(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._clear_env(monkeypatch)
        monkeypatch.setenv('NO_COLOR', '')
        assert _should_use_color(_FakeStream(isatty=True)) is False

    def test_force_color_overrides_non_tty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._clear_env(monkeypatch)
        monkeypatch.setenv('FORCE_COLOR', '1')
        assert _should_use_color(_FakeStream(isatty=False)) is True

    def test_force_color_precedes_no_color(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('FORCE_COLOR', '1')
        monkeypatch.setenv('NO_COLOR', '1')
        assert _should_use_color(_FakeStream(isatty=False)) is True

    def test_force_color_zero_does_not_force(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._clear_env(monkeypatch)
        monkeypatch.setenv('FORCE_COLOR', '0')
        assert _should_use_color(_FakeStream(isatty=False)) is False


class TestColorToggle:
    """ColoredFormatter emits ANSI only when use_color is True."""

    def test_no_color_omits_ansi(self) -> None:
        output = ColoredFormatter(use_color=False).format(_record())
        assert '\033' not in output
        assert '[worker]' in output
        assert '[INFO]' in output
        assert 'hello' in output

    def test_color_includes_ansi(self) -> None:
        output = ColoredFormatter(use_color=True).format(_record())
        assert '\033[' in output

    def test_configure_logging_disables_color_for_non_tty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(logging_mod, '_should_use_color', lambda stream: False)
        configure_logging(logging.INFO)
        root = logging.getLogger('horsies')
        handler = next(
            h for h in root.handlers if not isinstance(h, logging.NullHandler)
        )
        assert handler.formatter is not None
        assert '\033' not in handler.formatter.format(_record())

    def test_configure_logging_enables_color_for_tty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(logging_mod, '_should_use_color', lambda stream: True)
        configure_logging(logging.INFO)
        root = logging.getLogger('horsies')
        handler = next(
            h for h in root.handlers if not isinstance(h, logging.NullHandler)
        )
        assert handler.formatter is not None
        assert '\033[' in handler.formatter.format(_record())
