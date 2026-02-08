"""Unit tests for horsies logging module."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

from horsies.core.logging import get_logger, set_default_level

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _restore_default_level() -> Iterator[None]:
    """Save and restore the module-level _default_level after each test."""
    from horsies.core import logging as horsies_logging

    original = horsies_logging._default_level
    yield
    set_default_level(original)


class TestSetDefaultLevel:
    """Tests for set_default_level()."""

    def test_changes_module_variable(self) -> None:
        from horsies.core import logging as horsies_logging

        set_default_level(logging.DEBUG)
        assert horsies_logging._default_level == logging.DEBUG

        set_default_level(logging.WARNING)
        assert horsies_logging._default_level == logging.WARNING

    def test_new_logger_uses_default_level(self) -> None:
        import uuid

        set_default_level(logging.WARNING)

        unique_name = f'test_{uuid.uuid4().hex[:8]}'
        logger = get_logger(unique_name)

        assert logger.level == logging.WARNING

    def test_logger_handler_respects_default_level(self) -> None:
        import uuid

        set_default_level(logging.ERROR)

        unique_name = f'test_{uuid.uuid4().hex[:8]}'
        logger = get_logger(unique_name)

        assert len(logger.handlers) > 0
        for handler in logger.handlers:
            assert handler.level == logging.ERROR
