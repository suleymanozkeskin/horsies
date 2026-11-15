"""Root test configuration for horsies workflow tests."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load test environment before any test module imports
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env.test')


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line('markers', 'unit: Unit tests (no database)')
    config.addinivalue_line(
        'markers', 'integration: Integration tests (requires database)'
    )
    config.addinivalue_line('markers', 'slow: Long-running tests')


class _LibraryRefusals(logging.Handler):
    """Collects the refusals of `TaskError.data` that came from a library path."""

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, 'refusal_origin', None) == 'library':
            self.messages.append(record.getMessage())


@pytest.fixture(autouse=True)
def restore_process_role(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give the process role back after each test.

    A test that runs the worker child start in this process marks the process
    as one that runs task bodies, and result waits then fail closed. Later
    tests in the same process must see the role from before. The import is
    here so that the test environment loads before any horsies module.
    """
    from horsies.core import process_role as process_role_module

    monkeypatch.setattr(process_role_module, '_role', process_role_module.process_role())


@pytest.fixture(autouse=True)
def no_library_data_refusal(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail a test in which library code builds a `TaskError.data` that is refused.

    The code survives such a value, so nothing else would show the defect.
    """
    handler = _LibraryRefusals()
    codec_logger = logging.getLogger('horsies.codec')
    codec_logger.addHandler(handler)
    yield
    codec_logger.removeHandler(handler)
    if request.node.get_closest_marker('allow_library_data_refusal') is None:
        assert handler.messages == []
