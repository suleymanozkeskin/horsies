"""Root test configuration for horsies workflow tests."""

from __future__ import annotations

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
