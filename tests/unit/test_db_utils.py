"""Unit tests for horsies.core.utils.db error classification helpers."""

from __future__ import annotations

import pytest

from psycopg import InterfaceError, OperationalError
from sqlalchemy.exc import DBAPIError, OperationalError as SAOperationalError

from horsies.core.utils.db import is_dbapi_disconnect, is_retryable_connection_error


def _make_dbapi_error(
    *,
    connection_invalidated: bool = False,
    is_disconnect: bool = False,
) -> DBAPIError:
    exc = DBAPIError(
        statement='SELECT 1',
        params=None,
        orig=Exception('test'),
        connection_invalidated=connection_invalidated,
    )
    if is_disconnect:
        exc.is_disconnect = is_disconnect  # type: ignore[reportAttributeAccessIssue]
    return exc


@pytest.mark.unit
class TestIsDBAPIDisconnect:
    """Tests for is_dbapi_disconnect."""

    def test_connection_invalidated_true(self) -> None:
        exc = _make_dbapi_error(connection_invalidated=True)
        assert is_dbapi_disconnect(exc) is True

    def test_is_disconnect_true(self) -> None:
        exc = _make_dbapi_error(is_disconnect=True)
        assert is_dbapi_disconnect(exc) is True

    def test_both_flags_set(self) -> None:
        exc = _make_dbapi_error(connection_invalidated=True, is_disconnect=True)
        assert is_dbapi_disconnect(exc) is True

    def test_neither_flag_set(self) -> None:
        exc = _make_dbapi_error()
        assert is_dbapi_disconnect(exc) is False


@pytest.mark.unit
class TestIsRetryableConnectionError:
    """Tests for is_retryable_connection_error."""

    def test_psycopg_operational_error(self) -> None:
        assert is_retryable_connection_error(OperationalError('connection refused')) is True

    def test_psycopg_interface_error(self) -> None:
        assert is_retryable_connection_error(InterfaceError('broken pipe')) is True

    def test_sqlalchemy_operational_error(self) -> None:
        exc = SAOperationalError('lost connection', {}, Exception('orig'))
        assert is_retryable_connection_error(exc) is True

    def test_dbapi_error_with_disconnect(self) -> None:
        exc = _make_dbapi_error(connection_invalidated=True)
        assert is_retryable_connection_error(exc) is True

    def test_dbapi_error_with_is_disconnect_only(self) -> None:
        exc = _make_dbapi_error(is_disconnect=True)
        assert is_retryable_connection_error(exc) is True

    def test_dbapi_error_without_disconnect(self) -> None:
        exc = _make_dbapi_error()
        assert is_retryable_connection_error(exc) is False

    def test_sa_operational_error_not_invalidated_still_retryable(self) -> None:
        exc = SAOperationalError('conn lost', {}, Exception('orig'))
        exc.connection_invalidated = False
        assert is_retryable_connection_error(exc) is True

    def test_unrelated_exception_not_retryable(self) -> None:
        assert is_retryable_connection_error(ValueError('bad value')) is False

    def test_runtime_error_not_retryable(self) -> None:
        assert is_retryable_connection_error(RuntimeError('something')) is False

    def test_connection_refused_error_not_retryable(self) -> None:
        assert is_retryable_connection_error(ConnectionRefusedError()) is False

    def test_os_error_not_retryable(self) -> None:
        assert is_retryable_connection_error(OSError('timeout')) is False

    def test_base_exception_not_retryable(self) -> None:
        assert is_retryable_connection_error(KeyboardInterrupt()) is False

    def test_empty_message_psycopg_operational_error(self) -> None:
        assert is_retryable_connection_error(OperationalError('')) is True
