"""Unit tests for horsies.core.utils.url helpers."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from horsies.core.utils.url import mask_database_url, to_psycopg_url


@pytest.mark.unit
class TestMaskDatabaseUrl:
    """Tests for mask_database_url: normal path and fallback."""

    # --- Normal urlparse path ---

    def test_masks_password(self) -> None:
        """Standard URL password should be masked."""
        url = 'postgresql+psycopg://user:s3cret@localhost:5432/mydb'
        masked = mask_database_url(url)
        assert 's3cret' not in masked
        assert '***' in masked
        assert 'user' in masked
        assert 'localhost' in masked
        assert 'mydb' in masked

    def test_no_password_unchanged(self) -> None:
        """URL without password should be returned as-is."""
        url = 'postgresql+psycopg://localhost/db'
        assert mask_database_url(url) == url

    def test_preserves_port_and_path(self) -> None:
        """Port and database path should survive masking."""
        url = 'postgresql+psycopg://admin:pw@db.host:5433/prod'
        masked = mask_database_url(url)
        assert ':5433' in masked
        assert '/prod' in masked
        assert 'pw' not in masked

    def test_preserves_query_params(self) -> None:
        """Query parameters should survive masking."""
        url = 'postgresql+psycopg://u:p@host/db?sslmode=require'
        masked = mask_database_url(url)
        assert 'sslmode=require' in masked
        assert 'p' not in masked.split('@')[0].split(':')[-1]

    # --- Fallback path (urlparse raises) ---

    def test_fallback_with_at_sign(self) -> None:
        """When urlparse fails, fallback should mask password in URL containing @."""
        url = 'weird://user:secret@host/db'
        with patch('horsies.core.utils.url.urlparse', side_effect=Exception('parse failed')):
            masked = mask_database_url(url)
        assert 'secret' not in masked
        assert '***' in masked
        assert 'host/db' in masked

    def test_fallback_without_at_sign(self) -> None:
        """When urlparse fails and URL has no @, return as-is."""
        url = 'no-credentials-here'
        with patch('horsies.core.utils.url.urlparse', side_effect=Exception('parse failed')):
            masked = mask_database_url(url)
        assert masked == url

    def test_fallback_preserves_scheme_and_user(self) -> None:
        """Fallback masking should keep scheme and username."""
        url = 'pg://admin:topsecret@db.example.com:5432/production'
        with patch('horsies.core.utils.url.urlparse', side_effect=Exception('boom')):
            masked = mask_database_url(url)
        assert masked.startswith('pg://admin:***@')
        assert 'db.example.com:5432/production' in masked
        assert 'topsecret' not in masked


@pytest.mark.unit
class TestToPsycopgUrl:
    """Tests for SQLAlchemy URL normalization to psycopg URLs."""

    def test_converts_postgresql_psycopg_scheme(self) -> None:
        url = 'postgresql+psycopg://user:pass@localhost:5432/mydb?sslmode=require'
        assert to_psycopg_url(url) == 'postgresql://user:pass@localhost:5432/mydb?sslmode=require'

    def test_converts_postgresql_asyncpg_scheme(self) -> None:
        url = 'postgresql+asyncpg://user:pass@localhost:5432/mydb'
        assert to_psycopg_url(url) == 'postgresql://user:pass@localhost:5432/mydb'

    def test_leaves_already_normalized_url_unchanged(self) -> None:
        url = 'postgresql://user:pass@localhost/db'
        assert to_psycopg_url(url) == url

    def test_leaves_non_postgres_url_unchanged(self) -> None:
        url = 'sqlite:///tmp/test.db'
        assert to_psycopg_url(url) == url

    def test_fallback_rewrite_when_urlparse_fails(self) -> None:
        url = 'postgresql+psycopg://u:p@h/db'
        with patch('horsies.core.utils.url.urlparse', side_effect=Exception('parse failed')):
            assert to_psycopg_url(url) == 'postgresql://u:p@h/db'
