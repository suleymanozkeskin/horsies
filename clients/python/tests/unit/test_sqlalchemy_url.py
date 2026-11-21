"""Unit tests for the test helper tests/sqlalchemy_url.py."""

from __future__ import annotations

import pytest

from tests.sqlalchemy_url import sqlalchemy_url


@pytest.mark.unit
class TestSqlalchemyUrl:
    """The SQLAlchemy URL names the psycopg 3 driver and keeps the rest."""

    @pytest.mark.parametrize(
        'url',
        [
            'postgresql://u:p%40ss@h:5433/db?sslmode=require',
            'postgres://u:p%40ss@h:5433/db?sslmode=require',
        ],
    )
    def test_scheme_becomes_postgresql_psycopg(self, url: str) -> None:
        assert sqlalchemy_url(url) == (
            'postgresql+psycopg://u:p%40ss@h:5433/db?sslmode=require'
        )

    def test_refused_url_is_a_defect_of_the_caller(self) -> None:
        with pytest.raises(AssertionError):
            sqlalchemy_url('postgresql+psycopg://u:p@h/db')
