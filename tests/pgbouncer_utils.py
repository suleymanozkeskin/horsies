'''Shared helpers for PgBouncer contract tests.'''

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from uuid import uuid4
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
from psycopg import sql

from horsies.core.utils.url import to_psycopg_url


@dataclass(frozen=True)
class PgbouncerUrls:
    db_name: str
    direct: str
    transaction: str
    prepared: str
    session: str
    statement: str


def skip_if_pgbouncer_disabled() -> None:
    if os.environ.get("HORSIES_PGBOUNCER_TEST") != "1":
        pytest.skip(
            "PgBouncer contract tests require HORSIES_PGBOUNCER_TEST=1",
            allow_module_level=True,
        )


def _default_url(port: int, database: str = "horsies") -> str:
    password = os.environ.get("DB_PASSWORD", "testpassword")
    return f"postgresql+psycopg://postgres:{password}@localhost:{port}/{database}"


def _replace_database(url: str, database: str) -> str:
    parsed = urlparse(url)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            f"/{database}",
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )


def _template_urls() -> tuple[str, str, str, str, str]:
    return (
        os.environ.get("HORSIES_TEST_DATABASE_URL_DIRECT", _default_url(15432)),
        os.environ.get("HORSIES_TEST_DATABASE_URL_TRANSACTION", _default_url(16432)),
        os.environ.get("HORSIES_TEST_DATABASE_URL_PREPARED", _default_url(16435)),
        os.environ.get("HORSIES_TEST_DATABASE_URL_SESSION", _default_url(16433)),
        os.environ.get("HORSIES_TEST_DATABASE_URL_STATEMENT", _default_url(16434)),
    )


def _admin_url() -> str:
    direct, _, _, _, _ = _template_urls()
    return _replace_database(direct, "postgres")


@contextmanager
def isolated_pgbouncer_database(
    prefix: str = "horsies_pgbouncer",
) -> Generator[PgbouncerUrls, None, None]:
    '''Create a per-test Postgres database and return URLs for every pool mode.'''
    (
        direct_template,
        tx_template,
        prepared_template,
        session_template,
        statement_template,
    ) = _template_urls()
    db_name = f"{prefix}_{uuid4().hex}"
    admin_url = to_psycopg_url(_admin_url())

    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    urls = PgbouncerUrls(
        db_name=db_name,
        direct=_replace_database(direct_template, db_name),
        transaction=_replace_database(tx_template, db_name),
        prepared=_replace_database(prepared_template, db_name),
        session=_replace_database(session_template, db_name),
        statement=_replace_database(statement_template, db_name),
    )

    try:
        yield urls
    finally:
        with psycopg.connect(admin_url, autocommit=True) as conn:
            conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                AND pid <> pg_backend_pid()
                """,
                (db_name,),
            )
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
