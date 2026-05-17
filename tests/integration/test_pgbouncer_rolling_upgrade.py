'''Rolling-upgrade advisory-lock coverage for PgBouncer split URLs.'''

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from collections.abc import Generator
from pathlib import Path

import psycopg
import pytest

from horsies.core.utils.url import to_psycopg_url
from tests.pgbouncer_utils import (
    PgbouncerUrls,
    isolated_pgbouncer_database,
    skip_if_pgbouncer_disabled,
)


skip_if_pgbouncer_disabled()

pytestmark = [pytest.mark.integration, pytest.mark.pgbouncer]


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def pgbouncer_urls() -> Generator[PgbouncerUrls, None, None]:
    with isolated_pgbouncer_database("horsies_pgbouncer_rolling") as urls:
        yield urls


def _wait_for_marker(direct_url: str, label: str, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with psycopg.connect(to_psycopg_url(direct_url), autocommit=True) as conn:
            row = conn.execute(
                "SELECT 1 FROM pgbouncer_lock_probe WHERE label = %s",
                (label,),
            ).fetchone()
            if row is not None:
                return
        time.sleep(0.05)
    raise TimeoutError(f"marker {label!r} was not observed")


def _marker_exists(direct_url: str, label: str) -> bool:
    with psycopg.connect(to_psycopg_url(direct_url), autocommit=True) as conn:
        row = conn.execute(
            "SELECT 1 FROM pgbouncer_lock_probe WHERE label = %s",
            (label,),
        ).fetchone()
        return row is not None


def _child_env(pgbouncer_urls: PgbouncerUrls) -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["DIRECT_URL"] = pgbouncer_urls.direct
    env["POOLED_URL"] = pgbouncer_urls.transaction
    return env


def test_split_url_schema_lock_blocks_legacy_direct_url_initializer(
    pgbouncer_urls: PgbouncerUrls,
) -> None:
    with psycopg.connect(to_psycopg_url(pgbouncer_urls.direct), autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE pgbouncer_lock_probe (
                label text PRIMARY KEY,
                observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """
        )

    old_code = textwrap.dedent(
        """
        import os
        import time

        import psycopg

        from horsies.core.brokers.postgres import PostgresBroker
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.utils.url import to_psycopg_url

        direct_url = os.environ['DIRECT_URL']
        broker = PostgresBroker(PostgresConfig(database_url=direct_url))
        key = broker._legacy_schema_advisory_key()

        with psycopg.connect(to_psycopg_url(direct_url), autocommit=False) as lock_conn:
            with psycopg.connect(to_psycopg_url(direct_url), autocommit=True) as signal_conn:
                with lock_conn.transaction():
                    lock_conn.execute('SELECT pg_advisory_xact_lock(%s)', (key,))
                    signal_conn.execute(
                        "INSERT INTO pgbouncer_lock_probe (label) VALUES ('old_locked')"
                    )
                    time.sleep(2.0)
                signal_conn.execute(
                    "INSERT INTO pgbouncer_lock_probe (label) VALUES ('old_released')"
                )
        """
    )
    new_code = textwrap.dedent(
        """
        import asyncio
        import os

        import psycopg

        from horsies.core.brokers import postgres as pgmod
        from horsies.core.brokers.postgres import PostgresBroker
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.utils.url import to_psycopg_url

        direct_url = os.environ['DIRECT_URL']
        pooled_url = os.environ['POOLED_URL']

        def probe_create_all(_sync_conn):
            with psycopg.connect(to_psycopg_url(direct_url), autocommit=True) as conn:
                conn.execute(
                    "INSERT INTO pgbouncer_lock_probe (label) VALUES ('new_entered')"
                )
            raise RuntimeError('probe-stop')

        pgmod.Base.metadata.create_all = probe_create_all

        broker = PostgresBroker(
            PostgresConfig(
                database_url=pooled_url,
                session_database_url=direct_url,
                pgbouncer_transaction_mode=True,
            )
        )
        try:
            asyncio.run(broker._ensure_initialized())
        except RuntimeError as exc:
            if 'probe-stop' not in str(exc):
                raise
        """
    )

    old_proc = subprocess.Popen(
        [sys.executable, "-c", old_code],
        env=_child_env(pgbouncer_urls),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_marker(pgbouncer_urls.direct, "old_locked")

        new_proc = subprocess.Popen(
            [sys.executable, "-c", new_code],
            env=_child_env(pgbouncer_urls),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            time.sleep(0.5)
            assert not _marker_exists(pgbouncer_urls.direct, "new_entered")

            old_stdout, old_stderr = old_proc.communicate(timeout=10.0)
            assert old_proc.returncode == 0, f"old stdout={old_stdout}\nstderr={old_stderr}"

            new_stdout, new_stderr = new_proc.communicate(timeout=10.0)
            assert new_proc.returncode == 0, f"new stdout={new_stdout}\nstderr={new_stderr}"
        finally:
            if new_proc.poll() is None:
                new_proc.kill()
    finally:
        if old_proc.poll() is None:
            old_proc.kill()

    with psycopg.connect(to_psycopg_url(pgbouncer_urls.direct), autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT label, observed_at
            FROM pgbouncer_lock_probe
            WHERE label IN ('old_released', 'new_entered')
            """
        ).fetchall()
    markers = {str(label): observed_at for label, observed_at in rows}
    assert markers["new_entered"] >= markers["old_released"]
