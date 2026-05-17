'''Manual schema-version race probe for PgBouncer split URLs.

This script is intentionally destructive: with --drop-schema it drops all
Horsies-owned tables before spawning concurrent schema initializers. Use it
only against a disposable database.

Example:
    DATABASE_URL="postgresql+psycopg://...pooler..." \
    SESSION_DATABASE_URL="postgresql+psycopg://...direct..." \
    uv run python tests/manual/pgbouncer_schema_version_race.py --drop-schema
'''

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg

from horsies.core.schemas.migrations import SCHEMA_VERSION
from horsies.core.utils.url import to_psycopg_url


REPO_ROOT = Path(__file__).resolve().parents[2]


HORSIES_TABLES = (
    'horsies_task_attempts',
    'horsies_heartbeats',
    'horsies_worker_states',
    'horsies_schedule_state',
    'horsies_workflow_tasks',
    'horsies_workflows',
    'horsies_tasks',
    'horsies_schema_version',
)


def _safe_endpoint(url: str) -> str:
    parsed = urlparse(url)
    netloc = parsed.hostname or ''
    if parsed.port is not None:
        netloc = f'{netloc}:{parsed.port}'
    return urlunparse((parsed.scheme, netloc, parsed.path, '', parsed.query, ''))


def _drop_horsies_objects(direct_url: str) -> None:
    table_list = ', '.join(HORSIES_TABLES)
    with psycopg.connect(to_psycopg_url(direct_url), autocommit=True) as conn:
        conn.execute(f'DROP TABLE IF EXISTS {table_list} CASCADE')


def _schema_state(direct_url: str) -> tuple[list[tuple[int]], list[tuple[str]]]:
    with psycopg.connect(to_psycopg_url(direct_url), autocommit=True) as conn:
        version_exists = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = 'horsies_schema_version'
                  AND c.relkind = 'r'
            )
            """
        ).fetchone()[0]
        versions: list[tuple[int]] = []
        if version_exists:
            versions = conn.execute(
                'SELECT version FROM horsies_schema_version ORDER BY version'
            ).fetchall()
        rels = conn.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname LIKE 'horsies_%'
              AND c.relkind = 'r'
            ORDER BY c.relname
            """
        ).fetchall()
        return versions, rels


CHILD_CODE = textwrap.dedent(
    r'''
    from __future__ import annotations

    import asyncio
    import os
    import sys
    from types import MethodType

    sys.path.insert(0, os.environ["REPO_ROOT"])

    from horsies.core.brokers import postgres as pgmod
    from horsies.core.brokers.postgres import PostgresBroker
    from horsies.core.models.broker import PostgresConfig
    from horsies.core.models.task_pg import Base
    from horsies.core.schemas.migrations import SCHEMA_VERSION


    name = os.environ["PROBE_NAME"]
    state = {"ran_ddl": False, "double_check_version": None}

    original_create_all = Base.metadata.create_all


    def traced_create_all(bind, *args, **kwargs):
        state["ran_ddl"] = True
        print(f"{name}:running_ddl", flush=True)
        return original_create_all(bind, *args, **kwargs)


    async def forced_fast_read_zero(_self, _engine):
        print(f"{name}:fast_read_forced_0", flush=True)
        return 0


    async def main():
        Base.metadata.create_all = traced_create_all
        broker = PostgresBroker(
            PostgresConfig(
                database_url=os.environ["DATABASE_URL"],
                session_database_url=os.environ["SESSION_DATABASE_URL"],
                pgbouncer_transaction_mode=True,
            )
        )

        original_read_schema_version = broker._read_schema_version

        async def traced_read_schema_version(self, conn):
            version = await original_read_schema_version(conn)
            state["double_check_version"] = version
            print(f"{name}:double_check_version={version}", flush=True)
            return version

        broker._read_schema_version_if_exists = MethodType(forced_fast_read_zero, broker)
        broker._read_schema_version = MethodType(traced_read_schema_version, broker)

        result = await broker.ensure_schema_initialized()
        if (
            result.is_ok()
            and state["double_check_version"] is not None
            and state["double_check_version"] >= SCHEMA_VERSION
            and not state["ran_ddl"]
        ):
            print(f"{name}:double_check_exit", flush=True)

        print(
            f"{name}:result={'OK' if result.is_ok() else result.err_value.message}",
            flush=True,
        )
        await broker.close_async()
        Base.metadata.create_all = original_create_all


    asyncio.run(main())
    '''
)


def _run_probe(database_url: str, session_database_url: str, processes: int) -> str:
    env_base = os.environ.copy()
    env_base.update(
        {
            'DATABASE_URL': database_url,
            'SESSION_DATABASE_URL': session_database_url,
            'PGBOUNCER_TRANSACTION_MODE': '1',
            'PYTHONPATH': str(REPO_ROOT),
            'REPO_ROOT': str(REPO_ROOT),
        }
    )

    procs: list[subprocess.Popen[str]] = []
    for index in range(processes):
        env = dict(env_base)
        env['PROBE_NAME'] = f'p{index + 1}'
        procs.append(
            subprocess.Popen(
                [sys.executable, '-c', CHILD_CODE],
                cwd=str(REPO_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        )

    combined = ''
    for proc in procs:
        out, err = proc.communicate(timeout=120)
        combined += out + err
        print(out, end='')
        if err:
            print(err, end='')
        if proc.returncode != 0:
            raise RuntimeError(f'probe child exited with {proc.returncode}')
    return combined


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Verify concurrent schema init uses one DDL runner and locked double-check exits.'
    )
    parser.add_argument(
        '--database-url',
        default=os.environ.get('DATABASE_URL'),
        help='Runtime database URL, usually the PgBouncer transaction-pool URL.',
    )
    parser.add_argument(
        '--session-database-url',
        default=os.environ.get('SESSION_DATABASE_URL'),
        help='Direct/session database URL used for schema init and LISTEN.',
    )
    parser.add_argument(
        '--processes',
        type=int,
        default=4,
        help='Number of concurrent schema initializers to spawn.',
    )
    parser.add_argument(
        '--drop-schema',
        action='store_true',
        help='Required safety acknowledgement: drop all Horsies-owned tables first.',
    )
    args = parser.parse_args()

    if not args.database_url or not args.session_database_url:
        parser.error('database URLs are required via args or DATABASE_URL/SESSION_DATABASE_URL')
    if not args.drop_schema:
        parser.error('--drop-schema is required because this script is destructive')
    if args.processes < 2:
        parser.error('--processes must be at least 2')

    print('probe endpoints:')
    print(f'  runtime: {_safe_endpoint(args.database_url)}')
    print(f'  session: {_safe_endpoint(args.session_database_url)}')

    _drop_horsies_objects(args.session_database_url)
    before_versions, before_rels = _schema_state(args.session_database_url)
    print('before_versions:', before_versions)
    print('before_rels:', before_rels)

    output = _run_probe(
        args.database_url,
        args.session_database_url,
        args.processes,
    )

    after_versions, after_rels = _schema_state(args.session_database_url)
    print('after_versions:', after_versions)
    print('after_rels:', after_rels)

    summary = {
        'running_ddl': output.count(':running_ddl'),
        'double_check_exit': output.count(':double_check_exit'),
        'ok': output.count(':result=OK'),
    }
    print('summary:', summary)

    expected_versions = [(SCHEMA_VERSION,)]
    if after_versions != expected_versions:
        raise AssertionError(f'expected schema versions {expected_versions}, got {after_versions}')
    if summary['running_ddl'] != 1:
        raise AssertionError(f"expected exactly one DDL runner, got {summary['running_ddl']}")
    if summary['double_check_exit'] != args.processes - 1:
        raise AssertionError(
            f"expected {args.processes - 1} double-check exits, got {summary['double_check_exit']}"
        )
    if summary['ok'] != args.processes:
        raise AssertionError(f"expected {args.processes} successful processes, got {summary['ok']}")

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
