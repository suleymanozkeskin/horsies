"""The client runs no Python listener, resource set or SQLAlchemy engine.

- ``import horsies`` loads neither ``horsies.core.brokers.listener`` nor
  ``horsies.core.brokers.resources``, and neither module exists.
- A web process builds no SQLAlchemy engine: the monitoring app, an API
  request and the schema probe run on the core.

Each check runs in a new interpreter, so the modules and the engines of
other tests do not count. The broker URL has no server behind it: a read
that reaches the database fails at once.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from typing import Final

import pytest

pytestmark = [pytest.mark.unit]

SUBPROCESS_TIMEOUT_S: Final = 120.0
"""The bound of one check interpreter: its imports, and reads that fail at
once on a URL with no server."""

REMOVED_MODULES: Final = (
    'horsies.core.brokers.listener',
    'horsies.core.brokers.resources',
)

UNREACHABLE_URL: Final = 'postgresql://postgres:none@127.0.0.1:1/none'
"""Nothing listens on port 1, so every connect is refused at once."""


def _run_check(source: str) -> dict[str, object]:
    """Run ``source`` in a new interpreter and read the JSON object that it
    prints last."""
    completed = subprocess.run(
        [sys.executable, '-c', source],
        capture_output=True,
        text=True,
        check=False,
        timeout=SUBPROCESS_TIMEOUT_S,
    )
    assert completed.returncode == 0, completed.stderr
    answer: dict[str, object] = json.loads(completed.stdout.strip().splitlines()[-1])
    return answer


IMPORT_CHECK: Final = textwrap.dedent(
    f"""
    import importlib.util
    import json
    import sys

    import horsies

    removed = {REMOVED_MODULES!r}
    print(json.dumps({{
        'loaded': sorted(name for name in removed if name in sys.modules),
        'found': sorted(
            name for name in removed if importlib.util.find_spec(name) is not None
        ),
    }}))
    """,
)


def test_import_horsies_loads_neither_the_listener_nor_the_resource_sets() -> None:
    answer = _run_check(IMPORT_CHECK)

    assert answer == {'loaded': [], 'found': []}


WEB_PROCESS_CHECK: Final = textwrap.dedent(
    f"""
    import asyncio
    import json

    from sqlalchemy.engine import base as engine_base

    built = []
    engine_init = engine_base.Engine.__init__

    def recording_init(self, *args, **kwargs):
        built.append(type(self).__name__)
        engine_init(self, *args, **kwargs)

    engine_base.Engine.__init__ = recording_init

    from httpx import ASGITransport, AsyncClient
    from pydantic import SecretStr

    from horsies.core.app import Horsies
    from horsies.core.models.app import AppConfig
    from horsies.core.models.broker import PostgresConfig
    from horsies.web import AllowAll, create_monitoring_app
    from horsies.web.schema import SchemaProbe

    async def serve():
        app = Horsies(AppConfig(database=PostgresConfig(
            database_url=SecretStr({UNREACHABLE_URL!r}),
        )))
        web = create_monitoring_app(app, auth_policy=AllowAll())
        transport = ASGITransport(app=web)
        async with AsyncClient(transport=transport, base_url='http://test') as client:
            meta = await client.get('/api/meta')
        status = await SchemaProbe(app.get_broker()).status()
        _ = await app.get_broker().close_async()
        return meta.status_code, status.state.value

    meta_status, schema_state = asyncio.run(serve())
    print(json.dumps({{
        'engines': built,
        'meta_status': meta_status,
        'schema_state': schema_state,
    }}))
    """,
)


def test_a_web_process_builds_no_sqlalchemy_engine() -> None:
    answer = _run_check(WEB_PROCESS_CHECK)

    assert answer['engines'] == []
    assert answer['meta_status'] == 200
    assert answer['schema_state'] == 'UNKNOWN'
