"""Task that exercises the app-owned engine, holding a connection across slow I/O.

Standing in for the adopter's scraper tasks: each invocation checks out a
connection from the app-owned pool and holds it for the duration of a slow
query (``pg_sleep``), then returns it to the pool. Returned connections are
retained idle by QueuePool, which is what produces the post-burst floor the
repro measures.
"""

from __future__ import annotations

from sqlalchemy import text

from horsies.core.models.tasks import TaskError, TaskResult

from tests.spike.conn_accum.repro_app import app
from tests.spike.conn_accum.repro_db import app_engine


@app.task(task_name='repro_app_db_task')
def app_db_task(*, sleep_ms: int) -> TaskResult[str, TaskError]:
    """Check out the app-owned engine and hold the connection across a slow query."""
    with app_engine.connect() as conn:
        conn.execute(text('SELECT pg_sleep(:s)'), {'s': sleep_ms / 1000.0})
    return TaskResult(ok=f'ok:{sleep_ms}')
