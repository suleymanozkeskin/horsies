"""App-owned SQLAlchemy engine, mimicking an adopter's app/databases/postgres.py.

This is a MODULE-LEVEL QueuePool engine that is entirely separate from horsies'
own broker pool. It is tagged with a distinct ``application_name`` so its
connections are attributable in ``pg_stat_activity`` (everything horsies opens
uses the default application_name).

Connection lifecycle across worker children:
  - spawn (macOS default): each child re-imports this module and builds its OWN
    pool. Nothing is inherited from the parent.
  - fork (Linux default): each child inherits this engine object (and any open
    connections in its pool) from the parent.

Either way QueuePool has no idle-eviction: connections opened during a burst are
retained for the life of the child process and only closed on recycle-at-checkout
or process exit. That retention is what this repro measures.
"""

from __future__ import annotations

import os

from sqlalchemy import Engine, create_engine

APP_ENGINE_APPLICATION_NAME = 'repro_app_engine'

_DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ.get("DB_PASSWORD", "")}@localhost:5432/horsies',
)

# Small pool so per-child accumulation is easy to read in pg_stat_activity.
app_engine: Engine = create_engine(
    _DB_URL,
    pool_size=3,
    max_overflow=2,
    pool_recycle=1800,
    connect_args={'application_name': APP_ENGINE_APPLICATION_NAME},
)
