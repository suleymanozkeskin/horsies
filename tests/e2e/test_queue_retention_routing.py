"""A mapped queue routes a real task into its derived partition.

The unit tests assert the key a send stamps; they build the app
themselves and never terminalize anything. This boots a worker through
the CLI, sends a task on the mapped queue, and reads the partition the
completed record actually landed in.

The partition comes from `tableoid`, which is the routing fact itself
rather than an inference from the class column: a record can carry the
right class key and still sit in the wrong relation if the derived class
was never registered.
"""

from __future__ import annotations

import os
import time

import pytest
from sqlalchemy import create_engine, text

from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks.instance_queue_retention import (
    DERIVED_CLASS_KEY,
    mapped_task,
)

pytestmark = [pytest.mark.e2e]

INSTANCE = 'tests.e2e.tasks.instance_queue_retention:app'

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ.get("DB_PASSWORD", "")}'
    '@localhost:5432/horsies',
)


def _class_is_registered() -> bool:
    """The readiness condition IS the routing's precondition."""
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as connection:
            return (
                connection.execute(
                    text(
                        'SELECT 1 FROM horsies_retention_classes '
                        'WHERE class_key = :key'
                    ),
                    {'key': DERIVED_CLASS_KEY},
                )
            ).scalar_one_or_none() is not None
    finally:
        engine.dispose()


def _history_partition(task_id: str) -> str | None:
    """The relation the completed record landed in, or None if not moved."""
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as connection:
            return (
                connection.execute(
                    text(
                        'SELECT tableoid::regclass::text '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one_or_none()
    finally:
        engine.dispose()


def _forget_derived_class() -> None:
    """Drop the derived class and everything built for it.

    Order matters: the leaf catalog carries a foreign key to the class.
    """
    engine = create_engine(DB_URL, isolation_level='AUTOCOMMIT')
    try:
        with engine.connect() as connection:
            leaves = (
                connection.execute(
                    text(
                        'SELECT leaf_name FROM '
                        'horsies_task_history_leaf_catalog '
                        'WHERE class_key = :key'
                    ),
                    {'key': DERIVED_CLASS_KEY},
                )
            ).scalars().all()
            for leaf_name in leaves:
                connection.execute(
                    text(f'DROP TABLE IF EXISTS {leaf_name} CASCADE')
                )
            connection.execute(
                text(
                    'DELETE FROM horsies_task_history_leaf_catalog '
                    'WHERE class_key = :key'
                ),
                {'key': DERIVED_CLASS_KEY},
            )
            connection.execute(
                text(
                    'DROP TABLE IF EXISTS '
                    f'horsies_task_history_{DERIVED_CLASS_KEY} CASCADE'
                )
            )
            connection.execute(
                text(
                    'DELETE FROM horsies_retention_classes '
                    'WHERE class_key = :key'
                ),
                {'key': DERIVED_CLASS_KEY},
            )
    finally:
        engine.dispose()


def test_a_mapped_queue_lands_the_record_in_its_derived_partition() -> None:
    _forget_derived_class()
    try:
        with run_worker(
            INSTANCE,
            processes=1,
            timeout=60.0,
            ready_check=_class_is_registered,
        ):
            sent = mapped_task.send(x=21)
            assert sent.is_ok(), sent
            task_id = sent.ok_value.task_id

            deadline = time.monotonic() + 60.0
            partition = None
            while time.monotonic() < deadline:
                partition = _history_partition(task_id)
                if partition is not None:
                    break
                time.sleep(0.5)

        assert partition is not None, (
            'the task never reached history; routing cannot be judged'
        )
        assert DERIVED_CLASS_KEY in partition, (
            f'the record landed in {partition!r}, not in a leaf of the '
            f'class the queue mapping derives ({DERIVED_CLASS_KEY!r})'
        )
    finally:
        _forget_derived_class()
