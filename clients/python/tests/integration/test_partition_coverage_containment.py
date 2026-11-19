"""One unusable class does not cost the other classes their coverage.

The pass reads registered classes from the database in key order. A class
whose name arithmetic fails raised out of the loop, so every class sorting
after it silently stopped getting leaves — and because the trigger is a
registered row rather than a declaration, removing the declaration could
not clear it.

On the baseline a class that cannot be named cannot be stored: CHECK
`horsies_retention_classes_kind_shape` holds every period key to
`standard_<days>d`. The failures that remain reachable are a class whose
next leaf name is taken (a returned refusal) and a class whose parent
relation an operator dropped (a database error).

Coverage is idempotent, so a pass proves it served a class only when that
pass owes the class a leaf. Each count-based test registers its own passing
class. The premise is therefore independent of definition order, earlier
suite runs against the same database, and parallel collection.
"""

from __future__ import annotations

from typing import Final

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
)
from tests.broker_resources_support import current_resources

RETENTION_CLASSES: Final = 'horsies_retention_classes'
"""The class table of the baseline."""

MAX_RETENTION_CLASS_KEY_LENGTH: Final = 18
"""The longest class key whose daily leaves and their indexes have names
within the 63 bytes of PostgreSQL (the core's ``MAX_RETENTION_CLASS_KEY_LENGTH``)."""

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# 35 characters: past the length at which `daily_leaf_name` raises. The
# baseline refuses to store it.
_UNUSABLE_KEY = 'a' + 'k' * 34
# Period keys of the baseline form. The refusing class sorts BEFORE the
# default class (standard_30d) and the survivor sorts after both, so the
# survivor is reached only if the loop survives the refusing class.
_REFUSING_KEY = 'standard_1000d'
_RETURN_SURVIVOR_KEY = 'standard_9d'


async def test_the_keys_sort_in_the_order_the_tests_rest_on() -> None:
    """Guard the premise: the pass reaches the survivor after the refusal."""
    assert _REFUSING_KEY < DEFAULT_RETENTION_CLASS_KEY < _RETURN_SURVIVOR_KEY


async def test_the_database_refuses_a_class_that_cannot_be_named(
    broker: PostgresBroker,
) -> None:
    """The planted row the pass once had to survive cannot be stored."""
    assert len(_UNUSABLE_KEY) > MAX_RETENTION_CLASS_KEY_LENGTH
    with pytest.raises(IntegrityError) as refused:
        async with current_resources(broker).engine.begin() as connection:
            await connection.execute(
                text(
                    f'INSERT INTO {RETENTION_CLASSES} ('
                    'class_key, class_kind, retention_days, '
                    'heartbeat_horizon_hours, created_at) VALUES ('
                    ":key, 'PERIOD', 5, NULL, statement_timestamp())"
                ),
                {'key': _UNUSABLE_KEY},
            )
    orig = refused.value.orig
    assert isinstance(orig, psycopg.errors.CheckViolation), orig
    assert orig.diag.constraint_name == 'horsies_retention_classes_kind_shape'


# --- The offset-1 survivor: the only shape that sees the scope fix --------
#
# Rolling a savepoint back and releasing it are observationally identical
# when the class created nothing before refusing. Every other trigger in
# this file refuses at offset 0 or raises first, so none of them can tell
# the two apart. This one makes the class CREATE and THEN refuse.


