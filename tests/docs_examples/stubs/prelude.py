"""Stand-in names that the doc examples use but do not define.

Every extracted file starts with ``from prelude import *``. A page that
defines a name itself shadows the stand-in. The stand-ins are typed, so an
example that uses one wrongly still fails the check.
"""

import os  # noqa: F401
from dataclasses import dataclass  # noqa: F401
from datetime import datetime, time, timedelta, timezone  # noqa: F401
from enum import Enum  # noqa: F401
from typing import Any, Generic, Literal, Mapping, Sequence  # noqa: F401

from pydantic import SecretStr

from horsies import *  # noqa: F403
from horsies import (
    AppConfig,
    Horsies,
    PostgresConfig,
)

app = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://user:pass@localhost:5432/mydb'),
        ),
    ),
)


# The examples use these names for several types (a task handle or a workflow
# handle, a task function with any parameters), so the stand-ins are `Any`.
my_task: Any
broker: Any
handle: Any
spec: Any
ctx: Any
requests: Any
