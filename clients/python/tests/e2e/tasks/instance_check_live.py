"""App instance of the ``horsies check --live`` process tests
(``tests/e2e/test_check_live_command.py``).

The database URL comes from ``CHECK_LIVE_URL_ENV_NAME`` at import, so each
test gives its own URL to the ``horsies check`` process. The app registers no
task and has the e2e worker profiles: the live check also compares each
profile with the connection limit of the server. Only the ``horsies check``
process imports this module.
"""

from __future__ import annotations

import os

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode

from tests.e2e.helpers.env import CHECK_LIVE_URL_ENV_NAME
from tests.e2e.tasks.profiles import E2E_WORKER_PROFILES


def _database_url() -> str:
    """The value of ``CHECK_LIVE_URL_ENV_NAME``. Reads the environment only.

    Raises:
        RuntimeError: the variable is unset or empty. This instance has no
            default database.
    """
    match os.environ.get(CHECK_LIVE_URL_ENV_NAME, ''):
        case '':
            raise RuntimeError(
                f'{CHECK_LIVE_URL_ENV_NAME} is not set: the horsies check '
                'process tests set it to the database URL of the app',
            )
        case database_url:
            return database_url


config = AppConfig(
    queue_mode=QueueMode.DEFAULT,
    broker=PostgresConfig(database_url=SecretStr(_database_url())),
    worker_profiles=E2E_WORKER_PROFILES,
)

app = Horsies(config)
