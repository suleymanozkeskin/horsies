"""A minimal discoverable app for the ``horsies web`` boot tests.

The task lives in the sibling ``tasks`` module and nothing here imports it, so
the only thing that can put it in the registry is the startup validation that
``horsies web`` runs against an app path.
"""

from __future__ import annotations

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig

app = Horsies(
    config=AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://u:p@localhost/db'),
        ),
    ),
)
app.discover_tasks(['tests.unit.web_cli_sentinel.tasks'])
