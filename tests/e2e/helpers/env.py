"""One place that decides which database the e2e suite talks to.

Every caller wants the same thing: an explicit override if one is set,
otherwise a local URL built from `DB_PASSWORD`. Written inline as
`os.environ.get(NAME, f'...{os.environ["DB_PASSWORD"]}...')`, the default
is built BEFORE `get` chooses, so the fallback raised `KeyError` even
when the override was present and was the value that would have been
used. An override alone was therefore not enough to run the suite.

Resolution is lazy here, which is the whole fix: an override is honoured
without `DB_PASSWORD` existing.

Resolving with NOTHING set still yields a URL rather than raising, and
that is deliberate rather than lax. The unit suite imports e2e task
modules as fixtures for the app-locator tests -- `test_worker_imports`
loads `tests.e2e.tasks.instance` -- and the unit job carries no database
credentials. Import must therefore not require one. A URL that cannot
authenticate fails at connect time, where the test that needs a database
is the one that reports it, rather than at import, where a test that
only wanted the module object collapses instead.
"""

from __future__ import annotations

import os

DEFAULT_URL_ENV_NAMES = ('HORSIES_TEST_DATABASE_URL', 'HORSES_E2E_DB_URL')
"""Overrides, in precedence order."""


def e2e_database_url(*env_names: str) -> str:
    """The e2e database URL: first override set, else built locally.

    `env_names` overrides which variables are consulted, for the modules
    that historically read only one of them.
    """
    for name in env_names or DEFAULT_URL_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            return value
    password = os.environ.get('DB_PASSWORD', '')
    return (
        f'postgresql+psycopg://postgres:{password}@localhost:5432/horsies'
    )
