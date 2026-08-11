"""One place that decides which database the e2e suite talks to.

Every caller wants the same thing: an explicit override if one is set,
otherwise a local URL built from `DB_PASSWORD`. Written inline as
`os.environ.get(NAME, f'...{os.environ["DB_PASSWORD"]}...')`, the default
is built BEFORE `get` chooses, so the fallback raises `KeyError` even
when the override is present and is the value that would have been used.
An override alone was therefore not enough to run the suite.

Resolution is lazy here, and the failure names both ways out instead of
surfacing as a bare `KeyError: 'DB_PASSWORD'` from a module import.
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
    password = os.environ.get('DB_PASSWORD')
    if not password:
        consulted = ', '.join(env_names or DEFAULT_URL_ENV_NAMES)
        raise RuntimeError(
            'no e2e database configured: set DB_PASSWORD, or set one of '
            f'{consulted} to a full database URL'
        )
    return (
        f'postgresql+psycopg://postgres:{password}@localhost:5432/horsies'
    )
