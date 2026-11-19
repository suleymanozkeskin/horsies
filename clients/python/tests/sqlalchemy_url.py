"""The SQLAlchemy URL of a database URL, for the engines that tests open.

The library opens no SQLAlchemy engine: every statement of a broker runs in
the core. Tests that seed or read rows directly open their own engine on the
database URL of the test session.
"""

from __future__ import annotations

from typing import Final

from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import check_database_url_scheme

_SQLALCHEMY_SCHEME: Final = 'postgresql+psycopg'


def sqlalchemy_url(url: str) -> str:
    """The SQLAlchemy URL that opens ``url`` with psycopg 3: the scheme
    becomes ``postgresql+psycopg`` and the rest is kept as it is.

    ``url`` is a URL that ``check_database_url_scheme`` accepts, as
    ``PostgresConfig`` holds it. Any other URL is a defect of the test:
    ``AssertionError``.
    """
    match check_database_url_scheme(url):
        case Ok():
            _, _, rest = url.partition('://')
            return f'{_SQLALCHEMY_SCHEME}://{rest}'
        case Err(refusal):
            raise AssertionError(
                f'sqlalchemy_url got a database URL that the config check '
                f'refuses: {refusal!r}'
            )
