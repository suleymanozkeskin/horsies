"""The SQLAlchemy URL of a database URL, for the engines that the cross-client harnesses open.

The parity harness (``tests/parity/send.py``) and the perf harness
(``tests/perf/__main__.py``) open their own SQLAlchemy engine on a database
URL. The Python client's tests have their own module of the same name in
their own package.
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
    ``PostgresConfig`` holds it. Any other URL is a defect of the harness:
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
