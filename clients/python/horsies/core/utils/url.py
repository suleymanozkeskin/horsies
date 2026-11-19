# clients/python/horsies/core/utils/url.py
"""URL helpers: the database URL scheme check, safe logging, and driver URL
normalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final
from urllib.parse import urlparse, urlunparse

from horsies.core.types.result import Err, Ok, Result

DATABASE_URL_SCHEMES: Final = ('postgresql', 'postgres')
"""The schemes that a database URL may have. The URL names no driver:
horsies opens it with psycopg 3."""


@dataclass(frozen=True, slots=True)
class DatabaseUrlWithoutScheme:
    """The URL has no ``<scheme>://`` prefix."""


@dataclass(frozen=True, slots=True)
class DatabaseUrlNamesDriver:
    """The scheme names a driver after ``+``, for example
    ``postgresql+psycopg``."""

    scheme: str


@dataclass(frozen=True, slots=True)
class DatabaseUrlNotPostgres:
    """The scheme is not a PostgreSQL scheme."""

    scheme: str


type DatabaseUrlRefusal = (
    DatabaseUrlWithoutScheme | DatabaseUrlNamesDriver | DatabaseUrlNotPostgres
)


def check_database_url_scheme(url: str) -> Result[str, DatabaseUrlRefusal]:
    """Accept a URL whose scheme is ``postgresql`` or ``postgres``, or name
    why its scheme is refused.

    Reads the scheme only. A refusal carries the scheme and never the rest
    of the URL, which can hold a password.
    """
    scheme, separator, _ = url.partition('://')
    if separator == '' or scheme == '':
        return Err(DatabaseUrlWithoutScheme())
    if scheme in DATABASE_URL_SCHEMES:
        return Ok(url)
    base_scheme, plus, _ = scheme.partition('+')
    if plus != '' and base_scheme in DATABASE_URL_SCHEMES:
        return Err(DatabaseUrlNamesDriver(scheme=scheme))
    return Err(DatabaseUrlNotPostgres(scheme=scheme))


def mask_database_url(url: str) -> str:
    """Mask password in a database URL for secure logging.

    Uses ``urlparse`` for robust handling; falls back to string
    manipulation if the URL cannot be parsed.
    """
    try:
        parsed = urlparse(url)
        if parsed.password:
            netloc = parsed.netloc.replace(f':{parsed.password}@', ':***@')
            return urlunparse(
                (
                    parsed.scheme,
                    netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                ),
            )
        return url
    except Exception:
        if '@' not in url:
            return url
        pre, post = url.split('@', 1)
        scheme_user = pre.rsplit(':', 1)[0]
        return f'{scheme_user}:***@{post}'


def to_psycopg_url(url: str) -> str:
    """Normalize SQLAlchemy-style PostgreSQL URLs for direct psycopg usage.

    Examples:
    - ``postgresql+psycopg://...`` -> ``postgresql://...``
    - ``postgresql+asyncpg://...`` -> ``postgresql://...``

    Non-PostgreSQL or already-normalized URLs are returned unchanged.
    """
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme
        if not scheme:
            return url
        if '+' in scheme:
            base, driver = scheme.split('+', 1)
            if base in {'postgresql', 'postgres'} and driver in {'psycopg', 'asyncpg'}:
                return urlunparse(
                    (
                        base,
                        parsed.netloc,
                        parsed.path,
                        parsed.params,
                        parsed.query,
                        parsed.fragment,
                    ),
                )
        return url
    except Exception:
        return url.replace('+asyncpg', '').replace('+psycopg', '')
