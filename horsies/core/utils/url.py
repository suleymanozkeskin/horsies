# horsies/core/utils/url.py
"""URL helpers for safe logging and driver URL normalization."""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse


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
