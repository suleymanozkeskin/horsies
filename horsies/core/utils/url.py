# horsies/core/utils/url.py
"""URL helpers for safe logging."""

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
