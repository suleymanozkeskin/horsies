"""The retention classes of an enqueue: the default period and the class key
that a request stores. A finite class key is a function of its period
(``horsies.core.history.periods``), so a key always means one duration; the
core registers the classes.
"""

from __future__ import annotations

from typing import Final

from horsies._catalog.retention_class import (
    DEFAULT_RETENTION_CLASS_KEY as DEFAULT_RETENTION_CLASS_KEY,
)

from ..periods import RetentionClassKey, RetentionDays
from .tables import FOREVER_CLASS_KEY

DEFAULT_RETENTION_DAYS: Final = RetentionDays(30)
"""The default period. ``DEFAULT_RETENTION_CLASS_KEY`` (generated from
horsies-core) is its class key."""


def resolve_retention_class_key(requested: RetentionClassKey | None) -> str:
    """The stamped class key for one enqueue.

    ``None`` is the explicit forever choice; a period class key is stamped
    as it is. The absent-argument default lives at the enqueue signatures
    as ``DEFAULT_RETENTION_CLASS_KEY``, so absence and explicit forever
    cannot be confused.
    """
    return FOREVER_CLASS_KEY if requested is None else requested


