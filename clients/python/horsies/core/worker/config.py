"""The child preload setting of a worker profile."""

from __future__ import annotations

from enum import Enum


class ChildPreload(str, Enum):
    """What a preload parent imports before it forks children. Linux only.

    Elsewhere, and when a preload parent is down, children start with ``exec``.
    """

    # Library code only. Each child imports the app itself.
    LIBRARY = 'library'
    # Library and app. Children share module state from the app's import.
    APP = 'app'
    # No preload parent. Every child starts with ``exec``.
    NO_PRELOAD = 'no_preload'


