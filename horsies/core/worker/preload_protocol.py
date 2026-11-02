"""Constants shared by the worker and the preload parent.

The preload parent imports the application, so the worker must not import that
program. This module holds what both sides need.
"""

from __future__ import annotations

from typing import Final

SPAWN_REQUEST: Final = b'S'
