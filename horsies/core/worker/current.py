# app/core/worker/current.py
from __future__ import annotations
from typing import Optional
from horsies.core.app import Horsies

_current_app: Optional[Horsies] = None


def set_current_app(app: Horsies) -> None:
    global _current_app
    _current_app = app


def get_current_app() -> Horsies:
    if _current_app is None:
        raise RuntimeError('No current app set in this process')
    return _current_app
