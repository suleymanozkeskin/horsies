"""Typed values that several error catalogs share.

``NoCause`` is generated into ``horsies._catalog.error_detail`` by
horsies-codegen, with the error classes that name it; this module
re-exports it.
"""

from __future__ import annotations

from horsies._catalog.error_detail import NoCause as NoCause

__all__ = ['NoCause']
