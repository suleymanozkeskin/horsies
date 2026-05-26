"""Public codec API for the strict-serde redesign.

Re-exports the encoder/decoder primitives and the boundary type aliases
that constitute the stable codec surface. Modules outside `horsies.core`
should import from here rather than reaching into `typed`, `json_value`,
etc. directly.

See `ignored-content/design/strict-serde.md` §6 / §8.
"""

from __future__ import annotations

from horsies.core.codec.json_value import JsonValue, StrictJsonError
from horsies.core.codec.typed import (
    Json,
    TypeAnnotation,
    decode_task_result,
    decode_value,
    encode_task_result,
    encode_value,
)


__all__ = [
    'Json',
    'JsonValue',
    'StrictJsonError',
    'TypeAnnotation',
    'decode_task_result',
    'decode_value',
    'encode_task_result',
    'encode_value',
]
