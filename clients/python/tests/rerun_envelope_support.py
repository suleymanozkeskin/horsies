"""Python mirror of the SQL rerun envelope writer, for tests only.

`horsies_encode_rerun_input` (baseline SQL) frames the stored ``input`` and
``task_options`` texts byte for byte. Tests use this mirror to state the
bytes a move stores and to build decoder input.
"""

from __future__ import annotations


def frame_rerun_envelope(input_json: str, options_json: str | None) -> bytes:
    """The envelope bytes that the move stores for these two column texts."""
    options = options_json if options_json is not None else 'null'
    return f'{{"input":{input_json},"options":{options}}}'.encode('utf-8')
