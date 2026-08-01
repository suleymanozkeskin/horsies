# horsies/core/models/payload.py
from __future__ import annotations
from typing import Annotated, Optional
from pydantic import BaseModel, Field


class PayloadPolicy(BaseModel):
    """Size guardrail for serialized task payloads.

    Checked against the length of the already-serialized JSON string at
    the encode boundary — one integer comparison per enqueue/result, no
    extra serialization pass. Length is in characters, which for JSON
    wire payloads (ASCII-dominant) approximates UTF-8 bytes; the
    guardrail does not require byte exactness.

    Fields:
    - warn_bytes: Log a structured warning when a payload exceeds this size,
      rate-limited to once per (task_name, kind) per process. None disables.
    - reject_bytes: Fail an enqueue closed (typed PAYLOAD_TOO_LARGE error,
      nothing written) when its payload exceeds this size. Applies to
      enqueue payloads only — results are never rejected (completed work
      is never destroyed over size; results warn only). None (default)
      disables rejection.
    """

    warn_bytes: Annotated[Optional[int], Field(ge=1)] = Field(
        default=1_048_576,  # 1 MiB
        description=(
            'Warn when a serialized payload exceeds this many bytes '
            '(once per task_name and payload kind per process); '
            'None disables the warning'
        ),
    )

    reject_bytes: Annotated[Optional[int], Field(ge=1)] = Field(
        default=None,
        description=(
            'Reject an enqueue whose serialized payload exceeds this many '
            'bytes (typed PAYLOAD_TOO_LARGE error before any row is '
            'written); results are never rejected. None disables rejection'
        ),
    )
