# clients/python/horsies/core/models/payload.py
from __future__ import annotations
from typing import Annotated, ClassVar, Optional
from pydantic import BaseModel, ConfigDict, Field

from horsies._catalog.worker_manifest import PAYLOAD_BYTES_MAX


class PayloadPolicy(BaseModel):
    """Size guardrail for serialized task payloads.

    Checked against the size of the already-serialized JSON text at the
    encode boundary, in bytes of its UTF-8 text: a non-ASCII character
    counts 2 to 4 bytes. No extra serialization pass.

    Fields:
    - warn_bytes: Log a structured warning when a payload exceeds this size,
      rate-limited to once per (task_name, kind) per process. None disables.
    - reject_bytes: Fail an enqueue closed (typed PAYLOAD_TOO_LARGE error,
      nothing written) when its payload exceeds this size. A schedule over
      this size fails scheduler start and ``check``. Applies to enqueue
      payloads only — results are never rejected (completed work
      is never destroyed over size; results warn only). None (default)
      disables rejection.

    Each threshold is at most ``PAYLOAD_BYTES_MAX``, the bound of its field in
    the worker manifest.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    warn_bytes: Annotated[Optional[int], Field(ge=1, le=PAYLOAD_BYTES_MAX)] = Field(
        default=1_048_576,  # 1 MiB
        description=(
            'Warn when the UTF-8 text of a serialized payload exceeds this many bytes '
            '(once per task_name and payload kind per process); '
            'None disables the warning'
        ),
    )

    reject_bytes: Annotated[Optional[int], Field(ge=1, le=PAYLOAD_BYTES_MAX)] = Field(
        default=None,
        description=(
            'Reject an enqueue whose serialized payload exceeds this many '
            'bytes of UTF-8 text (typed PAYLOAD_TOO_LARGE error before any row is '
            'written); results are never rejected. None disables rejection'
        ),
    )
