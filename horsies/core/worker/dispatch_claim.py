"""The claim of one dispatch: its claim generation and the retry facts of its row.

``horsies_claim`` returns, for each row it takes, the claim time it set and
the row's ``retry_count``, ``max_retries`` and ``task_options``. The dispatch
carries them as one ``DispatchClaim`` from the claim to the end of the
attempt: every parent-side write of the dispatch fences on ``claimed_at``,
and the failed-attempt write decides its retry from the facts with no read
of the row.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class DispatchClaim:
    """The claim that a dispatch was born from.

    ``claimed_at`` is the claim generation. ``retry_count``, ``max_retries``
    and ``task_options`` are the row's values at the claim. They cannot
    change inside one claim generation: the two retry writers are the only
    writers of ``retry_count``, and both end the generation; the other two
    are written at enqueue.
    """

    claimed_at: datetime
    retry_count: int
    max_retries: int
    task_options: str | None


def dispatch_claim_of(row: Mapping[str, Any]) -> DispatchClaim:
    """The claim of a task row that ``horsies_claim`` took.

    A row without a claim time, or with a retry count or limit that is not
    an integer, is a defect of the claim function (``AssertionError``).
    Does no I/O.
    """
    match row:
        case {
            'claimed_at': datetime() as claimed_at,
            'retry_count': int() as retry_count,
            'max_retries': int() as max_retries,
            'task_options': str() | None as task_options,
        }:
            return DispatchClaim(
                claimed_at=claimed_at,
                retry_count=retry_count,
                max_retries=max_retries,
                task_options=task_options,
            )
        case _:
            raise AssertionError(
                f'horsies_claim returned task {row.get("id")!r} without its claim: '
                f'claimed_at={row.get("claimed_at", "missing")!r}, '
                f'retry_count={row.get("retry_count", "missing")!r}, '
                f'max_retries={row.get("max_retries", "missing")!r}',
            )
