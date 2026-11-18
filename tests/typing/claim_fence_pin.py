"""Type pin: the finalizing marker of a run needs its claim time.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. The wrong use
below passes None, and has an ignore comment. The config makes an unneeded
ignore comment an error, so a write that accepts a call without a claim
time again fails the check.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
from datetime import datetime

from horsies.core.worker.child_runner import mark_run_finalizing


def writes(claimed_at: datetime) -> None:
    mark_run_finalizing(uuid.UUID(int=1), 'w', claimed_at)
    mark_run_finalizing(uuid.UUID(int=1), 'w', None)  # pyright: ignore[reportArgumentType]
