"""Type pin: every claim-generation fence needs a claim time.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. Each wrong use
below leaves out the claim time or passes None, and has an ignore comment.
The config makes an unneeded ignore comment an error, so a fence or a write
that accepts a call without a claim time again fails the check.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
from datetime import datetime

from horsies.core.lifecycle.fences import OwnedClaim, OwnedClaimBatch
from horsies.core.worker.child_runner import mark_run_finalizing


def fences(claimed_at: datetime) -> None:
    OwnedClaim(worker_id='w', claimed_at=claimed_at)
    OwnedClaim(worker_id='w')  # pyright: ignore[reportCallIssue]
    OwnedClaim(worker_id='w', claimed_at=None)  # pyright: ignore[reportArgumentType]
    OwnedClaimBatch(worker_id='w', claim_generations=(('t', claimed_at),))
    OwnedClaimBatch('w', (('t', None),))  # pyright: ignore[reportArgumentType]


def writes(claimed_at: datetime) -> None:
    mark_run_finalizing(uuid.UUID(int=1), 'w', claimed_at)
    mark_run_finalizing(uuid.UUID(int=1), 'w', None)  # pyright: ignore[reportArgumentType]
