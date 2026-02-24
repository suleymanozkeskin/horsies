"""Shared default constants for the horsies library."""

# Default claim lease duration in milliseconds.
# Applied when claim_lease_ms is not explicitly configured.
# Bounded so a crashed worker's claims become reclaimable within this window.
DEFAULT_CLAIM_LEASE_MS: int = 60_000  # 60 seconds

# Maximum age (in ms) of a CLAIMED task that the heartbeat loop will renew.
# Tasks claimed longer ago than this are left to expire naturally, preventing
# indefinite lease renewal of orphaned CLAIMED tasks (e.g. after dispatch
# failure + requeue DB error).
MAX_CLAIM_RENEW_AGE_MS: int = 180_000  # 3 minutes
