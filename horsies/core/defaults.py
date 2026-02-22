"""Shared default constants for the horsies library."""

# Default claim lease duration in milliseconds.
# Applied when claim_lease_ms is not explicitly configured.
# Bounded so a crashed worker's claims become reclaimable within this window.
DEFAULT_CLAIM_LEASE_MS: int = 60_000  # 60 seconds
