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

# Time in milliseconds that sent data may stay unacknowledged before the
# kernel closes a database connection (libpq tcp_user_timeout). Without it,
# a write to a dead connection blocks for the kernel retransmission limit
# (about 15 minutes on Linux); TCP keepalives do not probe while sent data
# is unacknowledged.
DEFAULT_TCP_USER_TIMEOUT_MS: int = 30_000  # 30 seconds
