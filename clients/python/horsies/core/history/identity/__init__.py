"""Request identity: the reservation window of an idempotency key.

The core mints task ids, computes the command fingerprint and hashes the
scoped idempotency key; this package keeps the client's check of the
configured reservation window. Pure computation, no SQL and no I/O.
"""
