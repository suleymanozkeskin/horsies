"""Request identity primitives: task IDs, command fingerprints, scoped keys.

Task IDs are client-minted UUIDv7 values created during prepared-send
construction and preserved across uncertain-commit retries. The command
fingerprint is the version-1 canonical digest that decides replay versus
conflict for keyed enqueue. Scoped idempotency keys frame the caller's
exact opaque bytes under the task-name scope.

Everything here is pure computation — no SQL, no I/O beyond entropy.
"""
