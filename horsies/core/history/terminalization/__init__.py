"""Terminalization for split live/history storage.

The fifteen operations keep their stable typed wire contract — the same
signatures, the same outcome row shape, the same miss-classification
order — while their bodies perform the direct live-to-history move: one
transaction that snapshots the locked live row, captures the complete
attempt sequence as the canonical versioned snapshot, writes exactly one
immutable history row, creates deferred phase-2 evidence where the variant
owns it, deletes the live rows, and emits the transactional raw-ID
notification. Any failure rolls the whole transition back.

Families land one at a time behind the shared move; the completion family
is first. The projection each body writes is the ratified full history
projection — the schema program that installs these functions installs the
gated column fragments first.
"""
