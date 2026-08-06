"""The task-history schema program: frozen fragments in one declared order.

Every relation the history subsystem owns is defined here as an ordered
fragment list. Three column groups are conditioned on qualification gates
that have not closed — the attempt-snapshot columns, the rerun-input
columns, and the reservation-registry indexes — and live in `conditional`
as exact but unauthorized fragments: nothing in the frozen list references
them, no fragment emits them, and a failed gate returns its fragment to
the operator without rework anywhere else.

Final history carries no column defaults on authoritative facts: each
database-owned terminalization function supplies every persisted value, so
missing writer coverage fails at the insert instead of storing placeholder
provenance.
"""
