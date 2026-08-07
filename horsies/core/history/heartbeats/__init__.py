"""Heartbeat partition lifecycle over the shared leaf-catalog machinery.

Heartbeats are transient staleness evidence: they matter only while they
can refute a staleness verdict, so their retention horizon derives from
the configured staleness thresholds, never from a fixed constant. Leaves
are hourly — the heartbeat table is the highest-frequency write surface,
and hourly leaves keep drops small and constant — and the same
maintenance loop that owns history leaves owns their detach and drop.

The heartbeat parent is RANGE-partitioned on `sent_at` directly: there
are no retention classes to LIST over because heartbeats never enter
history. Catalog rows live in the shared leaf catalog under the reserved
class key, which the staged-loader manifest excludes at assembly, so no
lookup probe can ever target a heartbeat leaf.
"""
