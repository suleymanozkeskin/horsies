"""Partition lifecycle for task-history storage.

The manager owns the leaf catalog, create-ahead coverage, the concurrent
detach/finalize/drop lifecycle, and the health contract that fails before a
terminal transition encounters missing coverage. Whenever the finite leaf
set changes it must republish the staged lookup function through the
`publication` seam; attach and retirement never expose a snapshot in which
a retained row is invisible to the loader.

Constraint — bounded relations per transaction: a single
`DROP SCHEMA CASCADE` over a 512-leaf hierarchy takes an AccessExclusive
lock entry per relation — each leaf contributing its table, indexes, and
TOAST relations — and exhausts the shared lock table (sized as
`max_locks_per_transaction` x `max_connections`, roughly 6,400 entries at
the defaults of 64 and 100; the parameter is an average allowance, not a
per-transaction cap), failing with out-of-shared-memory.
Every maintenance operation here is one-leaf-per-transaction by design —
create, detach, finalize, and drop each address a single leaf, and
create-ahead runs one bounded horizon — and any future admin sweep,
reclassification, or transcode swap must keep a declared bound on
relations touched per transaction rather than iterating a hierarchy
inside one.
"""
