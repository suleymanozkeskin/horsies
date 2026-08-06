"""Partition lifecycle for task-history storage.

The manager owns the leaf catalog, create-ahead coverage, the concurrent
detach/finalize/drop lifecycle, and the health contract that fails before a
terminal transition encounters missing coverage. Whenever the finite leaf
set changes it must republish the staged lookup function through the
`publication` seam; attach and retirement never expose a snapshot in which
a retained row is invisible to the loader.
"""
