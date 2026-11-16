"""Typed database snapshot and compare for the dual-run harness.

A dual-run executes one test against two implementations and compares the
two database states. Ids, clock values and process samples differ between
the runs, so the compare works on normalized snapshots:

- ``rules``: the rule types, one rule per column, and the catalog checks.
- ``catalog``: ``BASELINE_CATALOG``, the rule of every baseline column.
- ``values``: normalized cell values and the clock text parser.
- ``coverage``: the live schema against the catalog.
- ``normalize``: raw rows to a normalized snapshot.
- ``compare``: two normalized snapshots to a typed diff.
- ``capture``: the read-only capture from a database connection.
"""
