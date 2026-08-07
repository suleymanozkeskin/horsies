"""Post-terminal rerun: a new request re-executing a recorded one.

A rerun is a new enqueue with a new task ID and recorded lineage, never
a mutation of the terminal record. One principle divides every column
of the new row: what the SOURCE REQUEST specified replays from the
source row — the lineage claims re-execution of that request, and
resolving current definitions could silently run different work — while
every enqueue-time policy snapshot of the NEW request resolves current,
because each enqueue makes its own retention promise. The provenance
table is that principle made exhaustive and pinned.
"""
