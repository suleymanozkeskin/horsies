"""Read paths over split live/history storage.

The staged lookup loader is the qualified answer to the boundary gate: one
client statement, one snapshot, a database-owned function that probes live
first, the forever child second, then finite leaves as static direct table
references — never the partitioned parent, whose plan-time fan-out locks
every leaf and was the measured rejection basis of the union form.

The function body is generated from the leaf catalog and republished by the
partition manager whenever the finite leaf set changes. UUIDv7 identifiers
prune the finite walk by embedded birth time under the five-second forward-
clock bound; legacy identifiers always walk every leaf, newest first.
"""
