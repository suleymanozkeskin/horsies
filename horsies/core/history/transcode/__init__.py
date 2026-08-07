"""Replacement-partition transcode: copy, verify, swap, retire.

The executor rewrites an archive component into replacement relations in
bounded committing batches, verifies full content BEFORE any lock, and
swaps bindings inside a non-queuing locked window that re-checks
staleness and identity only — never content. Budgets are the qualified
ones and nothing wider: the evidence covers one million rows per
component at one batch size, and the behavioral matrix pins exactly the
validated lines.

Session-rendering discipline, day one: deparsed text is HAZARDOUS in
comparison and hashing (it renders in the session's settings) and SAFE
in replay-as-DDL (rendered literals carry their offset and re-parse to
the same instant). Every capture that feeds a comparison or a hash runs
under the canonical-UTC pin; DDL replay copies the qualified shape
as-is.
"""
