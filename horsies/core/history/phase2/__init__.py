"""Workflow phase-2 recovery over split live/history storage.

Pending rows are narrow recovery evidence: they locate the authoritative
terminal material — a history row while history owns it, a quarantine row
after an exceptional repoint — and they are deleted only when a durable
disposition commits. Consumption never blocks on partition maintenance;
detach defers to pending, and the detach-horizon quarantine protocol owns
the one anomalous overlap.

Lock order is the engine's own documented invariant (N6, the 0.2.9
deadlock fix): workflows before workflow_tasks, with pending as a third
tier no other writer locks first.
"""
