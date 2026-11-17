"""Read paths over split live/history storage.

The history readers are database functions that one statement calls: they
probe the live table, then each attached history leaf by its task_id index,
never the partitioned parent. ``horsies_publish_history_readers``
(0008_history_maintenance_functions.sql) renders them from the leaf catalog;
the partition manager publishes again whenever the history leaf set changes.
UUIDv7 identifiers order likely probes by embedded birth time under a
five-second clock-skew allowance, then fall back to every skipped leaf
before returning absence; other identifiers walk every leaf, newest first.
"""
