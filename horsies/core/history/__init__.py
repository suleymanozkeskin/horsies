"""Task-history subsystem: immutable terminal storage behind typed boundaries.

Terminal tasks live in time-partitioned history storage; finite retention
leaves through partition detach and drop, never through row deletion. This
package owns the typed command/outcome vocabulary for history operations and
the partition lifecycle that keeps terminal transitions covered.

Nothing here is a public SQL surface. Relation names, generated functions,
and catalog shapes are internal; public consumers use the typed monitoring
API.
"""
