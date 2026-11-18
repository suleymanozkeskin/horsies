"""Archive codecs: every retained history format stays readable, forever.

History rows version four independent domains — the row projection, the
result envelope, the attempt snapshot, and the rerun input. Each domain
decodes every retained version and fails closed on everything else: an
unknown version, an unknown codec, a wrong content type, a digest mismatch,
or a corrupt payload is a typed decode failure, never absence and never a
silently skipped row.

Codecs operate on values, not tables. Nothing in this package executes SQL.
The SQL terminalization functions write the stored bytes and the read surfaces
decode them here, which keeps one decoder authoritative for both drivers.
The stored rerun input is read for a rerun in the core (``rerun_task``).
"""
