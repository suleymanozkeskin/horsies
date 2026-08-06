"""Archive codecs: every retained history format stays readable, forever.

History rows version four independent domains — the row projection, the
result envelope, the attempt snapshot, and the rerun input. Each domain
decodes every retained version and fails closed on everything else: an
unknown version, an unknown codec, a wrong content type, a digest mismatch,
or a corrupt payload is a typed decode failure, never absence and never a
silently skipped row.

Codecs operate on values, not tables. Nothing in this package executes SQL;
the terminalization writer stores what these codecs produce and the read
surfaces decode through them, which is what keeps one decoder authoritative
for both drivers and for the offline transcode inventory.
"""
