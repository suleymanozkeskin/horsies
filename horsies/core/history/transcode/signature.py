"""The relation schema signature: session-independent by construction.

The signature hashes the relation's structural identity — relkind and
storage options, every live column with its default expression, every
constraint definition, every index definition, every non-internal
trigger — so the verification token can prove a relation did not change
between verification and swap. Four of those inputs are deparsed text
(`pg_get_expr`, `pg_get_constraintdef`, `pg_get_indexdef`,
`pg_get_triggerdef`) and deparsed text renders in the CALLING SESSION'S
settings: a timestamptz default or a CHECK over timestamptz literals
hashes differently under a different session timezone, and a token
captured at verification would mismatch at swap on a relation that
never changed. The capture is one statement, so running it under the
canonical-UTC pin makes the signature session-independent by
construction — the hazard is comparison and hashing, and this module
is where both live.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..partitions.catalog import execute_with_utc_rendering


RELATION_SCHEMA_SIGNATURE_SQL = """
SELECT encode(sha256(convert_to(
    jsonb_build_object(
        'relation', jsonb_build_array(
            relation.relkind,
            relation.relpersistence,
            relation.relam,
            relation.reloptions
        ),
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    attribute.attnum,
                    attribute.attname,
                    attribute.atttypid,
                    attribute.atttypmod,
                    attribute.attcollation,
                    attribute.attnotnull,
                    attribute.attidentity,
                    attribute.attgenerated,
                    pg_get_expr(defaults.adbin, defaults.adrelid)
                ) ORDER BY attribute.attnum
            )
            FROM pg_attribute AS attribute
            LEFT JOIN pg_attrdef AS defaults
              ON defaults.adrelid = attribute.attrelid
             AND defaults.adnum = attribute.attnum
            WHERE attribute.attrelid = relation.oid
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    constraints.conname,
                    constraints.contype,
                    constraints.convalidated,
                    pg_get_constraintdef(constraints.oid, false)
                ) ORDER BY constraints.conname
            )
            FROM pg_constraint AS constraints
            WHERE constraints.conrelid = relation.oid
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    indexes.indisvalid,
                    indexes.indisready,
                    pg_get_indexdef(indexes.indexrelid)
                ) ORDER BY indexes.indexrelid
            )
            FROM pg_index AS indexes
            WHERE indexes.indrelid = relation.oid
        ), '[]'::jsonb),
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    triggers.tgenabled,
                    pg_get_triggerdef(triggers.oid, false)
                ) ORDER BY triggers.tgname
            )
            FROM pg_trigger AS triggers
            WHERE triggers.tgrelid = relation.oid
              AND NOT triggers.tgisinternal
        ), '[]'::jsonb)
    )::text,
    'UTF8'
)), 'hex')
FROM pg_class AS relation
WHERE relation.oid = CAST(:relation_oid AS oid)
"""


async def relation_schema_signature(
    connection: AsyncConnection,
    relation_oid: int,
) -> str | None:
    """The relation's structural hash, rendered under the UTC pin.

    None when the relation does not exist. Every caller that stores or
    compares this value inherits session independence from the capture
    itself; there is no unpinned variant to reach for.
    """
    result = await execute_with_utc_rendering(
        connection,
        text(RELATION_SCHEMA_SIGNATURE_SQL),
        {'relation_oid': relation_oid},
    )
    value = result.scalar_one_or_none()
    if value is None:
        return None
    if not isinstance(value, str):
        raise AssertionError('schema signature did not decode as text')
    return value
