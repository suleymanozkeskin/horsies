"""Generation of the staged task-lookup function from the leaf catalog.

The generated function is the qualified boundary-gate shape: PL/pgSQL that
probes the live table, then every cataloged history leaf as a static direct
table reference. This includes the RANGE leaves of the forever class. Each
reached statement is eligible for plan
caching, and no statement ever references the partitioned parent — the
parent's plan-time fan-out over ~514 relations is the measured failure the
staged form replaced.

UUIDv7 identifiers use their embedded birth time as a probe-order hint, never
as an existence proof. Likely leaves are probed oldest-first after subtracting
the five-second clock-skew allowance. If they miss, every skipped leaf is
probed newest-first before absence is returned. Caller-supplied identifiers
and historical rows therefore remain readable even when their embedded time
violates the expected enqueue clock bound. When every attached leaf carries
verified birth metadata, a birth before the global minimum remains a safe
constant-time absence: that minimum is derived from the stored rows, not an
assertion about caller clocks. Legacy identifiers walk every leaf newest-first.

The function never assumes a finite request lifetime. Regeneration is owned
by the partition manager through the publication seam; this module renders
what the catalog manifest says and nothing else.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from ..commands import is_safe_identifier
from ..names import (
    HEARTBEAT_CLASS_KEY,
    LIVE_TASKS,
    TASK_DETAIL_FUNCTION,
    TASK_HISTORY_PARENT,
    TASK_LOOKUP_FUNCTION,
    TASK_LOOKUP_MANIFEST,
    TASK_LOOKUP_TYPE,
    TASK_PROVENANCE_FUNCTION,
    TASK_PROVENANCE_TYPE,
)
from ..partitions.catalog import LeafCatalogRow


CLOCK_BOUND_SECONDS = 5
"""Clock-skew allowance used only to order likely UUIDv7 probes."""


TASK_LOOKUP_TYPE_DDL = f"""
CREATE TYPE {TASK_LOOKUP_TYPE} AS (
    found boolean,
    location text,
    task_id uuid,
    fingerprint_version smallint,
    command_fingerprint bytea
)
"""

TASK_PROVENANCE_TYPE_DDL = f"""
CREATE TYPE {TASK_PROVENANCE_TYPE} AS (
    found boolean,
    location text,
    task_id uuid,
    status text,
    terminal_at timestamptz,
    terminalization_kind text
)
"""

TASK_LOOKUP_MANIFEST_DDL = f"""
CREATE TABLE {TASK_LOOKUP_MANIFEST} (
    leaf_name text PRIMARY KEY,
    probe_position integer NOT NULL UNIQUE,
    lower_anchor timestamptz NOT NULL,
    upper_anchor timestamptz NOT NULL,
    min_birth_at timestamptz,
    published_at timestamptz NOT NULL,
    CHECK (probe_position >= 0),
    CHECK (lower_anchor < upper_anchor)
)
"""


@dataclass(frozen=True, slots=True)
class LookupLeaf:
    """One finite leaf as the generated function will probe it."""

    relation_name: str
    lower_anchor: datetime
    upper_anchor: datetime
    min_birth_at: datetime | None

    def __post_init__(self) -> None:
        if not is_safe_identifier(self.relation_name):
            raise ValueError(
                f'lookup leaf name is not a safe identifier: '
                f'{self.relation_name!r}'
            )
        if self.lower_anchor.tzinfo is None or self.upper_anchor.tzinfo is None:
            raise ValueError('lookup leaf bounds must be timezone-aware')
        if self.lower_anchor >= self.upper_anchor:
            raise ValueError('lookup leaf bounds must be increasing')
        if self.min_birth_at is not None and self.min_birth_at.tzinfo is None:
            raise ValueError('lookup leaf birth metadata must be timezone-aware')


@dataclass(frozen=True, slots=True)
class LookupManifest:
    """What the generated functions probe, and where history begins.

    Two jobs, and only the first is filtered by relation existence.
    `leaves` is the probe list: a relation that does not exist cannot be
    probed, and naming it anyway kills the function at execution.

    `birth_floor` is the retained-history floor, computed over the
    COMPLETE attached set including leaves whose relation is gone. It is
    set only when every attached leaf carried verified birth metadata; a
    single unverified leaf disables the constant-time purged fast path
    and the walk decides absence the long way.

    The floor is deliberately not narrowed to the probe list. Absence and
    the EXPLANATION of absence are different claims: a task whose leaf was
    destroyed out of band is genuinely unreadable, but it does not predate
    retained history, and saying it does would present an accidental drop
    as ordinary ageing. A floor computed over the full attached set errs
    low, which only ever means probing before concluding absence.
    """

    leaves: tuple[LookupLeaf, ...]
    birth_floor: datetime | None


def manifest_from_catalog(
    rows: Sequence[LeafCatalogRow],
    *,
    absent_relations: frozenset[str] = frozenset(),
) -> LookupManifest:
    """Build the generation manifest from attached catalog rows.

    The rows must be the complete attached set across every retention
    class — the caller reads them through the catalog module's
    all-classes query. Ordering is normalized here; duplicate relation
    names are a contract violation the constructor surfaces.

    `absent_relations` names attached leaves whose relation no longer
    exists. They leave the probe list and stay in the floor, per the two
    jobs `LookupManifest` documents.

    Non-history catalog consumers are excluded HERE, at assembly, so
    every manifest consumer inherits the filter: heartbeat leaves share
    the leaf catalog but carry no history columns, and a generated probe
    against one would reference fingerprint columns that do not exist —
    CREATE would succeed (bodies are unvalidated) and execution would
    die at that probe, or worse resolve wrongly through the coincidental
    task_id column.
    """
    history_rows = [
        row for row in rows if row.class_key != HEARTBEAT_CLASS_KEY
    ]
    ordered = sorted(
        history_rows, key=lambda row: (row.lower_anchor, row.leaf_name)
    )
    names = [row.leaf_name for row in ordered]
    if len(set(names)) != len(names):
        raise ValueError('lookup manifest relation names must be distinct')
    leaves = tuple(
        LookupLeaf(
            relation_name=row.leaf_name,
            lower_anchor=row.lower_anchor,
            upper_anchor=row.upper_anchor,
            min_birth_at=row.min_birth_at,
        )
        for row in ordered
        if row.leaf_name not in absent_relations
    )
    birth_floor: datetime | None = None
    if ordered and all(row.min_birth_verified for row in ordered):
        observed = [row.min_birth_at for row in ordered if row.min_birth_at]
        if observed:
            birth_floor = min(observed)
    return LookupManifest(leaves=leaves, birth_floor=birth_floor)


def render_staged_lookup_function(manifest: LookupManifest) -> str:
    """Render the identity lookup: found rows carry fingerprint facts.

    The live table's identifier column is `id`, not `task_id`; the live
    probe reads it as `task_id` into the composite so the wire shape is
    uniform while the predicate matches the real column.
    """
    return _staged_function(
        function_name=TASK_LOOKUP_FUNCTION,
        return_type=TASK_LOOKUP_TYPE,
        declares=(
            'v_task_id uuid;',
            'v_fingerprint_version smallint;',
            'v_fingerprint bytea;',
        ),
        absence_values='NULL, NULL, NULL, NULL',
        live_probe=_identity_probe(
            LIVE_TASKS, location='LIVE', id_column='id'
        ),
        history_probe=lambda relation: _identity_probe(
            relation, location='HISTORY'
        ),
        manifest=manifest,
    )


def render_staged_provenance_function(manifest: LookupManifest) -> str:
    """Render the provenance lookup: found rows carry terminal facts.

    The terminalization miss classifier consumes this to distinguish
    already-applied from foreign terminalization after a task has moved to
    history — the same staged mechanism, never the partitioned parent. A
    live hit carries null terminal facts: liveness is the fact, and the
    caller re-reads the live row under its own lock.
    """
    live_probe = f'''
        IF p_include_live THEN
{_provenance_live_probe()}
        END IF;
'''
    return _staged_function(
        function_name=TASK_PROVENANCE_FUNCTION,
        return_type=TASK_PROVENANCE_TYPE,
        extra_parameters=', p_include_live boolean DEFAULT TRUE',
        declares=(
            'v_task_id uuid;',
            'v_status text;',
            'v_terminal_at timestamptz;',
            'v_kind text;',
        ),
        absence_values='NULL, NULL, NULL, NULL, NULL',
        live_probe=live_probe,
        history_probe=_provenance_history_probe,
        manifest=manifest,
    )


def render_staged_detail_function(manifest: LookupManifest) -> str:
    """Render the detail lookup: a history hit carries the full frozen row.

    A HISTORY result from the identity lookup cannot re-derive its leaf —
    the caller lacks the LIST key — so detail returns the whole projection
    from the probing leaf in the one statement that finds it. The row
    travels as the parent's composite (`%ROWTYPE` resolves per session, so
    the shape tracks installed columns); a live hit signals location only,
    because live rows are not frozen-shaped and the live read layer owns
    them. Absence returns no rows.
    """
    detail_row = f'SELECT h.* INTO v_row FROM {{relation}} h ' \
        'WHERE h.task_id = p_task_id;'

    def history_probe(relation: str) -> str:
        if not is_safe_identifier(relation):
            raise ValueError(
                f'relation name is not a safe identifier: {relation!r}'
            )
        return f"""
        {detail_row.format(relation=relation)}
        IF FOUND THEN
            RETURN QUERY SELECT 'HISTORY'::text, v_row;
            RETURN;
        END IF;
"""

    live_probe = f"""
        IF EXISTS (SELECT 1 FROM {LIVE_TASKS} WHERE id = p_task_id) THEN
            RETURN QUERY SELECT
                'LIVE'::text, NULL::{TASK_HISTORY_PARENT};
            RETURN;
        END IF;
"""
    return _staged_function(
        function_name=TASK_DETAIL_FUNCTION,
        return_type=(
            f'TABLE (location text, task_row {TASK_HISTORY_PARENT})'
        ),
        declares=(f'v_row {TASK_HISTORY_PARENT}%ROWTYPE;',),
        absence_statement='RETURN;',
        live_probe=live_probe,
        history_probe=history_probe,
        manifest=manifest,
    )


def _staged_function(
    *,
    function_name: str,
    return_type: str,
    declares: tuple[str, ...],
    live_probe: str,
    history_probe: Callable[[str], str],
    manifest: LookupManifest,
    absence_values: str | None = None,
    absence_statement: str | None = None,
    extra_parameters: str = '',
) -> str:
    """The one staged skeleton all three generated functions render from.

    Absence is either a composite `ROW(FALSE, ...)` return built from
    `absence_values` (the scalar-returning pair) or a caller-supplied
    statement (`RETURN;` for the set-returning detail form). Exactly one
    must be given.
    """
    if (absence_values is None) == (absence_statement is None):
        raise ValueError('exactly one absence form must be supplied')
    absence = (
        absence_statement
        if absence_statement is not None
        else f'RETURN ROW(FALSE, {absence_values})::{return_type};'
    )
    if not manifest.leaves:
        finite_section = ''
    else:
        floor_check = ''
        if manifest.birth_floor is not None:
            floor_check = f"""
            IF v_birth_at < {_timestamp_literal(manifest.birth_floor)} THEN
                {absence}
            END IF;
"""
        pruned_probes = '\n'.join(
            _pruned_probe(leaf, history_probe(leaf.relation_name))
            for leaf in manifest.leaves
        )
        fallback_probes = '\n'.join(
            _fallback_probe(leaf, history_probe(leaf.relation_name))
            for leaf in reversed(manifest.leaves)
        )
        legacy_probes = '\n'.join(
            history_probe(leaf.relation_name)
            for leaf in reversed(manifest.leaves)
        )
        finite_section = f"""
        v_uuid_bytes := uuid_send(p_task_id);
        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7
           AND (get_byte(v_uuid_bytes, 8) & 192) = 128
        THEN
            v_birth_milliseconds :=
                (get_byte(v_uuid_bytes, 0)::bigint << 40)
                | (get_byte(v_uuid_bytes, 1)::bigint << 32)
                | (get_byte(v_uuid_bytes, 2)::bigint << 24)
                | (get_byte(v_uuid_bytes, 3)::bigint << 16)
                | (get_byte(v_uuid_bytes, 4)::bigint << 8)
                | get_byte(v_uuid_bytes, 5)::bigint;
            v_birth_at := to_timestamp(
                v_birth_milliseconds::double precision / 1000.0
            );
{floor_check}
            v_effective_birth :=
                v_birth_at - INTERVAL '{CLOCK_BOUND_SECONDS} seconds';
{pruned_probes}
            -- Birth time is an optimization hint, not an integrity
            -- constraint. Probe every leaf skipped above before declaring
            -- absence so a caller-clock violation cannot hide a retained row.
{fallback_probes}
        ELSE
{legacy_probes}
        END IF;
"""

    declare_block = '\n'.join(f'        {declare}' for declare in declares)
    return f"""
    CREATE OR REPLACE FUNCTION {function_name}(p_task_id uuid{extra_parameters})
    RETURNS {return_type}
    LANGUAGE plpgsql
    STABLE
    AS $function$
    DECLARE
{declare_block}
        v_uuid_bytes bytea;
        v_birth_milliseconds bigint;
        v_birth_at timestamptz;
        v_effective_birth timestamptz;
    BEGIN
{live_probe}
{finite_section}
        {absence}
    END
    $function$
    """


def _pruned_probe(leaf: LookupLeaf, probe: str) -> str:
    upper = _timestamp_literal(leaf.upper_anchor)
    return f"""
            IF v_effective_birth < {upper} THEN
{probe}
            END IF;
"""


def _fallback_probe(leaf: LookupLeaf, probe: str) -> str:
    upper = _timestamp_literal(leaf.upper_anchor)
    return f"""
            IF v_effective_birth >= {upper} THEN
{probe}
            END IF;
"""


def _identity_probe(
    relation: str,
    *,
    location: str,
    id_column: str = 'task_id',
) -> str:
    if not is_safe_identifier(relation):
        raise ValueError(f'relation name is not a safe identifier: {relation!r}')
    return f"""
        SELECT {id_column}, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM {relation}
        WHERE {id_column} = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, '{location}', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::{TASK_LOOKUP_TYPE};
        END IF;
"""


def _provenance_live_probe() -> str:
    return f"""
        SELECT id INTO v_task_id
        FROM {LIVE_TASKS}
        WHERE id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'LIVE', v_task_id, NULL, NULL, NULL
            )::{TASK_PROVENANCE_TYPE};
        END IF;
"""


def _provenance_history_probe(relation: str) -> str:
    if not is_safe_identifier(relation):
        raise ValueError(f'relation name is not a safe identifier: {relation!r}')
    return f"""
        SELECT task_id, status, terminal_at, terminalization_kind
        INTO v_task_id, v_status, v_terminal_at, v_kind
        FROM {relation}
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_status, v_terminal_at, v_kind
            )::{TASK_PROVENANCE_TYPE};
        END IF;
"""


def _timestamp_literal(value: datetime) -> str:
    rendered = value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    return f"TIMESTAMPTZ '{rendered}'"
