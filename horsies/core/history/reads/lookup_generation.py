"""Generation of the staged task-lookup function from the leaf catalog.

The generated function is the qualified boundary-gate shape: PL/pgSQL that
probes the live table, then the forever child, then finite leaves as static
direct table references. Each reached statement is eligible for plan
caching, and no statement ever references the partitioned parent — the
parent's plan-time fan-out over ~514 relations is the measured failure the
staged form replaced.

UUIDv7 identifiers prune the finite walk. The embedded birth time is decoded
after the live and forever probes miss; a leaf is excluded only when its
terminal-time upper bound lies at or before the birth minus the five-second
forward-clock bound the database enforces at enqueue — a client clock that
is behind reduces pruning only, never produces false absence. When every
attached leaf carries verified birth metadata, a birth before the global
floor returns absence in constant time. Candidate leaves are probed
oldest-first: a task terminalizes at or after its birth, so the oldest
non-excluded leaf is the most likely location. Legacy identifiers walk
every leaf newest-first, unconditionally.

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
    LIVE_TASKS,
    TASK_HISTORY_FOREVER,
    TASK_LOOKUP_FUNCTION,
    TASK_LOOKUP_MANIFEST,
    TASK_LOOKUP_TYPE,
    TASK_PROVENANCE_FUNCTION,
    TASK_PROVENANCE_TYPE,
)
from ..partitions.catalog import LeafCatalogRow


CLOCK_BOUND_SECONDS = 5
"""The forward-clock bound shared with enqueue-time database validation."""


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
    """The complete attached finite-leaf set, oldest first.

    `birth_floor` is set only when every attached leaf carried verified
    birth metadata; a single unverified leaf disables the constant-time
    purged fast path and the walk decides absence the long way.
    """

    leaves: tuple[LookupLeaf, ...]
    birth_floor: datetime | None


def manifest_from_catalog(rows: Sequence[LeafCatalogRow]) -> LookupManifest:
    """Build the generation manifest from attached catalog rows.

    The rows must be the complete attached set across every retention
    class — the caller reads them through the catalog module's
    all-classes query. Ordering is normalized here; duplicate relation
    names are a contract violation the constructor surfaces.
    """
    ordered = sorted(rows, key=lambda row: (row.lower_anchor, row.leaf_name))
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
    )
    birth_floor: datetime | None = None
    if leaves and all(row.min_birth_verified for row in ordered):
        observed = [leaf.min_birth_at for leaf in leaves if leaf.min_birth_at]
        if observed:
            birth_floor = min(observed)
    return LookupManifest(leaves=leaves, birth_floor=birth_floor)


def render_staged_lookup_function(manifest: LookupManifest) -> str:
    """Render the identity lookup: found rows carry fingerprint facts."""
    return _staged_function(
        function_name=TASK_LOOKUP_FUNCTION,
        return_type=TASK_LOOKUP_TYPE,
        declares=(
            'v_task_id uuid;',
            'v_fingerprint_version smallint;',
            'v_fingerprint bytea;',
        ),
        absence_values='NULL, NULL, NULL, NULL',
        live_probe=_identity_probe(LIVE_TASKS, location='LIVE'),
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
    return _staged_function(
        function_name=TASK_PROVENANCE_FUNCTION,
        return_type=TASK_PROVENANCE_TYPE,
        declares=(
            'v_task_id uuid;',
            'v_status text;',
            'v_terminal_at timestamptz;',
            'v_kind text;',
        ),
        absence_values='NULL, NULL, NULL, NULL, NULL',
        live_probe=_provenance_live_probe(),
        history_probe=_provenance_history_probe,
        manifest=manifest,
    )


def _staged_function(
    *,
    function_name: str,
    return_type: str,
    declares: tuple[str, ...],
    absence_values: str,
    live_probe: str,
    history_probe: Callable[[str], str],
    manifest: LookupManifest,
) -> str:
    """The one staged skeleton both generated functions are rendered from."""
    absence = f'RETURN ROW(FALSE, {absence_values})::{return_type};'
    forever_probe = history_probe(TASK_HISTORY_FOREVER)

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
        ELSE
{legacy_probes}
        END IF;
"""

    declare_block = '\n'.join(f'        {declare}' for declare in declares)
    return f"""
    CREATE OR REPLACE FUNCTION {function_name}(p_task_id uuid)
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
{forever_probe}
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


def _identity_probe(relation: str, *, location: str) -> str:
    if not is_safe_identifier(relation):
        raise ValueError(f'relation name is not a safe identifier: {relation!r}')
    return f"""
        SELECT task_id, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM {relation}
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, '{location}', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::{TASK_LOOKUP_TYPE};
        END IF;
"""


def _provenance_live_probe() -> str:
    return f"""
        SELECT task_id INTO v_task_id
        FROM {LIVE_TASKS}
        WHERE task_id = p_task_id;
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
