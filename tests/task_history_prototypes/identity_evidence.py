"""Boundary-shape storage and point-lookup evidence for identity candidates."""

from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.perf.statistics import percentile_ms
from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_conditions,
)
from tests.task_history_prototypes.identity_schema import (
    extend_identity_history_leaves,
    install_identity_candidates,
    install_staged_lookup_prototype,
)
from tests.task_history_prototypes.measurements import (
    RelationFootprint,
    partition_tree_footprint,
    relation_footprint,
)
from tests.task_history_prototypes.qualification_io import (
    AtomicEvidenceWriter,
    QualificationProgress,
    QualificationProgressReporter,
    bounded_observation_milestones,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)


class IdentityCandidate(StrEnum):
    NO_DIRECTORY = 'no_directory'
    KEY_REGISTRY = 'key_registry'
    COMBINED_REGISTRY = 'combined_registry'
    STAGED_KEY_REGISTRY = 'staged_key_registry'


PRIMARY_IDENTITY_CANDIDATES = (
    IdentityCandidate.NO_DIRECTORY,
    IdentityCandidate.KEY_REGISTRY,
    IdentityCandidate.COMBINED_REGISTRY,
)


class LookupCategory(StrEnum):
    LIVE = 'live'
    RECENT_HISTORY = 'recent_history'
    OLDEST_HISTORY = 'oldest_history'
    FOREVER_HISTORY = 'forever_history'
    PURGED_IDENTITY = 'purged_identity'
    NEVER_SEEN = 'never_seen'


class LookupPosture(StrEnum):
    PREPARED_WARM = 'prepared_warm'
    UNPREPARED_WARM = 'unprepared_warm'
    BUFFER_COLD = 'buffer_cold'


class AbsoluteVerdict(StrEnum):
    PASS = 'PASS'
    FAIL = 'FAIL'
    INCONCLUSIVE = 'INCONCLUSIVE'


class IdentityCheckpointStatus(StrEnum):
    RUNNING = 'running'
    EXECUTION_FAILURE = 'execution_failure'
    COMPLETE = 'complete'


@dataclass(frozen=True, slots=True)
class LookupLatency:
    category: LookupCategory
    posture: LookupPosture
    observations: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    p99_ci_low_ms: float
    p99_ci_high_ms: float
    maximum_ms: float
    budget_ms: float
    verdict: AbsoluteVerdict


@dataclass(frozen=True, slots=True)
class IdentityCandidateEvidence:
    candidate: IdentityCandidate
    live_rows: int
    finite_history_rows: int
    forever_history_rows: int
    attached_finite_leaves: int
    keyed_percent: int
    load_seconds: float
    live_footprint: RelationFootprint
    history_footprint: RelationFootprint
    registry_footprint: RelationFootprint
    lookup: tuple[LookupLatency, ...]


@dataclass(frozen=True, slots=True)
class IdentityEvidence:
    conditions: EvidenceConditions
    warm_observations_per_category: int
    cold_observation_ladder: tuple[int, ...]
    cold_observations_maximum: int
    bootstrap_resamples: int
    ballast_bytes: int
    seed: int
    candidates: tuple[IdentityCandidateEvidence, ...]


@dataclass(frozen=True, slots=True)
class IdentityActiveCell:
    candidate: IdentityCandidate
    candidate_index: int
    candidate_total: int
    category: LookupCategory
    category_index: int
    category_total: int
    posture: LookupPosture
    cell_index: int
    cell_total: int
    rung: int | None
    observations_completed: int
    observation_target: int


@dataclass(frozen=True, slots=True)
class IdentityFinalizedCell:
    candidate: IdentityCandidate
    candidate_index: int
    candidate_total: int
    cell_index: int
    cell_total: int
    result: LookupLatency


@dataclass(frozen=True, slots=True)
class IdentityPartialEvidence:
    status: IdentityCheckpointStatus
    conditions: EvidenceConditions
    workload: dict[str, int | tuple[int, ...]]
    ballast_bytes: int | None
    finalized_cells: tuple[IdentityFinalizedCell, ...]
    active_cell: IdentityActiveCell | None


@dataclass(slots=True)
class _IdentityRunObserver:
    conditions: EvidenceConditions
    workload: dict[str, int | tuple[int, ...]]
    writer: AtomicEvidenceWriter
    progress: QualificationProgressReporter
    candidate_total: int = len(PRIMARY_IDENTITY_CANDIDATES)
    ballast_bytes: int | None = None
    finalized_cells: list[IdentityFinalizedCell] = field(
        default_factory=lambda: []
    )
    active_cell: IdentityActiveCell | None = None

    def checkpoint(
        self,
        status: IdentityCheckpointStatus = IdentityCheckpointStatus.RUNNING,
    ) -> None:
        self.writer.write(
            IdentityPartialEvidence(
                status=status,
                conditions=self.conditions,
                workload=self.workload,
                ballast_bytes=self.ballast_bytes,
                finalized_cells=tuple(self.finalized_cells),
                active_cell=self.active_cell,
            )
        )

    def emit_phase(
        self,
        phase: str,
        status: str,
        *,
        candidate: IdentityCandidate | None = None,
        candidate_index: int | None = None,
        candidate_total: int | None = None,
        live_rows: int | None = None,
        finite_history_rows: int | None = None,
        forever_history_rows: int | None = None,
    ) -> None:
        self.progress.emit(
            QualificationProgress(
                scenario='identity-lookup',
                phase=phase,
                status=status,
                candidate=None if candidate is None else candidate.value,
                candidate_index=candidate_index,
                candidate_total=candidate_total,
                live_rows=live_rows,
                finite_history_rows=finite_history_rows,
                forever_history_rows=forever_history_rows,
            )
        )

    def start_cell(
        self,
        *,
        candidate: IdentityCandidate,
        candidate_index: int,
        category: LookupCategory,
        posture: LookupPosture,
        cell_index: int,
        rung: int | None,
        observations_completed: int,
        observation_target: int,
    ) -> None:
        self.active_cell = IdentityActiveCell(
            candidate=candidate,
            candidate_index=candidate_index,
            candidate_total=self.candidate_total,
            category=category,
            category_index=tuple(LookupCategory).index(category) + 1,
            category_total=len(LookupCategory),
            posture=posture,
            cell_index=cell_index,
            cell_total=len(LookupCategory) * len(LookupPosture),
            rung=rung,
            observations_completed=observations_completed,
            observation_target=observation_target,
        )
        self.checkpoint()
        self._emit_cell('started')

    def record_observations(self, completed: int) -> None:
        if self.active_cell is None:
            raise AssertionError('observation progress requires an active cell')
        active = self.active_cell
        self.active_cell = IdentityActiveCell(
            candidate=active.candidate,
            candidate_index=active.candidate_index,
            candidate_total=active.candidate_total,
            category=active.category,
            category_index=active.category_index,
            category_total=active.category_total,
            posture=active.posture,
            cell_index=active.cell_index,
            cell_total=active.cell_total,
            rung=active.rung,
            observations_completed=completed,
            observation_target=active.observation_target,
        )
        self._emit_cell('running')

    def finalize_cell(self, result: LookupLatency) -> None:
        if self.active_cell is None:
            raise AssertionError('cell finalization requires an active cell')
        active = self.active_cell
        if result.observations != active.observations_completed:
            raise AssertionError('finalized observations differ from checkpoint')
        self.finalized_cells.append(
            IdentityFinalizedCell(
                candidate=active.candidate,
                candidate_index=active.candidate_index,
                candidate_total=active.candidate_total,
                cell_index=active.cell_index,
                cell_total=active.cell_total,
                result=result,
            )
        )
        self._emit_cell(result.verdict.value)
        self.active_cell = None
        self.checkpoint()

    def report_interim(self, verdict: AbsoluteVerdict) -> None:
        if verdict is not AbsoluteVerdict.INCONCLUSIVE:
            raise ValueError('only an inconclusive cell may advance a rung')
        self._emit_cell(verdict.value)

    def _emit_cell(self, status: str) -> None:
        if self.active_cell is None:
            raise AssertionError('cell progress requires an active cell')
        active = self.active_cell
        self.progress.emit(
            QualificationProgress(
                scenario='identity-lookup',
                phase='lookup-cell',
                status=status,
                candidate=active.candidate.value,
                candidate_index=active.candidate_index,
                candidate_total=active.candidate_total,
                category=active.category.value,
                category_index=active.category_index,
                category_total=active.category_total,
                posture=active.posture.value,
                cell_index=active.cell_index,
                cell_total=active.cell_total,
                rung=active.rung,
                observations=active.observations_completed,
                observation_target=active.observation_target,
            )
        )


_PREPARED_BUDGETS = {
    LookupCategory.LIVE: 10.0,
    LookupCategory.RECENT_HISTORY: 20.0,
    LookupCategory.OLDEST_HISTORY: 20.0,
    LookupCategory.FOREVER_HISTORY: 20.0,
    LookupCategory.PURGED_IDENTITY: 50.0,
    LookupCategory.NEVER_SEEN: 50.0,
}
_UNPREPARED_BUDGETS = {
    LookupCategory.LIVE: 15.0,
    LookupCategory.RECENT_HISTORY: 40.0,
    LookupCategory.OLDEST_HISTORY: 40.0,
    LookupCategory.FOREVER_HISTORY: 40.0,
    LookupCategory.PURGED_IDENTITY: 100.0,
    LookupCategory.NEVER_SEEN: 100.0,
}
_COLD_BUDGETS = {
    LookupCategory.LIVE: 25.0,
    LookupCategory.RECENT_HISTORY: 75.0,
    LookupCategory.OLDEST_HISTORY: 100.0,
    LookupCategory.FOREVER_HISTORY: 100.0,
    LookupCategory.PURGED_IDENTITY: 150.0,
    LookupCategory.NEVER_SEEN: 150.0,
}
BOUNDARY_COLD_OBSERVATION_LADDER = (500, 2_000, 10_000)


async def collect_identity_evidence(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    live_rows: int,
    finite_history_rows: int,
    forever_history_rows: int,
    attached_finite_leaves: int,
    keyed_percent: int,
    warm_observations_per_category: int,
    cold_observations_per_category: int,
    bootstrap_resamples: int,
    seed: int,
    candidates: tuple[IdentityCandidate, ...] = PRIMARY_IDENTITY_CANDIDATES,
    checkpoint_path: Path | None = None,
    progress: QualificationProgressReporter | None = None,
) -> IdentityEvidence:
    if not candidates:
        raise ValueError('identity evidence requires at least one candidate')
    if len(set(candidates)) != len(candidates):
        raise ValueError('identity evidence candidates must be distinct')
    _validate_workload(
        live_rows=live_rows,
        finite_history_rows=finite_history_rows,
        forever_history_rows=forever_history_rows,
        attached_finite_leaves=attached_finite_leaves,
        keyed_percent=keyed_percent,
        warm_observations_per_category=warm_observations_per_category,
        cold_observations_per_category=cold_observations_per_category,
        bootstrap_resamples=bootstrap_resamples,
        run_kind=run_kind,
    )
    cold_observation_ladder = _cold_observation_ladder(
        run_kind,
        maximum=cold_observations_per_category,
    )
    conditions = await collect_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture=(
            'prepared and unprepared warm runs; buffer-cold runs scan '
            'recorded ballast larger than shared_buffers before each lookup'
        ),
        prepared_posture=(
            'explicit server PREPARE for prepared runs; unique statement text '
            'for every unprepared and buffer-cold observation'
        ),
    )
    observer = _IdentityRunObserver(
        conditions=conditions,
        workload={
            'live_rows': live_rows,
            'finite_history_rows': finite_history_rows,
            'forever_history_rows': forever_history_rows,
            'attached_finite_leaves': attached_finite_leaves,
            'keyed_percent': keyed_percent,
            'warm_observations_per_category': warm_observations_per_category,
            'cold_observation_ladder': cold_observation_ladder,
            'cold_observations_maximum': cold_observations_per_category,
            'bootstrap_resamples': bootstrap_resamples,
            'seed': seed,
        },
        writer=AtomicEvidenceWriter(checkpoint_path),
        progress=progress or QualificationProgressReporter(),
        candidate_total=len(candidates),
    )
    observer.checkpoint()
    observer.emit_phase('schema', 'started')
    schema = PrototypeSchema(f'history_identity_evidence_{uuid4().hex[:8]}')
    await install_archive_candidates(connection, schema)
    await install_identity_candidates(connection, schema)
    await extend_identity_history_leaves(
        connection,
        schema,
        target_leaf_count=attached_finite_leaves,
    )
    if IdentityCandidate.STAGED_KEY_REGISTRY in candidates:
        await install_staged_lookup_prototype(
            connection,
            schema,
            prefix='key_registry',
            newest_first_leaf_names=tuple(
                f'key_registry_history_finite_{year}'
                for year in range(
                    2026 + attached_finite_leaves - 1,
                    2025,
                    -1,
                )
            ),
        )
    await connection.commit()
    observer.emit_phase('schema', 'complete')
    try:
        ballast_bytes = await _install_ballast(connection, schema)
        observer.ballast_bytes = ballast_bytes
        observer.checkpoint()
        observer.emit_phase('ballast', 'complete')
        measured_candidates: list[IdentityCandidateEvidence] = []
        for candidate_index, candidate in enumerate(candidates, start=1):
            observer.emit_phase(
                'candidate',
                'started',
                candidate=candidate,
                candidate_index=candidate_index,
                candidate_total=len(candidates),
            )
            measured = await _measure_identity_candidate(
                connection,
                schema,
                candidate=candidate,
                candidate_index=candidate_index,
                candidate_total=len(candidates),
                live_rows=live_rows,
                finite_history_rows=finite_history_rows,
                forever_history_rows=forever_history_rows,
                attached_finite_leaves=attached_finite_leaves,
                keyed_percent=keyed_percent,
                warm_observations_per_category=warm_observations_per_category,
                cold_observation_ladder=cold_observation_ladder,
                ballast_bytes=ballast_bytes,
                bootstrap_resamples=bootstrap_resamples,
                seed=seed,
                observer=observer,
            )
            measured_candidates.append(measured)
            observer.emit_phase(
                'candidate',
                'complete',
                candidate=candidate,
                candidate_index=candidate_index,
                candidate_total=len(candidates),
            )
            await _truncate_candidate(connection, schema, candidate)
            await connection.commit()
        evidence = IdentityEvidence(
            conditions=conditions,
            warm_observations_per_category=warm_observations_per_category,
            cold_observation_ladder=cold_observation_ladder,
            cold_observations_maximum=cold_observations_per_category,
            bootstrap_resamples=bootstrap_resamples,
            ballast_bytes=ballast_bytes,
            seed=seed,
            candidates=tuple(measured_candidates),
        )
    except BaseException:
        observer.checkpoint(IdentityCheckpointStatus.EXECUTION_FAILURE)
        raise
    finally:
        try:
            await connection.rollback()
            await remove_archive_candidates(connection, schema)
            await connection.commit()
        except BaseException:
            observer.checkpoint(IdentityCheckpointStatus.EXECUTION_FAILURE)
            raise
    observer.checkpoint(IdentityCheckpointStatus.COMPLETE)
    observer.emit_phase('qualification', 'complete')
    return evidence


def _cold_observation_ladder(
    run_kind: EvidenceRunKind,
    *,
    maximum: int,
) -> tuple[int, ...]:
    match run_kind:
        case EvidenceRunKind.SMOKE:
            return (maximum,)
        case EvidenceRunKind.GATE:
            if maximum != BOUNDARY_COLD_OBSERVATION_LADDER[-1]:
                raise ValueError(
                    'identity gate evidence requires the declared '
                    '500/2,000/10,000 buffer-cold observation ladder with '
                    '10,000 as its maximum'
                )
            return BOUNDARY_COLD_OBSERVATION_LADDER


def _validate_workload(
    *,
    live_rows: int,
    finite_history_rows: int,
    forever_history_rows: int,
    attached_finite_leaves: int,
    keyed_percent: int,
    warm_observations_per_category: int,
    cold_observations_per_category: int,
    bootstrap_resamples: int,
    run_kind: EvidenceRunKind,
) -> None:
    if live_rows <= 0:
        raise ValueError('live rows must be positive')
    if finite_history_rows < attached_finite_leaves * 3:
        raise ValueError('finite history must provide at least three rows per leaf')
    if forever_history_rows <= 0:
        raise ValueError('forever history rows must be positive')
    if not 8 <= attached_finite_leaves <= 512:
        raise ValueError('attached finite leaves must be between 8 and 512')
    if keyed_percent not in {0, 1, 10, 100}:
        raise ValueError('keyed percent must be one of 0, 1, 10, or 100')
    if warm_observations_per_category <= 0:
        raise ValueError('warm observations must be positive')
    if cold_observations_per_category <= 0:
        raise ValueError('cold observations must be positive')
    if bootstrap_resamples <= 0:
        raise ValueError('bootstrap resamples must be positive')
    if run_kind is EvidenceRunKind.GATE:
        if (
            live_rows < 100_000
            or finite_history_rows < 10_000_000
            or forever_history_rows < 1_000_000
            or attached_finite_leaves != 512
        ):
            raise ValueError(
                'identity gate evidence requires the declared boundary data shape'
            )
        if warm_observations_per_category < 10_000:
            raise ValueError(
                'identity gate evidence requires 10,000 warm observations '
                'per category'
            )
        if bootstrap_resamples < 1_000:
            raise ValueError('identity gate evidence requires 1,000 resamples')


async def _measure_identity_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: IdentityCandidate,
    candidate_index: int,
    candidate_total: int,
    live_rows: int,
    finite_history_rows: int,
    forever_history_rows: int,
    attached_finite_leaves: int,
    keyed_percent: int,
    warm_observations_per_category: int,
    cold_observation_ladder: tuple[int, ...],
    ballast_bytes: int,
    bootstrap_resamples: int,
    seed: int,
    observer: _IdentityRunObserver,
) -> IdentityCandidateEvidence:
    prefix = _storage_prefix(candidate)
    await _truncate_candidate(connection, schema, candidate)
    await connection.commit()
    started = time.perf_counter()
    await _seed_live(
        connection,
        schema,
        candidate=candidate,
        rows=live_rows,
        keyed_percent=keyed_percent,
    )
    await _seed_finite_history(
        connection,
        schema,
        candidate=candidate,
        rows=finite_history_rows,
        leaves=attached_finite_leaves,
        keyed_percent=keyed_percent,
    )
    await _seed_forever_history(
        connection,
        schema,
        candidate=candidate,
        rows=forever_history_rows,
        keyed_percent=keyed_percent,
    )
    await _seed_registry(
        connection,
        schema,
        candidate=candidate,
        live_rows=live_rows,
        finite_history_rows=finite_history_rows,
        forever_history_rows=forever_history_rows,
        leaves=attached_finite_leaves,
        keyed_percent=keyed_percent,
    )
    purged_ids = _sample_ids(
        count=max(warm_observations_per_category, cold_observation_ladder[-1]),
        upper=finite_history_rows,
        leaves=attached_finite_leaves,
        seed=seed + 9,
    )
    await _remove_purged_history(connection, schema, candidate, purged_ids)
    await connection.commit()
    load_seconds = time.perf_counter() - started
    observer.emit_phase(
        'seed',
        'complete',
        candidate=candidate,
        candidate_index=candidate_index,
        candidate_total=candidate_total,
        live_rows=live_rows,
        finite_history_rows=finite_history_rows,
        forever_history_rows=forever_history_rows,
    )
    await _analyze_candidate(connection, schema, candidate)
    await connection.commit()

    live_footprint = await relation_footprint(
        connection,
        f'{schema.name}.{prefix}_live',
    )
    history_footprint = await partition_tree_footprint(
        connection,
        f'{schema.name}.{prefix}_history',
    )
    registry_footprint = await _registry_footprint(connection, schema, candidate)
    identifiers = _lookup_identifiers(
        live_rows=live_rows,
        finite_history_rows=finite_history_rows,
        forever_history_rows=forever_history_rows,
        leaves=attached_finite_leaves,
        warm_count=warm_observations_per_category,
        cold_count=cold_observation_ladder[-1],
        purged_ids=purged_ids,
        seed=seed,
    )
    await _verify_lookup_semantics(connection, schema, candidate, identifiers)
    lookup = await _measure_lookups(
        connection,
        schema,
        candidate=candidate,
        candidate_index=candidate_index,
        identifiers=identifiers,
        warm_observations_per_category=warm_observations_per_category,
        cold_observation_ladder=cold_observation_ladder,
        ballast_bytes=ballast_bytes,
        bootstrap_resamples=bootstrap_resamples,
        bootstrap_seed=seed,
        observer=observer,
    )
    return IdentityCandidateEvidence(
        candidate=candidate,
        live_rows=live_rows,
        finite_history_rows=finite_history_rows,
        forever_history_rows=forever_history_rows,
        attached_finite_leaves=attached_finite_leaves,
        keyed_percent=keyed_percent,
        load_seconds=load_seconds,
        live_footprint=live_footprint,
        history_footprint=history_footprint,
        registry_footprint=registry_footprint,
        lookup=lookup,
    )


def _storage_prefix(candidate: IdentityCandidate) -> str:
    match candidate:
        case IdentityCandidate.NO_DIRECTORY:
            return 'no_directory'
        case IdentityCandidate.KEY_REGISTRY:
            return 'key_registry'
        case IdentityCandidate.STAGED_KEY_REGISTRY:
            return 'key_registry'
        case IdentityCandidate.COMBINED_REGISTRY:
            return 'combined'


def _lookup_function_name(candidate: IdentityCandidate) -> str:
    match candidate:
        case IdentityCandidate.STAGED_KEY_REGISTRY:
            return 'key_registry_staged'
        case _:
            return candidate.value


async def _truncate_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
) -> None:
    prefix = _storage_prefix(candidate)
    await connection.execute(
        text(
            f'TRUNCATE {schema.sql}.{prefix}_live, '
            f'{schema.sql}.{prefix}_history'
        )
    )
    match candidate:
        case IdentityCandidate.NO_DIRECTORY:
            pass
        case (
            IdentityCandidate.KEY_REGISTRY
            | IdentityCandidate.STAGED_KEY_REGISTRY
        ):
            await connection.execute(
                text(f'TRUNCATE {schema.sql}.key_reservations')
            )
        case IdentityCandidate.COMBINED_REGISTRY:
            await connection.execute(
                text(f'TRUNCATE {schema.sql}.combined_registry')
            )


def _key_predicate(keyed_percent: int) -> str:
    return f'mod(series - 1, 100) < {keyed_percent}'


def _digest_sql(domain: str) -> str:
    return (
        f"decode(md5('{domain}-a-' || series::text) "
        f"|| md5('{domain}-b-' || series::text), 'hex')"
    )


async def _seed_live(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: IdentityCandidate,
    rows: int,
    keyed_percent: int,
) -> None:
    prefix = _storage_prefix(candidate)
    if candidate is IdentityCandidate.NO_DIRECTORY:
        key_columns = (
            ', idempotency_key_digest, key_scope_version, '
            'idempotency_window, idempotency_expires_at'
        )
        key_values = f""",
            CASE WHEN {_key_predicate(keyed_percent)}
                 THEN {_digest_sql('live-key')} END,
            CASE WHEN {_key_predicate(keyed_percent)} THEN 1::smallint END,
            CASE WHEN {_key_predicate(keyed_percent)}
                 THEN interval '24 hours' END,
            CASE WHEN {_key_predicate(keyed_percent)}
                 THEN 'infinity'::timestamptz END
        """
    else:
        key_columns = ''
        key_values = ''
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.{prefix}_live (
                task_id, task_name, fingerprint_version,
                command_fingerprint, retention_class_key, created_at
                {key_columns}
            )
            SELECT md5('live-' || series::text)::uuid::text,
                   'prototype.task', 1, {_digest_sql('live-fingerprint')},
                   'finite_30d_v1', '2026-08-05T00:00:00Z'::timestamptz
                   {key_values}
            FROM generate_series(1, :rows) AS series
            """
        ),
        {'rows': rows},
    )


async def _seed_finite_history(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: IdentityCandidate,
    rows: int,
    leaves: int,
    keyed_percent: int,
) -> None:
    await _seed_history(
        connection,
        schema,
        candidate=candidate,
        row_kind='finite',
        rows=rows,
        retention_class_key='finite_30d_v1',
        terminal_expression=(
            "make_timestamptz(2026 + mod(series - 1, :leaves)::integer, "
            "6, 1, 0, 0, 0, 'UTC')"
        ),
        leaves=leaves,
        keyed_percent=keyed_percent,
    )


async def _seed_forever_history(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: IdentityCandidate,
    rows: int,
    keyed_percent: int,
) -> None:
    await _seed_history(
        connection,
        schema,
        candidate=candidate,
        row_kind='forever',
        rows=rows,
        retention_class_key='forever',
        terminal_expression="'2026-08-05T00:00:00Z'::timestamptz",
        leaves=1,
        keyed_percent=keyed_percent,
    )


async def _seed_history(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: IdentityCandidate,
    row_kind: str,
    rows: int,
    retention_class_key: str,
    terminal_expression: str,
    leaves: int,
    keyed_percent: int,
) -> None:
    prefix = _storage_prefix(candidate)
    if candidate is IdentityCandidate.NO_DIRECTORY:
        key_columns = (
            ', idempotency_key_digest, key_scope_version, '
            'idempotency_window, idempotency_expires_at'
        )
        key_values = f""",
            CASE WHEN {_key_predicate(keyed_percent)}
                 THEN {_digest_sql(f'{row_kind}-key')} END,
            CASE WHEN {_key_predicate(keyed_percent)} THEN 1::smallint END,
            CASE WHEN {_key_predicate(keyed_percent)}
                 THEN interval '24 hours' END,
            CASE WHEN {_key_predicate(keyed_percent)}
                 THEN 'infinity'::timestamptz END
        """
    else:
        key_columns = ''
        key_values = ''
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.{prefix}_history (
                task_id, task_name, fingerprint_version,
                command_fingerprint, retention_class_key, terminal_at
                {key_columns}
            )
            SELECT md5('{row_kind}-' || series::text)::uuid::text,
                   'prototype.task', 1,
                   {_digest_sql(f'{row_kind}-fingerprint')},
                   :retention_class_key, {terminal_expression}
                   {key_values}
            FROM generate_series(1, :rows) AS series
            """
        ),
        {
            'rows': rows,
            'leaves': leaves,
            'retention_class_key': retention_class_key,
        },
    )


async def _seed_registry(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: IdentityCandidate,
    live_rows: int,
    finite_history_rows: int,
    forever_history_rows: int,
    leaves: int,
    keyed_percent: int,
) -> None:
    match candidate:
        case IdentityCandidate.NO_DIRECTORY:
            return
        case (
            IdentityCandidate.KEY_REGISTRY
            | IdentityCandidate.STAGED_KEY_REGISTRY
        ):
            if keyed_percent == 0:
                return
            for row_kind, rows, disposition in (
                ('live', live_rows, 'LIVE'),
                ('finite', finite_history_rows, 'TERMINAL'),
                ('forever', forever_history_rows, 'TERMINAL'),
            ):
                await connection.execute(
                    text(
                        f"""
                        INSERT INTO {schema.sql}.key_reservations (
                            idempotency_key_digest, key_scope_version,
                            fingerprint_version, command_fingerprint,
                            task_id, disposition, reservation_window,
                            expires_at
                        )
                        SELECT {_digest_sql(f'{row_kind}-key')}, 1, 1,
                               {_digest_sql(f'{row_kind}-fingerprint')},
                               md5('{row_kind}-' || series::text)::uuid::text,
                               :disposition, interval '24 hours',
                               'infinity'::timestamptz
                        FROM generate_series(1, :rows) AS series
                        WHERE {_key_predicate(keyed_percent)}
                        """
                    ),
                    {'rows': rows, 'disposition': disposition},
                )
        case IdentityCandidate.COMBINED_REGISTRY:
            await _seed_combined_registry_part(
                connection,
                schema,
                row_kind='live',
                rows=live_rows,
                location='LIVE',
                retention_class_key='finite_30d_v1',
                anchor_expression='NULL::timestamptz',
                keyed_percent=keyed_percent,
            )
            await _seed_combined_registry_part(
                connection,
                schema,
                row_kind='finite',
                rows=finite_history_rows,
                location='HISTORY',
                retention_class_key='finite_30d_v1',
                anchor_expression=(
                    'make_timestamptz(2026 + '
                    'mod(series - 1, :leaves)::integer, '
                    "6, 1, 0, 0, 0, 'UTC')"
                ),
                keyed_percent=keyed_percent,
                leaves=leaves,
            )
            await _seed_combined_registry_part(
                connection,
                schema,
                row_kind='forever',
                rows=forever_history_rows,
                location='HISTORY',
                retention_class_key='forever',
                anchor_expression="'2026-08-05T00:00:00Z'::timestamptz",
                keyed_percent=keyed_percent,
            )


async def _seed_combined_registry_part(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    row_kind: str,
    rows: int,
    location: str,
    retention_class_key: str,
    anchor_expression: str,
    keyed_percent: int,
    leaves: int = 1,
) -> None:
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.combined_registry (
                task_id, idempotency_key_digest, key_scope_version,
                fingerprint_version, command_fingerprint, location,
                retention_class_key, retention_anchor_at, key_window,
                key_expires_at
            )
            SELECT md5('{row_kind}-' || series::text)::uuid::text,
                   CASE WHEN {_key_predicate(keyed_percent)}
                        THEN {_digest_sql(f'{row_kind}-key')} END,
                   CASE WHEN {_key_predicate(keyed_percent)} THEN 1::smallint END,
                   1, {_digest_sql(f'{row_kind}-fingerprint')},
                   :location, :retention_class_key, {anchor_expression},
                   CASE WHEN {_key_predicate(keyed_percent)}
                        THEN interval '24 hours' END,
                   CASE WHEN {_key_predicate(keyed_percent)}
                        THEN 'infinity'::timestamptz END
            FROM generate_series(1, :rows) AS series
            """
        ),
        {
            'rows': rows,
            'location': location,
            'retention_class_key': retention_class_key,
            'leaves': leaves,
        },
    )


async def _remove_purged_history(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
    purged_ids: tuple[str, ...],
) -> None:
    prefix = _storage_prefix(candidate)
    await connection.execute(
        text(
            f"""
            DELETE FROM {schema.sql}.{prefix}_history
            WHERE task_id = ANY(CAST(:task_ids AS varchar[]))
            """
        ),
        {'task_ids': list(purged_ids)},
    )


async def _analyze_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
) -> None:
    prefix = _storage_prefix(candidate)
    await connection.execute(text(f'ANALYZE {schema.sql}.{prefix}_live'))
    await connection.execute(text(f'ANALYZE {schema.sql}.{prefix}_history'))
    match candidate:
        case IdentityCandidate.NO_DIRECTORY:
            pass
        case (
            IdentityCandidate.KEY_REGISTRY
            | IdentityCandidate.STAGED_KEY_REGISTRY
        ):
            await connection.execute(
                text(f'ANALYZE {schema.sql}.key_reservations')
            )
        case IdentityCandidate.COMBINED_REGISTRY:
            await connection.execute(
                text(f'ANALYZE {schema.sql}.combined_registry')
            )


async def _registry_footprint(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
) -> RelationFootprint:
    match candidate:
        case IdentityCandidate.NO_DIRECTORY:
            return RelationFootprint(0, 0, 0, 0)
        case (
            IdentityCandidate.KEY_REGISTRY
            | IdentityCandidate.STAGED_KEY_REGISTRY
        ):
            relation = f'{schema.name}.key_reservations'
        case IdentityCandidate.COMBINED_REGISTRY:
            relation = f'{schema.name}.combined_registry'
    return await relation_footprint(connection, relation)


def _lookup_identifiers(
    *,
    live_rows: int,
    finite_history_rows: int,
    forever_history_rows: int,
    leaves: int,
    warm_count: int,
    cold_count: int,
    purged_ids: tuple[str, ...],
    seed: int,
) -> dict[LookupCategory, tuple[str, ...]]:
    count = max(warm_count, cold_count)
    generator = random.Random(seed)
    recent_numbers = _numbers_for_leaf(
        leaf_offset=leaves - 1,
        leaves=leaves,
        upper=finite_history_rows,
    )
    oldest_numbers = _numbers_for_leaf(
        leaf_offset=0,
        leaves=leaves,
        upper=finite_history_rows,
    )
    return {
        LookupCategory.LIVE: tuple(
            _task_id('live', generator.randint(1, live_rows)) for _ in range(count)
        ),
        LookupCategory.RECENT_HISTORY: tuple(
            _task_id('finite', generator.choice(recent_numbers)) for _ in range(count)
        ),
        LookupCategory.OLDEST_HISTORY: tuple(
            _task_id('finite', generator.choice(oldest_numbers)) for _ in range(count)
        ),
        LookupCategory.FOREVER_HISTORY: tuple(
            _task_id('forever', generator.randint(1, forever_history_rows))
            for _ in range(count)
        ),
        LookupCategory.PURGED_IDENTITY: purged_ids[:count],
        LookupCategory.NEVER_SEEN: tuple(
            _task_id('never', number) for number in range(1, count + 1)
        ),
    }


def _numbers_for_leaf(*, leaf_offset: int, leaves: int, upper: int) -> tuple[int, ...]:
    first = leaf_offset + 1
    return tuple(range(first, upper + 1, leaves))


def _sample_ids(
    *,
    count: int,
    upper: int,
    leaves: int,
    seed: int,
) -> tuple[str, ...]:
    generator = random.Random(seed)
    available = _numbers_for_leaf(leaf_offset=1, leaves=leaves, upper=upper)
    if count > len(available):
        raise ValueError('purged identity sample exceeds finite history rows')
    return tuple(_task_id('finite', number) for number in generator.sample(available, count))


def _task_id(kind: str, number: int) -> str:
    digest = hashlib.md5(
        f'{kind}-{number}'.encode(),
        usedforsecurity=False,
    ).hexdigest()
    return str(UUID(digest))


async def _verify_lookup_semantics(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
    identifiers: dict[LookupCategory, tuple[str, ...]],
) -> None:
    for category, task_ids in identifiers.items():
        observed = await _lookup_found(
            connection,
            schema,
            candidate,
            task_ids[0],
        )
        expected = category not in {
            LookupCategory.PURGED_IDENTITY,
            LookupCategory.NEVER_SEEN,
        }
        if observed is not expected:
            raise RuntimeError(
                f'{candidate.value} {category.value} lookup returned {observed}'
            )


async def _lookup_found(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
    task_id: str,
) -> bool:
    function_name = _lookup_function_name(candidate)
    return bool(
        (
            await connection.execute(
                text(
                    f"""
                    SELECT (located).found
                    FROM (
                        SELECT {schema.sql}.lookup_{function_name}(
                            CAST(:task_id AS varchar(36))
                        ) AS located
                    ) AS lookup
                    """
                ),
                {'task_id': task_id},
            )
        ).scalar_one()
    )


async def _measure_lookups(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: IdentityCandidate,
    candidate_index: int,
    identifiers: dict[LookupCategory, tuple[str, ...]],
    warm_observations_per_category: int,
    cold_observation_ladder: tuple[int, ...],
    ballast_bytes: int,
    bootstrap_resamples: int,
    bootstrap_seed: int,
    observer: _IdentityRunObserver,
) -> tuple[LookupLatency, ...]:
    function_name = _lookup_function_name(candidate)
    prepared_name = f'{schema.name}_{candidate.value}_lookup'
    await connection.execute(
        text(
            f"""
            PREPARE {prepared_name}(varchar(36)) AS
            SELECT (located).found
            FROM (
                SELECT {schema.sql}.lookup_{function_name}($1) AS located
            ) AS lookup
            """
        )
    )
    results: list[LookupLatency] = []
    try:
        for category_index, (category, task_ids) in enumerate(
            identifiers.items()
        ):
            prepared_cell_index = category_index * len(LookupPosture) + 1
            observer.start_cell(
                candidate=candidate,
                candidate_index=candidate_index,
                category=category,
                posture=LookupPosture.PREPARED_WARM,
                cell_index=prepared_cell_index,
                rung=None,
                observations_completed=0,
                observation_target=warm_observations_per_category,
            )
            prepared = await _prepared_timings(
                connection,
                prepared_name,
                task_ids[:warm_observations_per_category],
                observer=observer,
            )
            prepared_result = _latency_result(
                category,
                LookupPosture.PREPARED_WARM,
                prepared,
                _PREPARED_BUDGETS[category],
                resamples=bootstrap_resamples,
                seed=bootstrap_seed,
            )
            observer.finalize_cell(prepared_result)
            results.append(prepared_result)

            observer.start_cell(
                candidate=candidate,
                candidate_index=candidate_index,
                category=category,
                posture=LookupPosture.UNPREPARED_WARM,
                cell_index=prepared_cell_index + 1,
                rung=None,
                observations_completed=0,
                observation_target=warm_observations_per_category,
            )
            unprepared = await _unprepared_timings(
                connection,
                schema,
                candidate,
                category,
                task_ids[:warm_observations_per_category],
                observer=observer,
            )
            unprepared_result = _latency_result(
                category,
                LookupPosture.UNPREPARED_WARM,
                unprepared,
                _UNPREPARED_BUDGETS[category],
                resamples=bootstrap_resamples,
                seed=bootstrap_seed + 1,
            )
            observer.finalize_cell(unprepared_result)
            results.append(unprepared_result)

            cold_result = await _measure_cold_ladder(
                connection,
                schema,
                candidate,
                candidate_index,
                category,
                prepared_cell_index + 2,
                task_ids,
                cold_observation_ladder,
                ballast_bytes=ballast_bytes,
                bootstrap_resamples=bootstrap_resamples,
                bootstrap_seed=bootstrap_seed + 2,
                observer=observer,
            )
            results.append(cold_result)
    finally:
        await connection.execute(text(f'DEALLOCATE {prepared_name}'))
    return tuple(results)


async def _prepared_timings(
    connection: AsyncConnection,
    statement_name: str,
    task_ids: tuple[str, ...],
    *,
    observer: _IdentityRunObserver,
) -> list[float]:
    samples: list[float] = []
    milestones = bounded_observation_milestones(len(task_ids))
    for observation, task_id in enumerate(task_ids, start=1):
        _validate_task_id(task_id)
        started = time.perf_counter_ns()
        await connection.execute(text(f"EXECUTE {statement_name}('{task_id}')"))
        samples.append((time.perf_counter_ns() - started) / 1_000_000)
        if observation in milestones:
            observer.record_observations(observation)
    return samples


async def _unprepared_timings(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
    category: LookupCategory,
    task_ids: tuple[str, ...],
    *,
    observer: _IdentityRunObserver,
) -> list[float]:
    function_name = _lookup_function_name(candidate)
    samples: list[float] = []
    milestones = bounded_observation_milestones(len(task_ids))
    for observation, task_id in enumerate(task_ids):
        started = time.perf_counter_ns()
        await connection.execute(
            text(
                f"""
                SELECT (located).found
                FROM (
                    SELECT {schema.sql}.lookup_{function_name}(
                        CAST(:task_id AS varchar(36))
                    ) AS located
                ) AS lookup
                /* unprepared {category.value} observation {observation} */
                """
            ),
            {'task_id': task_id},
        )
        samples.append((time.perf_counter_ns() - started) / 1_000_000)
        completed = observation + 1
        if completed in milestones:
            observer.record_observations(completed)
    return samples


async def _measure_cold_ladder(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
    candidate_index: int,
    category: LookupCategory,
    cell_index: int,
    task_ids: tuple[str, ...],
    observation_ladder: tuple[int, ...],
    *,
    ballast_bytes: int,
    bootstrap_resamples: int,
    bootstrap_seed: int,
    observer: _IdentityRunObserver,
) -> LookupLatency:
    samples: list[float] = []
    for rung_index, target in enumerate(observation_ladder, start=1):
        observer.start_cell(
            candidate=candidate,
            candidate_index=candidate_index,
            category=category,
            posture=LookupPosture.BUFFER_COLD,
            cell_index=cell_index,
            rung=rung_index,
            observations_completed=len(samples),
            observation_target=target,
        )
        samples.extend(
            await _cold_timings(
                connection,
                schema,
                candidate,
                category,
                task_ids[len(samples) : target],
                observation_offset=len(samples),
                observation_target=target,
                ballast_bytes=ballast_bytes,
                observer=observer,
            )
        )
        result = _latency_result(
            category,
            LookupPosture.BUFFER_COLD,
            samples,
            _COLD_BUDGETS[category],
            resamples=bootstrap_resamples,
            seed=bootstrap_seed,
        )
        if result.verdict is not AbsoluteVerdict.INCONCLUSIVE:
            observer.finalize_cell(result)
            return result
        if target == observation_ladder[-1]:
            observer.finalize_cell(result)
            return result
        observer.report_interim(AbsoluteVerdict.INCONCLUSIVE)
    raise AssertionError('cold observation ladder must contain a rung')


async def _cold_timings(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    candidate: IdentityCandidate,
    category: LookupCategory,
    task_ids: tuple[str, ...],
    *,
    observation_offset: int,
    observation_target: int,
    ballast_bytes: int,
    observer: _IdentityRunObserver,
) -> list[float]:
    if ballast_bytes <= 0:
        raise RuntimeError('buffer-cold measurement requires non-empty ballast')
    samples: list[float] = []
    function_name = _lookup_function_name(candidate)
    milestones = _bounded_range_milestones(
        observation_offset,
        observation_target,
    )
    for observation, task_id in enumerate(task_ids, start=observation_offset):
        await connection.execute(
            text(
                f"""
                SELECT sum(
                    series + octet_length(a) + octet_length(b)
                    + octet_length(c) + octet_length(d)
                )
                FROM {schema.sql}.lookup_ballast
                """
            )
        )
        started = time.perf_counter_ns()
        await connection.execute(
            text(
                f"""
                SELECT (located).found
                FROM (
                    SELECT {schema.sql}.lookup_{function_name}(
                        CAST(:task_id AS varchar(36))
                    ) AS located
                ) AS lookup
                /* buffer-cold {category.value} observation {observation} */
                """
            ),
            {'task_id': task_id},
        )
        samples.append((time.perf_counter_ns() - started) / 1_000_000)
        completed = observation + 1
        if completed in milestones:
            observer.record_observations(completed)
    return samples


def _bounded_range_milestones(start: int, target: int) -> set[int]:
    if not 0 <= start < target:
        raise ValueError('observation range must advance')
    remaining = target - start
    return {
        start + max(1, round(remaining * index / 10))
        for index in range(1, 11)
    }


def _latency_result(
    category: LookupCategory,
    posture: LookupPosture,
    samples: list[float],
    budget_ms: float,
    *,
    resamples: int,
    seed: int,
) -> LookupLatency:
    p99 = percentile_ms(samples, 99)
    ci_low, ci_high = _absolute_percentile_interval(
        samples,
        percentile=99,
        resamples=resamples,
        seed=seed,
    )
    if ci_high <= budget_ms:
        verdict = AbsoluteVerdict.PASS
    elif ci_low > budget_ms:
        verdict = AbsoluteVerdict.FAIL
    else:
        verdict = AbsoluteVerdict.INCONCLUSIVE
    return LookupLatency(
        category=category,
        posture=posture,
        observations=len(samples),
        p50_ms=percentile_ms(samples, 50),
        p95_ms=percentile_ms(samples, 95),
        p99_ms=p99,
        p99_ci_low_ms=ci_low,
        p99_ci_high_ms=ci_high,
        maximum_ms=max(samples),
        budget_ms=budget_ms,
        verdict=verdict,
    )


def _absolute_percentile_interval(
    samples: list[float],
    *,
    percentile: float,
    resamples: int,
    seed: int,
) -> tuple[float, float]:
    generator = random.Random(seed)
    estimates = sorted(
        percentile_ms(
            generator.choices(samples, k=len(samples)),
            percentile,
        )
        for _ in range(resamples)
    )
    low_index = max(0, round(0.025 * len(estimates)) - 1)
    high_index = min(len(estimates) - 1, round(0.975 * len(estimates)) - 1)
    return estimates[low_index], estimates[high_index]


async def _install_ballast(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> int:
    shared_buffers = (
        await connection.execute(
            text("SELECT pg_size_bytes(current_setting('shared_buffers'))")
        )
    ).scalar_one()
    target_bytes = shared_buffers * 2
    estimated_row_bytes = 168
    rows = target_bytes // estimated_row_bytes + 1
    await connection.execute(
        text(
            f"""
            CREATE UNLOGGED TABLE {schema.sql}.lookup_ballast AS
            SELECT series::bigint,
                   md5('a-' || series::text) AS a,
                   md5('b-' || series::text) AS b,
                   md5('c-' || series::text) AS c,
                   md5('d-' || series::text) AS d
            FROM generate_series(1, :rows) AS series
            """
        ),
        {'rows': rows},
    )
    await connection.commit()
    footprint = await relation_footprint(
        connection,
        f'{schema.name}.lookup_ballast',
    )
    if footprint.heap_bytes <= shared_buffers:
        raise RuntimeError('lookup ballast does not exceed shared_buffers')
    return footprint.heap_bytes


def _validate_task_id(task_id: str) -> None:
    if str(UUID(task_id)) != task_id:
        raise ValueError('prepared lookup task ID must be a canonical UUID')
