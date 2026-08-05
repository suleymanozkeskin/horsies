"""Run condition-checked task-history prototype evidence scenarios."""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from tests.perf.prepare import apply_schema
from tests.task_history_prototypes.evidence import (
    EvidenceRunKind,
    PayloadShape,
    collect_administrative_result_evidence,
    collect_attempt_storage_evidence,
    collect_rerun_storage_evidence,
    evidence_json,
)
from tests.task_history_prototypes.identity_evidence import collect_identity_evidence
from tests.task_history_prototypes.recovery_evidence import (
    collect_pending_locator_evidence,
)
from tests.task_history_prototypes.transcode import ArchiveComponent
from tests.task_history_prototypes.transcode_evidence import (
    collect_archive_transcode_evidence,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='tests.task_history_prototypes')
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument(
        '--run-kind',
        type=EvidenceRunKind,
        choices=list(EvidenceRunKind),
        default=EvidenceRunKind.SMOKE,
    )
    parser.add_argument('--server-image', required=True)
    parser.add_argument('--host-description', required=True)
    parser.add_argument('--storage-description', required=True)
    parser.add_argument(
        '--scenario',
        required=True,
        choices=(
            'attempt-storage',
            'rerun-storage',
            'administrative-result',
            'identity-lookup',
            'pending-locator',
            'archive-transcode',
        ),
    )
    parser.add_argument('--rows', type=int, default=100)
    parser.add_argument('--result-bytes', type=int, default=200)
    parser.add_argument('--rerun-input-bytes', type=int, default=64 * 1024)
    parser.add_argument('--attempts-per-task', type=int, default=4)
    parser.add_argument('--batch-size', type=int, default=10_000)
    parser.add_argument(
        '--archive-component',
        type=ArchiveComponent,
        choices=list(ArchiveComponent),
        default=ArchiveComponent.RESULT,
    )
    parser.add_argument('--prior-result-bytes', type=int, default=200)
    parser.add_argument('--live-rows', type=int, default=1_000)
    parser.add_argument('--finite-history-rows', type=int, default=10_000)
    parser.add_argument('--forever-history-rows', type=int, default=1_000)
    parser.add_argument('--attached-finite-leaves', type=int, default=8)
    parser.add_argument(
        '--keyed-percent',
        type=int,
        choices=(0, 1, 10, 100),
        default=10,
    )
    parser.add_argument('--warm-observations', type=int, default=100)
    parser.add_argument('--cold-observations', type=int, default=10)
    parser.add_argument('--bootstrap-resamples', type=int, default=200)
    parser.add_argument(
        '--payload-shape',
        type=PayloadShape,
        choices=list(PayloadShape),
        default=PayloadShape.COMPRESSIBLE,
    )
    parser.add_argument('--seed', type=int, default=20260805)
    parser.add_argument('--demo-quiesced', action='store_true')
    parser.add_argument('--apply-schema', action='store_true')
    arguments = parser.parse_args(argv)

    if arguments.rows <= 0:
        parser.error('--rows must be positive')
    if arguments.apply_schema:
        apply_schema(arguments.dsn)

    evidence = asyncio.run(_run(arguments))
    print(evidence_json(evidence))
    return 0


async def _run(arguments: argparse.Namespace) -> object:
    engine = create_async_engine(arguments.dsn)
    try:
        async with engine.connect() as connection:
            match arguments.scenario:
                case 'attempt-storage':
                    return await collect_attempt_storage_evidence(
                        connection,
                        commit=arguments.commit,
                        run_kind=arguments.run_kind,
                        server_image=arguments.server_image,
                        host_description=arguments.host_description,
                        storage_description=arguments.storage_description,
                        demo_quiesced=arguments.demo_quiesced,
                        rows=arguments.rows,
                        result_bytes=arguments.result_bytes,
                        attempts_per_task=arguments.attempts_per_task,
                        payload_shape=arguments.payload_shape,
                        detail_observations=arguments.warm_observations,
                        bootstrap_resamples=arguments.bootstrap_resamples,
                        seed=arguments.seed,
                    )
                case 'rerun-storage':
                    return await collect_rerun_storage_evidence(
                        connection,
                        commit=arguments.commit,
                        run_kind=arguments.run_kind,
                        server_image=arguments.server_image,
                        host_description=arguments.host_description,
                        storage_description=arguments.storage_description,
                        demo_quiesced=arguments.demo_quiesced,
                        rows=arguments.rows,
                        result_bytes=arguments.result_bytes,
                        rerun_input_bytes=arguments.rerun_input_bytes,
                        payload_shape=arguments.payload_shape,
                        seed=arguments.seed,
                    )
                case 'administrative-result':
                    return await collect_administrative_result_evidence(
                        connection,
                        commit=arguments.commit,
                        run_kind=arguments.run_kind,
                        server_image=arguments.server_image,
                        host_description=arguments.host_description,
                        storage_description=arguments.storage_description,
                        demo_quiesced=arguments.demo_quiesced,
                        rows=arguments.rows,
                        prior_result_bytes=arguments.prior_result_bytes,
                        payload_shape=arguments.payload_shape,
                        seed=arguments.seed,
                    )
                case 'identity-lookup':
                    return await collect_identity_evidence(
                        connection,
                        commit=arguments.commit,
                        run_kind=arguments.run_kind,
                        server_image=arguments.server_image,
                        host_description=arguments.host_description,
                        storage_description=arguments.storage_description,
                        demo_quiesced=arguments.demo_quiesced,
                        live_rows=arguments.live_rows,
                        finite_history_rows=arguments.finite_history_rows,
                        forever_history_rows=arguments.forever_history_rows,
                        attached_finite_leaves=arguments.attached_finite_leaves,
                        keyed_percent=arguments.keyed_percent,
                        warm_observations_per_category=(
                            arguments.warm_observations
                        ),
                        cold_observations_per_category=(
                            arguments.cold_observations
                        ),
                        bootstrap_resamples=arguments.bootstrap_resamples,
                        seed=arguments.seed,
                    )
                case 'pending-locator':
                    return await collect_pending_locator_evidence(
                        connection,
                        commit=arguments.commit,
                        run_kind=arguments.run_kind,
                        server_image=arguments.server_image,
                        host_description=arguments.host_description,
                        storage_description=arguments.storage_description,
                        demo_quiesced=arguments.demo_quiesced,
                    )
                case 'archive-transcode':
                    return await collect_archive_transcode_evidence(
                        connection,
                        commit=arguments.commit,
                        run_kind=arguments.run_kind,
                        server_image=arguments.server_image,
                        host_description=arguments.host_description,
                        storage_description=arguments.storage_description,
                        demo_quiesced=arguments.demo_quiesced,
                        component=arguments.archive_component,
                        rows=arguments.rows,
                        batch_size=arguments.batch_size,
                        payload_bytes=arguments.result_bytes,
                        attempts_per_task=arguments.attempts_per_task,
                    )
                case _:
                    raise AssertionError('argparse accepted an unknown scenario')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    raise SystemExit(main())
