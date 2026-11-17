"""The operation table of the history switch.

One row per Python entry point of the history and retention module that
calls the core. The coverage and pruning passes are in pool mode: the core
opens and commits its own transactions on its session-work pool. Run:
``pytest tests/integration --dual-run=history``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode

HISTORY_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='periodic partition coverage',
        production_path='horsies.core.worker.reaper:ReaperMixin._run_partition_coverage',
        reference_path='tests.dualrun.reference.coverage_caller:run_partition_coverage',
        mode=PoolMode(),
        core_methods=('ensure_partition_coverage_async',),
    ),
    DualRunOperation(
        name='startup partition coverage',
        production_path='horsies.core.worker.worker:Worker._ensure_startup_coverage',
        reference_path='tests.dualrun.reference.coverage_caller:ensure_startup_coverage',
        mode=PoolMode(),
        core_methods=('ensure_partition_coverage_async',),
    ),
    DualRunOperation(
        name='partition pruning',
        production_path='horsies.core.worker.reaper:ReaperMixin._run_partition_pruning',
        reference_path='tests.dualrun.reference.pruning_caller:run_partition_pruning',
        mode=PoolMode(),
        core_methods=('prune_expired_partitions_async',),
    ),
)
