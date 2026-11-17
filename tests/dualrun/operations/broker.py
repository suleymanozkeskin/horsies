"""The operation table of the broker switch.

One row per Python entry point of the broker module that calls the core.
Every operation of this switch runs in pool mode: the core runs its own
statements on its own pool. Run: ``pytest tests/integration --dual-run=broker``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode

BROKER_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='database ping',
        production_path='horsies.core.brokers.postgres:PostgresBroker.ping_database_async',
        reference_path='tests.dualrun.reference.ping_caller:ping_database_async',
        mode=PoolMode(),
        core_methods=('ping_database_async',),
    ),
    DualRunOperation(
        name='schema install',
        production_path='horsies.core.brokers.postgres:PostgresBroker.ensure_schema_initialized',
        reference_path='tests.dualrun.reference.install_caller:ensure_schema_initialized',
        mode=PoolMode(),
        core_methods=('install_schema_async',),
    ),
    DualRunOperation(
        name='schema install before an operation',
        production_path='horsies.core.brokers.postgres:PostgresBroker._ensure_initialized',
        reference_path='tests.dualrun.reference.install_caller:ensure_initialized',
        mode=PoolMode(),
        core_methods=('install_schema_async',),
    ),
)
