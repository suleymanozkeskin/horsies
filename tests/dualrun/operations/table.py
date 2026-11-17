"""The types of an operation table of a module switch.

During a switch, each production entry point of the module calls the core. Its
Python body before the switch stays as a reference under
``tests/dualrun/reference/``. A table names each pair, so that the integration
tests can run once with the references in the place of the production entry
points (the Python run) and once as shipped (the core run).

A path is ``'<module>:<attribute path>'``, for example
``'horsies.core.brokers.postgres:PostgresBroker.requeue_stale_claimed'``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True, slots=True)
class PoolMode:
    """The core opens and commits its own transactions on a core pool for the
    same database. The test's own sessions read the committed state."""


type OperationMode = PoolMode
"""The modes that a table uses. The statement and the connection modes come
with the first module whose operations run inside a caller's transaction."""


class SwitchModule(Enum):
    """A module whose switch has an operation table."""

    LIFECYCLE = 'lifecycle'
    BROKER = 'broker'
    MONITORING_AND_WEB = 'monitoring-and-web'
    WORKFLOW_ENGINE = 'workflow-engine'


class Implementation(Enum):
    """The implementation that one run of a test uses."""

    PYTHON_REFERENCE = 'python'
    """The reference of each operation is in the place of its production entry point."""

    CORE = 'core'
    """Production as shipped: the entry points call the core."""


@dataclass(frozen=True, slots=True)
class DualRunOperation:
    """One switched entry point and its Python reference.

    ``production_path`` and ``reference_path`` name functions with the same
    signature. ``core_methods`` names the ``CoreHandle`` methods that the
    production entry point calls, and ``core_functions`` the functions of
    ``horsies._core`` that it calls (a pure call needs no handle): in the
    Python run each one raises ``TripwireHit``.
    """

    name: str
    production_path: str
    reference_path: str
    mode: OperationMode
    core_methods: tuple[str, ...]
    core_functions: tuple[str, ...] = ()


class TripwireHit(BaseException):
    """A run called the implementation of the other run: the rebinding missed
    a call path.

    A ``BaseException``, so that a production ``except Exception`` does not
    hide it. ``args`` is ``(operation, implementation, caller)``.
    """
