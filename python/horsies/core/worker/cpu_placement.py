"""Where the worker process and its children run: ``ParentCpu``, a setting of
the worker profile. The core places the processes.
"""

from __future__ import annotations

import enum


class ParentCpu(enum.Enum):
    """Where the worker process runs, a setting of the worker profile.

    SHARED: the worker process and its children run on the CPUs they inherit.
    OWN_CORE: the worker process runs on one physical core, both SMT threads;
    the children run on the other CPUs that the worker may use. Linux only.
    """

    SHARED = 'SHARED'
    OWN_CORE = 'OWN_CORE'


