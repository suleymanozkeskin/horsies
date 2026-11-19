"""The ``parent_cpu`` setting of a worker profile (``ParentCpu``) reaches the
worker manifest. The core places the worker and its children."""

from __future__ import annotations

import pytest

from horsies.core.models.worker_profile import (
    IsolatedChildren,
    WorkerProfile,
    describe_worker_profile,
)
from horsies.core.worker.cpu_placement import ParentCpu
from horsies.core.worker_manifest import worker_profile_manifest

pytestmark = [pytest.mark.unit]

def test_the_profile_setting_reaches_the_worker_settings() -> None:
    """The setting reaches the profile of the worker manifest, which the core
    worker runs (``worker_profile_manifest``)."""
    profile = WorkerProfile(isolated=IsolatedChildren(count=2), parent_cpu=ParentCpu.OWN_CORE)

    assert worker_profile_manifest(profile).parent_cpu == 'OWN_CORE'
    assert 'parent_cpu=OWN_CORE' in describe_worker_profile(profile)
    assert WorkerProfile(isolated=IsolatedChildren(count=2)).parent_cpu is ParentCpu.SHARED
