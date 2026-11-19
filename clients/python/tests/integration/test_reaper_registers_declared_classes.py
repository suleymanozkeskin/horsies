"""The retention periods that the config declares reach the worker's
maintenance pass.

The core's maintenance pass registers each period class of the retention
section of the worker manifest. A worker built from an `AppConfig` must carry
`AppConfig.retention` into that section, or every declaration is inert while
the registrar's own tests stay green. The test enters at `to_worker_manifest`,
the one place where the worker gets its config, and reads the section the core
receives.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import SecretStr

from horsies._catalog import worker_manifest as wire
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.retention import RetentionConfig
from horsies.core.worker_manifest import (
    CurrentDirectory,
    child_argv,
    child_sys_path,
    python_child_launch,
    to_worker_manifest,
)

DECLARED_DURATION = timedelta(days=11)


@pytest.mark.integration
class TestReaperRegistersDeclaredClasses:
    def test_the_worker_config_carries_retention_to_the_pass(self) -> None:
        """The retention split's seam: `AppConfig.retention` must reach the
        retention section of the worker manifest, which the core's pass
        reads to register the declared classes."""
        config = AppConfig(
            broker=PostgresConfig(database_url=SecretStr('postgresql://unused:unused@localhost/unused')),
            retention=RetentionConfig(retention_periods=(DECLARED_DURATION,)),
        )
        launch = python_child_launch(
            executable='/usr/bin/python3',
            interpreter_flags=(),
            app_locator='app.main:app',
            imports=(),
            sys_path_roots=(),
            parent_sys_path=child_sys_path([], CurrentDirectory('/srv/cwd')),
            parent_argv=child_argv([]),
            log_level='INFO',
        ).unwrap()

        manifest = to_worker_manifest(
            config,
            profile=None,
            metrics_endpoint=wire.MetricsEndpointNoMetricsEndpoint(),
            tasks={},
            child_launch=launch,
            idempotency_reservation_window=timedelta(hours=24),
        )

        assert manifest.retention.retention_periods == (DECLARED_DURATION.days,)
