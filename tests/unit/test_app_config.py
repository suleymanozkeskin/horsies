"""Unit tests for AppConfig validation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    MultipleValidationErrors,
)
from horsies.core.models.app import AppConfig
from datetime import timedelta

from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.retention import RetentionConfig
from horsies.core.models.schedule import (
    DailySchedule,
    HourlySchedule,
    IntervalSchedule,
    MonthlySchedule,
    ScheduleConfig,
    TaskSchedule,
    Weekday,
    WeeklySchedule,
)
from horsies.core.utils.url import mask_database_url

from datetime import time as datetime_time


# Shared broker config for all tests
BROKER = PostgresConfig(
    database_url='postgresql+psycopg://user:pass@localhost/db',
    pool_size=5,
    max_overflow=5,
)

# Recovery config with short heartbeat for tests using small claim_lease_ms values.
# Satisfies the lease >= 2x heartbeat constraint (min_lease = 2000ms).
SHORT_HB_RECOVERY = RecoveryConfig(claimer_heartbeat_interval_ms=1_000)


@pytest.mark.unit
class TestPostgresConfigPgBouncer:
    """Tests for split PostgreSQL URL configuration."""

    def test_worker_pool_defaults_are_smaller_than_producer_pool(self) -> None:
        config = PostgresConfig(
            database_url='postgresql+psycopg://user:pass@localhost/db',
        )

        # Producer defaults follow SQLAlchemy's (spec change, 0.1.7);
        # the old 30+30 let one producer pin 60 server connections.
        assert config.pool_size == 5
        assert config.max_overflow == 10
        assert config.worker_pool_size == 3
        assert config.worker_max_overflow == 2
        assert config.worker_child_pool_min_size == 0
        assert config.worker_child_pool_max_size == 2

        worker_config = config.worker_runtime_config()

        assert worker_config.pool_size == 3
        assert worker_config.max_overflow == 2
        assert worker_config.database_url == config.database_url

    def test_worker_pool_can_inherit_producer_pool(self) -> None:
        config = PostgresConfig(
            database_url='postgresql+psycopg://user:pass@localhost/db',
            pool_size=11,
            max_overflow=7,
            worker_pool_size=None,
            worker_max_overflow=None,
        )

        worker_config = config.worker_runtime_config()

        assert worker_config.pool_size == 11
        assert worker_config.max_overflow == 7

    def test_sqlalchemy_engine_kwargs_excludes_worker_only_fields(self) -> None:
        config = PostgresConfig(
            database_url='postgresql+psycopg://user:pass@localhost/db',
        )

        kwargs = config.sqlalchemy_engine_kwargs()

        assert 'worker_pool_size' not in kwargs
        assert 'worker_max_overflow' not in kwargs
        assert 'worker_child_pool_min_size' not in kwargs
        assert 'worker_child_pool_max_size' not in kwargs

    @pytest.mark.parametrize(
        ('field', 'value'),
        [
            ('worker_pool_size', 0),
            ('worker_max_overflow', -1),
            ('worker_child_pool_min_size', -1),
            ('worker_child_pool_max_size', 0),
        ],
    )
    def test_worker_pool_rejects_invalid_bounds(
        self,
        field: str,
        value: int,
    ) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            PostgresConfig(
                database_url='postgresql+psycopg://user:pass@localhost/db',
                **{field: value},
            )

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_BROKER_POOL

    def test_worker_child_pool_min_must_not_exceed_max(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            PostgresConfig(
                database_url='postgresql+psycopg://user:pass@localhost/db',
                worker_child_pool_min_size=3,
                worker_child_pool_max_size=2,
            )

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_BROKER_POOL

    def test_effective_session_database_url_defaults_to_database_url(self) -> None:
        config = PostgresConfig(
            database_url='postgresql+psycopg://user:pass@localhost/db',
        )

        # database_url is SecretStr (0.1.7); the effective property unwraps.
        assert (
            config.effective_session_database_url
            == config.database_url.get_secret_value()
        )

    def test_split_database_urls_are_valid(self) -> None:
        config = PostgresConfig(
            database_url='postgresql+psycopg://user:pass@pooler:6432/db',
            session_database_url='postgresql+psycopg://user:pass@direct:5432/db',
            pgbouncer_transaction_mode=True,
        )

        assert config.effective_session_database_url == (
            'postgresql+psycopg://user:pass@direct:5432/db'
        )
        # Keepalives default on; PgBouncer mode also disables prepared statements.
        assert config.pooled_connect_args == {
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 3,
            'prepare_threshold': None,
        }

    def test_database_urls_never_leak_via_repr_or_dump(self) -> None:
        """Credential-bearing URLs are SecretStr: repr/str/model_dump mask.

        Regression for the defense-in-depth gap where any adopter logging
        the config object (or a debugger/error tracker capturing locals)
        got the cleartext password.
        """
        password = 'sup3r-secret-pw'
        config = PostgresConfig(
            database_url=f'postgresql+psycopg://user:{password}@pooler/db',
            session_database_url=(
                f'postgresql+psycopg://user:{password}@direct/db'
            ),
        )

        assert password not in repr(config)
        assert password not in str(config)
        assert password not in str(config.model_dump())
        # The unwrap path still yields the real URL for engine construction.
        assert password in config.database_url.get_secret_value()
        assert password in config.effective_session_database_url

    def test_worker_config_repr_masks_dsns(self) -> None:
        """WorkerConfig auto-repr carried all three DSNs in cleartext."""
        from horsies.core.worker.config import WorkerConfig

        password = 'sup3r-secret-pw'
        cfg = WorkerConfig(
            dsn=f'postgresql+psycopg://user:{password}@host/db',
            psycopg_dsn=f'postgresql://user:{password}@host/db',
            session_dsn=f'postgresql+psycopg://user:{password}@direct/db',
            queues=['default'],
        )

        assert password not in repr(cfg)
        assert 'default' in repr(cfg)

    def test_pgbouncer_mode_requires_session_database_url(self) -> None:
        with pytest.raises(ConfigurationError, match='session_database_url required'):
            PostgresConfig(
                database_url='postgresql+psycopg://user:pass@pooler:6432/db',
                pgbouncer_transaction_mode=True,
            )

    @pytest.mark.parametrize(
        'url',
        [
            'postgresql+psycopg2://user:pass@localhost/db',
            'postgresql+psycopgx://user:pass@localhost/db',
            'postgresql://user:pass@localhost/db',
        ],
    )
    def test_database_url_requires_exact_psycopg3_scheme(self, url: str) -> None:
        with pytest.raises(ConfigurationError, match='invalid database URL scheme'):
            PostgresConfig(database_url=url)

    def test_session_database_url_requires_exact_psycopg3_scheme(self) -> None:
        with pytest.raises(
            ConfigurationError, match='invalid session database URL scheme'
        ):
            PostgresConfig(
                database_url='postgresql+psycopg://user:pass@localhost/db',
                session_database_url='postgresql+psycopg2://user:pass@localhost/db',
            )


@pytest.mark.unit
class TestPostgresConfigKeepalives:
    """TCP keepalive configuration on broker/child connections.

    Regression for idle pooled connections reaped mid-query: pool_pre_ping
    and pool_recycle are checkout-time guards and cannot catch an in-flight
    connection death (GH issue #100).
    """

    _LOCAL_URL = 'postgresql+psycopg://user:pass@localhost/db'

    def test_keepalives_default_on(self) -> None:
        config = PostgresConfig(database_url=self._LOCAL_URL)

        assert config.tcp_keepalives is True
        assert config.keepalive_connect_args() == {
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 3,
        }

    def test_keepalives_present_in_pooled_connect_args_without_pgbouncer(
        self,
    ) -> None:
        config = PostgresConfig(database_url=self._LOCAL_URL)

        # Direct (non-PgBouncer) URLs still get keepalives, and no
        # prepare_threshold knob.
        assert config.pooled_connect_args == {
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 3,
        }

    def test_keepalives_disabled_yields_empty_connect_args(self) -> None:
        config = PostgresConfig(
            database_url=self._LOCAL_URL,
            tcp_keepalives=False,
        )

        assert config.keepalive_connect_args() == {}
        assert config.pooled_connect_args == {}

    def test_custom_keepalive_values_flow_through(self) -> None:
        config = PostgresConfig(
            database_url=self._LOCAL_URL,
            tcp_keepalives_idle=15,
            tcp_keepalives_interval=5,
            tcp_keepalives_count=2,
        )

        assert config.pooled_connect_args == {
            'keepalives': 1,
            'keepalives_idle': 15,
            'keepalives_interval': 5,
            'keepalives_count': 2,
        }

    @pytest.mark.parametrize(
        'field_name',
        [
            'tcp_keepalives_idle',
            'tcp_keepalives_interval',
            'tcp_keepalives_count',
        ],
    )
    def test_non_positive_keepalive_values_rejected_when_enabled(
        self, field_name: str
    ) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            PostgresConfig(
                database_url=self._LOCAL_URL,
                **{field_name: 0},
            )

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_KEEPALIVE

    def test_non_positive_values_allowed_when_keepalives_disabled(self) -> None:
        # Disabled keepalives ignore the interval/idle/count values entirely.
        config = PostgresConfig(
            database_url=self._LOCAL_URL,
            tcp_keepalives=False,
            tcp_keepalives_idle=0,
        )

        assert config.pooled_connect_args == {}

    def test_keepalive_fields_excluded_from_engine_kwargs(self) -> None:
        config = PostgresConfig(database_url=self._LOCAL_URL)
        engine_kwargs = config.sqlalchemy_engine_kwargs()

        for field_name in (
            'tcp_keepalives',
            'tcp_keepalives_idle',
            'tcp_keepalives_interval',
            'tcp_keepalives_count',
        ):
            assert field_name not in engine_kwargs


@pytest.mark.unit
class TestWorkerConfigPgBouncerChildKwargs:
    """child_connect_kwargs stays consistent with pgbouncer_transaction_mode.

    Regression: decoupling the child pool from the boolean must not let a
    direct WorkerConfig(pgbouncer_transaction_mode=True) ship a child pool
    with prepared statements enabled against transaction-pooled PgBouncer.
    """

    def test_boolean_alone_disables_child_prepared_statements(self) -> None:
        from horsies.core.worker.config import WorkerConfig

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@pooler:6432/db',
            psycopg_dsn='postgresql://u:p@direct/db',
            queues=['default'],
            pgbouncer_transaction_mode=True,
        )

        assert cfg.child_connect_kwargs == {'prepare_threshold': None}

    def test_merges_with_existing_keepalive_kwargs(self) -> None:
        from horsies.core.worker.config import WorkerConfig

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@pooler:6432/db',
            psycopg_dsn='postgresql://u:p@direct/db',
            queues=['default'],
            pgbouncer_transaction_mode=True,
            child_connect_kwargs={'keepalives': 1, 'keepalives_idle': 30},
        )

        assert cfg.child_connect_kwargs == {
            'keepalives': 1,
            'keepalives_idle': 30,
            'prepare_threshold': None,
        }

    def test_no_pgbouncer_leaves_child_kwargs_untouched(self) -> None:
        from horsies.core.worker.config import WorkerConfig

        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@host/db',
            psycopg_dsn='postgresql://u:p@host/db',
            queues=['default'],
            child_connect_kwargs={'keepalives': 1},
        )

        assert cfg.child_connect_kwargs == {'keepalives': 1}


@pytest.mark.unit
class TestPrefetchBufferValidation:
    """Tests for prefetch_buffer configuration validation."""

    def test_prefetch_buffer_zero_is_valid(self) -> None:
        """prefetch_buffer=0 (hard cap mode) should be valid."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=0,
        )
        assert config.prefetch_buffer == 0

    def test_prefetch_buffer_negative_raises(self) -> None:
        """prefetch_buffer < 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='prefetch_buffer must be non-negative'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=-1,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_prefetch_buffer_positive_requires_claim_lease(self) -> None:
        """prefetch_buffer > 0 without claim_lease_ms should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='claim_lease_ms required'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=4,
                claim_lease_ms=None,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_prefetch_buffer_positive_with_claim_lease_is_valid(self) -> None:
        """prefetch_buffer > 0 with valid claim_lease_ms should be valid."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=4,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.prefetch_buffer == 4
        assert config.claim_lease_ms == 5000


@pytest.mark.unit
class TestClaimLeaseMsValidation:
    """Tests for claim_lease_ms configuration validation."""

    def test_claim_lease_ms_none_is_valid_with_no_prefetch(self) -> None:
        """claim_lease_ms=None is valid when prefetch_buffer=0."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=0,
            claim_lease_ms=None,
        )
        assert config.claim_lease_ms is None

    def test_claim_lease_ms_with_no_prefetch_accepted(self) -> None:
        """claim_lease_ms is allowed in hard-cap mode (overrides default 60s lease)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=0,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.claim_lease_ms == 5000

    def test_claim_lease_ms_zero_raises(self) -> None:
        """claim_lease_ms=0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='claim_lease_ms must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=4,
                claim_lease_ms=0,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_claim_lease_ms_negative_raises(self) -> None:
        """claim_lease_ms < 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='claim_lease_ms must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=4,
                claim_lease_ms=-1000,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_claim_lease_ms_positive_is_valid(self) -> None:
        """claim_lease_ms > 0 should be valid."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=4,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.claim_lease_ms == 5000


@pytest.mark.unit
class TestMaxClaimRenewAgeMsValidation:
    """Tests for max_claim_renew_age_ms configuration validation."""

    def test_default_value_is_valid(self) -> None:
        """Default max_claim_renew_age_ms (180_000) should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.max_claim_renew_age_ms == 180_000

    def test_zero_raises(self) -> None:
        """max_claim_renew_age_ms=0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError,
            match='max_claim_renew_age_ms must be positive',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                max_claim_renew_age_ms=0,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_negative_raises(self) -> None:
        """max_claim_renew_age_ms < 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError,
            match='max_claim_renew_age_ms must be positive',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                max_claim_renew_age_ms=-1000,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_less_than_effective_lease_raises(self) -> None:
        """max_claim_renew_age_ms < effective lease should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError,
            match='max_claim_renew_age_ms must be >= effective claim lease',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                claim_lease_ms=10_000,
                max_claim_renew_age_ms=5_000,
                recovery=RecoveryConfig(claimer_heartbeat_interval_ms=2_000),
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_equal_to_effective_lease_is_valid(self) -> None:
        """max_claim_renew_age_ms == effective lease is the minimum valid value."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            claim_lease_ms=5_000,
            max_claim_renew_age_ms=5_000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.max_claim_renew_age_ms == 5_000

    def test_greater_than_effective_lease_is_valid(self) -> None:
        """max_claim_renew_age_ms > effective lease should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            claim_lease_ms=5_000,
            max_claim_renew_age_ms=60_000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.max_claim_renew_age_ms == 60_000

    def test_uses_default_lease_when_claim_lease_ms_none(self) -> None:
        """When claim_lease_ms is None, effective lease is DEFAULT_CLAIM_LEASE_MS (60s).

        max_claim_renew_age_ms must be >= 60_000 in that case.
        """
        with pytest.raises(
            ConfigurationError,
            match='max_claim_renew_age_ms must be >= effective claim lease',
        ):
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                claim_lease_ms=None,
                max_claim_renew_age_ms=30_000,
            )


@pytest.mark.unit
class TestClusterWideCapPrefetchConflict:
    """Tests for cluster_wide_cap and prefetch_buffer conflict validation."""

    def test_cluster_wide_cap_with_no_prefetch_is_valid(self) -> None:
        """cluster_wide_cap with prefetch_buffer=0 should be valid (hard cap mode)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            cluster_wide_cap=50,
            prefetch_buffer=0,
        )
        assert config.cluster_wide_cap == 50
        assert config.prefetch_buffer == 0

    def test_cluster_wide_cap_with_prefetch_raises(self) -> None:
        """cluster_wide_cap with prefetch_buffer > 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap incompatible with prefetch mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=50,
                prefetch_buffer=4,
                claim_lease_ms=5000,
                recovery=SHORT_HB_RECOVERY,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_no_cluster_cap_with_prefetch_is_valid(self) -> None:
        """No cluster_wide_cap with prefetch_buffer > 0 should be valid (soft cap mode)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            cluster_wide_cap=None,
            prefetch_buffer=4,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.cluster_wide_cap is None
        assert config.prefetch_buffer == 4


@pytest.mark.unit
class TestClusterWideCapBoundaries:
    """Tests for cluster_wide_cap boundary values."""

    def test_cluster_wide_cap_zero_raises(self) -> None:
        """cluster_wide_cap=0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=0,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_cluster_wide_cap_negative_raises(self) -> None:
        """cluster_wide_cap=-1 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_cluster_wide_cap_minimum_valid(self) -> None:
        """cluster_wide_cap=1 should be the minimum valid value."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            cluster_wide_cap=1,
        )
        assert config.cluster_wide_cap == 1


@pytest.mark.unit
class TestQueueModeValidation:
    """Tests for queue_mode and custom_queues validation."""

    def test_default_mode_with_custom_queues_raises(self) -> None:
        """DEFAULT mode with custom_queues provided should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='custom_queues must be None in DEFAULT mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                custom_queues=[CustomQueueConfig(name='q1')],
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_custom_mode_with_none_queues_raises(self) -> None:
        """CUSTOM mode with custom_queues=None should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='custom_queues required in CUSTOM mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.CUSTOM,
                broker=BROKER,
                custom_queues=None,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_custom_mode_with_empty_queues_raises(self) -> None:
        """CUSTOM mode with empty custom_queues should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='custom_queues required in CUSTOM mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.CUSTOM,
                broker=BROKER,
                custom_queues=[],
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_custom_mode_with_duplicate_names_raises(self) -> None:
        """CUSTOM mode with duplicate queue names should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='duplicate queue names in custom_queues'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.CUSTOM,
                broker=BROKER,
                custom_queues=[
                    CustomQueueConfig(name='q1'),
                    CustomQueueConfig(name='q1'),
                ],
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_max_concurrency_none_means_uncapped(self) -> None:
        """max_concurrency=None is the explicit uncapped sentinel."""
        queue = CustomQueueConfig(name='bulk', max_concurrency=None)
        assert queue.max_concurrency is None

    def test_max_concurrency_zero_pauses_claiming(self) -> None:
        """0 is a valid edge: the queue is claimable-from by nobody."""
        queue = CustomQueueConfig(name='drained', max_concurrency=0)
        assert queue.max_concurrency == 0

    def test_max_concurrency_negative_rejected(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            CustomQueueConfig(name='bad', max_concurrency=-1)

    def test_format_for_logging_uncapped_queue_labelled(self) -> None:
        """None renders as 'uncapped' in the startup banner, not 'None'."""
        config = AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=BROKER,
            custom_queues=[
                CustomQueueConfig(name='bulk', max_concurrency=None),
            ],
        )
        formatted = config._format_for_logging()
        assert 'max_concurrency=uncapped' in formatted
        assert 'max_concurrency=None' not in formatted

    def test_custom_mode_with_valid_queues(self) -> None:
        """CUSTOM mode with valid unique queues should succeed."""
        config = AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=BROKER,
            custom_queues=[
                CustomQueueConfig(name='high'),
                CustomQueueConfig(name='low'),
            ],
        )
        assert config.custom_queues is not None
        assert len(config.custom_queues) == 2

    def test_default_mode_without_custom_queues(self) -> None:
        """DEFAULT mode without custom_queues should succeed."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.queue_mode == QueueMode.DEFAULT
        assert config.custom_queues is None

    def test_custom_mode_queue_names_preserved(self) -> None:
        """Queue names should be preserved exactly as provided."""
        config = AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=BROKER,
            custom_queues=[
                CustomQueueConfig(name='priority'),
                CustomQueueConfig(name='background'),
            ],
        )
        assert config.custom_queues is not None
        names = [q.name for q in config.custom_queues]
        assert names == ['priority', 'background']


@pytest.mark.unit
class TestDefaultBehavior:
    """Tests for default configuration behavior."""

    def test_defaults_to_hard_cap_mode(self) -> None:
        """Default config should use hard cap mode (prefetch_buffer=0)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.prefetch_buffer == 0
        assert config.claim_lease_ms is None

    def test_cluster_wide_cap_defaults_to_none(self) -> None:
        """cluster_wide_cap should default to None (unlimited)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.cluster_wide_cap is None

    def test_queue_mode_defaults_to_default(self) -> None:
        """queue_mode should default to DEFAULT."""
        config = AppConfig(broker=BROKER)
        assert config.queue_mode == QueueMode.DEFAULT

    def test_custom_queues_defaults_to_none(self) -> None:
        """custom_queues should default to None."""
        config = AppConfig(broker=BROKER)
        assert config.custom_queues is None

    def test_default_unhandled_error_code_default(self) -> None:
        """default_unhandled_error_code should default to 'UNHANDLED_EXCEPTION'."""
        config = AppConfig(broker=BROKER)
        assert config.default_unhandled_error_code == 'UNHANDLED_EXCEPTION'


@pytest.mark.unit
class TestMultiErrorCollection:
    """Tests for phase-gated error collection in AppConfig."""

    def test_multiple_independent_errors_collected(self) -> None:
        """Multiple independent config errors are collected together."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
            )
        errors = exc_info.value.report.errors
        assert len(errors) == 2
        messages = [e.message for e in errors]
        assert 'cluster_wide_cap must be positive' in messages
        assert 'prefetch_buffer must be non-negative' in messages

    def test_single_error_still_raises_original_type(self) -> None:
        """Single config error still raises ConfigurationError (backward compat)."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_multi_error_is_horsies_error(self) -> None:
        """MultipleValidationErrors is catchable as HorsiesError."""
        with pytest.raises(HorsiesError):
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
            )

    def test_three_independent_errors_collected(self) -> None:
        """Three independent config errors are collected together."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
                default_unhandled_error_code='TimeoutError',
            )
        errors = exc_info.value.report.errors
        assert len(errors) == 3

    def test_multi_error_individual_codes(self) -> None:
        """Each error in a multi-error report has the correct error code."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
                default_unhandled_error_code='TimeoutError',
            )
        errors = exc_info.value.report.errors
        codes = {e.code for e in errors}
        assert ErrorCode.CONFIG_INVALID_CLUSTER_CAP in codes
        assert ErrorCode.CONFIG_INVALID_PREFETCH in codes
        assert ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER in codes


@pytest.mark.unit
class TestRetryCodeValidation:
    """Validation for retry/error-code safety settings."""

    def test_invalid_default_unhandled_error_code_raises(self) -> None:
        """Exception-like names are rejected as error codes."""
        with pytest.raises(
            ConfigurationError,
            match='default_unhandled_error_code',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                default_unhandled_error_code='TimeoutError',
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_invalid_mapper_value_format_raises(self) -> None:
        """Mapper values that look like exception names are rejected."""
        with pytest.raises(
            ConfigurationError,
            match='Mapper value',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                exception_mapper={ValueError: 'TimeoutError'},
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_valid_default_unhandled_error_code(self) -> None:
        """UPPER_SNAKE_CASE error codes should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            default_unhandled_error_code='CUSTOM_CODE',
        )
        assert config.default_unhandled_error_code == 'CUSTOM_CODE'

    def test_lowercase_error_code_raises(self) -> None:
        """Lowercase error codes should be rejected."""
        with pytest.raises(
            ConfigurationError,
            match='default_unhandled_error_code',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                default_unhandled_error_code='lowercase_code',
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_non_base_exception_mapper_key_raises(self) -> None:
        """Mapper keys that are not BaseException subclasses should be rejected.

        Pydantic validates the dict type annotation before the model_validator runs,
        so this raises pydantic ValidationError, not ConfigurationError.
        """
        with pytest.raises(ValidationError, match='is_subclass_of'):
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                exception_mapper={str: 'SOME_CODE'},  # type: ignore[dict-item]
            )

    def test_valid_exception_mapper_accepted(self) -> None:
        """Valid exception mapper entries should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            exception_mapper={ValueError: 'VALUE_ERROR'},
        )
        assert ValueError in config.exception_mapper
        assert config.exception_mapper[ValueError] == 'VALUE_ERROR'


@pytest.mark.unit
class TestAppConfigImmutability:
    """Tests for frozen model behavior."""

    def test_frozen_model_rejects_mutation(self) -> None:
        """AppConfig is frozen and should reject attribute assignment."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        with pytest.raises(ValidationError):
            config.prefetch_buffer = 1  # type: ignore[misc]


@pytest.mark.unit
class TestLogConfig:
    """Tests for log_config, _format_for_logging, and mask_database_url."""

    def test_mask_database_url_hides_password(self) -> None:
        """Password in database URL should be masked."""
        url = 'postgresql+psycopg://user:secret@localhost/db'
        masked = mask_database_url(url)
        assert 'secret' not in masked
        assert '***' in masked
        assert 'user' in masked
        assert 'localhost' in masked

    def test_mask_database_url_no_password(self) -> None:
        """URL without password should remain unchanged."""
        url = 'postgresql+psycopg://localhost/db'
        masked = mask_database_url(url)
        assert masked == url

    def test_mask_database_url_malformed_fallback(self) -> None:
        """Malformed URL with @ should use fallback masking."""
        # Force the except branch by providing something that urlparse
        # handles but has no parsed password, while still containing @
        url = 'not-a-real-scheme://user:secret@host/db'
        masked = mask_database_url(url)
        assert 'secret' not in masked

    def test_format_for_logging_contains_queue_mode(self) -> None:
        """Formatted output should contain queue_mode."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        formatted = config._format_for_logging()
        assert 'queue_mode:' in formatted
        assert 'DEFAULT' in formatted

    def test_format_for_logging_custom_queues_listed(self) -> None:
        """CUSTOM mode should list queue names in formatted output."""
        config = AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=BROKER,
            custom_queues=[
                CustomQueueConfig(name='fast', priority=1, max_concurrency=10),
                CustomQueueConfig(name='slow', priority=2, max_concurrency=3),
            ],
        )
        formatted = config._format_for_logging()
        assert 'fast' in formatted
        assert 'slow' in formatted
        assert 'custom_queues:' in formatted

    def test_log_config_skips_in_child_process(self) -> None:
        """log_config should not log when HORSIES_CHILD_PROCESS=1."""
        config = AppConfig(broker=BROKER)
        logger = MagicMock()
        with patch.dict('os.environ', {'HORSIES_CHILD_PROCESS': '1'}):
            config.log_config(logger=logger)
        logger.info.assert_not_called()

    def test_log_config_uses_provided_logger(self) -> None:
        """log_config should call logger.info when provided."""
        config = AppConfig(broker=BROKER)
        logger = MagicMock()
        with patch.dict('os.environ', {}, clear=False):
            # Ensure HORSIES_CHILD_PROCESS is not set
            env = dict(
                **{
                    k: v
                    for k, v in __import__('os').environ.items()
                    if k != 'HORSIES_CHILD_PROCESS'
                }
            )
            with patch.dict('os.environ', env, clear=True):
                config.log_config(logger=logger)
        logger.info.assert_called_once()

    def test_log_config_none_logger_uses_library_logger(self) -> None:
        """log_config with no logger should use the 'horsies' library logger."""
        config = AppConfig(broker=BROKER)
        with patch.dict('os.environ', {}, clear=False):
            env = {
                k: v
                for k, v in __import__('os').environ.items()
                if k != 'HORSIES_CHILD_PROCESS'
            }
            with patch.dict('os.environ', env, clear=True):
                with patch('logging.getLogger') as mock_get_logger:
                    mock_logger = MagicMock()
                    mock_get_logger.return_value = mock_logger
                    config.log_config(logger=None)
                    mock_get_logger.assert_called_once_with('horsies')
                    mock_logger.info.assert_called_once()


@pytest.mark.unit
class TestFormatForLoggingExtended:
    """Tests for _format_for_logging covering cluster_wide_cap, claim_lease_ms, exception_mapper, schedule."""

    def test_cluster_wide_cap_displayed(self) -> None:
        """cluster_wide_cap should appear in formatted output when set."""
        config = AppConfig(broker=BROKER, cluster_wide_cap=50)
        formatted = config._format_for_logging()
        assert 'cluster_wide_cap: 50' in formatted

    def test_cluster_wide_cap_omitted_when_none(self) -> None:
        """cluster_wide_cap should not appear when None."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'cluster_wide_cap' not in formatted

    def test_claim_lease_ms_displayed(self) -> None:
        """claim_lease_ms should appear in formatted output when set."""
        config = AppConfig(
            broker=BROKER,
            prefetch_buffer=4,
            claim_lease_ms=30_000,
            recovery=RecoveryConfig(claimer_heartbeat_interval_ms=10_000),
        )
        formatted = config._format_for_logging()
        assert 'claim_lease_ms: 30000ms' in formatted

    def test_claim_lease_ms_omitted_when_none(self) -> None:
        """claim_lease_ms should not appear when None."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'claim_lease_ms' not in formatted

    def test_exception_mapper_displayed(self) -> None:
        """Non-empty exception mapper should show mapping count."""
        config = AppConfig(
            broker=BROKER,
            exception_mapper={ValueError: 'VALUE_ERROR', TypeError: 'TYPE_ERROR'},
        )
        formatted = config._format_for_logging()
        assert 'exception_mapper: 2 mapping(s)' in formatted

    def test_custom_default_error_code_displayed(self) -> None:
        """Non-default unhandled error code should appear in output."""
        config = AppConfig(
            broker=BROKER,
            default_unhandled_error_code='MY_ERROR',
        )
        formatted = config._format_for_logging()
        assert 'default_unhandled_error_code: MY_ERROR' in formatted

    def test_default_error_code_omitted_when_default(self) -> None:
        """Default unhandled error code should not appear in output."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'default_unhandled_error_code' not in formatted

    def test_retention_hours_displayed(self) -> None:
        """Retention hours should appear in formatted output."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'retention_hours:' in formatted
        assert 'heartbeats' not in formatted.split('retention_hours:')[1].split('\n')[0]
        assert 'worker_states=168' in formatted
        assert 'terminal_workflows=720' in formatted

    def test_schedule_displayed(self) -> None:
        """Schedule config should appear when set."""
        config = AppConfig(
            broker=BROKER,
            schedule=ScheduleConfig(
                enabled=True,
                schedules=[
                    TaskSchedule(
                        name='daily_cleanup',
                        task_name='cleanup',
                        pattern=DailySchedule(time=datetime_time(3, 0, 0)),
                    ),
                ],
                check_interval_seconds=5,
            ),
        )
        formatted = config._format_for_logging()
        assert 'schedule:' in formatted
        assert 'enabled: True' in formatted
        assert '1 schedule(s)' in formatted
        assert 'daily_cleanup' in formatted
        assert 'cleanup' in formatted
        assert 'check_interval: 5s' in formatted

    def test_schedule_omitted_when_none(self) -> None:
        """Schedule section should not appear when schedule is None."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'schedule:' not in formatted


@pytest.mark.unit
class TestFormatSchedulePattern:
    """Tests for AppConfig._format_schedule_pattern covering all match cases."""

    def test_interval_all_components(self) -> None:
        """Interval with days, hours, minutes, seconds should format all parts."""
        pattern = IntervalSchedule(days=1, hours=2, minutes=30, seconds=15)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'every 1d 2h 30m 15s'

    def test_interval_hours_only(self) -> None:
        """Interval with only hours should format just hours."""
        pattern = IntervalSchedule(hours=4)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'every 4h'

    def test_interval_minutes_and_seconds(self) -> None:
        """Interval with minutes and seconds only."""
        pattern = IntervalSchedule(minutes=10, seconds=30)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'every 10m 30s'

    def test_hourly(self) -> None:
        """Hourly pattern should format with zero-padded minute:second."""
        pattern = HourlySchedule(minute=5, second=30)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'hourly at :05:30'

    def test_hourly_defaults(self) -> None:
        """Hourly with default second should zero-pad correctly."""
        pattern = HourlySchedule(minute=15)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'hourly at :15:00'

    def test_daily(self) -> None:
        """Daily pattern should format time as HH:MM:SS."""
        pattern = DailySchedule(time=datetime_time(14, 30, 0))
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'daily at 14:30:00'

    def test_weekly(self) -> None:
        """Weekly pattern should list day names and time."""
        pattern = WeeklySchedule(
            days=[Weekday.MONDAY, Weekday.FRIDAY],
            time=datetime_time(9, 0, 0),
        )
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'weekly on monday, friday at 09:00:00'

    def test_monthly(self) -> None:
        """Monthly pattern should show day number and time."""
        pattern = MonthlySchedule(day=15, time=datetime_time(3, 0, 0))
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'monthly on day 15 at 03:00:00'


@pytest.mark.unit
class TestGetBrokerErrorHandling:
    """Regression tests for Horsies.get_broker() error wrapping (HRS-211)."""

    def test_worker_role_get_broker_uses_worker_pool_profile(self) -> None:
        """Worker app brokers should not use producer-sized pool defaults."""
        from horsies import Horsies

        config = PostgresConfig(
            database_url='postgresql+psycopg://user:pass@localhost/db',
            pool_size=30,
            max_overflow=30,
            worker_pool_size=3,
            worker_max_overflow=2,
        )
        app = Horsies(config=AppConfig(queue_mode=QueueMode.DEFAULT, broker=config))
        app.set_role('worker')

        with patch('horsies.core.app.PostgresBroker') as mock_broker_cls:
            app.get_broker()

        broker_config = mock_broker_cls.call_args.args[0]
        assert broker_config.pool_size == 3
        assert broker_config.max_overflow == 2

    def test_non_horsies_error_wrapped_as_broker_init_failed(self) -> None:
        """Non-HorsiesError exceptions are wrapped with BROKER_INIT_FAILED."""
        from horsies import Horsies

        app = Horsies(config=AppConfig(queue_mode=QueueMode.DEFAULT, broker=BROKER))
        app._broker = None  # ensure fresh init path

        with patch(
            'horsies.core.app.PostgresBroker',
            side_effect=RuntimeError('driver missing'),
        ):
            with pytest.raises(HorsiesError) as exc_info:
                app.get_broker()

            assert exc_info.value.code == ErrorCode.BROKER_INIT_FAILED
            assert 'driver missing' in str(exc_info.value.message)
            assert exc_info.value.__cause__ is not None

    def test_horsies_error_passes_through(self) -> None:
        """HorsiesError from broker init is re-raised, not double-wrapped."""
        from horsies import Horsies

        app = Horsies(config=AppConfig(queue_mode=QueueMode.DEFAULT, broker=BROKER))
        app._broker = None

        original = HorsiesError(
            message='bad config',
            code=ErrorCode.BROKER_INVALID_URL,
        )
        with patch(
            'horsies.core.app.PostgresBroker',
            side_effect=original,
        ):
            with pytest.raises(HorsiesError) as exc_info:
                app.get_broker()

            assert exc_info.value is original
            assert exc_info.value.code == ErrorCode.BROKER_INVALID_URL


@pytest.mark.unit
class TestWorkerStateSnapshotInterval:
    """RecoveryConfig.worker_state_snapshot_interval_ms bounds and default."""

    def test_default_is_30_seconds(self) -> None:
        assert RecoveryConfig().worker_state_snapshot_interval_ms == 30_000

    def test_bounds_accept_valid_edges(self) -> None:
        assert RecoveryConfig(
            worker_state_snapshot_interval_ms=1_000,
        ).worker_state_snapshot_interval_ms == 1_000
        assert RecoveryConfig(
            worker_state_snapshot_interval_ms=300_000,
        ).worker_state_snapshot_interval_ms == 300_000

    def test_below_minimum_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RecoveryConfig(worker_state_snapshot_interval_ms=999)

    def test_above_maximum_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RecoveryConfig(worker_state_snapshot_interval_ms=300_001)


@pytest.mark.unit
class TestRetentionSweepInterval:
    """RetentionConfig.retention_sweep_interval_s bounds and default."""

    def test_default_is_5_minutes(self) -> None:
        assert RetentionConfig().retention_sweep_interval_s == 300

    def test_bounds_accept_valid_edges(self) -> None:
        assert RetentionConfig(retention_sweep_interval_s=30,
        ).retention_sweep_interval_s == 30
        assert RetentionConfig(retention_sweep_interval_s=86_400,
        ).retention_sweep_interval_s == 86_400

    def test_below_minimum_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RetentionConfig(retention_sweep_interval_s=29)

    def test_above_maximum_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RetentionConfig(retention_sweep_interval_s=86_401)


@pytest.mark.unit
class TestRetentionDeleteBatchSize:
    """RetentionConfig.retention_delete_batch_size bounds and default."""

    def test_default_is_500(self) -> None:
        assert RetentionConfig().retention_delete_batch_size == 500

    def test_bounds_accept_valid_edges(self) -> None:
        assert RetentionConfig(retention_delete_batch_size=50,
        ).retention_delete_batch_size == 50
        assert RetentionConfig(retention_delete_batch_size=10_000,
        ).retention_delete_batch_size == 10_000

    def test_below_minimum_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RetentionConfig(retention_delete_batch_size=49)

    def test_above_maximum_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RetentionConfig(retention_delete_batch_size=10_001)


@pytest.mark.unit
class TestRemovedRetentionKnobsRejected:
    """The removed knobs fail closed with their successors named."""

    def test_queue_override_names_retention_classes(self) -> None:
        with pytest.raises(Exception, match='retention class'):
            RecoveryConfig(
                queue_terminal_record_retention_hours={'default': 24},
            )

    def test_heartbeat_window_names_partition_drops(self) -> None:
        with pytest.raises(Exception, match='drop whole'):
            RecoveryConfig(heartbeat_retention_hours=24)

    def test_surviving_knob_governs_workflow_records(self) -> None:
        cfg = RetentionConfig(terminal_record_retention_hours=48)
        assert cfg.terminal_record_retention_hours == 48


class TestPayloadPolicyWiring:
    """AppConfig carries a PayloadPolicy with warn-only defaults."""

    def test_default_policy(self) -> None:
        config = AppConfig(queue_mode=QueueMode.DEFAULT, broker=BROKER)
        assert config.payload.warn_bytes == 1_048_576
        assert config.payload.reject_bytes is None


@pytest.mark.unit
class TestQueueRetentionNamesARealQueue:
    """A mapped queue must be a queue the deployment has.

    The open failure is silent and permanent: a misspelled queue is
    accepted, its class is registered, and leaves and indexes are built
    for it on every maintenance pass forever while nothing routes into
    it — and the queue the adopter meant keeps the 30-day default. The
    deployment has neither the policy it configured nor an error.

    `RetentionConfig` cannot self-check this: it does not know the queue
    set. `AppConfig` owns both, so the cross-check lives there.
    """

    def _broker(self) -> PostgresConfig:
        return PostgresConfig(
            database_url='postgresql+psycopg://u:p@localhost/db'
        )

    def test_default_mode_accepts_the_default_queue(self) -> None:
        config = AppConfig(
            broker=self._broker(),
            queue_mode=QueueMode.DEFAULT,
            retention=RetentionConfig(
                queue_retention={'default': timedelta(days=7)}
            ),
        )

        assert 'default' in config.retention.queue_retention

    def test_default_mode_refuses_any_other_queue(self) -> None:
        with pytest.raises(ConfigurationError) as caught:
            AppConfig(
                broker=self._broker(),
                queue_mode=QueueMode.DEFAULT,
                retention=RetentionConfig(
                    queue_retention={'emails': timedelta(days=7)}
                ),
            )

        assert 'emails' in str(caught.value)

    def test_custom_mode_accepts_a_configured_queue(self) -> None:
        config = AppConfig(
            broker=self._broker(),
            queue_mode=QueueMode.CUSTOM,
            custom_queues=[CustomQueueConfig(name='emails', priority=100)],
            retention=RetentionConfig(
                queue_retention={'emails': timedelta(days=7)}
            ),
        )

        assert config.retention.queue_retention['emails'] == timedelta(days=7)

    def test_custom_mode_refuses_a_typo(self) -> None:
        """The failure this guard exists for."""
        with pytest.raises(ConfigurationError) as caught:
            AppConfig(
                broker=self._broker(),
                queue_mode=QueueMode.CUSTOM,
                custom_queues=[CustomQueueConfig(name='emails', priority=100)],
                retention=RetentionConfig(
                    queue_retention={'emals': timedelta(days=7)}
                ),
            )

        message = str(caught.value)
        assert 'emals' in message, 'the refusal must name the bad key'
        assert 'emails' in message, (
            'the refusal must list the queues that do exist, or the '
            'adopter cannot see the typo'
        )

    def test_an_empty_mapping_is_unaffected(self) -> None:
        config = AppConfig(broker=self._broker(), queue_mode=QueueMode.DEFAULT)

        assert config.retention.queue_retention == {}
