"""Contract between the Pydantic broker config and the core's producer manifest.

`to_producer_manifest` maps `PostgresConfig` into the manifest generated
from horsies-core (`clients/python/horsies/_catalog/manifest.py`). These tests pin that
every config field is mapped or named as outside the producer manifest,
that every manifest field has a source, that the mapping copies each value,
and which SQLAlchemy pool values the config keeps: each meaning that the
core can honor is accepted and reaches the manifest as a typed value; an
unlimited size and a value without meaning are refused. The TCP keepalive
and user timeout values have the core's bounds: a value at a bound reaches
the manifest, and a value past it is refused by the config.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Final, cast

import pytest
from pydantic import SecretStr

from horsies._catalog.manifest import (
    KEEPALIVE_PROBES_MAX,
    KEEPALIVE_PROBES_MIN,
    KEEPALIVE_SECONDS_MAX,
    KEEPALIVE_SECONDS_MIN,
    MANIFEST_VERSION,
    POOL_CONNECTIONS_MAX,
    RESERVATION_WINDOW_MICROS_MAX,
    POOL_SIZE_MAX,
    TCP_USER_TIMEOUT_MS_MAX,
    WHOLE_SECONDS_MAX,
    DatabaseManifest,
    PoolRecycleAfterSeconds,
    PoolRecycleNever,
    ProducerManifest,
    SendManifest,
    SessionDatabaseSameAsDatabaseUrl,
    SchemaMode,
    SessionDatabaseSeparate,
    TcpKeepalivesConfigOff,
    TcpKeepalivesConfigOn,
    TcpUserTimeoutConfigAfter,
    TcpUserTimeoutConfigSystemDefault,
    dump_producer_manifest,
)
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.retention import RetentionConfig
from horsies.core.history.identity.keys import (
    IDEMPOTENCY_WINDOW_MAX,
    validate_reservation_window,
)
import horsies.core.manifest as manifest_module
from horsies._core import InstallCoverage
from horsies.core.manifest import (
    core_database_url,
    install_coverage_of,
    reservation_window_micros,
    tcp_keepalives_of,
    tcp_user_timeout_of,
    to_producer_manifest,
    wire_schema_mode,
)
from horsies.core.models import broker as broker_config
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schema_mode import SchemaMode as AppSchemaMode

pytestmark = [pytest.mark.unit]

MAIN_URL: Final = 'postgresql://user:secret@pooler:6432/app'
SESSION_URL: Final = 'postgresql://user:secret@direct:5432/app'

# PostgresConfig field -> the manifest path it fills.
MAPPED_FIELDS: Final[dict[str, str]] = {
    'database_url': 'database.database_url',
    'session_database': 'database.session_database',
    'pgbouncer_transaction_mode': 'database.pgbouncer_transaction_mode',
    'pool_pre_ping': 'database.pool_pre_ping',
    'pool_size': 'database.pool_size',
    'max_overflow': 'database.max_overflow',
    'pool_timeout_seconds': 'database.pool_timeout_seconds',
    'pool_recycle': 'database.pool_recycle',
    'echo': 'database.echo',
    'tcp_keepalives': 'database.tcp_keepalives',
    'tcp_user_timeout': 'database.tcp_user_timeout',
    'retain_rerun_input_default': 'send.retain_rerun_input_default',
}

# PostgresConfig fields that the producer manifest does not hold.
# worker_pool_pre_ping belongs to the worker.
OUTSIDE_PRODUCER_MANIFEST: Final[frozenset[str]] = frozenset(
    {'worker_pool_pre_ping'},
)

# Manifest paths that come from to_producer_manifest's parameters or from
# the manifest version, not from PostgresConfig.
NOT_FROM_CONFIG: Final[frozenset[str]] = frozenset(
    {'manifest_version', 'schema', 'send.idempotency_reservation_window'},
)

# Most nesting levels of a manifest document. The producer manifest has 3.
MAX_DOCUMENT_DEPTH: Final = 8


def _config_with_every_value_set() -> PostgresConfig:
    return PostgresConfig(
        database_url=SecretStr(MAIN_URL),
        session_database=broker_config.SessionDatabaseSeparate(url=SecretStr(SESSION_URL)),
        pgbouncer_transaction_mode=True,
        pool_pre_ping=False,
        pool_size=7,
        max_overflow=3,
        pool_timeout_seconds=11,
        pool_recycle=broker_config.PoolRecycleAfterSeconds(seconds=600),
        echo=True,
        tcp_keepalives=broker_config.TcpKeepalivesOn(
            idle_seconds=45,
            interval_seconds=7,
            probes=5,
        ),
        tcp_user_timeout=broker_config.TcpUserTimeoutAfter(milliseconds=1500),
        retain_rerun_input_default=True,
    )


def _leaf_paths(node: object, prefix: str, depth: int) -> list[str]:
    """The dotted paths of the leaves of a JSON document."""
    assert depth <= MAX_DOCUMENT_DEPTH, 'the manifest document nests too deep'
    match node:
        case dict():
            # A JSON object: its keys are text.
            members = cast('dict[str, object]', node)
            paths: list[str] = []
            for key, child in members.items():
                path = f'{prefix}.{key}' if prefix else key
                paths.extend(_leaf_paths(child, path, depth + 1))
            return paths
        case _:
            return [prefix]


def test_every_config_field_is_mapped_or_named_as_outside() -> None:
    config_fields = set(PostgresConfig.model_fields)
    assert set(MAPPED_FIELDS).isdisjoint(OUTSIDE_PRODUCER_MANIFEST)
    assert config_fields == set(MAPPED_FIELDS) | OUTSIDE_PRODUCER_MANIFEST


def test_every_manifest_field_has_a_source() -> None:
    manifest = to_producer_manifest(
        _config_with_every_value_set(),
        schema='MIGRATE_AND_VALIDATE',
        idempotency_reservation_window=timedelta(seconds=60),
    )
    document: object = json.loads(dump_producer_manifest(manifest))
    sources = set(MAPPED_FIELDS.values()) | NOT_FROM_CONFIG
    unsourced = [
        path
        for path in _leaf_paths(document, '', 0)
        if not any(path == source or path.startswith(f'{source}.') for source in sources)
    ]
    assert unsourced == []


def test_a_config_resolves_field_by_field() -> None:
    manifest = to_producer_manifest(
        _config_with_every_value_set(),
        schema='OBSERVE_ONLY',
        idempotency_reservation_window=timedelta(days=1),
    )
    assert manifest == ProducerManifest(
        manifest_version=MANIFEST_VERSION,
        database=DatabaseManifest(
            database_url='postgresql://user:secret@pooler:6432/app',
            session_database=SessionDatabaseSeparate(
                url='postgresql://user:secret@direct:5432/app',
            ),
            pgbouncer_transaction_mode=True,
            pool_pre_ping=False,
            pool_size=7,
            max_overflow=3,
            pool_timeout_seconds=11,
            pool_recycle=PoolRecycleAfterSeconds(seconds=600),
            echo=True,
            tcp_keepalives=TcpKeepalivesConfigOn(
                idle_seconds=45,
                interval_seconds=7,
                probes=5,
            ),
            tcp_user_timeout=TcpUserTimeoutConfigAfter(milliseconds=1500),
        ),
        schema='OBSERVE_ONLY',
        send=SendManifest(
            retain_rerun_input_default=True,
            idempotency_reservation_window=86_400_000_000,
        ),
    )


def test_the_dump_is_the_json_document_the_core_parses() -> None:
    manifest = to_producer_manifest(
        PostgresConfig(database_url=SecretStr(MAIN_URL)),
        schema='MIGRATE_AND_VALIDATE',
        idempotency_reservation_window=timedelta(seconds=60),
    )
    document: object = json.loads(dump_producer_manifest(manifest))
    assert document == {
        'manifest_version': 1,
        'database': {
            'database_url': 'postgresql://user:secret@pooler:6432/app',
            'session_database': {'kind': 'SAME_AS_DATABASE_URL'},
            'pgbouncer_transaction_mode': False,
            'pool_pre_ping': True,
            'pool_size': 5,
            'max_overflow': 10,
            'pool_timeout_seconds': 30,
            'pool_recycle': {'kind': 'AFTER_SECONDS', 'seconds': 1800},
            'echo': False,
            'tcp_keepalives': {
                'kind': 'ON',
                'idle_seconds': 30,
                'interval_seconds': 10,
                'probes': 3,
            },
            'tcp_user_timeout': {'kind': 'AFTER', 'milliseconds': 30_000},
        },
        'schema': 'MIGRATE_AND_VALIDATE',
        'send': {
            'retain_rerun_input_default': False,
            'idempotency_reservation_window': 60_000_000,
        },
    }
    assert manifest.database.session_database == SessionDatabaseSameAsDatabaseUrl()


def _database_of(field: str, value: object) -> DatabaseManifest:
    """The database manifest of a config with `field` set to `value`."""
    config = PostgresConfig.model_validate({'database_url': MAIN_URL, field: value})
    return to_producer_manifest(
        config,
        schema='MIGRATE_AND_VALIDATE',
        idempotency_reservation_window=timedelta(seconds=60),
    ).database


def _refusal_of(field: str, value: int) -> ConfigurationError:
    with pytest.raises(ConfigurationError) as raised:
        PostgresConfig.model_validate({'database_url': MAIN_URL, field: value})
    error = raised.value
    assert error.code == ErrorCode.CONFIG_INVALID_BROKER_POOL
    assert f'got: {field}={value}' in error.notes
    return error


def _recycle_refusal_of(seconds: int) -> ConfigurationError:
    with pytest.raises(ConfigurationError) as raised:
        PostgresConfig(
            database_url=SecretStr(MAIN_URL),
            pool_recycle=broker_config.PoolRecycleAfterSeconds(seconds=seconds),
        )
    error = raised.value
    assert error.code == ErrorCode.CONFIG_INVALID_BROKER_POOL
    assert f'got: pool_recycle.seconds={seconds}' in error.notes
    return error


@pytest.mark.parametrize('size', [1, POOL_SIZE_MAX])
def test_a_pool_size_is_a_number_of_connections(size: int) -> None:
    assert _database_of('pool_size', size).pool_size == size


def test_an_unlimited_pool_size_is_refused_with_the_fix() -> None:
    error = _refusal_of('pool_size', 0)
    assert any('no size limit' in note for note in error.notes)
    assert error.help_text == f'give a pool size from 1 to {POOL_SIZE_MAX}'


@pytest.mark.parametrize('size', [-1, POOL_SIZE_MAX + 1])
def test_a_pool_size_without_meaning_is_refused(size: int) -> None:
    _ = _refusal_of('pool_size', size)


def test_an_overflow_of_zero_means_no_overflow() -> None:
    assert _database_of('max_overflow', 0).max_overflow == 0


def test_an_unlimited_overflow_is_refused_with_the_fix() -> None:
    error = _refusal_of('max_overflow', -1)
    assert any('no overflow limit' in note for note in error.notes)
    assert error.help_text == f'give a number from 0 to {POOL_CONNECTIONS_MAX}'


@pytest.mark.parametrize('overflow', [-2, POOL_CONNECTIONS_MAX + 1])
def test_an_overflow_without_meaning_is_refused(overflow: int) -> None:
    _ = _refusal_of('max_overflow', overflow)


def test_a_pool_timeout_seconds_of_zero_means_a_checkout_does_not_wait() -> None:
    assert _database_of('pool_timeout_seconds', 0).pool_timeout_seconds == 0


@pytest.mark.parametrize('timeout', [-1, WHOLE_SECONDS_MAX + 1])
def test_a_pool_timeout_seconds_without_meaning_is_refused(timeout: int) -> None:
    _ = _refusal_of('pool_timeout_seconds', timeout)


def test_pool_recycle_never_is_the_never_variant() -> None:
    assert _database_of('pool_recycle', broker_config.PoolRecycleNever()).pool_recycle == (
        PoolRecycleNever()
    )


@pytest.mark.parametrize('seconds', [0, 1800, WHOLE_SECONDS_MAX])
def test_a_pool_recycle_age_is_a_number_of_seconds(seconds: int) -> None:
    recycle = broker_config.PoolRecycleAfterSeconds(seconds=seconds)
    assert _database_of('pool_recycle', recycle).pool_recycle == (
        PoolRecycleAfterSeconds(seconds=seconds)
    )


@pytest.mark.parametrize('seconds', [-5, -1, WHOLE_SECONDS_MAX + 1])
def test_a_pool_recycle_without_meaning_is_refused(seconds: int) -> None:
    _ = _recycle_refusal_of(seconds)


def test_keepalives_off_is_the_off_variant() -> None:
    config = PostgresConfig(
        database_url=SecretStr(MAIN_URL),
        tcp_keepalives=broker_config.TcpKeepalivesOff(),
    )
    assert tcp_keepalives_of(config) == TcpKeepalivesConfigOff()


def test_a_keepalive_value_that_the_validator_refuses_is_a_defect_here() -> None:
    with pytest.raises(ConfigurationError):
        PostgresConfig(
            database_url=SecretStr(MAIN_URL),
            tcp_keepalives=broker_config.TcpKeepalivesOn(idle_seconds=0),
        )
    unvalidated = PostgresConfig.model_construct(
        database_url=SecretStr(MAIN_URL),
        tcp_keepalives=broker_config.TcpKeepalivesOn(idle_seconds=0),
    )
    with pytest.raises(AssertionError, match='keepalive values'):
        tcp_keepalives_of(unvalidated)


@pytest.mark.parametrize(
    ('seconds', 'probes'),
    [
        (KEEPALIVE_SECONDS_MIN, KEEPALIVE_PROBES_MIN),
        (KEEPALIVE_SECONDS_MAX, KEEPALIVE_PROBES_MAX),
    ],
    ids=['lower-bounds', 'upper-bounds'],
)
def test_keepalive_values_at_the_core_bounds_reach_the_manifest(
    seconds: int,
    probes: int,
) -> None:
    config = PostgresConfig(
        database_url=SecretStr(MAIN_URL),
        tcp_keepalives=broker_config.TcpKeepalivesOn(
            idle_seconds=seconds,
            interval_seconds=seconds,
            probes=probes,
        ),
    )
    manifest = to_producer_manifest(
        config,
        schema='MIGRATE_AND_VALIDATE',
        idempotency_reservation_window=timedelta(seconds=60),
    )
    assert manifest.database.tcp_keepalives == TcpKeepalivesConfigOn(
        idle_seconds=seconds,
        interval_seconds=seconds,
        probes=probes,
    )


@pytest.mark.parametrize(
    ('setting', 'path', 'value'),
    [
        (
            broker_config.TcpKeepalivesOn(idle_seconds=KEEPALIVE_SECONDS_MAX + 1),
            'tcp_keepalives.idle_seconds',
            KEEPALIVE_SECONDS_MAX + 1,
        ),
        (
            broker_config.TcpKeepalivesOn(interval_seconds=KEEPALIVE_SECONDS_MAX + 1),
            'tcp_keepalives.interval_seconds',
            KEEPALIVE_SECONDS_MAX + 1,
        ),
        (
            broker_config.TcpKeepalivesOn(probes=KEEPALIVE_PROBES_MAX + 1),
            'tcp_keepalives.probes',
            KEEPALIVE_PROBES_MAX + 1,
        ),
        (
            broker_config.TcpUserTimeoutAfter(milliseconds=TCP_USER_TIMEOUT_MS_MAX + 1),
            'tcp_user_timeout.milliseconds',
            TCP_USER_TIMEOUT_MS_MAX + 1,
        ),
    ],
)
def test_a_tcp_value_past_the_core_bound_is_refused_by_the_config(
    setting: object,
    path: str,
    value: int,
) -> None:
    field = path.partition('.')[0]
    with pytest.raises(ConfigurationError) as raised:
        PostgresConfig.model_validate({'database_url': MAIN_URL, field: setting})
    assert raised.value.code == ErrorCode.CONFIG_INVALID_KEEPALIVE
    assert f'got: {path}={value}' in raised.value.notes


def test_the_tcp_user_timeout_system_default_is_its_variant() -> None:
    assert _database_of(
        'tcp_user_timeout', broker_config.TcpUserTimeoutSystemDefault(),
    ).tcp_user_timeout == TcpUserTimeoutConfigSystemDefault()


@pytest.mark.parametrize('milliseconds', [1, 30_000, TCP_USER_TIMEOUT_MS_MAX])
def test_a_tcp_user_timeout_is_a_number_of_milliseconds(milliseconds: int) -> None:
    timeout = broker_config.TcpUserTimeoutAfter(milliseconds=milliseconds)
    assert _database_of('tcp_user_timeout', timeout).tcp_user_timeout == (
        TcpUserTimeoutConfigAfter(milliseconds=milliseconds)
    )


def test_a_tcp_user_timeout_that_the_validator_refuses_is_a_defect_here() -> None:
    zero = broker_config.TcpUserTimeoutAfter(milliseconds=0)
    with pytest.raises(ConfigurationError):
        PostgresConfig(database_url=SecretStr(MAIN_URL), tcp_user_timeout=zero)
    unvalidated = PostgresConfig.model_construct(
        database_url=SecretStr(MAIN_URL), tcp_user_timeout=zero,
    )
    with pytest.raises(AssertionError, match='tcp_user_timeout.milliseconds=0'):
        tcp_user_timeout_of(unvalidated)


def test_a_url_with_another_scheme_is_a_defect_named_without_its_password() -> None:
    with pytest.raises(AssertionError) as raised:
        core_database_url('postgresql+psycopg://user:secret@host/app')
    assert 'postgresql+psycopg' in str(raised.value)
    assert 'secret' not in str(raised.value)


@pytest.mark.parametrize(
    'url',
    ['postgresql://user:secret@host/app', 'postgres://user:secret@host/app'],
)
def test_the_core_reads_the_postgresql_scheme(url: str) -> None:
    assert core_database_url(url) == 'postgresql://user:secret@host/app'


@pytest.mark.parametrize(
    ('window', 'micros'),
    [
        (timedelta(microseconds=1), 1),
        (timedelta(seconds=1, microseconds=500_000), 1_500_000),
        (timedelta(hours=24), 86_400_000_000),
        (IDEMPOTENCY_WINDOW_MAX, RESERVATION_WINDOW_MICROS_MAX),
    ],
    ids=['one-microsecond', 'one-and-a-half-seconds', 'default-day', 'thirty-days'],
)
def test_a_valid_window_reaches_the_manifest_in_exact_microseconds(
    window: timedelta,
    micros: int,
) -> None:
    validated = validate_reservation_window(window)
    manifest = to_producer_manifest(
        PostgresConfig(database_url=SecretStr(MAIN_URL)),
        schema='MIGRATE_AND_VALIDATE',
        idempotency_reservation_window=validated,
    )
    assert manifest.send.idempotency_reservation_window == micros
    assert timedelta(microseconds=micros) == window


def test_python_s_window_bound_is_the_manifest_s() -> None:
    assert IDEMPOTENCY_WINDOW_MAX // timedelta(microseconds=1) == RESERVATION_WINDOW_MICROS_MAX


@pytest.mark.parametrize(
    'window',
    [timedelta(0), timedelta(microseconds=-1), IDEMPOTENCY_WINDOW_MAX + timedelta(microseconds=1)],
    ids=['zero', 'negative', 'past-thirty-days'],
)
def test_a_window_that_the_validator_refuses_is_a_defect_here(window: timedelta) -> None:
    with pytest.raises(ValueError):
        validate_reservation_window(window)
    with pytest.raises(AssertionError, match='reservation window'):
        reservation_window_micros(window)


@pytest.mark.parametrize(
    ('app_mode', 'mode'),
    [
        (AppSchemaMode.MIGRATE_AND_VALIDATE, 'MIGRATE_AND_VALIDATE'),
        (AppSchemaMode.OBSERVE_ONLY, 'OBSERVE_ONLY'),
        (AppSchemaMode.ASSUME_INITIALIZED, 'ASSUME_INITIALIZED'),
    ],
)
def test_each_schema_mode_has_its_manifest_text(
    app_mode: AppSchemaMode,
    mode: SchemaMode,
) -> None:
    assert wire_schema_mode(app_mode) == mode
    assert app_mode.value == mode


def test_an_assume_initialized_manifest_dumps_its_mode() -> None:
    manifest = to_producer_manifest(
        PostgresConfig(database_url=SecretStr(MAIN_URL)),
        schema=wire_schema_mode(AppSchemaMode.ASSUME_INITIALIZED),
        idempotency_reservation_window=timedelta(hours=24),
    )
    document: object = json.loads(dump_producer_manifest(manifest))
    assert cast('dict[str, object]', document)['schema'] == 'ASSUME_INITIALIZED'


# --- the install coverage request ---


def _recorded_install_coverage(
    monkeypatch: pytest.MonkeyPatch,
    retention: RetentionConfig,
) -> list[dict[str, object]]:
    """The keyword arguments of each ``InstallCoverage`` that
    ``install_coverage_of(retention)`` builds."""
    recorded: list[dict[str, object]] = []

    def record(**arguments: object) -> object:
        recorded.append(arguments)
        return arguments

    monkeypatch.setattr(manifest_module, 'InstallCoverage', record)
    _ = install_coverage_of(retention)
    return recorded


def test_the_install_coverage_carries_the_horizons_and_every_sendable_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retention = RetentionConfig(
        history_leaf_horizon_days=4,
        heartbeat_leaf_horizon_hours=12,
        retention_periods=(timedelta(days=7),),
        queue_retention={'reports': timedelta(days=90), 'audit': None},
    )
    assert _recorded_install_coverage(monkeypatch, retention) == [
        {
            'history_horizon_days': 4,
            'heartbeat_horizon_hours': 12,
            'declared_period_days': [7, 30, 90],
        }
    ]


def test_the_default_retention_covers_the_default_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _recorded_install_coverage(monkeypatch, RetentionConfig()) == [
        {
            'history_horizon_days': 3,
            'heartbeat_horizon_hours': 6,
            'declared_period_days': [30],
        }
    ]


def test_the_core_accepts_the_install_coverage_of_a_validated_config() -> None:
    retention = RetentionConfig(retention_periods=(timedelta(days=1825),))
    assert isinstance(install_coverage_of(retention), InstallCoverage)

