"""Contract between the Pydantic broker config and the core's producer manifest.

`to_producer_manifest` maps `PostgresConfig` into the manifest generated
from horsies-core (`horsies/_catalog/manifest.py`). These tests pin that
every config field is mapped or named as outside the producer manifest,
that every manifest field has a source, that the mapping copies each value,
and which SQLAlchemy pool values the config keeps: each meaning that the
core can honor is accepted and reaches the manifest as a typed value; an
unlimited size and a value without meaning are refused.
"""

from __future__ import annotations

import json
from typing import Final, cast

import pytest
from pydantic import SecretStr

from horsies._catalog.manifest import (
    MANIFEST_VERSION,
    POOL_CONNECTIONS_MAX,
    POOL_SIZE_MAX,
    WHOLE_SECONDS_MAX,
    DatabaseManifest,
    PoolRecycleAfterSeconds,
    PoolRecycleNever,
    ProducerManifest,
    SendManifest,
    SessionDatabaseSameAsDatabaseUrl,
    SessionDatabaseSeparate,
    dump_producer_manifest,
)
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.manifest import core_database_url, to_producer_manifest
from horsies.core.models.broker import PostgresConfig

pytestmark = [pytest.mark.unit]

MAIN_URL: Final = 'postgresql+psycopg://user:secret@pooler:6432/app'
SESSION_URL: Final = 'postgresql+psycopg://user:secret@direct:5432/app'

# PostgresConfig field -> the manifest path it fills.
MAPPED_FIELDS: Final[dict[str, str]] = {
    'database_url': 'database.database_url',
    'session_database_url': 'database.session_database',
    'pgbouncer_transaction_mode': 'database.pgbouncer_transaction_mode',
    'pool_pre_ping': 'database.pool_pre_ping',
    'pool_size': 'database.pool_size',
    'max_overflow': 'database.max_overflow',
    'pool_timeout': 'database.pool_timeout',
    'pool_recycle': 'database.pool_recycle',
    'echo': 'database.echo',
    'retain_rerun_input_default': 'send.retain_rerun_input_default',
}

# PostgresConfig fields that the producer manifest does not hold.
# worker_pool_pre_ping belongs to the worker. The TCP keepalive settings and
# tcp_user_timeout_ms have no manifest field yet, so the core uses its own
# values for them.
OUTSIDE_PRODUCER_MANIFEST: Final[frozenset[str]] = frozenset(
    {
        'worker_pool_pre_ping',
        'tcp_keepalives',
        'tcp_keepalives_idle',
        'tcp_keepalives_interval',
        'tcp_keepalives_count',
        'tcp_user_timeout_ms',
    },
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
        session_database_url=SecretStr(SESSION_URL),
        pgbouncer_transaction_mode=True,
        pool_pre_ping=False,
        pool_size=7,
        max_overflow=3,
        pool_timeout=11,
        pool_recycle=600,
        echo=True,
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
        idempotency_reservation_window_seconds=60,
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
        idempotency_reservation_window_seconds=86_400,
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
            pool_timeout=11,
            pool_recycle=PoolRecycleAfterSeconds(seconds=600),
            echo=True,
        ),
        schema='OBSERVE_ONLY',
        send=SendManifest(
            retain_rerun_input_default=True,
            idempotency_reservation_window=86_400,
        ),
    )


def test_the_dump_is_the_json_document_the_core_parses() -> None:
    manifest = to_producer_manifest(
        PostgresConfig(database_url=SecretStr(MAIN_URL)),
        schema='MIGRATE_AND_VALIDATE',
        idempotency_reservation_window_seconds=60,
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
            'pool_timeout': 30,
            'pool_recycle': {'kind': 'AFTER_SECONDS', 'seconds': 1800},
            'echo': False,
        },
        'schema': 'MIGRATE_AND_VALIDATE',
        'send': {
            'retain_rerun_input_default': False,
            'idempotency_reservation_window': 60,
        },
    }
    assert manifest.database.session_database == SessionDatabaseSameAsDatabaseUrl()


def _database_of(field: str, value: int) -> DatabaseManifest:
    """The database manifest of a config with `field` set to `value`."""
    config = PostgresConfig.model_validate({'database_url': MAIN_URL, field: value})
    return to_producer_manifest(
        config,
        schema='MIGRATE_AND_VALIDATE',
        idempotency_reservation_window_seconds=60,
    ).database


def _refusal_of(field: str, value: int) -> ConfigurationError:
    with pytest.raises(ConfigurationError) as raised:
        PostgresConfig.model_validate({'database_url': MAIN_URL, field: value})
    error = raised.value
    assert error.code == ErrorCode.CONFIG_INVALID_BROKER_POOL
    assert f'got: {field}={value}' in error.notes
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


def test_a_pool_timeout_of_zero_means_a_checkout_does_not_wait() -> None:
    assert _database_of('pool_timeout', 0).pool_timeout == 0


@pytest.mark.parametrize('timeout', [-1, WHOLE_SECONDS_MAX + 1])
def test_a_pool_timeout_without_meaning_is_refused(timeout: int) -> None:
    _ = _refusal_of('pool_timeout', timeout)


def test_a_pool_recycle_of_minus_one_is_the_never_variant() -> None:
    assert _database_of('pool_recycle', -1).pool_recycle == PoolRecycleNever()


@pytest.mark.parametrize('seconds', [0, 1800, WHOLE_SECONDS_MAX])
def test_a_pool_recycle_age_is_a_number_of_seconds(seconds: int) -> None:
    assert _database_of('pool_recycle', seconds).pool_recycle == (
        PoolRecycleAfterSeconds(seconds=seconds)
    )


@pytest.mark.parametrize('seconds', [-5, -2, WHOLE_SECONDS_MAX + 1])
def test_a_pool_recycle_without_meaning_is_refused(seconds: int) -> None:
    _ = _refusal_of('pool_recycle', seconds)


def test_a_url_with_another_scheme_is_a_defect_named_without_its_password() -> None:
    with pytest.raises(AssertionError) as raised:
        core_database_url('postgres://user:secret@host/app')
    assert 'postgres://' in str(raised.value)
    assert 'secret' not in str(raised.value)
