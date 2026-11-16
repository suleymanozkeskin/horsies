"""Contract between the Pydantic broker config and the core's producer manifest.

`to_producer_manifest` maps `PostgresConfig` into the manifest generated
from horsies-core (`horsies/_catalog/manifest.py`). These tests pin that
every config field is mapped or named as outside the producer manifest,
that every manifest field has a source, that the mapping copies each value,
and that the Pydantic bounds are the core's bounds.
"""

from __future__ import annotations

import json
from typing import Final, cast

import pytest
from annotated_types import Ge, Le
from pydantic import SecretStr, ValidationError

from horsies._catalog.manifest import (
    MANIFEST_VERSION,
    POOL_CONNECTIONS_MAX,
    POOL_CONNECTIONS_MIN,
    WHOLE_SECONDS_MAX,
    WHOLE_SECONDS_MIN,
    DatabaseManifest,
    ProducerManifest,
    SendManifest,
    SessionDatabaseSameAsDatabaseUrl,
    SessionDatabaseSeparate,
    dump_producer_manifest,
)
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

# PostgresConfig field -> (Pydantic lower bound, upper bound) from the core.
CORE_BOUNDS: Final[dict[str, tuple[int, int]]] = {
    'pool_size': (POOL_CONNECTIONS_MIN, POOL_CONNECTIONS_MAX),
    'max_overflow': (POOL_CONNECTIONS_MIN, POOL_CONNECTIONS_MAX),
    'pool_timeout': (WHOLE_SECONDS_MIN, WHOLE_SECONDS_MAX),
    'pool_recycle': (WHOLE_SECONDS_MIN, WHOLE_SECONDS_MAX),
}

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
            pool_recycle=600,
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
            'pool_recycle': 1800,
            'echo': False,
        },
        'schema': 'MIGRATE_AND_VALIDATE',
        'send': {
            'retain_rerun_input_default': False,
            'idempotency_reservation_window': 60,
        },
    }
    assert manifest.database.session_database == SessionDatabaseSameAsDatabaseUrl()


@pytest.mark.parametrize('field', sorted(CORE_BOUNDS))
def test_pydantic_bounds_are_the_core_bounds(field: str) -> None:
    lower, upper = CORE_BOUNDS[field]
    metadata = PostgresConfig.model_fields[field].metadata
    assert [item.ge for item in metadata if isinstance(item, Ge)] == [lower]
    assert [item.le for item in metadata if isinstance(item, Le)] == [upper]
    for outside in (lower - 1, upper + 1):
        with pytest.raises(ValidationError):
            PostgresConfig.model_validate({'database_url': MAIN_URL, field: outside})


def test_a_url_with_another_scheme_is_a_defect_named_without_its_password() -> None:
    with pytest.raises(AssertionError) as raised:
        core_database_url('postgres://user:secret@host/app')
    assert 'postgres://' in str(raised.value)
    assert 'secret' not in str(raised.value)
