# showcase/hemline/settings.py
"""Database URL resolution for the Hemline showcase.

Hemline owns its own database (``hemline_demo`` by default) so a demo run
never shares rows with the horsies development database. Both the horsies
tables and the ``hemline_*`` tables live in that one database.

Resolution order, first match wins:

1. ``HEMLINE_DATABASE_URL`` in the environment — used verbatim.
2. ``HEMLINE_DATABASE_URL`` in the repository ``.env`` — used verbatim.
3. ``DATABASE_URL`` in the environment, with its database name replaced by
   ``hemline_demo`` — reuses the local credentials already configured for
   horsies development without touching that database.
4. ``DATABASE_URL`` in the repository ``.env``, same replacement.
5. :data:`DEFAULT_DATABASE_URL`.

Rules 3 and 4 only ever rewrite the database name; host, port, credentials,
and query parameters are carried over untouched.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit, urlunsplit

HEMLINE_URL_VARIABLE: Final[str] = 'HEMLINE_DATABASE_URL'
SHARED_URL_VARIABLE: Final[str] = 'DATABASE_URL'
HEMLINE_DATABASE_NAME: Final[str] = 'hemline_demo'
MAINTENANCE_DATABASE_NAME: Final[str] = 'postgres'
SQLALCHEMY_SCHEME: Final[str] = 'postgresql+psycopg'
PSYCOPG_SCHEME: Final[str] = 'postgresql'
DEFAULT_DATABASE_URL: Final[str] = (
    f'{SQLALCHEMY_SCHEME}://postgres:postgres@localhost:5432/{HEMLINE_DATABASE_NAME}'
)


class SettingsError(RuntimeError):
    """A database URL could not be resolved into something usable."""


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """Every connection string Hemline needs, derived from one URL."""

    url: str
    """SQLAlchemy URL handed to :class:`horsies.PostgresConfig`."""

    psycopg_dsn: str
    """Same target, plain libpq form, used by ``store`` for domain tables."""

    maintenance_dsn: str
    """The ``postgres`` database on the same server, used to create the demo one."""

    database_name: str
    """Database the showcase writes to."""

    source: str
    """Which resolution rule produced :attr:`url` — printed by the scenarios."""


def _repository_root() -> Path | None:
    """Return the nearest ancestor directory holding ``pyproject.toml``."""
    for candidate in Path(__file__).resolve().parents:
        if (candidate / 'pyproject.toml').is_file():
            return candidate
    return None


def _read_env_file(path: Path) -> dict[str, str]:
    """Parse ``KEY=value`` lines. Comments, blanks, and bare keys are skipped."""
    try:
        content = path.read_text(encoding='utf-8')
    except OSError as error:
        raise SettingsError(
            f'cannot read {path}: {error}. '
            f'Set {HEMLINE_URL_VARIABLE} in the environment instead.',
        ) from error

    values: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip().removeprefix('export ').strip()
        if not line or line.startswith('#'):
            continue
        key, separator, value = line.partition('=')
        if not separator:
            continue
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _env_file_values() -> dict[str, str]:
    """Values from the repository ``.env``; empty when there is no such file."""
    root = _repository_root()
    if root is None:
        return {}
    env_path = root / '.env'
    if not env_path.is_file():
        return {}
    return _read_env_file(env_path)


def _with_database_name(url: str, database_name: str) -> str:
    """Return ``url`` pointing at ``database_name`` on the same server."""
    parts = urlsplit(url)
    return urlunsplit((
        parts.scheme,
        parts.netloc,
        f'/{database_name}',
        parts.query,
        parts.fragment,
    ))


def _with_scheme(url: str, scheme: str) -> str:
    """Return ``url`` with its scheme replaced."""
    parts = urlsplit(url)
    return urlunsplit((
        scheme,
        parts.netloc,
        parts.path,
        parts.query,
        parts.fragment,
    ))


def _select_url(env_file: dict[str, str]) -> tuple[str, str]:
    """Return ``(url, source)`` for the first resolution rule that matches."""
    direct_environment = os.environ.get(HEMLINE_URL_VARIABLE, '').strip()
    if direct_environment:
        return direct_environment, f'environment {HEMLINE_URL_VARIABLE}'

    direct_env_file = env_file.get(HEMLINE_URL_VARIABLE, '').strip()
    if direct_env_file:
        return direct_env_file, f'.env {HEMLINE_URL_VARIABLE}'

    shared_environment = os.environ.get(SHARED_URL_VARIABLE, '').strip()
    if shared_environment:
        return (
            _with_database_name(shared_environment, HEMLINE_DATABASE_NAME),
            f'environment {SHARED_URL_VARIABLE} (database -> {HEMLINE_DATABASE_NAME})',
        )

    shared_env_file = env_file.get(SHARED_URL_VARIABLE, '').strip()
    if shared_env_file:
        return (
            _with_database_name(shared_env_file, HEMLINE_DATABASE_NAME),
            f'.env {SHARED_URL_VARIABLE} (database -> {HEMLINE_DATABASE_NAME})',
        )

    return DEFAULT_DATABASE_URL, 'built-in default'


def _validate_url(url: str, source: str) -> str:
    """Return the database name, rejecting URLs horsies cannot use."""
    if not url.startswith(f'{SQLALCHEMY_SCHEME}://'):
        raise SettingsError(
            f'{source} must start with "{SQLALCHEMY_SCHEME}://" — got "{url}". '
            'horsies requires the psycopg3 async driver (HRS-203).',
        )
    database_name = urlsplit(url).path.lstrip('/')
    if not database_name:
        raise SettingsError(
            f'{source} has no database name: "{url}". '
            f'Append "/{HEMLINE_DATABASE_NAME}".',
        )
    return database_name


def resolve_database_settings() -> DatabaseSettings:
    """Resolve every connection string from the first rule that matches."""
    url, source = _select_url(_env_file_values())
    database_name = _validate_url(url, source)
    psycopg_dsn = _with_scheme(url, PSYCOPG_SCHEME)
    return DatabaseSettings(
        url=url,
        psycopg_dsn=psycopg_dsn,
        maintenance_dsn=_with_database_name(psycopg_dsn, MAINTENANCE_DATABASE_NAME),
        database_name=database_name,
        source=source,
    )


DATABASE: Final[DatabaseSettings] = resolve_database_settings()
