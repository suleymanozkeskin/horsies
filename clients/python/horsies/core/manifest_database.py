"""The database-section mapping that the producer and worker manifest builders share.

`clients/python/horsies/_catalog/manifest.py` and `clients/python/horsies/_catalog/worker_manifest.py` are
generated as two modules, so each has its own classes for the same Rust types
(`manifest.PoolRecycleNever` is not `worker_manifest.PoolRecycleNever`). The
rules that turn the validated `PostgresConfig` values into these types are
written once here. Each function takes the constructors of the target module
as keyword arguments and returns that module's type; the builders pass their
generated classes (`clients/python/horsies/core/manifest.py`, `clients/python/horsies/core/worker_manifest.py`).

Pydantic refuses every user error first, so a value that breaks a rule here is
a defect: it raises `AssertionError`. Every function is pure.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final, Protocol

from horsies.core.models.broker import SQLALCHEMY_NEVER_RECYCLE, PostgresConfig
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import check_database_url_scheme

CORE_URL_SCHEME: Final = 'postgresql://'
"""The scheme that the core's connection layer reads."""


def core_database_url(url: str) -> str:
    """`url` with the scheme that the core reads: `postgres://` becomes
    `postgresql://`, and the rest is kept as it is.

    The `PostgresConfig` validators admit only `postgresql://` and
    `postgres://` (`check_database_url_scheme`), so another URL here is a
    defect: it raises `AssertionError`, which names the refusal and never
    the rest of the URL (it can hold a password).
    """
    match check_database_url_scheme(url):
        case Ok():
            _, _, rest = url.partition('://')
            return CORE_URL_SCHEME + rest
        case Err(refusal):
            raise AssertionError(
                f'a validated database URL has the scheme postgresql or '
                f'postgres; got {refusal!r}'
            )


class SeparateSessionClass[S](Protocol):
    """The constructor of the separate session database variant."""

    def __call__(self, *, url: str) -> S: ...


class PoolRecycleAfterSecondsClass[R](Protocol):
    """The constructor of the pool recycle variant with an age in seconds."""

    def __call__(self, *, seconds: int) -> R: ...


class KeepalivesOnClass[K](Protocol):
    """The constructor of the keepalives-on variant with its three values."""

    def __call__(
        self, *, idle_seconds: int, interval_seconds: int, probes: int
    ) -> K: ...


class UserTimeoutAfterClass[T](Protocol):
    """The constructor of the TCP user timeout variant with milliseconds."""

    def __call__(self, *, milliseconds: int) -> T: ...


def session_database_with[S](
    config: PostgresConfig,
    *,
    same_as_database_url: Callable[[], S],
    separate: SeparateSessionClass[S],
) -> S:
    """No `session_database_url` is the main URL; a URL is the separate
    variant with the scheme that the core reads (`core_database_url`)."""
    match config.session_database_url:
        case None:
            return same_as_database_url()
        case session_url:
            return separate(url=core_database_url(session_url.get_secret_value()))


def pool_recycle_with[R](
    value: int,
    *,
    never: Callable[[], R],
    after_seconds: PoolRecycleAfterSecondsClass[R],
) -> R:
    """The typed form of a validated `pool_recycle`: `-1` (SQLAlchemy's
    "never") is the never variant, any other value a number of seconds.

    The `PostgresConfig` validator admits only these, so another value is a
    defect and raises `AssertionError`.
    """
    match value:
        case _ if value == SQLALCHEMY_NEVER_RECYCLE:
            return never()
        case _:
            assert value >= 0, (
                f'pool_recycle={value} passed the PostgresConfig validator, which '
                'admits only -1 and a number of seconds'
            )
            return after_seconds(seconds=value)


def tcp_keepalives_with[K](
    config: PostgresConfig,
    *,
    off: Callable[[], K],
    on: KeepalivesOnClass[K],
) -> K:
    """The typed form of the validated keepalive settings: off, or on with
    the idle time, the interval and the probe count. The three values are
    not read when `tcp_keepalives` is False.

    The `PostgresConfig` validator admits only values of at least 1 when
    keepalives are on, so another value is a defect and raises
    `AssertionError`.
    """
    match config.tcp_keepalives:
        case False:
            return off()
        case True:
            values = (
                config.tcp_keepalives_idle,
                config.tcp_keepalives_interval,
                config.tcp_keepalives_count,
            )
            assert min(values) >= 1, (
                f'keepalive values {values} passed the PostgresConfig validator, '
                'which admits only values of at least 1 when tcp_keepalives is True'
            )
            return on(
                idle_seconds=config.tcp_keepalives_idle,
                interval_seconds=config.tcp_keepalives_interval,
                probes=config.tcp_keepalives_count,
            )


def tcp_user_timeout_with[T](
    milliseconds: int,
    *,
    system_default: Callable[[], T],
    after: UserTimeoutAfterClass[T],
) -> T:
    """The typed form of a validated `tcp_user_timeout_ms`: `0` is the
    operating system's timeout, any other value a number of milliseconds.

    The `PostgresConfig` validator admits only values of at least 0, so
    another value is a defect and raises `AssertionError`.
    """
    match milliseconds:
        case 0:
            return system_default()
        case _:
            assert milliseconds > 0, (
                f'tcp_user_timeout_ms={milliseconds} passed the PostgresConfig '
                'validator, which admits only values of at least 0'
            )
            return after(milliseconds=milliseconds)
