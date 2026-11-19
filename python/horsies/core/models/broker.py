from collections.abc import Mapping
from typing import Any, ClassVar, Final, assert_never, cast

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from horsies._catalog.manifest import (
    KEEPALIVE_PROBES_MAX,
    KEEPALIVE_PROBES_MIN,
    KEEPALIVE_SECONDS_MAX,
    KEEPALIVE_SECONDS_MIN,
    POOL_CONNECTIONS_MAX,
    POOL_CONNECTIONS_MIN,
    POOL_SIZE_MAX,
    POOL_SIZE_MIN,
    TCP_USER_TIMEOUT_MS_MAX,
    TCP_USER_TIMEOUT_MS_MIN,
    WHOLE_SECONDS_MAX,
    WHOLE_SECONDS_MIN,
)
from horsies.core.defaults import DEFAULT_POOL_RECYCLE_S, DEFAULT_TCP_USER_TIMEOUT_MS
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import (
    DatabaseUrlNamesDriver,
    DatabaseUrlNotPostgres,
    DatabaseUrlRefusal,
    DatabaseUrlWithoutScheme,
    check_database_url_scheme,
)
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
)

SQLALCHEMY_UNLIMITED_OVERFLOW: Final = -1
"""SQLAlchemy's `max_overflow` for no overflow limit."""

SQLALCHEMY_NEVER_RECYCLE: Final = -1
"""SQLAlchemy's `pool_recycle` for a connection that is never replaced by age."""

SQLALCHEMY_UNLIMITED_POOL_SIZE: Final = 0
"""SQLAlchemy's `pool_size` for no size limit."""

LIBPQ_SYSTEM_DEFAULT_TCP_USER_TIMEOUT: Final = 0
"""libpq's `tcp_user_timeout` for the operating system's timeout."""

MOVED_TO_WORKER_PROFILE: Final[Mapping[str, str]] = {
    'worker_child_pool_min_size': 'min_size=N',
    'worker_child_pool_max_size': 'max_size=PoolSize(connections=N)',
    'worker_child_pool_check': 'check=False',
}
"""The child pool fields that 0.7.0 removed from ``PostgresConfig``, and the
argument of the pool of each child kind in a worker profile
(``IsolatedPool``, ``SharedPool``) that replaces each one."""

MOVED_TO_PARENT_POOL: Final[Mapping[str, str]] = {
    'worker_pool_size': (
        'the worker process opens its pool with the resolved pool.max_size '
        'of its worker profile'
    ),
    'worker_max_overflow': (
        'the pool of the worker process has no overflow: it holds at most '
        'pool.max_size connections'
    ),
}
"""The worker pool fields that 0.7.0 removed from ``PostgresConfig``, and the
note of the refusal of each one. ``WorkerProfile.pool`` replaces both."""


def _pool_value_error(
    field: str, value: int, cause: str, help_text: str,
) -> ConfigurationError:
    """The refusal of a pool value of `PostgresConfig`."""
    return ConfigurationError(
        message=f'invalid {field}',
        code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
        notes=[f'got: {field}={value}', cause],
        help_text=help_text,
    )


def _tcp_value_error(
    field: str, value: int, cause: str, help_text: str,
) -> ConfigurationError:
    """The refusal of a TCP keepalive or user timeout value of `PostgresConfig`."""
    return ConfigurationError(
        message=f'invalid {field}',
        code=ErrorCode.CONFIG_INVALID_KEEPALIVE,
        notes=[f'got: {field}={value}', cause],
        help_text=help_text,
    )


class PostgresConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')

    _ENGINE_KWARG_EXCLUDES: ClassVar[set[str]] = {
        'database_url',
        'session_database_url',
        'pgbouncer_transaction_mode',
        'retain_rerun_input_default',
        'pool_pre_ping',
        'worker_pool_pre_ping',
        'tcp_keepalives',
        'tcp_keepalives_idle',
        'tcp_keepalives_interval',
        'tcp_keepalives_count',
        'tcp_user_timeout_ms',
    }

    # SecretStr: repr()/model_dump() of the config must never expose
    # credentials; internal consumers unwrap via get_secret_value().
    database_url: SecretStr = Field(..., description='The URL of the PostgreSQL database')
    session_database_url: SecretStr | None = Field(
        default=None,
        description=(
            'Direct/session-capable PostgreSQL URL for schema DDL, partition '
            'maintenance, and LISTEN/NOTIFY'
        ),
    )
    pgbouncer_transaction_mode: bool = Field(
        default=False,
        description='Disable prepared statements for transaction-pooled PgBouncer URLs',
    )
    # The ping runs at checkout only for a connection that waited in the
    # pool for longer than 1 s (the core's idle ping, DEFAULT_IDLE_PING_AFTER
    # of crates/horsies-core/src/db/pool.rs). A connection that dies within
    # that time of its check-in fails the next statement on it, as one that
    # dies after a ping does.
    pool_pre_ping: bool = Field(
        default=True,
        description=(
            'Whether to ping a pooled connection at checkout when it waited '
            'in the pool for longer than 1 s'
        ),
    )
    # The pre-ping of the worker process's engine (worker_process_config).
    # The worker sends most of its writes as one statement in autocommit:
    # one round trip each. A pre-ping adds a round trip to a checkout of a
    # connection that waited in the pool for longer than 1 s. With it off,
    # a dead pooled connection shows at the first statement instead, as a
    # connection error: finalize retries its phase and classifies the
    # replay, the claim loop retries the pass. TCP keepalives and
    # tcp_user_timeout_ms still apply.
    worker_pool_pre_ping: bool = Field(
        default=False,
        description=(
            'Whether the worker process pre-pings its pooled connections; '
            'pool_pre_ping applies to other engines'
        ),
    )
    # The producer defaults match SQLAlchemy's (5 + 10) for each pool. A
    # producer's broker holds one core pool of this size on database_url for
    # every thread and event loop that calls it, and one more on
    # session_database_url when that URL differs. See Connection Ceiling in
    # the broker config docs. Raise them for producers that send at a high
    # rate. A worker process sizes its own pool in its worker
    # profile (WorkerProfile.pool), and task code in a worker child opens
    # the task_send_pool of its child kind.
    # The pool values keep each SQLAlchemy meaning that the core can honor,
    # with the core's bounds (horsies._catalog.manifest): a config that
    # Pydantic accepts is one that the core accepts. See the validators below.
    pool_size: int = Field(
        default=5, description='The size of the database connection pool'
    )
    max_overflow: int = Field(
        default=10, description='The maximum number of connections to allow in the pool'
    )
    retain_rerun_input_default: bool = Field(
        default=False,
        description=(
            'App-level default for enqueue-time rerun-input retention; '
            'a per-task value overrides it, and the resolved value is '
            'snapshotted at enqueue'
        ),
    )
    pool_timeout: int = Field(
        default=30, description='The timeout for acquiring a connection from the pool'
    )
    pool_recycle: int = Field(
        default=DEFAULT_POOL_RECYCLE_S,
        description='The number of seconds to recycle connections',
    )
    # TCP keepalives keep idle pooled sockets warm so a server-side or
    # middlebox idle-reap is detected and recycled at the socket layer
    # instead of surfacing as a mid-query OperationalError. pool_pre_ping
    # and pool_recycle are checkout-time guards and cannot catch a
    # connection that dies in-flight. These apply to the broker engine pool
    # and the per-child connection pool; the LISTEN/NOTIFY listener sets its
    # own keepalives independently.
    # Each TCP value has the core's bounds (horsies._catalog.manifest), which
    # are Linux's limits (tcp(7)). macOS's limits are higher (TCP_KEEPALIVE
    # and TCP_KEEPINTVL 4,294,967 s, TCP_KEEPCNT 2^31 - 1), and macOS has no
    # TCP_USER_TIMEOUT (libpq ignores tcp_user_timeout there). So on each
    # supported platform the core's bound is the lower one: a config that
    # Pydantic accepts is one that the core and the socket accept. See
    # validate_tcp_settings.
    tcp_keepalives: bool = Field(
        default=True,
        description='Enable libpq TCP keepalives on broker and child-process connections',
    )
    tcp_keepalives_idle: int = Field(
        default=30,
        description='Idle seconds before the first keepalive probe (libpq keepalives_idle)',
    )
    tcp_keepalives_interval: int = Field(
        default=10,
        description='Seconds between keepalive probes (libpq keepalives_interval)',
    )
    tcp_keepalives_count: int = Field(
        default=3,
        description='Unacknowledged probes before the connection is dropped (libpq keepalives_count)',
    )
    # Keepalives probe only an idle connection. While sent data waits for
    # an acknowledgement, the kernel retransmission limit decides instead,
    # and a write to a dead connection (no reset from the server side)
    # blocks for about 15 minutes on Linux. tcp_user_timeout bounds that
    # wait. It does not affect a long query: the client's sent data is
    # acknowledged while it waits for the result.
    tcp_user_timeout_ms: int = Field(
        default=DEFAULT_TCP_USER_TIMEOUT_MS,
        description=(
            'Milliseconds that sent data may stay unacknowledged before the '
            'connection is closed (libpq tcp_user_timeout); 0 uses the '
            'operating system default'
        ),
    )
    # With echo on, the core logs each statement it runs at INFO on the
    # horsies.sql logger: the SQL text as the message, the statement name as
    # the attribute ``statement``, never the bound values. The broker then
    # sets that logger to INFO and, when it has no handler, adds one stdout
    # handler per process (horsies.core.logging.enable_statement_echo), as
    # SQLAlchemy's echo does for its engine logger.
    echo: bool = Field(
        default=False,
        description=(
            'Log each SQL statement at INFO on the horsies.sql logger, without '
            'bound values; a stdout handler is added when the logger has none'
        ),
    )

    @model_validator(mode='before')
    @classmethod
    def reject_moved_pool_fields(cls, data: object) -> object:
        """Fail closed on the pool fields that moved to the worker profiles.

        In 0.7.0 each child kind has its own pool
        (``IsolatedChildren.pool``, ``SharedAsyncChildren.pool``,
        ``SharedThreadChildren.pool``), and the worker process has its own
        (``WorkerProfile.pool``). A config that still names a removed field
        is refused, never silently ignored. Every such field is reported at
        once, so one edit fixes the config.
        """
        if not isinstance(data, dict):
            return data
        fields = cast('dict[str, object]', data)
        report = ValidationReport('broker')
        for name, pool_argument in MOVED_TO_WORKER_PROFILE.items():
            if name in fields:
                report.add(_moved_child_pool_field_error(name, pool_argument))
        for name, note in MOVED_TO_PARENT_POOL.items():
            if name in fields:
                report.add(_moved_parent_pool_field_error(name, note))
        raise_collected(report)
        return fields

    @field_validator('pool_size')
    def validate_pool_size(cls, v: int) -> int:
        """Accept a pool size from ``POOL_SIZE_MIN`` to ``POOL_SIZE_MAX``.

        Any other value raises ``ConfigurationError``
        (``CONFIG_INVALID_BROKER_POOL``). ``0`` is refused with its own cause:
        SQLAlchemy reads it as no size limit, and the core always bounds its
        connections.
        """
        match v:
            case _ if POOL_SIZE_MIN <= v <= POOL_SIZE_MAX:
                return v
            case _ if v == SQLALCHEMY_UNLIMITED_POOL_SIZE:
                raise _pool_value_error(
                    'pool_size',
                    v,
                    'pool_size=0 means no size limit in SQLAlchemy, and the core '
                    'always bounds its connections',
                    f'give a pool size from {POOL_SIZE_MIN} to {POOL_SIZE_MAX}',
                )
            case _:
                raise _pool_value_error(
                    'pool_size',
                    v,
                    f'a pool size is from {POOL_SIZE_MIN} to {POOL_SIZE_MAX}',
                    f'give a pool size from {POOL_SIZE_MIN} to {POOL_SIZE_MAX}',
                )

    @field_validator('max_overflow')
    def validate_max_overflow(cls, v: int) -> int:
        """Accept an overflow from ``POOL_CONNECTIONS_MIN`` to ``POOL_CONNECTIONS_MAX``.

        Any other value raises ``ConfigurationError``
        (``CONFIG_INVALID_BROKER_POOL``). ``-1`` is refused with its own cause:
        SQLAlchemy reads it as no overflow limit, and the core always bounds
        its connections.
        """
        match v:
            case _ if POOL_CONNECTIONS_MIN <= v <= POOL_CONNECTIONS_MAX:
                return v
            case _ if v == SQLALCHEMY_UNLIMITED_OVERFLOW:
                raise _pool_value_error(
                    'max_overflow',
                    v,
                    'max_overflow=-1 means no overflow limit in SQLAlchemy, and the '
                    'core always bounds its connections',
                    f'give a number from {POOL_CONNECTIONS_MIN} to {POOL_CONNECTIONS_MAX}',
                )
            case _:
                raise _pool_value_error(
                    'max_overflow',
                    v,
                    f'max_overflow is from {POOL_CONNECTIONS_MIN} to {POOL_CONNECTIONS_MAX}',
                    f'give a number from {POOL_CONNECTIONS_MIN} to {POOL_CONNECTIONS_MAX}',
                )

    @field_validator('pool_timeout')
    def validate_pool_timeout(cls, v: int) -> int:
        """Accept whole seconds from ``WHOLE_SECONDS_MIN`` to ``WHOLE_SECONDS_MAX``.

        ``0`` means that a checkout does not wait. Any other value raises
        ``ConfigurationError`` (``CONFIG_INVALID_BROKER_POOL``).
        """
        match v:
            case _ if WHOLE_SECONDS_MIN <= v <= WHOLE_SECONDS_MAX:
                return v
            case _:
                raise _pool_value_error(
                    'pool_timeout',
                    v,
                    f'pool_timeout is from {WHOLE_SECONDS_MIN} (a checkout does not wait) '
                    f'to {WHOLE_SECONDS_MAX} seconds',
                    f'give a number of seconds from {WHOLE_SECONDS_MIN} to {WHOLE_SECONDS_MAX}',
                )

    @field_validator('pool_recycle')
    def validate_pool_recycle(cls, v: int) -> int:
        """Accept ``-1`` or whole seconds from ``WHOLE_SECONDS_MIN`` to ``WHOLE_SECONDS_MAX``.

        ``-1`` means that a connection is never replaced by age. Any other
        value raises ``ConfigurationError`` (``CONFIG_INVALID_BROKER_POOL``).
        """
        match v:
            case _ if WHOLE_SECONDS_MIN <= v <= WHOLE_SECONDS_MAX:
                return v
            case _ if v == SQLALCHEMY_NEVER_RECYCLE:
                return v
            case _:
                raise _pool_value_error(
                    'pool_recycle',
                    v,
                    'pool_recycle is -1 (never replace a connection by age) or '
                    f'{WHOLE_SECONDS_MIN} to {WHOLE_SECONDS_MAX} seconds',
                    'give -1 or a number of seconds',
                )

    @field_validator('database_url')
    def validate_database_url(cls, v: SecretStr) -> SecretStr:
        """Accept only a ``postgresql://`` or ``postgres://`` URL."""
        _raise_for_url_scheme('database_url', v.get_secret_value())
        return v

    @field_validator('session_database_url')
    def validate_session_database_url(cls, v: SecretStr | None) -> SecretStr | None:
        """Accept only a ``postgresql://`` or ``postgres://`` URL, or None."""
        if v is None:
            return v
        _raise_for_url_scheme('session_database_url', v.get_secret_value())
        return v

    @model_validator(mode='after')
    def validate_pgbouncer_configuration(self) -> 'PostgresConfig':
        if self.pgbouncer_transaction_mode and self.session_database_url is None:
            raise ConfigurationError(
                message='session_database_url required when pgbouncer_transaction_mode=True',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    'PgBouncer transaction pooling cannot support session locks or persistent LISTEN sessions',
                ],
                help_text='use a direct/session-capable Postgres URL for session_database_url',
            )
        if (
            self.pgbouncer_transaction_mode
            and self.session_database_url == self.database_url
        ):
            raise ConfigurationError(
                message='session_database_url must not use the transaction-pool URL',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    'partition maintenance and LISTEN require PostgreSQL session affinity',
                ],
                help_text='use a direct/session-capable Postgres URL for session_database_url',
            )
        return self

    @model_validator(mode='after')
    def validate_tcp_settings(self) -> 'PostgresConfig':
        """Refuse a TCP value outside the core's bounds
        (`horsies._catalog.manifest`), with `CONFIG_INVALID_KEEPALIVE`.

        The three keepalive values are checked only when `tcp_keepalives`
        is True: when it is False, no connection and no manifest reads
        them. `tcp_user_timeout_ms` is `0` (the operating system's
        timeout) or a number of milliseconds within the core's bounds. The
        first value out of bounds is refused, in field order.
        """
        match self.tcp_keepalives:
            case True:
                for name, value, low, high in (
                    (
                        'tcp_keepalives_idle',
                        self.tcp_keepalives_idle,
                        KEEPALIVE_SECONDS_MIN,
                        KEEPALIVE_SECONDS_MAX,
                    ),
                    (
                        'tcp_keepalives_interval',
                        self.tcp_keepalives_interval,
                        KEEPALIVE_SECONDS_MIN,
                        KEEPALIVE_SECONDS_MAX,
                    ),
                    (
                        'tcp_keepalives_count',
                        self.tcp_keepalives_count,
                        KEEPALIVE_PROBES_MIN,
                        KEEPALIVE_PROBES_MAX,
                    ),
                ):
                    if not low <= value <= high:
                        raise _tcp_value_error(
                            name,
                            value,
                            f'{name} is from {low} to {high} when tcp_keepalives is True',
                            f'give a number from {low} to {high}, or set '
                            'tcp_keepalives=False to turn keepalives off',
                        )
            case False:
                pass
        timeout = self.tcp_user_timeout_ms
        match timeout:
            case _ if timeout == LIBPQ_SYSTEM_DEFAULT_TCP_USER_TIMEOUT:
                return self
            case _ if TCP_USER_TIMEOUT_MS_MIN <= timeout <= TCP_USER_TIMEOUT_MS_MAX:
                return self
            case _:
                raise _tcp_value_error(
                    'tcp_user_timeout_ms',
                    timeout,
                    f'tcp_user_timeout_ms is {LIBPQ_SYSTEM_DEFAULT_TCP_USER_TIMEOUT} '
                    '(the operating system default) or '
                    f'{TCP_USER_TIMEOUT_MS_MIN} to {TCP_USER_TIMEOUT_MS_MAX} milliseconds',
                    f'give {LIBPQ_SYSTEM_DEFAULT_TCP_USER_TIMEOUT} or a number of '
                    f'milliseconds from {TCP_USER_TIMEOUT_MS_MIN} to {TCP_USER_TIMEOUT_MS_MAX}',
                )

    @property
    def effective_session_database_url(self) -> str:
        url = self.session_database_url or self.database_url
        return url.get_secret_value()

    def keepalive_connect_args(self) -> dict[str, int]:
        """libpq keepalive params for psycopg connections, empty when disabled."""
        if not self.tcp_keepalives:
            return {}
        return {
            'keepalives': 1,
            'keepalives_idle': self.tcp_keepalives_idle,
            'keepalives_interval': self.tcp_keepalives_interval,
            'keepalives_count': self.tcp_keepalives_count,
        }

    def tcp_connect_args(self) -> dict[str, int]:
        """libpq TCP params for psycopg connections: the keepalive params
        and tcp_user_timeout."""
        return {
            **self.keepalive_connect_args(),
            'tcp_user_timeout': self.tcp_user_timeout_ms,
        }

    @property
    def pooled_connect_args(self) -> dict[str, object]:
        """psycopg connect_args for pooled broker/child connections.

        Merges the TCP params with the PgBouncer transaction-mode knob
        (prepared statements disabled). Both are valid psycopg connect
        kwargs and coexist.
        """
        args: dict[str, object] = dict(self.tcp_connect_args())
        if self.pgbouncer_transaction_mode:
            args['prepare_threshold'] = None
        return args

    def sqlalchemy_engine_kwargs(self) -> dict[str, Any]:
        """Return only kwargs accepted by SQLAlchemy's engine constructor.

        ``hide_parameters=True`` is fixed: bound values are task arguments
        and results, so statement error text and ``echo`` logs show the
        statement without them. The flag changes the text only; the
        exception's ``params`` attribute still holds the values. Postgres's
        own error text is not affected: a constraint violation's
        ``DETAIL: Failing row contains (...)`` shows column values, each cut
        to 64 bytes.
        """
        return {
            **self.model_dump(
                exclude=self._ENGINE_KWARG_EXCLUDES,
                exclude_none=True,
            ),
            'hide_parameters': True,
        }

    def with_exact_pool(self, connections: int) -> 'PostgresConfig':
        """A copy whose engine pool holds up to ``connections`` connections
        with no overflow: ``pool_size = connections``, ``max_overflow = 0``.
        Every other field, ``pool_pre_ping`` included, is kept.

        A worker child opens the broker of its task code with the
        ``task_send_pool`` of its kind, a producer engine that keeps
        ``pool_pre_ping``. ``connections`` is 1 or more, from a validated
        profile. Below 1 is a defect: ``AssertionError``.
        """
        assert connections >= 1, f'connections must be >= 1; got {connections}'
        return self.model_copy(
            update={'pool_size': connections, 'max_overflow': 0},
        )

    def worker_process_config(self, parent_pool: int) -> 'PostgresConfig':
        """The broker config of the worker process: a pool of up to
        ``parent_pool`` connections with no overflow (``with_exact_pool``),
        the resolved ``WorkerProfile.pool``, and ``worker_pool_pre_ping`` as
        its pre-ping. ``parent_pool`` is 1 or more. Below 1 is a defect:
        ``AssertionError``."""
        return self.with_exact_pool(parent_pool).model_copy(
            update={'pool_pre_ping': self.worker_pool_pre_ping},
        )


def _moved_child_pool_field_error(name: str, pool_argument: str) -> ConfigurationError:
    """The refusal of a removed child pool field: it names the pool field of
    each child kind in a worker profile that replaces it."""
    return ConfigurationError(
        message=f'{name} was removed in 0.7.0',
        code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
        notes=[
            'each child kind of a worker profile has its own connection pool',
        ],
        help_text=(
            'set the pool of each child kind in AppConfig.worker_profiles: '
            f'IsolatedChildren(pool=IsolatedPool({pool_argument})), '
            f'SharedAsyncChildren(pool=SharedPool({pool_argument})) or '
            f'SharedThreadChildren(pool=SharedPool({pool_argument}))'
        ),
    )


def _moved_parent_pool_field_error(name: str, note: str) -> ConfigurationError:
    """The refusal of a removed worker pool field: it names the pool of the
    worker process in a worker profile that replaces it."""
    return ConfigurationError(
        message=f'{name} was removed in 0.7.0',
        code=ErrorCode.CONFIG_INVALID_BROKER_POOL,
        notes=[
            note,
            'task code in a worker child that sends opens the task_send_pool '
            'of its child kind, not this pool',
        ],
        help_text=(
            'set the pool of the worker process in AppConfig.worker_profiles: '
            'WorkerProfile(pool=ParentPool(max_size=PoolSize(connections=N))); '
            'the default, PoolSizeFromSeats(), derives it from the seats of '
            'the profile'
        ),
    )


def _raise_for_url_scheme(field_name: str, url: str) -> None:
    """Raise ``ConfigurationError`` (``BROKER_INVALID_URL``) for a URL whose
    scheme is not ``postgresql`` or ``postgres``."""
    match check_database_url_scheme(url):
        case Ok():
            return
        case Err(refusal):
            raise _url_scheme_error(field_name, refusal)


def _url_scheme_error(
    field_name: str,
    refusal: DatabaseUrlRefusal,
) -> ConfigurationError:
    """The refusal of a URL scheme. It names the scheme, never the rest of
    the URL, and the fix."""
    match refusal:
        case DatabaseUrlNamesDriver(scheme=scheme):
            return ConfigurationError(
                message=f'invalid {field_name} scheme {scheme!r}',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    f'got: {scheme}://...',
                    'the URL names a driver; horsies opens the database '
                    'with psycopg 3 and takes no driver in the URL',
                ],
                help_text=(
                    f"replace '{scheme}://' with 'postgresql://' "
                    "(postgresql://user:pass@host/db)"
                ),
            )
        case DatabaseUrlNotPostgres(scheme=scheme):
            return ConfigurationError(
                message=f'invalid {field_name} scheme {scheme!r}',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    f'got: {scheme}://...',
                    'horsies accepts the schemes postgresql:// and postgres://',
                ],
                help_text="use 'postgresql://user:pass@host/db'",
            )
        case DatabaseUrlWithoutScheme():
            return ConfigurationError(
                message=f'invalid {field_name}: the URL has no scheme',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=['horsies accepts the schemes postgresql:// and postgres://'],
                help_text="use 'postgresql://user:pass@host/db'",
            )
        case _ as unreachable:
            assert_never(unreachable)
