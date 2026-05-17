from pydantic import BaseModel, Field, field_validator, model_validator
from horsies.core.errors import ConfigurationError, ErrorCode


class PostgresConfig(BaseModel):
    database_url: str = Field(..., description='The URL of the PostgreSQL database')
    session_database_url: str | None = Field(
        default=None,
        description='Direct/session-capable PostgreSQL URL for LISTEN/NOTIFY and schema DDL',
    )
    pgbouncer_transaction_mode: bool = Field(
        default=False,
        description='Disable prepared statements for transaction-pooled PgBouncer URLs',
    )
    pool_pre_ping: bool = Field(
        default=True, description='Whether to pre-ping the database connection pool'
    )
    pool_size: int = Field(
        default=30, description='The size of the database connection pool'
    )
    max_overflow: int = Field(
        default=30, description='The maximum number of connections to allow in the pool'
    )
    pool_timeout: int = Field(
        default=30, description='The timeout for acquiring a connection from the pool'
    )
    pool_recycle: int = Field(
        default=1800, description='The number of seconds to recycle connections'
    )
    echo: bool = Field(default=False, description='Whether to echo the SQL statements')

    @field_validator('database_url')
    def validate_database_url(cls, v: str) -> str:
        if not v.startswith('postgresql+psycopg://'):
            raise ConfigurationError(
                message='invalid database URL scheme',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    f"got: {v.split('://')[0] if '://' in v else v[:20]}://...",
                    'horsies only supports psycopg3 (async PostgreSQL driver)',
                ],
                help_text="use 'postgresql+psycopg://user:pass@host/db'",
            )
        return v

    @field_validator('session_database_url')
    def validate_session_database_url(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not v.startswith('postgresql+psycopg://'):
            raise ConfigurationError(
                message='invalid session database URL scheme',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    f"got: {v.split('://')[0] if '://' in v else v[:20]}://...",
                    'session_database_url must use the psycopg3 PostgreSQL driver',
                ],
                help_text="use 'postgresql+psycopg://user:pass@host/db'",
            )
        return v

    @model_validator(mode='after')
    def validate_pgbouncer_configuration(self) -> 'PostgresConfig':
        if self.pgbouncer_transaction_mode and self.session_database_url is None:
            raise ConfigurationError(
                message='session_database_url required when pgbouncer_transaction_mode=True',
                code=ErrorCode.BROKER_INVALID_URL,
                notes=[
                    'PgBouncer transaction pooling cannot support persistent LISTEN sessions',
                ],
                help_text='use a direct/session-capable Postgres URL for LISTEN/NOTIFY',
            )
        return self

    @property
    def effective_session_database_url(self) -> str:
        return self.session_database_url or self.database_url

    @property
    def pooled_connect_args(self) -> dict[str, object]:
        if self.pgbouncer_transaction_mode:
            return {'prepare_threshold': None}
        return {}
