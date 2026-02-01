import warnings
from pydantic import BaseModel, Field, field_validator
from horsies.core.errors import ConfigurationError, ErrorCode


class PostgresConfig(BaseModel):
    database_url: str = Field(..., description='The URL of the PostgreSQL database')
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
        if not v.startswith('postgresql+psycopg'):
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


# =============================================================================
# DEPRECATED: These exceptions are no longer raised by the broker.
# Use TaskResult with LibraryErrorCode.TASK_NOT_FOUND and LibraryErrorCode.WAIT_TIMEOUT instead.
# =============================================================================


class TaskNotFoundError(Exception):
    """
    DEPRECATED: No longer raised by broker.get_result().

    The broker now returns TaskResult(err=TaskError(error_code=LibraryErrorCode.TASK_NOT_FOUND)).
    Check result.is_err() and result.err.error_code instead of catching this exception.
    """

    def __init__(self, *args: object) -> None:
        warnings.warn(
            'TaskNotFoundError is deprecated. '
            'Use TaskResult with LibraryErrorCode.TASK_NOT_FOUND instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args)


class TaskTimeoutError(Exception):
    """
    DEPRECATED: No longer raised by broker.get_result().

    The broker now returns TaskResult(err=TaskError(error_code=LibraryErrorCode.WAIT_TIMEOUT)).
    Check result.is_err() and result.err.error_code instead of catching this exception.
    """

    def __init__(self, *args: object) -> None:
        warnings.warn(
            'TaskTimeoutError is deprecated. '
            'Use TaskResult with LibraryErrorCode.WAIT_TIMEOUT instead.',
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args)
