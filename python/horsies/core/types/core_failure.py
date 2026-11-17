"""The exception that stands for a failure record of the core.

The core reports an expected failure of an operation as a record (a message
with its subject and cause, and a retryable flag), not as an exception. Where
the Python facade must give an exception, it builds a ``CoreOperationFailed``
from the record:

- the ``exception`` field of a broker error, which holds a ``BaseException``;
- the raise of a facade whose Python caller raised the driver's exception
  (the worker's claim release).

The core exception classes of #796 (a database failure with its SQLSTATE, a
connection failure without one) replace it when the core reports them.
"""

from __future__ import annotations


class CoreOperationFailed(Exception):
    """A core operation reported a failure record.

    - ``subject``: the operation, for example
      ``'PostgresBroker.requeue_stale_claimed'``.
    - ``message``: the message of the record, with the subject and the cause
      that the core gave.
    - ``retryable``: the core's classification of the cause: ``True`` when
      the condition may clear, for example a lost connection.

    ``args`` is ``(subject, message, retryable)``.
    """

    subject: str
    message: str
    retryable: bool

    def __init__(self, subject: str, message: str, retryable: bool) -> None:
        super().__init__(subject, message, retryable)
        self.subject = subject
        self.message = message
        self.retryable = retryable

    def __str__(self) -> str:
        return f'{self.subject}: {self.message}'
