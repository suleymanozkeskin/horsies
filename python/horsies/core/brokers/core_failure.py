"""The cause that a broker error holds when the core reported the failure.

The core reports an expected failure of an operation as a record (a message
with its subject and cause, and a retryable flag), not as an exception. The
``exception`` field of the broker error classes holds a ``BaseException``, so
the facade stores a ``CoreOperationFailed`` built from the record. The core
exception classes of #796 (a database failure with its SQLSTATE, a connection
failure without one) replace it when the core reports them.
"""

from __future__ import annotations


class CoreOperationFailed(Exception):
    """A core operation reported a failure record.

    ``subject`` names the operation, for example
    ``'PostgresBroker.requeue_stale_claimed'``. ``message`` is the message of
    the record, with the subject and the cause that the core gave.
    ``args`` is ``(subject, message)``.
    """

    subject: str
    message: str

    def __init__(self, subject: str, message: str) -> None:
        super().__init__(subject, message)
        self.subject = subject
        self.message = message

    def __str__(self) -> str:
        return f'{self.subject}: {self.message}'
