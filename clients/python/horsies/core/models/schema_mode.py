"""The schema behaviour of an app and of its broker, and the role of the app
process."""

from __future__ import annotations

from enum import Enum


class SchemaMode(str, Enum):
    """What the broker of an app does with the database schema.

    The value is the text of the producer manifest.
    """

    MIGRATE_AND_VALIDATE = 'MIGRATE_AND_VALIDATE'
    """The first operation applies the migrations and checks the schema
    version. The default."""

    OBSERVE_ONLY = 'OBSERVE_ONLY'
    """The broker never runs schema statements (monitoring reads)."""

    ASSUME_INITIALIZED = 'ASSUME_INITIALIZED'
    """The broker never runs schema statements and uses no listener: an
    operation that needs the listener answers ``LISTENER_DISABLED``."""


class AppRole(str, Enum):
    """What the process of an app does. The CLI sets it after discovery.

    A worker skips the schedule checks: it never sends a scheduled task.
    """

    PRODUCER = 'producer'
    """Sends tasks and reads their state. The role of a new app."""

    WORKER = 'worker'
    """Runs a worker."""

    SCHEDULER = 'scheduler'
    """Runs the scheduler."""
