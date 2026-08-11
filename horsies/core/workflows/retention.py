"""Retention class resolution for workflow-enqueued tasks.

Workflow nodes are enqueued by the engine rather than through a task
send, but they land on a queue like any other task, so the queue's
retention mapping governs them the same way.

Its own module because two callers need it — the engine and the
lifecycle path — and reaching across for a private name made the
lifecycle path depend on the engine's internals for one derivation it
does not otherwise share.
"""

from __future__ import annotations

from typing import Any

from horsies.core.models.retention import resolve_queue_retention_class


def queue_retention_class(broker: Any, queue_name: str) -> str | None:
    """The retention class a workflow backing task's queue resolves to.

    `broker` may be absent on paths that build parameters without one;
    no broker means no configuration to read, which resolves to the
    default class.
    """
    app = getattr(broker, 'app', None)
    config = getattr(app, 'config', None)
    return resolve_queue_retention_class(
        getattr(config, 'retention', None), queue_name
    )
