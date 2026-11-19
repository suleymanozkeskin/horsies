"""The sync enqueue wrapper mirrors ``enqueue_async``'s keyword surface.

The sync wrapper forwards a hand-listed set of keywords to
``enqueue_async``. A keyword added to the async signature but not the
wrapper raises ``TypeError`` only at a real sync call site — unit tests
with mock brokers accept any keyword, and async integration never
touches the wrapper, so the first caller to notice is a production sync
send. The signature-parity pin makes the omission a unit failure
instead: every keyword-capable parameter of ``enqueue_async`` must
appear on ``enqueue``.
"""

from __future__ import annotations

import inspect

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY


def _keyword_names(func: object) -> set[str]:
    signature = inspect.signature(func)  # type: ignore[arg-type]
    return {
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind
        in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
        and name != 'self'
    }


def test_sync_enqueue_carries_every_async_keyword() -> None:
    missing = _keyword_names(PostgresBroker.enqueue_async) - _keyword_names(
        PostgresBroker.enqueue
    )
    assert not missing, (
        'PostgresBroker.enqueue is missing keyword(s) enqueue_async '
        f'accepts: {sorted(missing)} — the sync wrapper forwards a '
        'hand-listed set, so every new enqueue keyword must be added '
        'to both the signature and the forwarding call'
    )


def test_sync_enqueue_retention_default_matches_async() -> None:
    sync_default = inspect.signature(PostgresBroker.enqueue).parameters[
        'retention_class_key'
    ].default
    async_default = inspect.signature(
        PostgresBroker.enqueue_async
    ).parameters['retention_class_key'].default
    assert sync_default == async_default == DEFAULT_RETENTION_CLASS_KEY
