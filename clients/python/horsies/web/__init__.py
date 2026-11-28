"""Monitoring web application for a horsies deployment.

Requires the ``web`` extra::

    pip install 'horsies[web]'

Mount it under an application that already authenticates its callers:

```python
from fastapi import FastAPI

from horsies.web import AllowAll, create_monitoring_app

host_app = FastAPI()  # your application, which authenticates its callers
host_app.mount('/monitoring', create_monitoring_app(app, authorization_policy=AllowAll()))
```

Or serve it standalone with ``horsies web``.

Nothing in ``horsies.monitoring`` imports this package: the query and action
APIs stay available without the extra.
"""

from __future__ import annotations

try:
    import fastapi as _fastapi
except ImportError as exc:  # pragma: no cover - exercised by the extra's absence
    raise ImportError(
        'horsies.web requires the web extra. Install it with: '
        "pip install 'horsies[web]'"
    ) from exc

del _fastapi

from horsies.web.app import (  # noqa: E402 - must follow the extra guard
    MetaResponse,
    MonitoringUIConfig,
    TraceLink,
    TraceLinkTemplate,
    TraceLinkText,
    create_monitoring_app,
)
from horsies.web.schema import (  # noqa: E402 - must follow the extra guard
    SCHEMA_INCOMPATIBLE,
    SCHEMA_UNKNOWN,
    SchemaIncompatible,
    SchemaProbe,
    SchemaState,
    SchemaStatus,
)
from horsies.web.authorization import (  # noqa: E402 - must follow the extra guard
    INTENT_HEADER,
    INTENT_VALUE,
    Actions,
    AllowAll,
    MonitoringAuthorizationPolicy,
    TrustedHeader,
    ViewOnly,
)

__all__ = [
    'INTENT_HEADER',
    'INTENT_VALUE',
    'SCHEMA_INCOMPATIBLE',
    'SCHEMA_UNKNOWN',
    'SchemaIncompatible',
    'SchemaProbe',
    'SchemaState',
    'SchemaStatus',
    'Actions',
    'AllowAll',
    'MetaResponse',
    'MonitoringAuthorizationPolicy',
    'MonitoringUIConfig',
    'TraceLink',
    'TraceLinkTemplate',
    'TraceLinkText',
    'TrustedHeader',
    'ViewOnly',
    'create_monitoring_app',
]
