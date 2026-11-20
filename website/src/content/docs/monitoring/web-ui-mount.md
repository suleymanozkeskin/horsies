---
title: Web UI Mount
section: python
summary: Mount the monitoring app in a FastAPI application with create_monitoring_app, choose or write an authorization policy, and add a stylesheet.
related: [web-ui-deployment, web-ui-overview, ../cli]
tags: [monitoring, web, fastapi, authentication, api]
---

# Web UI Mount

`create_monitoring_app` builds the monitoring app as a FastAPI sub-app. It needs the `horsies[web]` extra. The modes, the authorization rules, the connection footprint and the schema states are on [Web UI Deployment & Authentication](/horsies/monitoring/web-ui-deployment/). The standalone server is the `horsies web` command: see the [CLI Reference](/horsies/cli/#horsies-web).

## How To

### Install the Extra

```bash
pip install "horsies[web]"
```

```bash
uv add "horsies[web]"
```

The extra adds `fastapi` and `uvicorn`. The query package `horsies.monitoring` imports neither and is available without the extra.

### Mount the App

```python
from fastapi import FastAPI
from horsies.web import AllowAll, create_monitoring_app

from myapp.tasks import app as horsies_app

api = FastAPI()
api.mount(
    "/monitoring",
    create_monitoring_app(horsies_app, auth_policy=AllowAll()),
)
```

Mount it at any path. `AllowAll()` states that the host application's own authentication guards this mount.

### Write a Policy

A policy implements the `MonitoringAuthPolicy` protocol:

```python
from typing import Protocol

from fastapi import Request


class MonitoringAuthPolicy(Protocol):
    async def can_view(self, request: Request) -> bool: ...
    async def can_act(self, request: Request) -> bool: ...
```

Implement the protocol to delegate to the session, JWT or RBAC layer of the host app:

```python
from fastapi import Request

from myapp.security import current_user


class SessionPolicy:
    async def can_view(self, request: Request) -> bool:
        user = await current_user(request)
        return user is not None

    async def can_act(self, request: Request) -> bool:
        user = await current_user(request)
        return user is not None and user.is_operator
```

### Add a Stylesheet

```python
from horsies.web import MonitoringUIConfig, ViewOnly, create_monitoring_app

from myapp.tasks import app as horsies_app

monitoring = create_monitoring_app(
    horsies_app,
    auth_policy=ViewOnly(),
    config=MonitoringUIConfig(custom_css_url="/static/horsies-theme.css"),
)
```

## API Reference

### `create_monitoring_app(app, *, auth_policy, config=None, actions_enabled=True) -> FastAPI`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `app` | `Horsies` | Yes | The app whose broker the monitoring app uses |
| `auth_policy` | `MonitoringAuthPolicy` | Yes | Keyword-only, no default |
| `config` | `MonitoringUIConfig \| None` | No | The page customization |
| `actions_enabled` | `bool` | No | `False` refuses every action, whatever the policy answers. Default `True` |

**Returns:** `FastAPI`, the sub-app to mount.

The sub-app also serves `GET /metrics`: the fleet metrics of the app's database, for a request that `can_view` allows. The `queue` and `schedule` label values are the queues and the schedules of `app.config`. See [Fleet Collector](/horsies/observability/fleet-collector/).

### Policies

| Constructor | Policy |
|-------------|--------|
| `AllowAll()` | `AllowAll` |
| `ViewOnly()` | `ViewOnly` |
| `TrustedHeader(header_name: str, *, allow_actions: bool)` | `TrustedHeader` with that header |

What each policy answers is in [Authorization policies](/horsies/monitoring/web-ui-deployment/#authorization-policies).

### `MonitoringUIConfig`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `custom_css_url` | `str \| None` | `None` | The URL of one stylesheet. Its link goes last in the served `index.html` |
| `trace_link` | `TraceLinkTemplate \| NoTraceLink` | `NoTraceLink()` | The link of a task's trace id in the task detail. `TraceLinkTemplate(template=...)` takes an `http` or `https` URL with one `{trace_id}`, at most 2,048 bytes of printable ASCII with no space, for example `https://grafana.example.com/explore?traceId={trace_id}`; another template raises `ConfigurationError` (`HRS-221`) that names the rule. With `NoTraceLink()` the task detail shows the trace id as text. A task sent with no trace shows no trace row |

### Static Assets

The SPA is compiled into `horsies._core`: the core embeds the committed build of `webui/` (`core/webui-dist`), so an installed wheel and a build from a clone serve the same page with no extra step. The page and its assets carry no data and are served without a call of the policy.
