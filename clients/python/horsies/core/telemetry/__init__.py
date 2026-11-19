"""The OpenTelemetry side of the Python client.

- `otel_api`: whether the OpenTelemetry API (`horsies[otel]`) is installed,
  found without an import of `opentelemetry`.

A module of this package imports `opentelemetry` only when the app's config
is `OtelTracing`. With `NoTracing` no module of the package imports it.
"""
