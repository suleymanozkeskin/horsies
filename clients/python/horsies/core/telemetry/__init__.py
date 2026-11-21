"""The OpenTelemetry side of the Python client.

- `otel_api`: whether the OpenTelemetry API (`horsies[otel]`) is installed,
  found without an import of `opentelemetry`.
- `send_telemetry`: the send telemetry of an app (`NoSendTelemetry` or
  `OtelSendTelemetry`), and the run of a send or a workflow start inside its
  span.
- `producer`: the spans and the producer metrics through the OpenTelemetry
  API; it imports `opentelemetry`.

A module of this package imports `opentelemetry` only when the app's config
is `OtelTracing`. With `NoTracing` no module of the package imports it.
"""
