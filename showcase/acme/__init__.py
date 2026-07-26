# showcase/acme/__init__.py
"""Acme Clothing — the horsies showcase application.

A fictional fast-fashion retailer whose orders, payments, stock, and
shipments are real rows in a real database; only the outside world
(payment provider, courier, printer, mail gateway) is simulated.

Run everything from the repository root:

    uv run horsies worker showcase.acme.app:app --processes 12
    uv run horsies scheduler showcase.acme.app:app
    uv run horsies web showcase.acme.app:app --enable-actions
    uv run python -m showcase.acme.scenarios seed
"""
