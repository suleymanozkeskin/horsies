# showcase/hemline/__init__.py
"""Hemline — the horsies showcase application.

A fictional fast-fashion retailer whose orders, payments, stock, and
shipments are real rows in a real database; only the outside world
(payment provider, courier, printer, mail gateway) is simulated.

Run everything from the repository root:

    uv run horsies worker showcase.hemline.app:app --processes 12
    uv run horsies scheduler showcase.hemline.app:app
    uv run horsies web showcase.hemline.app:app --enable-actions
    uv run python -m showcase.hemline.scenarios seed
"""
