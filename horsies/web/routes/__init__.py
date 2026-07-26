"""Route modules for the monitoring app.

Each module exposes ``build_router`` so its routes close over the broker or
app they need, rather than reaching for shared global state.
"""
