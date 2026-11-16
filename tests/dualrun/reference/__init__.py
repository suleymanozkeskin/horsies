"""The statements of each switched operation, as the Python caller sent them before its switch.

One module per operation. Each keeps the statement text of the caller before
it moved to its baseline function (binds in psycopg form), and the order in
which the caller sent them. The modules exist only for the reference side of
the operation dual-runs (``tests/dualrun/operation.py``); no production code
imports them.
"""
