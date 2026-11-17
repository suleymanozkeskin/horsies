"""The operation tables of the module switches, and the rebinding that runs the
integration tests once on the Python reference and once on the core.

- ``table``: the types of an operation table.
- ``lifecycle``: the table of the lifecycle switch.
- ``broker``: the table of the broker switch.
- ``monitoring_and_web``: the table of the monitoring and web switch.
- ``rebinding``: the identity rebinding, the tripwires and the call counters.
- ``plugin``: the pytest option ``--dual-run=<module>``, the ``implementation``
  fixture and the summary of the counters.
"""
