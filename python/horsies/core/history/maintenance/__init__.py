"""History maintenance: leaf coverage, staged-reader publication, and pruning.

``coverage`` creates the leaves that writes need and publishes the staged
readers; ``pruning`` detaches and drops expired leaves; ``database`` is the
typed handle to the session-capable database that both use.
"""
