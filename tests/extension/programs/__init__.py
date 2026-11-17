"""The programs that the extension tests run in fresh interpreters.

Each module runs as ``python -m tests.extension.programs.<module> <case>
<report path>``: it runs one named case and writes the case's report
(``report.run_case``).
"""
