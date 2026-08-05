# Contributing

## Task terminalization boundary

A transition of `horsies_tasks.status` to `COMPLETED`, `FAILED`, `CANCELLED`,
or `EXPIRED` must use the lifecycle boundary:

1. Define or extend an exhaustive command in
   `horsies/core/lifecycle/commands.py`.
2. Execute it through `horsies/core/lifecycle/persistence.py`.
3. Define the database mutation only in
   `horsies/core/schemas/terminalization.py`.
4. Return a typed outcome. Expected race outcomes are data, not exceptions or
   row-count interpretation.
5. Add the operation to the lifecycle matrix and exercise its applied,
   replay, refusal, lost-claim, and absence cases as applicable.

Do not add a direct terminal-status update to a broker, worker, monitoring,
workflow, or model module. Callers own transaction composition for attempt and
workflow-node writes; terminalization functions never commit.

`tests/unit/test_terminal_writer_inventory.py` enforces the database-program
boundary. `tests/integration/test_terminal_writer_characterization.py`
executes the sixteen original transition contracts against PostgreSQL.
