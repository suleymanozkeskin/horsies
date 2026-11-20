# Contributing

## Task terminalization boundary

A transition of `horsies_tasks.status` to `COMPLETED`, `FAILED`, `CANCELLED`,
or `EXPIRED` must use the lifecycle boundary of the core:

1. Define or extend an exhaustive command in
   `core/src/core/lifecycle/commands.rs` (`TerminalizationCommand`).
2. Execute it through `core/src/broker/terminalization.rs`.
3. Define the database mutation only in the baseline SQL functions
   (`core/baseline/`).
4. Return a typed outcome (`TerminalizationOutcome`,
   `core/src/core/lifecycle/outcomes.rs`). Expected race outcomes are data,
   not exceptions or row-count interpretation.
5. Add the operation to the lifecycle matrix and exercise its applied,
   replay, refusal, lost-claim, and absence cases as applicable.

Do not add a direct terminal-status update to a broker, worker, monitoring,
workflow, or model module. Callers own transaction composition for attempt and
workflow-node writes; terminalization functions never commit.

`core/tests/writer_inventory.rs` enforces the database-program boundary.
`core/src/broker/terminalization_matrix.rs` executes the transition matrix
against PostgreSQL, and
`core/src/broker/lifecycle_tests/terminal_writer_characterization.rs` names,
for each terminalization kind, one test where its writer applies and one
where it refuses. `clients/python/tests/lifecycle_matrix.py` declares the
sixteen terminal writer contracts, and
`clients/python/tests/unit/test_lifecycle_matrix.py` checks the declaration
against the baseline SQL functions.
