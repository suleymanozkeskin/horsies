<p align="center">
  <img src="https://suleymanozkeskin.github.io/horsies/galloping-horsie.jpg" alt="Horsies Logo" width="200" style="border-radius: 20px" />
</p>

# Horsies

**PostgreSQL-backed background task queue and workflow engine for Python.**

[**Full Documentation**](https://suleymanozkeskin.github.io/horsies/) | [**PyPI**](https://pypi.org/project/horsies/) | [**GitHub**](https://github.com/suleymanozkeskin/horsies)

---

## Why Postgres

Every guarantee horsies makes is a Postgres primitive, not a
client-side convention:

- **Claiming**: one server-side function under `FOR UPDATE SKIP
  LOCKED`; a claim-generation fence rejects stale attempts, including
  a worker re-claiming its own requeued task.
- **Finalization**: row lock → immutable attempt-history append →
  state transition, one transaction.
- **Recovery**: all state is rows and timestamps; the reaper
  reconstructs and repairs after a worker dies mid-flight — nothing
  in-flight exists only in a broker's memory.
- **Workflows**: fan-in, completion, and subworkflow cascades are
  multi-row transitions under a documented lock order.
- **Dispatch**: LISTEN/NOTIFY push — no polling.
- **Inspection**: history is plain tables — SQL, `EXPLAIN`, your
  existing backups; Syce reads the same rows.

Measured hot-path latencies: [performance](https://suleymanozkeskin.github.io/horsies/internals/performance/).

## Monitoring

Horsies includes **Syce**, a terminal-based UI for monitoring your cluster in real-time.

![Syce Dashboard](https://suleymanozkeskin.github.io/horsies/images/syce/dashboard.png)

[**Syce Setup & Usage**](https://suleymanozkeskin.github.io/horsies/monitoring/syce-overview/)

## Testing & Correctness

![Coverage](https://raw.githubusercontent.com/suleymanozkeskin/horsies/main/.github/badges/coverage.svg)

3,200+ test functions across unit, real-Postgres integration, e2e
worker-process, and PgBouncer contract suites — a 2.2:1 test-to-source line
ratio, weighted toward failure paths: crash recovery, claim fencing,
cancel/completion races, rolling upgrades, and `EXPLAIN ANALYZE` plan
assertions pinning the hot-path statements to their indexes. The
claim/finalize semantics are additionally cross-validated against an
independent Rust reimplementation of the engine.

Pre-1.0 is an API-contract statement, not an engine-maturity statement:
breaking API changes may still land in minor releases and are documented in
the [changelog](CHANGELOG.md).
