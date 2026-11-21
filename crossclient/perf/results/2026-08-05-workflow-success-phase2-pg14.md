# workflow-success-phase2

one-node workflow success through terminal persistence, phase 2, workflow completion and queue wake

## Conditions

| | |
|---|---|
| measured | 2026-08-05T00:37:13+00:00 |
| server | PostgreSQL 14.23 (Debian 14.23-1.pgdg13+1) |
| compared | existing statement against database-owned operation |
| observations per side | 10000 |
| block size | 100 |
| pre-existing terminal rows | 100000 |
| result payload | 44 bytes |
| batch | not applicable |
| fsync | off |
| full_page_writes | off |
| synchronous_commit | off |
| autovacuum | off |
| bootstrap resamples | 1000 |
| bootstrap seed | 20260804 |
| demo units quiesced | yes |

## Latency

| percentile | baseline | candidate | delta | 95% interval | budget | verdict |
|---|---|---|---|---|---|---|
| p50 | 10.121 ms | 10.307 ms | +0.186 ms | +0.173 to +0.199 ms | 1.012 ms | PASS |
| p99 | 12.807 ms | 12.857 ms | +0.050 ms | -0.240 to +0.421 ms | 2.000 ms | PASS |

## Instrumented plan evidence

One eligible transition per side, executed with `EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)` and rolled back. These executions are excluded from the latency and server-count measurements.

| | baseline | candidate |
|---|---|---|
| node shape | `ModifyTable(horsies_tasks) > Index Scan(horsies_tasks)` | `Function Scan(horsies_complete_locked_task)` |
| shared hit blocks | 26 | 29 |
| shared read blocks | 0 | 0 |
| shared dirtied blocks | 0 | 0 |
| shared written blocks | 0 | 0 |
| WAL records | 10 | 10 |
| WAL full-page images | 0 | 0 |
| WAL bytes | 952 | 968 |

## Server counts

| | baseline | candidate |
|---|---|---|
| client statements | 120000 | 120000 |
| statements inside functions | 50000 | 70000 |
| write transactions | 20000 | 20000 |
| client rows | 110000 | 110000 |
| nested rows | 50000 | 70000 |
| terminal task rows | 10000 | 10000 |
| WAL records per terminal task row | 37.45 | 37.47 |
| WAL bytes per terminal task row | 3352 | 3378 |
| full-page images | 0 | 0 |

## Contract checks

Limits: client statements may not increase; write transactions and terminal task rows must match; WAL-record delta must be at most 0.100 per terminal task row; WAL-byte delta must be at most the greater of 10% or 128 bytes per terminal task row.

- PASS

Full-page writes are disabled in this disposable measurement environment, so checkpoint-dependent image bytes are absent from the WAL-byte comparison.

**Verdict: PASS**
