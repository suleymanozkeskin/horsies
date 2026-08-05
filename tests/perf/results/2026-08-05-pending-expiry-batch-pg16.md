# pending-expiry-batch

deadline expiry of unclaimed tasks, one bounded batch

## Conditions

| | |
|---|---|
| measured | 2026-08-05T02:25:04+00:00 |
| server | PostgreSQL 16.14 (Debian 16.14-1.pgdg13+1) |
| compared | existing statement against database-owned operation |
| observations per side | 100 |
| block size | 10 |
| pre-existing terminal rows | 100000 |
| result payload | 200 bytes |
| batch | 500 rows |
| fsync | off |
| full_page_writes | off |
| synchronous_commit | off |
| autovacuum | off |
| bootstrap resamples | 1000 |
| bootstrap seed | 20260804 |
| demo units quiesced | yes |

## Batch performance

| metric | baseline | candidate | comparison | 95% interval | budget | verdict |
|---|---|---|---|---|---|---|
| throughput | 4166 rows/s | 4000 rows/s | 0.960x | 0.949x to 0.972x | >= 0.90x | PASS |
| p95 statement-to-commit duration | 127.271 ms | 133.733 ms | +6.461 ms | -1.367 to +11.802 ms | 12.727 ms | PASS |

The duration envelope starts before the statement can acquire its first row lock and ends after commit releases every lock. It includes result decoding and outcome logging performed before commit.

## Instrumented plan evidence

One eligible transition per side, executed with `EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)` and rolled back. These executions are excluded from the latency and server-count measurements.

| | baseline | candidate |
|---|---|---|
| node shape | `ModifyTable(horsies_tasks) > Nested Loop > Subquery Scan > Limit > LockRows > Index Scan(horsies_tasks) > Index Scan(horsies_tasks)` | `Function Scan(horsies_expire_pending_tasks)` |
| shared hit blocks | 15198 | 16034 |
| shared read blocks | 2 | 0 |
| shared dirtied blocks | 34 | 32 |
| shared written blocks | 32 | 32 |
| WAL records | 6070 | 6087 |
| WAL full-page images | 0 | 0 |
| WAL bytes | 617572 | 625970 |

## Server counts

| | baseline | candidate |
|---|---|---|
| client statements | 100 | 100 |
| statements inside functions | 100000 | 100100 |
| write transactions | 100 | 100 |
| client rows | 50000 | 50000 |
| nested rows | 100000 | 150000 |
| terminal task rows | 50000 | 50000 |
| WAL records per terminal task row | 12.23 | 12.17 |
| WAL bytes per terminal task row | 1242 | 1253 |
| full-page images | 0 | 0 |

## Contract checks

Limits: client statements may not increase; write transactions and terminal task rows must match; WAL-record delta must be at most 0.100 per terminal task row; WAL-byte delta must be at most the greater of 10% or 128 bytes per terminal task row.

- PASS

Full-page writes are disabled in this disposable measurement environment, so checkpoint-dependent image bytes are absent from the WAL-byte comparison.

**Verdict: PASS**
