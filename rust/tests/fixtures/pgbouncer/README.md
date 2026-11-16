# PgBouncer Test Stack

This stack is only for Horsies contract tests. It intentionally uses a static
`userlist.txt` with plain local test credentials; managed providers may use a
different authentication path. Authentication is not part of the contract under
test here.

Images are pinned by digest and forced to `linux/amd64` so CI cannot drift when
upstream tags move. The fixture targets Postgres 18 and PgBouncer v1.24.1-p1,
while keeping provider-specific details out of the contract. Host ports are high
(`15432`, `16432`, `16433`, `16434`, `16435`) to avoid colliding with developer
machines that already run Postgres or PgBouncer. The primary transaction-pool
service uses `max_prepared_statements = 200`, matching managed providers that
support protocol-level prepared statements for SQLx clients. The `16435`
transaction-pool service intentionally sets `max_prepared_statements = 0` and
exists only for the unsupported-PgBouncer negative test.

```bash
docker compose -f tests/fixtures/pgbouncer/compose.yaml up -d --wait

HORSIES_PGBOUNCER_TEST=1 \
cargo test -p horsies-test-worker --test pgbouncer_contract -- --test-threads=1

docker compose -f tests/fixtures/pgbouncer/compose.yaml down -v
```

Keep this job in the full/manual workflow until it has run at least 10 times
with zero PgBouncer-specific flakes, then promote it to PR-required CI.
