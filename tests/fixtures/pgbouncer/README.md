# PgBouncer Test Stack

This stack is only for Horsies contract tests. It intentionally uses a static
`userlist.txt` with plain local test credentials; managed providers may use a
different authentication path. Authentication is not part of the contract under
test here.

Images are pinned by digest and forced to `linux/amd64` so CI cannot drift when
upstream tags move. The fixture intentionally targets Postgres 18 and PgBouncer
v1.24.1-p1, while keeping provider-specific details out of the contract.
Host ports are intentionally high (`15432`, `16432`, `16433`, `16434`, `16435`)
to avoid colliding with developer machines that already run Postgres or
PgBouncer. The `16435` transaction-pool service has `default_pool_size = 1` and
exists only for the prepared-statement collision control test.

```bash
docker compose -f tests/fixtures/pgbouncer/compose.yaml up -d --wait

HORSIES_PGBOUNCER_TEST=1 \
DB_PASSWORD=testpassword \
uv run pytest tests/integration/test_pgbouncer_contract.py \
  tests/integration/test_pgbouncer_rolling_upgrade.py \
  tests/integration/test_pgbouncer_prepared_statements.py -q

HORSIES_PGBOUNCER_TEST=1 \
DB_PASSWORD=testpassword \
uv run pytest tests/e2e/test_pgbouncer_smoke.py -q

docker compose -f tests/fixtures/pgbouncer/compose.yaml down -v
```
