# Acme Rust deployment inputs

This directory contains deployment inputs. It does not deploy the showcase.
The deployment owner selects the host, database name, proxy route, service
cadence, and rollback steps.

## Release build for x86_64 Linux

Build from the repository root. The showcase is a workspace member with
`publish = false`. Build it from the repository. Do not install it from
crates.io.

```sh
rustup target add x86_64-unknown-linux-gnu
cargo build --release \
  --target x86_64-unknown-linux-gnu \
  -p acme-showcase --features web --bin acme
install -D -m 0755 \
  target/x86_64-unknown-linux-gnu/release/acme \
  /opt/acme-rust/bin/acme
```

The build requires a Rust toolchain and network access for the first
dependency download. The target must be `x86_64-unknown-linux-gnu` on the
Ubuntu x86_64 host. The `web` feature embeds the monitoring SPA.

## Environment contract

Do not rely on `DATABASE_URL` in a service. Set the native SQLx URL explicitly.
The accepted URL forms are `postgresql://`, `postgres://`, and
`postgresql+psycopg://`. The last form is normalized for Python `.env`
compatibility.

Create the environment files with the deployment owner's permissions. Do not
put credentials in unit files committed to the repository.

| Unit | Environment file | Required variables |
| --- | --- | --- |
| `acme-rust-worker.service` | `/etc/acme-rust/worker.env` | `ACME_DATABASE_URL`, `RUST_LOG` |
| `acme-rust-worker-2.service` | `/etc/acme-rust/worker-2.env` | `ACME_DATABASE_URL`, `RUST_LOG` |
| `acme-rust-scheduler.service` | `/etc/acme-rust/scheduler.env` | `ACME_DATABASE_URL`, `RUST_LOG` |
| `acme-rust-web.service` | `/etc/acme-rust/web.env` | `ACME_DATABASE_URL`, `RUST_LOG` |
| `acme-rust-steady.service` | `/etc/acme-rust/steady.env` | `ACME_DATABASE_URL`, `ACME_WEB_URL`, `RUST_LOG` |

Use this shape for each file. Replace the database host, credentials, and
database name with the deployment values.

```dotenv
ACME_DATABASE_URL=postgresql://acme:REPLACE_ME@127.0.0.1:5432/acme_demo_rust
RUST_LOG=info
```

The steady unit also needs the public or proxy URL used in its task links:

```dotenv
ACME_DATABASE_URL=postgresql://acme:REPLACE_ME@127.0.0.1:5432/acme_demo_rust
ACME_WEB_URL=https://acme-rust.example.invalid
RUST_LOG=info
```

The database is separate from the Python showcase database. The database
must already be reachable by the service account. Run `acme seed` once before
starting the long-lived units.

## systemd units

Install the binary at `/opt/acme-rust/bin/acme`. Set `User=acme-rust` and
`WorkingDirectory=/opt/acme-rust` to the deployment paths. The units below
use no migration command. The worker and scheduler use the public demo
commands. The web unit binds loopback and uses trusted-header authentication
for a reverse proxy.

### `acme-rust-worker.service`

```ini
[Unit]
Description=Acme Rust task worker
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=acme-rust
WorkingDirectory=/opt/acme-rust
EnvironmentFile=/etc/acme-rust/worker.env
ExecStart=/opt/acme-rust/bin/acme worker
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### `acme-rust-worker-2.service`

Use a second process only when the host has capacity for it.

```ini
[Unit]
Description=Acme Rust task worker (second process)
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=acme-rust
WorkingDirectory=/opt/acme-rust
EnvironmentFile=/etc/acme-rust/worker-2.env
ExecStart=/opt/acme-rust/bin/acme worker
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### `acme-rust-scheduler.service`

```ini
[Unit]
Description=Acme Rust scheduler
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=acme-rust
WorkingDirectory=/opt/acme-rust
EnvironmentFile=/etc/acme-rust/scheduler.env
ExecStart=/opt/acme-rust/bin/acme scheduler
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### `acme-rust-web.service`

`--auth none` is valid only for loopback. A proxy must strip and set the
trusted header before forwarding a request. The web process is read-only by
default. Enable actions only when the proxy and operator policy require it.

```ini
[Unit]
Description=Acme Rust monitoring web server
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=acme-rust
WorkingDirectory=/opt/acme-rust
EnvironmentFile=/etc/acme-rust/web.env
ExecStart=/opt/acme-rust/bin/acme web --host 127.0.0.1 --port 8600 --auth trusted-header --trusted-header X-Forwarded-User
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

The reverse proxy must use the web unit's loopback listener. Do not expose
port 8600 directly to the network. The proxy must replace, not forward, the
`X-Forwarded-User` request header.

### `acme-rust-steady.service`

This unit is optional. It runs the open-ended steady scenario. Stop it before
changing the database or running a bounded scenario.

```ini
[Unit]
Description=Acme Rust steady order stream
Wants=network-online.target
After=network-online.target acme-rust-worker.service acme-rust-scheduler.service

[Service]
Type=simple
User=acme-rust
WorkingDirectory=/opt/acme-rust
EnvironmentFile=/etc/acme-rust/steady.env
ExecStart=/opt/acme-rust/bin/acme steady
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

The deployment owner controls the cadence by the showcase tuning constants
and service lifecycle. The steady unit does not create a database or run
migrations.
