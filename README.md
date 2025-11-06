# GraphDB Workspace

GraphDB is an experimental graph database implemented in Rust. The repository is
organised as a Cargo workspace made up of three primary crates:

- `common` – shared data structures such as nodes, edges, and attribute types.
- `graphdb-core` – the storage abstraction layer, query parser, and in-memory /
  file-backed storage backends.
- `daemon` – a long-running background service that exposes the database over a
  simple HTTP API.

The workspace currently focuses on the daemon. It performs proper UNIX
daemonisation (double fork, `setsid`, stdio redirection, PID file handling) and
builds on top of [Axum](https://github.com/tokio-rs/axum) for the HTTP surface.

## Quick Start

```sh
# Run all checks
cargo test --workspace

# Launch the daemon with the sample configuration
env GRAPHDB_DAEMON_CONFIG=daemon/config/dev.toml \
  cargo run -p daemon
```

By default the daemon:

- Changes to `/` after daemonising.
- Writes its PID file and log output beneath the workspace `target/` directory.
- Uses the simple file-backed storage backend with data persisted under
  `target/data/`.
- Listens for HTTP requests on `127.0.0.1:8080`.

You can send queries with `curl`:

```sh
# Insert a node
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"query":"CREATE (:Person { name: \"Ada\", city: \"Zurich\" })"}' \
  http://127.0.0.1:8080/query | jq

# Select nodes
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT MATCH (p:Person) RETURN p"}' \
  http://127.0.0.1:8080/query | jq
```

Responses follow the structure:

```json
{
  "status": "ok",
  "messages": ["created node …"],
  "selected_nodes": [ { … } ]
}
```

Errors return HTTP 4xx/5xx responses with a JSON body of `{ "status": "error",
"error": "…" }`.

## Configuration

The daemon reads configuration from three sources (highest precedence first):

1. A CLI argument (`graphdb <config-path>`).
2. The `GRAPHDB_DAEMON_CONFIG` environment variable.
3. Built-in defaults.

All paths in the configuration are normalised relative to the config file so the
process can safely `chdir("/")` during daemonisation.

```toml
working_directory = "/path/to/workdir"
pid_file = "./graphdb.pid"
stdout = "./graphdb.log"
stderr = "./graphdb.err"
umask = 0o022
log_level = "info"

[storage]
backend = "simple"            # or "memory"
directory = "./data"          # required for "simple"

[server]
bind_address = "127.0.0.1"
port = 8080
http2_only = false
tcp_nodelay = true
worker_threads = 4             # optional
concurrency_limit = 128        # optional
body_limit = 1048576           # optional bytes

# Uncomment to enable TLS termination
#[server.tls]
#cert_path = "./cert.pem"
#key_path = "./key.pem"
```

### Storage Backends

- `memory` – purely in-memory backend, no persistence.
- `simple` – JSON files on disk (`nodes/*.json`, `edges/*.json`). On startup the
  daemon hydrates the cache by loading existing files, so inserts survive across
  restarts provided the data directory is preserved.

### HTTP API

The daemon currently accepts script payloads that consist of the supported
Cypher-inspired statements:

- `CREATE (:Label { ... })` and `CREATE (a:Label { ... })-[:TYPE { ... }]->(:Other { ... })`
- `INSERT NODE …`, `INSERT EDGE …`
- `DELETE NODE id`, `DELETE EDGE id`
- `UPDATE NODE/EDGE id SET …`
- `SELECT MATCH (…) [WHERE …] RETURN …`

Multiple statements can be separated with semicolons. New features should extend
`graphdb-core`'s parser (`core/src/query/parser.rs`).

### Graceful Shutdown

`SIGINT`, `SIGTERM`, and `SIGQUIT` trigger graceful shutdown:

- The PID file is removed.
- Axum receives a shutdown signal and finishes in-flight requests.
- The daemon exits cleanly after the HTTP server drains.

`SIGHUP` is currently ignored.

## Development Notes

- Formatting: `cargo fmt`
- Linting: `cargo check --workspace`
- Tests: `cargo test --workspace`
- The daemon logs the runtime PID and PID file location at startup. Check the
  configured `stdout` path for structured logs.

When working on the HTTP surface, consider adding middleware (rate limiting,
tracing, auth) via Axum layers. The configuration module is the central place to
introduce new tunables—validate and normalise paths there so the daemon remains
robust after the daemonisation step.
