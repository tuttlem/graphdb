![Logo](img/logo.png)

## Overview


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

## System Catalog


GraphDB now maintains a native system catalog that mirrors the pattern used by
Postgres (`pg_catalog`) and SQL Server (`sys`). The catalog is bootstrapped
whenever a `Database` is constructed and is exposed through the
`SystemCatalog` handle (`Database::catalog()`):

- **Schemas and Roles** – built-in `system` and `public` roles plus default
  `system`/`public` schemas. Use `SystemCatalog::ensure_role` and
  `SystemCatalog::ensure_schema` to extend them.
- **Users and Grants** – register users via `SystemCatalog::register_user` and
  grant privileges with `SystemCatalog::grant_privilege`. Privileges are simple
  `SELECT | INSERT | UPDATE | DELETE` bit flags scoped to catalog objects.
- **Node/Edge Classes** – every node insert automatically creates or updates a
  `system.node_classes` entry for each label (or `__default__` when no label).
  Edge inserts derive an edge class using the source/target primary labels
  (falling back to `__link__`). The catalog records schema ownership,
  properties, and the physical storage handle.
- **Indexes** – `SystemCatalog::register_index` captures logical index metadata
  for node or edge classes so an index build component can subscribe to catalog
  change events.
- **Process Accounting** – `Database::register_process`, `heartbeat_process`,
  and `complete_process` push entries into `system.processes`. A
  `ProcessWatch` (`Database::process_watch(timeout)`) surfaces stale sessions
  for basic activity auditing.

When the daemon runs with the simple storage backend, the entire catalog is
persisted to `catalog.json` next to the `nodes/` and `edges/` directories. Every
metadata mutation (new labels, roles, users, grants, processes, etc.) rewrites
that snapshot so a cold restart fully restores system schemas and privileges
before graph data is hydrated.

## Web Client


The repository now includes a lightweight React client under `client/`. It can
run in dev mode (`npm run dev`) or be bundled (`npm run build`). Point the build
output (`client/dist`) at the daemon by setting `server.client_dir` in the
configuration and the HTTP API will also serve the SPA from the same binary.

To develop locally:

```sh
cd client
npm install
npm run dev                # proxies /query to http://127.0.0.1:8080
```

For production builds served by the daemon:

```sh
cd client
npm run build
# update daemon config:
# [server]
# client_dir = "../client/dist"
```

The UI lets you issue arbitrary statements, view returned nodes, inspect
procedure results (for catalog calls), and replay recent queries.

The catalog keeps a per-table epoch counter along with a global epoch. The
`CatalogCache` (`Database::catalog_cache()`) provides a read-through cache: when
an epoch advances, cached entries are invalidated and repopulated on demand.

### Calling Catalog Procedures


You can introspect the catalog over HTTP using Neo4j-style `CALL` statements.
The parser understands `CALL graphdb.<procedure>()` and returns the rows in the
`procedures` section of the JSON response. Available procedures today:

- `graphdb.nodeClasses()` – logical node classes (labels) that have been
  observed, including schema/owner metadata.
- `graphdb.edgeClasses()` – logical edge classes with endpoint metadata.
- `graphdb.roles()` – defined roles and their inheritance graph.
- `graphdb.users()` – registered users and their default/login roles.

Example request:

```sh
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"query":"CALL graphdb.nodeClasses();"}' \
  http://127.0.0.1:8080/query | jq
```

Snippet of the response payload:

```json
{
  "status": "ok",
  "messages": ["call graphdb.nodeClasses returned 1 row(s)"],
  "selected_nodes": [],
  "procedures": [
    {
      "name": "graphdb.nodeClasses",
      "rows": [
        {
          "id": "…",
          "schema_id": "…",
          "name": "Person",
          "owner_role": "…",
          "properties": ["name", "city"],
          "version": 1,
          "created_at_seconds": 1720000000
        }
      ]
    }
  ]
}
```

### Using the Privilege Model

Node and edge inserts go through the privilege pipeline automatically. The
default owner role is `system`, which short-circuits the check, but the helper
APIs make it easy to enforce custom authorisation:

```rust
use graphdb_core::{CatalogObject, Database, Privilege, RoleId};

fn grant_insert(db: &Database<impl graphdb_core::StorageBackend>, role: RoleId, class: &str) {
    let catalog = db.catalog();
    let schema = catalog.default_schema_id();
    let props: Vec<String> = Vec::new();
    let class_id = catalog
        .ensure_node_class(schema, class, catalog.system_role_id(), &props)
        .expect("node class");
    catalog
        .grant_privilege(role, CatalogObject::NodeClass(class_id), Privilege::INSERT, catalog.system_role_id())
        .expect("grant");
}
```

Any role without the required privilege triggers a `DatabaseError::Unauthorized`
when it attempts to write.

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
client_dir = "../client/dist"   # optional SPA bundle served by the daemon

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
