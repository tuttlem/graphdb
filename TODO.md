# TODO

* [x] Convert output logs to be JSON (structured logging everywhere)
* [x] Add performance (latency) data to logging (local telemetry only)
* [x] Improve HTTP request logging with trace IDs & correlation across client/daemon
* [x] Further CYPHER coverage (MATCH/WHERE/WITH/RETURN chaining, aggregates)
* [x] Remove SELECT from the parser (there is no SELECT in Cypher)
* [x] Path algorithm: Dijkstra shortest path
* [x] Path algorithm: A* heuristic shortest path
* [x] Path algorithm: Yen's k-shortest simple paths
* [x] Path algorithm: Delta-stepping single-source shortest path
* [ ] Path algorithm: Single-source shortest path (unweighted)
* [x] Path algorithm: All-pairs shortest path
* [x] Path algorithm: Breadth-first search
* [x] Path algorithm: Depth-first search
* [x] Path algorithm: Random walk
* [x] Path algorithm: Minimum weight spanning tree
* [ ] Implement query planner + execution engine beyond naive scans
* [ ] Introduce transactions + ACID semantics for storage backends
* [ ] Dense filesystem backend + pluggable key-value storage adapters
* [ ] Cluster mode (Raft/gossip) for multi-node deployments
* [ ] Full authentication/authorization layer (users/roles + tokens)
* [ ] TLS + mTLS configuration for daemon + built-in cert reloads
* [ ] Client web UI improvements (history sidebar, path visualisation, streaming results)
* [ ] Client CLI (cURL replacement) with script runner + REPL
* [ ] Client SDK Rust (sync + async)
* [ ] Client SDK TypeScript + generated JavaScript build
* [ ] Client SDK Python (popular QA automation)
* [ ] Expose metrics endpoint (Prometheus) for queries / storage / catalog epochs
* [ ] Provide admin API for catalog inspection & mutation (e.g. grant revoke)
* [ ] Config hot-reload + SIGHUP handling
* [ ] Structured error codes for API responses
* [ ] Binary protocol for high-throughput ingestion
* [ ] Query cache and prepared statements
* [ ] Built-in graph algorithms (PageRank, connected components, BFS service)
* [ ] Tracing integration (OpenTelemetry) from web client to daemon
* [ ] Benchmark suite and stress harness
* [ ] Live schema inspector in client UI
* [ ] CLI to dump/load catalog snapshots
* [ ] Multi-tenant catalog separation & namespaces
* [ ] Temporal queries (valid-time / transaction-time)
* [ ] Schema validation / migration tooling
* [ ] Streaming changefeed / CDC API
* [ ] Notebook-style docs/examples for common Cypher patterns
* [x] Predicate functions
* [x] Scalar functions
* [x] Aggregation functions
* [x] List functions
* [x] Math functions (numeric)
* [x] Math functions (logarithmic)
* [x] Math functions (trig)
* [ ] Spatial functions
* [ ] Data load
* [ ] Data unload
