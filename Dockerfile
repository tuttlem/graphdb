# Build the React client
FROM node:20-alpine AS client-builder
WORKDIR /app/client
COPY client/ ./
RUN npm install && npm run build

# Build the daemon binary
FROM rust:1.81 AS rust-builder
RUN rustup toolchain install nightly && rustup default nightly
WORKDIR /app
COPY Cargo.lock Cargo.toml ./
COPY common ./common
COPY core ./core
COPY daemon ./daemon
RUN cargo build --release -p daemon

# Final runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=rust-builder /app/target/release/graphdb /usr/local/bin/graphdb
COPY --from=client-builder /app/client/dist /app/client/dist
COPY docker/config.toml /etc/graphdb/config.toml
ENV GRAPHDB_DAEMON_CONFIG=/etc/graphdb/config.toml
ENV GRAPHDB_FOREGROUND=1
VOLUME ["/data"]
EXPOSE 8080
CMD ["graphdb"]
