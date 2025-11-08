use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures_util::StreamExt;
use hyper::server::accept::from_stream;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::TcpListenerStream;
use tower::ServiceExt;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
};
use uuid::Uuid;

use crate::config::{DaemonConfig, TlsSettings};
use crate::database::DatabaseHandle;
use crate::error::{DaemonError, Result};
use crate::signals::ShutdownSignal;

pub async fn run(
    config: &DaemonConfig,
    database: DatabaseHandle,
    shutdown: ShutdownSignal,
) -> Result<()> {
    let addr = config.socket_addr()?;
    let state = AppState { database };

    let mut app = Router::new()
        .route("/query", post(handle_query))
        .route("/v1/query", post(handle_query))
        .with_state(state)
        .layer(CorsLayer::permissive());

    if let Some(limit) = config.server().body_limit {
        app = app.layer(DefaultBodyLimit::max(limit));
    }

    if let Some(limit) = config.server().concurrency_limit {
        app = app.layer(ConcurrencyLimitLayer::new(limit));
    }

    app = app.layer(from_fn(log_requests));

    if let Some(client_dir) = config.server().client_dir() {
        let client_dir = client_dir.to_path_buf();
        let spa = ServeDir::new(client_dir.clone())
            .not_found_service(ServeFile::new(client_dir.join("index.html")));
        let static_router = Router::new().fallback({
            let spa = spa.clone();
            move |req: Request<Body>| {
                let service = spa.clone();
                async move {
                    service.oneshot(req).await.map_err(|error| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("static file error: {error}"),
                        )
                    })
                }
            }
        });
        app = app.merge(static_router);
    }

    tracing::info!(event = "server.listen", %addr, "listening for connections");

    if let Some(tls) = config.server().tls().cloned() {
        serve_with_tls(addr, app, config, tls, shutdown).await
    } else {
        serve_plain(addr, app, config, shutdown).await
    }
}

#[derive(Clone)]
struct AppState {
    database: DatabaseHandle,
}

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
}

#[derive(Serialize)]
struct SuccessResponse {
    status: &'static str,
    messages: Vec<String>,
    selected_nodes: Vec<graphdb_core::Node>,
    procedures: Vec<crate::executor::ProcedureResult>,
    paths: Vec<crate::executor::PathResult>,
    path_pairs: Vec<crate::executor::PathPairResult>,
}

#[derive(Serialize)]
struct ErrorResponse {
    status: &'static str,
    error: String,
}

async fn handle_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Json<SuccessResponse>, ApiError> {
    let preview: String = request.query.chars().take(200).collect();
    let query_len = request.query.len();
    let truncated = query_len > preview.len();
    let query_id = Uuid::new_v4();
    tracing::info!(
        event = "query.received",
        query_id = %query_id,
        query_length = query_len,
        truncated = truncated,
        preview = %preview,
        "received query"
    );

    let start = Instant::now();
    let result = state.database.execute_script(&request.query);

    match result {
        Ok(report) => {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            tracing::info!(
                event = "query.completed",
                query_id = %query_id,
                elapsed_ms,
                messages = report.messages.len(),
                selected_nodes = report.selected_nodes.len(),
                procedures = report.procedures.len(),
                paths = report.paths.len(),
                path_pairs = report.path_pairs.len(),
                "query executed"
            );
            Ok(Json(SuccessResponse {
                status: "ok",
                messages: report.messages,
                selected_nodes: report.selected_nodes,
                procedures: report.procedures,
                paths: report.paths,
                path_pairs: report.path_pairs,
            }))
        }
        Err(err) => {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            tracing::error!(
                event = "query.failed",
                query_id = %query_id,
                elapsed_ms,
                error = %err,
                "query execution failed"
            );
            Err(ApiError::from(err))
        }
    }
}

async fn log_requests<B>(req: Request<B>, next: Next<B>) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed();
    tracing::info!(
        event = "http.request",
        method = %method,
        path = %uri.path(),
        status = response.status().as_u16(),
        elapsed_ms = elapsed.as_millis() as u64,
        "request completed"
    );
    response
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<DaemonError> for ApiError {
    fn from(err: DaemonError) -> Self {
        let (status, message) = map_daemon_error(&err);
        ApiError::new(status, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let payload = ErrorResponse {
            status: "error",
            error: self.message,
        };
        (self.status, Json(payload)).into_response()
    }
}

fn map_daemon_error(err: &DaemonError) -> (StatusCode, String) {
    match err {
        DaemonError::Query(_) | DaemonError::QueryParse(_) => {
            (StatusCode::BAD_REQUEST, err.to_string())
        }
        DaemonError::Database(_)
        | DaemonError::Storage(_)
        | DaemonError::Io(_)
        | DaemonError::Nix(_)
        | DaemonError::Logger(_)
        | DaemonError::Config(_)
        | DaemonError::Toml(_)
        | DaemonError::Json(_)
        | DaemonError::Http(_) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn serve_plain(
    addr: SocketAddr,
    app: Router,
    config: &DaemonConfig,
    shutdown: ShutdownSignal,
) -> Result<()> {
    let mut server = axum::Server::bind(&addr).tcp_nodelay(config.server().tcp_nodelay);
    if config.server().http2_only {
        server = server.http2_only(true);
    }

    server
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move { shutdown.wait().await })
        .await?;

    Ok(())
}

async fn serve_with_tls(
    addr: SocketAddr,
    app: Router,
    config: &DaemonConfig,
    tls: TlsSettings,
    shutdown: ShutdownSignal,
) -> Result<()> {
    let tls_config = load_rustls_config(&tls, config.server().http2_only)?;
    let acceptor = TlsAcceptor::from(tls_config);
    let listener = TcpListener::bind(addr).await?;

    let tcp_nodelay = config.server().tcp_nodelay;
    let incoming = TcpListenerStream::new(listener)
        .then(move |conn| {
            let acceptor = acceptor.clone();
            async move {
                match conn {
                    Ok(stream) => {
                        if tcp_nodelay {
                            if let Err(err) = stream.set_nodelay(true) {
                                tracing::warn!(
                                    event = "server.tcp_nodelay_failed",
                                    error = %err,
                                    "failed to set tcp_nodelay"
                                );
                            }
                        }
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => Some(Ok::<_, std::io::Error>(tls_stream)),
                            Err(err) => {
                                tracing::error!(
                                    event = "server.tls_handshake_failed",
                                    error = %err,
                                    "TLS handshake error"
                                );
                                None
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!(
                            event = "server.accept_failed",
                            error = %err,
                            "TLS listener accept error"
                        );
                        None
                    }
                }
            }
        })
        .filter_map(|item| async move { item });

    let incoming = from_stream(incoming);

    let mut server = axum::Server::builder(incoming);
    if config.server().http2_only {
        server = server.http2_only(true);
    }

    server
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move { shutdown.wait().await })
        .await?;

    Ok(())
}

fn load_rustls_config(tls: &TlsSettings, http2_only: bool) -> Result<Arc<ServerConfig>> {
    let certs = {
        let cert_file = File::open(&tls.cert_path)?;
        let mut reader = BufReader::new(cert_file);
        let cert_chain = certs(&mut reader).map_err(|err| {
            DaemonError::Config(format!("failed to parse TLS certificate: {err}"))
        })?;
        if cert_chain.is_empty() {
            return Err(DaemonError::Config(
                "server.tls.cert_path contained no certificates".into(),
            ));
        }
        cert_chain.into_iter().map(Certificate).collect::<Vec<_>>()
    };

    let key = {
        let key_file = File::open(&tls.key_path)?;
        let mut reader = BufReader::new(key_file);
        let mut keys = pkcs8_private_keys(&mut reader).map_err(|err| {
            DaemonError::Config(format!("failed to parse TLS private key: {err}"))
        })?;

        if keys.is_empty() {
            let key_file = File::open(&tls.key_path)?;
            let mut reader = BufReader::new(key_file);
            keys = rsa_private_keys(&mut reader).map_err(|err| {
                DaemonError::Config(format!("failed to parse RSA private key: {err}"))
            })?;
        }

        let key = keys.into_iter().next().ok_or_else(|| {
            DaemonError::Config("server.tls.key_path contained no private keys".into())
        })?;

        PrivateKey(key)
    };

    let mut config = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| DaemonError::Config(format!("failed to build TLS configuration: {err}")))?;

    config.alpn_protocols = if http2_only {
        vec![b"h2".to_vec()]
    } else {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    };

    Ok(Arc::new(config))
}
