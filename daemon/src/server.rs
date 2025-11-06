use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures_util::StreamExt;
use hyper::server::accept::from_stream;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::TcpListenerStream;
use tower::limit::ConcurrencyLimitLayer;

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
        .with_state(state);

    if let Some(limit) = config.server().body_limit {
        app = app.layer(DefaultBodyLimit::max(limit));
    }

    if let Some(limit) = config.server().concurrency_limit {
        app = app.layer(ConcurrencyLimitLayer::new(limit));
    }

    log::info!("listening on {addr}");

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
    let report = state
        .database
        .execute_script(&request.query)
        .map_err(ApiError::from)?;

    Ok(Json(SuccessResponse {
        status: "ok",
        messages: report.messages,
        selected_nodes: report.selected_nodes,
        procedures: report.procedures,
    }))
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
                                log::warn!("failed to set tcp_nodelay: {err}");
                            }
                        }
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => Some(Ok::<_, std::io::Error>(tls_stream)),
                            Err(err) => {
                                log::error!("TLS handshake error: {err}");
                                None
                            }
                        }
                    }
                    Err(err) => {
                        log::error!("TLS listener accept error: {err}");
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
