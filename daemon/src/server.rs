use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower::limit::ConcurrencyLimitLayer;

use crate::config::DaemonConfig;
use crate::database::DatabaseHandle;
use crate::error::{DaemonError, Result};

pub async fn run(config: &DaemonConfig, database: DatabaseHandle) -> Result<()> {
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

    let mut server = axum::Server::bind(&addr).tcp_nodelay(config.server().tcp_nodelay);
    if config.server().http2_only {
        server = server.http2_only(true);
    }

    server.serve(app.into_make_service()).await?;

    Ok(())
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
