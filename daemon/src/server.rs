use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use serde::Deserialize;
use serde::Serialize;

use crate::config::DaemonConfig;
use crate::database::DatabaseHandle;
use crate::error::{DaemonError, Result};

pub fn run(config: &DaemonConfig, database: DatabaseHandle) -> Result<()> {
    let addr = config.socket_addr()?;
    let listener = TcpListener::bind(addr)?;
    log::info!("listening on {addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&database);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, db) {
                        log::warn!("client handler error: {err}");
                    }
                });
            }
            Err(err) => {
                log::error!("listener accept error: {err}");
            }
        }
    }

    Ok(())
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

struct HttpError {
    status: &'static str,
    message: String,
}

fn handle_client(mut stream: TcpStream, database: DatabaseHandle) -> Result<()> {
    let peer = stream.peer_addr().ok();

    let request = match read_request(&mut stream) {
        Ok(request) => request,
        Err(err) => {
            respond_with_error(&mut stream, err.status, &err.message)?;
            return Ok(());
        }
    };

    match database.execute_script(&request.query) {
        Ok(report) => {
            let response = SuccessResponse {
                status: "ok",
                messages: report.messages,
                selected_nodes: report.selected_nodes,
            };
            write_response(&mut stream, "HTTP/1.1 200 OK\r\n", &response)?;
        }
        Err(err) => {
            let (status, message) = map_daemon_error(&err);
            respond_with_error(&mut stream, status, &message)?;
        }
    }

    if let Some(addr) = peer {
        log::debug!("finished request from {addr}");
    }

    Ok(())
}

fn read_request(stream: &mut TcpStream) -> std::result::Result<QueryRequest, HttpError> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    match reader.read_line(&mut request_line) {
        Ok(0) => {
            return Err(HttpError {
                status: "HTTP/1.1 400 Bad Request\r\n",
                message: "empty request".into(),
            });
        }
        Ok(_) => {}
        Err(err) => {
            return Err(HttpError {
                status: "HTTP/1.1 500 Internal Server Error\r\n",
                message: format!("failed to read request line: {err}"),
            });
        }
    }

    let (method, path, _version) = parse_request_line(&request_line).map_err(|err| HttpError {
        status: "HTTP/1.1 400 Bad Request\r\n",
        message: err.to_string(),
    })?;

    if method != "POST" {
        return Err(HttpError {
            status: "HTTP/1.1 405 Method Not Allowed\r\n",
            message: "method not allowed".into(),
        });
    }

    if path != "/query" && path != "/v1/query" {
        return Err(HttpError {
            status: "HTTP/1.1 404 Not Found\r\n",
            message: "not found".into(),
        });
    }

    let content_length = read_content_length(&mut reader)?;

    let mut body = vec![0_u8; content_length];
    if let Err(err) = reader.read_exact(&mut body) {
        return Err(HttpError {
            status: "HTTP/1.1 400 Bad Request\r\n",
            message: format!("failed to read request body: {err}"),
        });
    }

    let payload = match String::from_utf8(body) {
        Ok(data) => data,
        Err(_) => {
            return Err(HttpError {
                status: "HTTP/1.1 400 Bad Request\r\n",
                message: "request body must be valid UTF-8".into(),
            });
        }
    };

    serde_json::from_str(&payload).map_err(|err| HttpError {
        status: "HTTP/1.1 400 Bad Request\r\n",
        message: format!("invalid JSON payload: {err}"),
    })
}

fn read_content_length(
    reader: &mut BufReader<&mut TcpStream>,
) -> std::result::Result<usize, HttpError> {
    let mut content_length = None;
    loop {
        let mut header_line = String::new();
        if reader
            .read_line(&mut header_line)
            .map_err(|err| HttpError {
                status: "HTTP/1.1 500 Internal Server Error\r\n",
                message: format!("failed to read headers: {err}"),
            })?
            == 0
        {
            return Err(HttpError {
                status: "HTTP/1.1 400 Bad Request\r\n",
                message: "unexpected end of headers".into(),
            });
        }
        let trimmed = header_line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some(value) = header_value(trimmed, "content-length") {
            match value.trim().parse::<usize>() {
                Ok(len) => content_length = Some(len),
                Err(_) => {
                    return Err(HttpError {
                        status: "HTTP/1.1 400 Bad Request\r\n",
                        message: "invalid Content-Length header".into(),
                    });
                }
            }
        }
    }

    content_length.ok_or_else(|| HttpError {
        status: "HTTP/1.1 411 Length Required\r\n",
        message: "missing Content-Length header".into(),
    })
}

fn parse_request_line(line: &str) -> Result<(&str, &str, &str)> {
    let mut parts = line.trim_end().split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| DaemonError::Query("missing method".into()))?;
    let path = parts
        .next()
        .ok_or_else(|| DaemonError::Query("missing path".into()))?;
    let version = parts
        .next()
        .ok_or_else(|| DaemonError::Query("missing protocol version".into()))?;
    Ok((method, path, version))
}

fn header_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let mut splits = line.splitn(2, ':');
    let header_key = splits.next()?.trim();
    if header_key.eq_ignore_ascii_case(key) {
        splits.next().map(|v| v.trim())
    } else {
        None
    }
}

fn write_response<T: Serialize>(stream: &mut TcpStream, status: &str, body: &T) -> Result<()> {
    let payload = serde_json::to_vec(body)?;
    let headers = format!(
        "{status}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    stream.write_all(headers.as_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

pub fn respond_with_error(stream: &mut TcpStream, status_line: &str, message: &str) -> Result<()> {
    let response = ErrorResponse {
        status: "error",
        error: message.to_string(),
    };
    write_response(stream, status_line, &response)
}

fn map_daemon_error(err: &DaemonError) -> (&'static str, String) {
    match err {
        DaemonError::Query(_) | DaemonError::QueryParse(_) => {
            ("HTTP/1.1 400 Bad Request\r\n", err.to_string())
        }
        DaemonError::Database(_) | DaemonError::Storage(_) => {
            ("HTTP/1.1 500 Internal Server Error\r\n", err.to_string())
        }
        DaemonError::Io(_)
        | DaemonError::Nix(_)
        | DaemonError::Logger(_)
        | DaemonError::Config(_)
        | DaemonError::Toml(_)
        | DaemonError::Json(_) => ("HTTP/1.1 500 Internal Server Error\r\n", err.to_string()),
    }
}
