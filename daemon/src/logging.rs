use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::DaemonConfig;
use crate::error::{DaemonError, Result};

pub fn init_logging(config: &DaemonConfig) -> Result<()> {
    let env_filter = build_env_filter(config)?;
    let writer = make_writer(config)?;

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_file(true)
                .with_line_number(true)
                .with_target(true)
                .with_level(true)
                .with_timer(fmt::time::UtcTime::rfc_3339())
                .with_writer(writer),
        )
        .try_init()
        .map_err(|err| DaemonError::Logger(err.to_string()))?;

    Ok(())
}

fn make_writer(config: &DaemonConfig) -> Result<BoxMakeWriter> {
    if std::env::var_os("GRAPHDB_FOREGROUND").is_some() {
        return Ok(BoxMakeWriter::new(|| Box::new(io::stdout())));
    }

    if let Some(path) = config.stdout() {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let shared = SharedFileWriter::new(file);
        return Ok(BoxMakeWriter::new(move || Box::new(shared.clone())));
    }

    Ok(BoxMakeWriter::new(|| Box::new(io::stdout())))
}

#[derive(Clone)]
struct SharedFileWriter {
    inner: Arc<Mutex<File>>,
}

impl SharedFileWriter {
    fn new(file: File) -> Self {
        Self {
            inner: Arc::new(Mutex::new(file)),
        }
    }
}

impl Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "log file lock poisoned"))?;
        guard.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "log file lock poisoned"))?;
        guard.flush()
    }
}

fn build_env_filter(config: &DaemonConfig) -> Result<EnvFilter> {
    let directive = config.log_level.as_deref().unwrap_or("info");
    EnvFilter::try_new(directive)
        .map_err(|err| DaemonError::Logger(format!("invalid log level '{directive}': {err}")))
}

#[macro_export]
macro_rules! fatal {
    (target: $target:expr, $($arg:tt)+) => {
        tracing::event!(target: $target, tracing::Level::ERROR, severity = "FATAL", $($arg)+);
    };
    ($($arg:tt)+) => {
        tracing::event!(tracing::Level::ERROR, severity = "FATAL", $($arg)+);
    };
}
