mod config;
mod daemon;
mod database;
mod error;
mod executor;
mod logging;
mod plugins;
mod server;
mod signals;

use crate::config::DaemonConfig;
use crate::daemon::daemonize;
use crate::database::shared_database;
use crate::error::Result;
use crate::logging::init_logging;
use crate::signals::SignalManager;
use std::thread;
use tokio::runtime::Builder;

fn main() -> Result<()> {
    let cli_config = std::env::args().nth(1);
    let config = DaemonConfig::from_sources(cli_config.as_deref())?;

    let context = daemonize(&config)?;
    init_logging(&config)?;

    let pid_file = context.pid_file_path_owned();
    if let Some(path) = pid_file.as_ref() {
        tracing::info!(
            event = "daemon.pid_file",
            path = %path.display(),
            "writing pid file"
        );
    }

    let (signal_manager, shutdown) = SignalManager::install(pid_file.clone())?;
    let _signal_manager = signal_manager;

    let database = shared_database(&config)?;
    let plugin_guard = plugins::load_plugins(config.plugin_dir())?;
    tracing::info!(
        event = "plugins.initialised",
        count = plugin_guard.count(),
        "loaded custom function libraries"
    );

    tracing::info!(
        event = "daemon.started",
        pid = nix::unistd::getpid().as_raw(),
        "graphdb daemon initialised"
    );

    let worker_threads = config
        .server()
        .worker_threads()
        .or_else(|| thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(1);

    let runtime = Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .thread_name("graphdb-rt")
        .build()?;

    if let Err(err) = runtime.block_on(server::run(&config, database, shutdown)) {
        fatal!(event = "daemon.runtime_exit", error = %err, "server runtime terminated");
        return Err(err);
    }

    tracing::info!(event = "daemon.stopped", "runtime exited cleanly");
    Ok(())
}
