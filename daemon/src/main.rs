mod config;
mod daemon;
mod database;
mod error;
mod executor;
mod logging;
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
        log::info!("writing pid file to {}", path.display());
    }

    let (signal_manager, shutdown) = SignalManager::install(pid_file.clone())?;
    let _signal_manager = signal_manager;

    let database = shared_database(&config)?;

    log::info!("graphdb daemon running with pid {}", nix::unistd::getpid());

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

    runtime.block_on(server::run(&config, database, shutdown))?;
    Ok(())
}
