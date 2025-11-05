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

fn main() -> Result<()> {
    let cli_config = std::env::args().nth(1);
    let config = DaemonConfig::from_sources(cli_config.as_deref())?;

    let context = daemonize(&config)?;
    init_logging(&config)?;

    let pid_file = context.pid_file_path_owned();
    let _signal_manager = SignalManager::install(pid_file)?;

    let database = shared_database(&config)?;

    log::info!("graphdb daemon running with pid {}", nix::unistd::getpid());

    server::run(&config, database)?;
    Ok(())
}
