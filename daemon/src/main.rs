mod config;
mod daemon;
mod error;
mod logging;
mod signals;

use std::time::Duration;

use crate::config::DaemonConfig;
use crate::daemon::daemonize;
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

    log::info!("graphdb daemon running with pid {}", nix::unistd::getpid());

    run();
}

fn run() -> ! {
    loop {
        log::debug!("daemon heartbeat");
        std::thread::sleep(Duration::from_secs(5));
    }
}
