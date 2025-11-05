use std::path::PathBuf;
use std::thread::{self, JoinHandle};

use signal_hook::consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM};
use signal_hook::iterator::Signals;

use crate::error::Result;

pub struct SignalManager {
    _handle: JoinHandle<()>,
}

impl SignalManager {
    pub fn install(pid_file: Option<PathBuf>) -> Result<Self> {
        let signals = Signals::new([SIGTERM, SIGINT, SIGQUIT, SIGHUP])?;

        let handle = thread::spawn(move || {
            let mut signals = signals;
            for sig in signals.forever() {
                match sig {
                    SIGTERM | SIGINT | SIGQUIT => {
                        log::info!("received signal {sig}; shutting down");
                        if let Some(path) = pid_file.as_ref() {
                            if let Err(err) = std::fs::remove_file(path) {
                                if err.kind() != std::io::ErrorKind::NotFound {
                                    log::warn!(
                                        "failed to remove pid file {}: {}",
                                        path.display(),
                                        err
                                    );
                                }
                            }
                        }
                        std::process::exit(0);
                    }
                    SIGHUP => {
                        log::info!("received SIGHUP; ignoring");
                    }
                    _ => {}
                }
            }
        });

        Ok(Self { _handle: handle })
    }
}
