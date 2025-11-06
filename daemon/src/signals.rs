use std::path::PathBuf;
use std::thread::{self, JoinHandle};

use signal_hook::consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM};
use signal_hook::iterator::Signals;
use tokio::sync::oneshot;

use crate::error::Result;

pub struct SignalManager {
    _handle: JoinHandle<()>,
}

#[derive(Debug)]
pub struct ShutdownSignal {
    receiver: oneshot::Receiver<()>,
}

impl ShutdownSignal {
    pub async fn wait(self) {
        let _ = self.receiver.await;
    }
}

impl SignalManager {
    pub fn install(pid_file: Option<PathBuf>) -> Result<(Self, ShutdownSignal)> {
        let signals = Signals::new([SIGTERM, SIGINT, SIGQUIT, SIGHUP])?;
        let (tx, rx) = oneshot::channel();

        let handle = thread::spawn(move || {
            let mut signals = signals;
            let mut tx = Some(tx);
            for sig in signals.forever() {
                match sig {
                    SIGTERM | SIGINT | SIGQUIT => {
                        log::info!("received signal {sig}; initiating shutdown");
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
                        if let Some(sender) = tx.take() {
                            let _ = sender.send(());
                        }
                        break;
                    }
                    SIGHUP => {
                        log::info!("received SIGHUP; ignoring");
                    }
                    _ => {}
                }
            }
        });

        Ok((Self { _handle: handle }, ShutdownSignal { receiver: rx }))
    }
}
