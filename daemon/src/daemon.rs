use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use nix::libc::{STDERR_FILENO, STDIN_FILENO, STDOUT_FILENO};
use nix::sys::signal::{SigHandler, Signal, signal};
use nix::sys::stat::{Mode, umask};
use nix::unistd::{ForkResult, close, dup2, fork, getpid, setsid};

use crate::config::DaemonConfig;
use crate::error::Result;

pub struct DaemonContext {
    pid_file: Option<PidFileGuard>,
}

impl DaemonContext {
    pub fn pid_file_path_owned(&self) -> Option<PathBuf> {
        self.pid_file
            .as_ref()
            .map(|guard| guard.path().to_path_buf())
    }
}

struct PidFileGuard {
    path: PathBuf,
}

impl PidFileGuard {
    fn acquire(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o644)
            .open(&path)?;
        writeln!(file, "{}", getpid())?;

        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup_silently(&self) {
        if let Err(err) = fs::remove_file(&self.path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                log::warn!("failed to remove pid file {}: {}", self.path.display(), err);
            }
        }
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        self.cleanup_silently();
    }
}

pub fn daemonize(config: &DaemonConfig) -> Result<DaemonContext> {
    // First fork detaches from controlling terminal while allowing the parent to exit.
    unsafe {
        match fork()? {
            ForkResult::Parent { .. } => std::process::exit(0),
            ForkResult::Child => {}
        }
    }

    setsid()?;

    unsafe {
        signal(Signal::SIGHUP, SigHandler::SigIgn)?;
        signal(Signal::SIGCHLD, SigHandler::SigIgn)?;
        signal(Signal::SIGTTOU, SigHandler::SigIgn)?;
        signal(Signal::SIGTTIN, SigHandler::SigIgn)?;
        signal(Signal::SIGPIPE, SigHandler::SigIgn)?;
    }

    unsafe {
        match fork()? {
            ForkResult::Parent { .. } => std::process::exit(0),
            ForkResult::Child => {}
        }
    }

    let mask = Mode::from_bits_truncate(config.umask.unwrap_or(0));
    umask(mask);

    env::set_current_dir(&config.working_directory)?;

    redirect_standard_descriptors(config)?;

    let pid_file = match config.pid_file() {
        Some(path) => Some(PidFileGuard::acquire(path.to_path_buf())?),
        None => None,
    };

    Ok(DaemonContext { pid_file })
}

fn redirect_standard_descriptors(config: &DaemonConfig) -> Result<()> {
    for fd in [STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO] {
        let _ = close(fd);
    }

    let stdin_target = config.stdin().unwrap_or_else(|| Path::new("/dev/null"));
    let stdout_target = config.stdout().unwrap_or_else(|| Path::new("/dev/null"));
    let stderr_target = config.stderr().unwrap_or(stdout_target);

    let stdin_file = open_input(stdin_target)?;
    dup2(stdin_file.as_raw_fd(), STDIN_FILENO)?;

    let stdout_file = open_output(stdout_target)?;
    dup2(stdout_file.as_raw_fd(), STDOUT_FILENO)?;

    if stderr_target == stdout_target {
        dup2(stdout_file.as_raw_fd(), STDERR_FILENO)?;
    } else {
        let stderr_file = open_output(stderr_target)?;
        dup2(stderr_file.as_raw_fd(), STDERR_FILENO)?;
    }

    Ok(())
}

fn open_input(path: &Path) -> Result<File> {
    if path == Path::new("/dev/null") {
        Ok(File::open(path)?)
    } else {
        Ok(OpenOptions::new().read(true).open(path)?)
    }
}

fn open_output(path: &Path) -> Result<File> {
    if path == Path::new("/dev/null") {
        let mut options = OpenOptions::new();
        options.write(true);
        return Ok(options.open(path)?);
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let mut options = OpenOptions::new();
    options.write(true).create(true).append(true).mode(0o644);
    Ok(options.open(path)?)
}
