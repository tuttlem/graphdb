use env_logger::{Builder, Env, Target, WriteStyle};

use crate::config::DaemonConfig;
use crate::error::Result;

pub fn init_logging(config: &DaemonConfig) -> Result<()> {
    let env = if let Some(level) = config.log_level.as_deref() {
        Env::default().default_filter_or(level)
    } else {
        Env::default().default_filter_or("info")
    };

    let mut builder = Builder::from_env(env);
    builder.target(Target::Stdout);
    builder.write_style(WriteStyle::Never);
    builder.format_timestamp_secs();
    builder.try_init()?;

    Ok(())
}
