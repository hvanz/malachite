//! Shell command execution utilities.

use color_eyre::eyre::{bail, Context, Result};
use std::path::Path;
use tracing::debug;

/// Execute a command in a given directory, inheriting stdout/stderr.
pub(crate) fn exec(cmd: &str, args: &[&str], dir: &Path) -> Result<()> {
    debug!(%cmd, args=%args.join(" "), dir=%dir.display(), "Executing");

    let status = std::process::Command::new(cmd)
        .args(args)
        .current_dir(dir)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .wrap_err_with(|| format!("Failed to execute {cmd} {}", args.join(" ")))?;

    if status.success() {
        Ok(())
    } else {
        let code = status
            .code()
            .map_or_else(|| "signal".to_string(), |c| c.to_string());
        bail!("Command failed with exit code {code}: {cmd} {}", args.join(" "))
    }
}

/// Execute a command in a given directory and return its stdout.
#[allow(dead_code)]
pub(crate) fn exec_with_output(cmd: &str, args: &[&str], dir: &Path) -> Result<String> {
    debug!(%cmd, args=%args.join(" "), dir=%dir.display(), "Executing (capture output)");

    let output = std::process::Command::new(cmd)
        .args(args)
        .current_dir(dir)
        .output()
        .wrap_err_with(|| format!("Failed to execute {cmd} {}", args.join(" ")))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!("Command failed: {stderr}")
    }
}
