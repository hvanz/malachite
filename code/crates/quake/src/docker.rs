//! Docker and Docker Compose command wrappers.

use color_eyre::eyre::{bail, Context, Result};
use std::path::Path;

use crate::shell;

/// Check if a Docker Compose file exists at the given path.
pub(crate) fn compose_file_exists(compose_path: &Path) -> Result<()> {
    if !compose_path.exists() {
        bail!("{} not found", compose_path.display());
    }
    Ok(())
}

/// Execute a `docker` command.
pub(crate) fn exec(dir: &Path, args: &[&str]) -> Result<()> {
    shell::exec("docker", args, dir).wrap_err("Failed to execute docker command")
}

/// Execute a `docker compose` command with the given compose file.
pub(crate) fn compose_exec(dir: &Path, compose_path: &Path, args: &[&str]) -> Result<()> {
    compose_file_exists(compose_path).wrap_err("Run `quake setup` to generate testnet files")?;

    let compose_path_str = compose_path
        .to_str()
        .expect("Failed to convert compose file path to string");

    let mut compose_args = vec!["compose", "-f", compose_path_str];
    compose_args.extend_from_slice(args);
    shell::exec("docker", &compose_args, dir).wrap_err("Failed to execute docker compose command")
}

/// Check whether a Docker image exists locally.
pub(crate) fn image_exists(image: &str, dir: &Path) -> bool {
    let result = shell::exec_with_output(
        "docker",
        &[
            "images",
            "--format",
            "{{.Repository}}:{{.Tag}}",
            "--filter",
            &format!("reference={image}"),
        ],
        dir,
    );
    match result {
        Ok(output) => !output.trim().is_empty(),
        Err(_) => false,
    }
}

/// Disconnect a container from a Docker network.
pub(crate) fn network_disconnect(dir: &Path, network: &str, container: &str) -> Result<()> {
    exec(dir, &["network", "disconnect", network, container])
        .wrap_err_with(|| format!("Failed to disconnect {container} from {network}"))
}

/// Connect a container to a Docker network, optionally with a fixed IP.
pub(crate) fn network_connect(
    dir: &Path,
    network: &str,
    container: &str,
    ip: Option<&str>,
) -> Result<()> {
    let mut args = vec!["network", "connect"];
    if let Some(ip) = ip {
        args.extend_from_slice(&["--ip", ip]);
    }
    args.extend_from_slice(&[network, container]);
    exec(dir, &args).wrap_err_with(|| format!("Failed to connect {container} to {network}"))
}

/// Kill one or more containers.
pub(crate) fn kill(dir: &Path, containers: &[String]) -> Result<()> {
    let mut args: Vec<&str> = vec!["kill"];
    args.extend(containers.iter().map(|s| s.as_str()));
    exec(dir, &args).wrap_err("Failed to kill containers")
}

/// Start one or more stopped containers.
pub(crate) fn start(dir: &Path, containers: &[String]) -> Result<()> {
    let mut args: Vec<&str> = vec!["start"];
    args.extend(containers.iter().map(|s| s.as_str()));
    exec(dir, &args).wrap_err("Failed to start containers")
}

/// Pause one or more containers.
pub(crate) fn pause(dir: &Path, containers: &[String]) -> Result<()> {
    let mut args: Vec<&str> = vec!["pause"];
    args.extend(containers.iter().map(|s| s.as_str()));
    exec(dir, &args).wrap_err("Failed to pause containers")
}

/// Unpause one or more containers.
pub(crate) fn unpause(dir: &Path, containers: &[String]) -> Result<()> {
    let mut args: Vec<&str> = vec!["unpause"];
    args.extend(containers.iter().map(|s| s.as_str()));
    exec(dir, &args).wrap_err("Failed to unpause containers")
}

/// Restart one or more containers.
pub(crate) fn restart(dir: &Path, containers: &[String]) -> Result<()> {
    let mut args: Vec<&str> = vec!["restart"];
    args.extend(containers.iter().map(|s| s.as_str()));
    exec(dir, &args).wrap_err("Failed to restart containers")
}

/// Start (up -d) services via Docker Compose.
pub(crate) fn compose_up(
    dir: &Path,
    compose_path: &Path,
    services: &[impl AsRef<str>],
) -> Result<()> {
    let mut args: Vec<&str> = vec!["up", "-d"];
    args.extend(services.iter().map(|s| s.as_ref()));
    compose_exec(dir, compose_path, &args)
}

/// Stop services via Docker Compose.
pub(crate) fn compose_stop(
    dir: &Path,
    compose_path: &Path,
    services: &[impl AsRef<str>],
) -> Result<()> {
    let mut args: Vec<&str> = vec!["stop"];
    args.extend(services.iter().map(|s| s.as_ref()));
    compose_exec(dir, compose_path, &args)
}

/// Tear down services via Docker Compose.
pub(crate) fn compose_down(dir: &Path, compose_path: &Path, extra_args: &[&str]) -> Result<()> {
    let mut args: Vec<&str> = vec!["down"];
    args.extend_from_slice(extra_args);
    compose_exec(dir, compose_path, &args)
}

/// Output logs from Docker Compose services.
pub(crate) fn compose_logs(
    dir: &Path,
    compose_path: &Path,
    services: &[impl AsRef<str>],
    follow: bool,
) -> Result<()> {
    let mut args: Vec<&str> = vec!["logs"];
    if follow {
        args.push("-f");
    }
    args.extend(services.iter().map(|s| s.as_ref()));
    compose_exec(dir, compose_path, &args)
}

/// Execute a `docker compose` command and return its stdout.
#[allow(dead_code)]
pub(crate) fn compose_exec_with_output(
    dir: &Path,
    compose_path: &Path,
    args: &[&str],
) -> Result<String> {
    compose_file_exists(compose_path)?;

    let compose_path_str = compose_path
        .to_str()
        .expect("Failed to convert compose file path to string");

    let mut compose_args = vec!["compose", "-f", compose_path_str];
    compose_args.extend_from_slice(args);
    shell::exec_with_output("docker", &compose_args, dir)
}
