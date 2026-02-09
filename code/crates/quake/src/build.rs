//! Docker image build logic for the testnet.

use std::fs;
use std::path::Path;

use color_eyre::eyre::{Context, Result};
use tracing::{debug, info};

use crate::docker;

/// Build the testnet Docker image via multi-stage Docker build.
///
/// 1. Exports macOS Keychain CA certs (including corporate CAs) for Docker
/// 2. Reads GITHUB_TOKEN from `.env` and passes it as a Docker secret
/// 3. Builds inside Docker following the same pattern as Dockerfile.malachite
pub fn build_image(root_dir: &Path, quake_dir: &Path, image: &str) -> Result<()> {
    // Step 1: export macOS Keychain certs (includes corporate CAs)
    let ca_cert_path = quake_dir.join("ca-certificates.crt");
    info!("Exporting macOS CA certificates...");
    let certs = crate::shell::exec_with_output(
        "security",
        &[
            "find-certificate",
            "-a",
            "-p",
            "/System/Library/Keychains/SystemRootCertificates.keychain",
            "/Library/Keychains/System.keychain",
        ],
        root_dir,
    )
    .context("Failed to export macOS CA certificates")?;
    fs::write(&ca_cert_path, &certs)?;
    debug!("Exported CA certs to {}", ca_cert_path.display());

    // Step 2: read GITHUB_TOKEN from .env
    let env_path = root_dir.join(".env");
    let github_token = if env_path.exists() {
        let env_content = fs::read_to_string(&env_path)?;
        env_content.lines().find_map(|line| {
            line.strip_prefix("GITHUB_TOKEN=")
                .map(|v| v.trim().to_string())
        })
    } else {
        std::env::var("GITHUB_TOKEN").ok()
    };

    // Step 3: docker build with token secret
    info!("Building Docker image: {image}");
    let mut args: Vec<String> = vec!["build".to_string()];
    if let Some(ref token) = github_token {
        args.push("--secret".to_string());
        args.push("id=GITHUB_TOKEN,env=GITHUB_TOKEN".to_string());
        // Set the env var so Docker can read it
        std::env::set_var("GITHUB_TOKEN", token);
        debug!("Passing GITHUB_TOKEN as Docker secret");
    }
    args.extend_from_slice(&[
        "-t".to_string(),
        image.to_string(),
        "-f".to_string(),
        "Dockerfile".to_string(),
        ".".to_string(),
    ]);

    let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    docker::exec(root_dir, &args_ref)?;

    info!("✅ Docker image built: {image}");
    Ok(())
}
