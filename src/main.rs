use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use log::{info, error, warn, debug};
use std::sync::Arc;

/// Struct to parse command-line arguments using clap
#[derive(Parser, Debug)]
#[command(
    name = "Directory Replicator",
    version = "2.0",
    author = "Your Name",
    about = "Continuously replicates directories to multiple SSH devices using rsync."
)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, value_name = "FILE", default_value = "config.toml")]
    config: PathBuf,

    /// Sets the level of verbosity
    #[arg(short, long, value_name = "LEVEL", default_value = "debug")]
    log_level: String,
}

/// Struct representing a single replication task
#[derive(Debug, Deserialize, Clone)]
struct Replication {
    name: String,
    local: String,
    remote: String,
    remote_dir: String,
    interval: Option<u64>, // Optional, default to 5 seconds
}

/// Struct to deserialize the entire configuration file
#[derive(Debug, Deserialize)]
struct Config {
    replications: Vec<Replication>,
}

#[tokio::main]
async fn main() {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize the logger with the specified log level
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&args.log_level)).init();

    info!("Starting Directory Replicator");

    // Read and parse the configuration file
    let config_content = match fs::read_to_string(&args.config) {
        Ok(content) => content,
        Err(e) => {
            error!("Failed to read config file {}: {}", args.config.display(), e);
            return;
        }
    };

    // Determine configuration format based on file extension
    let config: Config = if args.config.extension().and_then(|s| s.to_str()) == Some("yaml")
        || args.config.extension().and_then(|s| s.to_str()) == Some("yml")
    {
        match serde_yaml::from_str(&config_content) {
            Ok(cfg) => cfg,
            Err(e) => {
                error!("Failed to parse YAML config file: {}", e);
                return;
            }
        }
    } else {
        // Default to TOML
        match toml::from_str(&config_content) {
            Ok(cfg) => cfg,
            Err(e) => {
                error!("Failed to parse TOML config file: {}", e);
                return;
            }
        }
    };

    // Shared logging and replication state
    let replications = Arc::new(config.replications);

    // Spawn a separate task for each replication
    let mut handles = Vec::new();

    for replication in replications.iter() {
        info!("Starting replication task: {}", replication.name);
        info!(
            "Command: rsync -avz --delete {} {}:{}",
            replication.local, replication.remote, replication.remote_dir
        );
        info!(
            "Interval: {} seconds",
            replication.interval.unwrap_or(5)
        );
        let replication = replication.clone();
        let handle = tokio::spawn(async move {
            // Ensure remote directory exists
            info!(
                "[{}] Ensuring remote directory exists: {}",
                replication.name, replication.remote_dir
            );
            let mkdir_status = Command::new("ssh")
                .args(&[
                    &replication.remote,
                    &format!("mkdir -p {}", replication.remote_dir),
                ])
                .status()
                .await;

            match mkdir_status {
                Ok(s) if s.success() => {
                    debug!("Ensured remote directory exists: {}", replication.remote_dir);
                }
                Ok(s) => {
                    error!(
                        "[{}] Failed to create remote directory {} with status {}",
                        replication.name, replication.remote_dir, s
                    );
                    return;
                }
                Err(e) => {
                    error!(
                        "[{}] SSH command to create remote directory {} failed: {}",
                        replication.name, replication.remote_dir, e
                    );
                    return;
                }
            }

            let interval_duration = Duration::from_secs(replication.interval.unwrap_or(5));
            loop {
                debug!("Executing rsync for replication: {}", replication.name);

                // Execute rsync command
                let status = Command::new("rsync")
                    .args(&[
                        "-avz",
                        "--delete",
                        &replication.local,
                        &format!("{}:{}", replication.remote, replication.remote_dir),
                    ])
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .status()
                    .await;

                match status {
                    Ok(s) if s.success() => {
                        info!(
                            "[{}] Successfully synced '{}' to '{}:{}'.",
                            replication.name, replication.local, replication.remote, replication.remote_dir
                        );
                    }
                    Ok(s) => {
                        warn!(
                            "[{}] Rsync exited with status {} for '{}' to '{}:{}'",
                            replication.name, s, replication.local, replication.remote, replication.remote_dir
                        );
                    }
                    Err(e) => {
                        error!(
                            "[{}] Failed to execute rsync for '{}' to '{}:{}': {}",
                            replication.name, replication.local, replication.remote, replication.remote_dir, e
                        );
                    }
                }

                // Wait for the specified interval before the next sync
                sleep(interval_duration).await;
            }
        });

        handles.push(handle);
    }

    info!("All replication tasks have been spawned.");

    // Await all replication tasks (they run indefinitely)
    for handle in handles {
        if let Err(e) = handle.await {
            error!("A replication task encountered an error: {}", e);
        }
    }
}