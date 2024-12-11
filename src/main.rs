use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::Duration;
use log::{info, error, warn, debug};
use std::sync::Arc;
use notify::{Event, RecursiveMode, Watcher};
use std::sync::mpsc;
use std::result::Result;

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
    min_interval: Option<u64>, // New field for minimum interval between uploads
    delete: Option<bool>,      // New field to control '--delete' flag in rsync
    retry_interval: Option<u64>, // New field for retry interval in seconds
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

    // Load and parse the configuration file
    let config = match load_config(&args.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return;
        }
    };

    // Shared replications state
    let replications = Arc::new(config.replications);

    // Spawn a separate task for each replication
    let mut handles = Vec::new();

    for replication in replications.iter() {
        info!("Starting replication task: {}", replication.name);
        let replication = replication.clone();
        let handle = tokio::spawn(run_replication_task(replication));
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

/// Loads and parses the configuration file
fn load_config(config_path: &PathBuf) -> Result<Config, String> {
    // Read and parse the configuration file
    let config_content = match fs::read_to_string(config_path) {
        Ok(content) => content,
        Err(e) => {
            return Err(format!("Failed to read config file {}: {}", config_path.display(), e));
        }
    };

    // Determine configuration format based on file extension
    if config_path.extension().and_then(|s| s.to_str()) == Some("yaml")
        || config_path.extension().and_then(|s| s.to_str()) == Some("yml")
    {
        match serde_yaml::from_str(&config_content) {
            Ok(cfg) => Ok(cfg),
            Err(e) => Err(format!("Failed to parse YAML config file: {}", e)),
        }
    } else {
        // Default to TOML
        match toml::from_str(&config_content) {
            Ok(cfg) => Ok(cfg),
            Err(e) => Err(format!("Failed to parse TOML config file: {}", e)),
        }
    }
}

/// Runs the replication task for a single replication configuration
async fn run_replication_task(replication: Replication) {
    let retry_interval = Duration::from_secs(replication.retry_interval.unwrap_or(10));
    let min_interval = Duration::from_secs(replication.min_interval.unwrap_or(5));
    let mut remote_accessible = false;

    loop {
        // Check if remote is accessible
        if is_remote_accessible(&replication).await {
            if !remote_accessible {
                info!(
                    "[{}] Remote is now accessible. Resuming normal operation.",
                    replication.name
                );
                remote_accessible = true;

                // Ensure remote directory exists
                if let Err(e) = ensure_remote_directory(&replication).await {
                    error!(
                        "[{}] Failed to ensure remote directory exists: {}",
                        replication.name, e
                    );
                    return;
                }

                // Start watching the local directory
                if let Err(e) = watch_and_sync(replication.clone(), min_interval).await {
                    error!(
                        "[{}] Error in watch_and_sync: {}",
                        replication.name, e
                    );
                    remote_accessible = false;
                    continue;
                }
            }
        } else {
            if remote_accessible {
                warn!(
                    "[{}] Remote became inaccessible. Entering retry mode.",
                    replication.name
                );
                remote_accessible = false;
            }
            // Wait before retrying
            tokio::time::sleep(retry_interval).await;
        }
    }
}

/// Checks if the remote host is accessible via SSH
async fn is_remote_accessible(replication: &Replication) -> bool {
    let check_status = Command::new("ssh")
        .args(&[&replication.remote, "echo 'remote accessible'"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;

    match check_status {
        Ok(status) => status.success(),
        Err(_) => false,
    }
}

/// Ensures that the remote directory exists by creating it if necessary
async fn ensure_remote_directory(replication: &Replication) -> Result<(), String> {
    let mkdir_status = Command::new("ssh")
        .args(&[
            &replication.remote,
            &format!("mkdir -p {}", replication.remote_dir),
        ])
        .status()
        .await;

    match mkdir_status {
        Ok(s) if s.success() => {
            debug!("[{}] Ensured remote directory exists: {}", replication.name, replication.remote_dir);
            Ok(())
        }
        Ok(s) => Err(format!(
            "Failed to create remote directory {} with status {}",
            replication.remote_dir, s
        )),
        Err(e) => Err(format!(
            "SSH command to create remote directory {} failed: {}",
            replication.remote_dir, e
        )),
    }
}

/// Watches the local directory and synchronizes changes to the remote directory
async fn watch_and_sync(replication: Replication, min_interval: Duration) -> Result<(), String> {
    // Create a channel to receive file change events
    let (tx, rx) = mpsc::channel::<Result<Event, notify::Error>>();
    let mut watcher = notify::recommended_watcher(tx).expect("Failed to create watcher");
    // Start watching the local directory
    let watch_path = PathBuf::from(&replication.local);
    watcher
        .watch(&watch_path, RecursiveMode::Recursive)
        .unwrap();

    // Perform initial sync
    perform_sync(&replication).await;

    let mut last_change = std::time::Instant::now();

    loop {
        match rx.recv() {
            Ok(event) => {
                // File change detected
                debug!(
                    "[{}] Detected change: {:?}",
                    replication.name, event
                );
                let now = std::time::Instant::now();
                if now.duration_since(last_change) >= min_interval {
                    last_change = now;
                    // Trigger sync
                    perform_sync(&replication).await;
                } else {
                    // Schedule sync after the remaining interval
                    let wait_time = min_interval - now.duration_since(last_change);
                    let replication_clone = replication.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(wait_time).await;
                        perform_sync(&replication_clone).await;
                    });
                }
            }
            Err(e) => {
                return Err(format!("Watch error: {}", e));
            }
        }
    }
}

async fn perform_sync(replication: &Replication) {
    // Build rsync arguments
    let mut rsync_args = vec!["-avz"];
    if replication.delete.unwrap_or(false) {
        rsync_args.push("--delete");
    }
    rsync_args.push(&replication.local);
    let remote_path = format!("{}:{}", replication.remote, replication.remote_dir);
    rsync_args.push(&remote_path);

    // Execute rsync command
    let status = Command::new("rsync")
        .args(&rsync_args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .status()
        .await;

    match status {
        Ok(s) if s.success() => {
            info!(
                "[{}] Successfully synced '{}' to '{}:{}'.",
                replication.name,
                replication.local,
                replication.remote,
                replication.remote_dir
            );
        }
        Ok(s) => {
            warn!(
                "[{}] Rsync exited with status {}. Remote may be inaccessible. Entering retry mode.",
                replication.name, s
            );
        }
        Err(e) => {
            error!(
                "[{}] Failed to execute rsync for '{}' to '{}:{}': {}",
                replication.name, replication.local, replication.remote, replication.remote_dir, e
            );
        }
    }
}