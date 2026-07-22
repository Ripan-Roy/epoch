use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use epoch_core::DeploymentMode;
use epoch_engine::EpochEngine;
use epoch_node::{router, spawn_maintenance, validate_allowed_origins};
use epoch_storage::{DEFAULT_WAL_SEGMENT_BYTES, MIN_WAL_SEGMENT_BYTES, StandaloneWal};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_ALLOWED_ORIGINS: &str =
    "http://127.0.0.1:5173,http://localhost:5173,http://127.0.0.1:4173,http://localhost:4173";

#[derive(Debug, Parser)]
#[command(name = "epoch-node", version, about = "Epoch standalone data node")]
struct Args {
    #[arg(long, env = "EPOCH_HTTP_LISTEN", default_value = "127.0.0.1:7601")]
    http_listen: SocketAddr,
    #[arg(long, env = "EPOCH_LOG", default_value = "info")]
    log: String,
    #[arg(long, env = "EPOCH_DATA_DIR", default_value = ".epoch")]
    data_dir: PathBuf,
    #[arg(
        long,
        env = "EPOCH_WAL_SEGMENT_BYTES",
        default_value_t = DEFAULT_WAL_SEGMENT_BYTES,
        value_parser = clap::value_parser!(u64).range(MIN_WAL_SEGMENT_BYTES..)
    )]
    wal_segment_bytes: u64,
    #[arg(
        long,
        env = "EPOCH_ALLOWED_ORIGINS",
        value_delimiter = ',',
        default_value = DEFAULT_ALLOWED_ORIGINS
    )]
    allowed_origins: Vec<String>,
    #[arg(long, env = "EPOCH_JSON_LOGS")]
    json_logs: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_allowed_origins(&args.allowed_origins)?;
    let filter = EnvFilter::try_new(&args.log).unwrap_or_else(|_| EnvFilter::new("info"));
    if args.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let wal_directory = args.data_dir.join("engine-wal");
    let legacy_wal_path = args.data_dir.join("engine.wal");
    let wal = StandaloneWal::open(&args.data_dir, args.wal_segment_bytes)?;
    let recovered_partial_tail = wal.recovered_partial_tail();
    let wal_segment_count = wal.segment_count();
    let wal_layout = if wal.uses_legacy_layout() {
        "legacy-single-file"
    } else {
        "segmented-v1"
    };
    if recovered_partial_tail {
        warn!(
            directory = %wal_directory.display(),
            legacy_path = %legacy_wal_path.display(),
            "discarded an incomplete WAL tail during recovery"
        );
    }
    let engine = Arc::new(EpochEngine::with_commit_log(
        DeploymentMode::Standalone,
        Arc::new(epoch_core::SystemClock),
        Box::new(wal),
    )?);
    let app = router(engine.clone(), &args.allowed_origins)?;
    let maintenance = spawn_maintenance(engine.clone());
    let listener = TcpListener::bind(args.http_listen).await?;
    info!(
        address = %args.http_listen,
        data_dir = %args.data_dir.display(),
        wal_directory = %wal_directory.display(),
        wal_segment_bytes = args.wal_segment_bytes,
        wal_segment_count,
        wal_layout,
        guarantee_ceiling = ?engine.health().guarantee_ceiling,
        "Epoch standalone node is listening"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    maintenance.abort();
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install termination handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_segment_target_rejects_values_smaller_than_a_frame_header() {
        assert!(
            Args::try_parse_from([
                "epoch-node",
                "--wal-segment-bytes",
                &(MIN_WAL_SEGMENT_BYTES - 1).to_string(),
            ])
            .is_err()
        );
    }

    #[test]
    fn wal_segment_target_accepts_the_storage_minimum() {
        let args = Args::try_parse_from([
            "epoch-node",
            "--wal-segment-bytes",
            &MIN_WAL_SEGMENT_BYTES.to_string(),
        ])
        .unwrap();
        assert_eq!(args.wal_segment_bytes, MIN_WAL_SEGMENT_BYTES);
    }

    #[test]
    fn browser_origins_have_safe_local_defaults() {
        let args = Args::try_parse_from(["epoch-node"]).unwrap();
        assert_eq!(
            args.allowed_origins,
            [
                "http://127.0.0.1:5173",
                "http://localhost:5173",
                "http://127.0.0.1:4173",
                "http://localhost:4173"
            ]
        );
    }

    #[test]
    fn browser_origins_accept_a_comma_delimited_allowlist() {
        let args = Args::try_parse_from([
            "epoch-node",
            "--allowed-origins",
            "https://console.example,http://127.0.0.1:4173",
        ])
        .unwrap();
        assert_eq!(
            args.allowed_origins,
            ["https://console.example", "http://127.0.0.1:4173"]
        );
    }
}
