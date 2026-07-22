use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use epoch_core::DeploymentMode;
use epoch_engine::EpochEngine;
use epoch_node::{router, spawn_maintenance};
use epoch_storage::FileWal;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "epoch-node", version, about = "Epoch standalone data node")]
struct Args {
    #[arg(long, env = "EPOCH_HTTP_LISTEN", default_value = "127.0.0.1:7601")]
    http_listen: SocketAddr,
    #[arg(long, env = "EPOCH_LOG", default_value = "info")]
    log: String,
    #[arg(long, env = "EPOCH_DATA_DIR", default_value = ".epoch")]
    data_dir: PathBuf,
    #[arg(long, env = "EPOCH_JSON_LOGS")]
    json_logs: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let filter = EnvFilter::try_new(&args.log).unwrap_or_else(|_| EnvFilter::new("info"));
    if args.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    std::fs::create_dir_all(&args.data_dir)?;
    let wal_path = args.data_dir.join("engine.wal");
    let wal = FileWal::open(&wal_path)?;
    if wal.recovered_partial_tail() {
        warn!(path = %wal_path.display(), "discarded an incomplete WAL tail during recovery");
    }
    let engine = Arc::new(EpochEngine::with_commit_log(
        DeploymentMode::Standalone,
        Arc::new(epoch_core::SystemClock),
        Box::new(wal),
    )?);
    let maintenance = spawn_maintenance(engine.clone());
    let listener = TcpListener::bind(args.http_listen).await?;
    info!(
        address = %args.http_listen,
        data_dir = %args.data_dir.display(),
        guarantee_ceiling = ?engine.health().guarantee_ceiling,
        "Epoch standalone node is listening"
    );
    axum::serve(listener, router(engine))
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
