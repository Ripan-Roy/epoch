use std::{error::Error, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use clap::Parser;
use epoch_core::DeploymentMode;
use epoch_engine::EpochEngine;
use epoch_node::{
    consensus::{ConsensusProbeConfig, ConsensusProbeError, ConsensusProbeRuntime},
    router, spawn_maintenance, validate_allowed_origins,
};
use epoch_storage::{DEFAULT_WAL_SEGMENT_BYTES, MIN_WAL_SEGMENT_BYTES, StandaloneWal};
use tokio::{net::TcpListener, sync::watch};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_ALLOWED_ORIGINS: &str =
    "http://127.0.0.1:5173,http://localhost:5173,http://127.0.0.1:4173,http://localhost:4173";
const DEFAULT_CONSENSUS_LISTEN: &str = "127.0.0.1:7701";
const DEFAULT_CONSENSUS_TICK_MS: u64 = 100;
const SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

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
    #[arg(long, env = "EPOCH_CONSENSUS_PROBE_ENABLED")]
    consensus_probe_enabled: bool,
    #[arg(long, env = "EPOCH_CONSENSUS_NODE_ID")]
    consensus_node_id: Option<u64>,
    #[arg(long, env = "EPOCH_CONSENSUS_GROUP_ID", default_value_t = 1)]
    consensus_group_id: u64,
    #[arg(long, env = "EPOCH_CONSENSUS_GROUP_EPOCH", default_value_t = 1)]
    consensus_group_epoch: u64,
    #[arg(
        long,
        env = "EPOCH_CONSENSUS_LISTEN",
        default_value = DEFAULT_CONSENSUS_LISTEN
    )]
    consensus_listen: SocketAddr,
    #[arg(long, env = "EPOCH_CONSENSUS_PEERS")]
    consensus_peers: Option<String>,
    #[arg(
        long,
        env = "EPOCH_CONSENSUS_TICK_MS",
        default_value_t = DEFAULT_CONSENSUS_TICK_MS
    )]
    consensus_tick_ms: u64,
}

#[derive(Debug)]
struct ConsensusProbeLaunch {
    config: ConsensusProbeConfig,
    listen: SocketAddr,
    stable_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    validate_allowed_origins(&args.allowed_origins)?;
    let consensus_probe = consensus_probe_launch(&args)?;
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
        Arc::new(epoch_core::SystemClock::default()),
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

    let serving_result = if let Some(launch) = consensus_probe {
        let stable_parent = launch.stable_path.parent().ok_or_else(|| {
            ConsensusProbeError::InvalidConfiguration(format!(
                "consensus stable path has no parent: {}",
                launch.stable_path.display()
            ))
        })?;
        std::fs::create_dir_all(stable_parent)?;
        let consensus_listener = TcpListener::bind(launch.listen).await?;
        let node_id = launch.config.node_id();
        let group_id = launch.config.group_id();
        let group_epoch = launch.config.group_epoch();
        let tick_interval = launch.config.tick_interval();
        let runtime = ConsensusProbeRuntime::start(launch.config, &launch.stable_path).await?;
        let recovery = runtime.recovery();
        let consensus_app = runtime
            .internal_router()
            .merge(runtime.experimental_router());
        info!(
            address = %launch.listen,
            %node_id,
            %group_id,
            %group_epoch,
            tick_ms = tick_interval.as_millis(),
            stable_path = %launch.stable_path.display(),
            stable_generation = recovery.stable_generation,
            applied_index = %recovery.applied_index,
            repaired_partial_tail = recovery.repaired_partial_tail,
            profile_replication = false,
            profile_guarantee_ceiling = "local_durable",
            peer_authentication = "none",
            "experimental fixed-voter consensus probe is listening"
        );
        let server_result =
            serve_with_consensus_probe(listener, app, consensus_listener, consensus_app)
                .await
                .map_err(boxed_error);
        let runtime_result = runtime.shutdown().await.map_err(boxed_error);
        server_result.and(runtime_result)
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(boxed_error)
    };
    maintenance.abort();
    let _ = maintenance.await;
    serving_result
}

fn boxed_error(error: impl Error + 'static) -> Box<dyn Error> {
    Box::new(error)
}

fn consensus_probe_launch(
    args: &Args,
) -> Result<Option<ConsensusProbeLaunch>, ConsensusProbeError> {
    if !args.consensus_probe_enabled {
        return Ok(None);
    }
    let node_id = args.consensus_node_id.ok_or_else(|| {
        ConsensusProbeError::InvalidConfiguration(
            "EPOCH_CONSENSUS_NODE_ID is required when the probe is enabled".into(),
        )
    })?;
    let peer_spec = args.consensus_peers.as_deref().ok_or_else(|| {
        ConsensusProbeError::InvalidConfiguration(
            "EPOCH_CONSENSUS_PEERS is required when the probe is enabled".into(),
        )
    })?;
    let config = ConsensusProbeConfig::from_peer_spec(
        node_id,
        args.consensus_group_id,
        args.consensus_group_epoch,
        peer_spec,
        Duration::from_millis(args.consensus_tick_ms),
    )?;
    let stable_path = args
        .data_dir
        .join("consensus")
        .join(format!("group-{}", config.group_id().get()))
        .join(format!("node-{}.wal", config.node_id().get()));
    Ok(Some(ConsensusProbeLaunch {
        config,
        listen: args.consensus_listen,
        stable_path,
    }))
}

async fn serve_with_consensus_probe(
    public_listener: TcpListener,
    public_app: axum::Router,
    consensus_listener: TcpListener,
    consensus_app: axum::Router,
) -> std::io::Result<()> {
    let (shutdown, public_shutdown) = watch::channel(false);
    let consensus_shutdown = public_shutdown.clone();
    let public_server = async move {
        axum::serve(public_listener, public_app)
            .with_graceful_shutdown(wait_for_shutdown(public_shutdown))
            .await
    };
    let consensus_server = async move {
        axum::serve(consensus_listener, consensus_app)
            .with_graceful_shutdown(wait_for_shutdown(consensus_shutdown))
            .await
    };
    tokio::pin!(public_server);
    tokio::pin!(consensus_server);

    let (public_finished, consensus_finished, first_result) = tokio::select! {
        () = shutdown_signal() => (false, false, Ok(())),
        result = &mut public_server => (true, false, result),
        result = &mut consensus_server => (false, true, result),
    };
    let _ = shutdown.send(true);
    let drain_servers = async {
        let public_drain = async {
            if public_finished {
                Ok(())
            } else {
                public_server.await
            }
        };
        let consensus_drain = async {
            if consensus_finished {
                Ok(())
            } else {
                consensus_server.await
            }
        };
        let (public_result, consensus_result) = tokio::join!(public_drain, consensus_drain);
        first_result?;
        public_result?;
        consensus_result
    };
    tokio::time::timeout(SERVER_SHUTDOWN_TIMEOUT, drain_servers)
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "HTTP servers did not drain within {} seconds",
                    SERVER_SHUTDOWN_TIMEOUT.as_secs()
                ),
            )
        })?
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
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

    #[test]
    fn consensus_probe_is_opt_in() {
        let args = Args::try_parse_from(["epoch-node"]).unwrap();
        assert!(!args.consensus_probe_enabled);
        assert!(consensus_probe_launch(&args).unwrap().is_none());
    }

    #[test]
    fn enabled_consensus_probe_requires_identity_and_peers() {
        let missing_identity = Args::try_parse_from([
            "epoch-node",
            "--consensus-probe-enabled",
            "--consensus-peers",
            "1=http://127.0.0.1:7701,2=http://127.0.0.1:7702,3=http://127.0.0.1:7703",
        ])
        .unwrap();
        assert!(
            consensus_probe_launch(&missing_identity)
                .unwrap_err()
                .to_string()
                .contains("EPOCH_CONSENSUS_NODE_ID")
        );

        let missing_peers = Args::try_parse_from([
            "epoch-node",
            "--consensus-probe-enabled",
            "--consensus-node-id",
            "1",
        ])
        .unwrap();
        assert!(
            consensus_probe_launch(&missing_peers)
                .unwrap_err()
                .to_string()
                .contains("EPOCH_CONSENSUS_PEERS")
        );
    }

    #[test]
    fn consensus_probe_uses_identity_scoped_stable_path() {
        let args = Args::try_parse_from([
            "epoch-node",
            "--data-dir",
            "/tmp/epoch-probe-test",
            "--consensus-probe-enabled",
            "--consensus-node-id",
            "2",
            "--consensus-group-id",
            "7",
            "--consensus-group-epoch",
            "3",
            "--consensus-listen",
            "127.0.0.1:7702",
            "--consensus-peers",
            "1=http://127.0.0.1:7701,2=http://127.0.0.1:7702,3=http://127.0.0.1:7703",
            "--consensus-tick-ms",
            "250",
        ])
        .unwrap();
        let launch = consensus_probe_launch(&args).unwrap().unwrap();
        assert_eq!(launch.config.node_id().get(), 2);
        assert_eq!(launch.config.group_id().get(), 7);
        assert_eq!(launch.config.group_epoch().get(), 3);
        assert_eq!(launch.config.tick_interval(), Duration::from_millis(250));
        assert_eq!(launch.listen, "127.0.0.1:7702".parse().unwrap());
        assert_eq!(
            launch.stable_path,
            PathBuf::from("/tmp/epoch-probe-test/consensus/group-7/node-2.wal")
        );
    }
}
