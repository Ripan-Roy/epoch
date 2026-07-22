use std::{
    error::Error, future::Future, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration,
};

use clap::Parser;
use epoch_core::{DeploymentMode, SystemClock};
use epoch_engine::EpochEngine;
use epoch_node::{
    consensus::{
        CommittedProposalApplier, ConsensusProbeConfig, ConsensusProbeError, ConsensusProbeRuntime,
    },
    router, spawn_maintenance,
    stream_tablet::{self, DEFAULT_COMMIT_WAIT, StreamTabletService},
    validate_allowed_origins,
};
use epoch_storage::{DEFAULT_WAL_SEGMENT_BYTES, MIN_WAL_SEGMENT_BYTES, StandaloneWal};
use epoch_tablet::StreamTabletScope;
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
    #[arg(long, env = "EPOCH_EXPERIMENTAL_STREAM_TABLET_ENABLED")]
    experimental_stream_tablet_enabled: bool,
    #[arg(
        long,
        env = "EPOCH_EXPERIMENTAL_STREAM_TABLET_NAME",
        default_value = "experimental-stream"
    )]
    experimental_stream_tablet_name: String,
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
    stream_tablet_scope: Option<StreamTabletScope>,
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
    let clock = Arc::new(SystemClock::default());
    let engine = Arc::new(EpochEngine::with_commit_log(
        DeploymentMode::Standalone,
        clock.clone(),
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
        serve_consensus_mode(launch, listener, app, clock).await
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

async fn serve_consensus_mode(
    launch: ConsensusProbeLaunch,
    public_listener: TcpListener,
    public_app: axum::Router,
    clock: Arc<SystemClock>,
) -> Result<(), Box<dyn Error>> {
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
    let (runtime, consensus_app, profile_replication) =
        start_consensus_mode(&launch, clock).await?;
    let recovery = runtime.recovery();
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
        profile_replication,
        profile_guarantee_ceiling = if profile_replication {
            "experimental_fixed_voter_majority"
        } else {
            "local_durable"
        },
        peer_authentication = "none",
        "experimental fixed-voter consensus probe is listening"
    );
    let server_result = serve_with_consensus_probe(
        public_listener,
        public_app,
        consensus_listener,
        consensus_app,
        runtime.wait_for_actor_failure(),
    )
    .await
    .map_err(boxed_error);
    let runtime_result = runtime.shutdown().await.map_err(boxed_error);
    server_result.and(runtime_result)
}

async fn start_consensus_mode(
    launch: &ConsensusProbeLaunch,
    clock: Arc<SystemClock>,
) -> Result<(ConsensusProbeRuntime, axum::Router, bool), Box<dyn Error>> {
    if let Some(scope) = launch.stream_tablet_scope.clone() {
        let tablet = StreamTabletService::new(scope)?;
        let applier: Arc<dyn CommittedProposalApplier> = tablet.clone();
        let runtime = ConsensusProbeRuntime::start_with_profile_applier(
            launch.config.clone(),
            &launch.stable_path,
            applier,
        )
        .await?;
        let app = runtime.internal_router().merge(stream_tablet::router(
            tablet,
            runtime.handle(),
            clock,
            DEFAULT_COMMIT_WAIT,
        ));
        Ok((runtime, app, true))
    } else {
        let runtime =
            ConsensusProbeRuntime::start(launch.config.clone(), &launch.stable_path).await?;
        let app = runtime
            .internal_router()
            .merge(runtime.experimental_router());
        Ok((runtime, app, false))
    }
}

fn boxed_error(error: impl Error + 'static) -> Box<dyn Error> {
    Box::new(error)
}

fn consensus_probe_launch(
    args: &Args,
) -> Result<Option<ConsensusProbeLaunch>, ConsensusProbeError> {
    if args.experimental_stream_tablet_enabled && !args.consensus_probe_enabled {
        return Err(ConsensusProbeError::InvalidConfiguration(
            "EPOCH_EXPERIMENTAL_STREAM_TABLET_ENABLED requires EPOCH_CONSENSUS_PROBE_ENABLED"
                .into(),
        ));
    }
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
    let stream_tablet_scope = args
        .experimental_stream_tablet_enabled
        .then(|| {
            StreamTabletScope::new(
                config.group_id().get(),
                config.group_epoch().get(),
                &args.experimental_stream_tablet_name,
            )
            .map_err(|error| ConsensusProbeError::InvalidConfiguration(error.to_string()))
        })
        .transpose()?;
    Ok(Some(ConsensusProbeLaunch {
        config,
        listen: args.consensus_listen,
        stable_path,
        stream_tablet_scope,
    }))
}

async fn serve_with_consensus_probe(
    public_listener: TcpListener,
    public_app: axum::Router,
    consensus_listener: TcpListener,
    consensus_app: axum::Router,
    actor_failure: impl Future<Output = ConsensusProbeError>,
) -> std::io::Result<()> {
    serve_with_consensus_probe_until(
        public_listener,
        public_app,
        consensus_listener,
        consensus_app,
        shutdown_signal(),
        actor_failure,
    )
    .await
}

async fn serve_with_consensus_probe_until(
    public_listener: TcpListener,
    public_app: axum::Router,
    consensus_listener: TcpListener,
    consensus_app: axum::Router,
    shutdown: impl Future<Output = ()>,
    actor_failure: impl Future<Output = ConsensusProbeError>,
) -> std::io::Result<()> {
    let (drain_tx, public_shutdown) = watch::channel(false);
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
    tokio::pin!(shutdown);
    tokio::pin!(actor_failure);

    let (public_finished, consensus_finished, first_result) = tokio::select! {
        biased;
        error = &mut actor_failure => (
            false,
            false,
            Err(std::io::Error::other(error)),
        ),
        () = &mut shutdown => (false, false, Ok(())),
        result = &mut public_server => (true, false, result),
        result = &mut consensus_server => (false, true, result),
    };
    let _ = drain_tx.send(true);
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{Router, extract::State, http::StatusCode, routing::get};
    use tokio::sync::{Notify, oneshot};

    use super::*;

    #[derive(Clone)]
    struct DrainState {
        started: Arc<AtomicUsize>,
        release: Arc<Notify>,
    }

    async fn held_request(State(state): State<DrainState>) -> StatusCode {
        state.started.fetch_add(1, Ordering::SeqCst);
        state.release.notified().await;
        StatusCode::NO_CONTENT
    }

    #[tokio::test]
    async fn actor_failure_drains_both_http_servers_and_returns_the_cause() {
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let consensus_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let consensus_address = consensus_listener.local_addr().unwrap();
        let state = DrainState {
            started: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(Notify::new()),
        };
        let app = || {
            Router::new()
                .route("/held", get(held_request))
                .with_state(state.clone())
        };
        let (failure_tx, failure_rx) = oneshot::channel();
        let mut serving = tokio::spawn(serve_with_consensus_probe_until(
            public_listener,
            app(),
            consensus_listener,
            app(),
            std::future::pending(),
            async move {
                failure_rx
                    .await
                    .expect("test actor failure should be delivered")
            },
        ));
        let public_request = tokio::spawn(reqwest::get(format!("http://{public_address}/held")));
        let consensus_request =
            tokio::spawn(reqwest::get(format!("http://{consensus_address}/held")));
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.started.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both requests should enter their handlers");

        failure_tx
            .send(ConsensusProbeError::ProfileApplication(
                "injected supervision failure".into(),
            ))
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut serving)
                .await
                .is_err(),
            "server supervision should drain in-flight requests"
        );
        state.release.notify_waiters();

        assert_eq!(
            public_request.await.unwrap().unwrap().status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            consensus_request.await.unwrap().unwrap().status(),
            StatusCode::NO_CONTENT
        );
        let error = serving
            .await
            .expect("server supervisor should not panic")
            .expect_err("actor failure must fail the server supervisor");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert!(error.to_string().contains("injected supervision failure"));
    }

    #[tokio::test]
    async fn operator_shutdown_remains_successful_while_the_actor_is_healthy() {
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let consensus_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        serve_with_consensus_probe_until(
            public_listener,
            Router::new(),
            consensus_listener,
            Router::new(),
            std::future::ready(()),
            std::future::pending::<ConsensusProbeError>(),
        )
        .await
        .expect("operator shutdown should drain both healthy servers successfully");
    }

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
    fn experimental_stream_tablet_requires_the_consensus_runtime() {
        let args =
            Args::try_parse_from(["epoch-node", "--experimental-stream-tablet-enabled"]).unwrap();
        assert!(
            consensus_probe_launch(&args)
                .unwrap_err()
                .to_string()
                .contains("EPOCH_CONSENSUS_PROBE_ENABLED")
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
        assert!(launch.stream_tablet_scope.is_none());
    }

    #[test]
    fn experimental_stream_tablet_uses_the_consensus_group_scope() {
        let args = Args::try_parse_from([
            "epoch-node",
            "--consensus-probe-enabled",
            "--experimental-stream-tablet-enabled",
            "--experimental-stream-tablet-name",
            "orders",
            "--consensus-node-id",
            "2",
            "--consensus-group-id",
            "7",
            "--consensus-group-epoch",
            "3",
            "--consensus-peers",
            "1=http://127.0.0.1:7701,2=http://127.0.0.1:7702,3=http://127.0.0.1:7703",
        ])
        .unwrap();
        let launch = consensus_probe_launch(&args).unwrap().unwrap();
        assert_eq!(
            launch.stream_tablet_scope,
            Some(StreamTabletScope::new(7, 3, "orders").unwrap())
        );
    }
}
