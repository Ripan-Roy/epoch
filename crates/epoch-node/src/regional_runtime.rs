//! Lifecycle wrapper for the complete regional catalog and tablet runtime.

use std::{
    fmt::{self, Formatter},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use axum::Router;
use epoch_consensus::CommittedProposal;
use epoch_core::Clock;
use thiserror::Error;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};

use crate::{
    catalog_api::{
        DEFAULT_CATALOG_COMMIT_WAIT, RegionalCatalogApiError, RegionalCatalogState,
        SharedRegionalTabletMaterializer, regional_catalog_router,
    },
    catalog_tablet::{CatalogTabletScope, CatalogTabletService},
    consensus::{CommittedProposalApplier, ConsensusProbeConfig},
    consensus_groups::{
        ConsensusGroupSupervisor, ConsensusGroupSupervisorError, SupervisedConsensusGroupFailure,
        shared_internal_peer_router,
    },
    regional_maintenance::{RegionalMaintenanceStatus, run_regional_maintenance_pass},
    regional_router::{
        DEFAULT_REGIONAL_READ_BARRIER_TIMEOUT, MAX_REGIONAL_READ_BARRIER_TIMEOUT,
        regional_tablet_router_with_read_timeout,
    },
    regional_topology::{NodeTopology, regional_topology_router},
    tablet_materializer::{RegionalTabletMaterializer, TabletMaterializerError},
};

const DEFAULT_PROFILE_COMMIT_WAIT: Duration = Duration::from_secs(5);
pub const DEFAULT_REGIONAL_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_REGIONAL_MAINTENANCE_INTERVAL: Duration = Duration::from_mins(1);

#[derive(Clone)]
pub struct RegionalRuntimeConfig {
    pub catalog_group: ConsensusProbeConfig,
    pub data_dir: PathBuf,
    pub max_groups: usize,
    pub topology: NodeTopology,
    pub clock: Arc<dyn Clock>,
    pub catalog_commit_wait: Duration,
    pub profile_commit_wait: Duration,
    pub read_barrier_timeout: Duration,
    pub maintenance_interval: Duration,
}

impl fmt::Debug for RegionalRuntimeConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegionalRuntimeConfig")
            .field("catalog_group", &self.catalog_group)
            .field("data_dir", &self.data_dir)
            .field("max_groups", &self.max_groups)
            .field("topology", &self.topology)
            .field("catalog_commit_wait", &self.catalog_commit_wait)
            .field("profile_commit_wait", &self.profile_commit_wait)
            .field("read_barrier_timeout", &self.read_barrier_timeout)
            .field("maintenance_interval", &self.maintenance_interval)
            .finish_non_exhaustive()
    }
}

impl RegionalRuntimeConfig {
    pub fn new(
        catalog_group: ConsensusProbeConfig,
        data_dir: impl Into<PathBuf>,
        max_groups: usize,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let topology = NodeTopology::new(
            catalog_group.node_id().get(),
            "local",
            "local",
            "general-purpose",
            catalog_group.voters().map(epoch_consensus::NodeId::get),
            max_groups,
        )
        .expect("validated consensus config produces valid local topology");
        Self {
            catalog_group,
            data_dir: data_dir.into(),
            max_groups,
            topology,
            clock,
            catalog_commit_wait: DEFAULT_CATALOG_COMMIT_WAIT,
            profile_commit_wait: DEFAULT_PROFILE_COMMIT_WAIT,
            read_barrier_timeout: DEFAULT_REGIONAL_READ_BARRIER_TIMEOUT,
            maintenance_interval: DEFAULT_REGIONAL_MAINTENANCE_INTERVAL,
        }
    }

    #[must_use]
    pub fn with_topology(mut self, topology: NodeTopology) -> Self {
        self.topology = topology;
        self
    }

    #[must_use]
    pub fn with_read_barrier_timeout(mut self, timeout: Duration) -> Self {
        self.read_barrier_timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_maintenance_interval(mut self, interval: Duration) -> Self {
        self.maintenance_interval = interval;
        self
    }
}

#[derive(Debug, Clone, Error)]
pub enum RegionalRuntimeFailure {
    #[error("supervised consensus failure: {0}")]
    Consensus(String),
    #[error("catalog reconciliation failure: {0}")]
    Reconciliation(String),
    #[error("regional runtime notification channel closed unexpectedly")]
    NotificationChannelClosed,
}

#[derive(Debug, Error)]
pub enum RegionalRuntimeError {
    #[error(transparent)]
    Supervisor(#[from] ConsensusGroupSupervisorError),
    #[error(transparent)]
    Materializer(#[from] TabletMaterializerError),
    #[error(transparent)]
    Catalog(#[from] RegionalCatalogApiError),
    #[error("invalid regional runtime configuration: {0}")]
    InvalidConfiguration(String),
    #[error("regional runtime storage could not be prepared: {0}")]
    Storage(String),
    #[error("regional runtime background task failed: {0}")]
    TaskJoin(String),
}

pub struct RegionalNodeRuntime {
    public_router: Router,
    peer_router: Router,
    catalog_state: RegionalCatalogState,
    materializer: SharedRegionalTabletMaterializer,
    stop: watch::Sender<bool>,
    failure: watch::Receiver<Option<RegionalRuntimeFailure>>,
    background: Option<JoinHandle<()>>,
}

impl fmt::Debug for RegionalNodeRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegionalNodeRuntime")
            .field("catalog_state", &self.catalog_state)
            .field("stopped", &*self.stop.borrow())
            .finish_non_exhaustive()
    }
}

impl RegionalNodeRuntime {
    pub async fn start(config: RegionalRuntimeConfig) -> Result<Self, RegionalRuntimeError> {
        if config.max_groups < 2 {
            return Err(RegionalRuntimeError::InvalidConfiguration(
                "regional mode requires capacity for the catalog and at least one data group"
                    .into(),
            ));
        }
        if config.catalog_commit_wait.is_zero()
            || config.profile_commit_wait.is_zero()
            || config.read_barrier_timeout.is_zero()
            || config.read_barrier_timeout > MAX_REGIONAL_READ_BARRIER_TIMEOUT
            || config.maintenance_interval.is_zero()
            || config.maintenance_interval > MAX_REGIONAL_MAINTENANCE_INTERVAL
        {
            return Err(RegionalRuntimeError::InvalidConfiguration(
                "catalog/profile waits must be non-zero and read-barrier/maintenance intervals must be between 1 ms and 60 seconds".into(),
            ));
        }
        let catalog_scope = CatalogTabletScope::new(
            config.catalog_group.group_id().get(),
            config.catalog_group.group_epoch().get(),
        )
        .map_err(RegionalRuntimeError::InvalidConfiguration)?;
        let catalog = CatalogTabletService::new(catalog_scope);
        let mut supervisor =
            ConsensusGroupSupervisor::new(config.catalog_group.node_id().get(), config.max_groups)?;
        let stable_directory = config
            .data_dir
            .join("consensus")
            .join(format!("group-{}", config.catalog_group.group_id().get()));
        std::fs::create_dir_all(&stable_directory)
            .map_err(|error| RegionalRuntimeError::Storage(error.to_string()))?;
        let stable_path =
            stable_directory.join(format!("node-{}.wal", config.catalog_group.node_id().get()));
        let applier: Arc<dyn CommittedProposalApplier> = catalog.clone();
        let catalog_consensus = supervisor
            .start_group(config.catalog_group.clone(), stable_path, Some(applier))
            .await?;
        let materializer = RegionalTabletMaterializer::new(
            supervisor,
            config.catalog_group,
            config.data_dir,
            Arc::clone(&config.clock),
            config.profile_commit_wait,
        )?;
        let directory = materializer.directory();
        let peer_router = shared_internal_peer_router(materializer.peer_registry());
        let group_failures = materializer.subscribe_group_failures();
        let materializer = Arc::new(Mutex::new(materializer));
        let catalog_state = RegionalCatalogState::new(
            catalog,
            catalog_consensus,
            Arc::clone(&materializer),
            config.catalog_commit_wait,
        )
        .map_err(RegionalRuntimeError::InvalidConfiguration)?;
        let catalog_commits = catalog_state.subscribe_commits();
        catalog_state.reconcile_latest().await?;

        let maintenance_interval_ms = u64::try_from(config.maintenance_interval.as_millis())
            .map_err(|_| {
                RegionalRuntimeError::InvalidConfiguration(
                    "maintenance interval cannot be represented in milliseconds".into(),
                )
            })?;
        let maintenance_status = RegionalMaintenanceStatus::new(maintenance_interval_ms);
        let public_router = regional_catalog_router(catalog_state.clone())
            .merge(regional_tablet_router_with_read_timeout(
                directory.clone(),
                config.read_barrier_timeout,
            ))
            .merge(regional_topology_router(
                config.topology,
                directory.clone(),
                Arc::clone(&maintenance_status),
            ));
        let (stop, failure, background) = spawn_background(
            catalog_state.clone(),
            catalog_commits,
            group_failures,
            directory,
            config.clock,
            config.maintenance_interval,
            maintenance_status,
        );

        Ok(Self {
            public_router,
            peer_router,
            catalog_state,
            materializer,
            stop,
            failure,
            background: Some(background),
        })
    }

    pub fn public_router(&self) -> Router {
        self.public_router.clone()
    }

    pub fn peer_router(&self) -> Router {
        self.peer_router.clone()
    }

    pub fn catalog_state(&self) -> RegionalCatalogState {
        self.catalog_state.clone()
    }

    pub async fn wait_for_failure(&self) -> RegionalRuntimeFailure {
        let mut failure = self.failure.clone();
        loop {
            if let Some(error) = failure.borrow().clone() {
                return error;
            }
            if failure.changed().await.is_err() {
                return RegionalRuntimeFailure::NotificationChannelClosed;
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), RegionalRuntimeError> {
        let _ = self.stop.send(true);
        if let Some(background) = self.background.take() {
            background
                .await
                .map_err(|error| RegionalRuntimeError::TaskJoin(error.to_string()))?;
        }
        self.materializer.lock().await.shutdown().await?;
        Ok(())
    }
}

fn spawn_background(
    reconcile_state: RegionalCatalogState,
    mut catalog_commits: tokio::sync::broadcast::Receiver<CommittedProposal>,
    mut group_failures: tokio::sync::broadcast::Receiver<SupervisedConsensusGroupFailure>,
    directory: crate::tablet_materializer::TabletDirectory,
    clock: Arc<dyn Clock>,
    maintenance_interval: Duration,
    maintenance_status: Arc<RegionalMaintenanceStatus>,
) -> (
    watch::Sender<bool>,
    watch::Receiver<Option<RegionalRuntimeFailure>>,
    JoinHandle<()>,
) {
    let (stop, mut stopped) = watch::channel(false);
    let (failure_tx, failure) = watch::channel(None);
    let background = tokio::spawn(async move {
        let mut maintenance = tokio::time::interval(maintenance_interval);
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                changed = stopped.changed() => {
                    if changed.is_err() || *stopped.borrow() {
                        return;
                    }
                }
                group_failure = group_failures.recv() => {
                    let failure = match group_failure {
                        Ok(failure) => RegionalRuntimeFailure::Consensus(failure.to_string()),
                        Err(_) => RegionalRuntimeFailure::NotificationChannelClosed,
                    };
                    failure_tx.send_replace(Some(failure));
                    return;
                }
                _ = maintenance.tick() => {
                    let now_ms = clock.wall_time_ms();
                    let (pass, error) = run_regional_maintenance_pass(&directory, now_ms).await;
                    maintenance_status.record(now_ms, pass, error);
                }
                commit = catalog_commits.recv() => {
                    match commit {
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            if let Err(error) = reconcile_state.reconcile_latest().await {
                                failure_tx.send_replace(Some(
                                    RegionalRuntimeFailure::Reconciliation(error.to_string())
                                ));
                                return;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            failure_tx.send_replace(Some(
                                RegionalRuntimeFailure::NotificationChannelClosed
                            ));
                            return;
                        }
                    }
                }
            }
        }
    });
    (stop, failure, background)
}

impl Drop for RegionalNodeRuntime {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(background) = self.background.take() {
            background.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};
    use epoch_core::ManualClock;
    use tempfile::TempDir;
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::catalog_api::REGIONAL_CATALOG_PATH;

    #[tokio::test]
    async fn empty_regional_runtime_exposes_catalog_and_stops_cleanly() {
        let directory = TempDir::new().expect("temp directory should be created");
        let config = ConsensusProbeConfig::new(
            1,
            1,
            1,
            [
                (1, Url::parse("http://127.0.0.1:42001/").unwrap()),
                (2, Url::parse("http://127.0.0.1:42002/").unwrap()),
                (3, Url::parse("http://127.0.0.1:42003/").unwrap()),
            ],
            Duration::from_mins(1),
        )
        .unwrap();
        let mut runtime = RegionalNodeRuntime::start(RegionalRuntimeConfig::new(
            config,
            directory.path(),
            8,
            Arc::new(ManualClock::new(1_000)),
        ))
        .await
        .expect("regional runtime should start");
        let response = runtime
            .public_router()
            .oneshot(
                Request::get(REGIONAL_CATALOG_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        runtime.shutdown().await.expect("runtime should stop");
    }
}
