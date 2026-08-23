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
    epoch_target_delivery::{
        EpochTargetDeliveryConfig, EpochTargetDeliveryStatus, EpochTargetDeliveryWorker,
        run_epoch_target_delivery_pass,
    },
    managed_target_delivery::{
        ManagedTargetDeliveryConfig, ManagedTargetDeliveryStatus, ManagedTargetDeliveryWorker,
        run_managed_target_delivery_pass,
    },
    regional_checkpoint::{RegionalCheckpointStatus, run_regional_checkpoint_pass},
    regional_maintenance::{RegionalMaintenanceStatus, run_regional_maintenance_pass},
    regional_router::{
        DEFAULT_REGIONAL_READ_BARRIER_TIMEOUT, MAX_REGIONAL_READ_BARRIER_TIMEOUT,
        regional_tablet_router_with_read_timeout,
    },
    regional_topology::{NodeTopology, regional_topology_router},
    tablet_materializer::{RegionalTabletMaterializer, TabletMaterializerError},
    webhook_delivery::{
        WebhookDeliveryConfig, WebhookDeliveryStatus, WebhookDeliveryWorker,
        run_webhook_delivery_pass,
    },
};

const DEFAULT_PROFILE_COMMIT_WAIT: Duration = Duration::from_secs(5);
pub const DEFAULT_REGIONAL_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_REGIONAL_MAINTENANCE_INTERVAL: Duration = Duration::from_mins(1);
pub const DEFAULT_REGIONAL_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_REGIONAL_CHECKPOINT_MIN_APPLIED_ENTRIES: u64 = 1_024;
pub const MAX_REGIONAL_CHECKPOINT_INTERVAL: Duration = Duration::from_mins(10);

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
    pub checkpoint_interval: Duration,
    pub checkpoint_min_applied_entries: u64,
    pub epoch_target_delivery: EpochTargetDeliveryConfig,
    pub managed_target_delivery: ManagedTargetDeliveryConfig,
    pub webhook_delivery: Option<WebhookDeliveryConfig>,
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
            .field("checkpoint_interval", &self.checkpoint_interval)
            .field(
                "checkpoint_min_applied_entries",
                &self.checkpoint_min_applied_entries,
            )
            .field("epoch_target_delivery", &self.epoch_target_delivery)
            .field("managed_target_delivery", &self.managed_target_delivery)
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
            checkpoint_interval: DEFAULT_REGIONAL_CHECKPOINT_INTERVAL,
            checkpoint_min_applied_entries: DEFAULT_REGIONAL_CHECKPOINT_MIN_APPLIED_ENTRIES,
            epoch_target_delivery: EpochTargetDeliveryConfig::default(),
            managed_target_delivery: ManagedTargetDeliveryConfig::default(),
            webhook_delivery: None,
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

    #[must_use]
    pub fn with_checkpoint_policy(mut self, interval: Duration, min_applied_entries: u64) -> Self {
        self.checkpoint_interval = interval;
        self.checkpoint_min_applied_entries = min_applied_entries;
        self
    }

    #[must_use]
    pub fn with_webhook_delivery(mut self, config: Option<WebhookDeliveryConfig>) -> Self {
        self.webhook_delivery = config;
        self
    }

    #[must_use]
    pub fn with_epoch_target_delivery(mut self, config: EpochTargetDeliveryConfig) -> Self {
        self.epoch_target_delivery = config;
        self
    }

    #[must_use]
    pub fn with_managed_target_delivery(mut self, config: ManagedTargetDeliveryConfig) -> Self {
        self.managed_target_delivery = config;
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

struct RegionalBackground {
    reconcile_state: RegionalCatalogState,
    directory: crate::tablet_materializer::TabletDirectory,
    clock: Arc<dyn Clock>,
    maintenance_interval: Duration,
    maintenance_status: Arc<RegionalMaintenanceStatus>,
    checkpoint_interval: Duration,
    checkpoint_min_applied_entries: u64,
    checkpoint_status: Arc<RegionalCheckpointStatus>,
    epoch_target_worker: EpochTargetDeliveryWorker,
    epoch_target_status: Arc<EpochTargetDeliveryStatus>,
    managed_target_worker: ManagedTargetDeliveryWorker,
    managed_target_status: Arc<ManagedTargetDeliveryStatus>,
    webhook_worker: Option<WebhookDeliveryWorker>,
    webhook_status: Arc<WebhookDeliveryStatus>,
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
    #[allow(
        clippy::too_many_lines,
        reason = "runtime startup wires independently tested catalog, tablet, maintenance, checkpoint, webhook, topology, and supervision components"
    )]
    pub async fn start(config: RegionalRuntimeConfig) -> Result<Self, RegionalRuntimeError> {
        validate_config(&config)?;
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
        let materializer = RegionalTabletMaterializer::new_with_cluster_id(
            supervisor,
            config.catalog_group,
            config.data_dir,
            Arc::clone(&config.clock),
            config.profile_commit_wait,
            config.topology.region(),
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
        let checkpoint_interval_ms = u64::try_from(config.checkpoint_interval.as_millis())
            .map_err(|_| {
                RegionalRuntimeError::InvalidConfiguration(
                    "checkpoint interval cannot be represented in milliseconds".into(),
                )
            })?;
        let checkpoint_status = RegionalCheckpointStatus::new(
            checkpoint_interval_ms,
            config.checkpoint_min_applied_entries,
        );
        let epoch_target_worker = EpochTargetDeliveryWorker::new(
            config.epoch_target_delivery.clone(),
            config.profile_commit_wait,
        )
        .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
        let epoch_target_status =
            EpochTargetDeliveryStatus::new(config.epoch_target_delivery.interval);
        let managed_target_worker = ManagedTargetDeliveryWorker::new(
            config.managed_target_delivery.clone(),
            config.profile_commit_wait,
        )
        .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
        let managed_target_status =
            ManagedTargetDeliveryStatus::new(config.managed_target_delivery.interval);
        let webhook_worker = config
            .webhook_delivery
            .clone()
            .map(|webhook| WebhookDeliveryWorker::new(webhook, config.profile_commit_wait))
            .transpose()
            .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
        let webhook_status = config
            .webhook_delivery
            .as_ref()
            .map_or_else(WebhookDeliveryStatus::disabled, |webhook| {
                WebhookDeliveryStatus::enabled(webhook.interval)
            });
        let public_router = regional_catalog_router(catalog_state.clone())
            .merge(regional_tablet_router_with_read_timeout(
                directory.clone(),
                config.read_barrier_timeout,
            ))
            .merge(regional_topology_router(
                config.topology,
                directory.clone(),
                Arc::clone(&maintenance_status),
                Arc::clone(&checkpoint_status),
                Arc::clone(&epoch_target_status),
                Arc::clone(&managed_target_status),
                Arc::clone(&webhook_status),
            ));
        let background_config = RegionalBackground {
            reconcile_state: catalog_state.clone(),
            directory,
            clock: config.clock,
            maintenance_interval: config.maintenance_interval,
            maintenance_status,
            checkpoint_interval: config.checkpoint_interval,
            checkpoint_min_applied_entries: config.checkpoint_min_applied_entries,
            checkpoint_status,
            epoch_target_worker,
            epoch_target_status,
            managed_target_worker,
            managed_target_status,
            webhook_worker,
            webhook_status,
        };
        let (stop, failure, background) =
            spawn_background(background_config, catalog_commits, group_failures);

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

fn validate_config(config: &RegionalRuntimeConfig) -> Result<(), RegionalRuntimeError> {
    if config.max_groups < 2 {
        return Err(RegionalRuntimeError::InvalidConfiguration(
            "regional mode requires capacity for the catalog and at least one data group".into(),
        ));
    }
    if config.catalog_commit_wait.is_zero()
        || config.profile_commit_wait.is_zero()
        || config.read_barrier_timeout.is_zero()
        || config.read_barrier_timeout > MAX_REGIONAL_READ_BARRIER_TIMEOUT
        || config.maintenance_interval.is_zero()
        || config.maintenance_interval > MAX_REGIONAL_MAINTENANCE_INTERVAL
        || config.checkpoint_interval.is_zero()
        || config.checkpoint_interval > MAX_REGIONAL_CHECKPOINT_INTERVAL
        || config.checkpoint_min_applied_entries == 0
    {
        return Err(RegionalRuntimeError::InvalidConfiguration(
            "catalog/profile waits and checkpoint threshold must be non-zero; read-barrier/maintenance intervals must be at most 60 seconds and checkpoint interval at most 10 minutes".into(),
        ));
    }
    if let Some(webhook) = &config.webhook_delivery {
        webhook
            .validate()
            .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
    }
    config
        .epoch_target_delivery
        .validate()
        .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
    config
        .managed_target_delivery
        .validate()
        .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
    Ok(())
}

struct BackgroundIntervals {
    maintenance: tokio::time::Interval,
    checkpoints: tokio::time::Interval,
    epoch_targets: tokio::time::Interval,
    managed_targets: tokio::time::Interval,
    webhooks: Option<tokio::time::Interval>,
}

impl BackgroundIntervals {
    fn new(background: &RegionalBackground) -> Self {
        Self {
            maintenance: background_interval(background.maintenance_interval),
            checkpoints: background_interval(background.checkpoint_interval),
            epoch_targets: background_interval(background.epoch_target_worker.config().interval),
            managed_targets: background_interval(
                background.managed_target_worker.config().interval,
            ),
            webhooks: background
                .webhook_worker
                .as_ref()
                .map(|worker| background_interval(worker.config().interval)),
        }
    }
}

fn background_interval(period: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

fn spawn_background(
    background: RegionalBackground,
    mut catalog_commits: tokio::sync::broadcast::Receiver<CommittedProposal>,
    mut group_failures: tokio::sync::broadcast::Receiver<SupervisedConsensusGroupFailure>,
) -> (
    watch::Sender<bool>,
    watch::Receiver<Option<RegionalRuntimeFailure>>,
    JoinHandle<()>,
) {
    let (stop, mut stopped) = watch::channel(false);
    let (failure_tx, failure) = watch::channel(None);
    let background = tokio::spawn(async move {
        let mut intervals = BackgroundIntervals::new(&background);
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
                _ = intervals.maintenance.tick() => {
                    let now_ms = background.clock.wall_time_ms();
                    let (pass, error) = run_regional_maintenance_pass(&background.directory, now_ms).await;
                    background.maintenance_status.record(now_ms, pass, error);
                }
                _ = intervals.checkpoints.tick() => {
                    let now_ms = background.clock.wall_time_ms();
                    let (pass, groups, error) = run_regional_checkpoint_pass(
                        &background.reconcile_state.consensus_handle(),
                        &background.directory,
                        background.checkpoint_min_applied_entries,
                    ).await;
                    background.checkpoint_status.record(now_ms, pass, groups, error);
                }
                _ = intervals.epoch_targets.tick() => {
                    let now_ms = background.clock.wall_time_ms();
                    let (pass, error) = run_epoch_target_delivery_pass(
                        &background.directory,
                        &background.epoch_target_worker,
                        background.clock.as_ref(),
                    ).await;
                    background.epoch_target_status.record(now_ms, pass, error);
                }
                _ = intervals.managed_targets.tick() => {
                    let now_ms = background.clock.wall_time_ms();
                    let (pass, error) = run_managed_target_delivery_pass(
                        &background.directory,
                        &background.managed_target_worker,
                        background.clock.as_ref(),
                    ).await;
                    background.managed_target_status.record(now_ms, pass, error);
                }
                () = async {
                    if let Some(interval) = intervals.webhooks.as_mut() {
                        interval.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    let now_ms = background.clock.wall_time_ms();
                    if let Some(worker) = background.webhook_worker.as_ref() {
                        let (pass, error) = run_webhook_delivery_pass(
                            &background.directory,
                            worker,
                            background.clock.as_ref(),
                        ).await;
                        background.webhook_status.record(now_ms, pass, error);
                    }
                }
                commit = catalog_commits.recv() => {
                    match commit {
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            if let Err(error) = background.reconcile_state.reconcile_latest().await {
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
