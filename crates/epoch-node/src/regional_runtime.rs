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
    regional_backup_api::{
        RegionalBackupArtifact, RegionalBackupState, regional_backup_peer_router,
        regional_backup_router,
    },
    regional_checkpoint::{RegionalCheckpointStatus, run_regional_checkpoint_pass},
    regional_maintenance::{RegionalMaintenanceStatus, run_regional_maintenance_pass},
    regional_maintenance_api::regional_maintenance_router,
    regional_membership::{PendingTabletMembershipAction, run_tablet_membership_pass},
    regional_router::{
        DEFAULT_REGIONAL_READ_BARRIER_TIMEOUT, MAX_REGIONAL_READ_BARRIER_TIMEOUT,
        regional_tablet_router_with_read_timeout,
    },
    regional_topology::{NodeTopology, RegionalTopologyStatuses, regional_topology_router},
    source_connector_delivery::{
        DEFAULT_SOURCE_CONNECTOR_INTERVAL, SourceConnectorDeliveryStatus,
        SourceConnectorDeliveryWorker, run_source_connector_delivery_pass,
    },
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
const CATALOG_MEMBERSHIP_RECONCILE_INTERVAL: Duration = Duration::from_millis(250);

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
    pub source_connector_interval: Duration,
    pub webhook_delivery: Option<WebhookDeliveryConfig>,
    pub restore_artifact: Option<Arc<RegionalBackupArtifact>>,
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
            .field("source_connector_interval", &self.source_connector_interval)
            .field("restore_pending", &self.restore_artifact.is_some())
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
            catalog_group
                .voters()
                .iter()
                .copied()
                .map(epoch_consensus::NodeId::get)
                .collect::<Vec<_>>(),
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
            source_connector_interval: DEFAULT_SOURCE_CONNECTOR_INTERVAL,
            webhook_delivery: None,
            restore_artifact: None,
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

    #[must_use]
    pub fn with_source_connector_interval(mut self, interval: Duration) -> Self {
        self.source_connector_interval = interval;
        self
    }

    #[must_use]
    pub fn with_restore_artifact(mut self, artifact: Arc<RegionalBackupArtifact>) -> Self {
        self.restore_artifact = Some(artifact);
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
    #[error("regional runtime semantic restore failed: {0}")]
    Restore(String),
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
    source_connector_worker: SourceConnectorDeliveryWorker,
    source_connector_status: Arc<SourceConnectorDeliveryStatus>,
    webhook_worker: Option<WebhookDeliveryWorker>,
    webhook_status: Arc<WebhookDeliveryStatus>,
    catalog_membership_pending: Option<epoch_consensus::NodeId>,
    tablet_membership_pending: std::collections::BTreeMap<u64, PendingTabletMembershipAction>,
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
        let backup_coordinator_config = config.catalog_group.clone();
        if let Some(artifact) = config.restore_artifact.as_ref() {
            artifact
                .validate_for_restore()
                .map_err(|error| RegionalRuntimeError::Restore(error.to_string()))?;
            validate_fresh_restore_directory(&config.data_dir)?;
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
        if let Some(artifact) = config.restore_artifact.as_ref() {
            artifact
                .restore_group(&config.catalog_group, &stable_path)
                .map_err(|error| RegionalRuntimeError::Restore(error.to_string()))?;
        }
        let applier: Arc<dyn CommittedProposalApplier> = catalog.clone();
        let catalog_consensus = supervisor
            .start_group(config.catalog_group.clone(), stable_path, Some(applier))
            .await?;
        let materializer = RegionalTabletMaterializer::new_with_cluster_id_and_restore(
            supervisor,
            config.catalog_group,
            config.data_dir,
            Arc::clone(&config.clock),
            config.profile_commit_wait,
            config.topology.region(),
            config.restore_artifact,
        )?;
        let directory = materializer.directory();
        let peer_registry = materializer.peer_registry();
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
        let source_connector_worker = SourceConnectorDeliveryWorker::new(
            config.source_connector_interval,
            config.managed_target_delivery.clone(),
            config.profile_commit_wait,
        )
        .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
        let source_connector_status =
            SourceConnectorDeliveryStatus::new(config.source_connector_interval);
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
        let backup_state = RegionalBackupState::new(
            catalog_state.consensus_handle(),
            directory.clone(),
            Arc::clone(&config.clock),
            config.read_barrier_timeout,
            backup_coordinator_config,
        )
        .map_err(|error| RegionalRuntimeError::InvalidConfiguration(error.to_string()))?;
        let peer_router = shared_internal_peer_router(peer_registry.clone())
            .merge(regional_maintenance_router(peer_registry))
            .merge(regional_backup_peer_router(backup_state.clone()));
        let public_router = regional_catalog_router(catalog_state.clone())
            .merge(regional_tablet_router_with_read_timeout(
                directory.clone(),
                config.read_barrier_timeout,
            ))
            .merge(regional_topology_router(
                config.topology,
                directory.clone(),
                RegionalTopologyStatuses::new(
                    Arc::clone(&maintenance_status),
                    Arc::clone(&checkpoint_status),
                    Arc::clone(&epoch_target_status),
                    Arc::clone(&managed_target_status),
                    Arc::clone(&source_connector_status),
                    Arc::clone(&webhook_status),
                ),
            ))
            .merge(regional_backup_router(backup_state));
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
            source_connector_worker,
            source_connector_status,
            webhook_worker,
            webhook_status,
            catalog_membership_pending: None,
            tablet_membership_pending: std::collections::BTreeMap::new(),
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

fn validate_fresh_restore_directory(path: &std::path::Path) -> Result<(), RegionalRuntimeError> {
    let path = path.join("consensus");
    match std::fs::metadata(&path) {
        Ok(metadata) if !metadata.is_dir() => Err(RegionalRuntimeError::Restore(format!(
            "restore destination {} is not a directory",
            path.display()
        ))),
        Ok(_) => {
            let mut entries = std::fs::read_dir(&path)
                .map_err(|error| RegionalRuntimeError::Storage(error.to_string()))?;
            if entries
                .next()
                .transpose()
                .map_err(|error| RegionalRuntimeError::Storage(error.to_string()))?
                .is_some()
            {
                Err(RegionalRuntimeError::Restore(format!(
                    "restore destination {} is not empty",
                    path.display()
                )))
            } else {
                Ok(())
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(RegionalRuntimeError::Storage(error.to_string())),
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
    catalog_membership: tokio::time::Interval,
    maintenance: tokio::time::Interval,
    checkpoints: tokio::time::Interval,
    epoch_targets: tokio::time::Interval,
    managed_targets: tokio::time::Interval,
    source_connectors: tokio::time::Interval,
    webhooks: Option<tokio::time::Interval>,
}

impl BackgroundIntervals {
    fn new(background: &RegionalBackground) -> Self {
        Self {
            catalog_membership: background_interval(CATALOG_MEMBERSHIP_RECONCILE_INTERVAL),
            maintenance: background_interval(background.maintenance_interval),
            checkpoints: background_interval(background.checkpoint_interval),
            epoch_targets: background_interval(background.epoch_target_worker.config().interval),
            managed_targets: background_interval(
                background.managed_target_worker.config().interval,
            ),
            source_connectors: background_interval(background.source_connector_worker.interval()),
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

#[allow(
    clippy::too_many_lines,
    reason = "the single select loop makes shutdown, supervision, and every periodic task ordering explicit"
)]
fn spawn_background(
    mut background: RegionalBackground,
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
                _ = intervals.catalog_membership.tick() => {
                    if let Err(error) = reconcile_catalog_membership(
                        &background.reconcile_state.consensus_handle(),
                        &mut background.catalog_membership_pending,
                    ).await {
                        tracing::warn!(%error, "regional catalog learner reconciliation deferred");
                    }
                    if let Err(error) = run_tablet_membership_pass(
                        &background.reconcile_state,
                        &background.directory,
                        &mut background.tablet_membership_pending,
                    ).await {
                        tracing::warn!(%error, "regional tablet membership reconciliation deferred");
                    }
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
                _ = intervals.source_connectors.tick() => {
                    let now_ms = background.clock.wall_time_ms();
                    let (pass, error) = run_source_connector_delivery_pass(
                        &background.directory,
                        &background.source_connector_worker,
                        background.clock.as_ref(),
                    ).await;
                    background.source_connector_status.record(now_ms, pass, error);
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

async fn reconcile_catalog_membership(
    handle: &crate::consensus::ConsensusProbeHandle,
    pending: &mut Option<epoch_consensus::NodeId>,
) -> Result<(), String> {
    let membership = handle
        .membership()
        .await
        .map_err(|error| error.to_string())?;
    if !membership.outgoing_voters.is_empty() {
        return Ok(());
    }
    let admitted = membership
        .voters
        .iter()
        .chain(&membership.learners)
        .chain(&membership.staged_learners)
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let status = handle.status().await.map_err(|error| error.to_string())?;
    if pending.is_some_and(|node_id| admitted.contains(&node_id)) {
        *pending = None;
    }
    if pending.is_some() {
        if status.role != epoch_consensus::ConsensusRole::Leader {
            *pending = None;
        }
        return Ok(());
    }
    if status.role != epoch_consensus::ConsensusRole::Leader {
        return Ok(());
    }
    if let Some(node_id) = membership
        .allowed_members
        .iter()
        .copied()
        .find(|node_id| !admitted.contains(node_id))
    {
        handle
            .add_learner(node_id.get())
            .await
            .map_err(|error| error.to_string())?;
        *pending = Some(node_id);
    }
    Ok(())
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
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::CONTENT_TYPE},
    };
    use epoch_core::{ManualClock, ResourceKind};
    use tempfile::TempDir;
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::{
        catalog_api::REGIONAL_CATALOG_PATH,
        regional_backup_encryption::{decrypt_backup, encrypt_backup},
        regional_router::{RESOURCE_GENERATION_HEADER, TABLET_EPOCH_HEADER},
    };

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[allow(
        clippy::too_many_lines,
        reason = "the end-to-end campaign intentionally keeps cluster construction, placement, distributed backup, and shutdown in one test"
    )]
    async fn seven_physical_nodes_place_three_voter_tablets_on_distinct_subsets() {
        let listeners = bind_peer_listeners(7).await;
        let peers = listeners
            .iter()
            .enumerate()
            .map(|(index, listener)| {
                (
                    u64::try_from(index + 1).unwrap(),
                    Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let directories = (0..7).map(|_| TempDir::new().unwrap()).collect::<Vec<_>>();
        let mut runtimes = Vec::new();
        for (index, directory) in directories.iter().enumerate() {
            let node_id = u64::try_from(index + 1).unwrap();
            let consensus =
                ConsensusProbeConfig::new(node_id, 1, 1, peers.clone(), Duration::from_millis(50))
                    .unwrap()
                    .with_initial_voters([1, 2, 3])
                    .unwrap();
            let topology = NodeTopology::new(
                node_id,
                "ap-south",
                format!("zone-{}", index + 1),
                "general-purpose",
                [1, 2, 3],
                16,
            )
            .unwrap();
            runtimes.push(
                RegionalNodeRuntime::start(
                    RegionalRuntimeConfig::new(
                        consensus,
                        directory.path(),
                        16,
                        Arc::new(ManualClock::new(1_000)),
                    )
                    .with_topology(topology),
                )
                .await
                .unwrap(),
            );
        }
        let mut servers = listeners
            .into_iter()
            .zip(&runtimes)
            .map(|(listener, runtime)| {
                let router = runtime.peer_router();
                tokio::spawn(async move { axum::serve(listener, router).await.unwrap() })
            })
            .collect::<Vec<_>>();

        let leader = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (index, runtime) in runtimes.iter().enumerate() {
                    if runtime
                        .catalog_state
                        .consensus_handle()
                        .status()
                        .await
                        .unwrap()
                        .role
                        == epoch_consensus::ConsensusRole::Leader
                    {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("catalog should elect a leader");

        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let membership = runtimes[leader]
                    .catalog_state
                    .consensus_handle()
                    .membership()
                    .await
                    .unwrap();
                if membership
                    .learners
                    .iter()
                    .copied()
                    .map(epoch_consensus::NodeId::get)
                    .eq(4..=7)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("catalog should admit every non-voter physical node as a learner");

        let response = runtimes[leader]
            .public_router()
            .oneshot(
                Request::put(
                    "/experimental/v1/regional/catalog/resources/acme/shop/dev/core/stream/orders",
                )
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "request_token": "seven-node-placement-v1",
                        "expected_generation": "0",
                        "shard_count": 3,
                        "replica_count": 3,
                        "tablet_placements": [
                            {"shard_index": 0, "voter_node_ids": [1, 2, 3]},
                            {"shard_index": 1, "voter_node_ids": [4, 5, 6]},
                            {"shard_index": 2, "voter_node_ids": [2, 5, 7]}
                        ]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "catalog apply failed: {}",
            String::from_utf8_lossy(&to_bytes(response.into_body(), 64 * 1024).await.unwrap())
        );

        let expected_shards = [
            vec![0],
            vec![0, 2],
            vec![0],
            vec![1],
            vec![1, 2],
            vec![1],
            vec![2],
        ];
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let mut converged = true;
                for (index, runtime) in runtimes.iter().enumerate() {
                    let mut shards = runtime
                        .materializer
                        .lock()
                        .await
                        .directory()
                        .tablets()
                        .unwrap()
                        .into_iter()
                        .map(|metadata| metadata.descriptor.shard_index)
                        .collect::<Vec<_>>();
                    shards.sort_unstable();
                    converged &= shards == expected_shards[index];
                }
                if converged {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("every physical node should converge to only its assigned tablets");

        let artifact = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let response = runtimes[leader]
                    .public_router()
                    .oneshot(
                        Request::post(crate::regional_backup_api::REGIONAL_BACKUP_PATH)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                if response.status() == StatusCode::CREATED {
                    let encoded = to_bytes(response.into_body(), 128 * 1024 * 1024)
                        .await
                        .unwrap();
                    break RegionalBackupArtifact::decode(&encoded).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("catalog leader should collect checkpoints from disjoint tablet leaders");
        assert_eq!(artifact.groups.len(), 4);
        assert_eq!(artifact.groups[0].group_id, 1);
        assert!(artifact.groups.iter().skip(1).any(|group| {
            group
                .resource
                .as_ref()
                .is_some_and(|metadata| metadata.descriptor.voter_node_ids == [4, 5, 6])
        }));

        for server in &mut servers {
            server.abort();
            let _ = server.await;
        }
        for mut runtime in runtimes {
            runtime.shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[allow(
        clippy::too_many_lines,
        reason = "the replacement campaign intentionally proves the API plan, learner catch-up, voter transition, catalog finalization, data continuity, removed-node shutdown, and durable reopen together"
    )]
    async fn catalog_planned_voter_replacement_catches_up_finalizes_and_reopens() {
        let directories = (0..4).map(|_| TempDir::new().unwrap()).collect::<Vec<_>>();
        let (mut runtimes, mut servers) = start_runtime_cluster(&directories, None).await;
        let catalog_leader = wait_for_catalog_leader(&runtimes).await;
        wait_for_catalog_learner(&runtimes[catalog_leader], 4).await;

        let response = runtimes[catalog_leader]
            .public_router()
            .oneshot(
                Request::put(
                    "/experimental/v1/regional/catalog/resources/acme/shop/dev/core/stream/orders",
                )
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "request_token": "replace-orders-create-v1",
                        "expected_generation": "0",
                        "shard_count": 1,
                        "replica_count": 3,
                        "tablet_placements": [
                            {"shard_index": 0, "voter_node_ids": [1, 2, 3]}
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let tablet = &created["mutation"]["resource"]["tablets"][0];
        let tablet_id = tablet["tablet_id"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(tablet["voter_node_ids"], serde_json::json!(["1", "2", "3"]));
        wait_for_tablet_hosts(&runtimes, &[true, true, true, false]).await;

        let (data_leader, data_handle) = profile_leader(&runtimes, ResourceKind::Stream).await;
        let response = runtimes[data_leader]
            .public_router()
            .oneshot(
                Request::post(
                    "/experimental/v1/regional/resources/acme/shop/dev/core/stream/orders/shards/0/data/records",
                )
                .header(CONTENT_TYPE, "application/json")
                .header(RESOURCE_GENERATION_HEADER, "1")
                .header(TABLET_EPOCH_HEADER, "1")
                .body(Body::from(
                    serde_json::json!({
                        "idempotency_key": "replace-orders-record-v1",
                        "expected_term": data_handle.status().await.unwrap().term.get().to_string(),
                        "partition": 0,
                        "envelope": {
                            "id": "replace-orders-record-v1",
                            "source": "membership-test",
                            "type": "order.created",
                            "time_ms": "1000",
                            "payload": {"order_id": "A-1"}
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
        assert!(response.status().is_success());
        let checkpoint = data_handle
            .checkpoint()
            .await
            .expect("the Stream leader should compact before voter replacement");
        assert!(checkpoint.index.get() > 0);

        let catalog_leader = wait_for_catalog_leader(&runtimes).await;
        let response = runtimes[catalog_leader]
            .public_router()
            .oneshot(
                Request::post(format!(
                    "/experimental/v1/regional/catalog/tablets/{tablet_id}/membership"
                ))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "request_token": "replace-orders-node-3-with-4-v1",
                        "expected_tablet_epoch": "1",
                        "expected_resource_generation": "1",
                        "target_voter_node_ids": ["1", "2", "4"]
                    })
                    .to_string(),
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "{}",
            String::from_utf8_lossy(&body)
        );

        wait_for_replacement_finalization(&runtimes, tablet_id).await;
        assert_stream_record_visible(&runtimes[3], 1).await;
        stop_runtime_cluster(&mut runtimes, &mut servers).await;

        let (mut reopened, mut reopened_servers) = start_runtime_cluster(&directories, None).await;
        wait_for_replacement_finalization(&reopened, tablet_id).await;
        assert_stream_record_visible(&reopened[3], 1).await;
        stop_runtime_cluster(&mut reopened, &mut reopened_servers).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn regional_backup_restores_all_profiles_into_fresh_cluster_then_reopens() {
        let source_directories = (0..3).map(|_| TempDir::new().unwrap()).collect::<Vec<_>>();
        let (mut source, mut source_servers) =
            start_runtime_cluster(&source_directories, None).await;
        let catalog_leader = wait_for_catalog_leader(&source).await;
        for (kind, name) in [
            ("stream", "orders"),
            ("cache", "sessions"),
            ("queue", "jobs"),
            ("event-bus", "events"),
        ] {
            let response = source[catalog_leader]
                .public_router()
                .oneshot(
                    Request::put(format!(
                        "/experimental/v1/regional/catalog/resources/acme/shop/dev/core/{kind}/{name}"
                    ))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_token": format!("semantic-restore-{kind}-v1"),
                            "expected_generation": "0",
                            "shard_count": 1,
                            "replica_count": 3,
                            "tablet_placements": [
                                {"shard_index": 0, "voter_node_ids": [1, 2, 3]}
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::CREATED,
                "failed to create {kind}"
            );
        }
        wait_for_tablet_count(&source, 4).await;
        seed_every_profile(&source).await;

        let captured = capture_artifact(&source[catalog_leader]).await;
        assert_eq!(captured.groups.len(), 5);
        let artifact = encrypt_and_authenticate_backup(&captured);
        let expected_digests = artifact_digests(&artifact);
        stop_runtime_cluster(&mut source, &mut source_servers).await;

        let restored_directories = (0..3).map(|_| TempDir::new().unwrap()).collect::<Vec<_>>();
        let (mut restored, mut restored_servers) =
            start_runtime_cluster(&restored_directories, Some(Arc::clone(&artifact))).await;
        let restored_catalog_leader = wait_for_catalog_leader(&restored).await;
        wait_for_tablet_count(&restored, 4).await;
        let restored_artifact = capture_artifact(&restored[restored_catalog_leader]).await;
        assert_eq!(artifact_digests(&restored_artifact), expected_digests);
        stop_runtime_cluster(&mut restored, &mut restored_servers).await;

        let peers = (1..=3)
            .map(|node_id| {
                (
                    node_id,
                    Url::parse(&format!("http://127.0.0.1:{}/", 49_000 + node_id)).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let non_fresh = RegionalRuntimeConfig::new(
            ConsensusProbeConfig::new(1, 1, 1, peers, Duration::from_millis(50)).unwrap(),
            restored_directories[0].path(),
            16,
            Arc::new(ManualClock::new(2_000)),
        )
        .with_restore_artifact(Arc::clone(&artifact));
        assert!(matches!(
            RegionalNodeRuntime::start(non_fresh).await,
            Err(RegionalRuntimeError::Restore(_))
        ));

        let (mut reopened, mut reopened_servers) =
            start_runtime_cluster(&restored_directories, None).await;
        let reopened_catalog_leader = wait_for_catalog_leader(&reopened).await;
        wait_for_tablet_count(&reopened, 4).await;
        let reopened_artifact = capture_artifact(&reopened[reopened_catalog_leader]).await;
        assert_eq!(artifact_digests(&reopened_artifact), expected_digests);
        stop_runtime_cluster(&mut reopened, &mut reopened_servers).await;
    }

    fn encrypt_and_authenticate_backup(
        captured: &RegionalBackupArtifact,
    ) -> Arc<RegionalBackupArtifact> {
        let directory = TempDir::new().unwrap();
        let encrypted_path = directory.path().join("all-profiles.epoch-backup.enc");
        let decrypted_path = directory.path().join("all-profiles.json");
        let encryption_key = [42_u8; 32];
        encrypt_backup(
            captured,
            &encryption_key,
            "semantic-restore-key",
            2_000,
            &encrypted_path,
        )
        .unwrap();
        let artifact =
            Arc::new(decrypt_backup(&encrypted_path, &encryption_key, &decrypted_path).unwrap());
        assert_eq!(artifact.manifest_sha256, captured.manifest_sha256);

        let mut tampered = std::fs::read(&encrypted_path).unwrap();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        let tampered_path = directory.path().join("tampered.epoch-backup.enc");
        std::fs::write(&tampered_path, tampered).unwrap();
        assert!(
            decrypt_backup(
                &tampered_path,
                &encryption_key,
                &directory.path().join("tampered.json")
            )
            .is_err()
        );
        artifact
    }

    async fn start_runtime_cluster(
        directories: &[TempDir],
        restore: Option<Arc<RegionalBackupArtifact>>,
    ) -> (Vec<RegionalNodeRuntime>, Vec<JoinHandle<()>>) {
        let listeners = bind_peer_listeners(directories.len()).await;
        let peers = listeners
            .iter()
            .enumerate()
            .map(|(index, listener)| {
                (
                    u64::try_from(index + 1).unwrap(),
                    Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let mut runtimes = Vec::new();
        for (index, directory) in directories.iter().enumerate() {
            let node_id = u64::try_from(index + 1).unwrap();
            let topology = NodeTopology::new(
                node_id,
                "ap-south",
                format!("zone-{}", index + 1),
                "general-purpose",
                [1, 2, 3],
                16,
            )
            .unwrap();
            let mut config = RegionalRuntimeConfig::new(
                ConsensusProbeConfig::new(node_id, 1, 1, peers.clone(), Duration::from_millis(50))
                    .unwrap(),
                directory.path(),
                16,
                Arc::new(ManualClock::new(2_000)),
            )
            .with_topology(topology);
            if let Some(artifact) = restore.as_ref() {
                config = config.with_restore_artifact(Arc::clone(artifact));
            }
            runtimes.push(RegionalNodeRuntime::start(config).await.unwrap());
        }
        let servers = listeners
            .into_iter()
            .zip(&runtimes)
            .map(|(listener, runtime)| {
                let router = runtime.peer_router();
                tokio::spawn(async move { axum::serve(listener, router).await.unwrap() })
            })
            .collect();
        (runtimes, servers)
    }

    async fn wait_for_catalog_leader(runtimes: &[RegionalNodeRuntime]) -> usize {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (index, runtime) in runtimes.iter().enumerate() {
                    if runtime
                        .catalog_state
                        .consensus_handle()
                        .status()
                        .await
                        .unwrap()
                        .role
                        == epoch_consensus::ConsensusRole::Leader
                    {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("catalog should elect a leader")
    }

    async fn wait_for_catalog_learner(runtime: &RegionalNodeRuntime, learner: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let membership = runtime
                    .catalog_state
                    .consensus_handle()
                    .membership()
                    .await
                    .unwrap();
                if membership
                    .learners
                    .iter()
                    .any(|node_id| node_id.get() == learner)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("catalog learner should catch up");
    }

    async fn wait_for_tablet_hosts(runtimes: &[RegionalNodeRuntime], expected: &[bool]) {
        assert_eq!(runtimes.len(), expected.len());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut observed = Vec::with_capacity(runtimes.len());
                for runtime in runtimes {
                    let directory = runtime.materializer.lock().await.directory();
                    observed.push(directory.tablets().is_ok_and(|tablets| !tablets.is_empty()));
                }
                if observed == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("tablet host set should converge");
    }

    async fn wait_for_replacement_finalization(runtimes: &[RegionalNodeRuntime], tablet_id: u64) {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let mut converged = true;
                for (index, runtime) in runtimes.iter().enumerate() {
                    let snapshot = runtime.catalog_state.catalog_snapshot().unwrap();
                    let descriptor = snapshot
                        .resources
                        .iter()
                        .flat_map(|resource| &resource.tablets)
                        .find(|descriptor| descriptor.tablet_id == tablet_id);
                    converged &= descriptor.is_some_and(|descriptor| {
                        descriptor.resource_generation == 1
                            && descriptor.voter_node_ids == [1, 2, 4]
                            && descriptor.bootstrap_voter_node_ids == [1, 2, 3]
                            && descriptor.target_voter_node_ids.is_empty()
                    });

                    let directory = runtime.materializer.lock().await.directory();
                    let route = directory.route(tablet_id).unwrap();
                    let should_host = matches!(index + 1, 1 | 2 | 4);
                    if should_host {
                        let Some(route) = route else {
                            converged = false;
                            continue;
                        };
                        let Ok(membership) = route.consensus().membership().await else {
                            converged = false;
                            continue;
                        };
                        converged &= membership
                            .voters
                            .iter()
                            .map(|node_id| node_id.get())
                            .eq([1, 2, 4])
                            && membership.outgoing_voters.is_empty()
                            && membership.staged_learners.is_empty()
                            && !membership.auto_leave;
                    } else {
                        converged &= route.is_none();
                    }
                }
                if converged {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("learner-first replacement should finalize on every physical node");
    }

    async fn assert_stream_record_visible(runtime: &RegionalNodeRuntime, generation: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let response = runtime
                    .public_router()
                    .oneshot(
                        Request::get(
                            "/experimental/v1/regional/resources/acme/shop/dev/core/stream/orders/shards/0/data/records?offset=0&limit=10",
                        )
                        .header(RESOURCE_GENERATION_HEADER, generation.to_string())
                        .header(TABLET_EPOCH_HEADER, "1")
                        .header(
                            crate::regional_router::READ_CONSISTENCY_HEADER,
                            "local_stale",
                        )
                        .body(Body::empty())
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                if response.status() == StatusCode::OK {
                    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
                    let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    if document["records"]
                        .as_array()
                        .is_some_and(|records| !records.is_empty())
                    {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("replacement voter should retain the committed Stream record");
    }

    async fn wait_for_tablet_count(runtimes: &[RegionalNodeRuntime], expected: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut converged = true;
                for runtime in runtimes {
                    converged &= runtime
                        .materializer
                        .lock()
                        .await
                        .directory()
                        .tablets()
                        .is_ok_and(|tablets| tablets.len() == expected);
                }
                if converged {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("tablet inventory should converge");
    }

    async fn profile_leader(
        runtimes: &[RegionalNodeRuntime],
        kind: ResourceKind,
    ) -> (usize, crate::consensus::ConsensusProbeHandle) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (index, runtime) in runtimes.iter().enumerate() {
                    let routes = runtime
                        .materializer
                        .lock()
                        .await
                        .directory()
                        .routes()
                        .unwrap();
                    for route in routes {
                        let handle = route.consensus();
                        if route.metadata().resource.kind == kind
                            && handle.status().await.unwrap().role
                                == epoch_consensus::ConsensusRole::Leader
                        {
                            return (index, handle);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("profile data group should elect a leader")
    }

    async fn seed_every_profile(runtimes: &[RegionalNodeRuntime]) {
        let envelope = |id: &str| {
            serde_json::json!({
                "id": id,
                "source": "semantic-restore-test",
                "type": "order.created",
                "time_ms": "1000",
                "payload": {"order_id": "A-1"}
            })
        };
        let cases = [
            (
                ResourceKind::Stream,
                "/experimental/v1/regional/resources/acme/shop/dev/core/stream/orders/shards/0/data/records",
                serde_json::json!({
                    "idempotency_key": "restored-stream-1",
                    "partition": 0,
                    "envelope": envelope("restored-stream-1")
                }),
            ),
            (
                ResourceKind::Cache,
                "/experimental/v1/regional/resources/acme/shop/dev/core/cache/sessions/shards/0/data/mutations",
                serde_json::json!({
                    "idempotency_key": "restored-cache-1",
                    "operation": {
                        "kind": "set",
                        "key": "session-A",
                        "value": {"kind": "string", "value": "active"}
                    }
                }),
            ),
            (
                ResourceKind::Queue,
                "/experimental/v1/regional/resources/acme/shop/dev/core/queue/jobs/shards/0/data/mutations",
                serde_json::json!({
                    "idempotency_key": "restored-queue-1",
                    "operation": {
                        "kind": "enqueue",
                        "partition": 0,
                        "envelope": envelope("restored-queue-1")
                    }
                }),
            ),
            (
                ResourceKind::EventBus,
                "/experimental/v1/regional/resources/acme/shop/dev/core/event-bus/events/shards/0/data/mutations",
                serde_json::json!({
                    "idempotency_key": "restored-bus-1",
                    "operation": {
                        "kind": "publish",
                        "envelope": envelope("restored-bus-1")
                    }
                }),
            ),
        ];
        for (kind, path, mut body) in cases {
            let (leader, handle) = profile_leader(runtimes, kind).await;
            body["expected_term"] =
                serde_json::json!(handle.status().await.unwrap().term.get().to_string());
            let response = runtimes[leader]
                .public_router()
                .oneshot(
                    Request::post(path)
                        .header(CONTENT_TYPE, "application/json")
                        .header(RESOURCE_GENERATION_HEADER, "1")
                        .header(TABLET_EPOCH_HEADER, "1")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "failed to seed {kind:?}: {}",
                String::from_utf8_lossy(&to_bytes(response.into_body(), 64 * 1024).await.unwrap())
            );
        }
    }

    async fn capture_artifact(runtime: &RegionalNodeRuntime) -> RegionalBackupArtifact {
        let response = runtime
            .public_router()
            .oneshot(
                Request::post(crate::regional_backup_api::REGIONAL_BACKUP_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let encoded = to_bytes(response.into_body(), 128 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "backup capture failed: {}",
            String::from_utf8_lossy(&encoded)
        );
        RegionalBackupArtifact::decode(&encoded).unwrap()
    }

    fn artifact_digests(artifact: &RegionalBackupArtifact) -> Vec<(u64, String)> {
        artifact
            .groups
            .iter()
            .map(|group| (group.group_id, group.state_sha256.clone()))
            .collect()
    }

    async fn stop_runtime_cluster(
        runtimes: &mut [RegionalNodeRuntime],
        servers: &mut Vec<JoinHandle<()>>,
    ) {
        for server in servers.drain(..) {
            server.abort();
            let _ = server.await;
        }
        for runtime in runtimes {
            runtime.shutdown().await.unwrap();
        }
    }

    async fn bind_peer_listeners(count: usize) -> Vec<tokio::net::TcpListener> {
        let mut listeners = Vec::with_capacity(count);
        for _ in 0..count {
            listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        listeners
    }
}
