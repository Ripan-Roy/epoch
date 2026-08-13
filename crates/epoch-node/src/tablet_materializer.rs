//! Reconciles committed catalog resources into typed local tablet runtimes.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Formatter},
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::Router;
use epoch_bus::BusConfig;
use epoch_catalog::{ResourceName, ResourceRecord, TabletDescriptor};
use epoch_core::{Clock, WorkloadProfile};
use epoch_tablet::{
    BusTabletScope, CacheTabletScope, QueueTabletScope, StreamTabletScope, TabletError,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    bus_tablet::{self, BusTabletService},
    cache_tablet::{self, CacheTabletService},
    consensus::{
        CommittedProposalApplier, ConsensusProbeConfig, ConsensusProbeError, ConsensusProbeHandle,
    },
    consensus_groups::{ConsensusGroupSupervisor, ConsensusGroupSupervisorError},
    queue_tablet::{self, QueueTabletService},
    regional_maintenance::RegionalMaintenanceProposal,
    stream_tablet::{self, StreamTabletService},
};

const SUPPORTED_REPLICA_COUNT: u16 = 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MaterializedTabletMetadata {
    pub resource: ResourceName,
    pub shard_count: u32,
    pub descriptor: TabletDescriptor,
}

#[derive(Clone)]
pub struct MaterializedTabletRoute {
    metadata: MaterializedTabletMetadata,
    router: Router,
    consensus: ConsensusProbeHandle,
    service: PendingTabletService,
}

impl fmt::Debug for MaterializedTabletRoute {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaterializedTabletRoute")
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

impl MaterializedTabletRoute {
    pub const fn metadata(&self) -> &MaterializedTabletMetadata {
        &self.metadata
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    pub fn consensus(&self) -> ConsensusProbeHandle {
        self.consensus.clone()
    }

    pub fn maintenance_proposals(
        &self,
        now_ms: u64,
    ) -> Result<Vec<RegionalMaintenanceProposal>, String> {
        self.service.maintenance_proposals(now_ms)
    }
}

#[derive(Debug, Clone, Error)]
pub enum TabletDirectoryError {
    #[error("tablet directory lock is poisoned")]
    Unavailable,
}

#[derive(Debug, Clone, Default)]
pub struct TabletDirectory {
    routes: Arc<RwLock<BTreeMap<u64, MaterializedTabletRoute>>>,
}

impl TabletDirectory {
    pub fn route(
        &self,
        tablet_id: u64,
    ) -> Result<Option<MaterializedTabletRoute>, TabletDirectoryError> {
        self.routes
            .read()
            .map_err(|_| TabletDirectoryError::Unavailable)
            .map(|routes| routes.get(&tablet_id).cloned())
    }

    pub fn resource_route(
        &self,
        resource: &ResourceName,
        shard_index: u32,
    ) -> Result<Option<MaterializedTabletRoute>, TabletDirectoryError> {
        self.routes
            .read()
            .map_err(|_| TabletDirectoryError::Unavailable)
            .map(|routes| {
                routes
                    .values()
                    .find(|route| {
                        route.metadata.resource == *resource
                            && route.metadata.descriptor.shard_index == shard_index
                    })
                    .cloned()
            })
    }

    pub fn tablets(&self) -> Result<Vec<MaterializedTabletMetadata>, TabletDirectoryError> {
        self.routes
            .read()
            .map_err(|_| TabletDirectoryError::Unavailable)
            .map(|routes| {
                routes
                    .values()
                    .map(|route| route.metadata.clone())
                    .collect()
            })
    }

    pub fn routes(&self) -> Result<Vec<MaterializedTabletRoute>, TabletDirectoryError> {
        self.routes
            .read()
            .map_err(|_| TabletDirectoryError::Unavailable)
            .map(|routes| routes.values().cloned().collect())
    }

    fn snapshot(&self) -> Result<BTreeMap<u64, MaterializedTabletRoute>, TabletDirectoryError> {
        self.routes
            .read()
            .map_err(|_| TabletDirectoryError::Unavailable)
            .map(|routes| routes.clone())
    }

    fn insert(
        &self,
        tablet_id: u64,
        route: MaterializedTabletRoute,
    ) -> Result<(), TabletDirectoryError> {
        self.routes
            .write()
            .map_err(|_| TabletDirectoryError::Unavailable)?
            .insert(tablet_id, route);
        Ok(())
    }

    fn update_metadata(
        &self,
        tablet_id: u64,
        metadata: MaterializedTabletMetadata,
    ) -> Result<(), TabletDirectoryError> {
        let mut routes = self
            .routes
            .write()
            .map_err(|_| TabletDirectoryError::Unavailable)?;
        let route = routes
            .get_mut(&tablet_id)
            .ok_or(TabletDirectoryError::Unavailable)?;
        route.metadata = metadata;
        Ok(())
    }

    fn remove(
        &self,
        tablet_id: u64,
    ) -> Result<Option<MaterializedTabletRoute>, TabletDirectoryError> {
        self.routes
            .write()
            .map_err(|_| TabletDirectoryError::Unavailable)
            .map(|mut routes| routes.remove(&tablet_id))
    }

    fn clear(&self) -> Result<(), TabletDirectoryError> {
        self.routes
            .write()
            .map_err(|_| TabletDirectoryError::Unavailable)?
            .clear();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TabletReconcileOutcome {
    pub started: usize,
    pub updated: usize,
    pub stopped: usize,
    pub unchanged: usize,
}

#[derive(Debug, Error)]
pub enum TabletMaterializerError {
    #[error("invalid catalog tablet set: {0}")]
    InvalidCatalog(String),
    #[error(transparent)]
    Tablet(#[from] TabletError),
    #[error(transparent)]
    Consensus(#[from] ConsensusProbeError),
    #[error(transparent)]
    Supervisor(#[from] ConsensusGroupSupervisorError),
    #[error(transparent)]
    Directory(#[from] TabletDirectoryError),
    #[error("tablet stable directory could not be prepared: {0}")]
    Storage(String),
    #[error("tablet reconciliation rollback failed: {0}")]
    Rollback(String),
}

pub type TabletMaterializerResult<T> = Result<T, TabletMaterializerError>;

#[derive(Clone)]
enum PendingTabletService {
    Cache(Arc<CacheTabletService>),
    Stream(Arc<StreamTabletService>),
    Queue(Arc<QueueTabletService>),
    Bus(Arc<BusTabletService>),
}

impl PendingTabletService {
    fn applier(&self) -> Arc<dyn CommittedProposalApplier> {
        match self {
            Self::Cache(service) => Arc::clone(service) as Arc<dyn CommittedProposalApplier>,
            Self::Stream(service) => Arc::clone(service) as Arc<dyn CommittedProposalApplier>,
            Self::Queue(service) => Arc::clone(service) as Arc<dyn CommittedProposalApplier>,
            Self::Bus(service) => Arc::clone(service) as Arc<dyn CommittedProposalApplier>,
        }
    }

    fn router(
        &self,
        consensus: ConsensusProbeHandle,
        clock: Arc<dyn Clock>,
        commit_wait: Duration,
    ) -> Router {
        match self {
            Self::Cache(service) => {
                cache_tablet::router(Arc::clone(service), consensus, clock, commit_wait)
            }
            Self::Stream(service) => {
                stream_tablet::router(Arc::clone(service), consensus, clock, commit_wait)
            }
            Self::Queue(service) => {
                queue_tablet::router(Arc::clone(service), consensus, clock, commit_wait)
            }
            Self::Bus(service) => {
                bus_tablet::router(Arc::clone(service), consensus, clock, commit_wait)
            }
        }
    }

    fn maintenance_proposals(
        &self,
        now_ms: u64,
    ) -> Result<Vec<RegionalMaintenanceProposal>, String> {
        match self {
            Self::Cache(service) => service.maintenance_proposals(now_ms),
            Self::Stream(service) => service.maintenance_proposals(now_ms),
            Self::Queue(service) => service.maintenance_proposals(now_ms),
            Self::Bus(service) => service.maintenance_proposals(now_ms),
        }
    }
}

/// Owns the node-local tablet supervisor and converges it to catalog state.
pub struct RegionalTabletMaterializer {
    supervisor: ConsensusGroupSupervisor,
    directory: TabletDirectory,
    group_template: ConsensusProbeConfig,
    data_dir: PathBuf,
    clock: Arc<dyn Clock>,
    commit_wait: Duration,
}

impl fmt::Debug for RegionalTabletMaterializer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegionalTabletMaterializer")
            .field("supervisor", &self.supervisor)
            .field("directory", &self.directory)
            .field("group_template", &self.group_template)
            .field("data_dir", &self.data_dir)
            .field("commit_wait", &self.commit_wait)
            .finish_non_exhaustive()
    }
}

impl RegionalTabletMaterializer {
    pub fn new(
        supervisor: ConsensusGroupSupervisor,
        group_template: ConsensusProbeConfig,
        data_dir: impl Into<PathBuf>,
        clock: Arc<dyn Clock>,
        commit_wait: Duration,
    ) -> TabletMaterializerResult<Self> {
        if supervisor.registry().node_id() != group_template.node_id().get() {
            return Err(TabletMaterializerError::InvalidCatalog(format!(
                "group template belongs to node {}; supervisor belongs to node {}",
                group_template.node_id(),
                supervisor.registry().node_id()
            )));
        }
        if commit_wait.is_zero() {
            return Err(TabletMaterializerError::InvalidCatalog(
                "tablet commit wait must be non-zero".into(),
            ));
        }
        Ok(Self {
            supervisor,
            directory: TabletDirectory::default(),
            group_template,
            data_dir: data_dir.into(),
            clock,
            commit_wait,
        })
    }

    pub fn directory(&self) -> TabletDirectory {
        self.directory.clone()
    }

    pub fn peer_registry(&self) -> crate::consensus_groups::ConsensusGroupRegistry {
        self.supervisor.registry()
    }

    pub fn subscribe_group_failures(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::consensus_groups::SupervisedConsensusGroupFailure>
    {
        self.supervisor.subscribe_failures()
    }

    pub async fn reconcile(
        &mut self,
        resources: &[ResourceRecord],
    ) -> TabletMaterializerResult<TabletReconcileOutcome> {
        let desired = validate_desired_tablets(resources)?;
        let current = self.directory.snapshot()?;

        for (tablet_id, route) in &current {
            if let Some(metadata) = desired.get(tablet_id)
                && !same_runtime_identity(&route.metadata, metadata)
            {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "tablet {tablet_id} changed immutable resource, shard, profile, group, or epoch identity"
                )));
            }
        }

        let mut outcome = TabletReconcileOutcome::default();
        for (&tablet_id, metadata) in &desired {
            if !current.contains_key(&tablet_id) {
                self.start_tablet(metadata.clone()).await?;
                outcome.started += 1;
            }
        }

        for (&tablet_id, route) in &current {
            if let Some(metadata) = desired.get(&tablet_id) {
                if route.metadata == *metadata {
                    outcome.unchanged += 1;
                } else {
                    self.directory
                        .update_metadata(tablet_id, metadata.clone())?;
                    outcome.updated += 1;
                }
            }
        }

        for (&tablet_id, route) in &current {
            if !desired.contains_key(&tablet_id) {
                self.directory.remove(tablet_id)?;
                self.supervisor
                    .stop_group(
                        route.metadata.descriptor.consensus_group_id,
                        route.metadata.descriptor.tablet_epoch,
                    )
                    .await?;
                outcome.stopped += 1;
            }
        }
        Ok(outcome)
    }

    async fn start_tablet(
        &mut self,
        metadata: MaterializedTabletMetadata,
    ) -> TabletMaterializerResult<()> {
        let descriptor = &metadata.descriptor;
        let scope = StreamTabletScope::new_with_consensus_group(
            descriptor.tablet_id,
            descriptor.consensus_group_id,
            descriptor.tablet_epoch,
            metadata.resource.name.clone(),
        )?;
        let service = match descriptor.workload_profile {
            WorkloadProfile::CacheAndState => PendingTabletService::Cache(
                CacheTabletService::with_default_config(CacheTabletScope::clone(&scope))?,
            ),
            WorkloadProfile::StreamLog => {
                PendingTabletService::Stream(StreamTabletService::new_for_shard(
                    scope.clone(),
                    descriptor.shard_index,
                    metadata.shard_count,
                )?)
            }
            WorkloadProfile::WorkQueue => PendingTabletService::Queue(
                QueueTabletService::with_default_config(QueueTabletScope::clone(&scope))?,
            ),
            WorkloadProfile::EventBus => PendingTabletService::Bus(BusTabletService::new(
                BusTabletScope::clone(&scope),
                BusConfig {
                    delivery_outbox: true,
                    ..BusConfig::default()
                },
            )?),
        };
        let config = self
            .group_template
            .for_group(descriptor.consensus_group_id, descriptor.tablet_epoch)?;
        let stable_directory = self
            .data_dir
            .join("consensus")
            .join(format!("group-{}", descriptor.consensus_group_id));
        std::fs::create_dir_all(&stable_directory)
            .map_err(|error| TabletMaterializerError::Storage(error.to_string()))?;
        let stable_path = stable_directory.join(format!("node-{}.wal", config.node_id().get()));
        let consensus = self
            .supervisor
            .start_group(config, stable_path, Some(service.applier()))
            .await?;
        let route = MaterializedTabletRoute {
            metadata,
            router: service.router(consensus.clone(), Arc::clone(&self.clock), self.commit_wait),
            consensus,
            service,
        };
        let group_id = route.metadata.descriptor.consensus_group_id;
        let group_epoch = route.metadata.descriptor.tablet_epoch;
        let tablet_id = route.metadata.descriptor.tablet_id;
        if let Err(error) = self.directory.insert(tablet_id, route) {
            let rollback = self.supervisor.stop_group(group_id, group_epoch).await;
            return Err(match rollback {
                Ok(()) => error.into(),
                Err(rollback) => TabletMaterializerError::Rollback(format!(
                    "{error}; group shutdown: {rollback}"
                )),
            });
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> TabletMaterializerResult<()> {
        self.directory.clear()?;
        self.supervisor.shutdown().await?;
        Ok(())
    }
}

fn validate_desired_tablets(
    resources: &[ResourceRecord],
) -> TabletMaterializerResult<BTreeMap<u64, MaterializedTabletMetadata>> {
    let mut desired = BTreeMap::new();
    let mut consensus_groups = BTreeSet::new();
    for resource in resources {
        resource
            .name
            .validate()
            .map_err(|error| TabletMaterializerError::InvalidCatalog(error.to_string()))?;
        let expected_tablet_count = usize::try_from(resource.spec.shard_count).map_err(|_| {
            TabletMaterializerError::InvalidCatalog(format!(
                "{} shard count cannot be represented",
                resource.name.canonical_name()
            ))
        })?;
        if resource.tablets.len() != expected_tablet_count {
            return Err(TabletMaterializerError::InvalidCatalog(format!(
                "{} declares {} shards but has {} tablets",
                resource.name.canonical_name(),
                resource.spec.shard_count,
                resource.tablets.len()
            )));
        }
        for (expected_shard, descriptor) in resource.tablets.iter().enumerate() {
            let expected_shard = u32::try_from(expected_shard).map_err(|_| {
                TabletMaterializerError::InvalidCatalog("shard index exceeds u32".into())
            })?;
            if descriptor.tablet_id == 0
                || descriptor.consensus_group_id == 0
                || descriptor.tablet_epoch == 0
                || resource.generation == 0
            {
                return Err(TabletMaterializerError::InvalidCatalog(
                    "tablet, consensus group, epoch, and generation IDs must be non-zero".into(),
                ));
            }
            if descriptor.shard_index != expected_shard {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "tablet {} has shard {}; expected {expected_shard}",
                    descriptor.tablet_id, descriptor.shard_index
                )));
            }
            if descriptor.resource_generation != resource.generation {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "tablet {} generation {} does not match resource generation {}",
                    descriptor.tablet_id, descriptor.resource_generation, resource.generation
                )));
            }
            if descriptor.workload_profile != resource.spec.workload_profile {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "tablet {} profile does not match its resource",
                    descriptor.tablet_id
                )));
            }
            if descriptor.replica_count != resource.spec.replica_count {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "tablet {} replica count does not match its resource",
                    descriptor.tablet_id
                )));
            }
            if descriptor.replica_count != SUPPORTED_REPLICA_COUNT {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "tablet {} requests {} replicas; the current regional runtime requires {SUPPORTED_REPLICA_COUNT}",
                    descriptor.tablet_id, descriptor.replica_count
                )));
            }
            if !consensus_groups.insert(descriptor.consensus_group_id) {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "consensus group {} is assigned to more than one tablet",
                    descriptor.consensus_group_id
                )));
            }
            if desired
                .insert(
                    descriptor.tablet_id,
                    MaterializedTabletMetadata {
                        resource: resource.name.clone(),
                        shard_count: resource.spec.shard_count,
                        descriptor: descriptor.clone(),
                    },
                )
                .is_some()
            {
                return Err(TabletMaterializerError::InvalidCatalog(format!(
                    "tablet {} is assigned more than once",
                    descriptor.tablet_id
                )));
            }
        }
    }
    Ok(desired)
}

fn same_runtime_identity(
    current: &MaterializedTabletMetadata,
    desired: &MaterializedTabletMetadata,
) -> bool {
    current.resource == desired.resource
        && current.descriptor.tablet_id == desired.descriptor.tablet_id
        && current.descriptor.consensus_group_id == desired.descriptor.consensus_group_id
        && current.descriptor.shard_index == desired.descriptor.shard_index
        && current.descriptor.tablet_epoch == desired.descriptor.tablet_epoch
        && current.descriptor.workload_profile == desired.descriptor.workload_profile
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use axum::{body::Body, http::Request};
    use epoch_catalog::{ApplyResource, CatalogCommand, DeleteResource, ResourceSpec};
    use epoch_consensus::{
        CommitReceipt, CommittedProposal, GroupEpoch, GroupId, LogIndex, ProposalId, Term,
    };
    use epoch_core::{ManualClock, ResourceKind};
    use tempfile::TempDir;
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::catalog_tablet::{CatalogTabletScope, CatalogTabletService};

    fn peer_url(port: u16) -> Url {
        Url::parse(&format!("http://127.0.0.1:{port}/")).expect("test peer URL should parse")
    }

    fn group_template() -> ConsensusProbeConfig {
        ConsensusProbeConfig::new(
            2,
            900,
            1,
            [
                (1, peer_url(40_001)),
                (2, peer_url(40_002)),
                (3, peer_url(40_003)),
            ],
            Duration::from_mins(1),
        )
        .expect("group template should be valid")
    }

    fn resource_command(
        token: &str,
        kind: ResourceKind,
        name: &str,
        profile: WorkloadProfile,
        shards: u32,
        expected_generation: Option<u64>,
    ) -> CatalogCommand {
        CatalogCommand::Apply(ApplyResource {
            request_token: token.into(),
            expected_generation,
            name: ResourceName::new("acme", "shop", "dev", "core", kind, name)
                .expect("resource name should be valid"),
            spec: ResourceSpec {
                workload_profile: profile,
                shard_count: shards,
                replica_count: SUPPORTED_REPLICA_COUNT,
            },
        })
    }

    fn committed(index: u64, command: &CatalogCommand) -> CommittedProposal {
        CommittedProposal {
            receipt: CommitReceipt {
                group_id: GroupId::new(900).unwrap(),
                group_epoch: GroupEpoch::new(1).unwrap(),
                proposal_id: ProposalId::new(1_000 + index).unwrap(),
                term: Term::new(1),
                log_index: LogIndex::new(index),
            },
            payload: command.encode().expect("command should encode"),
        }
    }

    fn catalog_with_all_profiles() -> Arc<CatalogTabletService> {
        let catalog = CatalogTabletService::new(CatalogTabletScope::new(900, 1).unwrap());
        for (index, command) in [
            resource_command(
                "cache-v1",
                ResourceKind::Cache,
                "sessions",
                WorkloadProfile::CacheAndState,
                1,
                None,
            ),
            resource_command(
                "stream-v1",
                ResourceKind::Stream,
                "orders",
                WorkloadProfile::StreamLog,
                1,
                None,
            ),
            resource_command(
                "queue-v1",
                ResourceKind::Queue,
                "jobs",
                WorkloadProfile::WorkQueue,
                1,
                None,
            ),
            resource_command(
                "bus-v1",
                ResourceKind::EventBus,
                "events",
                WorkloadProfile::EventBus,
                1,
                None,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            catalog
                .apply(&committed(
                    u64::try_from(index + 1).expect("index fits"),
                    &command,
                ))
                .expect("catalog command should apply");
        }
        catalog
    }

    fn new_materializer(data_dir: &Path) -> TabletMaterializerResult<RegionalTabletMaterializer> {
        RegionalTabletMaterializer::new(
            ConsensusGroupSupervisor::new(2, 16).expect("supervisor should be valid"),
            group_template(),
            data_dir,
            Arc::new(ManualClock::new(1_000)),
            Duration::from_secs(1),
        )
    }

    #[tokio::test]
    async fn committed_catalog_materializes_updates_deletes_and_recovers_all_profiles() {
        let directory = TempDir::new().expect("temp directory should be created");
        let catalog = catalog_with_all_profiles();
        let snapshot = catalog.snapshot().expect("catalog should be healthy");
        let mut materializer =
            new_materializer(directory.path()).expect("materializer should be valid");

        let initial = materializer
            .reconcile(&snapshot.resources)
            .await
            .expect("all profile groups should materialize");
        assert_eq!(
            initial,
            TabletReconcileOutcome {
                started: 4,
                ..TabletReconcileOutcome::default()
            }
        );
        assert_eq!(materializer.directory().tablets().unwrap().len(), 4);
        assert_profile_status_routes(&materializer.directory()).await;

        let unchanged = materializer
            .reconcile(&snapshot.resources)
            .await
            .expect("reconciliation should be idempotent");
        assert_eq!(
            unchanged,
            TabletReconcileOutcome {
                unchanged: 4,
                ..TabletReconcileOutcome::default()
            }
        );

        materializer
            .shutdown()
            .await
            .expect("first runtime should stop cleanly");
        let mut recovered =
            new_materializer(directory.path()).expect("recovered materializer should be valid");
        let recovery = recovered
            .reconcile(&snapshot.resources)
            .await
            .expect("fresh supervisor should reopen every profile tablet");
        assert_eq!(recovery.started, 4);
        assert_profile_status_routes(&recovered.directory()).await;

        catalog
            .apply(&committed(
                5,
                &resource_command(
                    "stream-expand-v2",
                    ResourceKind::Stream,
                    "orders",
                    WorkloadProfile::StreamLog,
                    2,
                    Some(1),
                ),
            ))
            .expect("stream should expand");
        let queue_name =
            ResourceName::new("acme", "shop", "dev", "core", ResourceKind::Queue, "jobs").unwrap();
        catalog
            .apply(&committed(
                6,
                &CatalogCommand::Delete(DeleteResource {
                    request_token: "queue-delete-v2".into(),
                    expected_generation: Some(1),
                    name: queue_name,
                }),
            ))
            .expect("queue should delete");
        let changed_snapshot = catalog.snapshot().expect("catalog should remain healthy");
        let changed = recovered
            .reconcile(&changed_snapshot.resources)
            .await
            .expect("expansion and deletion should converge");
        assert_eq!(changed.started, 1);
        assert_eq!(changed.updated, 1);
        assert_eq!(changed.stopped, 1);
        assert_eq!(changed.unchanged, 2);
        assert_eq!(recovered.directory().tablets().unwrap().len(), 4);
        assert_stream_shards(&recovered.directory(), 2);
        recovered
            .shutdown()
            .await
            .expect("recovered runtime should stop cleanly");

        let mut expanded_recovery =
            new_materializer(directory.path()).expect("expanded recovery should be valid");
        let reopened = expanded_recovery
            .reconcile(&changed_snapshot.resources)
            .await
            .expect("every expanded Stream shard should reopen");
        assert_eq!(reopened.started, 4);
        assert_stream_shards(&expanded_recovery.directory(), 2);
        expanded_recovery
            .shutdown()
            .await
            .expect("expanded recovery should stop cleanly");
    }

    fn assert_stream_shards(directory: &TabletDirectory, expected: u32) {
        let mut stream = directory
            .tablets()
            .expect("directory should be readable")
            .into_iter()
            .filter(|metadata| metadata.descriptor.workload_profile == WorkloadProfile::StreamLog)
            .collect::<Vec<_>>();
        stream.sort_by_key(|metadata| metadata.descriptor.shard_index);
        assert_eq!(stream.len(), usize::try_from(expected).unwrap());
        assert!(
            stream
                .iter()
                .all(|metadata| metadata.shard_count == expected)
        );
        assert_eq!(
            stream
                .iter()
                .map(|metadata| metadata.descriptor.shard_index)
                .collect::<Vec<_>>(),
            (0..expected).collect::<Vec<_>>()
        );
    }

    async fn assert_profile_status_routes(directory: &TabletDirectory) {
        let routes = directory.snapshot().expect("directory should be available");
        let mut observed = BTreeSet::new();
        for route in routes.values() {
            let (profile, path) = match route.metadata.descriptor.workload_profile {
                WorkloadProfile::CacheAndState => (
                    WorkloadProfile::CacheAndState,
                    cache_tablet::EXPERIMENTAL_CACHE_TABLET_STATUS_PATH,
                ),
                WorkloadProfile::StreamLog => (
                    WorkloadProfile::StreamLog,
                    stream_tablet::EXPERIMENTAL_STREAM_TABLET_STATUS_PATH,
                ),
                WorkloadProfile::WorkQueue => (
                    WorkloadProfile::WorkQueue,
                    queue_tablet::EXPERIMENTAL_QUEUE_TABLET_STATUS_PATH,
                ),
                WorkloadProfile::EventBus => (
                    WorkloadProfile::EventBus,
                    bus_tablet::EXPERIMENTAL_BUS_TABLET_STATUS_PATH,
                ),
            };
            let response = route
                .router()
                .oneshot(
                    Request::get(path)
                        .body(Body::empty())
                        .expect("request should build"),
                )
                .await
                .expect("profile router should answer");
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            observed.insert(profile);
        }
        assert_eq!(
            observed,
            BTreeSet::from([
                WorkloadProfile::CacheAndState,
                WorkloadProfile::StreamLog,
                WorkloadProfile::WorkQueue,
                WorkloadProfile::EventBus,
            ])
        );
    }
}
