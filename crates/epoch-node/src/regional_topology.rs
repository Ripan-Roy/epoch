//! Truthful node-local topology and consensus-group capacity reporting.

use std::collections::BTreeSet;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    regional_maintenance::RegionalMaintenanceStatus, tablet_materializer::TabletDirectory,
};

pub const REGIONAL_TOPOLOGY_PATH: &str = "/experimental/v1/regional/topology";
const MAX_TOPOLOGY_LABEL_BYTES: usize = 63;

/// Immutable placement identity for one regional node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTopology {
    node_id: u64,
    region: String,
    zone: String,
    node_class: String,
    consensus_voter_node_ids: [u64; 3],
    max_consensus_groups: usize,
}

/// Invalid or ambiguous topology configuration.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("invalid regional topology: {0}")]
pub struct NodeTopologyError(String);

impl NodeTopology {
    /// Builds a bounded topology record for the fixed-voter runtime.
    pub fn new(
        node_id: u64,
        region: impl Into<String>,
        zone: impl Into<String>,
        node_class: impl Into<String>,
        mut consensus_voter_node_ids: [u64; 3],
        max_consensus_groups: usize,
    ) -> Result<Self, NodeTopologyError> {
        if node_id == 0 {
            return Err(NodeTopologyError("node ID must be non-zero".into()));
        }
        let region = validate_label("region", region.into())?;
        let zone = validate_label("zone", zone.into())?;
        let node_class = validate_label("node class", node_class.into())?;
        consensus_voter_node_ids.sort_unstable();
        let voters = consensus_voter_node_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        if voters.len() != 3 || voters.contains(&0) {
            return Err(NodeTopologyError(
                "consensus voters must contain three distinct non-zero node IDs".into(),
            ));
        }
        if !voters.contains(&node_id) {
            return Err(NodeTopologyError(
                "local node must belong to the fixed consensus voter set".into(),
            ));
        }
        if max_consensus_groups == 0 {
            return Err(NodeTopologyError(
                "consensus-group capacity must be non-zero".into(),
            ));
        }
        Ok(Self {
            node_id,
            region,
            zone,
            node_class,
            consensus_voter_node_ids,
            max_consensus_groups,
        })
    }

    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn zone(&self) -> &str {
        &self.zone
    }

    pub fn node_class(&self) -> &str {
        &self.node_class
    }

    pub const fn consensus_voter_node_ids(&self) -> [u64; 3] {
        self.consensus_voter_node_ids
    }

    pub const fn max_consensus_groups(&self) -> usize {
        self.max_consensus_groups
    }
}

#[derive(Debug, Clone)]
struct TopologyState {
    topology: NodeTopology,
    directory: TabletDirectory,
    maintenance: std::sync::Arc<RegionalMaintenanceStatus>,
}

#[derive(Debug, Serialize)]
struct TopologyResponse {
    node_id: String,
    region: String,
    zone: String,
    node_class: String,
    consensus_voter_node_ids: Vec<String>,
    capacity: CapacityResponse,
    maintenance: crate::regional_maintenance::RegionalMaintenanceStatusSnapshot,
}

#[derive(Debug, Serialize)]
struct CapacityResponse {
    #[serde(rename = "max_consensus_groups")]
    maximum: usize,
    #[serde(rename = "used_consensus_groups")]
    used: usize,
    #[serde(rename = "available_consensus_groups")]
    available: usize,
}

#[derive(Debug, Serialize)]
struct TopologyErrorResponse {
    code: &'static str,
    message: &'static str,
    retryable: bool,
}

/// Exposes immutable node identity plus a live count of supervised groups.
pub fn regional_topology_router(
    topology: NodeTopology,
    directory: TabletDirectory,
    maintenance: std::sync::Arc<RegionalMaintenanceStatus>,
) -> Router {
    Router::new()
        .route(REGIONAL_TOPOLOGY_PATH, get(get_topology))
        .with_state(TopologyState {
            topology,
            directory,
            maintenance,
        })
}

async fn get_topology(State(state): State<TopologyState>) -> Response {
    let Ok(data_groups) = state.directory.tablets().map(|tablets| tablets.len()) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(TopologyErrorResponse {
                code: "topology_unavailable",
                message: "regional tablet directory is unavailable",
                retryable: true,
            }),
        )
            .into_response();
    };
    // The catalog group is supervised for the entire regional runtime lifetime.
    let used = data_groups.saturating_add(1);
    let maximum = state.topology.max_consensus_groups;
    Json(TopologyResponse {
        node_id: state.topology.node_id.to_string(),
        region: state.topology.region,
        zone: state.topology.zone,
        node_class: state.topology.node_class,
        consensus_voter_node_ids: state
            .topology
            .consensus_voter_node_ids
            .into_iter()
            .map(|node_id| node_id.to_string())
            .collect(),
        capacity: CapacityResponse {
            maximum,
            used,
            available: maximum.saturating_sub(used),
        },
        maintenance: state.maintenance.snapshot(),
    })
    .into_response()
}

fn validate_label(label: &str, value: String) -> Result<String, NodeTopologyError> {
    if value.is_empty()
        || value.len() > MAX_TOPOLOGY_LABEL_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        || !value.as_bytes()[0].is_ascii_alphanumeric()
    {
        return Err(NodeTopologyError(format!(
            "{label} must be a 1-{MAX_TOPOLOGY_LABEL_BYTES} byte identifier"
        )));
    }
    Ok(value)
}
