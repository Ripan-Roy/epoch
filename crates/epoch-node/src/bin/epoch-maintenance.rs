//! Guarded node verification and leadership drain for managed rolling upgrades.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{Parser, Subcommand};
use epoch_node::{
    regional_maintenance_api::{
        INTERNAL_MAINTENANCE_GROUPS_PATH, MaintenanceGroup, MaintenanceInventory, MaintenanceRole,
    },
    transport_security::{ClientTlsFiles, configure_client_builder},
};
use serde::{Deserialize, Serialize};
use url::Url;

const MAX_ENDPOINTS: usize = 1_024;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_RESPONSE_BYTES_U64: u64 = 16 * 1024 * 1024;
const MAX_ROUNDS: usize = 1_000;

#[derive(Debug, Parser)]
#[command(
    name = "epoch-maintenance",
    version,
    about = "Verify or safely drain one Epoch node over the internal mTLS API"
)]
struct Cli {
    #[arg(long, env = "EPOCH_MAINTENANCE_ENDPOINTS", value_delimiter = ',')]
    endpoints: Vec<Url>,
    #[arg(long, env = "EPOCH_MAINTENANCE_NODE_ID")]
    node_id: u64,
    #[arg(long, env = "EPOCH_MAINTENANCE_TLS_CA_PATH")]
    tls_ca: PathBuf,
    #[arg(long, env = "EPOCH_MAINTENANCE_TLS_CERT_PATH")]
    tls_certificate: PathBuf,
    #[arg(long, env = "EPOCH_MAINTENANCE_TLS_KEY_PATH")]
    tls_private_key: PathBuf,
    #[arg(long, env = "EPOCH_MAINTENANCE_ROUNDS", default_value_t = 40)]
    rounds: usize,
    #[arg(long, env = "EPOCH_MAINTENANCE_STATUS_PATH")]
    status_path: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum Command {
    Verify,
    Drain,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MaintenanceReceipt {
    state: &'static str,
    operation: &'static str,
    target_node_id: u64,
    observed_node_ids: Vec<u64>,
    groups_checked: usize,
    leadership_transfers: usize,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct TransferLeadershipRequest {
    group_epoch: u64,
    expected_term: u64,
    target_node_id: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferLeadershipResponse {
    state: String,
    group_id: u64,
    group_epoch: u64,
    expected_term: u64,
    target_node_id: u64,
}

#[derive(Debug, Clone, Copy)]
struct VerifiedGroup {
    group_id: u64,
    group_epoch: u64,
    leader_id: u64,
    leader_term: u64,
    transfer_target: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    validate_args(&cli)?;
    let client = configure_client_builder(
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20)),
        &ClientTlsFiles {
            ca: cli.tls_ca.clone(),
            certificate: cli.tls_certificate.clone(),
            private_key: cli.tls_private_key.clone(),
        },
    )?
    .build()?;

    let receipt = match cli.command {
        Command::Verify => verify_until_ready(&client, &cli).await?,
        Command::Drain => drain_until_ready(&client, &cli).await?,
    };
    emit_receipt(&receipt, cli.status_path.as_deref())?;
    Ok(())
}

fn validate_args(cli: &Cli) -> Result<(), Box<dyn Error>> {
    if cli.node_id == 0 {
        return Err("maintenance node ID must be non-zero".into());
    }
    if cli.endpoints.is_empty() || cli.endpoints.len() > MAX_ENDPOINTS {
        return Err(format!("maintenance requires 1..={MAX_ENDPOINTS} endpoints").into());
    }
    if cli.rounds == 0 || cli.rounds > MAX_ROUNDS {
        return Err(format!("maintenance rounds must be between 1 and {MAX_ROUNDS}").into());
    }
    let mut authorities = BTreeSet::new();
    for endpoint in &cli.endpoints {
        validate_endpoint(endpoint)?;
        if !authorities.insert(endpoint.as_str()) {
            return Err(format!("duplicate maintenance endpoint {endpoint}").into());
        }
    }
    Ok(())
}

fn validate_endpoint(endpoint: &Url) -> Result<(), Box<dyn Error>> {
    if endpoint.scheme() != "https"
        || endpoint.cannot_be_a_base()
        || endpoint.host_str().is_none()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !matches!(endpoint.path(), "" | "/")
    {
        return Err(
            format!("maintenance endpoint must be an HTTPS authority URL: {endpoint}").into(),
        );
    }
    Ok(())
}

async fn verify_until_ready(
    client: &reqwest::Client,
    cli: &Cli,
) -> Result<MaintenanceReceipt, Box<dyn Error>> {
    let mut last_error = String::new();
    for round in 1..=cli.rounds {
        match fetch_and_verify(client, &cli.endpoints, cli.node_id).await {
            Ok((inventories, groups)) => {
                return Ok(receipt(
                    "verified",
                    "verify",
                    cli.node_id,
                    &inventories,
                    &groups,
                    0,
                ));
            }
            Err(error) => last_error = error.to_string(),
        }
        pause_between_rounds(round, cli.rounds).await;
    }
    Err(format!(
        "node {} did not become upgrade-ready after {} rounds: {last_error}",
        cli.node_id, cli.rounds
    )
    .into())
}

async fn drain_until_ready(
    client: &reqwest::Client,
    cli: &Cli,
) -> Result<MaintenanceReceipt, Box<dyn Error>> {
    let mut transfer_count = 0usize;
    let mut last_error = String::new();
    for round in 1..=cli.rounds {
        match fetch_and_verify(client, &cli.endpoints, cli.node_id).await {
            Ok((inventories, groups)) => {
                let leaders = groups
                    .iter()
                    .copied()
                    .filter(|group| group.leader_id == cli.node_id)
                    .collect::<Vec<_>>();
                if leaders.is_empty() {
                    return Ok(receipt(
                        "drained",
                        "drain",
                        cli.node_id,
                        &inventories,
                        &groups,
                        transfer_count,
                    ));
                }
                if let Err(error) = transfer_leaders(client, &cli.endpoints, &leaders).await {
                    last_error = error.to_string();
                } else {
                    transfer_count = transfer_count.saturating_add(leaders.len());
                    last_error = "leadership transfer is still converging".into();
                }
            }
            Err(error) => last_error = error.to_string(),
        }
        pause_between_rounds(round, cli.rounds).await;
    }
    Err(format!(
        "node {} did not drain after {} rounds: {last_error}",
        cli.node_id, cli.rounds
    )
    .into())
}

async fn pause_between_rounds(round: usize, rounds: usize) {
    if round < rounds {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn fetch_and_verify(
    client: &reqwest::Client,
    endpoints: &[Url],
    target_node_id: u64,
) -> Result<(BTreeMap<u64, MaintenanceInventory>, Vec<VerifiedGroup>), Box<dyn Error>> {
    let inventories = fetch_inventories(client, endpoints).await?;
    let groups = verify_inventories(&inventories, target_node_id)?;
    Ok((inventories, groups))
}

async fn fetch_inventories(
    client: &reqwest::Client,
    endpoints: &[Url],
) -> Result<BTreeMap<u64, MaintenanceInventory>, Box<dyn Error>> {
    let mut inventories = BTreeMap::new();
    for endpoint in endpoints {
        let url = endpoint.join(INTERNAL_MAINTENANCE_GROUPS_PATH.trim_start_matches('/'))?;
        let response = client.get(url.clone()).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = bounded_response(response).await.unwrap_or_default();
            return Err(format!(
                "maintenance inventory {url} returned {status}: {}",
                String::from_utf8_lossy(&body)
            )
            .into());
        }
        let body = bounded_response(response).await?;
        let inventory: MaintenanceInventory = serde_json::from_slice(&body)?;
        if inventories.insert(inventory.node_id, inventory).is_some() {
            return Err("maintenance endpoints returned a duplicate node identity".into());
        }
    }
    Ok(inventories)
}

async fn bounded_response(mut response: reqwest::Response) -> Result<Vec<u8>, Box<dyn Error>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES_U64)
    {
        return Err("maintenance response exceeds the size limit".into());
    }
    let mut encoded = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if encoded.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err("maintenance response exceeds the size limit".into());
        }
        encoded.extend_from_slice(&chunk);
    }
    Ok(encoded)
}

fn verify_inventories(
    inventories: &BTreeMap<u64, MaintenanceInventory>,
    target_node_id: u64,
) -> Result<Vec<VerifiedGroup>, Box<dyn Error>> {
    inventories
        .get(&target_node_id)
        .ok_or_else(|| format!("target node {target_node_id} did not return an inventory"))?;
    validate_inventory_set(inventories)?;

    let mut canonical_groups = BTreeMap::new();
    for inventory in inventories.values() {
        for group in &inventory.groups {
            if let Some(existing) = canonical_groups.insert(group.group_id, group)
                && existing.group_epoch != group.group_epoch
            {
                return Err(format!(
                    "group {} is observed at epochs {} and {}",
                    group.group_id, existing.group_epoch, group.group_epoch
                )
                .into());
            }
        }
    }
    let mut verified = Vec::with_capacity(canonical_groups.len());
    for group in canonical_groups.values() {
        verified.push(verify_group(inventories, group)?);
    }
    Ok(verified)
}

fn validate_inventory_set(
    inventories: &BTreeMap<u64, MaintenanceInventory>,
) -> Result<(), Box<dyn Error>> {
    for (&node_id, inventory) in inventories {
        if node_id == 0 || inventory.node_id != node_id {
            return Err(format!("invalid maintenance inventory identity {node_id}").into());
        }
        let mut previous = None;
        for group in &inventory.groups {
            if group.group_id == 0 || group.group_epoch == 0 {
                return Err(format!("node {node_id} returned an invalid group identity").into());
            }
            if previous.is_some_and(|value| value >= group.group_id) {
                return Err(format!("node {node_id} returned unsorted or duplicate groups").into());
            }
            previous = Some(group.group_id);
        }
    }
    Ok(())
}

fn verify_group(
    inventories: &BTreeMap<u64, MaintenanceInventory>,
    target_group: &MaintenanceGroup,
) -> Result<VerifiedGroup, Box<dyn Error>> {
    validate_stable_membership(target_group)?;
    let mut observations = Vec::with_capacity(target_group.membership.voters.len());
    for voter in &target_group.membership.voters {
        let inventory = inventories.get(voter).ok_or_else(|| {
            format!(
                "group {} voter {voter} has no live maintenance endpoint",
                target_group.group_id
            )
        })?;
        let group = find_group(inventory, target_group.group_id, target_group.group_epoch)?;
        if group.membership != target_group.membership {
            return Err(format!(
                "group {} membership disagrees across voter observations at node {voter}",
                target_group.group_id
            )
            .into());
        }
        if group.fail_stopped {
            return Err(format!(
                "group {} is fail-stopped on node {voter}",
                target_group.group_id
            )
            .into());
        }
        observations.push((*voter, group));
    }

    let leaders = observations
        .iter()
        .filter(|(_, group)| group.role == MaintenanceRole::Leader)
        .collect::<Vec<_>>();
    if leaders.len() != 1 {
        return Err(format!(
            "group {} has {} observed leaders instead of one",
            target_group.group_id,
            leaders.len()
        )
        .into());
    }
    let leader_id = leaders[0].0;
    let leader = leaders[0].1;
    if leader.leader_id != Some(leader_id) {
        return Err(format!(
            "group {} leader identity is inconsistent",
            target_group.group_id
        )
        .into());
    }
    for (voter, group) in &observations {
        if group.leader_id != Some(leader_id) || group.term != leader.term {
            return Err(format!(
                "group {} node {voter} has a stale leader or term view",
                target_group.group_id
            )
            .into());
        }
        if group.applied_index < leader.commit_index || group.checkpoint_index > group.applied_index
        {
            return Err(format!(
                "group {} voter {voter} has not applied through leader commit {}",
                target_group.group_id, leader.commit_index
            )
            .into());
        }
    }

    let transfer_target = verify_replication_progress(leader_id, leader)?;

    Ok(VerifiedGroup {
        group_id: target_group.group_id,
        group_epoch: target_group.group_epoch,
        leader_id,
        leader_term: leader.term,
        transfer_target,
    })
}

fn validate_stable_membership(group: &MaintenanceGroup) -> Result<(), Box<dyn Error>> {
    let membership = &group.membership;
    if !matches!(membership.voters.len(), 3 | 5)
        || !strictly_sorted_unique(&membership.voters)
        || !strictly_sorted_unique(&membership.allowed_members)
        || !strictly_sorted_unique_or_empty(&membership.learners)
        || !membership
            .voters
            .iter()
            .chain(&membership.learners)
            .all(|node_id| membership.allowed_members.binary_search(node_id).is_ok())
        || membership
            .voters
            .iter()
            .any(|node_id| membership.learners.binary_search(node_id).is_ok())
        || !membership.outgoing_voters.is_empty()
        || !membership.staged_learners.is_empty()
        || membership.auto_leave
    {
        return Err(format!(
            "group {} does not have a stable three/five-voter membership",
            group.group_id
        )
        .into());
    }
    Ok(())
}

fn strictly_sorted_unique_or_empty(values: &[u64]) -> bool {
    values.is_empty() || strictly_sorted_unique(values)
}

fn strictly_sorted_unique(values: &[u64]) -> bool {
    values.iter().all(|value| *value != 0) && values.windows(2).all(|window| window[0] < window[1])
}

fn verify_replication_progress(
    leader_id: u64,
    leader: &MaintenanceGroup,
) -> Result<Option<u64>, Box<dyn Error>> {
    let progress = leader
        .replication_progress
        .iter()
        .map(|peer| (peer.node_id, peer))
        .collect::<BTreeMap<_, _>>();
    let mut candidates = Vec::new();
    for voter in &leader.membership.voters {
        let peer = progress.get(voter).ok_or_else(|| {
            format!(
                "group {} leader has no progress for voter {voter}",
                leader.group_id
            )
        })?;
        if peer.matched_index < leader.commit_index
            || peer.committed_index < leader.commit_index
            || peer.pending_snapshot_index != 0
            || (*voter != leader_id && !peer.recent_active)
        {
            return Err(format!("group {} voter {voter} is not caught up", leader.group_id).into());
        }
        if *voter != leader_id {
            candidates.push((peer.matched_index, *voter));
        }
    }
    candidates.sort_unstable_by(|left, right| right.0.cmp(&left.0).then(left.1.cmp(&right.1)));
    Ok(candidates.first().map(|(_, node_id)| *node_id))
}

fn find_group(
    inventory: &MaintenanceInventory,
    group_id: u64,
    group_epoch: u64,
) -> Result<&MaintenanceGroup, Box<dyn Error>> {
    let group = inventory
        .groups
        .binary_search_by_key(&group_id, |group| group.group_id)
        .ok()
        .and_then(|index| inventory.groups.get(index))
        .ok_or_else(|| format!("node {} does not host group {group_id}", inventory.node_id))?;
    if group.group_epoch != group_epoch {
        return Err(format!(
            "node {} group {group_id} epoch {} is fenced by expected epoch {group_epoch}",
            inventory.node_id, group.group_epoch
        )
        .into());
    }
    Ok(group)
}

async fn transfer_leaders(
    client: &reqwest::Client,
    endpoints: &[Url],
    leaders: &[VerifiedGroup],
) -> Result<(), Box<dyn Error>> {
    for group in leaders {
        let target_node_id = group.transfer_target.ok_or_else(|| {
            format!(
                "group {} has no safe leadership transfer target",
                group.group_id
            )
        })?;
        let request = TransferLeadershipRequest {
            group_epoch: group.group_epoch,
            expected_term: group.leader_term,
            target_node_id,
        };
        let mut accepted = false;
        let mut failures = Vec::new();
        for endpoint in endpoints {
            let url = endpoint.join(&format!(
                "internal/v1/maintenance/groups/{}/leadership",
                group.group_id
            ))?;
            let response = client.post(url.clone()).json(&request).send().await?;
            if response.status() == reqwest::StatusCode::ACCEPTED {
                let body = bounded_response(response).await?;
                let receipt: TransferLeadershipResponse = serde_json::from_slice(&body)?;
                validate_transfer_receipt(&receipt, group, target_node_id)?;
                accepted = true;
                break;
            }
            let status = response.status();
            let body = bounded_response(response).await.unwrap_or_default();
            failures.push(format!(
                "{url} returned {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }
        if !accepted {
            return Err(format!(
                "group {} leadership transfer was rejected by every endpoint: {}",
                group.group_id,
                failures.join("; ")
            )
            .into());
        }
    }
    Ok(())
}

fn validate_transfer_receipt(
    receipt: &TransferLeadershipResponse,
    group: &VerifiedGroup,
    target_node_id: u64,
) -> Result<(), Box<dyn Error>> {
    if receipt.state != "initiated"
        || receipt.group_id != group.group_id
        || receipt.group_epoch != group.group_epoch
        || receipt.expected_term != group.leader_term
        || receipt.target_node_id != target_node_id
    {
        return Err(format!(
            "group {} returned an invalid transfer receipt",
            group.group_id
        )
        .into());
    }
    Ok(())
}

fn receipt(
    state: &'static str,
    operation: &'static str,
    target_node_id: u64,
    inventories: &BTreeMap<u64, MaintenanceInventory>,
    groups: &[VerifiedGroup],
    leadership_transfers: usize,
) -> MaintenanceReceipt {
    MaintenanceReceipt {
        state,
        operation,
        target_node_id,
        observed_node_ids: inventories.keys().copied().collect(),
        groups_checked: groups.len(),
        leadership_transfers,
    }
}

fn emit_receipt(receipt: &MaintenanceReceipt, path: Option<&Path>) -> Result<(), Box<dyn Error>> {
    let encoded = serde_json::to_vec(receipt)?;
    if let Some(path) = path {
        fs::write(path, &encoded)?;
    }
    println!("{}", String::from_utf8(encoded)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use epoch_node::regional_maintenance_api::{MaintenanceMembership, MaintenancePeerProgress};

    use super::*;

    fn membership() -> MaintenanceMembership {
        MaintenanceMembership {
            allowed_members: vec![1, 2, 3],
            voters: vec![1, 2, 3],
            outgoing_voters: Vec::new(),
            learners: Vec::new(),
            staged_learners: Vec::new(),
            auto_leave: false,
        }
    }

    fn group(node_id: u64, leader_id: u64) -> MaintenanceGroup {
        MaintenanceGroup {
            group_id: 41,
            group_epoch: 7,
            role: if node_id == leader_id {
                MaintenanceRole::Leader
            } else {
                MaintenanceRole::Follower
            },
            leader_id: Some(leader_id),
            term: 9,
            commit_index: 12,
            applied_index: 12,
            checkpoint_index: 8,
            fail_stopped: false,
            membership: membership(),
            replication_progress: if node_id == leader_id {
                vec![
                    MaintenancePeerProgress {
                        node_id: 1,
                        matched_index: 12,
                        committed_index: 12,
                        pending_snapshot_index: 0,
                        recent_active: true,
                    },
                    MaintenancePeerProgress {
                        node_id: 2,
                        matched_index: 14,
                        committed_index: 12,
                        pending_snapshot_index: 0,
                        recent_active: true,
                    },
                    MaintenancePeerProgress {
                        node_id: 3,
                        matched_index: 13,
                        committed_index: 12,
                        pending_snapshot_index: 0,
                        recent_active: true,
                    },
                ]
            } else {
                Vec::new()
            },
        }
    }

    fn inventories() -> BTreeMap<u64, MaintenanceInventory> {
        (1..=3)
            .map(|node_id| {
                (
                    node_id,
                    MaintenanceInventory {
                        node_id,
                        groups: vec![group(node_id, 1)],
                    },
                )
            })
            .collect()
    }

    #[test]
    fn healthy_group_is_verified_and_selects_most_caught_up_transfer_target() {
        let verified = verify_inventories(&inventories(), 1).expect("healthy group");
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].leader_id, 1);
        assert_eq!(verified[0].transfer_target, Some(2));
    }

    #[test]
    fn verification_covers_cluster_groups_not_hosted_by_target_node() {
        let mut observed = inventories();
        observed.insert(
            4,
            MaintenanceInventory {
                node_id: 4,
                groups: Vec::new(),
            },
        );
        let verified = verify_inventories(&observed, 4).expect("cluster-wide verification");
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].group_id, 41);
    }

    #[test]
    fn joint_or_lagging_group_fails_closed() {
        let mut joint = inventories();
        for inventory in joint.values_mut() {
            inventory.groups[0].membership.outgoing_voters = vec![1, 2, 3];
        }
        assert!(verify_inventories(&joint, 1).is_err());

        let mut lagging = inventories();
        lagging.get_mut(&1).expect("leader").groups[0].replication_progress[1].matched_index = 11;
        assert!(verify_inventories(&lagging, 1).is_err());
    }

    #[test]
    fn maintenance_cli_requires_https_authorities_and_bounded_rounds() {
        let cli = Cli::try_parse_from([
            "epoch-maintenance",
            "--endpoints",
            "http://epoch-node-0:7701",
            "--node-id",
            "1",
            "--tls-ca",
            "ca.crt",
            "--tls-certificate",
            "tls.crt",
            "--tls-private-key",
            "tls.key",
            "verify",
        ])
        .expect("syntactically valid CLI");
        assert!(validate_args(&cli).is_err());
        assert!(Cli::try_parse_from(["epoch-maintenance", "verify"]).is_err());
    }
}
