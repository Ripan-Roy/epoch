use axum::{body::Body, http::Request};
use epoch_node::{
    regional_topology::{NodeTopology, regional_topology_router},
    tablet_materializer::TabletDirectory,
};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn topology_reports_fixed_voters_and_live_group_capacity() {
    let topology = NodeTopology::new(
        2,
        "ap-south",
        "ap-south-1b",
        "general-purpose",
        [1, 2, 3],
        16,
    )
    .expect("topology should be valid");
    let response = regional_topology_router(topology, TabletDirectory::default())
        .oneshot(
            Request::get("/experimental/v1/regional/topology")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    let encoded = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&encoded).unwrap();

    assert_eq!(body["node_id"], "2");
    assert_eq!(body["region"], "ap-south");
    assert_eq!(body["zone"], "ap-south-1b");
    assert_eq!(body["node_class"], "general-purpose");
    assert_eq!(
        body["consensus_voter_node_ids"],
        serde_json::json!(["1", "2", "3"])
    );
    assert_eq!(body["capacity"]["max_consensus_groups"], 16);
    assert_eq!(body["capacity"]["used_consensus_groups"], 1);
    assert_eq!(body["capacity"]["available_consensus_groups"], 15);
}

#[test]
fn topology_rejects_ambiguous_or_unbounded_identity() {
    assert!(NodeTopology::new(0, "ap-south", "zone-a", "general", [1, 2, 3], 16).is_err());
    assert!(NodeTopology::new(2, "ap south", "zone-a", "general", [1, 2, 3], 16).is_err());
    assert!(NodeTopology::new(2, "ap-south", "", "general", [1, 2, 3], 16).is_err());
    assert!(NodeTopology::new(2, "ap-south", "zone-a", "general", [1, 1, 2], 16).is_err());
    assert!(NodeTopology::new(4, "ap-south", "zone-a", "general", [1, 2, 3], 16).is_err());
    assert!(NodeTopology::new(2, "ap-south", "zone-a", "general", [1, 2, 3], 0).is_err());
}
