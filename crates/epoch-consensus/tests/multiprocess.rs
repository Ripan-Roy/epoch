mod support;

use support::{HarnessError, Lookup, ProcessCluster, Role};

const TEST_SEED: u64 = 0x4550_4f43_485f_5052;
const PROPOSAL_ID: u64 = 101;
const PAYLOAD: &[u8] = b"committed-after-partition";
const MAX_REPLICATION_TICKS: usize = 20;

#[test]
fn process_fixture_child() {
    if support::is_process_fixture_child() {
        support::run_process_fixture_child().expect("process fixture child must run");
    }
}

#[test]
#[ignore = "real-process crash smoke runs explicitly in integration CI"]
fn persistent_three_node_partition_and_sigkill_reopen() -> Result<(), HarnessError> {
    let mut cluster = ProcessCluster::start(TEST_SEED)?;
    cluster.campaign(1)?;
    assert_eq!(cluster.status(1)?.role, Role::Leader);

    let baseline_commit_index = cluster.status(1)?.commit_index;
    let baseline_event_count = cluster.commit_events().len();
    cluster.partition(&[1], &[2, 3])?;
    cluster.propose(1, PROPOSAL_ID, PAYLOAD)?;

    assert_eq!(cluster.commit_events().len(), baseline_event_count);
    assert_eq!(cluster.status(1)?.commit_index, baseline_commit_index);
    assert_eq!(cluster.lookup(1, PROPOSAL_ID)?, Lookup::Pending);
    assert_eq!(cluster.lookup(2, PROPOSAL_ID)?, Lookup::Unknown);
    assert_eq!(cluster.lookup(3, PROPOSAL_ID)?, Lookup::Unknown);

    cluster.heal_all()?;
    for _ in 0..MAX_REPLICATION_TICKS {
        cluster.tick(1)?;
        if (1..=3).all(|node_id| {
            matches!(
                cluster.lookup(node_id, PROPOSAL_ID),
                Ok(Lookup::Committed(_))
            )
        }) {
            break;
        }
    }

    let expected = match cluster.lookup(1, PROPOSAL_ID)? {
        Lookup::Committed(committed) => committed,
        other => panic!("node 1 did not commit proposal {PROPOSAL_ID}: {other:?}"),
    };
    assert_eq!(expected.proposal_id, PROPOSAL_ID);
    assert_eq!(expected.payload, PAYLOAD);
    for node_id in 2..=3 {
        assert_eq!(
            cluster.lookup(node_id, PROPOSAL_ID)?,
            Lookup::Committed(expected.clone())
        );
    }
    let published = &cluster.commit_events()[baseline_event_count..];
    assert_eq!(published.len(), 3);
    for node_id in 1..=3 {
        assert!(
            published
                .iter()
                .any(|event| { event.node_id == node_id && event.committed == expected })
        );
    }
    let expected_digest = cluster.digest(1)?;
    assert_eq!(cluster.digest(2)?, expected_digest);
    assert_eq!(cluster.digest(3)?, expected_digest);

    let event_count_before_reopen = cluster.commit_events().len();
    let recovery = cluster.crash_and_reopen(1)?;
    assert!(recovery.stable_generation > 0);
    assert_eq!(cluster.commit_events().len(), event_count_before_reopen);
    assert_eq!(
        cluster.lookup(1, PROPOSAL_ID)?,
        Lookup::Committed(expected.clone())
    );
    assert_eq!(cluster.digest(1)?, expected_digest);

    let event_count_before_full_reopen = cluster.commit_events().len();
    cluster.crash_all_and_reopen()?;
    assert_eq!(
        cluster.commit_events().len(),
        event_count_before_full_reopen
    );
    for node_id in 1..=3 {
        assert_eq!(
            cluster.lookup(node_id, PROPOSAL_ID)?,
            Lookup::Committed(expected.clone())
        );
        assert_eq!(cluster.digest(node_id)?, expected_digest);
    }

    cluster.finish();
    Ok(())
}
