use std::fs;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use epoch_auth::{
    Action, AuthenticationMethod, BootstrapPolicy, Decision, DecisionEvent, DecisionEventFields,
    DecisionReason, ResourceScope,
};
use tempfile::tempdir;

const POLICY: &[u8] = include_bytes!("../../../spec/auth/bootstrap-policy-v1.example.json");
const CROSS_LANGUAGE_JOURNAL: &[u8] =
    include_bytes!("../../../spec/auth/audit-journal-v1.example.ndjson");

#[test]
fn durable_audit_journal_verifies_the_cross_language_golden_record() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("audit.ndjson");
    fs::write(&path, CROSS_LANGUAGE_JOURNAL).unwrap();
    set_owner_only(&path);
    let journal = epoch_auth::AuditJournal::open(&path).unwrap();
    let page = journal.read_page(0, 10, &audit_principal()).unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].sequence, "1");
    assert_eq!(
        page.records[0].record_sha256,
        "c10b0019b9aa4f883aed2c004c00a525fe2bd0191c9364b4a4695ab99132459e"
    );
    assert_eq!(
        page.records[0].event.authentication_method,
        Some(AuthenticationMethod::OidcEddsa)
    );
}

#[test]
fn durable_audit_journal_appends_reopens_pages_and_verifies_chain() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("nested/audit.ndjson");
    let journal = epoch_auth::AuditJournal::open(&path).unwrap();
    for sequence in 1..=3 {
        journal.record(&event(sequence)).unwrap();
    }
    let principal = audit_principal();
    let first = journal.read_page(0, 2, &principal).unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.next_sequence, "2");
    assert!(!first.end_of_journal);
    assert_eq!(first.records[0].sequence, "1");
    assert_eq!(first.records[0].previous_sha256, "0".repeat(64));
    assert_ne!(first.records[0].record_sha256, "0".repeat(64));
    journal.sync().unwrap();
    drop(journal);

    let reopened = epoch_auth::AuditJournal::open(&path).unwrap();
    let second = reopened.read_page(2, 2, &principal).unwrap();
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.next_sequence, "3");
    assert!(second.end_of_journal);
    assert_eq!(second.records[0].event.request_id, "request-3");
}

#[test]
fn durable_audit_journal_filters_tenant_scope() {
    let directory = tempdir().unwrap();
    let journal = epoch_auth::AuditJournal::open(directory.path().join("audit.ndjson")).unwrap();
    journal.record(&event(1)).unwrap();
    let other = DecisionEvent::new(DecisionEventFields {
        request_id: "request-2".into(),
        principal_id: "development-admin".into(),
        policy_id: "epoch-development-v1".into(),
        authentication_method: Some(AuthenticationMethod::BootstrapToken),
        action: Action::AuditRead,
        decision: Decision::Allow,
        reason: DecisionReason::PolicyGrant,
        scope: ResourceScope::new("otherco", "payments", "production", "orders"),
    })
    .unwrap();
    journal.record(&other).unwrap();
    let policy = BootstrapPolicy::from_json(POLICY).unwrap();
    let tenant = policy
        .authenticate_bearer(Some("Bearer epoch-dev-reader-v1"))
        .unwrap();
    assert!(!tenant.has_action(Action::AuditRead));

    let identity_policy = identity_policy_with_tenant_auditor();
    let tenant = BootstrapPolicy::from_json(identity_policy.as_bytes())
        .unwrap()
        .authenticate_bearer(Some("Bearer epoch-dev-reader-v1"))
        .unwrap();
    let page = journal.read_page(0, 10, &tenant).unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].event.scope.organization, "acme");
    assert_eq!(page.next_sequence, "2");
    assert!(page.end_of_journal);
}

#[test]
fn durable_audit_journal_rejects_tampering_partial_tail_and_weak_permissions() {
    let directory = tempdir().unwrap();
    let original = directory.path().join("original.ndjson");
    let journal = epoch_auth::AuditJournal::open(&original).unwrap();
    journal.record(&event(1)).unwrap();
    journal.sync().unwrap();
    drop(journal);
    let encoded = fs::read(&original).unwrap();

    let tampered = directory.path().join("tampered.ndjson");
    fs::write(
        &tampered,
        String::from_utf8(encoded.clone())
            .unwrap()
            .replacen("request-1", "request-9", 1),
    )
    .unwrap();
    set_owner_only(&tampered);
    assert!(
        epoch_auth::AuditJournal::open(&tampered)
            .unwrap_err()
            .to_string()
            .contains("digest")
    );

    let partial = directory.path().join("partial.ndjson");
    fs::write(&partial, &encoded[..encoded.len() - 1]).unwrap();
    set_owner_only(&partial);
    assert!(
        epoch_auth::AuditJournal::open(&partial)
            .unwrap_err()
            .to_string()
            .contains("partial")
    );

    #[cfg(unix)]
    {
        let weak = directory.path().join("weak.ndjson");
        fs::write(&weak, []).unwrap();
        fs::set_permissions(&weak, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            epoch_auth::AuditJournal::open(&weak)
                .unwrap_err()
                .to_string()
                .contains("permissions")
        );
    }
}

#[test]
fn durable_audit_journal_becomes_sticky_failed_after_live_tampering() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("audit.ndjson");
    let journal = epoch_auth::AuditJournal::open(&path).unwrap();
    journal.record(&event(1)).unwrap();
    journal.sync().unwrap();
    let mut tampered = fs::read(&path).unwrap();
    let position = tampered
        .windows("request-1".len())
        .position(|window| window == b"request-1")
        .unwrap();
    tampered[position + "request-".len()] = b'9';
    fs::write(&path, tampered).unwrap();
    assert!(journal.read_page(0, 10, &audit_principal()).is_err());
    assert!(matches!(
        journal.record(&event(2)),
        Err(epoch_auth::AuditJournalError::Failed)
    ));
}

fn event(sequence: u8) -> DecisionEvent {
    DecisionEvent::new(DecisionEventFields {
        request_id: format!("request-{sequence}"),
        principal_id: "development-admin".into(),
        policy_id: "epoch-development-v1".into(),
        authentication_method: Some(AuthenticationMethod::BootstrapToken),
        action: Action::AuditRead,
        decision: Decision::Allow,
        reason: DecisionReason::PolicyGrant,
        scope: ResourceScope::new("acme", "payments", "production", "orders"),
    })
    .unwrap()
}

fn audit_principal() -> epoch_auth::Principal {
    BootstrapPolicy::from_json(POLICY)
        .unwrap()
        .authenticate_bearer(Some("Bearer epoch-dev-admin-v1"))
        .unwrap()
}

fn identity_policy_with_tenant_auditor() -> String {
    let mut policy: serde_json::Value = serde_json::from_slice(POLICY).unwrap();
    policy["principals"][1]["actions"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!("audit.read"));
    serde_json::to_string(&policy).unwrap()
}

#[cfg(unix)]
fn set_owner_only(path: &std::path::Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[cfg(not(unix))]
fn set_owner_only(_path: &std::path::Path) {}
