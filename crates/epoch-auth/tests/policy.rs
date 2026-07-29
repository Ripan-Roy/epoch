use epoch_auth::{
    Action, AuthenticationErrorKind, BootstrapPolicy, Decision, DecisionEvent, DecisionReason,
    ResourceScope,
};
use serde::Deserialize;
use serde_json::{Value, json};

const POLICY: &[u8] = include_bytes!("../../../spec/auth/bootstrap-policy-v1.example.json");
const DECISIONS: &[u8] = include_bytes!("../../../spec/auth/bootstrap-policy-v1-decisions.json");

#[derive(Debug, Deserialize)]
struct DecisionCorpus {
    format_version: u32,
    cases: Vec<DecisionCase>,
}

#[derive(Debug, Deserialize)]
struct DecisionCase {
    name: String,
    token: String,
    action: Action,
    scope: ResourceScope,
    allowed: bool,
}

type PolicyMutation = Box<dyn Fn(&mut Value)>;

#[test]
fn bootstrap_policy_matches_cross_language_decision_corpus() {
    let policy = BootstrapPolicy::from_json(POLICY).expect("example policy must parse");
    let corpus: DecisionCorpus =
        serde_json::from_slice(DECISIONS).expect("decision corpus must parse");
    assert_eq!(corpus.format_version, 1);
    for case in corpus.cases {
        let principal = policy
            .authenticate_bearer(Some(&format!("Bearer {}", case.token)))
            .unwrap_or_else(|error| panic!("{} authentication failed: {error}", case.name));
        assert_eq!(
            principal.allows(case.action, &case.scope),
            case.allowed,
            "{}",
            case.name
        );
    }
}

#[test]
fn bootstrap_authentication_fails_closed_without_leaking_credentials() {
    let policy = BootstrapPolicy::from_json(POLICY).unwrap();
    for (header, expected) in [
        (None, AuthenticationErrorKind::Missing),
        (Some("Basic abc"), AuthenticationErrorKind::Malformed),
        (Some("Bearer "), AuthenticationErrorKind::Malformed),
        (Some("Bearer one two"), AuthenticationErrorKind::Malformed),
        (
            Some("Bearer not-a-real-token"),
            AuthenticationErrorKind::Invalid,
        ),
    ] {
        let error = policy.authenticate_bearer(header).unwrap_err();
        assert_eq!(error.kind(), expected);
        assert!(!error.to_string().contains("not-a-real-token"));
    }
}

#[test]
fn bootstrap_policy_rejects_ambiguous_documents() {
    let original: Value = serde_json::from_slice(POLICY).unwrap();
    let cases: Vec<(&str, PolicyMutation)> = vec![
        (
            "unknown format",
            Box::new(|value| value["format_version"] = json!(2)),
        ),
        (
            "unknown field",
            Box::new(|value| value["unexpected"] = json!(true)),
        ),
        (
            "duplicate principal id",
            Box::new(|value| {
                let mut duplicate = value["principals"][0].clone();
                duplicate["token_sha256"] =
                    json!("1111111111111111111111111111111111111111111111111111111111111111");
                value["principals"].as_array_mut().unwrap().push(duplicate);
            }),
        ),
        (
            "duplicate fingerprint",
            Box::new(|value| {
                let mut duplicate = value["principals"][0].clone();
                duplicate["id"] = json!("duplicate-token");
                value["principals"].as_array_mut().unwrap().push(duplicate);
            }),
        ),
        (
            "unknown action",
            Box::new(|value| value["principals"][0]["actions"] = json!(["root"])),
        ),
        (
            "uppercase fingerprint",
            Box::new(|value| {
                let fingerprint = value["principals"][0]["token_sha256"]
                    .as_str()
                    .unwrap()
                    .to_uppercase();
                value["principals"][0]["token_sha256"] = json!(fingerprint);
            }),
        ),
        (
            "partial wildcard",
            Box::new(|value| {
                value["principals"][0]["scope"]["organization"] = json!("acme-*");
            }),
        ),
    ];
    for (name, mutate) in cases {
        let mut candidate = original.clone();
        mutate(&mut candidate);
        let encoded = serde_json::to_vec(&candidate).unwrap();
        assert!(
            BootstrapPolicy::from_json(&encoded).is_err(),
            "{name} unexpectedly parsed"
        );
    }
}

#[test]
fn principal_exposes_stable_identity_without_credential_material() {
    let policy = BootstrapPolicy::from_json(POLICY).unwrap();
    let principal = policy
        .authenticate_bearer(Some("Bearer epoch-dev-admin-v1"))
        .unwrap();
    assert_eq!(principal.id(), "development-admin");
    assert_eq!(principal.policy_id(), "epoch-development-v1");
    let debug = format!("{principal:?}");
    assert!(!debug.contains("epoch-dev-admin-v1"));
    assert!(!debug.contains("dae2068c"));
}

#[test]
fn audit_decisions_are_bounded_and_credential_free_by_construction() {
    let event = DecisionEvent::new(
        "request-123",
        "development-reader",
        "epoch-development-v1",
        Action::ResourceRead,
        Decision::Allow,
        DecisionReason::PolicyGrant,
        ResourceScope::new("acme", "payments", "production", "orders"),
    )
    .unwrap();
    assert_eq!(event.request_id(), "request-123");
    assert_eq!(event.action(), Action::ResourceRead);
    assert_eq!(event.decision(), Decision::Allow);
    assert_eq!(event.reason(), DecisionReason::PolicyGrant);
    let encoded = serde_json::to_string(&event).unwrap();
    assert!(!encoded.contains("epoch-dev-reader-v1"));

    let oversized = "x".repeat(257);
    assert!(
        DecisionEvent::new(
            oversized,
            "principal",
            "policy",
            Action::ResourceRead,
            Decision::Deny,
            DecisionReason::ScopeMismatch,
            ResourceScope::new("", "", "", ""),
        )
        .is_err()
    );
}
