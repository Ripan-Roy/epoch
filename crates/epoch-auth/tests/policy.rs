use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use epoch_auth::{
    Action, AuthenticationErrorKind, AuthenticationMethod, BootstrapPolicy, Decision,
    DecisionEvent, DecisionEventFields, DecisionReason, ResourceScope,
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::Deserialize;
use serde_json::{Value, json};

const POLICY: &[u8] = include_bytes!("../../../spec/auth/bootstrap-policy-v1.example.json");
const DECISIONS: &[u8] = include_bytes!("../../../spec/auth/bootstrap-policy-v1-decisions.json");
const IDENTITY_POLICY: &[u8] = include_bytes!("../../../spec/auth/identity-policy-v2.example.json");
const OIDC_SIGNING_SEED: [u8; 32] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
];

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
            Box::new(|value| value["format_version"] = json!(3)),
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
    let duplicate_field = String::from_utf8(POLICY.to_vec()).unwrap().replacen(
        "\"format_version\": 1",
        "\"format_version\": 1, \"format_version\": 1",
        1,
    );
    assert!(BootstrapPolicy::from_json(duplicate_field.as_bytes()).is_err());
}

#[test]
fn oidc_eddsa_authentication_enforces_signature_lifetime_role_scope_and_revocation() {
    let policy = BootstrapPolicy::from_json(IDENTITY_POLICY).unwrap();
    let claims = json!({
        "iss":"https://identity.epoch.example",
        "aud":["unrelated","epoch-api"],
        "sub":"workload-orders",
        "iat":1_000,
        "nbf":1_000,
        "exp":1_600,
        "jti":"active-token-1",
        "epoch_roles":["reader","writer"],
        "epoch_organization":"acme",
        "epoch_project":"payments",
        "epoch_environment":"production",
        "epoch_namespace":"orders"
    });
    let token = signed_oidc_token(&claims);
    let principal = policy
        .authenticate_bearer_at(Some(&format!("Bearer {token}")), 1_200)
        .unwrap();
    assert_eq!(
        principal.authentication_method(),
        AuthenticationMethod::OidcEddsa
    );
    assert!(principal.id().starts_with("oidc:"));
    assert!(principal.id().ends_with(":workload-orders"));
    assert!(principal.allows(
        Action::DataWrite,
        &ResourceScope::new("acme", "payments", "production", "orders")
    ));
    assert!(!principal.allows(
        Action::DataWrite,
        &ResourceScope::new("acme", "payments", "production", "other")
    ));

    let expired = policy
        .authenticate_bearer_at(Some(&format!("Bearer {token}")), 1_631)
        .unwrap_err();
    assert_eq!(expired.kind(), AuthenticationErrorKind::Expired);

    let future = policy
        .authenticate_bearer_at(Some(&format!("Bearer {token}")), 969)
        .unwrap_err();
    assert_eq!(future.kind(), AuthenticationErrorKind::NotYetValid);

    let mut revoked = claims.clone();
    revoked["jti"] = json!("revoked-token-1");
    let revoked = policy
        .authenticate_bearer_at(
            Some(&format!("Bearer {}", signed_oidc_token(&revoked))),
            1_200,
        )
        .unwrap_err();
    assert_eq!(revoked.kind(), AuthenticationErrorKind::Revoked);

    let mut wrong_audience = claims;
    wrong_audience["aud"] = json!("another-service");
    assert_eq!(
        policy
            .authenticate_bearer_at(
                Some(&format!("Bearer {}", signed_oidc_token(&wrong_audience))),
                1_200,
            )
            .unwrap_err()
            .kind(),
        AuthenticationErrorKind::Invalid
    );
}

#[test]
fn oidc_policy_rejects_unknown_algorithms_keys_claims_and_unbounded_lifetimes() {
    let original: Value = serde_json::from_slice(IDENTITY_POLICY).unwrap();
    let mut issuer_with_path = original.clone();
    issuer_with_path["oidc"]["issuers"][0]["issuer"] =
        json!("https://identity.epoch.example/realms/production/");
    BootstrapPolicy::from_json(&serde_json::to_vec(&issuer_with_path).unwrap()).unwrap();
    let cases: Vec<(&str, PolicyMutation)> = vec![
        (
            "duplicate claims",
            Box::new(|value| {
                value["oidc"]["scope_claims"]["organization"] = json!("epoch_roles");
            }),
        ),
        (
            "non HTTPS issuer",
            Box::new(|value| value["oidc"]["issuers"][0]["issuer"] = json!("http://issuer")),
        ),
        (
            "issuer without a valid hostname",
            Box::new(|value| value["oidc"]["issuers"][0]["issuer"] = json!("https://-")),
        ),
        (
            "issuer with user information",
            Box::new(|value| {
                value["oidc"]["issuers"][0]["issuer"] = json!("https://user@issuer.example");
            }),
        ),
        (
            "issuer with query",
            Box::new(|value| {
                value["oidc"]["issuers"][0]["issuer"] = json!("https://issuer.example?tenant=1");
            }),
        ),
        (
            "unsupported curve",
            Box::new(|value| value["oidc"]["issuers"][0]["keys"][0]["crv"] = json!("X25519")),
        ),
        (
            "unbounded lifetime",
            Box::new(|value| {
                value["oidc"]["issuers"][0]["maximum_token_lifetime_seconds"] = json!(86_401);
            }),
        ),
        (
            "unknown OIDC field",
            Box::new(|value| value["oidc"]["discovery"] = json!(true)),
        ),
    ];
    for (name, mutate) in cases {
        let mut candidate = original.clone();
        mutate(&mut candidate);
        assert!(
            BootstrapPolicy::from_json(&serde_json::to_vec(&candidate).unwrap()).is_err(),
            "{name} unexpectedly parsed"
        );
    }
}

#[test]
fn oidc_authentication_rejects_duplicate_signed_header_and_claim_names() {
    let policy = BootstrapPolicy::from_json(IDENTITY_POLICY).unwrap();
    let claims = br#"{"iss":"https://identity.epoch.example","aud":"epoch-api","sub":"workload-orders","iat":1000,"exp":1600,"jti":"active-token-1","epoch_roles":["reader"],"epoch_organization":"acme","epoch_project":"payments","epoch_environment":"production","epoch_namespace":"orders"}"#;
    let duplicate_header =
        br#"{"alg":"EdDSA","alg":"EdDSA","kid":"epoch-test-ed25519-1","typ":"JWT"}"#;
    let error = policy
        .authenticate_bearer_at(
            Some(&format!(
                "Bearer {}",
                signed_oidc_bytes(duplicate_header, claims)
            )),
            1_200,
        )
        .unwrap_err();
    assert_eq!(error.kind(), AuthenticationErrorKind::Malformed);

    let unknown_header = br#"{"alg":"EdDSA","kid":"epoch-test-ed25519-1","crit":[]}"#;
    let error = policy
        .authenticate_bearer_at(
            Some(&format!(
                "Bearer {}",
                signed_oidc_bytes(unknown_header, claims)
            )),
            1_200,
        )
        .unwrap_err();
    assert_eq!(error.kind(), AuthenticationErrorKind::Invalid);

    let duplicate_claims = String::from_utf8(claims.to_vec()).unwrap().replacen(
        "\"sub\":\"workload-orders\"",
        "\"sub\":\"workload-orders\",\"sub\":\"workload-orders\"",
        1,
    );
    let header = br#"{"alg":"EdDSA","kid":"epoch-test-ed25519-1","typ":"JWT"}"#;
    let error = policy
        .authenticate_bearer_at(
            Some(&format!(
                "Bearer {}",
                signed_oidc_bytes(header, duplicate_claims.as_bytes())
            )),
            1_200,
        )
        .unwrap_err();
    assert_eq!(error.kind(), AuthenticationErrorKind::Malformed);
}

fn signed_oidc_token(claims: &Value) -> String {
    let header = json!({
        "alg":"EdDSA",
        "kid":"epoch-test-ed25519-1",
        "typ":"JWT"
    });
    signed_oidc_bytes(
        &serde_json::to_vec(&header).unwrap(),
        &serde_json::to_vec(&claims).unwrap(),
    )
}

fn signed_oidc_bytes(header: &[u8], claims: &[u8]) -> String {
    let header = URL_SAFE_NO_PAD.encode(header);
    let claims = URL_SAFE_NO_PAD.encode(claims);
    let signing_input = format!("{header}.{claims}");
    let key_pair = Ed25519KeyPair::from_seed_unchecked(&OIDC_SIGNING_SEED).unwrap();
    assert_eq!(
        URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref()),
        "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"
    );
    let signature = URL_SAFE_NO_PAD.encode(key_pair.sign(signing_input.as_bytes()).as_ref());
    format!("{signing_input}.{signature}")
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
    let event = DecisionEvent::new(DecisionEventFields {
        request_id: "request-123".into(),
        principal_id: "development-reader".into(),
        policy_id: "epoch-development-v1".into(),
        authentication_method: Some(AuthenticationMethod::BootstrapToken),
        action: Action::ResourceRead,
        decision: Decision::Allow,
        reason: DecisionReason::PolicyGrant,
        scope: ResourceScope::new("acme", "payments", "production", "orders"),
    })
    .unwrap();
    assert_eq!(event.request_id(), "request-123");
    assert_eq!(event.action(), Action::ResourceRead);
    assert_eq!(event.decision(), Decision::Allow);
    assert_eq!(event.reason(), DecisionReason::PolicyGrant);
    let encoded = serde_json::to_string(&event).unwrap();
    assert!(!encoded.contains("epoch-dev-reader-v1"));

    let oversized = "x".repeat(257);
    assert!(
        DecisionEvent::new(DecisionEventFields {
            request_id: oversized,
            principal_id: "principal".into(),
            policy_id: "policy".into(),
            authentication_method: Some(AuthenticationMethod::BootstrapToken),
            action: Action::ResourceRead,
            decision: Decision::Deny,
            reason: DecisionReason::ScopeMismatch,
            scope: ResourceScope::new("", "", "", ""),
        })
        .is_err()
    );

    assert!(
        DecisionEvent::new(DecisionEventFields {
            request_id: "request-unsafe-scope".into(),
            principal_id: "principal".into(),
            policy_id: "policy".into(),
            authentication_method: None,
            action: Action::ResourceRead,
            decision: Decision::Deny,
            reason: DecisionReason::ScopeMismatch,
            scope: ResourceScope::new("acme", "payments/<unsafe>", "", "*"),
        })
        .is_err()
    );
}
