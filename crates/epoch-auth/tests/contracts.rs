use serde_json::{Value, json};

const BOOTSTRAP_SCHEMA: &[u8] =
    include_bytes!("../../../spec/auth/bootstrap-policy-v1.schema.json");
const BOOTSTRAP_EXAMPLE: &[u8] =
    include_bytes!("../../../spec/auth/bootstrap-policy-v1.example.json");
const IDENTITY_SCHEMA: &[u8] = include_bytes!("../../../spec/auth/identity-policy-v2.schema.json");
const IDENTITY_EXAMPLE: &[u8] =
    include_bytes!("../../../spec/auth/identity-policy-v2.example.json");
const AUDIT_SCHEMA: &[u8] = include_bytes!("../../../spec/auth/audit-journal-v1.schema.json");
const AUDIT_EXAMPLE: &[u8] = include_bytes!("../../../spec/auth/audit-journal-v1.example.ndjson");

#[test]
fn published_auth_examples_satisfy_their_json_schemas() {
    assert_schema_accepts(BOOTSTRAP_SCHEMA, BOOTSTRAP_EXAMPLE);
    assert_schema_accepts(IDENTITY_SCHEMA, IDENTITY_EXAMPLE);
    assert_schema_accepts(AUDIT_SCHEMA, AUDIT_EXAMPLE);
}

#[test]
fn identity_schema_requires_at_least_one_authentication_mechanism() {
    let schema: Value = serde_json::from_slice(IDENTITY_SCHEMA).unwrap();
    let validator = jsonschema::options()
        .with_pattern_options(jsonschema::PatternOptions::regex())
        .build(&schema)
        .unwrap();
    let invalid = json!({
        "format_version": 2,
        "policy_id": "empty-policy",
        "principals": []
    });
    assert!(!validator.is_valid(&invalid));

    let mut punctuation_only_issuer: Value = serde_json::from_slice(IDENTITY_EXAMPLE).unwrap();
    punctuation_only_issuer["oidc"]["issuers"][0]["issuer"] = json!("https://--");
    assert!(!validator.is_valid(&punctuation_only_issuer));

    let mut path_issuer: Value = serde_json::from_slice(IDENTITY_EXAMPLE).unwrap();
    path_issuer["oidc"]["issuers"][0]["issuer"] =
        json!("https://identity.epoch.example/realms/production/");
    assert!(validator.is_valid(&path_issuer));

    let mut user_info_issuer: Value = serde_json::from_slice(IDENTITY_EXAMPLE).unwrap();
    user_info_issuer["oidc"]["issuers"][0]["issuer"] = json!("https://user@issuer.example");
    assert!(!validator.is_valid(&user_info_issuer));
}

#[test]
fn audit_schema_rejects_non_canonical_scope_values() {
    let schema: Value = serde_json::from_slice(AUDIT_SCHEMA).unwrap();
    let validator = jsonschema::options()
        .with_pattern_options(jsonschema::PatternOptions::regex())
        .build(&schema)
        .unwrap();
    let mut invalid: Value = serde_json::from_slice(AUDIT_EXAMPLE).unwrap();
    invalid["event"]["scope"]["project"] = json!("payments/<unsafe>");
    assert!(!validator.is_valid(&invalid));
}

fn assert_schema_accepts(schema: &[u8], example: &[u8]) {
    let schema: Value = serde_json::from_slice(schema).unwrap();
    jsonschema::meta::validate(&schema).unwrap();
    let validator = jsonschema::options()
        .with_pattern_options(jsonschema::PatternOptions::regex())
        .build(&schema)
        .unwrap();
    let instance: Value = serde_json::from_slice(example).unwrap();
    if let Err(error) = validator.validate(&instance) {
        panic!("published example violates its schema: {}", error.masked());
    }
}
