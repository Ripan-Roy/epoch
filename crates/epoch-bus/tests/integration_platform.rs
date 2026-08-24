use std::collections::{BTreeMap, BTreeSet};

use epoch_bus::{
    ConnectorBatchCommit, ConnectorDirection, ConnectorKind, ConnectorRecordResult,
    ConnectorRegistry, ConnectorSpec, ConnectorStatus, EndpointObservation, EndpointRegistry,
    EnrichmentDefinition, EnrichmentLimits, EventCatalog, EventCatalogEntry, EventIntegrationState,
    EventTransform, FunctionDefinition, FunctionStatus, IntegrationOperation, MqttBrokerState,
    MqttConnect, MqttPublish, MqttQos, MqttSubscription, SchemaCompatibility, SchemaField,
    SchemaFormat, SchemaRegistration, SchemaRegistry, SchemaValidationMode, SchemaValidationPolicy,
    SchemaValueType, TransformLimits,
};
use epoch_core::EventEnvelope;
use serde_json::json;

fn schema_fields() -> Vec<SchemaField> {
    vec![
        SchemaField {
            path: "order.id".into(),
            value_type: SchemaValueType::String,
            required: true,
            default: None,
        },
        SchemaField {
            path: "order.total".into(),
            value_type: SchemaValueType::Integer,
            required: true,
            default: None,
        },
    ]
}

#[test]
fn declarative_transform_supports_projection_rename_constants_and_templates_with_limits() {
    let transform = EventTransform {
        add_headers: BTreeMap::from([("routed-by".into(), "epoch".into())]),
        payload_projection: BTreeMap::from([("id".into(), "order.id".into())]),
        rename_fields: BTreeMap::from([("customer.name".into(), "customer".into())]),
        constants: BTreeMap::from([("version".into(), json!(2))]),
        templates: BTreeMap::from([(
            "summary".into(),
            "order={{order.id}} customer={{customer.name}}".into(),
        )]),
        limits: TransformLimits {
            max_operations: 16,
            max_output_bytes: 4_096,
            max_value_bytes: 2_048,
            timeout_ms: 25,
            network_access: false,
        },
        enrichment_ref: None,
    };
    let mut input = event("evt-transform");
    input.payload = json!({
        "order": {"id": "o-1"},
        "customer": {"name": "Ada"},
        "private": "discard"
    });
    let output = transform.apply(&input).unwrap();
    assert_eq!(
        output.payload,
        json!({
            "customer": "Ada",
            "id": "o-1",
            "summary": "order=o-1 customer=Ada",
            "version": 2
        })
    );
    assert_eq!(output.headers["routed-by"], "epoch");

    let mut forbidden = transform;
    forbidden.limits.network_access = true;
    assert!(forbidden.apply(&input).is_err());
}

#[test]
fn broker_validation_and_bounded_lookup_enrichment_are_applied_before_routing() {
    let mut state = EventIntegrationState::default();
    state
        .schemas_mut()
        .register(
            SchemaRegistration {
                name: "order-event".into(),
                format: SchemaFormat::JsonSchema,
                definition: r#"{"type":"object"}"#.into(),
                compatibility: SchemaCompatibility::Backward,
                root_message: None,
                fields: schema_fields(),
            },
            1,
        )
        .unwrap();
    state
        .upsert_validation_policy(SchemaValidationPolicy {
            name: "orders".into(),
            event_type_pattern: "order.*".into(),
            schema_ref: "order-event@1".into(),
            mode: SchemaValidationMode::ProducerAndBroker,
        })
        .unwrap();
    state
        .upsert_enrichment(EnrichmentDefinition {
            name: "customer-tier".into(),
            lookup_path: "customer.id".into(),
            output_field: "customer_tier".into(),
            records: BTreeMap::from([("c-1".into(), json!("gold"))]),
            required: true,
            limits: EnrichmentLimits {
                timeout_ms: 10,
                max_input_bytes: 4_096,
                max_output_bytes: 4_096,
                network_access: false,
            },
        })
        .unwrap();

    let mut valid = EventEnvelope::new(
        "checkout",
        "order.created",
        json!({
            "order": {"id": "o-1", "total": 42},
            "customer": {"id": "c-1"}
        }),
        1,
    );
    valid.schema_ref = Some("order-event@1".into());
    state.validate_for_broker(&valid).unwrap();
    let enriched = state.enrich("customer-tier", &valid).unwrap();
    assert_eq!(enriched.payload["customer_tier"], "gold");

    valid.schema_ref = None;
    assert!(state.validate_for_producer(&valid).is_err());
    assert!(state.validate_for_broker(&valid).is_err());
}

#[test]
fn managed_functions_are_revisioned_bounded_and_pause_state_survives_updates() {
    let mut state = EventIntegrationState::default();
    let definition = FunctionDefinition {
        name: "invoice".into(),
        endpoint: "https://functions.example.com/invoice".into(),
        identity: "invoice-runtime".into(),
        secret_ref: Some("invoice-auth".into()),
        timeout_ms: 2_000,
        max_input_bytes: 256 * 1024,
        outbound_allowlist: BTreeSet::from(["functions.example.com".into()]),
    };
    state
        .apply(
            IntegrationOperation::UpsertFunction {
                definition: definition.clone(),
            },
            10,
        )
        .unwrap();
    state
        .apply(
            IntegrationOperation::SetFunctionStatus {
                name: "invoice".into(),
                status: FunctionStatus::Paused,
            },
            11,
        )
        .unwrap();
    state
        .apply(
            IntegrationOperation::UpsertFunction {
                definition: FunctionDefinition {
                    timeout_ms: 3_000,
                    ..definition
                },
            },
            12,
        )
        .unwrap();
    let function = state.function("invoice").unwrap();
    assert_eq!(function.revision, 2);
    assert_eq!(function.status, FunctionStatus::Paused);
    assert_eq!(function.updated_at_ms, 12);

    assert!(
        state
            .apply(
                IntegrationOperation::UpsertFunction {
                    definition: FunctionDefinition {
                        name: "unsafe".into(),
                        endpoint: "https://private.example.com/run".into(),
                        identity: "unsafe-runtime".into(),
                        secret_ref: None,
                        timeout_ms: 1_000,
                        max_input_bytes: 1_024,
                        outbound_allowlist: BTreeSet::new(),
                    },
                },
                13,
            )
            .is_err()
    );
}

#[test]
fn schema_revisions_validate_payloads_and_enforce_compatibility() {
    let mut registry = SchemaRegistry::default();
    let first = registry
        .register(
            SchemaRegistration {
                name: "order-event".into(),
                format: SchemaFormat::JsonSchema,
                definition: r#"{"type":"object"}"#.into(),
                compatibility: SchemaCompatibility::Backward,
                root_message: None,
                fields: schema_fields(),
            },
            10,
        )
        .unwrap();
    assert_eq!(first.reference(), "order-event@1");
    registry
        .validate_payload(
            "order-event@1",
            &json!({"order": {"id": "o-1", "total": 42}}),
        )
        .unwrap();
    assert!(
        registry
            .validate_payload("order-event@1", &json!({"order": {"id": "o-1"}}))
            .unwrap_err()
            .to_string()
            .contains("order.total")
    );

    let incompatible = SchemaRegistration {
        name: "order-event".into(),
        format: SchemaFormat::JsonSchema,
        definition: r#"{"type":"object"}"#.into(),
        compatibility: SchemaCompatibility::Backward,
        root_message: None,
        fields: vec![SchemaField {
            path: "order.id".into(),
            value_type: SchemaValueType::Integer,
            required: true,
            default: None,
        }],
    };
    assert!(registry.register(incompatible, 11).is_err());

    let compatible = SchemaRegistration {
        name: "order-event".into(),
        format: SchemaFormat::JsonSchema,
        definition: r#"{"type":"object"}"#.into(),
        compatibility: SchemaCompatibility::Backward,
        root_message: None,
        fields: schema_fields()
            .into_iter()
            .chain([SchemaField {
                path: "order.currency".into(),
                value_type: SchemaValueType::String,
                required: false,
                default: Some(json!("USD")),
            }])
            .collect(),
    };
    assert_eq!(registry.register(compatible, 12).unwrap().revision, 2);
}

#[test]
fn avro_and_protobuf_definitions_are_stored_with_strict_format_validation() {
    let mut registry = SchemaRegistry::default();
    for (name, format, definition) in [
        (
            "avro-order",
            SchemaFormat::Avro,
            r#"{"type":"record","name":"Order","fields":[]}"#,
        ),
        (
            "proto-order",
            SchemaFormat::Protobuf,
            "syntax = \"proto3\"; message Order { string id = 1; }",
        ),
    ] {
        registry
            .register(
                SchemaRegistration {
                    name: name.into(),
                    format,
                    definition: definition.into(),
                    compatibility: SchemaCompatibility::None,
                    root_message: None,
                    fields: Vec::new(),
                },
                1,
            )
            .unwrap();
    }
    assert_eq!(registry.schema_count(), 2);
    assert!(
        registry
            .register(
                SchemaRegistration {
                    name: "bad-proto".into(),
                    format: SchemaFormat::Protobuf,
                    definition: "not a descriptor".into(),
                    compatibility: SchemaCompatibility::None,
                    root_message: None,
                    fields: Vec::new(),
                },
                2,
            )
            .is_err()
    );
}

#[test]
fn official_schema_definitions_drive_payload_validation_without_field_overlays() {
    let cases = [
        (
            "json-order",
            SchemaFormat::JsonSchema,
            r#"{
                "$schema":"https://json-schema.org/draft/2020-12/schema",
                "type":"object",
                "additionalProperties":false,
                "required":["id","total"],
                "properties":{
                    "id":{"type":"string","minLength":1},
                    "total":{"type":"integer","minimum":0}
                }
            }"#,
        ),
        (
            "avro-order",
            SchemaFormat::Avro,
            r#"{
                "type":"record",
                "name":"Order",
                "fields":[
                    {"name":"id","type":"string"},
                    {"name":"total","type":"long"}
                ]
            }"#,
        ),
        (
            "proto-order",
            SchemaFormat::Protobuf,
            r#"syntax = "proto2";
               package epoch.tests;
               message Order {
                   required string id = 1;
                   required int64 total = 2;
               }"#,
        ),
    ];

    for (name, format, definition) in cases {
        let mut registry = SchemaRegistry::default();
        registry
            .register(
                SchemaRegistration {
                    name: name.into(),
                    format,
                    definition: definition.into(),
                    compatibility: SchemaCompatibility::Backward,
                    root_message: None,
                    fields: Vec::new(),
                },
                1,
            )
            .unwrap();

        registry
            .validate_payload(&format!("{name}@1"), &json!({"id":"o-1","total":42}))
            .unwrap();
        let missing = registry
            .validate_payload(&format!("{name}@1"), &json!({"id":"o-1"}))
            .unwrap_err()
            .to_string();
        assert!(
            missing.contains("total"),
            "{format:?} did not identify the missing total field: {missing}"
        );
        let invalid_value = "do-not-reflect-schema-payload";
        let error = registry
            .validate_payload(
                &format!("{name}@1"),
                &json!({"id":"o-1","total":invalid_value}),
            )
            .unwrap_err()
            .to_string();
        assert!(
            !error.contains(invalid_value),
            "{format:?} reflected a rejected payload value: {error}"
        );
    }
}

#[test]
fn official_schema_compilers_reject_malformed_definitions() {
    for (name, format, definition) in [
        (
            "bad-json",
            SchemaFormat::JsonSchema,
            r#"{"type":"definitely-not-a-json-schema-type"}"#,
        ),
        (
            "bad-avro",
            SchemaFormat::Avro,
            r#"{"type":"record","name":"Order","fields":[{"name":"id"}]}"#,
        ),
        (
            "bad-proto",
            SchemaFormat::Protobuf,
            "syntax = \"proto3\"; message Order { made_up id = 1; }",
        ),
    ] {
        let error = SchemaRegistry::default()
            .register(
                SchemaRegistration {
                    name: name.into(),
                    format,
                    definition: definition.into(),
                    compatibility: SchemaCompatibility::None,
                    root_message: None,
                    fields: Vec::new(),
                },
                1,
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("schema"),
            "{format:?} returned an unscoped compiler error: {error}"
        );
    }
}

#[test]
fn compatibility_is_derived_from_each_official_schema_definition() {
    let cases = [
        (
            "json-order",
            SchemaFormat::JsonSchema,
            r#"{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}"#,
            r#"{"type":"object","required":["id","region"],"properties":{"id":{"type":"string"},"region":{"type":"string"}}}"#,
        ),
        (
            "avro-order",
            SchemaFormat::Avro,
            r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#,
            r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"region","type":"string"}]}"#,
        ),
        (
            "proto-order",
            SchemaFormat::Protobuf,
            r#"syntax = "proto3"; message Order { string id = 1; }"#,
            r#"syntax = "proto3"; message Order { int64 id = 1; }"#,
        ),
    ];

    for (name, format, first, incompatible) in cases {
        let mut registry = SchemaRegistry::default();
        registry
            .register(
                SchemaRegistration {
                    name: name.into(),
                    format,
                    definition: first.into(),
                    compatibility: SchemaCompatibility::Backward,
                    root_message: None,
                    fields: Vec::new(),
                },
                1,
            )
            .unwrap();
        let error = registry
            .register(
                SchemaRegistration {
                    name: name.into(),
                    format,
                    definition: incompatible.into(),
                    compatibility: SchemaCompatibility::Backward,
                    root_message: None,
                    fields: Vec::new(),
                },
                2,
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("compatibility"),
            "{format:?} returned an unscoped compatibility error: {error}"
        );
    }
}

#[test]
fn protobuf_root_message_selects_one_message_from_a_self_contained_definition() {
    let definition = r#"
        syntax = "proto3";
        package epoch.orders;
        message Metadata { string trace_id = 1; }
        message Order {
            string id = 1;
            Metadata metadata = 2;
        }
    "#;
    let registration = |root_message| SchemaRegistration {
        name: "proto-order".into(),
        format: SchemaFormat::Protobuf,
        definition: definition.into(),
        compatibility: SchemaCompatibility::Backward,
        root_message,
        fields: Vec::new(),
    };

    assert!(
        SchemaRegistry::default()
            .register(registration(None), 1)
            .unwrap_err()
            .to_string()
            .contains("root_message")
    );

    let mut registry = SchemaRegistry::default();
    registry
        .register(registration(Some("epoch.orders.Order".into())), 1)
        .unwrap();
    registry
        .validate_payload(
            "proto-order@1",
            &json!({"id":"o-1","metadata":{"traceId":"trace-1"}}),
        )
        .unwrap();
    assert!(
        registry
            .validate_payload(
                "proto-order@1",
                &json!({"id":"o-1","metadata":{"unknown":true}}),
            )
            .is_err()
    );
}

#[test]
fn validation_policy_modes_separate_producer_advice_from_broker_enforcement() {
    let mut state = EventIntegrationState::default();
    state
        .schemas_mut()
        .register(
            SchemaRegistration {
                name: "typed-event".into(),
                format: SchemaFormat::JsonSchema,
                definition: r#"{
                    "type":"object",
                    "required":["id"],
                    "properties":{"id":{"type":"string"}}
                }"#
                .into(),
                compatibility: SchemaCompatibility::Backward,
                root_message: None,
                fields: Vec::new(),
            },
            1,
        )
        .unwrap();
    for (name, mode) in [
        ("disabled", SchemaValidationMode::Disabled),
        ("producer", SchemaValidationMode::Producer),
        ("broker", SchemaValidationMode::Broker),
        ("both", SchemaValidationMode::ProducerAndBroker),
    ] {
        state
            .upsert_validation_policy(SchemaValidationPolicy {
                name: name.into(),
                event_type_pattern: format!("{name}.*"),
                schema_ref: "typed-event@1".into(),
                mode,
            })
            .unwrap();
    }

    let invalid = |kind: &str| {
        let mut event = EventEnvelope::new("tests", format!("{kind}.event"), json!({"id": 7}), 1);
        event.schema_ref = Some("typed-event@1".into());
        event
    };
    assert!(state.validate_for_producer(&invalid("disabled")).is_ok());
    assert!(state.validate_for_broker(&invalid("disabled")).is_ok());
    assert!(state.validate_for_producer(&invalid("producer")).is_err());
    assert!(state.validate_for_broker(&invalid("producer")).is_ok());
    assert!(state.validate_for_producer(&invalid("broker")).is_ok());
    assert!(state.validate_for_broker(&invalid("broker")).is_err());
    assert!(state.validate_for_producer(&invalid("both")).is_err());
    assert!(state.validate_for_broker(&invalid("both")).is_err());
}

fn connector(name: &str) -> ConnectorSpec {
    ConnectorSpec {
        name: name.into(),
        kind: ConnectorKind::PostgresCdc,
        direction: ConnectorDirection::Source,
        secret_refs: BTreeSet::from(["postgres-primary".into()]),
        outbound_allowlist: BTreeSet::from(["db.internal.example:5432".into()]),
        identity: "connector-orders".into(),
        config: BTreeMap::from([("publication".into(), "epoch".into())]),
    }
}

#[test]
fn connector_checkpoints_are_idempotent_and_partial_failures_are_recoverable() {
    let mut registry = ConnectorRegistry::default();
    registry.upsert(connector("orders-cdc"), 10).unwrap();
    let partial = registry
        .commit_batch(
            "orders-cdc",
            ConnectorBatchCommit {
                batch_id: "batch-1".into(),
                source_from: "lsn:100".into(),
                source_to: "lsn:102".into(),
                target_idempotency_key: "orders-cdc/lsn:102".into(),
                records: vec![
                    ConnectorRecordResult::Applied {
                        record_id: "100".into(),
                    },
                    ConnectorRecordResult::RetryableFailure {
                        record_id: "101".into(),
                        reason: "target timeout".into(),
                    },
                    ConnectorRecordResult::RoutedToError {
                        record_id: "102".into(),
                        reason: "invalid row".into(),
                    },
                ],
                committed_at_ms: 20,
            },
        )
        .unwrap();
    assert!(!partial.checkpoint_advanced);
    assert_eq!(partial.retryable_failures, 1);
    assert_eq!(partial.error_routes, 1);
    assert!(registry.checkpoint("orders-cdc").is_none());

    let complete = ConnectorBatchCommit {
        batch_id: "batch-2".into(),
        source_from: "lsn:100".into(),
        source_to: "lsn:102".into(),
        target_idempotency_key: "orders-cdc/lsn:102".into(),
        records: vec![ConnectorRecordResult::Applied {
            record_id: "101".into(),
        }],
        committed_at_ms: 21,
    };
    let receipt = registry
        .commit_batch("orders-cdc", complete.clone())
        .unwrap();
    assert!(receipt.checkpoint_advanced);
    assert_eq!(
        registry.checkpoint("orders-cdc").unwrap().source_position,
        "lsn:102"
    );
    assert_eq!(
        registry
            .commit_batch("orders-cdc", complete.clone())
            .unwrap(),
        receipt
    );
    let replay_after_crash = ConnectorBatchCommit {
        committed_at_ms: 999,
        ..complete
    };
    assert_eq!(
        registry
            .commit_batch("orders-cdc", replay_after_crash)
            .unwrap(),
        receipt
    );

    registry.pause("orders-cdc", 22).unwrap();
    assert_eq!(
        registry.connector("orders-cdc").unwrap().status,
        ConnectorStatus::Paused
    );
    assert!(
        registry
            .request_replay("orders-cdc", "lsn:1", "lsn:10", 23)
            .is_err()
    );
    registry.resume("orders-cdc", 24).unwrap();
    assert_eq!(
        registry
            .request_replay("orders-cdc", "lsn:1", "lsn:10", 25)
            .unwrap()
            .sequence,
        1
    );
}

fn event(id: &str) -> EventEnvelope {
    let mut event = EventEnvelope::new("mqtt", "sensor.reading", json!({"value": 20}), 10);
    event.id = id.into();
    event
}

#[test]
fn mqtt_sessions_retained_messages_and_shared_subscriptions_are_deterministic() {
    let mut broker = MqttBrokerState::default();
    for client_id in ["worker-a", "worker-b"] {
        broker
            .connect(MqttConnect {
                client_id: client_id.into(),
                clean_start: false,
                session_expiry_ms: 60_000,
                connected_at_ms: 1,
            })
            .unwrap();
        broker
            .subscribe(
                client_id,
                MqttSubscription {
                    topic_filter: "sensors/+/temperature".into(),
                    qos: MqttQos::AtLeastOnce,
                    shared_group: Some("processors".into()),
                },
            )
            .unwrap();
    }
    let first = broker
        .publish(MqttPublish {
            topic: "sensors/a/temperature".into(),
            qos: MqttQos::ExactlyOnce,
            retain: true,
            envelope: event("evt-1"),
            published_at_ms: 2,
        })
        .unwrap();
    let second = broker
        .publish(MqttPublish {
            topic: "sensors/a/temperature".into(),
            qos: MqttQos::ExactlyOnce,
            retain: false,
            envelope: event("evt-2"),
            published_at_ms: 3,
        })
        .unwrap();
    assert_eq!(first.deliveries.len(), 1);
    assert_eq!(second.deliveries.len(), 1);
    assert_ne!(
        first.deliveries[0].client_id,
        second.deliveries[0].client_id
    );
    assert_eq!(first.deliveries[0].qos, MqttQos::AtLeastOnce);
    assert_eq!(
        broker
            .retained("sensors/a/temperature")
            .unwrap()
            .envelope
            .id,
        "evt-1"
    );
}

#[test]
fn endpoint_failover_and_event_catalog_have_stable_ordering() {
    let mut endpoints = EndpointRegistry::default();
    for observation in [
        EndpointObservation {
            pool: "payments".into(),
            endpoint: "https://west.example/events".into(),
            region: "us-west".into(),
            priority: 20,
            healthy: true,
            observed_at_ms: 10,
        },
        EndpointObservation {
            pool: "payments".into(),
            endpoint: "https://east.example/events".into(),
            region: "us-east".into(),
            priority: 10,
            healthy: false,
            observed_at_ms: 11,
        },
    ] {
        endpoints.observe(observation).unwrap();
    }
    assert_eq!(
        endpoints.route("payments").unwrap().endpoint,
        "https://west.example/events"
    );

    let mut catalog = EventCatalog::default();
    catalog
        .upsert(EventCatalogEntry {
            event_type: "order.created".into(),
            owner: "checkout-team".into(),
            schema_ref: Some("order-event@2".into()),
            sources: BTreeSet::from(["checkout".into()]),
            consumers: BTreeSet::from(["fulfilment".into(), "analytics".into()]),
            sample_payload: Some(json!({"order": {"id": "sample"}})),
            classification: "internal".into(),
            revision: 0,
        })
        .unwrap();
    let entry = catalog.entry("order.created").unwrap();
    assert_eq!(entry.revision, 1);
    assert_eq!(entry.consumers.iter().next().unwrap(), "analytics");
}
