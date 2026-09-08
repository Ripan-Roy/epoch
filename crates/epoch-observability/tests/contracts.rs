use std::time::Duration;

use epoch_observability::{
    LatencyCause, MetricsRegistry, OperationDimensions, Outcome, Protocol, ProtocolObservation,
    ProtocolOperation, RequestObservation, Stage, TenantScope, TraceParent, classify_http_request,
    otlp_traces_endpoint, validate_otlp_http_base,
};

#[test]
fn otlp_base_is_unambiguous_and_resolves_the_trace_signal_path() {
    let base = validate_otlp_http_base("http://collector:4318/").unwrap();
    assert_eq!(base, "http://collector:4318");
    assert_eq!(
        otlp_traces_endpoint(&base),
        "http://collector:4318/v1/traces"
    );
    for invalid in [
        "collector:4318",
        "file:///tmp/traces",
        "https://user:secret@collector:4318",
        "https://collector:4318?token=secret",
        "https://collector:4318/custom",
    ] {
        assert!(
            validate_otlp_http_base(invalid).is_err(),
            "accepted {invalid}"
        );
    }
}

#[test]
fn traceparent_accepts_canonical_w3c_context_and_rejects_unsafe_variants() {
    let context =
        TraceParent::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
    assert_eq!(context.version(), 0);
    assert_eq!(context.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(context.parent_id(), "00f067aa0ba902b7");
    assert!(context.sampled());
    assert_eq!(
        context.to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    );
    let extended_flags =
        TraceParent::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03").unwrap();
    assert!(extended_flags.sampled());
    assert_eq!(
        extended_flags.to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03"
    );

    for invalid in [
        "",
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-FF",
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
    ] {
        assert!(TraceParent::parse(invalid).is_err(), "accepted {invalid}");
    }
}

#[test]
fn classifier_removes_customer_resource_names_and_bounds_operations() {
    let dimensions = classify_http_request(
        "POST",
        "/v1/organizations/acme/projects/shop/environments/prod/namespaces/payments/streams/orders-2026/shards/19/records",
    );
    assert_eq!(dimensions.profile, "stream");
    assert_eq!(dimensions.operation, "produce");
    assert_eq!(dimensions.tenant.fingerprint(), "6dd52d1e8f77d816");
    assert!(!format!("{dimensions:?}").contains("orders-2026"));

    let unknown = classify_http_request(
        "POST",
        "/v1/organizations/acme/projects/shop/environments/prod/namespaces/payments/streams/orders-2026/shards/19/customer-controlled-operation",
    );
    assert_eq!(unknown.operation, "other");

    assert_eq!(
        classify_http_request("POST", "/v1/queues/jobs/acquire"),
        OperationDimensions::unscoped("queue", "acquire")
    );
}

#[test]
fn registry_caps_tenants_and_emits_deterministic_prometheus_histograms() {
    let registry = MetricsRegistry::new("epoch-node", 2).unwrap();
    for (tenant, elapsed, status) in [
        (
            TenantScope::new("acme", "shop", "prod", "core").unwrap(),
            4,
            200,
        ),
        (
            TenantScope::new("acme", "risk", "prod", "core").unwrap(),
            90,
            503,
        ),
        (
            TenantScope::new("acme", "third", "prod", "core").unwrap(),
            900,
            429,
        ),
    ] {
        registry.record_request(&RequestObservation {
            dimensions: OperationDimensions::new("cache", "get", tenant),
            outcome: Outcome::from_status(status),
            elapsed: Duration::from_millis(elapsed),
        });
    }

    let metrics = registry.render_prometheus();
    assert!(metrics.starts_with("# HELP epoch_http_requests_total"));
    assert!(metrics.contains("service=\"epoch-node\""));
    assert!(metrics.contains("tenant=\"overflow\""));
    assert!(metrics.contains("le=\"+Inf\""));
    assert!(metrics.contains("epoch_http_request_duration_seconds_sum"));
    assert!(!metrics.contains("acme"));
    assert!(!metrics.contains("shop"));
    assert_eq!(registry.snapshot().tenant_count, 2);
    assert_eq!(registry.snapshot().overflowed_tenants, 1);
}

#[test]
fn diagnostic_attributes_tail_latency_to_the_dominant_stage() {
    let registry = MetricsRegistry::new("epoch-node", 8).unwrap();
    let tenant = TenantScope::new("acme", "shop", "prod", "core").unwrap();
    for latency in [20, 25, 30, 1_100] {
        registry.record_stage(
            &tenant,
            "stream",
            Stage::Replication,
            Duration::from_millis(latency),
            Outcome::Success,
        );
    }
    registry.record_stage(
        &tenant,
        "stream",
        Stage::Routing,
        Duration::from_millis(80),
        Outcome::Success,
    );

    let diagnosis = registry.diagnose(&tenant, "stream").unwrap();
    assert_eq!(diagnosis.cause, LatencyCause::Replication);
    assert_eq!(diagnosis.stage, Stage::Replication);
    assert_eq!(diagnosis.observed_p99_ms, 1_100);
    assert!(diagnosis.recommendation.contains("replica"));
}

#[test]
fn invalid_service_and_tenant_labels_fail_closed() {
    assert!(MetricsRegistry::new("customer supplied service", 1).is_err());
    assert!(MetricsRegistry::new("epoch-node", 0).is_err());
    assert!(TenantScope::new("", "shop", "prod", "core").is_err());
    assert!(TenantScope::new("acme\nleak", "shop", "prod", "core").is_err());
}

#[test]
fn protocol_metrics_use_only_closed_dimensions() {
    let registry = MetricsRegistry::new("epoch-compat", 8).unwrap();
    registry.record_protocol(&ProtocolObservation {
        protocol: Protocol::Kafka,
        operation: ProtocolOperation::Produce,
        outcome: Outcome::Success,
        elapsed: Duration::from_millis(8),
    });
    registry.record_protocol(&ProtocolObservation {
        protocol: Protocol::Amqp091,
        operation: ProtocolOperation::Publish,
        outcome: Outcome::ServerError,
        elapsed: Duration::from_millis(25),
    });

    let metrics = registry.render_prometheus();
    assert!(metrics.contains("epoch_compat_requests_total"));
    assert!(metrics.contains("protocol=\"kafka\",operation=\"produce\""));
    assert!(metrics.contains("protocol=\"amqp091\",operation=\"publish\""));
    assert!(metrics.contains("epoch_compat_request_duration_seconds_bucket"));
}
