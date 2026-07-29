package regional

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"

	controlauth "epoch.local/epoch/control/internal/auth"
	"epoch.local/epoch/control/internal/resources"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestAuthenticatedRegionalAdminRejectsMissingInvalidAndUnauthorizedCredentials(t *testing.T) {
	registry := resources.NewRegistry()
	audit := controlauth.NewMemoryAuditSink()
	client := startAuthenticatedRegionalAdminClient(t, registry, audit)
	key := regionalKey(resources.KindQueue, "jobs")
	request := applyProtoRequest(t, key, "denied-create-jobs")

	_, err := client.ListResources(t.Context(), &epochv1.ListResourcesRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ListResources(missing credential) error = %v", err)
	}
	_, err = client.ListResources(
		bearerGRPCContext(t.Context(), "not-a-policy-token"),
		&epochv1.ListResourcesRequest{},
	)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ListResources(invalid credential) error = %v", err)
	}
	_, err = client.ApplyResource(
		bearerGRPCContext(t.Context(), "epoch-dev-reader-v1"),
		request,
	)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ApplyResource(reader) error = %v", err)
	}
	if registry.Count() != 0 {
		t.Fatalf("denied apply mutated registry: count = %d", registry.Count())
	}

	events := audit.Events()
	if len(events) != 3 {
		t.Fatalf("audit events = %+v", events)
	}
	for _, event := range events {
		if strings.Contains(event.PrincipalID, "epoch-dev-") ||
			strings.Contains(event.RequestID, "epoch-dev-") {
			t.Fatalf("audit event leaked credential material: %+v", event)
		}
	}
}

func TestAuthenticatedRegionalAdminFiltersListByPrincipalScope(t *testing.T) {
	registry := resources.NewRegistry()
	audit := controlauth.NewMemoryAuditSink()
	client := startAuthenticatedRegionalAdminClient(t, registry, audit)
	adminContext := bearerGRPCContext(t.Context(), "epoch-dev-admin-v1")
	acme := resources.ResourceKey{
		Organization: "acme",
		Project:      "payments",
		Environment:  "production",
		Namespace:    "orders",
		Kind:         resources.KindStream,
		Name:         "orders",
	}
	other := resources.ResourceKey{
		Organization: "otherco",
		Project:      "payments",
		Environment:  "production",
		Namespace:    "orders",
		Kind:         resources.KindStream,
		Name:         "other-orders",
	}
	for index, key := range []resources.ResourceKey{acme, other} {
		_, err := client.ApplyResource(
			adminContext,
			applyProtoRequest(t, key, "admin-create-"+key.Organization+"-"+string(rune('0'+index))),
		)
		if err != nil {
			t.Fatalf("ApplyResource(%s) error = %v", key.Organization, err)
		}
	}

	readerResult, err := client.ListResources(
		bearerGRPCContext(t.Context(), "epoch-dev-reader-v1"),
		&epochv1.ListResourcesRequest{PageSize: 10},
	)
	if err != nil {
		t.Fatalf("ListResources(reader) error = %v", err)
	}
	if len(readerResult.GetResources()) != 1 ||
		readerResult.GetResources()[0].GetName().GetOrganization() != "acme" {
		t.Fatalf("reader list leaked cross-tenant resources: %+v", readerResult)
	}

	outsiderResult, err := client.ListResources(
		bearerGRPCContext(t.Context(), "epoch-dev-outsider-v1"),
		&epochv1.ListResourcesRequest{PageSize: 10},
	)
	if err != nil {
		t.Fatalf("ListResources(outsider) error = %v", err)
	}
	if len(outsiderResult.GetResources()) != 1 ||
		outsiderResult.GetResources()[0].GetName().GetOrganization() != "otherco" {
		t.Fatalf("outsider list leaked cross-tenant resources: %+v", outsiderResult)
	}
}

func startAuthenticatedRegionalAdminClient(
	t *testing.T,
	registry *resources.Registry,
	audit controlauth.AuditSink,
) epochv1.RegionalAdminServiceClient {
	t.Helper()
	policy, err := controlauth.LoadPolicy(filepath.Join(
		"..",
		"..",
		"..",
		"spec",
		"auth",
		"bootstrap-policy-v1.example.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return servingObservation(1, 1, 3), nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			return servingObservation(1, 1, 3), nil
		},
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(
		controlauth.NewUnaryServerInterceptor(policy, audit),
	))
	epochv1.RegisterRegionalAdminServiceServer(
		server,
		NewAuthenticatedRegionalAdminServer(
			registry,
			NewReconciler(registry, authority),
			policy,
			audit,
		),
	)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("gRPC Serve() error = %v", err)
		}
	}()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///authenticated-regional-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("gRPC Close() error = %v", err)
		}
	})
	return epochv1.NewRegionalAdminServiceClient(connection)
}

func bearerGRPCContext(ctx context.Context, token string) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.Pairs(
			"authorization",
			"Bearer "+token,
			"x-request-id",
			"grpc-auth-test",
		),
	)
}
