package epoch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type managementStub struct {
	epochv1.RegionalAdminServiceClient
	deleteCall func(context.Context, *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error)
	listCall   func(context.Context, *epochv1.ListResourcesRequest) (*epochv1.ListResourcesResponse, error)
}

func (stub *managementStub) DeleteResource(ctx context.Context, request *epochv1.DeleteResourceRequest, _ ...grpc.CallOption) (*epochv1.DeleteResourceResponse, error) {
	return stub.deleteCall(ctx, request)
}

func (stub *managementStub) ListResources(ctx context.Context, request *epochv1.ListResourcesRequest, _ ...grpc.CallOption) (*epochv1.ListResourcesResponse, error) {
	return stub.listCall(ctx, request)
}

func managementTestName() *epochv1.ResourceName {
	return &epochv1.ResourceName{Organization: "acme", Project: "shop", Environment: "qa", Namespace: "core", Kind: epochv1.ResourceKind_RESOURCE_KIND_CACHE, Name: "orders"}
}

func TestManagementConfigRequiresExplicitTrustAndSafeTargets(t *testing.T) {
	for label, change := range map[string]func(*ManagementConfig){
		"no endpoints":       func(config *ManagementConfig) { config.Endpoints = nil },
		"no token":           func(config *ManagementConfig) { config.BearerToken = "" },
		"newline token":      func(config *ManagementConfig) { config.BearerToken = "secret\ninjected" },
		"whitespace token":   func(config *ManagementConfig) { config.BearerToken = " secret" },
		"zero timeout":       func(config *ManagementConfig) { config.Timeout = 0 },
		"implicit plaintext": func(config *ManagementConfig) { config.AllowInsecureLoopback = false },
		"remote plaintext":   func(config *ManagementConfig) { config.Endpoints = []string{"192.0.2.1:8081"} },
		"URL target":         func(config *ManagementConfig) { config.Endpoints = []string{"http://localhost:8081"} },
		"resolver target":    func(config *ManagementConfig) { config.Endpoints = []string{"dns:///localhost:8081"} },
		"duplicate targets":  func(config *ManagementConfig) { config.Endpoints = append(config.Endpoints, config.Endpoints[0]) },
		"invalid port":       func(config *ManagementConfig) { config.Endpoints = []string{"localhost:65536"} },
		"conflicting trust":  func(config *ManagementConfig) { config.TLS = &TLSConfig{} },
	} {
		t.Run(label, func(t *testing.T) {
			config := ManagementConfig{Endpoints: []string{"127.0.0.1:8081"}, BearerToken: "secret", Timeout: time.Second, AllowInsecureLoopback: true}
			change(&config)
			client, err := NewManagementClient(config)
			if client != nil {
				_ = client.Close()
			}
			if err == nil || strings.Contains(err.Error(), config.BearerToken) && config.BearerToken != "" {
				t.Fatalf("unsafe config accepted or credential echoed: %v", err)
			}
		})
	}
}

func TestManagementDeleteFailoverPreservesTokenPresenceDeadlineAndMetadata(t *testing.T) {
	for _, precondition := range []*uint64{nil, proto.Uint64(0), proto.Uint64(7)} {
		request := &epochv1.DeleteResourceRequest{RequestToken: "original-token", Name: managementTestName(), ExpectedGeneration: precondition}
		original := proto.Clone(request).(*epochv1.DeleteResourceRequest)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer caller-secret", "traceparent", "safe-trace"))
		deadline, _ := ctx.Deadline()
		calls := 0
		call := func(ctx context.Context, got *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
			calls++
			if !proto.Equal(got, original) {
				t.Fatal("failover altered request identity or OCC presence")
			}
			observed, _ := ctx.Deadline()
			if !observed.Equal(deadline) {
				t.Fatal("failover reset or widened the caller deadline")
			}
			md, _ := metadata.FromOutgoingContext(ctx)
			if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer sdk-secret" || md.Get("traceparent")[0] != "safe-trace" {
				t.Fatal("credential ambiguity or lost caller metadata")
			}
			if calls == 1 {
				got.RequestToken = "malicious-transport-mutation"
				got.ExpectedGeneration = proto.Uint64(42)
				return nil, status.Error(codes.Unavailable, "unknown original outcome")
			}
			return &epochv1.DeleteResourceResponse{Name: original.Name, Generation: 8, Replayed: true}, nil
		}
		client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{&managementStub{deleteCall: call}, &managementStub{deleteCall: call}}, token: "sdk-secret", timeout: 5 * time.Second}
		result, info, err := client.DeleteResource(ctx, request)
		if err != nil || !result.GetReplayed() || info.Attempts != 2 || info.Budget != 2 || info.OutcomeMayBeUnknown || !proto.Equal(request, original) {
			t.Fatalf("exact failover failed: result=%v info=%+v err=%v", result, info, err)
		}
	}
}

func TestManagementDoesNotRetrySemanticFailuresOrHideUnknownOutcomes(t *testing.T) {
	for _, code := range []codes.Code{codes.Aborted, codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument, codes.ResourceExhausted, codes.DeadlineExceeded, codes.Canceled, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			calls := 0
			failure := status.Error(code, "final detail")
			stub := &managementStub{deleteCall: func(context.Context, *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
				calls++
				return nil, failure
			}}
			client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub, stub}, token: "sdk-secret", timeout: time.Second}
			_, info, err := client.DeleteResource(context.Background(), &epochv1.DeleteResourceRequest{RequestToken: "original", Name: managementTestName()})
			want := 1
			if code == codes.Unavailable {
				want = 2
			}
			if calls != want || info.Attempts != want || status.Code(err) != code || !errors.Is(err, failure) {
				t.Fatalf("wrong attempts or lost original status: calls=%d info=%+v err=%v", calls, info, err)
			}
			if !info.OutcomeMayBeUnknown {
				t.Fatal("an error without a mutation receipt was treated as definite non-commit")
			}
		})
	}
}

func TestManagementFailsBeforeNetworkForInvalidRequestsCancellationAndClose(t *testing.T) {
	calls := 0
	stub := &managementStub{deleteCall: func(context.Context, *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
		calls++
		return nil, nil
	}}
	client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub}, token: "sdk-secret", timeout: time.Second}
	for _, request := range []*epochv1.DeleteResourceRequest{nil, {}, {Name: managementTestName()}, {RequestToken: "token"}, {RequestToken: " token ", Name: managementTestName()}} {
		if _, info, err := client.DeleteResource(context.Background(), request); err == nil || info.Attempts != 0 {
			t.Fatal("invalid mutation reached the transport")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := &epochv1.DeleteResourceRequest{RequestToken: "token", Name: managementTestName()}
	if _, info, err := client.DeleteResource(ctx, request); !errors.Is(err, context.Canceled) || status.Code(err) != codes.Canceled || info.Attempts != 0 {
		t.Fatalf("cancellation ignored: %+v, %v", info, err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal("Close was not idempotent")
	}
	if _, info, err := client.DeleteResource(context.Background(), request); err == nil || info.Attempts != 0 {
		t.Fatal("closed client accepted a mutation")
	}
	if calls != 0 {
		t.Fatal("local rejection made a network call")
	}
}

func TestManagementDoesNotReturnOrRetryInvalidMutationReceipts(t *testing.T) {
	for _, broken := range []*epochv1.DeleteResourceResponse{
		nil,
		{},
		{Name: managementTestName(), Deleted: true},
		{Name: &epochv1.ResourceName{Organization: "otherco"}, Generation: 1},
	} {
		calls := 0
		stub := &managementStub{deleteCall: func(context.Context, *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
			calls++
			return broken, nil
		}}
		client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub, stub}, token: "secret", timeout: time.Second}
		result, info, err := client.DeleteResource(context.Background(), &epochv1.DeleteResourceRequest{RequestToken: "original", Name: managementTestName()})
		if result != nil || status.Code(err) != codes.DataLoss || calls != 1 || !info.OutcomeMayBeUnknown {
			t.Fatalf("invalid receipt returned, retried, or treated as non-commit: %+v, %v", info, err)
		}
	}
}

func TestManagementCancellationAfterDispatchStopsFailoverButRetainsUnknownOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	stub := &managementStub{deleteCall: func(context.Context, *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
		calls++
		cancel()
		return nil, status.Error(codes.Unavailable, "response lost after possible commit")
	}}
	client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub, stub}, token: "secret", timeout: time.Second}
	result, info, err := client.DeleteResource(ctx, &epochv1.DeleteResourceRequest{RequestToken: "original", Name: managementTestName()})
	if result != nil || calls != 1 || info.Attempts != 1 || info.Budget != 2 || !info.OutcomeMayBeUnknown || status.Code(err) != codes.Canceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled mutation was retried or its uncertainty was lost: %+v, %v", info, err)
	}
}

func TestManagementConfiguredDeadlineBoundsTheWholeFailoverSequence(t *testing.T) {
	calls := 0
	var deadline time.Time
	first := &managementStub{deleteCall: func(ctx context.Context, _ *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
		calls++
		var exists bool
		deadline, exists = ctx.Deadline()
		if !exists {
			t.Fatal("configured timeout did not set a deadline")
		}
		return nil, status.Error(codes.Unavailable, "controller unavailable")
	}}
	last := &managementStub{deleteCall: func(ctx context.Context, _ *epochv1.DeleteResourceRequest) (*epochv1.DeleteResourceResponse, error) {
		calls++
		observed, _ := ctx.Deadline()
		if !observed.Equal(deadline) {
			t.Fatal("second controller reset the configured deadline")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{first, last, first}, token: "secret", timeout: 50 * time.Millisecond}
	result, info, err := client.DeleteResource(context.Background(), &epochv1.DeleteResourceRequest{RequestToken: "original", Name: managementTestName()})
	if result != nil || calls != 2 || info.Attempts != 2 || info.Budget != 3 || !info.OutcomeMayBeUnknown || status.Code(err) != codes.DeadlineExceeded || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired mutation was retried or its uncertainty was lost: %+v, %v", info, err)
	}
}

func TestManagementListRejectsUnknownFilterEnumsBeforeNetwork(t *testing.T) {
	calls := 0
	stub := &managementStub{listCall: func(context.Context, *epochv1.ListResourcesRequest) (*epochv1.ListResourcesResponse, error) {
		calls++
		return &epochv1.ListResourcesResponse{}, nil
	}}
	client := &ManagementClient{clients: []epochv1.RegionalAdminServiceClient{stub}, token: "secret", timeout: time.Second}
	for _, request := range []*epochv1.ListResourcesRequest{{Kind: 99}, {Classification: 99}} {
		if _, info, err := client.ListResources(context.Background(), request); status.Code(err) != codes.InvalidArgument || info.Attempts != 0 {
			t.Fatal("unknown filter enum reached the transport")
		}
	}
	if calls != 0 {
		t.Fatal("invalid filters made a network call")
	}
}
